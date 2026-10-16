// magpie-slowdown: opt-in, lossless coalescing middleware for magpie's gateway.
//
// ─── What this middleware can and cannot do ─────────────────────────────
// magpie's `onEvent(event, ctx)` contract (measured on magpie 0.1.1260):
//
//   * `ctx` = { protocol, model, stream, path, agent, options, state, reject }.
//     `ctx.state` is per request. There is NO write/flush/delay/timer on ctx.
//   * moejs has NO setTimeout/setInterval — only Promise, queueMicrotask,
//     console and Date.
//   * `onEvent` has a ~50 ms budget. Overrunning it makes magpie send the
//     event unchanged and stop waiting, so sleeping does not slow anything.
//   * Returning `null` drops the SSE frame. Its text is gone unless it is
//     re-emitted later.
//   * Returning an object re-serializes that object as the frame's `data:`
//     JSON. Returning an array serializes the whole array as ONE frame.
//   * `data: [DONE]` never reaches onEvent.
//
// Consequence: a middleware cannot stretch a response in wall-clock time.
// The honest thing it can do is reduce the number of frames while preserving
// every character of text. That is exactly what this file does.
//
// ─── The invariant ──────────────────────────────────────────────────────
// Every text-carrying event ends up in exactly one emitted frame: either
// directly, inside a merged frame emitted later, or injected into the
// protocol's terminator / flush slot. Returning `null` therefore only ever
// withholds a frame; it never discards text. The same holds for structured
// content riding on a text frame — chat `tool_calls` and Gemini / Responses
// non-text parts are aggregated across merged frames, and a tool_call frame
// is never overtaken by its own continuation fragments (arguments are
// order-sensitive JSON). Any unexpected internal throw fails open (the
// original event is returned unchanged) so a bug here can never eat a reply.

const DEFAULTS = Object.freeze({
  enabled: false,
  intervalMs: 50,
  maxBufferSize: 1000,
});

// Legacy option keys (`merge`, `flushOnEnd`) are intentionally absent: they
// are read from stored option blobs for backwards compatibility and ignored.
// resolveOptions only picks the keys it knows, so unknown keys are inert.

function positiveInt(value, fallback) {
  const n = Number(value);
  if (!Number.isFinite(n) || n <= 0) return fallback;
  return Math.max(1, Math.floor(n));
}

function resolveOptions(options) {
  const o = options && typeof options === "object" && !Array.isArray(options) ? options : {};
  return {
    enabled: typeof o.enabled === "boolean" ? o.enabled : DEFAULTS.enabled,
    intervalMs: positiveInt(o.intervalMs, DEFAULTS.intervalMs),
    maxBufferSize: positiveInt(o.maxBufferSize, DEFAULTS.maxBufferSize),
  };
}

// ─── text shape helpers ─────────────────────────────────────────────────

function readChoicesDelta(event) {
  const c = event.choices;
  if (!Array.isArray(c) || c.length === 0) return null;
  const first = c[0];
  if (!first || typeof first !== "object") return null;
  return first;
}

function readGeminiParts(event) {
  const cands = event.candidates;
  if (!Array.isArray(cands) || cands.length === 0) return null;
  const first = cands[0];
  if (!first || typeof first !== "object") return null;
  const content = first.content;
  if (!content || typeof content !== "object" || !Array.isArray(content.parts)) return null;
  return content.parts;
}

// Concatenate every text part, in order.
function partsText(parts) {
  let out = "";
  for (const p of parts) {
    if (p && typeof p === "object" && typeof p.text === "string") out += p.text;
  }
  return out;
}

// Extract the assistant text of one event, whatever protocol shape it uses.
// Order matters: an array `delta` is also an object, so arrays are checked
// before the generic object branch.
function extractText(event) {
  if (!event || typeof event !== "object") return "";
  const choice = readChoicesDelta(event);
  if (choice && choice.delta && typeof choice.delta === "object" && typeof choice.delta.content === "string") {
    return choice.delta.content;
  }
  const d = event.delta;
  if (Array.isArray(d)) return partsText(d);
  if (typeof d === "string") {
    // Responses: only the output_text channel is assistant text.
    // function_call_arguments / reasoning deltas pass through structurally —
    // absorbing them here would corrupt output text and lose the call.
    return event.type === "response.output_text.delta" ? d : "";
  }
  if (d && typeof d === "object" && typeof d.text === "string") return d.text;
  const parts = readGeminiParts(event);
  if (parts) return partsText(parts);
  if (typeof event.text === "string") return event.text; // Responses output_text.done
  return "";
}

// True when the event actually carries assistant text. Role-only chunks
// (delta: { role: "assistant" }), usage frames and keepalives return false
// and are passed through untouched, never buffered.
function carriesText(event) {
  return extractText(event).length > 0;
}

function extractRole(event) {
  const choice = readChoicesDelta(event);
  const r = choice && choice.delta && typeof choice.delta === "object" ? choice.delta.role : undefined;
  return typeof r === "string" ? r : undefined;
}

// Rewrite a delta-parts array so all text lives in the first text part,
// keeping any non-text parts. Used when concatenating Responses-style
// `delta: [{ text }, { text }]` into a single frame.
function writeDeltaParts(parts, text) {
  const out = [];
  let written = false;
  for (const p of parts) {
    if (p && typeof p === "object" && typeof p.text === "string") {
      if (written) continue; // absorbed into the first text part
      out.push({ ...p, text });
      written = true;
      continue;
    }
    out.push(p && typeof p === "object" ? { ...p } : p);
  }
  if (!written) out.push({ text });
  return out;
}

// Clone `event` along the path that holds text so a merge never mutates the
// buffered originals (or a slot marker that must stay intact).
function cloneTextShape(event) {
  const out = { ...event };
  if (Array.isArray(event.choices)) {
    out.choices = event.choices.map((c, i) => {
      if (i !== 0 || !c || typeof c !== "object") return c;
      return { ...c, delta: c.delta && typeof c.delta === "object" && !Array.isArray(c.delta) ? { ...c.delta } : c.delta };
    });
  }
  if (Array.isArray(event.candidates)) {
    out.candidates = event.candidates.map((c, i) => {
      if (i !== 0 || !c || typeof c !== "object") return c;
      const content = c.content && typeof c.content === "object" ? { ...c.content } : c.content;
      if (content && Array.isArray(content.parts)) {
        content.parts = content.parts.map((p) => (p && typeof p === "object" ? { ...p } : p));
      }
      return { ...c, content };
    });
  }
  if (Array.isArray(event.delta)) {
    out.delta = event.delta.map((p) => (p && typeof p === "object" ? { ...p } : p));
  } else if (event.delta && typeof event.delta === "object") {
    out.delta = { ...event.delta };
  }
  return out;
}

// Write `text` (and optionally `role`) into the event's text field, creating
// the container when the structural anchor already exists. Returns false when
// the event has nowhere to put text.
function setText(event, text, role) {
  if (!event || typeof event !== "object") return false;

  // Chat Completions: choices[0].delta.content
  const choice = readChoicesDelta(event);
  if (choice) {
    const delta = choice.delta && typeof choice.delta === "object" && !Array.isArray(choice.delta) ? { ...choice.delta } : {};
    delta.content = text;
    if (role !== undefined) delta.role = role;
    event.choices[0] = { ...choice, delta };
    return true;
  }

  // Responses with array delta, Anthropic / Responses with object delta.
  if (Array.isArray(event.delta)) {
    event.delta = writeDeltaParts(event.delta, text);
    return true;
  }
  if (event.delta && typeof event.delta === "object") {
    event.delta = { ...event.delta, text };
    return true;
  }
  if (typeof event.delta === "string") {
    event.delta = text;
    return true;
  }

  // Gemini: candidates[0].content.parts[*].text
  const cands = event.candidates;
  if (Array.isArray(cands) && cands.length > 0) {
    const first = cands[0];
    if (first && typeof first === "object") {
      const content = first.content && typeof first.content === "object" ? { ...first.content } : {};
      const parts = Array.isArray(content.parts) ? content.parts : [];
      const written = writeDeltaParts(parts, text);
      content.parts = written;
      event.candidates[0] = { ...first, content };
      return true;
    }
  }

  // Responses `response.output_text.done` carries the full text in `text`.
  if (typeof event.text === "string") {
    event.text = text;
    return true;
  }

  return false;
}

// ─── merging ────────────────────────────────────────────────────────────

// The chat tool_calls carried by one event, or null when it carries none.
function chatToolCalls(event) {
  const choice = readChoicesDelta(event);
  const delta = choice && choice.delta && typeof choice.delta === "object" && !Array.isArray(choice.delta) ? choice.delta : null;
  const tcs = delta && Array.isArray(delta.tool_calls) ? delta.tool_calls : null;
  return tcs && tcs.length > 0 ? tcs : null;
}

// Clone one chat tool_call delta without sharing nested state.
function cloneToolCall(tc) {
  return tc && typeof tc === "object"
    ? { ...tc, function: tc.function && typeof tc.function === "object" ? { ...tc.function } : tc.function }
    : tc;
}

// Merge two chat tool_calls lists. Fragments are paired by `index` when the
// provider sends one; an index-less fragment continues the most recent call
// in flight (the same fallback clients like OpenCode apply with
// `latestToolIndex`). First-seen identity wins, arguments concatenate in
// arrival order. Never mutates the inputs.
function mergeChatToolCalls(a, b) {
  const out = a.map(cloneToolCall);
  const look = new Map();
  out.forEach((tc, i) => {
    if (tc && tc.index != null && !look.has(`i:${tc.index}`)) look.set(`i:${tc.index}`, i);
  });
  for (const tc of b) {
    if (!tc || typeof tc !== "object") continue;
    let at;
    if (tc.index != null) {
      const key = `i:${tc.index}`;
      at = look.get(key);
      if (at === undefined) {
        out.push(cloneToolCall(tc));
        look.set(key, out.length - 1);
        continue;
      }
    } else if (out.length === 0) {
      out.push(cloneToolCall(tc));
      continue;
    } else {
      at = out.length - 1;
    }
    const acc = out[at];
    if (acc.id === undefined && tc.id !== undefined) acc.id = tc.id;
    if (acc.type === undefined && tc.type !== undefined) acc.type = tc.type;
    if (!acc.function || typeof acc.function !== "object") {
      acc.function = tc.function && typeof tc.function === "object" ? { ...tc.function } : tc.function;
    } else if (tc.function && typeof tc.function === "object") {
      if (acc.function.name === undefined) acc.function.name = tc.function.name;
      if (acc.function.arguments === undefined) acc.function.arguments = tc.function.arguments;
      else if (tc.function.arguments !== undefined) acc.function.arguments += tc.function.arguments;
    }
  }
  return out;
}

// Write an aggregated tool_calls list onto a chat event's delta, keeping any
// tool_calls the event already carries (theirs first, then `tcs`).
function attachChatToolCalls(event, tcs) {
  const choice = readChoicesDelta(event);
  if (!choice) return false;
  const delta = choice.delta && typeof choice.delta === "object" && !Array.isArray(choice.delta) ? { ...choice.delta } : {};
  const existing = Array.isArray(delta.tool_calls) ? delta.tool_calls : [];
  delta.tool_calls = existing.length > 0 ? mergeChatToolCalls(existing, tcs) : tcs;
  event.choices[0] = { ...choice, delta };
  return true;
}

// Non-text structured parts of one event (Gemini functionCall, Responses
// array-delta items, ...): everything the text coalescer must not drop.
// `family` pins the shape so a merge never mixes protocols.
function nonTextParts(event, family) {
  if (family === "responsesParts" && Array.isArray(event.delta)) {
    return event.delta.filter((p) => !(p && typeof p === "object" && typeof p.text === "string"));
  }
  if (family === "gemini" && Array.isArray(event.candidates) && event.candidates.length > 0) {
    const content = event.candidates[0] && typeof event.candidates[0] === "object" ? event.candidates[0].content : null;
    if (content && typeof content === "object" && Array.isArray(content.parts)) {
      return content.parts.filter((p) => !(p && typeof p === "object" && typeof p.text === "string"));
    }
  }
  return null;
}

const partsFamily = (event) =>
  Array.isArray(event.delta) ? "responsesParts" : Array.isArray(event.candidates) ? "gemini" : null;

// Append non-text structured parts to an event's own parts list. Mutates
// `event` along the parts path only — safe on fresh clones, never on
// buffered originals.
function attachParts(event, extras) {
  if (Array.isArray(event.delta)) {
    event.delta = [...event.delta, ...extras];
    return true;
  }
  if (Array.isArray(event.candidates) && event.candidates[0] && typeof event.candidates[0] === "object") {
    const first = event.candidates[0];
    const content = first.content && typeof first.content === "object" ? { ...first.content } : {};
    content.parts = [...(Array.isArray(content.parts) ? content.parts : []), ...extras];
    event.candidates = [...event.candidates];
    event.candidates[0] = { ...first, content };
    return true;
  }
  return false;
}

// Produce ONE event out of many, keeping the first event's identity fields
// (id / object / created / model / index / event / type), concatenating the
// text of every event in order, and aggregating the tool_calls of every
// event by stream key.
function mergeEvents(events) {
  if (!events || events.length === 0) return null;
  if (events.length === 1) return events[0];

  const family = partsFamily(events[0]);

  let text = "";
  let role;
  let tools = null;
  let extraParts = null;
  for (const ev of events) {
    text += extractText(ev);
    const r = extractRole(ev);
    if (r !== undefined && role === undefined) role = r;
    const tcs = chatToolCalls(ev);
    if (tcs) tools = tools ? mergeChatToolCalls(tools, tcs) : tcs.map(cloneToolCall);
    if (ev !== events[0]) {
      const parts = nonTextParts(ev, family);
      if (parts && parts.length > 0) extraParts = (extraParts ?? []).concat(parts.map((p) => ({ ...p })));
    }
  }
  if (!text && role === undefined && !tools && !extraParts) return events[0];

  const merged = cloneTextShape(events[0]);
  if (!setText(merged, text, role)) return events[0];
  if (extraParts) {
    // Preserve non-text structured parts from later events: append them after
    // the merged text parts (calls semantically follow the text anyway).
    attachParts(merged, extraParts);
  }
  if (tools) {
    // `tools` is the aggregate over ALL events (events[0] included), so it
    // replaces the merged event's own list — re-merging against it would
    // double-count events[0]'s fragments.
    const choice = readChoicesDelta(merged);
    if (!choice) return events[0];
    const delta =
      choice.delta && typeof choice.delta === "object" && !Array.isArray(choice.delta) ? { ...choice.delta } : {};
    delta.tool_calls = tools;
    merged.choices[0] = { ...choice, delta };
  }
  return merged;
}

function drainPending(state) {
  const pending = state.pending;
  state.pending = [];
  state.pendingIdentity = null;
  return pending;
}

// ─── protocol detection ─────────────────────────────────────────────────

// Terminators are detected structurally rather than per `ctx.protocol`, so a
// stream whose protocol is reported wrongly (or as unknown) still never loses
// its terminator. The terminator is never withheld or dropped.
function isStreamEnd(event) {
  if (!event || typeof event !== "object") return false;
  // Chat Completions: choices[].finish_reason != null
  if (Array.isArray(event.choices)) {
    for (const c of event.choices) {
      if (c && typeof c === "object" && c.finish_reason != null) return true;
    }
  }
  // Anthropic: message_stop, or message_delta carrying a stop_reason
  if (event.event === "message_stop" || event.type === "message_stop") return true;
  if ((event.event === "message_delta" || event.type === "message_delta") && event.delta && typeof event.delta === "object" && event.delta.stop_reason != null) {
    return true;
  }
  // Responses: response.completed / response.incomplete
  if (event.type === "response.completed" || event.type === "response.incomplete") return true;
  if (event.response && typeof event.response === "object" && (event.response.status === "completed" || event.response.status === "incomplete")) {
    return true;
  }
  // Gemini: candidates[].finishReason != null
  if (Array.isArray(event.candidates)) {
    for (const c of event.candidates) {
      if (c && typeof c === "object" && c.finishReason != null) return true;
    }
  }
  return false;
}

// The protocols this middleware knows how to shape. Anything else is passed
// through unchanged — the safe default when the shapes are unknown.
const KNOWN_PROTOCOLS = new Set(["chat", "anthropic", "responses", "gemini"]);

// Protocol-specific slots where a pending tail may still be flushed. Returns
// "anthropic" / "responses" / null. Unknown protocols get null (pass-through).
function flushSlotKind(protocol, event) {
  if (!event || typeof event !== "object") return null;
  const type = event.event != null ? event.event : event.type;
  if (protocol === "anthropic") {
    if (type === "content_block_stop") return "anthropic";
    return null;
  }
  if (protocol === "responses") {
    if (event.type === "response.output_text.done") return "responses";
    return null;
  }
  return null;
}

// Anthropic slot events belong to one content block. Only treat the slot as a
// flush slot when the buffered text belongs to the same block, otherwise the
// marker must survive untouched and the tail waits for the terminator.
function slotMatchesPending(state, event) {
  if (state.pending.length === 0) return false;
  const slotIndex = event.index;
  if (slotIndex == null) return true;
  const pendingIndex = state.pending[state.pending.length - 1].index;
  if (pendingIndex == null) return true;
  return pendingIndex === slotIndex;
}

function flushSlot(state, event, kind) {
  if (state.pending.length === 0) return event;
  if (!slotMatchesPending(state, event)) return event;

  const pending = drainPending(state);
  state.lastEmitAt = Date.now();

  if (kind === "anthropic") {
    // Return the merged content_block_delta INSTEAD of the content_block_stop
    // marker: a delta is what the client is already consuming, and the marker
    // carries no text of its own.
    const merged = mergeEvents(pending);
    const text = extractText(merged);
    if (!text) return event;
    return {
      event: "content_block_delta",
      type: "content_block_delta",
      index: event.index != null ? event.index : merged.index,
      delta: { type: "text_delta", text },
    };
  }

  // responses: output_text.done is supposed to hold the FULL text of the
  // output item. Absorb the pending text only when the slot belongs to the
  // same item as everything buffered; a done event for another item passes
  // through with the provider's own text (it is already authoritative).
  if (state.pendingIdentity !== null && state.pendingIdentity !== event.item_id) return event;
  const full = state.itemAccum ? (state.itemAccum.get(event.item_id) ?? "") : "";
  if (!full) return event;
  const slot = cloneTextShape(event);
  if (!setText(slot, full)) return event;
  return slot;
}

// Attach the buffered tail to the terminator so no text — and no buffered
// tool_calls — is lost. The terminator itself is always returned, with or
// without an injection.
function injectPending(state, event) {
  if (state.pending.length === 0) return event;
  const pending = drainPending(state);
  state.holdingTools = false;
  const merged = mergeEvents(pending);
  const tail = extractText(merged);
  const tools = chatToolCalls(merged);
  const extras = nonTextParts(merged, partsFamily(merged));
  if (!tail && !tools && (!extras || extras.length === 0)) return event;
  // A terminator may carry text of its own — Gemini's final candidate usually
  // does. That text was produced AFTER everything buffered here, so the tail
  // goes in front of it, and the terminator's own text is kept, not replaced.
  const text = tail + extractText(event);
  const role = extractRole(merged);
  if (setText(event, text, role) || tools || (extras && extras.length > 0)) {
    if (tools) attachChatToolCalls(event, tools);
    if (extras && extras.length > 0) attachParts(event, extras);
    return event;
  }

  // No text field anywhere on the terminator (e.g. a bare Anthropic
  // message_stop on a malformed stream that omitted content_block_stop).
  // Never discard silently: record it so it is observable. Buffered
  // tool_calls can still ride along on a chat terminator's delta.
  if (tools) attachChatToolCalls(event, tools);
  state.orphanText = (state.orphanText || "") + tail;
  console.error("slowdown: could not attach buffered text to terminator; tail withheld");
  return event;
}

// ─── magpie entry points ────────────────────────────────────────────────

function onRequest(body, ctx) {
  if (ctx && ctx.state) {
    ctx.state.slowdown = {
      opts: resolveOptions(ctx.options),
      pending: [],
      lastEmitAt: undefined, // set when the first text frame is emitted
      accum: "",
      orphanText: "",
      protocol: typeof ctx.protocol === "string" ? ctx.protocol : "unknown",
      holdingTools: false, // a tool_call-bearing frame is buffered; later ones queue behind it
      itemAccum: null, // responses: text seen per output item (Map<item_id, string>)
      pendingIdentity: null, // responses: item_id shared by everything buffered
    };
  }
  return body;
}

function onEvent(event, ctx) {
  try {
    const state = ctx && ctx.state && ctx.state.slowdown;
    if (!state || !state.opts || !state.opts.enabled) return event;
    const protocol = typeof ctx.protocol === "string" ? ctx.protocol : state.protocol;
    // Unknown protocol: best-effort detection is all we can do, and the only
    // safe action is to pass every event through unchanged.
    if (!KNOWN_PROTOCOLS.has(protocol)) return event;

    if (isStreamEnd(event)) {
      injectPending(state, event);
      return event; // a terminator is never withheld and never dropped
    }

    const slot = flushSlotKind(protocol, event);
    if (slot) return flushSlot(state, event, slot);

    const toolCalls = chatToolCalls(event);
    const hasText = carriesText(event);
    if (!hasText && !toolCalls) return event; // role / usage / keepalive

    const now = Date.now();
    // responses: track text per output item — the output_text.done slot for
    // one item must never carry another item's text.
    if (hasText) {
      const t = extractText(event);
      state.accum += t;
      if (protocol === "responses" && typeof event.item_id === "string") {
        state.itemAccum ??= new Map();
        state.itemAccum.set(event.item_id, (state.itemAccum.get(event.item_id) ?? "") + t);
      }
    }

    // Cross-item guard (responses): everything buffered shares one item_id.
    // A delta for a different item flushes what is buffered first, so its
    // text can never be attributed to the earlier item.
    const identity = protocol === "responses" && typeof event.item_id === "string" ? event.item_id : null;
    if (identity !== null && state.pendingIdentity !== null && identity !== state.pendingIdentity) {
      const out = mergeEvents(state.pending);
      state.pending = [event];
      state.holdingTools = false;
      state.pendingIdentity = identity;
      state.lastEmitAt = now;
      return out;
    }

    // Tool_call ordering guard: arguments fragments are order-sensitive JSON.
    // Once one tool_call-bearing frame is withheld, every later tool_call-
    // bearing frame queues behind it until the next flush, so the client
    // receives the fragments in arrival order. The buffer cap still wins:
    // falling through at the cap flushes the run as one merged frame.
    if (toolCalls && state.holdingTools && state.pending.length < state.opts.maxBufferSize) {
      state.pending.push(event);
      return null;
    }

    const windowUp =
      state.lastEmitAt !== undefined &&
      (now - state.lastEmitAt >= state.opts.intervalMs || state.pending.length >= state.opts.maxBufferSize);

    // Window elapsed, or the buffer hit its cap: emit everything buffered so
    // far plus this frame as ONE merged frame.
    if (windowUp) {
      const out = mergeEvents(state.pending.length === 0 ? [event] : state.pending.concat(event));
      state.pending = [];
      state.pendingIdentity = null;
      state.holdingTools = false;
      state.lastEmitAt = now;
      return out;
    }

    // First text frame of the request goes out at once: no added first-token
    // latency. `lastEmitAt === undefined` is what "nothing emitted yet" means;
    // `pending.length === 0` cannot be used as the gate, because an empty
    // buffer would then re-emit every frame and nothing would ever coalesce.
    if (hasText && state.lastEmitAt === undefined) {
      state.lastEmitAt = now;
      return event;
    }

    // A tool_call frame with nothing buffered ahead of it cannot be overtaken:
    // pass untouched, by reference.
    if (!hasText) return event;

    // Frame withheld — its text (and any tool_calls) is preserved inside
    // state.pending and will be emitted by a later merge, a flush slot, or
    // the terminator.
    if (state.pending.length === 0) state.pendingIdentity = identity;
    if (toolCalls) state.holdingTools = true;
    state.pending.push(event);
    return null;
  } catch {
    // Fail open: a middleware bug must never eat a reply.
    return event;
  }
}

function onResponse(body, ctx) {
  return body;
}

export { onRequest, onEvent, onResponse };

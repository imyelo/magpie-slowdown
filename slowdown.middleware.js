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
// protocol's terminator. Returning `null` therefore only ever withholds a
// frame; it never discards text. The same holds for structured content riding
// on a text frame — chat `tool_calls` and Gemini / Responses non-text parts are
// aggregated across merged frames, and a tool_call frame is never overtaken by
// its own continuation fragments (arguments are order-sensitive JSON). Any
// unexpected internal throw fails open (the original event is returned
// unchanged) so a bug here can never eat a reply.
//
// ─── Which shapes are coalesced ─────────────────────────────────────────
// A withheld tail needs a frame the client reads as *text* to be released
// into. Chat Completions and Gemini have no SSE `event:` line, so their
// terminator chunk can carry the last text; Responses has
// `response.output_text.done`, documented to hold the full text of its item.
// Anthropic has neither: `content_block_delta` is its only text-carrying event,
// and the `event:` name belongs to the upstream and cannot be rewritten from
// `onEvent` (only the `data:` JSON is ours). A tail held until
// `content_block_stop` would therefore ride a frame the SDK routes to its
// textless stop handler and discards — measured as a truncated reply. So every
// Anthropic frame is passed through untouched: the middleware is a no-op for
// Anthropic clients instead of a source of lost text.
//
// The branch is chosen from the event's OWN shape, never from `ctx.protocol`:
// that field names the *client's* API, while magpie relays an upstream's bytes
// verbatim when the provider serves the client's protocol, so the two can
// contradict each other. A shape we cannot recognise is passed through too.

const DEFAULTS = Object.freeze({
  enabled: false,
  intervalMs: 50,
  maxBufferSize: 1000,
});

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

// Extract the assistant text of one event, whatever wire shape it uses.
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

  // Responses with array (parts) delta, or a delta string.
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
    if (tc && tc.index !== null && tc.index !== undefined && !look.has(`i:${tc.index}`)) look.set(`i:${tc.index}`, i);
  });
  for (const tc of b) {
    if (!tc || typeof tc !== "object") continue;
    let at;
    if (tc.index !== null && tc.index !== undefined) {
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

// ─── shape detection ────────────────────────────────────────────────────

// The wire shape of one event, read from the event itself. `ctx.protocol`
// names the client's API and can contradict the bytes on the wire, so it is
// never consulted. Returns "chat" | "anthropic" | "responses" | "gemini", or
// null when the shape is not one we can safely rewrite.
//
// `choices` / `candidates` are checked before `type`, so a chat or Gemini
// chunk that happens to carry a `type` field is still read as chat / Gemini.
// Anthropic and Responses frames never carry those arrays.
function wireShape(event) {
  if (!event || typeof event !== "object") return null;
  if (Array.isArray(event.choices)) return "chat";
  if (Array.isArray(event.candidates)) return "gemini";
  if (typeof event.type === "string") {
    if (event.type.startsWith("response.")) return "responses";
    // message_start, message_delta, content_block_start, content_block_delta,
    // content_block_stop, ... — the whole Anthropic event vocabulary.
    if (event.type.startsWith("message_") || event.type.startsWith("content_block_")) return "anthropic";
  }
  return null;
}

// Terminators are detected structurally, so an unusual or mislabelled stream
// still never loses its terminator. The terminator is never withheld or
// dropped. Only the coalesced shapes need this: an Anthropic frame never
// reaches it (see the file header).
//
// The SSE `event:` name is not part of the event object the hook receives
// (magpie splits it out before calling), so it is never read here.
function isStreamEnd(event) {
  if (!event || typeof event !== "object") return false;
  // Chat Completions: choices[].finish_reason is neither null nor undefined
  if (Array.isArray(event.choices)) {
    for (const c of event.choices) {
      if (c && typeof c === "object" && c.finish_reason !== null && c.finish_reason !== undefined) return true;
    }
  }
  // Responses: response.completed / response.incomplete
  if (event.type === "response.completed" || event.type === "response.incomplete") return true;
  if (event.response && typeof event.response === "object" && (event.response.status === "completed" || event.response.status === "incomplete")) {
    return true;
  }
  // Gemini: candidates[].finishReason is neither null nor undefined
  if (Array.isArray(event.candidates)) {
    for (const c of event.candidates) {
      if (c && typeof c === "object" && c.finishReason !== null && c.finishReason !== undefined) return true;
    }
  }
  return false;
}

// Responses: `response.output_text.done` is documented to carry the FULL text
// of its output item, so it is a slot, not a text frame — its own `text`
// repeats what the deltas already said. Fill it from the text seen for its
// item and release anything buffered for that same item, so a later
// terminator has nothing left to orphan.
//
// A done event for another item is left alone (its provider text is already
// authoritative), and an empty window is left alone too: the provider's own
// done text is the full text in that case.
function responsesDoneSlot(state, event) {
  const full = state.itemAccum ? (state.itemAccum.get(event.item_id) ?? "") : "";
  // This event is terminal for its item: release the item's accumulated text
  // now instead of holding it for the rest of the request.
  if (state.itemAccum) state.itemAccum.delete(event.item_id);
  if (state.pending.length === 0) return event;
  if (state.pendingIdentity !== null && state.pendingIdentity !== event.item_id) return event;
  if (!full) return event;
  const slot = cloneTextShape(event);
  if (!setText(slot, full)) return event;
  drainPending(state);
  state.holdingTools = false;
  state.lastEmitAt = Date.now();
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
  // Attach text and structured payload separately: a failed setText() must not
  // be masked by tool_calls/parts that happen to attach successfully, or the
  // tail would vanish without reaching the orphan-text fallback below.
  const textAttached = setText(event, text, role);
  if (tools) attachChatToolCalls(event, tools);
  if (extras && extras.length > 0) attachParts(event, extras);
  if (textAttached || !tail) return event;

  // No text field anywhere on the terminator (e.g. a bare Anthropic
  // message_stop on a malformed stream that omitted content_block_stop).
  // Never discard silently: record it so it is observable. Buffered
  // tool_calls and parts were attached above when the event had a place for
  // them.
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
      orphanText: "",
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

    // The shape decides everything. Anthropic frames, and any shape we do not
    // recognise, are the cases where no frame is guaranteed to be readable as
    // text by the client, so nothing may be withheld: pass through unchanged
    // (same object, so magpie writes the original bytes).
    const shape = wireShape(event);
    if (shape === null || shape === "anthropic") return event;

    if (isStreamEnd(event)) {
      injectPending(state, event);
      return event; // a terminator is never withheld and never dropped
    }

    if (shape === "responses" && event.type === "response.output_text.done") {
      return responsesDoneSlot(state, event);
    }

    const toolCalls = chatToolCalls(event);
    const hasText = carriesText(event);
    if (!hasText && !toolCalls) return event; // role / usage / keepalive

    const now = Date.now();
    // responses: track text per output item — the output_text.done slot for
    // one item must never carry another item's text.
    if (hasText && shape === "responses" && typeof event.item_id === "string") {
      const t = extractText(event);
      state.itemAccum ??= new Map();
      state.itemAccum.set(event.item_id, (state.itemAccum.get(event.item_id) ?? "") + t);
    }

    // Cross-item guard (responses): everything buffered shares one item_id.
    // A delta for a different item flushes what is buffered first, so its
    // text can never be attributed to the earlier item.
    const identity = shape === "responses" && typeof event.item_id === "string" ? event.item_id : null;
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
    // state.pending and will be emitted by a later merge or the terminator.
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

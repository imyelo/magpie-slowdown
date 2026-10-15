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
// withholds a frame; it never discards text. Any unexpected internal throw
// fails open (the original event is returned unchanged) so a bug here can
// never eat a reply.

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
  if (typeof d === "string") return d;
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

// Produce ONE event out of many, keeping the first event's identity fields
// (id / object / created / model / index / event / type) and concatenating the
// text of every event in order.
function mergeEvents(events) {
  if (!events || events.length === 0) return null;
  if (events.length === 1) return events[0];

  let text = "";
  let role;
  for (const ev of events) {
    text += extractText(ev);
    const r = extractRole(ev);
    if (r !== undefined && role === undefined) role = r;
  }
  if (!text && role === undefined) return events[0];

  const merged = cloneTextShape(events[0]);
  if (!setText(merged, text, role)) return events[0];
  return merged;
}

function drainPending(state) {
  const pending = state.pending;
  state.pending = [];
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
  // output item, so it carries everything seen so far plus the tail. This is
  // exact for the common single-text-item stream; a stream with several output
  // items is served best-effort (the accumulator is not split per item).
  const full = state.accum;
  const slot = cloneTextShape(event);
  if (!setText(slot, full)) return event;
  return slot;
}

// Attach the buffered tail to the terminator so no text is lost. The
// terminator itself is always returned, with or without an injection.
function injectPending(state, event) {
  if (state.pending.length === 0) return event;
  const pending = drainPending(state);
  const merged = mergeEvents(pending);
  const tail = extractText(merged);
  if (!tail) return event;
  // A terminator may carry text of its own — Gemini's final candidate usually
  // does. That text was produced AFTER everything buffered here, so the tail
  // goes in front of it, and the terminator's own text is kept, not replaced.
  const text = tail + extractText(event);
  const role = extractRole(merged);
  if (setText(event, text, role)) return event;

  // No text field anywhere on the terminator (e.g. a bare Anthropic
  // message_stop on a malformed stream that omitted content_block_stop).
  // Never discard silently: record it so it is observable.
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

    if (!carriesText(event)) return event; // role / usage / keepalive

    const now = Date.now();
    state.accum += extractText(event);

    // First text frame of the request goes out at once: no added first-token
    // latency. `lastEmitAt === undefined` is what "nothing emitted yet" means;
    // `pending.length === 0` cannot be used as the gate, because an empty
    // buffer would then re-emit every frame and nothing would ever coalesce.
    if (state.lastEmitAt === undefined) {
      state.lastEmitAt = now;
      return event;
    }

    // Window elapsed, or the buffer hit its cap: emit everything buffered so
    // far plus this frame as ONE merged frame.
    if (now - state.lastEmitAt >= state.opts.intervalMs || state.pending.length >= state.opts.maxBufferSize) {
      const out = mergeEvents(state.pending.length === 0 ? [event] : state.pending.concat(event));
      state.pending = [];
      state.lastEmitAt = now;
      return out;
    }

    // Frame withheld — its text is preserved inside state.pending and will be
    // emitted by a later merge, a flush slot, or the terminator.
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

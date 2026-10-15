// slowdown.middleware.js
//
// magpie-slowdown: opt-in middleware that shapes magpie's gateway stream.
// When enabled, the middleware either rate-limits or coalesces the
// response stream. Default behavior is pass-through.
//
// Options (set via `magpie plugin options`):
//   enabled        master switch; default false (opt-in)
//   merge          rate-limit mechanism:
//                    true  → windowed merging: coalesce events within
//                            intervalMs, emit one event per window boundary
//                    false → slot scheduling: one event per intervalMs,
//                            excess buffered and delayed to next slot
//   intervalMs     ms; output cadence for both modes (default 50)
//                    rate-limit semantics: 1000 / intervalMs events/s
//                    coalesce semantics: merge events within intervalMs
//   maxBufferSize  events; buffer cap for both modes
//   flushOnEnd     false = wait for natural boundary (Q14.D = B)

const DEFAULTS = Object.freeze({
  enabled: false,
  merge: false,
  intervalMs: 50,
  maxBufferSize: 1000,
  flushOnEnd: false,
});

function resolveOptions(options) {
  const o = options ?? {};
  return {
    enabled: typeof o.enabled === "boolean" ? o.enabled : DEFAULTS.enabled,
    merge: typeof o.merge === "boolean" ? o.merge : DEFAULTS.merge,
    intervalMs: Math.max(1, Number(o.intervalMs) || DEFAULTS.intervalMs),
    maxBufferSize: Math.max(1, Number(o.maxBufferSize) || DEFAULTS.maxBufferSize),
    flushOnEnd: typeof o.flushOnEnd === "boolean" ? o.flushOnEnd : DEFAULTS.flushOnEnd,
  };
}

export function onRequest(body, ctx) {
  ctx.state.slowdown = {
    opts: resolveOptions(ctx.options),
    buffer: [],
    nextSlotTime: undefined,
    windowStartTime: undefined,
  };
  return body;
}

export function onEvent(event, ctx) {
  const state = ctx.state && ctx.state.slowdown;
  if (!state) return event;
  if (!state.opts.enabled) return event; // opt-in: pass-through unless enabled
  if (state.opts.merge) return coalesce(event, ctx, state);
  return rateLimit(event, state);
}

export function onResponse(body, ctx) {
  // Streamed responses finish via natural boundary (flushOnEnd=false default).
  // Non-streamed responses: pass through unchanged.
  return body;
}

// rate-limit: at most one event per `intervalMs`; excess is queued and
// released at the next available slot. FIFO queue with slot-based scheduling.
//
// On overflow (buffer at maxBufferSize when a new event arrives), the oldest
// queued event is force-flushed: emitted ahead of its scheduled slot to
// make room. This trades a short-term rate overshoot for data preservation.
function rateLimit(event, state) {
  const now = Date.now();
  const slotInterval = state.opts.intervalMs;

  if (state.nextSlotTime === undefined) state.nextSlotTime = now;

  // 1. Drain any queued event whose slot has arrived.
  if (state.buffer.length > 0 && state.buffer[0].releaseAt <= now) {
    return state.buffer.shift().event;
  }

  // 2. Current event fits in the current slot window.
  if (now >= state.nextSlotTime) {
    state.nextSlotTime = Math.max(now, state.nextSlotTime) + slotInterval;
    return event;
  }

  // 3. Queue for the next slot.
  if (state.buffer.length >= state.opts.maxBufferSize) {
    // Force-flush the oldest queued event to make room for the new one.
    // The drained event is emitted slightly before its scheduled slot —
    // a short-term rate overshoot that preserves data.
    const drained = state.buffer.shift();
    state.buffer.push({ event, releaseAt: state.nextSlotTime });
    state.nextSlotTime += slotInterval;
    return drained.event;
  }
  state.buffer.push({ event, releaseAt: state.nextSlotTime });
  state.nextSlotTime += slotInterval;
  return null;
}

// coalesce: events arriving within `intervalMs` are merged into a
// single event. The first event of the request emits immediately;
// each subsequent window's boundary flushes the buffered events as
// one merged event. Stream-end waits for the natural window
// boundary (flushOnEnd=false).
//
// On overflow (buffer at maxBufferSize within a window), the buffered
// events are force-flushed: merged and emitted immediately, and the
// window is reset. This trades the intervalMs cadence for data
// preservation under sustained overload.
function coalesce(event, ctx, state) {
  const now = Date.now();

  if (state.windowStartTime === undefined) {
    state.windowStartTime = now;
    return event; // first event: emit, start window
  }

  // Within window: buffer.
  if (now - state.windowStartTime < state.opts.intervalMs) {
    if (state.buffer.length >= state.opts.maxBufferSize) {
      // Force-flush: merge buffered events and emit them now, freeing space.
      // The current event starts a new window.
      const flushed = mergeEvents(state.buffer, ctx.protocol);
      state.buffer = [event];
      state.windowStartTime = now;
      return flushed;
    }
    state.buffer.push(event);
    return null;
  }

  // Window expired: flush merged buffer; current event starts new window.
  let output = null;
  if (state.buffer.length > 0) {
    output = mergeEvents(state.buffer, ctx.protocol);
  }
  state.buffer = [event];
  state.windowStartTime = now;
  return output;
}

// Merge a buffer of events into one by concatenating their text deltas.
// First event's structure is preserved; only the text delta is replaced.
function mergeEvents(events, protocol) {
  if (events.length === 0) return null;
  if (events.length === 1) return events[0];

  let mergedText = "";
  for (const ev of events) {
    const text = extractText(ev, protocol);
    if (text) mergedText += text;
  }
  if (!mergedText) return events[0];

  const first = events[0];
  setText(first, mergedText, protocol);
  return first;
}

function parseEventData(event) {
  if (!event || event.data === undefined || event.data === null) return null;
  if (typeof event.data === "string") {
    try {
      return JSON.parse(event.data);
    } catch {
      return null;
    }
  }
  return event.data;
}

function extractText(event, protocol) {
  const data = parseEventData(event);
  if (!data) return "";
  switch (protocol) {
    case "anthropic":
      return data.delta?.text ?? "";
    case "chat":
      return data.choices?.[0]?.delta?.content ?? "";
    case "responses": {
      if (typeof data.delta === "string") return data.delta;
      if (Array.isArray(data.delta)) {
        return data.delta.map((p) => (p && typeof p.text === "string" ? p.text : "")).join("");
      }
      return data.delta?.text ?? "";
    }
    case "gemini":
      return data.candidates?.[0]?.content?.parts?.[0]?.text ?? "";
    default:
      return "";
  }
}

function setText(event, text, protocol) {
  const data = parseEventData(event);
  if (!data) return;

  switch (protocol) {
    case "anthropic":
      if (!data.delta) data.delta = {};
      data.delta.text = text;
      break;
    case "chat":
      if (!data.choices) data.choices = [{}];
      if (!data.choices[0]) data.choices[0] = {};
      if (!data.choices[0].delta) data.choices[0].delta = {};
      data.choices[0].delta.content = text;
      break;
    case "responses": {
      if (typeof data.delta === "string") {
        data.delta = text;
      } else if (Array.isArray(data.delta)) {
        if (data.delta.length === 0) data.delta.push({ text });
        else {
          if (!data.delta[0]) data.delta[0] = {};
          data.delta[0].text = text;
        }
      } else if (data.delta && typeof data.delta === "object") {
        data.delta.text = text;
      } else {
        data.delta = text;
      }
      break;
    }
    case "gemini":
      if (!data.candidates) data.candidates = [{}];
      if (!data.candidates[0]) data.candidates[0] = {};
      if (!data.candidates[0].content) data.candidates[0].content = { parts: [{}] };
      if (!data.candidates[0].content.parts) data.candidates[0].content.parts = [{}];
      if (!data.candidates[0].content.parts[0]) data.candidates[0].content.parts[0] = {};
      data.candidates[0].content.parts[0].text = text;
      break;
  }

  if (typeof event.data === "string") {
    event.data = JSON.stringify(data);
  } else {
    event.data = data;
  }
}

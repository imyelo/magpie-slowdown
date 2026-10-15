// test.mjs — node:test harness for slowdown.middleware.js
//
// Validates the design decisions:
//   - default pass-through (both enabled and merge default to false)
//   - enabled=true, merge=false  : rate-limit at `rate` events/s
//   - enabled=true, merge=true   : coalesce events within windowMs
//   - maxBufferSize              : drops oldest on overflow
//   - stream-end                 : waits for natural boundary (Q14.D = B)
//   - protocol support           : anthropic / chat / responses / gemini

import { test } from "node:test";
import assert from "node:assert/strict";

import { onRequest, onEvent } from "./slowdown.middleware.js";

// ─── helpers ────────────────────────────────────────────────────────────

function makeCtx(options = {}, protocol = "anthropic") {
  return { options, state: {}, protocol };
}

function makeAnthropicEvent(text, index = 0) {
  return {
    event: "content_block_delta",
    data: JSON.stringify({
      type: "content_block_delta",
      index,
      delta: { type: "text_delta", text },
    }),
  };
}

function makeChatEvent(text) {
  return {
    data: JSON.stringify({
      choices: [{ delta: { content: text } }],
    }),
  };
}

function makeResponsesEventString(text) {
  return { data: JSON.stringify({ delta: text }) };
}

function makeResponsesEventParts(text) {
  return { data: JSON.stringify({ delta: [{ text }] }) };
}

function makeGeminiEvent(text) {
  return {
    data: JSON.stringify({
      candidates: [{ content: { parts: [{ text }] } }],
    }),
  };
}

function getText(event, protocol = "anthropic") {
  if (!event || event.data === undefined) return null;
  const data = typeof event.data === "string" ? JSON.parse(event.data) : event.data;
  if (protocol === "anthropic") return data.delta?.text ?? null;
  if (protocol === "chat") return data.choices?.[0]?.delta?.content ?? null;
  if (protocol === "responses") return typeof data.delta === "string" ? data.delta : data.delta?.[0]?.text ?? null;
  if (protocol === "gemini") return data.candidates?.[0]?.content?.parts?.[0]?.text ?? null;
  return null;
}

// Drive a sequence of events with controlled timestamps; collect outputs.
function drive(ctx, events, timestamps) {
  const outputs = [];
  const realNow = Date.now;
  try {
    for (let i = 0; i < events.length; i++) {
      const t = timestamps[i] ?? 0;
      Date.now = () => t;
      const out = onEvent(events[i], ctx);
      if (out !== null) outputs.push({ index: i, out });
    }
  } finally {
    Date.now = realNow;
  }
  return outputs;
}

// ─── pass-through (enabled=false) ───────────────────────────────────────

test("enabled=false: pass-through regardless of merge value", () => {
  for (const merge of [false, true]) {
    const ctx = makeCtx({ enabled: false, merge });
    onRequest(null, ctx);
    const ev = makeAnthropicEvent("hello");
    assert.equal(onEvent(ev, ctx), ev, `merge=${merge}`);
  }
});

test("default (no options): enabled=false, merge=false → pass-through", () => {
  const ctx = makeCtx({});
  onRequest(null, ctx);
  const ev = makeAnthropicEvent("hello");
  assert.equal(onEvent(ev, ctx), ev);
});

// ─── rate-limit (enabled=true, merge=false) ────────────────────────────

test("rate-limit: first event emits immediately, no buffer", () => {
  const ctx = makeCtx({ enabled: true, merge: false, intervalMs: 50 });
  onRequest(null, ctx);
  const outputs = drive(ctx, [makeAnthropicEvent("a"), makeAnthropicEvent("b")], [0, 10]);
  // slotInterval = 50ms. t=0 emits. t=10 (<50) queues.
  assert.equal(outputs.length, 1);
  assert.equal(getText(outputs[0].out), "a");
});

test("rate-limit: events at slot boundary each emit", () => {
  const ctx = makeCtx({ enabled: true, merge: false, intervalMs: 50 }); // slotInterval = 50
  onRequest(null, ctx);
  const outputs = drive(
    ctx,
    [makeAnthropicEvent("a"), makeAnthropicEvent("b"), makeAnthropicEvent("c")],
    [0, 50, 100],
  );
  assert.equal(outputs.length, 3);
  assert.equal(getText(outputs[0].out), "a");
  assert.equal(getText(outputs[1].out), "b");
  assert.equal(getText(outputs[2].out), "c");
});

test("rate-limit: burst queues; queued events drain on subsequent slot-time onEvent calls", () => {
  const ctx = makeCtx({ enabled: true, merge: false, intervalMs: 50 }); // slotInterval = 50
  onRequest(null, ctx);
  // Burst of 5 events at t=0,10,20,30,40 (t=0 emits, rest queue).
  // Then 5 events at slot times t=50,100,150,200,250 (each triggers a buffer drain;
  // t=250 also emits the current event inline since the buffer is empty by then).
  const events = Array.from({ length: 10 }, (_, i) => makeAnthropicEvent(`e${i}`));
  const timestamps = [0, 10, 20, 30, 40, 50, 100, 150, 200, 250];
  const outputs = drive(ctx, events, timestamps);
  // t=0 emits e0; t=50..200 drain e1..e4 from buffer; t=250 emits e9 inline.
  assert.equal(outputs.length, 6);
  assert.equal(getText(outputs[0].out), "e0");
  assert.equal(getText(outputs[1].out), "e1");
  assert.equal(getText(outputs[2].out), "e2");
  assert.equal(getText(outputs[3].out), "e3");
  assert.equal(getText(outputs[4].out), "e4");
  assert.equal(getText(outputs[5].out), "e9");
});

test("rate-limit: maxBufferSize force-flushes oldest (preserves data)", () => {
  const ctx = makeCtx({ enabled: true, merge: false, intervalMs: 50, maxBufferSize: 3 });
  onRequest(null, ctx);
  // 10 burst events at t=0,5,...,45. After t=0 emits e0, t=5..15 fill the buffer.
  // From t=20 onwards, each new event overflows → force-flush oldest + queue new.
  const events = Array.from({ length: 10 }, (_, i) => makeAnthropicEvent(`e${i}`));
  const timestamps = Array.from({ length: 10 }, (_, i) => i * 5);
  const outputs = drive(ctx, events, timestamps);
  // Outputs: t=0 (e0) + force-flushes at t=20..45 (e1..e6). 7 total.
  assert.equal(outputs.length, 7);
  assert.equal(getText(outputs[0].out), "e0");
  assert.equal(getText(outputs[1].out), "e1");
  assert.equal(getText(outputs[2].out), "e2");
  assert.equal(getText(outputs[3].out), "e3");
  assert.equal(getText(outputs[4].out), "e4");
  assert.equal(getText(outputs[5].out), "e5");
  assert.equal(getText(outputs[6].out), "e6");
  // Buffer holds the last 3 events that arrived after the overflow chain (stranded at end of stream).
  const bufferTexts = ctx.state.slowdown.buffer.map((b) => getText(b.event));
  assert.deepEqual(bufferTexts, ["e7", "e8", "e9"]);
});

// ─── coalesce (enabled=true, merge=true) ────────────────────────────────

test("coalesce: first event emits immediately", () => {
  const ctx = makeCtx({ enabled: true, merge: true, intervalMs: 50 });
  onRequest(null, ctx);
  const outputs = drive(ctx, [makeAnthropicEvent("a")], [0]);
  assert.equal(outputs.length, 1);
  assert.equal(getText(outputs[0].out), "a");
});

test("coalesce: events within window are merged at boundary", () => {
  const ctx = makeCtx({ enabled: true, merge: true, intervalMs: 50 });
  onRequest(null, ctx);
  // t=0 emits "a". t=10,20,30 buffer b,c,d. Boundary trigger at t=50.
  const events = ["a", "b", "c", "d", "e"].map(makeAnthropicEvent);
  const timestamps = [0, 10, 20, 30, 50];
  const outputs = drive(ctx, events, timestamps);
  assert.equal(outputs.length, 2);
  assert.equal(getText(outputs[0].out), "a");
  assert.equal(getText(outputs[1].out), "bcd");
});

test("coalesce: window boundary flushes merged buffer", () => {
  const ctx = makeCtx({ enabled: true, merge: true, intervalMs: 50 });
  onRequest(null, ctx);
  // t=0 emit "a"; t=10,20,30 buffer b,c,d; t=50 flush "bcd", buffer "e";
  // t=60,70 buffer f,g; t=100 flush "efg".
  const events = ["a", "b", "c", "d", "e", "f", "g", "h"].map(makeAnthropicEvent);
  const timestamps = [0, 10, 20, 30, 50, 60, 70, 100];
  const outputs = drive(ctx, events, timestamps);
  assert.equal(outputs.length, 3);
  assert.equal(getText(outputs[0].out), "a");
  assert.equal(getText(outputs[1].out), "bcd");
  assert.equal(getText(outputs[2].out), "efg");
});

test("coalesce: stream-end waits for natural boundary (Q14.D = B)", () => {
  const ctx = makeCtx({ enabled: true, merge: true, intervalMs: 50 });
  onRequest(null, ctx);
  // t=0 emit "a"; t=10,20 buffer b,c. Stream ends at t=20. Q14.D = B: do NOT flush.
  const events = ["a", "b", "c"].map(makeAnthropicEvent);
  const timestamps = [0, 10, 20];
  const outputs = drive(ctx, events, timestamps);
  assert.equal(outputs.length, 1, "should not flush at stream end");
  assert.equal(getText(outputs[0].out), "a");
  // Buffer still holds b, c (not flushed at stream end).
  assert.equal(ctx.state.slowdown.buffer.length, 2);
});

test("coalesce: maxBufferSize force-flushes current window (preserves data)", () => {
  const ctx = makeCtx({ enabled: true, merge: true, intervalMs: 50, maxBufferSize: 3 });
  onRequest(null, ctx);
  // t=0 emit e0; t=10..30 buffer e1,e2,e3 (full). t=40 overflows within window →
  // force-flush merge(e1,e2,e3), buffer=[e4], windowStartTime=40.
  // t=50..60 buffer e5,e6.
  const events = ["e0","e1","e2","e3","e4","e5","e6"].map(makeAnthropicEvent);
  const timestamps = [0, 10, 20, 30, 40, 50, 60];
  const outputs = drive(ctx, events, timestamps);
  // Buffer at end: [e4, e5, e6] (last 3 events, no more drain triggers).
  const bufferTexts = ctx.state.slowdown.buffer.map((ev) => getText(ev));
  assert.deepEqual(bufferTexts, ["e4", "e5", "e6"]);
  // Outputs: "e0" at t=0, "e1e2e3" at t=40 (force-flush of buffered events).
  assert.equal(outputs.length, 2);
  assert.equal(getText(outputs[0].out), "e0");
  assert.equal(getText(outputs[1].out), "e1e2e3");
});

// ─── protocol support (coalesce mode) ──────────────────────────────────

test("anthropic: extracts and merges text from delta.text", () => {
  const ctx = makeCtx({ enabled: true, merge: true, intervalMs: 50 }, "anthropic");
  onRequest(null, ctx);
  const events = ["Hel", "lo ", "world", "!", "x"].map(makeAnthropicEvent);
  const timestamps = [0, 10, 20, 30, 50];
  const outputs = drive(ctx, events, timestamps);
  assert.equal(outputs.length, 2);
  assert.equal(getText(outputs[0].out, "anthropic"), "Hel");
  assert.equal(getText(outputs[1].out, "anthropic"), "lo world!");
});

test("chat: extracts and merges content from choices[0].delta.content", () => {
  const ctx = makeCtx({ enabled: true, merge: true, intervalMs: 50 }, "chat");
  onRequest(null, ctx);
  const events = ["foo", "bar", "!", "x"].map(makeChatEvent);
  const timestamps = [0, 10, 20, 50];
  const outputs = drive(ctx, events, timestamps);
  assert.equal(outputs.length, 2);
  assert.equal(getText(outputs[0].out, "chat"), "foo");
  assert.equal(getText(outputs[1].out, "chat"), "bar!");
});

test("responses (string delta): concatenates", () => {
  const ctx = makeCtx({ enabled: true, merge: true, intervalMs: 50 }, "responses");
  onRequest(null, ctx);
  const events = [makeResponsesEventString("X"), makeResponsesEventString("Y"), makeResponsesEventString("Z"), makeResponsesEventString("x")];
  const timestamps = [0, 10, 20, 50];
  const outputs = drive(ctx, events, timestamps);
  assert.equal(outputs.length, 2);
  assert.equal(getText(outputs[0].out, "responses"), "X");
  assert.equal(getText(outputs[1].out, "responses"), "YZ");
});

test("responses (parts delta): concatenates text parts", () => {
  const ctx = makeCtx({ enabled: true, merge: true, intervalMs: 50 }, "responses");
  onRequest(null, ctx);
  const events = [makeResponsesEventParts("a"), makeResponsesEventParts("b"), makeResponsesEventParts("c"), makeResponsesEventParts("x")];
  const timestamps = [0, 10, 20, 50];
  const outputs = drive(ctx, events, timestamps);
  assert.equal(outputs.length, 2);
  assert.equal(getText(outputs[0].out, "responses"), "a");
  assert.equal(getText(outputs[1].out, "responses"), "bc");
});

test("gemini: extracts and merges from candidates[0].content.parts[0].text", () => {
  const ctx = makeCtx({ enabled: true, merge: true, intervalMs: 50 }, "gemini");
  onRequest(null, ctx);
  const events = ["p1", "p2", "p3", "x"].map(makeGeminiEvent);
  const timestamps = [0, 10, 20, 50];
  const outputs = drive(ctx, events, timestamps);
  assert.equal(outputs.length, 2);
  assert.equal(getText(outputs[0].out, "gemini"), "p1");
  assert.equal(getText(outputs[1].out, "gemini"), "p2p3");
});

// ─── defaults and option parsing ────────────────────────────────────────

test("defaults: enabled=false, merge=false, intervalMs=50, maxBufferSize=1000, flushOnEnd=false", () => {
  const ctx = makeCtx({});
  onRequest(null, ctx);
  assert.equal(ctx.state.slowdown.opts.enabled, false);
  assert.equal(ctx.state.slowdown.opts.merge, false);
  assert.equal(ctx.state.slowdown.opts.intervalMs, 50);
  assert.equal(ctx.state.slowdown.opts.maxBufferSize, 1000);
  assert.equal(ctx.state.slowdown.opts.flushOnEnd, false);
});

test("enabled falls back to default when not boolean", () => {
  const ctx = makeCtx({ enabled: "yes" }); // invalid
  onRequest(null, ctx);
  assert.equal(ctx.state.slowdown.opts.enabled, false);
});

test("merge falls back to default when not boolean", () => {
  const ctx = makeCtx({ merge: 1 }); // invalid
  onRequest(null, ctx);
  assert.equal(ctx.state.slowdown.opts.merge, false);
});

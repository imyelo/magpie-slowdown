// test.mjs — node:test harness for slowdown.middleware.js
//
// The middleware has exactly one job now: reduce the number of SSE frames
// without losing a single character of text. These tests assert the
// losslessness invariant per protocol, that terminators always survive,
// that the first text frame is emitted immediately, and that a bug inside
// the middleware fails open.

import { test } from "node:test";
import assert from "node:assert/strict";

import { onRequest, onEvent, onResponse } from "./slowdown.middleware.js";

// ─── helpers ────────────────────────────────────────────────────────────
//
// magpie spreads the parsed SSE JSON across top-level fields (id, object,
// model, choices, ...). There is no `data` wrapper key. `ctx.protocol`
// selects the shape of the payload and the terminator.

function makeCtx(options = {}, protocol = "anthropic") {
  return { options, state: {}, protocol };
}

// Chat Completions
function chatDelta(text, { role } = {}) {
  const delta = {};
  if (role) delta.role = role;
  if (text !== undefined) delta.content = text;
  return { id: "chatcmpl-1", object: "chat.completion.chunk", created: 1, model: "test-model", choices: [{ index: 0, delta }] };
}
function chatTerminator(reason = "stop") {
  return { id: "chatcmpl-1", object: "chat.completion.chunk", created: 1, model: "test-model", choices: [{ index: 0, delta: {}, finish_reason: reason }] };
}

// Anthropic
function anthropicDelta(text, index = 0) {
  return { event: "content_block_delta", type: "content_block_delta", index, delta: { type: "text_delta", text } };
}
function anthropicBlockStop(index = 0) {
  return { event: "content_block_stop", type: "content_block_stop", index };
}
function anthropicMessageDelta() {
  return { event: "message_delta", type: "message_delta", delta: { stop_reason: "end_turn", stop_sequence: null } };
}
function anthropicMessageStop() {
  return { event: "message_stop", type: "message_stop" };
}

// Responses API
function responsesDeltaString(text) {
  return { type: "response.output_text.delta", item_id: "msg_1", output_index: 0, delta: text };
}
function responsesDeltaParts(text) {
  return { type: "response.output_text.delta", item_id: "msg_1", output_index: 0, delta: [{ type: "output_text", text }] };
}
function responsesTextDone(text) {
  return { type: "response.output_text.done", item_id: "msg_1", output_index: 0, content_index: 0, text };
}
function responsesCompleted() {
  return { type: "response.completed", response: { id: "resp_1", status: "completed" } };
}

// Gemini
function geminiDelta(text) {
  return { candidates: [{ index: 0, content: { role: "model", parts: [{ text }] } }] };
}
function geminiTerminator() {
  return { candidates: [{ index: 0, content: { role: "model", parts: [{ text: "" }] }, finishReason: "STOP" }] };
}

// Drive a sequence of events with controlled timestamps; collect every
// non-null return value (magpie drops a frame when onEvent returns null).
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

// Collect every character of assistant text the client would read, in the
// order it would read it. This is the lossless-view of a stream.
function collectText(outputs, protocol) {
  let acc = "";
  for (const { out } of outputs) {
    if (protocol === "chat") {
      acc += out.choices?.[0]?.delta?.content ?? "";
    } else if (protocol === "anthropic") {
      acc += out.delta?.text ?? "";
    } else if (protocol === "gemini") {
      acc += out.candidates?.[0]?.content?.parts?.map((p) => p?.text ?? "").join("") ?? "";
    } else if (protocol === "responses") {
      // `response.output_text.done` repeats the FULL accumulated text, so it
      // is asserted separately rather than concatenated again here.
      if (out.type === "response.output_text.done") continue;
      if (typeof out.delta === "string") acc += out.delta;
      else if (Array.isArray(out.delta)) acc += out.delta.map((p) => p?.text ?? "").join("");
    }
  }
  return acc;
}

const WORDS = ["The ", "quick ", "brown ", "fox ", "jumps ", "over ", "the ", "lazy ", "dog"];
const INPUT_TEXT = WORDS.join("");
// Burst timings: the first frame goes out at t=0, everything else lands inside
// the same 50 ms window and gets buffered.
const BURST_TIMES = WORDS.map((_, i) => i * 5);

// ─── 1. losslessness per protocol ───────────────────────────────────────

test("losslessness: chat — emitted text equals input text, in order", () => {
  const ctx = makeCtx({ enabled: true, intervalMs: 50 }, "chat");
  onRequest(null, ctx);
  const events = [chatDelta(undefined, { role: "assistant" }), ...WORDS.map((w) => chatDelta(w)), chatTerminator()];
  const outputs = drive(ctx, events, [0, ...BURST_TIMES, 45]);
  assert.equal(collectText(outputs, "chat"), INPUT_TEXT);
  // The role-only chunk passed straight through, untouched.
  assert.equal(outputs[0].out, events[0]);
  // Identity fields of the first event survive the merge.
  for (const { out } of outputs) {
    assert.equal(out.id, "chatcmpl-1");
    assert.equal(out.object, "chat.completion.chunk");
    assert.equal(out.model, "test-model");
    assert.equal(out.created, 1);
  }
});

test("losslessness: anthropic — emitted text equals input text, in order", () => {
  const ctx = makeCtx({ enabled: true, intervalMs: 50 }, "anthropic");
  onRequest(null, ctx);
  const events = [...WORDS.map((w) => anthropicDelta(w)), anthropicBlockStop(), anthropicMessageDelta(), anthropicMessageStop()];
  const outputs = drive(ctx, events, [...BURST_TIMES, 45, 45, 45]);
  assert.equal(collectText(outputs, "anthropic"), INPUT_TEXT);
});

// In the Responses API `response.output_text.done` is documented to carry the
// FULL text of the output item, so it is the authoritative field. The
// assertions below therefore check both halves of the invariant: the streamed
// deltas that reached the client are a prefix of the input, and the done event
// carries the complete text (so nothing is lost for either a delta-reading or
// a done-reading client).
function assertResponsesLossless(outputs, fullText) {
  const deltas = collectText(outputs, "responses");
  assert.ok(fullText.startsWith(deltas), `streamed deltas must be a prefix of the input: ${JSON.stringify(deltas)}`);
  const done = outputs.find((o) => o.out.type === "response.output_text.done");
  assert.ok(done, "the output_text.done flush slot must be emitted");
  assert.equal(done.out.text, fullText);
  assert.ok(outputs.some((o) => o.out.type === "response.completed"), "response.completed must pass through");
}

test("losslessness: responses with string delta", () => {
  const ctx = makeCtx({ enabled: true, intervalMs: 50 }, "responses");
  onRequest(null, ctx);
  const events = [...WORDS.map((w) => responsesDeltaString(w)), responsesTextDone(""), responsesCompleted()];
  const outputs = drive(ctx, events, [...BURST_TIMES, 45, 45]);
  assertResponsesLossless(outputs, INPUT_TEXT);
});

test("losslessness: responses with array (parts) delta", () => {
  const ctx = makeCtx({ enabled: true, intervalMs: 50 }, "responses");
  onRequest(null, ctx);
  const events = [...WORDS.map((w) => responsesDeltaParts(w)), responsesTextDone(""), responsesCompleted()];
  const outputs = drive(ctx, events, [...BURST_TIMES, 45, 45]);
  assertResponsesLossless(outputs, INPUT_TEXT);
});

test("losslessness: gemini — emitted text equals input text, in order", () => {
  const ctx = makeCtx({ enabled: true, intervalMs: 50 }, "gemini");
  onRequest(null, ctx);
  const events = [...WORDS.map((w) => geminiDelta(w)), geminiTerminator()];
  const outputs = drive(ctx, events, [...BURST_TIMES, 45]);
  assert.equal(collectText(outputs, "gemini"), INPUT_TEXT);
});

test("losslessness: unknown protocol passes every event through unchanged", () => {
  const ctx = makeCtx({ enabled: true, intervalMs: 50 }, "mystery");
  onRequest(null, ctx);
  const events = [...WORDS.map((w) => chatDelta(w)), chatTerminator()];
  const outputs = drive(ctx, events, [...BURST_TIMES, 45]);
  assert.equal(outputs.length, events.length);
  for (let i = 0; i < events.length; i++) assert.equal(outputs[i].out, events[i]);
});

// ─── 2. terminators always survive ──────────────────────────────────────

test("terminator survives: chat finish_reason, with a non-empty buffer", () => {
  const ctx = makeCtx({ enabled: true, intervalMs: 50 }, "chat");
  onRequest(null, ctx);
  const events = [chatDelta("a"), chatDelta("b"), chatDelta("c"), chatTerminator("length")];
  const outputs = drive(ctx, events, [0, 5, 10, 15]);
  const term = outputs.at(-1).out;
  assert.equal(term.choices[0].finish_reason, "length");
  assert.equal(collectText(outputs, "chat"), "abc");
});

test("terminator survives: anthropic message_delta + message_stop, non-empty buffer", () => {
  const ctx = makeCtx({ enabled: true, intervalMs: 50 }, "anthropic");
  onRequest(null, ctx);
  const events = [anthropicDelta("a"), anthropicDelta("b"), anthropicMessageDelta(), anthropicMessageStop()];
  const outputs = drive(ctx, events, [0, 5, 10, 10]);
  assert.equal(outputs.some((o) => o.out.event === "message_delta"), true);
  assert.equal(outputs.some((o) => o.out.event === "message_stop"), true);
  // No flush slot was seen, so the tail rides on the message_delta delta.
  assert.equal(collectText(outputs, "anthropic"), "ab");
});

test("terminator survives: responses response.completed, non-empty buffer", () => {
  const ctx = makeCtx({ enabled: true, intervalMs: 50 }, "responses");
  onRequest(null, ctx);
  // No output_text.done slot in this stream, so the tail must ride on the
  // completed event itself.
  const events = [responsesDeltaString("a"), responsesDeltaString("b"), responsesCompleted()];
  const outputs = drive(ctx, events, [0, 5, 10]);
  assert.equal(outputs.some((o) => o.out.type === "response.completed"), true);
  // response.completed already carries the authoritative full text in
  // response.output, so nothing is missing client-side; the withheld tail is
  // recorded rather than discarded silently.
  assert.equal(ctx.state.slowdown.orphanText, "b");
});

test("terminator survives: gemini finishReason, non-empty buffer", () => {
  const ctx = makeCtx({ enabled: true, intervalMs: 50 }, "gemini");
  onRequest(null, ctx);
  const events = [geminiDelta("a"), geminiDelta("b"), geminiTerminator()];
  const outputs = drive(ctx, events, [0, 5, 10]);
  assert.equal(outputs.at(-1).out.candidates[0].finishReason, "STOP");
  assert.equal(collectText(outputs, "gemini"), "ab");
});

// A terminator is allowed to carry text of its own — Gemini's final candidate
// normally does. The tail must be appended to it, never written over it.
test("terminator text is preserved: a chat finish chunk that carries content", () => {
  const ctx = makeCtx({ enabled: true, intervalMs: 50 }, "chat");
  onRequest(null, ctx);
  const finishWithText = {
    ...chatTerminator("stop"),
    choices: [{ index: 0, delta: { content: "!" }, finish_reason: "stop" }],
  };
  const outputs = drive(ctx, [chatDelta("a"), chatDelta("b"), finishWithText], [0, 5, 10]);
  assert.equal(outputs.at(-1).out.choices[0].finish_reason, "stop");
  assert.equal(collectText(outputs, "chat"), "ab!");
});

test("terminator text is preserved: a gemini final candidate that carries parts", () => {
  const ctx = makeCtx({ enabled: true, intervalMs: 50 }, "gemini");
  onRequest(null, ctx);
  const finishWithText = {
    candidates: [{ index: 0, content: { role: "model", parts: [{ text: "!" }] }, finishReason: "STOP" }],
  };
  const outputs = drive(ctx, [geminiDelta("a"), geminiDelta("b"), finishWithText], [0, 5, 10]);
  assert.equal(outputs.at(-1).out.candidates[0].finishReason, "STOP");
  assert.equal(collectText(outputs, "gemini"), "ab!");
});

// ─── 3. the tail reaches the client ─────────────────────────────────────

test("tail: a burst immediately followed by the terminator keeps the last window", () => {
  const cases = [
    ["chat", makeCtx({ enabled: true, intervalMs: 50 }, "chat"), WORDS.map((w) => chatDelta(w)), chatTerminator()],
    ["gemini", makeCtx({ enabled: true, intervalMs: 50 }, "gemini"), WORDS.map((w) => geminiDelta(w)), geminiTerminator()],
    ["anthropic", makeCtx({ enabled: true, intervalMs: 50 }, "anthropic"), WORDS.map((w) => anthropicDelta(w)), anthropicBlockStop()],
  ];
  for (const [name, ctx, deltas, tail] of cases) {
    onRequest(null, ctx);
    const events = [...deltas, tail];
    const outputs = drive(ctx, events, [...BURST_TIMES, 45]);
    const text = collectText(outputs, name);
    assert.equal(text, INPUT_TEXT, name);
    assert.ok(text.endsWith("dog"), `${name}: last window's characters must reach the client`);
  }
});

// ─── 4. first text frame goes out immediately ───────────────────────────

test("first text frame is emitted immediately (no added first-token latency)", () => {
  for (const protocol of ["chat", "anthropic", "responses", "gemini"]) {
    const ctx = makeCtx({ enabled: true, intervalMs: 50 }, protocol);
    onRequest(null, ctx);
    const make = {
      chat: () => chatDelta("first"),
      anthropic: () => anthropicDelta("first"),
      responses: () => responsesDeltaString("first"),
      gemini: () => geminiDelta("first"),
    }[protocol];
    const first = make();
    const outputs = drive(ctx, [first, make(), make()], [0, 1, 2]);
    assert.equal(outputs[0].out, first, `${protocol}: the very first frame must be the original event object`);
    assert.equal(outputs.length, 1, `${protocol}: only the first frame is emitted inside the window`);
  }
});

// ─── 5. frames are actually coalesced ───────────────────────────────────

test("coalescing reduces the frame count for a burst", () => {
  // 9 deltas inside one 50 ms window: 1 immediate frame + 1 merged frame,
  // i.e. 2 frames instead of 9.
  for (const [name, ctx, deltas, tail] of [
    ["chat", makeCtx({ enabled: true, intervalMs: 50 }, "chat"), WORDS.map((w) => chatDelta(w)), chatTerminator()],
    ["anthropic", makeCtx({ enabled: true, intervalMs: 50 }, "anthropic"), WORDS.map((w) => anthropicDelta(w)), anthropicBlockStop()],
    ["gemini", makeCtx({ enabled: true, intervalMs: 50 }, "gemini"), WORDS.map((w) => geminiDelta(w)), geminiTerminator()],
  ]) {
    onRequest(null, ctx);
    const events = [...deltas, tail];
    const outputs = drive(ctx, events, [...BURST_TIMES, 45]);
    assert.equal(outputs.length, 2, `${name}: 9 deltas collapse into 2 frames, got ${outputs.length}`);
    assert.equal(collectText(outputs, name), INPUT_TEXT, `${name}: coalescing is lossless`);
  }
});

test("coalescing emits one frame per interval window across a long stream", () => {
  const ctx = makeCtx({ enabled: true, intervalMs: 50 }, "anthropic");
  onRequest(null, ctx);
  const texts = Array.from({ length: 20 }, (_, i) => `<${i}>`);
  const deltas = texts.map((t) => anthropicDelta(t));
  const times = deltas.map((_, i) => i * 10); // 200 ms of stream, 50 ms window
  const outputs = drive(ctx, [...deltas, anthropicBlockStop()], [...times, 200]);
  assert.ok(outputs.length < deltas.length, `fewer frames than events, got ${outputs.length}`);
  assert.equal(collectText(outputs, "anthropic"), texts.join(""), "lossless across windows");
});

// ─── 6. disabled ⇒ byte-identical pass-through ──────────────────────────

test("enabled=false: every event is returned by reference", () => {
  const ctx = makeCtx({ enabled: false, intervalMs: 50 }, "anthropic");
  onRequest(null, ctx);
  const events = [...WORDS.map((w) => anthropicDelta(w)), anthropicBlockStop(), anthropicMessageStop()];
  const outputs = drive(ctx, events, [...BURST_TIMES, 45, 45]);
  assert.equal(outputs.length, events.length);
  for (let i = 0; i < events.length; i++) assert.equal(outputs[i].out, events[i]);
});

test("no options at all: every event is returned by reference", () => {
  const ctx = makeCtx(undefined, "chat");
  onRequest(null, ctx);
  const events = [...WORDS.map((w) => chatDelta(w)), chatTerminator()];
  const outputs = drive(ctx, events, [...BURST_TIMES, 45]);
  assert.equal(outputs.length, events.length);
  for (let i = 0; i < events.length; i++) assert.equal(outputs[i].out, events[i]);
});

test("onRequest / onResponse return the body unchanged", () => {
  const ctx = makeCtx({ enabled: true }, "chat");
  const body = { messages: [{ role: "user", content: "hi" }] };
  assert.equal(onRequest(body, ctx), body);
  assert.equal(onResponse(body, ctx), body);
  assert.ok(ctx.state.slowdown);
});

test("onEvent without initialised state passes the event through", () => {
  const ctx = { options: { enabled: true }, protocol: "chat", state: {} };
  const ev = chatDelta("x");
  assert.equal(onEvent(ev, ctx), ev);
});

// ─── 7. maxBufferSize forces an early flush ─────────────────────────────

test("maxBufferSize forces an early flush without losing text", () => {
  const ctx = makeCtx({ enabled: true, intervalMs: 10000, maxBufferSize: 3 }, "anthropic");
  onRequest(null, ctx);
  const texts = ["a", "b", "c", "d", "e", "f", "g", "h"];
  const events = [...texts.map((t) => anthropicDelta(t)), anthropicBlockStop()];
  // Every event lands inside the (huge) interval, so only maxBufferSize can
  // cause an emission.
  const outputs = drive(ctx, events, [...texts.map(() => 1), 1]);
  assert.ok(outputs.length > 1, "the buffer cap must force intermediate flushes");
  assert.equal(collectText(outputs, "anthropic"), texts.join(""), "forced flushes stay lossless");
});

// ─── 8. option parsing ─────────────────────────────────────────────────

test("defaults: enabled=false, intervalMs=50, maxBufferSize=1000", () => {
  const ctx = makeCtx({});
  onRequest(null, ctx);
  assert.deepEqual(ctx.state.slowdown.opts, { enabled: false, intervalMs: 50, maxBufferSize: 1000 });
});

test("legacy keys (merge, flushOnEnd) are ignored, not fatal", () => {
  const ctx = makeCtx({ enabled: true, merge: false, flushOnEnd: false, intervalMs: 50 });
  onRequest(null, ctx);
  assert.deepEqual(ctx.state.slowdown.opts, { enabled: true, intervalMs: 50, maxBufferSize: 1000 });
  const ev = anthropicDelta("a");
  assert.equal(onEvent(ev, ctx), ev);
});

test("garbage option values fall back to defaults without throwing", () => {
  const garbage = [
    { enabled: "yes" },
    { enabled: null },
    { intervalMs: "abc" },
    { intervalMs: -1 },
    { intervalMs: 0 },
    { intervalMs: Infinity },
    { intervalMs: NaN },
    { intervalMs: null },
    { maxBufferSize: {} },
    { maxBufferSize: [] },
    { maxBufferSize: "10" },
    {},
    null,
    undefined,
    "not-an-object",
    42,
    [],
  ];
  for (const options of garbage) {
    const ctx = makeCtx(options, "chat");
    assert.doesNotThrow(() => onRequest(null, ctx), JSON.stringify(options));
    assert.equal(typeof ctx.state.slowdown.opts.enabled, "boolean");
    assert.ok(ctx.state.slowdown.opts.intervalMs >= 1);
    assert.ok(ctx.state.slowdown.opts.maxBufferSize >= 1);
    assert.doesNotThrow(() => onEvent(chatDelta("x"), ctx));
  }
});

test("numeric strings are coerced for intervalMs and maxBufferSize", () => {
  const ctx = makeCtx({ enabled: true, intervalMs: "50", maxBufferSize: "4" }, "anthropic");
  onRequest(null, ctx);
  assert.equal(ctx.state.slowdown.opts.intervalMs, 50);
  assert.equal(ctx.state.slowdown.opts.maxBufferSize, 4);
});

// ─── 9. fail open ───────────────────────────────────────────────────────

test("fail open: an event whose getters throw is returned unchanged", () => {
  const ctx = makeCtx({ enabled: true, intervalMs: 50 }, "chat");
  onRequest(null, ctx);
  const ev = { id: "x", object: "chat.completion.chunk" };
  Object.defineProperty(ev, "choices", {
    get() {
      throw new Error("boom");
    },
    enumerable: true,
  });
  let result;
  assert.doesNotThrow(() => {
    result = onEvent(ev, ctx);
  });
  assert.equal(result, ev, "the original event must be returned, not propagated as a throw");
});

// ─── 10. anthropic flush slot ───────────────────────────────────────────

test("anthropic flush slot: content_block_stop is replaced by the pending delta", () => {
  const ctx = makeCtx({ enabled: true, intervalMs: 50 }, "anthropic");
  onRequest(null, ctx);
  const events = [
    anthropicDelta("a"),
    anthropicDelta("b"),
    anthropicDelta("c"),
    anthropicBlockStop(0),
    anthropicMessageDelta(),
    anthropicMessageStop(),
  ];
  const outputs = drive(ctx, events, [0, 5, 10, 10, 10, 10]);

  // The marker is replaced, not emitted.
  assert.equal(outputs.some((o) => o.out.event === "content_block_stop"), false);

  // Its place in the stream is taken by a content_block_delta carrying the tail.
  const replacement = outputs[1].out;
  assert.equal(replacement.event, "content_block_delta");
  assert.equal(replacement.type, "content_block_delta");
  assert.equal(replacement.index, 0);
  assert.deepEqual(replacement.delta, { type: "text_delta", text: "bc" });

  // message_delta and message_stop always pass through untouched.
  const md = outputs.find((o) => o.out.event === "message_delta");
  assert.equal(md.out, events[4]);
  const ms = outputs.find((o) => o.out.event === "message_stop");
  assert.equal(ms.out, events[5]);

  assert.equal(collectText(outputs, "anthropic"), "abc");
});

test("anthropic flush slot with an empty buffer passes the marker through", () => {
  const ctx = makeCtx({ enabled: true, intervalMs: 50 }, "anthropic");
  onRequest(null, ctx);
  const marker = anthropicBlockStop(0);
  const outputs = drive(ctx, [marker], [0]);
  assert.equal(outputs.length, 1);
  assert.equal(outputs[0].out, marker);
});

test("anthropic flush slot for another content block leaves the marker intact", () => {
  const ctx = makeCtx({ enabled: true, intervalMs: 50 }, "anthropic");
  onRequest(null, ctx);
  // Text is buffered for block 0; the marker belongs to block 1 (a tool_use
  // block), so it must survive and the tail must wait for the terminator.
  const events = [anthropicDelta("tail", 0), anthropicBlockStop(1), anthropicMessageDelta()];
  const outputs = drive(ctx, events, [0, 5, 5]);
  const marker = outputs.find((o) => o.out.event === "content_block_stop");
  assert.equal(marker.out, events[1]);
  assert.equal(collectText(outputs, "anthropic"), "tail");
});

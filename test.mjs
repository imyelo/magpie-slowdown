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

// Anthropic is the one shape the middleware must not touch. `content_block_delta`
// is its only text-carrying event, and the SSE `event:` name is upstream's and
// cannot be rewritten from `onEvent`, so a tail held past the last delta has no
// frame to ride. The only lossless behaviour is a byte-identical pass-through:
// every event comes back as the same object, so magpie writes the original
// `event:` line and body.
test("losslessness: anthropic — every frame passes through by reference", () => {
  const ctx = makeCtx({ enabled: true, intervalMs: 50 }, "anthropic");
  onRequest(null, ctx);
  const events = [...WORDS.map((w) => anthropicDelta(w)), anthropicBlockStop(), anthropicMessageDelta(), anthropicMessageStop()];
  const outputs = drive(ctx, events, [...BURST_TIMES, 45, 45, 45]);
  assert.equal(outputs.length, events.length);
  for (let i = 0; i < events.length; i++) assert.equal(outputs[i].out, events[i]);
  assert.equal(collectText(outputs, "anthropic"), INPUT_TEXT, "pass-through is lossless");
  assert.equal(ctx.state.slowdown.pending.length, 0, "nothing may be withheld on the anthropic wire");
});

// `ctx.protocol` names the CLIENT's protocol; when magpie relays an upstream's
// bytes verbatim the two can disagree (a chat client asking a provider whose
// model answers in Anthropic shape). Dispatch must therefore follow the event's
// own shape, not `ctx.protocol`.
test("anthropic shape is detected from the event, not from ctx.protocol", () => {
  const ctx = makeCtx({ enabled: true, intervalMs: 50 }, "chat"); // client spoke chat; the bytes are anthropic
  onRequest(null, ctx);
  const events = [...WORDS.map((w) => anthropicDelta(w)), anthropicMessageStop()];
  const outputs = drive(ctx, events, [...BURST_TIMES, 45]);
  assert.equal(outputs.length, events.length);
  for (let i = 0; i < events.length; i++) assert.equal(outputs[i].out, events[i]);
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

// The done slot carries the item's whole text, so it is where the window ends:
// releasing the buffered deltas there leaves the later `response.completed`
// with nothing to orphan (and nothing to log).
test("responses: the done slot empties the window; nothing is orphaned later", () => {
  const ctx = makeCtx({ enabled: true, intervalMs: 50 }, "responses");
  onRequest(null, ctx);
  const events = [...WORDS.map((w) => responsesDeltaString(w)), responsesTextDone(""), responsesCompleted()];
  drive(ctx, events, [...BURST_TIMES, 45, 45]);
  assert.equal(ctx.state.slowdown.pending.length, 0, "the done slot must release the window");
  assert.equal(ctx.state.slowdown.orphanText, "", "no text may be reported orphaned after a done slot");
});

test("losslessness: gemini — emitted text equals input text, in order", () => {
  const ctx = makeCtx({ enabled: true, intervalMs: 50 }, "gemini");
  onRequest(null, ctx);
  const events = [...WORDS.map((w) => geminiDelta(w)), geminiTerminator()];
  const outputs = drive(ctx, events, [...BURST_TIMES, 45]);
  assert.equal(collectText(outputs, "gemini"), INPUT_TEXT);
});

test("losslessness: an unrecognised event shape passes every event through unchanged", () => {
  // Note the protocol is `chat`: the pass-through is driven by the SHAPE, so an
  // unknown body is left alone even when `ctx.protocol` names a shape we know.
  const ctx = makeCtx({ enabled: true, intervalMs: 50 }, "chat");
  onRequest(null, ctx);
  const events = [
    { some: "opaque" },
    { kind: "other", payload: { text: "x" } },
    { type: "vendor.mystery", text: "y" },
    { object: "page", data: [{ text: "z" }] },
  ];
  const outputs = drive(ctx, events, [0, 5, 10, 15]);
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

test("terminator survives: anthropic message_delta + message_stop, buffer untouched", () => {
  const ctx = makeCtx({ enabled: true, intervalMs: 50 }, "anthropic");
  onRequest(null, ctx);
  const events = [anthropicDelta("a"), anthropicDelta("b"), anthropicMessageDelta(), anthropicMessageStop()];
  const outputs = drive(ctx, events, [0, 5, 10, 10]);
  // Anthropic is a pass-through shape: the terminators are the upstream's own
  // events, returned by reference (see section 10).
  assert.ok(outputs.some((o) => o.out === events[2]), "message_delta passes through by reference");
  assert.ok(outputs.some((o) => o.out === events[3]), "message_stop passes through by reference");
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
  // Only the coalescing shapes: anthropic is a pass-through and has no window.
  for (const protocol of ["chat", "responses", "gemini"]) {
    const ctx = makeCtx({ enabled: true, intervalMs: 50 }, protocol);
    onRequest(null, ctx);
    const make = {
      chat: () => chatDelta("first"),
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
  const ctx = makeCtx({ enabled: true, intervalMs: 50 }, "chat");
  onRequest(null, ctx);
  const texts = Array.from({ length: 20 }, (_, i) => `<${i}>`);
  const deltas = texts.map((t) => chatDelta(t));
  const times = deltas.map((_, i) => i * 10); // 200 ms of stream, 50 ms window
  const outputs = drive(ctx, [...deltas, chatTerminator()], [...times, 200]);
  assert.ok(outputs.length < deltas.length, `fewer frames than events, got ${outputs.length}`);
  assert.equal(collectText(outputs, "chat"), texts.join(""), "lossless across windows");
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
  const ctx = makeCtx({ enabled: true, intervalMs: 10000, maxBufferSize: 3 }, "chat");
  onRequest(null, ctx);
  const texts = ["a", "b", "c", "d", "e", "f", "g", "h"];
  const events = [...texts.map((t) => chatDelta(t)), chatTerminator()];
  // Every event lands inside the (huge) interval, so only maxBufferSize can
  // cause an emission.
  const outputs = drive(ctx, events, [...texts.map(() => 1), 1]);
  assert.ok(outputs.length > 1, "the buffer cap must force intermediate flushes");
  assert.equal(collectText(outputs, "chat"), texts.join(""), "forced flushes stay lossless");
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

// ─── 10. anthropic is never coalesced ───────────────────────────────────
//
// Why there is no anthropic flush slot. `content_block_delta` is the only
// text-carrying Anthropic event; `content_block_stop` is a marker whose own
// payload carries no text, and the SSE `event:` name is upstream's and cannot
// be rewritten from `onEvent` (magpie's `writer.event` splits the frame and
// re-writes only the `data:` JSON — see README). So a tail withheld until
// `content_block_stop` can only be delivered on a frame the Anthropic SDK
// routes to its (textless) `content_block_stop` handler and discards. The
// measured result was a truncated reply. Passing every frame through is the
// only lossless option.
test("anthropic: the content_block_stop marker is not replaced or dropped", () => {
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
  assert.equal(outputs.length, events.length);
  for (let i = 0; i < events.length; i++) assert.equal(outputs[i].out, events[i], `event ${i} must pass through by reference`);
  assert.equal(collectText(outputs, "anthropic"), "abc");
});

test("anthropic: a stop marker alone is passed through", () => {
  const ctx = makeCtx({ enabled: true, intervalMs: 50 }, "anthropic");
  onRequest(null, ctx);
  const marker = anthropicBlockStop(0);
  const outputs = drive(ctx, [marker], [0]);
  assert.equal(outputs.length, 1);
  assert.equal(outputs[0].out, marker);
});

test("anthropic: text and a tool_use marker in the same stream are both untouched", () => {
  const ctx = makeCtx({ enabled: true, intervalMs: 50 }, "anthropic");
  onRequest(null, ctx);
  const events = [
    anthropicDelta("a", 0),
    { type: "content_block_start", index: 1, content_block: { type: "tool_use", id: "toolu_1", name: "read", input: {} } },
    { type: "content_block_delta", index: 1, delta: { type: "input_json_delta", partial_json: '{"path":' } },
    anthropicDelta("b", 0),
    anthropicBlockStop(1),
    anthropicMessageDelta(),
  ];
  const outputs = drive(ctx, events, [0, 5, 5, 10, 10, 10]);
  assert.equal(outputs.length, events.length);
  for (let i = 0; i < events.length; i++) assert.equal(outputs[i].out, events[i]);
});

// ─── 11. tool-call deltas are untouched ──────────────────────────────────
//
// Pure tool_call frames — a delta with `tool_calls` and no text — carry no
// text to coalesce, so they pass through by reference, byte for byte. Frames
// that mix text AND tool_calls (MiniMax-M3 emits these at thinking→tool
// boundaries) are coalesced; section 12 pins the invariant that their
// tool_calls survive the merge with identity and order intact.
test("chat tool_call delta: passed through by reference, untouched", () => {
  const ctx = makeCtx({ enabled: true, intervalMs: 50 }, "chat");
  onRequest(null, ctx);
  // The exact shape OpenCode's chat client rejects: a tool_calls delta with
  // no `id` and no `name`. The middleware must not rewrite it.
  const malformed = {
    id: "chatcmpl-1",
    object: "chat.completion.chunk",
    created: 1,
    model: "test-model",
    choices: [
      {
        index: 0,
        delta: {
          role: "assistant",
          content: null,
          tool_calls: [{ index: 0, function: { arguments: "{}" } }],
        },
      },
    ],
  };
  const out = onEvent(malformed, ctx);
  assert.equal(out, malformed, "tool_call delta must be the original object reference");
  assert.equal(out.choices[0].delta.tool_calls[0].function.arguments, "{}");
});

test("chat tool_call delta interleaved with text: text frames merge, tool_calls pass through", () => {
  const ctx = makeCtx({ enabled: true, intervalMs: 50 }, "chat");
  onRequest(null, ctx);
  const toolA = {
    id: "chatcmpl-1",
    object: "chat.completion.chunk",
    created: 1,
    model: "test-model",
    choices: [{ index: 0, delta: { tool_calls: [{ index: 0, id: "call_1", type: "function", function: { name: "f", arguments: "{" } }] } }],
  };
  const toolB = {
    id: "chatcmpl-1",
    object: "chat.completion.chunk",
    created: 1,
    model: "test-model",
    choices: [{ index: 0, delta: { tool_calls: [{ index: 0, function: { arguments: "}" } }] } }],
  };
  const outputs = drive(ctx, [chatDelta("hi"), toolA, chatDelta("!"), toolB, chatTerminator()], [0, 5, 10, 15, 20]);

  // Both tool_call deltas must survive untouched, by reference.
  const aOut = outputs.find((o) => o.out.choices?.[0]?.delta?.tool_calls?.[0]?.id === "call_1");
  assert.equal(aOut.out, toolA);
  assert.equal(aOut.index, 1);
  const bOut = outputs.find((o) => o.out.choices?.[0]?.delta?.tool_calls?.[0]?.function?.arguments === "}");
  assert.equal(bOut.out, toolB);
  assert.equal(bOut.index, 3);

  // All text must still reach the client.
  assert.equal(collectText(outputs, "chat"), "hi!");
});

// ─── 12. mixed content+tool_calls frames ─────────────────────────────────
//
// Real providers (MiniMax-M3 observed 2026-10-15) put the first tool_call
// delta — with `id` and `name` — inside a delta that ALSO carries thinking
// text. Such a frame is text-carrying, so it enters the coalescing window;
// merging must preserve its tool_calls, and the tool_call frames behind it
// must not overtake it: arguments fragments are order-sensitive JSON.
test("chat mixed content+tool_calls frame: buffered identity survives in order with its continuation args", () => {
  const ctx = makeCtx({ enabled: true, intervalMs: 50 }, "chat");
  onRequest(null, ctx);
  const mixed = {
    id: "chatcmpl-1",
    object: "chat.completion.chunk",
    created: 1,
    model: "test-model",
    choices: [
      {
        index: 0,
        delta: {
          role: "assistant",
          content: "先搜索",
          tool_calls: [{ index: 0, id: "call_1", type: "function", function: { name: "f", arguments: "{" } }],
        },
      },
    ],
  };
  const cont1 = {
    id: "chatcmpl-1",
    object: "chat.completion.chunk",
    created: 1,
    model: "test-model",
    choices: [{ index: 0, delta: { role: "assistant", tool_calls: [{ index: 0, function: { arguments: "\"q\":1" } }] } }],
  };
  const cont2 = {
    id: "chatcmpl-1",
    object: "chat.completion.chunk",
    created: 1,
    model: "test-model",
    choices: [{ index: 0, delta: { role: "assistant", tool_calls: [{ index: 0, function: { arguments: "}" } }] } }],
  };
  const outputs = drive(
    ctx,
    [chatDelta("我来帮您"), chatDelta("先搜索文档"), mixed, cont1, cont2, chatTerminator("tool_calls")],
    [0, 5, 10, 15, 20, 25],
  );

  // Text survives unchanged — including the mixed frame's own content.
  assert.equal(collectText(outputs, "chat"), "我来帮您先搜索文档先搜索");

  // The identity frame's tool_calls must reach the client with the
  // continuation fragments concatenated AFTER its own, in arrival order.
  const carrier = outputs.find((o) => o.out.choices?.[0]?.delta?.tool_calls?.length);
  assert.ok(carrier, "tool_calls must reach the client");
  const tcs = carrier.out.choices[0].delta.tool_calls;
  assert.equal(tcs.length, 1);
  assert.equal(tcs[0].id, "call_1");
  assert.equal(tcs[0].function.name, "f");
  assert.equal(tcs[0].function.arguments, "{\"q\":1}");
});

test("chat parallel tool calls crossing the window boundary: both indices aggregate, order kept", () => {
  const ctx = makeCtx({ enabled: true, intervalMs: 50 }, "chat");
  onRequest(null, ctx);
  const mixedA = {
    id: "chatcmpl-1",
    object: "chat.completion.chunk",
    created: 1,
    model: "test-model",
    choices: [
      {
        index: 0,
        delta: {
          role: "assistant",
          content: "先并行",
          tool_calls: [{ index: 0, id: "call_A", type: "function", function: { name: "search", arguments: "{\"x\":1" } }],
        },
      },
    ],
  };
  const contA = {
    id: "chatcmpl-1",
    object: "chat.completion.chunk",
    created: 1,
    model: "test-model",
    choices: [{ index: 0, delta: { role: "assistant", tool_calls: [{ index: 0, function: { arguments: "}" } }] } }],
  };
  const mixedB = {
    id: "chatcmpl-1",
    object: "chat.completion.chunk",
    created: 1,
    model: "test-model",
    choices: [
      {
        index: 0,
        delta: {
          role: "assistant",
          content: "再查",
          tool_calls: [{ index: 1, id: "call_B", type: "function", function: { name: "fetch", arguments: "{\"y\":2" } }],
        },
      },
    ],
  };
  const contB = {
    id: "chatcmpl-1",
    object: "chat.completion.chunk",
    created: 1,
    model: "test-model",
    choices: [{ index: 0, delta: { role: "assistant", tool_calls: [{ index: 1, function: { arguments: "}" } }] } }],
  };
  // t=100 crosses the 50 ms window started by the first text frame at t=0.
  const outputs = drive(ctx, [chatDelta("开始"), mixedA, contA, mixedB, contB, chatTerminator("tool_calls")], [0, 5, 10, 100, 105, 110]);

  assert.equal(collectText(outputs, "chat"), "开始先并行再查");

  // Both calls must reach the client, each with identity and complete args.
  // Accumulate the way a client does: identity from the first frame that has
  // it, arguments concatenated in arrival order.
  const seen = new Map();
  for (const { out } of outputs) {
    for (const tc of out.choices?.[0]?.delta?.tool_calls ?? []) {
      const prev = seen.get(tc.index);
      if (!prev) {
        seen.set(tc.index, { id: tc.id, name: tc.function?.name, arguments: tc.function?.arguments ?? "" });
      } else {
        seen.set(tc.index, {
          id: prev.id ?? tc.id,
          name: prev.name ?? tc.function?.name,
          arguments: (prev.arguments ?? "") + (tc.function?.arguments ?? ""),
        });
      }
    }
  }
  assert.deepEqual(
    [...seen.entries()].map(([index, tc]) => ({ index, id: tc.id, name: tc.name, arguments: tc.arguments })),
    [
      { index: 0, id: "call_A", name: "search", arguments: "{\"x\":1}" },
      { index: 1, id: "call_B", name: "fetch", arguments: "{\"y\":2}" },
    ],
  );
});

test("chat split identity without index: id and name on separate frames still merge into one call", () => {
  const ctx = makeCtx({ enabled: true, intervalMs: 50 }, "chat");
  onRequest(null, ctx);
  // A provider that omits `index` and splits identity: frame 1 carries text
  // plus the call id, frame 2 the name, frame 3+ the arguments.
  const withId = {
    id: "chatcmpl-1",
    object: "chat.completion.chunk",
    created: 1,
    model: "test-model",
    choices: [
      {
        index: 0,
        delta: {
          role: "assistant",
          content: "调用",
          tool_calls: [{ id: "call_S", type: "function", function: { arguments: "" } }],
        },
      },
    ],
  };
  const withName = {
    id: "chatcmpl-1",
    object: "chat.completion.chunk",
    created: 1,
    model: "test-model",
    choices: [{ index: 0, delta: { tool_calls: [{ function: { name: "f", arguments: "{\"a\":" } }] } }],
  };
  const withArgs = {
    id: "chatcmpl-1",
    object: "chat.completion.chunk",
    created: 1,
    model: "test-model",
    choices: [{ index: 0, delta: { tool_calls: [{ function: { arguments: "1}" } }] } }],
  };
  const outputs = drive(ctx, [chatDelta("hi"), withId, withName, withArgs, chatTerminator("tool_calls")], [0, 5, 10, 15, 20]);

  assert.equal(collectText(outputs, "chat"), "hi调用");

  const carrier = outputs.find((o) => o.out.choices?.[0]?.delta?.tool_calls?.length);
  assert.ok(carrier, "tool_calls must reach the client");
  const tcs = carrier.out.choices[0].delta.tool_calls;
  assert.equal(tcs.length, 1, "three partial frames must aggregate into ONE call");
  assert.equal(tcs[0].id, "call_S");
  assert.equal(tcs[0].function.name, "f");
  assert.equal(tcs[0].function.arguments, "{\"a\":1}");
});

test("gemini functionCall part survives merge when buffered behind text", () => {
  const ctx = makeCtx({ enabled: true, intervalMs: 50 }, "gemini");
  onRequest(null, ctx);
  const mixed = {
    candidates: [
      {
        index: 0,
        content: {
          role: "model",
          parts: [{ text: "查一下" }, { functionCall: { name: "search", args: { q: "ai" } } }],
        },
      },
    ],
  };
  const outputs = drive(ctx, [geminiDelta("我来帮您"), geminiDelta("正在搜索"), mixed, geminiTerminator()], [0, 5, 10, 15]);

  assert.equal(collectText(outputs, "gemini"), "我来帮您正在搜索查一下");

  const call = outputs
    .flatMap((o) => o.out.candidates?.[0]?.content?.parts ?? [])
    .find((p) => p?.functionCall);
  assert.ok(call, "functionCall part must reach the client");
  assert.deepEqual(call.functionCall, { name: "search", args: { q: "ai" } });
});

test("responses: function_call_arguments deltas are not treated as text", () => {
  const ctx = makeCtx({ enabled: true, intervalMs: 50 }, "responses");
  onRequest(null, ctx);
  const argsDelta = { type: "response.function_call_arguments.delta", item_id: "fc_1", output_index: 1, delta: "{\"q\":" };
  const outputs = drive(
    ctx,
    [responsesDeltaString("a"), argsDelta, responsesDeltaString("b"), responsesTextDone(""), responsesCompleted()],
    [0, 5, 10, 45, 45],
  );

  const passed = outputs.find((o) => o.out === argsDelta);
  assert.ok(passed, "function_call_arguments delta must pass through, never buffered");

  const done = outputs.find((o) => o.out.type === "response.output_text.done");
  assert.ok(done, "output_text.done slot must still be emitted");
  assert.equal(done.out.text, "ab");
});

test("responses: parallel item text is never merged across item_id", () => {
  const ctx = makeCtx({ enabled: true, intervalMs: 50 }, "responses");
  onRequest(null, ctx);
  const d1 = { type: "response.output_text.delta", item_id: "msg_1", output_index: 0, delta: "first" };
  const d2 = { type: "response.output_text.delta", item_id: "msg_2", output_index: 1, delta: "second" };
  const done1 = { type: "response.output_text.done", item_id: "msg_1", output_index: 0, content_index: 0, text: "first" };
  const done2 = { type: "response.output_text.done", item_id: "msg_2", output_index: 1, content_index: 1, text: "second" };
  const outputs = drive(ctx, [d1, d2, done1, done2, responsesCompleted()], [0, 5, 10, 15, 20]);

  const doneTexts = outputs.filter((o) => o.out.type === "response.output_text.done").map((o) => o.out.text);
  assert.deepEqual(doneTexts, ["first", "second"]);

  // msg_2's buffered delta is legitimately absorbed into its own done slot —
  // the slot is the authoritative full-text carrier for responses. What must
  // never happen is cross-item attribution (asserted via doneTexts above).
  assert.ok(outputs.some((o) => o.out.type === "response.completed"), "response.completed must pass through");
});

test("chat tool_call run honors maxBufferSize: forced flush emits one ordered merged frame", () => {
  const ctx = makeCtx({ enabled: true, intervalMs: 50, maxBufferSize: 2 }, "chat");
  onRequest(null, ctx);
  const mixed = {
    id: "chatcmpl-1",
    object: "chat.completion.chunk",
    created: 1,
    model: "test-model",
    choices: [
      {
        index: 0,
        delta: {
          role: "assistant",
          content: "查",
          tool_calls: [{ index: 0, id: "call_1", type: "function", function: { name: "f", arguments: "{" } }],
        },
      },
    ],
  };
  const cont = (args) => ({
    id: "chatcmpl-1",
    object: "chat.completion.chunk",
    created: 1,
    model: "test-model",
    choices: [{ index: 0, delta: { role: "assistant", tool_calls: [{ index: 0, function: { arguments: args } }] } }],
  });

  let maxPending = 0;
  const outputs = [];
  const events = [mixed, cont("\"a\":1"), cont("}")];
  const realNow = Date.now;
  try {
    events.forEach((ev, i) => {
      Date.now = () => i * 5;
      const out = onEvent(ev, ctx);
      maxPending = Math.max(maxPending, ctx.state.slowdown.pending.length);
      if (out !== null) outputs.push(out);
    });
    Date.now = () => 20;
    const term = onEvent(chatTerminator("tool_calls"), ctx);
    if (term !== null) outputs.push(term);
  } finally {
    Date.now = realNow;
  }

  assert.ok(maxPending <= 2, `pending must never exceed maxBufferSize, saw ${maxPending}`);
  // The forced flush carries the call complete and in order; nothing is lost.
  const tcs = outputs.flatMap((o) => o.choices?.[0]?.delta?.tool_calls ?? []);
  const args = tcs.map((tc) => tc.function?.arguments ?? "").join("");
  assert.equal(args, "{\"a\":1}");
  assert.equal(tcs.find((tc) => tc.id)?.id, "call_1");
  assert.ok(outputs.some((o) => o.choices?.[0]?.finish_reason === "tool_calls"), "terminator survives");
});

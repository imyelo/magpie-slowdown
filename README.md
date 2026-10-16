# @yelo/magpie-slowdown

Magpie gateway middleware that **coalesces** a streamed response into fewer,
larger frames — without losing a single character of text.

## What it can and cannot do

The name is historical. A magpie middleware **cannot slow a stream down in
wall-clock time**, and this is a property of the host, not a missing feature:

| Constraint | Consequence |
| ---------- | ----------- |
| `onEvent` may only return one event, an object, `null` (drop) or `undefined` (unchanged) | one upstream frame can never become several downstream frames |
| An **array** return is serialized as a single frame's JSON (`data: [{…},{…}]`) | it does not expand, and it breaks clients |
| moejs has no `setTimeout` / `setInterval`, and `ctx` exposes no write, flush or delay handle | there is no way to hold a frame back and release it later |
| `onEvent` has a ~50 ms budget; overrunning it makes magpie send the event **unchanged** | sleeping inside the hook does not slow anything, it just wastes the frame |
| The response ends when the upstream ends | total duration is fixed by the provider |

So the only honest thing a middleware can do is **reduce the number of frames
while preserving every character**. That is what this one does. If you are
looking for real output throttling, it has to live upstream of the gateway, in
the provider or the agent client.

## What it does

Events carrying text that arrive within `intervalMs` of each other are held
back and merged, then released as a single frame at the next window boundary.
The wire sees fewer frames with the same text.

- The first text frame of a response is emitted immediately — no added
  first-token latency.
- Frames that carry no text (`role`, usage, keep-alives) and no tool calls
  pass straight through. Pure tool_call deltas pass by reference, untouched.
- A frame that mixes text **and** tool calls (MiniMax-M3 emits these at
  thinking→tool boundaries) is coalesced like any text frame, but its
  `tool_calls` survive: merged frames aggregate them per `index` (first-seen
  identity wins, `arguments` concatenated in arrival order), and a withheld
  tool_call frame holds back later fragments so arguments JSON is never
  reordered. Gemini non-text parts (`functionCall`, …) and Responses non-text
  delta parts are preserved the same way.
- Responses text is tracked per `item_id`: a merged `output_text.done` slot
  only ever carries the text of its own item, and
  `function_call_arguments.delta` events are never treated as text.
- The stream terminator is **never** dropped, and anything still pending when
  it arrives — text, tool_calls, parts — is delivered with it, so the tail of
  a response cannot vanish.
- If anything in the hook throws, the original event is returned unchanged. A
  bug in here must never eat a reply.
- `enabled: false` (the default) returns every event untouched — the same
  object reference, so magpie's own serialization is unaffected.

## Install

```sh
magpie plugin add /path/to/this/repo
magpie plugin options /path/to/this/repo '{"enabled":true,"intervalMs":50}'
```

`magpie plugin` takes effect within about a second; magpie does not need a
restart. **Always finish with `magpie plugin off /path/to/this/repo`** — a
middleware left on applies to every request through the gateway, including your
own agent turn.

## Options

| Field           | Type | Default | Description |
| --------------- | ---- | ------- | ----------- |
| `enabled`       | bool | `false` | Master switch. When `false` every event passes through unchanged. |
| `intervalMs`    | int  | `50`    | Merge window in milliseconds. At most one merged frame is emitted per window. |
| `maxBufferSize` | int  | `1000`  | Cap on frames held in the pending window. Hitting it flushes early. Text is still preserved. |

Legacy `merge` and `flushOnEnd` keys may still be sitting in your stored
options JSON; they are ignored. `flushOnEnd:false` was the tail-loss bug and
no longer exists as a behaviour.

## Supported event shapes

| Protocol | `ctx.protocol` | Text field | Terminator | Where a pending tail is flushed |
| -------- | -------------- | ---------- | ---------- | ------------------------------ |
| OpenAI Chat Completions | `chat` | `choices[0].delta.content` | `choices[].finish_reason != null` | into the terminator chunk (a final chunk may legally carry both content and `finish_reason`) |
| Anthropic Messages | `anthropic` | `delta.text` | `message_delta` with `stop_reason`, or `message_stop` | at `content_block_stop`, whose marker is replaced by the pending text; `message_delta` and `message_stop` always survive |
| OpenAI Responses | `responses` | `delta` (string, or parts array) | `response.completed` / `response.incomplete` | into `response.output_text.done`, whose `text` is meant to hold the full text so far |
| Gemini | `gemini` | `candidates[0].content.parts[0].text` | `candidates[].finishReason != null` | into the terminator candidate |

An unrecognised protocol is passed through unchanged.

`data: [DONE]` is never seen by `onEvent` — magpie forwards that frame raw.

## Development

```sh
npm test                                   # unit tests
node .scratch/mock-provider/check-stream.mjs --model mock-stream/mock-fast --expect 'AxxxxxxZ'
```

The checker drives a real request through a running magpie gateway and asserts
that the text arrived complete and the terminator survived — the only check
that catches a middleware eating a response, which unit tests cannot. See
`.scratch/mock-provider/README.md` to start the mock upstream.

## License

Apache-2.0

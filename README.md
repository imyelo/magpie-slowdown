# @yelo/magpie-slowdown

Magpie middleware that slows down streamed token responses.

## What it does

This middleware sits in `magpie`'s gateway between the provider and the agent. By default it is a pass-through. When enabled, it limits the output frequency:

- **`merge: false`** — at most one event per `intervalMs`; excess events are buffered (delay, not drop) and released at the next available slot.
- **`merge: true`** — events arriving within `intervalMs` are coalesced into a single event emitted at each window boundary.

It is useful when debugging agent tools: bursts of small events can hide or amplify problems, and shaping the stream into a steadier pace makes the behavior easier to observe.

## Install

```
magpie plugin add @yelo/magpie-slowdown
```

To turn it on (opt-in by default), set `enabled`:

```
magpie plugin options slowdown '{
  "enabled": true,
  "merge": false,
  "intervalMs": 50
}'
```

## Options

| Field           | Type | Default | Description                                                                                |
| --------------- | ---- | ------- | ------------------------------------------------------------------------------------------ |
| `enabled`       | bool | `false` | Master switch. When `false`, the middleware passes events through unchanged.              |
| `merge`         | bool | `false` | When `true`, output is rate-limited by **windowed merging**: events within each `intervalMs` window are coalesced into one event emitted at the window boundary. When `false`, output is rate-limited by **slot scheduling**: one event per `intervalMs`, excess buffered and delayed. Only takes effect when `enabled` is `true`. |
| `intervalMs`    | int  | `50`    | Output cadence in milliseconds. In rate-limit mode this gives 1000 / `intervalMs` events per second. In coalesce mode this is the merge window length. |
| `maxBufferSize` | int  | `1000`  | Maximum events buffered. If the buffer would overflow, the oldest queued event is force-flushed (emitted early) to make room — preventing data loss at the cost of a short-term rate overshoot. |
| `flushOnEnd`    | bool | `false` | `false` = wait for the natural window or slot boundary at the end of a stream.             |

## Behavior

| `enabled` | `merge` | Effect                                                      |
| --------- | ------- | ----------------------------------------------------------- |
| `false`   | any     | Pass-through (default).                                     |
| `true`    | `false` | Limit output frequency via slot scheduling.                 |
| `true`    | `true`  | Limit output frequency via windowed merging of events.      |

Both active modes bound the buffer with `maxBufferSize` and delay (never drop) excess events.

## Behavior notes

- The middleware operates on **events**, not tokens. A single event may carry a partial word; in coalesce mode the merged event concatenates the text deltas from each buffered event.
- At the end of a stream, buffered events wait for the natural window or slot boundary (`flushOnEnd: false` by default).

## Supported event shapes

- **Anthropic Messages** (`content_block_delta`, `delta.text`)
- **OpenAI Chat Completions** (`choices[0].delta.content`)
- **OpenAI Responses** (`delta` as string, or `delta[0].text` as parts array)
- **Gemini** (`candidates[0].content.parts[0].text`)

## Development

```
npm test
```

Runs the Node test suite in `test.mjs`.

## License

Apache-2.0

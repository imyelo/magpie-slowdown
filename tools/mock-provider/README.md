# mock-provider

A local upstream that speaks all four streaming protocols magpie serves, plus the
checker used to verify gateway middleware end to end. Nothing here talks to a
paid provider.

## Run it

```sh
node tools/mock-provider/mock-provider.mjs
# mock-provider on http://127.0.0.1:9876
#   chat       mock-fast, mock-idle      POST /v1/chat/completions
#   anthropic  mock-anthropic            POST /v1/messages
#   responses  mock-responses            POST /v1/responses
#   gemini     mock-gemini               POST /v1beta/models/mock-gemini:streamGenerateContent
#   8 text chunks, 5ms apart, text="x" head=A tail=Z → "AxxxxxxZ"
```

Env knobs: `MOCK_PORT` `MOCK_TOTAL` `MOCK_SPACING` `MOCK_TEXT` `MOCK_FIRST`
`MOCK_LAST` `MOCK_MODEL` `MOCK_MODEL2` `MOCK_MODEL3` `MOCK_MODEL4` `MOCK_MODEL5`
`MOCK_QUIET`.

Every response is `A` + `x`×(`MOCK_TOTAL`−2) + `Z`, so a truncated answer is
visible in one glance.

## The four shapes

| model id | protocol | path asked | frames |
| --- | --- | --- | --- |
| `mock-fast` | OpenAI Chat Completions | `/v1/chat/completions` | role chunk, N × `delta.content`, `finish_reason`, `[DONE]` |
| `mock-idle` | OpenAI Chat Completions | `/v1/chat/completions` | a second chat model; same frames |
| `mock-anthropic` | Anthropic Messages | `/v1/messages` | `message_start`, `content_block_start`, N × `content_block_delta`, `content_block_stop`, `message_delta`, `message_stop` (`event: <type>` + `data: <json>`) |
| `mock-responses` | OpenAI Responses | `/v1/responses` | `response.created`, N × `response.output_text.delta`, `response.output_text.done`, `response.completed` — no `[DONE]`, OpenAI sends none |
| `mock-gemini` | Gemini generateContent | `/v1beta/models/mock-gemini:streamGenerateContent` | N × `candidates[0].content.parts[0].text`, the last carrying `finishReason: "STOP"` |

The path decides the shape, so any path ending in `/chat/completions`,
`/messages` or `/responses` and any `:streamGenerateContent` / `:generateContent`
path is served, with or without the `/v1` or `/v1beta` prefix. One exception:
on `/v1/chat/completions` a protocol model id (`mock-anthropic`,
`mock-responses`, `mock-gemini`) decides instead, which is how you can ask any of
the four shapes from one path. A request with `stream: false` — or, for Gemini,
a `:generateContent` path — gets the same payload as one JSON object.

`GET /v1/models` and `GET /v1beta/models` list all five ids in the two shapes
magpie asks for.

## Point magpie at it

magpie translates whatever the caller speaks into whatever the *provider* speaks
(`magpie provider --help`: the fields are `url`, `anthropic`, `responses`,
`gemini`, `decide`, `key`, `models`, `models.url`, `catalog`, `icon`,
`header.*`). So a provider with only a `chat` base makes magpie answer an
Anthropic or Gemini client out of the mock's **chat** stream — fine for testing
magpie's translation, useless for testing a middleware that is meant to see the
real protocol. Give the mock provider all four bases:

```sh
magpie provider add mock-stream \
  url=http://127.0.0.1:9876 \
  anthropic=http://127.0.0.1:9876 \
  responses=http://127.0.0.1:9876 \
  gemini=http://127.0.0.1:9876 \
  key=mock icon=openai
magpie provider models mock-stream mock-fast mock-idle mock-anthropic mock-responses mock-gemini
```

or hand-write it in `~/.config/magpie/providers.json`:

```json
{ "id": "mock-stream", "name": "Mock Stream", "icon": "openai", "key": "mock",
  "chat": "http://127.0.0.1:9876/v1",
  "anthropic": "http://127.0.0.1:9876",
  "responses": "http://127.0.0.1:9876/v1",
  "gemini": "http://127.0.0.1:9876" }
```

`models.url` (`http://127.0.0.1:9876/v1/models`) lets magpie fetch the list
itself instead of being told it.

With that, each client protocol reaches the mock in its own shape:

- OpenAI Chat client → mock `/v1/chat/completions`
- Anthropic client (Claude Code, the Anthropic SDK) → mock `/v1/messages`
- Responses client (Codex, the Responses SDK) → mock `/v1/responses`
- Gemini client (Gemini CLI, the google-genai SDK) → mock
  `/v1beta/models/mock-gemini:streamGenerateContent?alt=sse`

## Check a stream

```sh
node tools/mock-provider/check-stream.mjs \
  --protocol chat --model mock-stream/mock-fast --expect 'AxxxxxxZ'
```

```
protocol   chat
target     gateway http://127.0.0.1:3425/v1/chat/completions
model      mock-stream/mock-fast
frames     11
span       72ms
text       "AxxxxxxZ" (8 chars)
head/tail  "A" / "Z"
terminator finish_reason=true [DONE]=true
values     finish_reason=stop
arrival    24,27,33,38,43,49,55,60,66,72,72ms  gaps=[3,6,5,5,6,6,5,6,6,0]
   0     24ms role=assistant
   1     27ms "A"
   …
   9     72ms {}
  10     72ms [DONE]
OK
```

`--protocol chat|anthropic|responses|gemini` picks the endpoint, the request body
and the parser; it defaults to `chat` and each protocol has its own default
`--model`. What counts as "the stream finished" is per protocol too:

| protocol | must survive |
| --- | --- |
| chat | `finish_reason` and the `[DONE]` sentinel |
| anthropic | `content_block_stop`, `message_delta`'s `stop_reason`, `message_stop` |
| responses | `response.output_text.done` (whose text holds the whole item; the deltas must be a prefix of it) and `response.completed` |
| gemini | a final candidate carrying `finishReason` |

Exit 0 only when every expectation held. Each violated expectation gets its own
`FAIL:` line — a missing terminator, an unparseable frame, a `head/tail` that
is no longer `A`/`Z`, or, with `--expect '<text>'`, an answer that is not
exactly that. Pass no `--expect` to inspect only.

`--max-frames <n>` bounds the frame count. With the mock's default 8 chunks, a
chat stream is 11 frames with the plugin off and about 4 with it on, so
`--max-frames 6` fails without the plugin and passes with it. This is the flag
that shows coalescing; `--expect` alone passes either way.

`--upstream http://127.0.0.1:9876` skips the gateway and proves the mock itself,
which is what to reach for when the gateway's own model list is the thing in
doubt.

## Verifying a middleware end to end

With the mock running, the provider entry above in place, and `mock-fast` /
`mock-anthropic` / `mock-responses` / `mock-gemini` all exposed:

```sh
magpie plugin on  /path/to/slowdown.middleware.js
magpie plugin options /path/to/slowdown.middleware.js '{"enabled":true}'

for p in chat anthropic responses gemini; do
  node tools/mock-provider/check-stream.mjs --protocol "$p" --expect 'AxxxxxxZ' || break
done

magpie plugin off /path/to/slowdown.middleware.js     # disable it when the run is over
```

Each run must print `OK`; a middleware that swallows or reorders a frame shows
up as a missing terminator or a short `text`. Add `--max-frames` to require
evidence of coalescing too. `arrival` tells you nothing here: `onEvent` cannot
delay the stream, so the mock's ~5 ms cadence stays the same with the plugin on
or off. `mock-idle` is a second chat model with the same frames; this middleware
dispatches on the event shape, not on the model id.

## Turning a middleware on and off

`magpie plugin` takes effect within about a second — no restart of magpie is
needed, despite what older notes in this repo claim. Keep the window short
anyway: a middleware left on silently eats every streaming response on the
gateway, including your own agent turn.

```sh
magpie plugin on  /path/to/slowdown.middleware.js
magpie plugin options /path/to/slowdown.middleware.js '{"enabled":true,...}'
magpie plugin off /path/to/slowdown.middleware.js     # disable it when the run is over
```

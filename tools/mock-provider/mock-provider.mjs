#!/usr/bin/env node
// tools/mock-provider/mock-provider.mjs
//
// A tiny upstream that speaks four streaming protocols, so gateway middleware
// can be exercised end to end without spending a real provider. One server
// covers all of them; magpie reaches it through a provider entry such as
//
//   magpie provider add mock-stream \
//     url=http://127.0.0.1:9876 \
//     anthropic=http://127.0.0.1:9876 \
//     responses=http://127.0.0.1:9876 \
//     gemini=http://127.0.0.1:9876 \
//     key=mock icon=openai
//   magpie provider models mock-stream mock-fast mock-idle mock-anthropic \
//     mock-responses mock-gemini
//
//   (or hand-write {"id":"mock-stream", …} in ~/.config/magpie/providers.json)
//
// Models, and the protocol each one is meant to be served with:
//
//   mock-fast       OpenAI Chat Completions   POST /v1/chat/completions
//   mock-idle       OpenAI Chat Completions   POST /v1/chat/completions
//   mock-anthropic  Anthropic Messages        POST /v1/messages
//   mock-responses  OpenAI Responses          POST /v1/responses
//   mock-gemini     Gemini generateContent    POST /v1beta/models/mock-gemini:streamGenerateContent
//
//   mock-fast  → the "interesting" model (a middleware may burn time here)
//   mock-idle  → the control model (must pass straight through)
//
// Which shape a response takes is decided by the path asked (the protocol the
// caller speaks); on /v1/chat/completions a known protocol model id
// (mock-anthropic, mock-responses, mock-gemini) instead decides, so one request
// can ask for any shape without changing any path. Any path ending in
// /chat/completions, /messages or /responses, and any :streamGenerateContent /
// :generateContent path, is served — with or without the /v1 or /v1beta prefix.
//
// Frame shapes:
//
//   chat        role chunk, N text chunks, a finish_reason chunk, `[DONE]`
//   anthropic   message_start, content_block_start, N × content_block_delta,
//               content_block_stop, message_delta (stop_reason), message_stop
//               (framed as `event: <type>` + `data: <json>`)
//   responses   response.created, N × response.output_text.delta,
//               response.output_text.done (carries the whole text),
//               response.completed   (no `[DONE]`; OpenAI does not send one)
//   gemini      N candidates, the last one carrying finishReason: "STOP"
//
// A request that does not stream gets the same payload as one object.
//
// Env knobs:
//   MOCK_PORT     listen port                      (default 9876)
//   MOCK_TOTAL    text chunks per response         (default 8)
//   MOCK_SPACING  ms between chunks                (default 5)
//   MOCK_TEXT     text of each chunk               (default "x")
//   MOCK_FIRST    text of the first text chunk     (default "A")
//   MOCK_LAST     text of the last text chunk      (default "Z")
//   MOCK_MODEL    chat model                       (default "mock-fast")
//   MOCK_MODEL2   chat control model               (default "mock-idle")
//   MOCK_MODEL3   Anthropic Messages model         (default "mock-anthropic")
//   MOCK_MODEL4   OpenAI Responses model           (default "mock-responses")
//   MOCK_MODEL5   Gemini model                     (default "mock-gemini")
//   MOCK_QUIET    "1" to silence per-request logging (default: log one line per request)
//
// The first/last chunk texts differ from the rest so a test can tell whether
// the head and the tail of the stream survived a middleware.

import http from "node:http";

const PORT = Number(process.env.MOCK_PORT ?? 9876);
const TOTAL = Number(process.env.MOCK_TOTAL ?? 8);
const SPACING = Number(process.env.MOCK_SPACING ?? 5);
const TEXT = process.env.MOCK_TEXT ?? "x";
const FIRST = process.env.MOCK_FIRST ?? "A";
const LAST = process.env.MOCK_LAST ?? "Z";
const CHAT_MODEL = process.env.MOCK_MODEL ?? "mock-fast";
const IDLE_MODEL = process.env.MOCK_MODEL2 ?? "mock-idle";
const ANTHROPIC_MODEL = process.env.MOCK_MODEL3 ?? "mock-anthropic";
const RESPONSES_MODEL = process.env.MOCK_MODEL4 ?? "mock-responses";
const GEMINI_MODEL = process.env.MOCK_MODEL5 ?? "mock-gemini";
const QUIET = process.env.MOCK_QUIET === "1";

const CHAT_MODELS = [CHAT_MODEL, IDLE_MODEL];
const ALL_MODELS = [...CHAT_MODELS, ANTHROPIC_MODEL, RESPONSES_MODEL, GEMINI_MODEL];

// A protocol model id asked for on the OpenAI Chat path decides the shape;
// anywhere else the path does.
const MODEL_SHAPE = new Map([
  [ANTHROPIC_MODEL, "anthropic"],
  [RESPONSES_MODEL, "responses"],
  [GEMINI_MODEL, "gemini"],
]);

const shapeOfPath = (path) => {
  const p = path.split("?")[0].replace(/\/+$/, "");
  if (/:(?:stream)?[Gg]enerateContent$/.test(p) || /:countTokens$/.test(p)) return "gemini";
  if (p.endsWith("/messages")) return "anthropic";
  if (p.endsWith("/responses")) return "responses";
  if (p.endsWith("/chat/completions")) return "chat";
  return null;
};

const isStreamPath = (path) => !/:generateContent$/.test(path.split("?")[0]);

// The chunk texts, head and tail marked so a truncated answer is obvious.
const chunkTexts = () =>
  Array.from({ length: TOTAL }, (_, i) => (i === 0 ? FIRST : i === TOTAL - 1 ? LAST : TEXT));
const wholeText = () => chunkTexts().join("");

// The model name can arrive percent-encoded in the URL. A malformed escape
// must not throw: the mock answers or 404s, it does not exit.
const decodeModel = (raw) => {
  try {
    return decodeURIComponent(raw);
  } catch {
    return raw;
  }
};

const listModels = () => ({
  object: "list",
  data: ALL_MODELS.map((id) => ({
    id,
    object: "model",
    created: Math.floor(Date.now() / 1000),
    owned_by: "mock",
  })),
});

// ---------------------------------------------------------------- chat

const chatBody = (id, created, model, chunks) => ({
  id,
  object: "chat.completion",
  created,
  model,
  choices: [
    {
      index: 0,
      message: { role: "assistant", content: chunks.join("") },
      logprobs: null,
      finish_reason: "stop",
    },
  ],
  usage: { prompt_tokens: 11, completion_tokens: chunks.length, total_tokens: 11 + chunks.length },
});

const chatChunk = (id, created, model, delta, finish = null) => ({
  id,
  object: "chat.completion.chunk",
  created,
  model,
  choices: [{ index: 0, delta, logprobs: null, ...(finish ? { finish_reason: finish } : {}) }],
});

// ---------------------------------------------------------------- anthropic

const anthropicBody = (id, model, chunks) => ({
  id: `msg_${id}`,
  type: "message",
  role: "assistant",
  model,
  content: [{ type: "text", text: chunks.join("") }],
  stop_reason: "end_turn",
  stop_sequence: null,
  usage: { input_tokens: 11, output_tokens: chunks.length },
});

const anthropicFrames = (id, model, chunks) => {
  const f = [];
  const ev = (type, payload) => ({ event: type, payload });
  f.push(
    ev("message_start", {
      type: "message_start",
      message: {
        id: `msg_${id}`,
        type: "message",
        role: "assistant",
        model,
        content: [],
        stop_reason: null,
        stop_sequence: null,
        usage: { input_tokens: 11, output_tokens: 0 },
      },
    }),
  );
  f.push(ev("content_block_start", { type: "content_block_start", index: 0, content_block: { type: "text", text: "" } }));
  for (const c of chunks) {
    f.push(
      ev("content_block_delta", {
        type: "content_block_delta",
        index: 0,
        delta: { type: "text_delta", text: c },
      }),
    );
  }
  f.push(ev("content_block_stop", { type: "content_block_stop", index: 0 }));
  f.push(
    ev("message_delta", {
      type: "message_delta",
      delta: { stop_reason: "end_turn", stop_sequence: null },
      usage: { output_tokens: chunks.length },
    }),
  );
  f.push(ev("message_stop", { type: "message_stop" }));
  return f;
};

// ---------------------------------------------------------------- responses

const responsesMessageItem = (id, text) => ({
  id: `msg_${id}`,
  type: "message",
  status: "completed",
  role: "assistant",
  content: [{ type: "output_text", text, annotations: [] }],
});

const responsesObject = (id, model, status, output) => ({
  id: `resp_${id}`,
  object: "response",
  created_at: Math.floor(Date.now() / 1000),
  status,
  model,
  output,
  error: null,
  incomplete_details: null,
  instructions: null,
  max_output_tokens: null,
  parallel_tool_calls: true,
  previous_response_id: null,
  reasoning: { effort: null, summary: null },
  store: false,
  temperature: 1,
  text: { format: { type: "text" } },
  tool_choice: "auto",
  tools: [],
  top_p: 1,
  truncation: "disabled",
  usage:
    status === "in_progress"
      ? null
      : {
          input_tokens: 11,
          input_tokens_details: { cached_tokens: 0 },
          output_tokens: TOTAL,
          output_tokens_details: { reasoning_tokens: 0 },
          total_tokens: 11 + TOTAL,
        },
  user: null,
  metadata: {},
});

const responsesFrames = (id, model, chunks) => {
  const itemId = `msg_${id}`;
  const f = [];
  f.push({ payload: { type: "response.created", sequence_number: 0, response: responsesObject(id, model, "in_progress", []) } });
  chunks.forEach((text, i) => {
    f.push({
      payload: {
        type: "response.output_text.delta",
        sequence_number: i + 1,
        item_id: itemId,
        output_index: 0,
        content_index: 0,
        delta: text,
      },
    });
  });
  f.push({
    payload: {
      type: "response.output_text.done",
      sequence_number: chunks.length + 1,
      item_id: itemId,
      output_index: 0,
      content_index: 0,
      text: chunks.join(""),
    },
  });
  f.push({
    payload: {
      type: "response.completed",
      sequence_number: chunks.length + 2,
      response: responsesObject(id, model, "completed", [responsesMessageItem(id, chunks.join(""))]),
    },
  });
  return f;
};

// ---------------------------------------------------------------- gemini

const geminiFrame = (id, model, text, finish = null) => ({
  candidates: [
    {
      content: { role: "model", parts: [{ text }] },
      index: 0,
      ...(finish ? { finishReason: finish } : {}),
    },
  ],
  usageMetadata: {
    promptTokenCount: 11,
    candidatesTokenCount: TOTAL,
    totalTokenCount: 11 + TOTAL,
  },
  modelVersion: model,
  responseId: `resp_${id}`,
});

const geminiBody = (id, model, text) => ({
  candidates: [
    {
      content: { role: "model", parts: [{ text }] },
      finishReason: "STOP",
      index: 0,
      safetyRatings: [],
    },
  ],
  usageMetadata: { promptTokenCount: 11, candidatesTokenCount: TOTAL, totalTokenCount: 11 + TOTAL },
  modelVersion: model,
  responseId: `resp_${id}`,
});

// ---------------------------------------------------------------- serving

const sseHeaders = {
  "content-type": "text/event-stream; charset=utf-8",
  "cache-control": "no-cache",
  connection: "keep-alive",
  "x-accel-buffering": "no",
};

// Frames are grouped: everything in one group is written together, then the
// mock waits MOCK_SPACING ms before the next group. That is what makes a
// paced, gap-by-gap stream for the middleware to slow down.
const streamShape = (shape, id, model, chunks) => {
  switch (shape) {
    case "anthropic":
      return anthropicFrames(id, model, chunks).map((f) => [
        f.event ? `event: ${f.event}\ndata: ${JSON.stringify(f.payload)}\n\n` : `data: ${JSON.stringify(f.payload)}\n\n`,
      ]);
    case "responses":
      return responsesFrames(id, model, chunks).map((f) => [`data: ${JSON.stringify(f.payload)}\n\n`]);
    case "gemini":
      return chunks.map((text, i) => {
        const last = i === chunks.length - 1;
        return [`data: ${JSON.stringify(geminiFrame(id, model, text, last ? "STOP" : null))}\n\n`];
      });
    case "chat":
    default: {
      const created = Math.floor(Date.now() / 1000);
      const groups = [[`data: ${JSON.stringify(chatChunk(id, created, model, { role: "assistant", content: "" }))}\n\n`]];
      for (const text of chunks) {
        groups.push([`data: ${JSON.stringify(chatChunk(id, created, model, { content: text }))}\n\n`]);
      }
      groups.push([
        `data: ${JSON.stringify(chatChunk(id, created, model, {}, "stop"))}\n\n`,
        "data: [DONE]\n\n",
      ]);
      return groups;
    }
  }
};

const nonStreamBody = (shape, id, model, chunks, created) => {
  switch (shape) {
    case "anthropic":
      return anthropicBody(id, model, chunks);
    case "responses":
      return responsesObject(id, model, "completed", [responsesMessageItem(id, chunks.join(""))]);
    case "gemini":
      return geminiBody(id, model, chunks.join(""));
    case "chat":
    default:
      return chatBody(id, created, model, chunks);
  }
};

const server = http.createServer((req, res) => {
  const url = new URL(req.url, `http://127.0.0.1:${PORT}`);
  const path = url.pathname;

  if (req.method === "GET" && path.endsWith("/v1/models")) {
    res.writeHead(200, { "content-type": "application/json" });
    res.end(JSON.stringify(listModels()));
    return;
  }

  if (req.method === "GET" && path.endsWith("/v1beta/models")) {
    res.writeHead(200, { "content-type": "application/json" });
    res.end(
      JSON.stringify({
        models: ALL_MODELS.map((id) => ({
          name: `models/${id}`,
          baseModelId: id,
          displayName: id,
          version: "001",
          supportedGenerationMethods: ["generateContent", "streamGenerateContent", "countTokens"],
        })),
      }),
    );
    return;
  }

  const shape = shapeOfPath(path);
  if (req.method !== "POST" || shape == null) {
    res.writeHead(404, { "content-type": "application/json" });
    res.end(
      JSON.stringify({
        error: {
          code: 404,
          message: `mock-provider serves /v1/chat/completions, /v1/messages, /v1/responses, /v1beta/models/{model}:streamGenerateContent and their :generateContent siblings, not ${path}`,
        },
      }),
    );
    return;
  }

  let raw = "";
  req.on("error", () => req.destroy()); // an aborted client must not take the mock down
  req.on("data", (c) => (raw += c));
  req.on("end", () => {
    let body = {};
    try {
      body = JSON.parse(raw || "{}");
    } catch {
      body = {};
    }

    // The Gemini path names the model in the URL; everywhere else it is in the
    // body.
    const fromPath = /\/models\/([^/:]+):/.exec(path)?.[1];
    const requested = decodeModel(fromPath || body.model || CHAT_MODEL);
    // A protocol model id beats the path only on the OpenAI Chat path, which is
    // the one path all four shapes can be asked for.
    const answered = shape === "chat" && MODEL_SHAPE.has(requested) ? MODEL_SHAPE.get(requested) : shape;

    const id = `mock-${Date.now().toString(36)}`;
    const created = Math.floor(Date.now() / 1000);
    const model = ALL_MODELS.includes(requested) ? requested : body.model || CHAT_MODEL;
    const chunks = chunkTexts();

    if (!QUIET) {
      console.log(`${new Date().toISOString()} ${req.method} ${path} → ${answered} (${model})`);
    }

    const wantsStream =
      answered === "gemini" ? isStreamPath(path) : body.stream === true || url.searchParams.get("alt") === "sse";

    if (!wantsStream) {
      res.writeHead(200, { "content-type": "application/json" });
      res.end(JSON.stringify(nonStreamBody(answered, id, model, chunks, created)));
      return;
    }

    res.writeHead(200, sseHeaders);

    const groups = streamShape(answered, id, model, chunks);
    const wait = (ms) => new Promise((r) => setTimeout(r, ms));
    const stopped = () => res.writableEnded || res.destroyed;

    (async () => {
      for (const [i, group] of groups.entries()) {
        if (i > 0) await wait(SPACING);
        if (stopped()) return;
        res.write(group.join(""));
      }
      if (!stopped()) res.end();
    })().catch((e) => {
      if (!stopped()) res.end();
      console.error(`mock-provider: ${e?.message ?? e}`);
    });
  });
});

server.listen(PORT, "127.0.0.1", () => {
  console.log(`mock-provider on http://127.0.0.1:${PORT}`);
  console.log(`  chat       ${CHAT_MODEL}, ${IDLE_MODEL}      POST /v1/chat/completions`);
  console.log(`  anthropic  ${ANTHROPIC_MODEL}               POST /v1/messages`);
  console.log(`  responses  ${RESPONSES_MODEL}               POST /v1/responses`);
  console.log(`  gemini     ${GEMINI_MODEL}      POST /v1beta/models/${GEMINI_MODEL}:streamGenerateContent`);
  console.log(
    `  ${TOTAL} text chunks, ${SPACING}ms apart, text=${JSON.stringify(TEXT)} head=${FIRST} tail=${LAST} → ${JSON.stringify(wholeText())}`,
  );
});

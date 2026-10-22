#!/usr/bin/env node
// tools/mock-provider/check-stream.mjs
//
// Sends one streaming request — through the magpie gateway by default, or
// straight at the mock with --upstream — and reports what actually came back:
// frame count, per-frame arrival time, the concatenated text, and whether the
// protocol's terminator survived. This is the check that catches a middleware
// eating the response; unit tests cannot.
//
//   node tools/mock-provider/check-stream.mjs --protocol chat \
//     --model mock-stream/mock-fast --expect 'AxxxxxxZ'
//   node tools/mock-provider/check-stream.mjs --protocol anthropic \
//     --model mock-stream/mock-anthropic --expect 'AxxxxxxZ'
//   node tools/mock-provider/check-stream.mjs --protocol responses \
//     --model mock-stream/mock-responses --expect 'AxxxxxxZ'
//   node tools/mock-provider/check-stream.mjs --protocol gemini \
//     --model mock-stream/mock-gemini --expect 'AxxxxxxZ'
//
//   --upstream http://127.0.0.1:9876   skip the gateway, prove the mock itself
//   --gateway http://127.0.0.1:3425   where magpie listens (default)
//   --key magpie                      the gateway's key (default)
//   --expect '<text>'                 the whole answer must be exactly this
//   --text                            the request's text (default "hi")
//   --max-frames <n>                  the stream must arrive in at most n frames
//                                     (with the plugin on; proves coalescing)
//
// Exit code 0 = every expectation held, 1 = at least one failed; each violated
// expectation gets its own FAIL: line.

const argv = process.argv.slice(2);
const arg = (name, dflt) => {
  const i = argv.indexOf(`--${name}`);
  return i >= 0 && argv[i + 1] ? argv[i + 1] : dflt;
};

const json = (frame) => {
  try {
    return JSON.parse(frame.data);
  } catch {
    return null;
  }
};
const short = (v) => {
  const s = JSON.stringify(v) ?? "";
  return s.length > 40 ? `${s.slice(0, 39)}…` : s;
};

// ------------------------------------------------------------- the four protocols

const PROTOCOLS = {
  chat: {
    defaultModel: "mock-stream/mock-fast",
    // OpenAI Chat Completions
    request: (base, model, key, text) => ({
      url: `${base}/v1/chat/completions`,
      headers: { "content-type": "application/json", authorization: `Bearer ${key}` },
      body: { model, stream: true, messages: [{ role: "user", content: text }] },
    }),
    // what: finish_reason and the [DONE] sentinel
    needs: ["text", "finish_reason", "[DONE]"],
    parse: (frames) => {
      let text = "";
      let finish = null;
      let done = false;
      for (const f of frames) {
        if (f.data === "[DONE]") {
          done = true;
          continue;
        }
        const ev = json(f);
        if (!ev) continue;
        const choice = ev?.choices?.[0];
        const delta = choice?.delta?.content;
        if (typeof delta === "string" && delta.length > 0) text += delta;
        if (choice?.finish_reason != null) finish = choice.finish_reason;
      }
      return {
        text,
        saw: { text: text.length > 0, finish_reason: finish != null, "[DONE]": done },
        detail: { finish_reason: finish },
        label: (f) => {
          if (f.data === "[DONE]") return "[DONE]";
          const ev = json(f);
          const delta = ev?.choices?.[0]?.delta;
          if (delta?.role) return `role=${delta.role}`;
          return short(delta?.content ?? delta ?? {});
        },
      };
    },
  },

  anthropic: {
    defaultModel: "mock-stream/mock-anthropic",
    // Anthropic Messages
    request: (base, model, key, text) => ({
      url: `${base}/v1/messages`,
      headers: {
        "content-type": "application/json",
        authorization: `Bearer ${key}`,
        "x-api-key": key,
        "anthropic-version": "2023-06-01",
      },
      body: { model, max_tokens: 1024, stream: true, messages: [{ role: "user", content: text }] },
    }),
    // what: message_stop, plus the block and stop_reason frames before it
    needs: ["text", "content_block_stop", "stop_reason", "message_stop"],
    parse: (frames) => {
      let text = "";
      let stop = null;
      let blockStop = false;
      let messageStop = false;
      for (const f of frames) {
        const ev = json(f);
        const type = f.event || ev?.type;
        if (type === "content_block_delta" && ev?.delta?.type === "text_delta") {
          const t = ev.delta.text;
          if (typeof t === "string") text += t;
        } else if (type === "message_delta") {
          if (ev?.delta?.stop_reason != null) stop = ev.delta.stop_reason;
        } else if (type === "content_block_stop") {
          blockStop = true;
        } else if (type === "message_stop") {
          messageStop = true;
        }
      }
      return {
        text,
        saw: {
          text: text.length > 0,
          content_block_stop: blockStop,
          stop_reason: stop != null,
          message_stop: messageStop,
        },
        detail: { stop_reason: stop },
        label: (f) => {
          const ev = json(f);
          const type = f.event || ev?.type || "?";
          const t = ev?.delta?.text ?? ev?.delta?.stop_reason ?? ev?.text;
          return t == null ? type : `${type} ${JSON.stringify(t)}`;
        },
      };
    },
  },

  responses: {
    defaultModel: "mock-stream/mock-responses",
    // OpenAI Responses (Codex speaks this)
    request: (base, model, key, text) => ({
      url: `${base}/v1/responses`,
      headers: { "content-type": "application/json", authorization: `Bearer ${key}` },
      body: { model, stream: true, input: text },
    }),
    // what: response.completed, plus the done frame carrying the whole text.
    // OpenAI sends no [DONE] sentinel on this API.
    needs: ["text", "response.output_text.done", "response.completed"],
    parse: (frames) => {
      // A Responses consumer reconstructs each output item from its
      // `output_text.done.text`, which the spec says holds the FULL text of
      // that item; the deltas are a preview. A coalescing middleware
      // legitimately withholds the tail of the item's text until that frame,
      // so the emitted deltas must be a PREFIX of the done text, never longer
      // or reordered.
      const items = new Map();
      const itemFor = (id) => {
        if (!items.has(id)) items.set(id, { deltas: "", doneText: null });
        return items.get(id);
      };
      let created = false;
      let completed = false;
      let status = null;
      for (const f of frames) {
        const ev = json(f);
        if (!ev) continue;
        switch (ev.type) {
          case "response.created":
            created = true;
            break;
          case "response.output_text.delta":
            if (typeof ev.delta === "string") itemFor(ev.item_id ?? "?").deltas += ev.delta;
            break;
          case "response.output_text.done":
            itemFor(ev.item_id ?? "?").doneText = ev.text ?? null;
            break;
          case "response.completed":
            completed = true;
            status = ev?.response?.status ?? null;
            break;
        }
      }
      const all = [...items.values()];
      const deltas = all.map((i) => i.deltas).join("");
      const doneText = all.map((i) => i.doneText).filter((t) => t != null).join("");
      const text = all.map((i) => i.doneText ?? i.deltas).join("");
      const problems = [];
      for (const [id, item] of items) {
        if (item.doneText != null && !item.doneText.startsWith(item.deltas)) {
          problems.push(
            `item ${JSON.stringify(id)}: emitted deltas ${JSON.stringify(item.deltas)} are not a prefix of output_text.done.text ${JSON.stringify(item.doneText)}`,
          );
        }
      }
      return {
        text,
        problems,
        saw: {
          text: text.length > 0,
          "response.output_text.done": all.some((i) => i.doneText != null),
          "response.completed": completed,
        },
        detail: {
          response_created: created,
          status,
          deltas: JSON.stringify(deltas),
          done_text: doneText.length > 0 ? doneText : null,
        },
        label: (f) => {
          const ev = json(f);
          if (ev?.type === "response.output_text.delta") return `response.output_text.delta ${JSON.stringify(ev.delta)}`;
          if (ev?.type === "response.completed") return `response.completed status=${ev?.response?.status}`;
          return ev?.type ?? "?";
        },
      };
    },
  },

  gemini: {
    defaultModel: "mock-stream/mock-gemini",
    // Gemini generateContent; the model is named in the path, as Gemini CLI
    // and the google-genai SDKs name it.
    request: (base, model, key, text) => ({
      url: `${base}/v1beta/models/${encodeURIComponent(model)}:streamGenerateContent?alt=sse`,
      headers: { "content-type": "application/json", "x-goog-api-key": key, authorization: `Bearer ${key}` },
      body: { contents: [{ role: "user", parts: [{ text }] }] },
    }),
    // what: a final candidate carrying finishReason
    needs: ["text", "finishReason"],
    parse: (frames) => {
      let text = "";
      let finish = null;
      for (const f of frames) {
        const ev = json(f);
        const cand = ev?.candidates?.[0];
        const t = cand?.content?.parts?.[0]?.text;
        if (typeof t === "string" && t.length > 0) text += t;
        if (cand?.finishReason != null) finish = cand.finishReason;
      }
      return {
        text,
        saw: { text: text.length > 0, finishReason: finish != null },
        detail: { finishReason: finish },
        label: (f) => {
          const ev = json(f);
          const cand = ev?.candidates?.[0];
          const t = cand?.content?.parts?.[0]?.text;
          return cand?.finishReason != null ? `${JSON.stringify(t)} finishReason=${cand.finishReason}` : JSON.stringify(t ?? {});
        },
      };
    },
  },
};

const ALIASES = {
  openai: "chat",
  "openai-chat": "chat",
  messages: "anthropic",
  "openai-responses": "responses",
  "gemini-generate": "gemini",
};

// ------------------------------------------------------------- run

const protocolName = (arg("protocol", "chat") || "chat").toLowerCase();
const protocol = PROTOCOLS[ALIASES[protocolName] ?? protocolName];
if (!protocol) {
  console.error(`unknown --protocol ${protocolName}; one of ${Object.keys(PROTOCOLS).join(", ")}`);
  process.exit(2);
}

const model = arg("model", protocol.defaultModel);
const expect = arg("expect", null);
const maxFramesArg = arg("max-frames", null);
const maxFrames = maxFramesArg == null ? null : Number(maxFramesArg);
if (maxFrames != null && (!Number.isInteger(maxFrames) || maxFrames < 1)) {
  console.error(`--max-frames must be a positive integer, got ${JSON.stringify(maxFramesArg)}`);
  process.exit(2);
}
const ask = arg("text", "hi");
const upstream = arg("upstream", null);
const base = (upstream ?? arg("gateway", "http://127.0.0.1:3425")).replace(/\/$/, "");
const key = arg("key", "magpie");

const { url, headers, body } = protocol.request(base, model, key, ask);

const t0 = Date.now();
let res;
try {
  res = await fetch(url, { method: "POST", headers, body: JSON.stringify(body) });
} catch (e) {
  console.error(`FAIL: could not reach ${url}: ${e?.message ?? e}`);
  process.exit(1);
}

if (!res.ok || !res.body) {
  console.error(`FAIL: ${upstream ? "mock" : "gateway"} returned ${res.status} ${res.statusText} for ${url}`);
  try {
    console.error((await res.text()).slice(0, 400));
  } catch {}
  process.exit(1);
}

// SSE frames as they complete, keeping arrival times for the gap report.
const frames = [];
try {
  const decoder = new TextDecoder();
  let buf = "";
  for await (const piece of res.body) {
    buf += decoder.decode(piece, { stream: true });
    buf = buf.replace(/\r\n/g, "\n"); // a proxy may hand back CRLF
    let cut;
    while ((cut = buf.indexOf("\n\n")) >= 0) {
      const block = buf.slice(0, cut);
      buf = buf.slice(cut + 2);
      const frame = { at: Date.now() - t0, event: null, data: null };
      for (const line of block.split("\n")) {
        if (line.startsWith("event:")) frame.event = line.slice(6).trim();
        else if (line.startsWith("data:")) frame.data = line.slice(5).trim();
      }
      if (frame.data != null) frames.push(frame);
    }
  }
  if (buf.trim()) {
    for (const line of buf.split("\n")) {
      if (!line.startsWith("data:")) continue;
      frames.push({ at: Date.now() - t0, event: null, data: line.slice(5).trim() });
    }
  }
} catch (e) {
  console.error(`FAIL: stream from ${url} broke: ${e?.message ?? e}`);
  process.exit(1);
}

const unparsed = frames.filter((f) => f.data !== "[DONE]" && !json(f)).length;
const { text, saw, detail, label, problems: parseProblems = [] } = protocol.parse(frames);

const gaps = frames.slice(1).map((f, i) => f.at - frames[i].at);
const problems = [...parseProblems];
if (frames.length === 0) problems.push("no SSE frames arrived at all");
if (unparsed > 0) problems.push(`${unparsed} frame(s) were not valid JSON`);
for (const need of protocol.needs) {
  if (!saw[need]) problems.push(`no ${need} frame arrived`);
}
if (maxFrames != null && frames.length > maxFrames) {
  problems.push(`frames ${frames.length} > --max-frames ${maxFrames}: the stream was not coalesced`);
}
if (expect != null && text !== expect) {
  problems.push(`text mismatch: got ${JSON.stringify(text)} want ${JSON.stringify(expect)}`);
}

console.log(`protocol   ${protocolName}`);
console.log(`target     ${upstream ? "upstream" : "gateway"} ${url}`);
console.log(`model      ${model}`);
console.log(`frames     ${frames.length}`);
console.log(`span       ${frames.length ? frames.at(-1).at : 0}ms`);
console.log(`text       ${JSON.stringify(text)} (${text.length} chars)`);
console.log(`head/tail  ${JSON.stringify(text.slice(0, 1))} / ${JSON.stringify(text.slice(-1))}`);
console.log(
  `terminator ${protocol.needs.filter((n) => n !== "text").map((n) => `${n}=${saw[n]}`).join(" ")}`,
);
const values = Object.entries(detail).filter(([, v]) => v != null && v !== false && typeof v !== "boolean");
if (values.length) console.log(`values     ${values.map(([k, v]) => `${k}=${v}`).join(" ")}`);
console.log(`arrival    ${frames.map((f) => f.at).join(",")}ms  gaps=[${gaps.join(",")}]`);
frames.forEach((f, i) => console.log(`  ${String(i).padStart(2)} ${String(f.at).padStart(6)}ms ${label(f)}`));

if (problems.length) {
  for (const p of problems) console.error(`FAIL: ${p}`);
  process.exit(1);
}
console.log("\nOK");

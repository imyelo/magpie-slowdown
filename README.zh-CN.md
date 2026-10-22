# @yelo/magpie-slowdown

> 🐢 一个降低 Agent 客户端收到流式响应频率的 magpie 网关中间件。

[English](./README.md) | 中文

magpie-slowdown 通过把窗口内的多帧合并成一个大帧，达到在不改变实际 TPS 的同时，控制客户端接收 API 响应事件频率的效果 —— 有助于调试 Agent 客户端在不同输出频率下的表现。

支持三种响应协议：Chat Completions、Responses 和 Gemini；*不支持 Anthropic，它的流将原速透传。*

## 安装

```sh
magpie plugin add @yelo/magpie-slowdown
magpie plugin options @yelo/magpie-slowdown '{"enabled":true,"intervalMs":50}'
```

改动约一秒内生效。magpie 不需要重启。

也可以通过 magpie 的 `plugin off` 手动禁用：

```sh
magpie plugin off @yelo/magpie-slowdown
```

## 选项

| 字段 | 类型 | 默认值 | 说明 |
| ---- | ---- | ------ | ---- |
| `enabled` | bool | `false` | 总开关。为 `false` 时每个 event 原样透传。 |
| `intervalMs` | int | `50` | 合并窗口，单位毫秒。每个窗口最多发出一帧合并后的帧。 |
| `maxBufferSize` | int | `1000` | pending 窗口里最多扣留多少帧。触顶就提前 flush，文本仍然完整保留。 |

## 为什么不是逐帧降速

magpie-slowdown 降低的是 **客户端收到 event 的频率**，不是降低 TPS，也不改变总响应时长。在每个时间窗口内把数据堆积到缓冲区，并在下一个窗口开始时合并缓冲区内堆积的所有数据，以此降低下游每秒收到的事件数。

受限于 Magpie Plugin 当前版本的 API，中间件无法主动向下游发出新 event，所以 magpie-slowdown **不延长总响应时间**：

| 事实背景 | 结论 |
| ---- | ---- |
| `onEvent` 只能返回一个 event、一个对象、`null`（丢弃）或 `undefined`（保持不变） | 单个上游 event 无法产生多个下游 events |
| 返回数组会被当成单帧的 JSON（`data: [{…},{…}]`） | 它不会展开成多帧，而且会破坏客户端 |
| moejs 没有 `setTimeout` / `setInterval`，`ctx` 也没有暴露 write、flush 或 delay 句柄 | 无法延迟发送事件 |
| `onEvent` 有 50 ms 的等待上限。超时后 magpie 会把该 event 原样发出 | 在 hook 里阻塞进程没有实际意义 |
| 响应在上游结束时结束 | 总时长由 provider 决定 |

—— 目前中间件能控制的只有 **帧数**。

## 合并规则

在 `intervalMs` 以内的、携带文本的 event 会被转入缓存。到下一个窗口边界时，它们作为一帧合并发出。客户端收到的帧更少，而文本完全相同。

- 一个响应的第一帧文本立即发出，首 token 延迟不变。
- 不携带文本（`role`、usage、keep-alive）、也没有 tool call 的帧直接透传。纯 tool_call delta 按引用原样通过，不做改动。
- 同时携带文本和 tool call 的帧（经测试 MiniMax-M3 在 thinking→tool 边界会发这种帧）按普通文本帧合并，`tool_calls` 不丢。合并后的帧按 `index` 聚合 tool call：先出现的 identity 胜出，`arguments` 按到达顺序拼接。被转入缓存的 tool_call 帧会挡住后续片段，所以 arguments JSON 不会被重排。Gemini 的非文本 parts（`functionCall` 等）和 Responses 的非文本 delta parts 同样保留。
- Responses 的文本按 `item_id` 分开追踪。合并后的 `output_text.done` 只携带它自己的 item 文本。中间件不会把 `function_call_arguments.delta` 当文本处理。
- 终止帧始终不会被丢弃。它到达时，还没发出的内容（文本、tool_calls、parts）跟着它一起送出。所以响应的尾部不会消失。
- hook 里抛异常时，原始 event 原样返回。
- `enabled: false` 是默认值。此时每个 event 原样返回，对象引用不变，magpie 自己的序列化不受影响。

**中间件永远不合并 Anthropic 帧。整条 Anthropic 流原样透传。**

> Anthropic 流的尾部没有安全的落点，所以中间件不碰它的任何一帧。细节见 [哪些响应协议支持合并](#哪些响应协议支持合并)。

## 支持的响应协议

中间件从 **event 本身**判断协议，不看 `ctx.protocol`。`ctx.protocol` 描述的是客户端所用的 API。provider 按客户端协议提供服务时，magpie 原样转发上游的字节，所以 chat 客户端可能在读 Anthropic 协议的字节，而 `ctx.protocol` 说的却是另一回事。协议认不出的 event，以及每一个 Anthropic 帧，都原样透传。

| 协议 | 识别依据 | 文本字段 | 终止帧 | pending 尾帧的去处 |
| ---- | -------- | -------- | ------ | ------------------ |
| OpenAI Chat Completions | `choices[]` | `choices[0].delta.content` | `choices[].finish_reason` 已设置 | 终止帧本身（最后一个 chunk 可以同时携带 content 和 `finish_reason`） |
| OpenAI Responses | `type: "response.…"` | `delta`（字符串或 parts 数组） | `response.completed` / `response.incomplete` | `response.output_text.done`，它的 `text` 本就用于承载到目前为止的完整文本 |
| Gemini | `candidates[]` | `candidates[0].content.parts[0].text` | `candidates[].finishReason` 已设置 | 终止 candidate |
| Anthropic Messages | `type: "message_…"` / `"content_block_…"` | — | — | **无：永远透传，绝不合并** |

`data: [DONE]` 由 magpie 原样转发，`onEvent` 看不到它。

### 哪些响应协议支持合并

`chat`、`responses` 和 `gemini` 协议的响应支持合并。这几个协议均有明确的终止帧，最后一个窗口的缓冲区数据可以在终止帧发出。

Anthropic 协议的响应不支持合并。Anthropic 的终止帧不支持携带文本；如果合并，最后一个窗口的缓冲区数据将会丢失。

## 开发

工具链由 `mise.toml` 固定（Node 24 与 Yarn 4），`package.json` 在
`packageManager` 中声明同一个 Yarn 版本，Corepack 可直接激活。

```sh
mise install
yarn
```

单元测试:

```sh
yarn test
```

使用 [Mock Provider](tools/mock-provider/README.md) 进行集成测试。

```sh
node tools/mock-provider/check-stream.mjs --model mock-stream/mock-fast --expect 'AxxxxxxZ'
```

CI 通过 `jdx/mise-action` 安装 `mise.toml` 固定的同一套工具链，再执行 `yarn install --immutable && yarn test`。

## License

Apache-2.0

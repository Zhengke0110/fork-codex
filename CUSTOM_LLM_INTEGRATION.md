# Custom LLM Integration — 项目状态与路线图

> 阶段名称：**DeepSeek Custom Provider and Protocol Compatibility Integration**
> 分支：`experiment/security-agent-bootstrap`（基线 `d7ba5ff955`）
> 最近提交：`3863bda5e1 feat: integrate custom OpenAI-compatible LLM via CODEX_LLM_* + chat/completions`

## 一、项目目标

在不重写 Codex 的前提下，接入一个可自定义配置的 OpenAI-compatible 国产大模型（当前目标 **DeepSeek-V4-Pro**），并满足：

1. 保留 Codex 原有 Agent Loop、Session、Turn、Skills、MCP、Shell、审批、沙箱、上下文管理能力。
2. 模型名 / API Key / Base URL **全部来自项目根目录 `.env`**（`CODEX_LLM_*`），不硬编码。
3. 自定义模型启用后**无需 ChatGPT/OpenAI 登录**。
4. 支持文本对话、流式输出、工具调用。
5. 后续通过该模型编排 Codex 已有 Skills / MCP / 内置工具完成 **CTF 解题**。

## 二、核心架构（已实现）

```
项目根 .env (CODEX_LLM_*)
   │  arg0 仅放行 CODEX_LLM_*（其余 CODEX_* 仍被过滤；不覆盖进程已有变量）
   ▼
ConfigLayerSource::EnvLlm (precedence 28)
   │  build_codex_llm_env_layer → [model_providers.<id>]
   │    base_url / env_key / requires_openai_auth=false / wire_api / stream_idle_timeout_ms
   ▼
Config 解析 → 选择自定义 provider
   │  requires_openai_auth=false → 登录门控全部短路（零门控代码改动）
   │  env_key → 运行时惰性读取 API Key → Authorization: Bearer
   ▼
core/client.rs: match wire_api
   ├─ Responses        → 原有 /responses 路径（不变）
   └─ ChatCompletions  → stream_chat_completions_api
                          ├─ requests/chat.rs:  ResponseItem → messages（函数调用→tool_calls，输出→role:tool）
                          ├─ endpoint/chat.rs:  POST /chat/completions
                          └─ sse/chat.rs:       chat delta → 统一 ResponseEvent
                                                 （OutputItemAdded→OutputTextDelta→OutputItemDone）
```

**设计要点**：chat SSE 解析器产出与 Responses **完全相同的 `ResponseEvent`**，因此 `map_response_stream`、会话循环、工具执行、TUI 全部复用，零改动。Codex core 的唯一改动是 `wire_api` 分发臂 + `stream_chat_completions_api` 方法。

## 三、已完成工作

### Stage 1 — 配置与免登录基础 ✅
- `arg0/src/lib.rs`：读取项目根 `.env`，**仅放行 `CODEX_LLM_*`**；进程已有环境变量优先（dotenv 语义）。
- `config`：新增 `ConfigLayerSource::EnvLlm`（precedence 28）+ `build_codex_llm_env_layer()`，严格 opt-in（无 key/base_url 时不产生层）。
- 免登录：`requires_openai_auth=false` 自动跳过 ChatGPT 登录。
- 8 处穷尽 match 同步更新（config / hooks / tui / app-server / core-skills 等）。

### Stage 2A — chat/completions 请求与流式文本 ✅
- `model-provider-info`：`WireApi::ChatCompletions` 变体（接受 `chat_completions` 与遗留 `chat`）；config schema 已重新生成。
- `codex-api/requests/chat.rs`（新）：`build_chat_completions_body()`，ResponseItem → messages 转换。
- `codex-api/sse/chat.rs`（新）：chat SSE → `ResponseEvent`，含完整 assistant 消息 item 生命周期。
- `codex-api/endpoint/chat.rs`（新）：`ChatCompletionsClient`。
- `core/client.rs`：`WireApi::ChatCompletions` 分发臂 + `stream_chat_completions_api()`。
- `tools`：`create_tools_json_for_chat_completions_api()`（Function 工具 → chat function）。

### Stage 2B — 工具调用、错误映射、集成测试 ✅
- 工具调用增量参数拼接、多工具同轮、`finish_reason` 映射（length→ContextWindowExceeded）、token usage 映射。
- 真实 Agent Loop 集成测试 `core/tests/suite/chat_completions_loop.rs`。

## 四、验收状态（离线 + 真实 Agent Loop）

| 验收项 | 状态 | 说明 |
|---|---|---|
| 配置可自定义（Key/Model/Base URL） | ✅ | 运行时来自 `CODEX_LLM_*`，未硬编码 |
| 无配置时原生行为不变 | ✅ | EnvLlm 层 opt-in，缺变量时返回 None |
| 免 OpenAI 登录 | ✅ | `requires_openai_auth=false` |
| URL 拼接（5 种形态） | ✅ | 无 `//chat`、`/v1/v1`、重复路径 |
| 环境变量优先级 | ✅（修复后）| 原有覆盖 bug 已修 |
| 离线 SSE 协议（文本/中文/参数分片/多工具/usage/finish_reason/错误） | ✅ | 13 个 codex-api 测试 |
| 真实 Agent Loop 文本轮次 | ✅ | `chat_completions_plain_text_turn` |
| 真实 Agent Loop 单工具往返（多轮 follow-up） | ✅ | `chat_completions_single_tool_roundtrip` |
| 工具兼容性普查 | ✅ | Function 工具（含 MCP/extension）可转；Freeform/WebSearch/ToolSearch/Namespace 跳过 |
| Responses 路径未受影响 | ✅ | 全工作区编译通过，Responses 测试通过 |
| 无新增可归因回归 | ✅ | codex-core 失败均为环境性（MCP stdio/shell/图像） |
| **真实 DeepSeek 模型 e2e** | ⏳ **阻塞** | 沙箱网络禁用；需在有网络机器上跑 4 条命令 |
| Skill / MCP 专用 live 测试 | ⏳ | 机制已验证（Function 工具路径），live 测试延后 |
| 基线 worktree 对照 | ⚠️ 未完成 | 磁盘空间不足，无法独立构建基线 |

## 五、阶段性目标（Roadmap）

### 🟢 已完成
- [x] Stage 1：自定义 provider 配置层 + 免登录
- [x] Stage 2A：chat/completions 请求 + 流式文本
- [x] Stage 2B：工具调用 + 错误映射 + 真实 Agent Loop 集成测试
- [x] 离线协议/配置验收（Phase 0–3, 7, 8 可执行部分）

### 🟡 进行中 / 待用户在联网环境完成
- [ ] **Phase 4：真实 DeepSeek 模型 e2e**（决定性验收）。在配置好 `.env` 的联网机器上执行：
  ```bash
  cargo run --manifest-path codex-rs/Cargo.toml -p codex-cli -- exec "只回复：CUSTOM_MODEL_OK"
  # 然后：读 README 第一行（单工具）/ 查找并读 AGENTS.md（多轮）/ 读不存在文件后恢复（错误恢复）
  ```
  预期全部通过即可将验收状态翻转为 **ACCEPTED**。代码侧无遗留阻塞。

### 🔵 后续阶段（未开始）
- [ ] **Phase 5：Skill 专用 live 验收**（最小测试 Skill + 真实模型）。
- [ ] **Phase 6：MCP 专用 live 验收**（最小只读 MCP server + 真实模型）。MCP 工具已是 Function 类型，机制可行。
- [ ] **CTF 解题编排**（原始目标 6）：通过 DeepSeek 模型 + Codex Skills/MCP/内置工具完成 CTF 任务。

## 六、本轮发现并修复的真实缺陷

| 缺陷 | 分类 | 修复 |
|---|---|---|
| `set_llm_only` 无条件 `set_var`，覆盖进程已有 `CODEX_LLM_*` | A 自定义配置 | 改为仅在变量未设置时 set |
| chat 解析器直接发 `OutputTextDelta`，缺消息 item 生命周期（Agent Loop `OutputTextDelta without active item` 断言）| D Chat SSE 解析 | 加入 `OutputItemAdded`→`OutputTextDelta`→`OutputItemDone` 生命周期 |

（另有测试夹具 malformed JSON 问题，非产品缺陷，已用 `serde_json::json!` 修复。）

## 七、已知限制

1. **项目目录 `.env` 信任边界**暂未处理（本轮暂缓）。
2. **Shell/MCP 子进程 API Key 继承隔离**暂未处理（本轮暂缓）。
3. **非 Function Tool 的 Chat 映射不完整**：`apply_patch`(Freeform)/`web_search`/`tool_search`/`namespace` 被跳过（chat 路径下文件编辑需用 shell）。
4. **reasoning 扩展字段**（effort/store/include/text.format 等）在 chat 路径丢弃。
5. **远程 thread-config（proto）**暂不感知 `chat_completions`（本地 `.env` 路径不经过它）。

## 八、关键文件索引

| 关注点 | 路径 |
|---|---|
| `.env` 模板 | `.env.example` |
| `.env` 加载（CODEX_LLM_* 放行） | `codex-rs/arg0/src/lib.rs` |
| 配置层源 + EnvLlm | `codex-rs/config/src/config_layer_source.rs` |
| EnvLlm 层构建 | `codex-rs/config/src/loader/mod.rs` (`build_codex_llm_env_layer`) |
| WireApi 变体 | `codex-rs/model-provider-info/src/lib.rs` |
| chat 请求转换 | `codex-rs/codex-api/src/requests/chat.rs` |
| chat SSE 解析 | `codex-rs/codex-api/src/sse/chat.rs` |
| chat endpoint client | `codex-rs/codex-api/src/endpoint/chat.rs` |
| wire_api 分发 | `codex-rs/core/src/client.rs` (`stream_chat_completions_api`) |
| chat 工具转换 | `codex-rs/tools/src/tool_spec.rs` (`create_tools_json_for_chat_completions_api`) |
| 真实 Agent Loop 集成测试 | `codex-rs/core/tests/suite/chat_completions_loop.rs` |
| config schema | `codex-rs/core/config.schema.json` |

## 九、本地验证命令

```bash
# 单元/协议测试（无需网络）
just test -p codex-api -p codex-tools -p codex-config -p codex-arg0 -p codex-model-provider-info

# 真实 Agent Loop 集成测试（wiremock，仅需 loopback）
just test -p codex-core chat_completions_

# 格式化 / clippy（codex-rs 目录）
just fmt
just fix -p codex-api -p codex-core -p codex-config

# 真实模型 e2e（需网络 + .env）
cargo run --manifest-path codex-rs/Cargo.toml -p codex-cli -- exec "只回复：CUSTOM_MODEL_OK"
```

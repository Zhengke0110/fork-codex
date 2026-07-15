//! Acceptance: drives the real Codex Agent Loop over a Chat Completions
//! (`wire_api = chat_completions`) model endpoint backed by a local mock,
//! covering the same tool-call roundtrip shape as the Responses path. These
//! tests exercise the genuine Session/Turn/tool-execution loop; they are not
//! unit tests of the parser. They require loopback network and real command
//! execution, so they skip under a network-disabled Codex sandbox.

use codex_model_provider_info::WireApi;
use core_test_support::skip_if_no_network;
use core_test_support::test_codex::TestCodexBuilder;
use core_test_support::test_codex::TestCodexHarness;
use core_test_support::test_codex::test_codex;
use serde_json::Value;
use serde_json::json;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use wiremock::Mock;
use wiremock::MockServer;
use wiremock::Respond;
use wiremock::ResponseTemplate;
use wiremock::matchers::method;
use wiremock::matchers::path_regex;

/// One Chat Completions SSE `data:` line for a streamed text delta.
fn chunk_text(id: &str, text: &str) -> String {
    serde_json::json!({
        "id": id,
        "choices": [{"index": 0, "delta": {"content": text}}]
    })
    .to_string()
}

/// One Chat Completions SSE `data:` line that starts a tool call and signals
/// the end of the turn for the assistant (`finish_reason = tool_calls`).
fn chunk_tool_call(id: &str, call_id: &str, name: &str, arguments: &str) -> String {
    serde_json::json!({
        "id": id,
        "choices": [{
            "index": 0,
            "delta": {
                "tool_calls": [{
                    "index": 0,
                    "id": call_id,
                    "type": "function",
                    "function": {"name": name, "arguments": arguments}
                }]
            }
        }]
    })
    .to_string()
}

fn chunk_finish(id: &str, reason: &str) -> String {
    serde_json::json!({
        "id": id,
        "choices": [{"index": 0, "delta": {}, "finish_reason": reason}]
    })
    .to_string()
}

/// Assemble Chat Completions SSE chunks into a wire body terminated by [DONE].
fn chat_sse(chunks: &[String]) -> String {
    let mut body = String::new();
    for chunk in chunks {
        body.push_str("data: ");
        body.push_str(chunk);
        body.push_str("\n\n");
    }
    body.push_str("data: [DONE]\n\n");
    body
}

struct SeqResponder {
    num_calls: AtomicUsize,
    responses: Vec<String>,
}

impl Respond for SeqResponder {
    fn respond(&self, _: &wiremock::Request) -> ResponseTemplate {
        let call_num = self.num_calls.fetch_add(1, Ordering::SeqCst);
        let body = self
            .responses
            .get(call_num)
            .unwrap_or_else(|| panic!("no chat completion response for call {call_num}"));
        ResponseTemplate::new(200)
            .insert_header("content-type", "text/event-stream")
            .set_body_string(body.clone())
    }
}

/// Mount a sequenced set of Chat Completions SSE bodies: request N receives
/// `bodies[N]`. Matches the `/chat/completions` path so it does not collide
/// with the Responses mock used by the rest of the harness.
async fn mount_chat_sse_sequence(server: &MockServer, bodies: Vec<String>) {
    let num_calls = bodies.len();
    Mock::given(method("POST"))
        .and(path_regex(".*/chat/completions$"))
        .respond_with(SeqResponder {
            num_calls: AtomicUsize::new(0),
            responses: bodies,
        })
        .up_to_n_times(num_calls as u64)
        .expect(num_calls as u64)
        .mount(server)
        .await;
}

/// Builder that flips the resolved model provider onto the Chat Completions
/// wire protocol while keeping the harness-provided base URL and API key auth.
fn chat_completions_builder() -> TestCodexBuilder {
    test_codex().with_model("gpt-5.4").with_config(|config| {
        config.model_provider.wire_api = WireApi::ChatCompletions;
    })
}

/// Phase 3, Test A: a single tool-call roundtrip over the Chat Completions wire
/// protocol. The model (mock) requests a shell command; Codex executes it; the
/// tool result is sent back as a `role: tool` message in the second request;
/// the model then produces a final answer.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn chat_completions_single_tool_roundtrip() -> anyhow::Result<()> {
    skip_if_no_network!(Ok(()));

    let harness = TestCodexHarness::with_builder(chat_completions_builder()).await?;
    let call_id = "chat-shell-call";

    let arguments = serde_json::to_string(&json!({
        "command": "echo roundtrip-ok",
        "timeout_ms": 2_000,
        "login": Value::Null,
    }))
    .expect("serialize shell arguments");

    let first_turn = chat_sse(&[
        chunk_tool_call("resp-1", call_id, "shell_command", &arguments),
        chunk_finish("resp-1", "tool_calls"),
    ]);
    let second_turn = chat_sse(&[
        chunk_text("resp-2", "done: "),
        chunk_text("resp-2", "roundtrip-ok"),
        chunk_finish("resp-2", "stop"),
    ]);
    mount_chat_sse_sequence(harness.server(), vec![first_turn, second_turn]).await;

    harness.submit("run the echo command").await?;

    // The harness's request-capture helpers are Responses-specific, so inspect
    // the captured Chat Completions requests directly. Two turns are expected:
    // the assistant tool call, then the final answer after the tool result.
    let chat_requests: Vec<Value> = harness
        .server()
        .received_requests()
        .await
        .expect("mock server should expose received requests")
        .into_iter()
        .filter(|request| request.url.path().ends_with("/chat/completions"))
        .map(|request| {
            request
                .body_json::<Value>()
                .expect("chat request body is JSON")
        })
        .collect();
    assert_eq!(
        chat_requests.len(),
        2,
        "expected two chat/completions requests (tool call + follow-up)"
    );
    let messages = chat_requests[1]
        .get("messages")
        .and_then(|value| value.as_array())
        .expect("second request has a messages array");
    let tool_result = messages
        .iter()
        .find(|message| {
            message.get("role").and_then(|v| v.as_str()) == Some("tool")
                && message.get("tool_call_id").and_then(|v| v.as_str()) == Some(call_id)
        })
        .expect("second request carries the tool result as role:tool");
    let content = tool_result
        .get("content")
        .and_then(|v| v.as_str())
        .expect("tool result content");
    assert!(
        content.contains("roundtrip-ok"),
        "tool result should contain roundtrip-ok, got: {content}"
    );

    Ok(())
}

/// Phase 3, Test B (text-only): verifies the Chat Completions wire path flows
/// through the real Agent Loop for a plain streaming answer (no tool), so the
/// turn-handling integration is exercised even where tool execution is not.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn chat_completions_plain_text_turn() -> anyhow::Result<()> {
    skip_if_no_network!(Ok(()));

    let harness = TestCodexHarness::with_builder(chat_completions_builder()).await?;
    let turn = chat_sse(&[
        chunk_text("resp-1", "CUSTOM_MODEL_OK"),
        chunk_finish("resp-1", "stop"),
    ]);
    mount_chat_sse_sequence(harness.server(), vec![turn]).await;

    harness.submit("reply with CUSTOM_MODEL_OK").await?;
    // A successful turn completion (no error, no panic) over the chat wire path
    // is the integration signal; the assistant text is streamed via the same
    // ResponseEvent::OutputTextDelta path unit-tested in codex-api.
    Ok(())
}

//! Server-Sent Events parsing for the Chat Completions API.
//!
//! Chat Completions streams a sequence of JSON chunks (one per `data:` line)
//! terminated by a `data: [DONE]` sentinel. This module parses those chunks and
//! emits the same [`ResponseEvent`] types the Codex core loop already consumes
//! for the Responses API, so downstream turn handling is shared.

use crate::common::ResponseEvent;
use crate::common::ResponseStream;
use crate::error::ApiError;
use crate::telemetry::SseTelemetry;
use codex_client::ByteStream;
use codex_client::StreamResponse;
use codex_protocol::ResponseItemId;
use codex_protocol::models::ContentItem;
use codex_protocol::models::ResponseItem;
use codex_protocol::protocol::TokenUsage;
use eventsource_stream::Eventsource;
use futures::StreamExt;
use serde::Deserialize;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::OnceLock;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::time::Instant;
use tokio::time::timeout;
use tracing::debug;
use tracing::trace;

const REQUEST_ID_HEADER: &str = "x-request-id";
const TURN_STATE_HEADER: &str = "x-codex-turn-state";
const DONE_SENTINEL: &str = "[DONE]";

/// Spawn a background task that parses a Chat Completions SSE response into the
/// shared [`ResponseEvent`] stream. Mirrors `spawn_response_stream` for the
/// Responses API.
pub fn spawn_chat_completions_stream(
    stream_response: StreamResponse,
    idle_timeout: Duration,
    telemetry: Option<Arc<dyn SseTelemetry>>,
    turn_state: Option<Arc<OnceLock<String>>>,
) -> ResponseStream {
    let upstream_request_id = stream_response
        .headers
        .get(REQUEST_ID_HEADER)
        .and_then(|value| value.to_str().ok())
        .map(str::to_string);
    if let Some(turn_state) = turn_state.as_ref()
        && let Some(header_value) = stream_response
            .headers
            .get(TURN_STATE_HEADER)
            .and_then(|value| value.to_str().ok())
    {
        let _ = turn_state.set(header_value.to_string());
    }

    let (tx_event, rx_event) = mpsc::channel::<Result<ResponseEvent, ApiError>>(1600);
    tokio::spawn(async move {
        process_chat_completions_sse(stream_response.bytes, tx_event, idle_timeout, telemetry)
            .await;
    });

    ResponseStream {
        rx_event,
        upstream_request_id,
    }
}

async fn process_chat_completions_sse(
    stream: ByteStream,
    tx_event: mpsc::Sender<Result<ResponseEvent, ApiError>>,
    idle_timeout: Duration,
    telemetry: Option<Arc<dyn SseTelemetry>>,
) {
    let mut stream = stream.eventsource();
    let mut response_id = String::new();
    let mut tool_calls: HashMap<i64, ToolCallAccumulator> = HashMap::new();
    let mut last_usage: Option<ChatUsage> = None;
    let mut assistant_message_id: Option<ResponseItemId> = None;
    let mut assistant_text = String::new();

    loop {
        let start = Instant::now();
        let response = timeout(idle_timeout, stream.next()).await;
        if let Some(t) = telemetry.as_ref() {
            t.on_sse_poll(&response, start.elapsed());
        }
        let sse = match response {
            Ok(Some(Ok(sse))) => sse,
            Ok(Some(Err(e))) => {
                debug!("Chat completions SSE error: {e:#}");
                let _ = tx_event.send(Err(ApiError::Stream(e.to_string()))).await;
                return;
            }
            // Stream ended without an explicit terminal event; finalize the turn.
            Ok(None) => {
                emit_final_events(
                    &tx_event,
                    &response_id,
                    &mut tool_calls,
                    last_usage.take(),
                    assistant_message_id
                        .as_ref()
                        .map(|id| (id, assistant_text.as_str())),
                )
                .await;
                return;
            }
            Err(_) => {
                let _ = tx_event
                    .send(Err(ApiError::Stream("idle timeout waiting for SSE".into())))
                    .await;
                return;
            }
        };

        trace!("Chat completions SSE event: {}", &sse.data);

        if sse.data.trim() == DONE_SENTINEL {
            emit_final_events(
                &tx_event,
                &response_id,
                &mut tool_calls,
                last_usage.take(),
                assistant_message_id
                    .as_ref()
                    .map(|id| (id, assistant_text.as_str())),
            )
            .await;
            return;
        }

        let chunk: ChatCompletionChunk = match serde_json::from_str(&sse.data) {
            Ok(chunk) => chunk,
            Err(e) => {
                debug!(
                    "Failed to parse chat completions chunk: {e}, data: {}",
                    &sse.data
                );
                continue;
            }
        };

        if let Some(id) = chunk.id.filter(|id| !id.is_empty()) {
            response_id = id;
        }
        if let Some(usage) = chunk.usage {
            last_usage = Some(usage);
        }

        let mut finish_reason: Option<String> = None;
        for choice in chunk.choices.unwrap_or_default() {
            let choice_finish = choice.finish_reason;
            if let Some(delta) = choice.delta {
                if let Some(content) = delta.content.filter(|c| !c.is_empty()) {
                    if assistant_message_id.is_none() {
                        let id = ResponseItemId::from_server("chat_msg".to_string());
                        assistant_message_id = Some(id.clone());
                        if tx_event
                            .send(Ok(ResponseEvent::OutputItemAdded(ResponseItem::Message {
                                id: Some(id),
                                role: "assistant".to_string(),
                                content: Vec::new(),
                                phase: None,
                                internal_chat_message_metadata_passthrough: None,
                            })))
                            .await
                            .is_err()
                        {
                            return;
                        }
                    }
                    assistant_text.push_str(&content);
                    if tx_event
                        .send(Ok(ResponseEvent::OutputTextDelta(content)))
                        .await
                        .is_err()
                    {
                        return;
                    }
                }
                if let Some(deltas) = delta.tool_calls {
                    for delta in deltas {
                        if !merge_tool_call_delta(&mut tool_calls, &tx_event, delta).await {
                            return;
                        }
                    }
                }
            }
            if choice_finish.is_some() {
                finish_reason = choice_finish;
            }
        }

        match finish_reason.as_deref() {
            Some("stop") => {
                // The model finished its answer without requesting tools.
                tool_calls.clear();
                emit_final_events(
                    &tx_event,
                    &response_id,
                    &mut tool_calls,
                    last_usage.take(),
                    assistant_message_id
                        .as_ref()
                        .map(|id| (id, assistant_text.as_str())),
                )
                .await;
                return;
            }
            Some("tool_calls") => {
                emit_final_events(
                    &tx_event,
                    &response_id,
                    &mut tool_calls,
                    last_usage.take(),
                    assistant_message_id
                        .as_ref()
                        .map(|id| (id, assistant_text.as_str())),
                )
                .await;
                return;
            }
            Some("length") => {
                let _ = tx_event.send(Err(ApiError::ContextWindowExceeded)).await;
                return;
            }
            _ => {}
        }
    }
}

/// Merge one streamed tool-call delta into the accumulator, emitting an
/// incremental [`ResponseEvent::ToolCallInputDelta`] for argument fragments.
/// Returns `false` if the event receiver is gone and the stream should stop.
async fn merge_tool_call_delta(
    tool_calls: &mut HashMap<i64, ToolCallAccumulator>,
    tx_event: &mpsc::Sender<Result<ResponseEvent, ApiError>>,
    delta: ChatToolCallDelta,
) -> bool {
    let accum = tool_calls
        .entry(delta.index)
        .or_insert_with(|| ToolCallAccumulator::new(format!("chat_tool_{}", delta.index)));
    if delta.id.is_some() {
        accum.call_id = delta.id;
    }
    if let Some(function) = delta.function {
        if function.name.is_some() {
            accum.name = function.name;
        }
        if let Some(arguments) = function.arguments.filter(|a| !a.is_empty()) {
            if tx_event
                .send(Ok(ResponseEvent::ToolCallInputDelta {
                    item_id: accum.item_id.clone(),
                    call_id: accum.call_id.clone(),
                    delta: arguments.clone(),
                }))
                .await
                .is_err()
            {
                return false;
            }
            accum.arguments.push_str(&arguments);
        }
    }
    true
}

/// Emit any accumulated tool calls as `FunctionCall` items followed by a
/// `Completed` event. When tool calls are present the model is awaiting tool
/// results, so `end_turn` is `false`; otherwise the turn is complete.
async fn emit_final_events(
    tx_event: &mpsc::Sender<Result<ResponseEvent, ApiError>>,
    response_id: &str,
    tool_calls: &mut HashMap<i64, ToolCallAccumulator>,
    usage: Option<ChatUsage>,
    assistant: Option<(&ResponseItemId, &str)>,
) {
    let events = build_final_events(response_id, tool_calls, usage, assistant);
    for event in events {
        let is_completed = matches!(event, ResponseEvent::Completed { .. });
        if tx_event.send(Ok(event)).await.is_err() {
            return;
        }
        if is_completed {
            return;
        }
    }
}

fn build_final_events(
    response_id: &str,
    tool_calls: &mut HashMap<i64, ToolCallAccumulator>,
    usage: Option<ChatUsage>,
    assistant: Option<(&ResponseItemId, &str)>,
) -> Vec<ResponseEvent> {
    let mut events = Vec::new();
    // Finalize the streamed assistant message first (assistant text precedes
    // any tool calls in the same turn).
    if let Some((id, text)) = assistant.filter(|(_, text)| !text.is_empty()) {
        events.push(ResponseEvent::OutputItemDone(ResponseItem::Message {
            id: Some(id.clone()),
            role: "assistant".to_string(),
            content: vec![ContentItem::OutputText {
                text: text.to_string(),
            }],
            phase: None,
            internal_chat_message_metadata_passthrough: None,
        }));
    }
    let mut indices: Vec<i64> = tool_calls.keys().copied().collect();
    indices.sort_unstable();
    let mut tool_call_count = 0;
    for index in indices {
        let Some(accum) = tool_calls.remove(&index) else {
            continue;
        };
        let Some(name) = accum.name else {
            continue;
        };
        let call_id = accum.call_id.unwrap_or_else(|| format!("call_{index}"));
        events.push(ResponseEvent::OutputItemDone(ResponseItem::FunctionCall {
            id: None,
            name,
            namespace: None,
            arguments: accum.arguments,
            call_id,
            internal_chat_message_metadata_passthrough: None,
        }));
        tool_call_count += 1;
    }
    events.push(ResponseEvent::Completed {
        response_id: response_id.to_string(),
        token_usage: build_token_usage(usage),
        // `stop` (no tool calls) ends the turn; `tool_calls` leaves it open so
        // the conversation loop executes the tools and follows up.
        end_turn: Some(tool_call_count == 0),
    });
    events
}

/// Map a Chat Completions `usage` block to Codex's [`TokenUsage`]. Chat
/// completions does not report cached or reasoning tokens, so those are zero.
fn build_token_usage(usage: Option<ChatUsage>) -> Option<TokenUsage> {
    let usage = usage?;
    Some(TokenUsage {
        input_tokens: usage.prompt_tokens.unwrap_or(0),
        cached_input_tokens: 0,
        output_tokens: usage.completion_tokens.unwrap_or(0),
        reasoning_output_tokens: 0,
        total_tokens: usage.total_tokens.unwrap_or(0),
    })
}

#[derive(Debug, Deserialize)]
struct ChatCompletionChunk {
    #[serde(default)]
    id: Option<String>,
    #[serde(default)]
    choices: Option<Vec<ChatChoice>>,
    #[serde(default)]
    usage: Option<ChatUsage>,
}

#[derive(Debug, Clone, Deserialize)]
struct ChatUsage {
    #[serde(default)]
    prompt_tokens: Option<i64>,
    #[serde(default)]
    completion_tokens: Option<i64>,
    #[serde(default)]
    total_tokens: Option<i64>,
}

#[derive(Debug, Deserialize)]
struct ChatChoice {
    #[serde(default)]
    delta: Option<ChatDelta>,
    #[serde(default)]
    finish_reason: Option<String>,
}

#[derive(Debug, Deserialize)]
struct ChatDelta {
    #[serde(default)]
    content: Option<String>,
    #[serde(default)]
    tool_calls: Option<Vec<ChatToolCallDelta>>,
}

#[derive(Debug, Deserialize)]
struct ChatToolCallDelta {
    index: i64,
    #[serde(default)]
    id: Option<String>,
    #[serde(default)]
    function: Option<ChatToolCallFunction>,
}

#[derive(Debug, Deserialize)]
struct ChatToolCallFunction {
    #[serde(default)]
    name: Option<String>,
    #[serde(default)]
    arguments: Option<String>,
}

#[derive(Debug)]
struct ToolCallAccumulator {
    item_id: String,
    call_id: Option<String>,
    name: Option<String>,
    arguments: String,
}

impl ToolCallAccumulator {
    fn new(item_id: String) -> Self {
        Self {
            item_id,
            call_id: None,
            name: None,
            arguments: String::new(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn final_events_emit_function_calls_then_incomplete_turn() {
        let mut tool_calls = HashMap::new();
        tool_calls.insert(
            0,
            ToolCallAccumulator {
                item_id: "chat_tool_0".to_string(),
                call_id: Some("call_a".to_string()),
                name: Some("foo".to_string()),
                arguments: "{\"x\":1}".to_string(),
            },
        );
        tool_calls.insert(
            1,
            ToolCallAccumulator {
                item_id: "chat_tool_1".to_string(),
                call_id: Some("call_b".to_string()),
                name: Some("bar".to_string()),
                arguments: "{}".to_string(),
            },
        );
        let events = build_final_events("resp_1", &mut tool_calls, None, None);
        // Two function calls (sorted by index) followed by a non-terminal Completed.
        assert_eq!(events.len(), 3);
        assert!(matches!(
            events[0],
            ResponseEvent::OutputItemDone(ResponseItem::FunctionCall { ref name, .. }) if name == "foo"
        ));
        assert!(matches!(
            events[1],
            ResponseEvent::OutputItemDone(ResponseItem::FunctionCall { ref name, .. }) if name == "bar"
        ));
        assert!(matches!(
            events[2],
            ResponseEvent::Completed {
                end_turn: Some(false),
                ..
            }
        ));
    }

    #[test]
    fn final_events_without_tool_calls_completes_turn() {
        let mut tool_calls = HashMap::new();
        let events = build_final_events("resp_2", &mut tool_calls, None, None);
        assert_eq!(events.len(), 1);
        assert!(matches!(
            events[0],
            ResponseEvent::Completed {
                end_turn: Some(true),
                token_usage: None,
                ..
            }
        ));
    }

    #[tokio::test]
    async fn parses_streaming_text_and_tool_call_roundtrip() {
        use bytes::Bytes;
        use codex_client::StreamResponse;
        use codex_client::TransportError;
        use futures::StreamExt;
        use futures::stream;
        use http::HeaderMap;
        use http::StatusCode;

        // A captured-style Chat Completions SSE stream: text deltas, a tool
        // call (arguments split across two chunks), then [DONE].
        let sse = [
            r#"data: {"id":"chatcmpl-1","choices":[{"index":0,"delta":{"role":"assistant","content":"Hello"}}]}"#,
            r#"data: {"id":"chatcmpl-1","choices":[{"index":0,"delta":{"content":" world"}}]}"#,
            r#"data: {"id":"chatcmpl-1","choices":[{"index":0,"delta":{"tool_calls":[{"index":0,"id":"call_42","function":{"name":"echo","arguments":"{\"msg\":"}}]}}]}"#,
            r#"data: {"id":"chatcmpl-1","choices":[{"index":0,"delta":{"tool_calls":[{"index":0,"function":{"arguments":"\"hi\"}"}}]},"finish_reason":"tool_calls"}]}"#,
            "data: [DONE]",
        ]
        .join("\n\n");

        let bytes_stream =
            stream::iter(vec![Ok::<Bytes, TransportError>(Bytes::from(sse))]).boxed();
        let stream_response = StreamResponse {
            status: StatusCode::OK,
            headers: HeaderMap::new(),
            bytes: bytes_stream,
        };

        let mut response_stream =
            spawn_chat_completions_stream(stream_response, Duration::from_secs(5), None, None);

        let mut text = String::new();
        let mut saw_function_call = false;
        let mut saw_completed = false;
        while let Some(event) = response_stream.rx_event.recv().await {
            match event {
                Ok(ResponseEvent::OutputTextDelta(delta)) => text.push_str(&delta),
                Ok(ResponseEvent::OutputItemDone(ResponseItem::FunctionCall {
                    name,
                    arguments,
                    call_id,
                    ..
                })) => {
                    saw_function_call = true;
                    assert_eq!(name, "echo");
                    assert_eq!(arguments, "{\"msg\":\"hi\"}");
                    assert_eq!(call_id, "call_42");
                }
                Ok(ResponseEvent::Completed {
                    end_turn: Some(false),
                    ..
                }) => saw_completed = true,
                Ok(_) => {}
                Err(e) => panic!("unexpected error event: {e:?}"),
            }
        }

        assert_eq!(text, "Hello world");
        assert!(saw_function_call, "expected a streamed function call");
        assert!(
            saw_completed,
            "expected a non-terminal Completed after tool calls"
        );
    }

    /// Drive the Chat Completions SSE parser end-to-end with a synthesized
    /// SSE body (no network) and collect every emitted event.
    async fn parse_sse(sse_body: &str) -> Vec<Result<ResponseEvent, ApiError>> {
        use bytes::Bytes;
        use codex_client::StreamResponse;
        use codex_client::TransportError;
        use futures::StreamExt;
        use futures::stream;
        use http::HeaderMap;
        use http::StatusCode;

        let bytes_stream = stream::iter(vec![Ok::<Bytes, TransportError>(Bytes::from(
            sse_body.to_string(),
        ))])
        .boxed();
        let stream_response = StreamResponse {
            status: StatusCode::OK,
            headers: HeaderMap::new(),
            bytes: bytes_stream,
        };
        let mut response_stream =
            spawn_chat_completions_stream(stream_response, Duration::from_secs(5), None, None);
        let mut events = Vec::new();
        while let Some(event) = response_stream.rx_event.recv().await {
            events.push(event);
        }
        events
    }

    fn collect_text(events: &[Result<ResponseEvent, ApiError>]) -> String {
        let mut text = String::new();
        for event in events {
            if let Ok(ResponseEvent::OutputTextDelta(delta)) = event {
                text.push_str(delta);
            }
        }
        text
    }

    #[tokio::test]
    async fn streaming_text_concatenates_chinese_without_loss() {
        let body = [
            r#"data: {"id":"c1","choices":[{"index":0,"delta":{"content":"你"}}]}"#,
            r#"data: {"id":"c1","choices":[{"index":0,"delta":{"content":"好"}}]}"#,
            r#"data: {"id":"c1","choices":[{"index":0,"delta":{}}]}"#,
            r#"data: {"id":"c1","choices":[{"index":0,"delta":{},"finish_reason":"stop"}]}"#,
            "data: [DONE]",
        ]
        .join("\n\n");
        let events = parse_sse(&body).await;
        assert_eq!(collect_text(&events), "你好");
        assert!(events.iter().any(|e| matches!(
            e,
            Ok(ResponseEvent::Completed {
                end_turn: Some(true),
                ..
            })
        )));
    }

    #[tokio::test]
    async fn multiple_tool_calls_in_one_turn_stay_separate() {
        let body = [
            r#"data: {"id":"c1","choices":[{"index":0,"delta":{"tool_calls":[{"index":0,"id":"call_a","function":{"name":"read","arguments":"{\"p\":\"A\"}"}}]}}]}"#,
            r#"data: {"id":"c1","choices":[{"index":0,"delta":{"tool_calls":[{"index":1,"id":"call_b","function":{"name":"read","arguments":"{\"p\":\"B\"}"}}]}}]}"#,
            r#"data: {"id":"c1","choices":[{"index":0,"delta":{},"finish_reason":"tool_calls"}]}"#,
            "data: [DONE]",
        ]
        .join("\n\n");
        let events = parse_sse(&body).await;
        let calls: Vec<(String, String, String)> = events
            .iter()
            .filter_map(|e| match e {
                Ok(ResponseEvent::OutputItemDone(ResponseItem::FunctionCall {
                    name,
                    arguments,
                    call_id,
                    ..
                })) => Some((name.clone(), arguments.clone(), call_id.clone())),
                _ => None,
            })
            .collect();
        assert_eq!(calls.len(), 2, "expected two separate tool calls");
        assert_eq!(calls[0].2, "call_a");
        assert_eq!(calls[1].2, "call_b");
        assert_ne!(calls[0].2, calls[1].2, "tool call ids must not cross");
        assert_eq!(calls[0].0, "read");
        assert_eq!(calls[1].0, "read");
        assert_eq!(calls[0].1, "{\"p\":\"A\"}");
        assert_eq!(calls[1].1, "{\"p\":\"B\"}");
    }

    #[tokio::test]
    async fn usage_maps_when_present_and_absent_is_none() {
        let with_usage = [
            r#"data: {"id":"c1","choices":[{"index":0,"delta":{"content":"hi"},"finish_reason":"stop"}],"usage":{"prompt_tokens":5,"completion_tokens":2,"total_tokens":7}}"#,
            "data: [DONE]",
        ]
        .join("\n\n");
        let events = parse_sse(&with_usage).await;
        let usage = events
            .iter()
            .find_map(|e| match e {
                Ok(ResponseEvent::Completed { token_usage, .. }) => token_usage.clone(),
                _ => None,
            })
            .expect("token usage present");
        assert_eq!(usage.input_tokens, 5);
        assert_eq!(usage.output_tokens, 2);
        assert_eq!(usage.total_tokens, 7);

        let no_usage = [
            r#"data: {"id":"c1","choices":[{"index":0,"delta":{"content":"hi"},"finish_reason":"stop"}]}"#,
            "data: [DONE]",
        ]
        .join("\n\n");
        let events = parse_sse(&no_usage).await;
        let token_usage = events.iter().find_map(|e| match e {
            Ok(ResponseEvent::Completed { token_usage, .. }) => token_usage.clone(),
            _ => None,
        });
        assert!(
            token_usage.is_none(),
            "missing usage must not panic and must stay None"
        );
    }

    #[tokio::test]
    async fn finish_reason_length_is_context_window_exceeded() {
        let body = [
            r#"data: {"id":"c1","choices":[{"index":0,"delta":{"content":"x"},"finish_reason":"length"}]}"#,
            "data: [DONE]",
        ]
        .join("\n\n");
        let events = parse_sse(&body).await;
        assert!(
            events
                .iter()
                .any(|e| matches!(e, Err(ApiError::ContextWindowExceeded))),
            "length must map to ContextWindowExceeded; events: {events:?}"
        );
    }

    #[tokio::test]
    async fn unknown_finish_reason_is_safe_and_finalizes() {
        // content_filter and unknown strings must not panic; the turn still
        // finalizes on [DONE].
        let body = [
            r#"data: {"id":"c1","choices":[{"index":0,"delta":{"content":"x"},"finish_reason":"content_filter"}]}"#,
            r#"data: {"id":"c1","choices":[{"index":0,"delta":{}}]}"#,
            "data: [DONE]",
        ]
        .join("\n\n");
        let events = parse_sse(&body).await;
        assert!(
            !events.iter().any(std::result::Result::is_err),
            "unknown finish_reason must not produce an error: {events:?}"
        );
        assert!(
            events
                .iter()
                .any(|e| matches!(e, Ok(ResponseEvent::Completed { .. }))),
            "turn must still finalize: {events:?}"
        );
    }

    #[tokio::test]
    async fn malformed_chunk_is_skipped_without_panicking() {
        let body = [
            "data: not-json-at-all",
            r#"data: {"id":"c1","choices":[{"index":0,"delta":{"content":"ok"},"finish_reason":"stop"}]}"#,
            "data: [DONE]",
        ]
        .join("\n\n");
        let events = parse_sse(&body).await;
        assert_eq!(collect_text(&events), "ok");
    }

    #[tokio::test]
    async fn only_done_sentinel_and_empty_stream_finalize_cleanly() {
        let events = parse_sse("data: [DONE]").await;
        assert!(
            events.iter().any(|e| matches!(
                e,
                Ok(ResponseEvent::Completed {
                    end_turn: Some(true),
                    ..
                })
            )),
            "only-[DONE] must cleanly complete: {events:?}"
        );

        let events = parse_sse("").await;
        assert!(
            events
                .iter()
                .any(|e| matches!(e, Ok(ResponseEvent::Completed { .. }))),
            "empty stream must finalize without panic: {events:?}"
        );
    }
}

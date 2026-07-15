//! Request-body construction for the Chat Completions API.
//!
//! Codex's internal conversation model is built on Responses API
//! [`ResponseItem`]s. This module flattens that item list into the flat
//! `messages` array expected by OpenAI-compatible Chat Completions endpoints,
//! mapping function calls and their outputs to Chat Completions tool messages.

use codex_protocol::models::ContentItem;
use codex_protocol::models::ResponseItem;
use serde_json::Value;
use serde_json::json;

/// Build a Chat Completions streaming request body from a Codex conversation.
///
/// `instructions` becomes a leading `system` message when non-empty. Consecutive
/// function/custom tool calls are grouped into a single assistant message with a
/// `tool_calls` array; their outputs become `role: "tool"` messages. Reasoning,
/// compaction, web-search, image-generation, local-shell, and tool-search items
/// have no Chat Completions equivalent and are dropped on this wire path.
///
/// `tools` must already be in Chat Completions format (see
/// [`codex_tools::create_tools_json_for_chat_completions_api`]).
pub fn build_chat_completions_body(
    model: &str,
    instructions: &str,
    input: &[ResponseItem],
    tools: &[Value],
    parallel_tool_calls: bool,
) -> Value {
    let mut messages = Vec::<Value>::new();
    if !instructions.is_empty() {
        messages.push(json!({"role": "system", "content": instructions}));
    }

    let mut pending_tool_calls: Vec<Value> = Vec::new();
    for item in input {
        match item {
            ResponseItem::Message { role, content, .. } => {
                flush_tool_calls(&mut messages, &mut pending_tool_calls);
                messages.push(message_to_chat(role, content));
            }
            ResponseItem::FunctionCall {
                name,
                arguments,
                call_id,
                ..
            } => {
                push_tool_call(&mut pending_tool_calls, call_id, name, arguments);
            }
            ResponseItem::CustomToolCall {
                name,
                call_id,
                input: arguments,
                ..
            } => {
                push_tool_call(&mut pending_tool_calls, call_id, name, arguments);
            }
            ResponseItem::FunctionCallOutput {
                call_id, output, ..
            }
            | ResponseItem::CustomToolCallOutput {
                call_id, output, ..
            } => {
                flush_tool_calls(&mut messages, &mut pending_tool_calls);
                let content = output.body.to_text().unwrap_or_default();
                messages.push(json!({
                    "role": "tool",
                    "tool_call_id": call_id,
                    "content": content,
                }));
            }
            // Reasoning, compaction, web search, image generation, local shell,
            // tool search, and agent messages have no Chat Completions
            // equivalent and are dropped on this wire path.
            _ => {}
        }
    }
    flush_tool_calls(&mut messages, &mut pending_tool_calls);

    let mut body = json!({
        "model": model,
        "messages": messages,
        "stream": true,
        "stream_options": {"include_usage": true},
    });
    if !tools.is_empty() {
        body["tools"] = json!(tools);
        body["tool_choice"] = json!("auto");
        body["parallel_tool_calls"] = json!(parallel_tool_calls);
    }
    body
}

fn message_to_chat(role: &str, content: &[ContentItem]) -> Value {
    let mut text = String::new();
    let mut parts: Vec<Value> = Vec::new();
    let mut has_image = false;
    for item in content {
        match item {
            ContentItem::InputText { text: segment }
            | ContentItem::OutputText { text: segment } => {
                text.push_str(segment);
                parts.push(json!({"type": "text", "text": segment}));
            }
            ContentItem::InputImage { image_url, .. } => {
                has_image = true;
                parts.push(json!({"type": "image_url", "image_url": {"url": image_url}}));
            }
        }
    }
    let content_value = if has_image {
        Value::Array(parts)
    } else {
        Value::String(text)
    };
    json!({"role": role, "content": content_value})
}

fn push_tool_call(pending: &mut Vec<Value>, call_id: &str, name: &str, arguments: &str) {
    pending.push(json!({
        "id": call_id,
        "type": "function",
        "function": {"name": name, "arguments": arguments},
    }));
}

fn flush_tool_calls(messages: &mut Vec<Value>, pending: &mut Vec<Value>) {
    if pending.is_empty() {
        return;
    }
    let tool_calls = std::mem::take(pending);
    messages.push(json!({"role": "assistant", "tool_calls": tool_calls}));
}

#[cfg(test)]
mod tests {
    use super::*;
    use codex_protocol::models::FunctionCallOutputBody;
    use codex_protocol::models::FunctionCallOutputPayload;

    fn user(text: &str) -> ResponseItem {
        ResponseItem::Message {
            id: None,
            role: "user".to_string(),
            content: vec![ContentItem::InputText {
                text: text.to_string(),
            }],
            phase: None,
            internal_chat_message_metadata_passthrough: None,
        }
    }

    fn assistant(text: &str) -> ResponseItem {
        ResponseItem::Message {
            id: None,
            role: "assistant".to_string(),
            content: vec![ContentItem::OutputText {
                text: text.to_string(),
            }],
            phase: None,
            internal_chat_message_metadata_passthrough: None,
        }
    }

    #[test]
    fn builds_messages_with_tool_roundtrip() {
        let input = vec![
            user("hello"),
            ResponseItem::FunctionCall {
                id: None,
                name: "get_weather".to_string(),
                namespace: None,
                arguments: r#"{"city":"SF"}"#.to_string(),
                call_id: "call_1".to_string(),
                internal_chat_message_metadata_passthrough: None,
            },
            ResponseItem::FunctionCallOutput {
                id: None,
                call_id: "call_1".to_string(),
                output: FunctionCallOutputPayload {
                    body: FunctionCallOutputBody::Text("sunny".to_string()),
                    success: Some(true),
                },
                internal_chat_message_metadata_passthrough: None,
            },
            assistant("It is sunny."),
        ];
        let tools = vec![serde_json::json!({
            "type": "function",
            "function": {"name": "get_weather", "parameters": {}}
        })];
        let body =
            build_chat_completions_body("deepseek-v4-pro", "Be helpful.", &input, &tools, true);

        let messages = body
            .get("messages")
            .and_then(|m| m.as_array())
            .expect("messages is an array");
        // system + user + assistant(tool_calls) + tool + assistant(text)
        assert_eq!(messages.len(), 5);
        assert_eq!(
            messages[0].get("role").and_then(|v| v.as_str()),
            Some("system")
        );
        assert_eq!(
            messages[1].get("role").and_then(|v| v.as_str()),
            Some("user")
        );
        let tool_call_msg = &messages[2];
        assert_eq!(
            tool_call_msg.get("role").and_then(|v| v.as_str()),
            Some("assistant")
        );
        let tool_calls = tool_call_msg
            .get("tool_calls")
            .and_then(|v| v.as_array())
            .expect("tool_calls");
        assert_eq!(tool_calls.len(), 1);
        assert_eq!(
            tool_calls[0].get("id").and_then(|v| v.as_str()),
            Some("call_1")
        );
        assert_eq!(
            messages[3].get("role").and_then(|v| v.as_str()),
            Some("tool")
        );
        assert_eq!(
            messages[3].get("tool_call_id").and_then(|v| v.as_str()),
            Some("call_1")
        );
        assert_eq!(
            messages[3].get("content").and_then(|v| v.as_str()),
            Some("sunny")
        );
        assert_eq!(
            messages[4].get("role").and_then(|v| v.as_str()),
            Some("assistant")
        );

        assert_eq!(
            body.get("model").and_then(|v| v.as_str()),
            Some("deepseek-v4-pro")
        );
        assert_eq!(
            body.get("stream").and_then(serde_json::Value::as_bool),
            Some(true)
        );
        assert_eq!(
            body.get("tool_choice").and_then(|v| v.as_str()),
            Some("auto")
        );
        assert_eq!(
            body.get("parallel_tool_calls")
                .and_then(serde_json::Value::as_bool),
            Some(true)
        );
    }

    #[test]
    fn omits_system_message_and_tools_when_empty() {
        let body = build_chat_completions_body("m", "", &[user("hi")], &[], false);
        let messages = body
            .get("messages")
            .and_then(|m| m.as_array())
            .expect("messages is an array");
        assert_eq!(messages.len(), 1);
        assert_eq!(
            messages[0].get("role").and_then(|v| v.as_str()),
            Some("user")
        );
        assert!(body.get("tools").is_none());
        assert!(body.get("tool_choice").is_none());
    }
}

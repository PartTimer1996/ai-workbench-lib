use anyhow::{Context, Result};
use aws_sdk_bedrockruntime::{primitives::Blob, Client as BedrockClient};
use serde_json::{json, Value};
use std::sync::Arc;
use tracing::info;

pub struct ModelRunner {
    bedrock_client: Arc<BedrockClient>,
}

impl ModelRunner {
    pub fn new(bedrock_client: Arc<BedrockClient>) -> Self {
        Self { bedrock_client }
    }

    /// Invoke a Bedrock model with the given prompt and model configuration
    pub async fn invoke_model(&self, model_id: &str, prompt: &str, max_tokens: u32) -> Result<(String, Option<u32>)> {
        // Truncate if too long (rough token estimate: ~4 chars per token)
        let max_chars = 15000; // Leave room for response
        let truncated_prompt = if prompt.len() > max_chars {
            format!("{}...\n\n[Content truncated for analysis]", &prompt[..max_chars])
        } else {
            prompt.to_string()
        };

        // Build request body based on model type
        let request_body = self.build_request_body(model_id, &truncated_prompt, max_tokens);

        let response = self
            .bedrock_client
            .invoke_model()
            .model_id(model_id)
            .content_type("application/json")
            .accept("application/json")
            .body(Blob::new(request_body.to_string().as_bytes()))
            .send()
            .await
            .context(format!("Failed to invoke Bedrock model {}", model_id))?;

        let response_body: Value = serde_json::from_slice(&response.body().as_ref())
            .context("Failed to parse Bedrock response")?;

        // Extract output and token usage
        let output = self.extract_output(model_id, &response_body)?;
        let tokens_used = self.extract_token_usage(&response_body);

        info!("Model {} invoked successfully, {} tokens used", model_id, tokens_used.unwrap_or(0));

        Ok((output, tokens_used))
    }

    /// Build request body based on model type
    fn build_request_body(&self, model_id: &str, prompt: &str, max_tokens: u32) -> Value {
        match model_id {
            id if id.starts_with("anthropic.claude") => {
                json!({
                    "anthropic_version": "bedrock-2023-05-31",
                    "max_tokens": max_tokens,
                    "messages": [
                        {
                            "role": "user",
                            "content": prompt
                        }
                    ]
                })
            }
            id if id.starts_with("amazon.nova") => {
                json!({
                    "messages": [
                        {
                            "role": "user",
                            "content": [
                                {
                                    "text": prompt
                                }
                            ]
                        }
                    ],
                    "inferenceConfig": {
                        "max_new_tokens": max_tokens
                    }
                })
            }
            id if id.starts_with("mistral.") => {
                json!({
                    "prompt": prompt,
                    "max_tokens": max_tokens
                })
            }
            id if id.starts_with("cohere.") => {
                json!({
                    "prompt": prompt,
                    "max_tokens": max_tokens
                })
            }
            id if id.starts_with("meta.llama") => {
                json!({
                    "prompt": prompt,
                    "max_gen_len": max_tokens
                })
            }
            _ => {
                // Default format
                json!({
                    "prompt": prompt,
                    "max_tokens": max_tokens
                })
            }
        }
    }

    /// Extract output from response based on model type
    fn extract_output(&self, model_id: &str, response_body: &Value) -> Result<String> {
        let output = match model_id {
            id if id.starts_with("anthropic.claude") => {
                response_body["content"][0]["text"]
                    .as_str()
                    .unwrap_or("No content generated")
                    .to_string()
            }
            id if id.starts_with("amazon.nova") => {
                response_body["output"]["message"]["content"][0]["text"]
                    .as_str()
                    .unwrap_or("No content generated")
                    .to_string()
            }
            id if id.starts_with("mistral.") => {
                response_body["outputs"][0]["text"]
                    .as_str()
                    .unwrap_or("No content generated")
                    .to_string()
            }
            id if id.starts_with("cohere.") => {
                response_body["generations"][0]["text"]
                    .as_str()
                    .unwrap_or("No content generated")
                    .to_string()
            }
            id if id.starts_with("meta.llama") => {
                response_body["generation"]
                    .as_str()
                    .unwrap_or("No content generated")
                    .to_string()
            }
            _ => {
                response_body["completion"]
                    .as_str()
                    .or_else(|| response_body["text"].as_str())
                    .or_else(|| response_body["outputs"][0]["text"].as_str())
                    .or_else(|| response_body["generations"][0]["text"].as_str())
                    .or_else(|| response_body["generation"].as_str())
                    .unwrap_or("No content generated")
                    .to_string()
            }
        };

        Ok(output)
    }

    /// Extract token usage from response
    fn extract_token_usage(&self, response_body: &Value) -> Option<u32> {
        response_body["usage"]["total_tokens"]
            .as_u64()
            .or_else(|| {
                response_body["amazon-bedrock-invocationMetrics"]["inputTokenCount"]
                    .as_u64()
                    .and_then(|input| {
                        response_body["amazon-bedrock-invocationMetrics"]["outputTokenCount"]
                            .as_u64()
                            .map(|output| input + output)
                    })
            })
            .map(|t| t as u32)
    }
}

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
        info!("Invoking model {} with {} character prompt", model_id, truncated_prompt.len());
        
        let response = self
            .bedrock_client
            .invoke_model()
            .model_id(model_id)
            .content_type("application/json")
            .accept("application/json")
            .body(Blob::new(request_body.to_string().as_bytes()))
            .send()
            .await
            .map_err(|e| {
                anyhow::anyhow!(
                    "Failed to invoke Bedrock model {}: {}. This could be due to:\n\
                    1. Model not available in your region\n\
                    2. Insufficient permissions\n\
                    3. Model access not enabled\n\
                    4. Invalid model ID\n\
                    Original error: {}", 
                    model_id, e, e
                )
            })?;

        let response_body: Value = serde_json::from_slice(&response.body().as_ref())
            .context("Failed to parse Bedrock response")?;

        // Log the raw response for debugging
        info!("Bedrock response for model {}: {}", model_id, 
              serde_json::to_string_pretty(&response_body).unwrap_or_else(|_| "Could not serialize response".to_string()));

        // Extract output and token usage
        let output = self.extract_output(model_id, &response_body)?;
        let tokens_used = self.extract_token_usage(&response_body);

        info!("Model {} invoked successfully, {} tokens used", model_id, tokens_used.unwrap_or(0));

        Ok((output, tokens_used))
    }

    /// Invoke a model with file content and analysis prompt
    pub async fn invoke_model_with_file_content(
        &self, 
        model_id: &str, 
        file_content: &str, 
        analysis_prompt: &str, 
        max_tokens: u32
    ) -> Result<(String, Option<u32>)> {
        // Log details about the inputs
        info!("invoke_model_with_file_content called:");
        info!("  - model_id: {}", model_id);
        info!("  - file_content length: {} chars", file_content.len());
        info!("  - analysis_prompt: {}", analysis_prompt);
        info!("  - file_content preview (first 200 chars): {}", 
              &file_content.chars().take(200).collect::<String>());
        
        // Create a comprehensive prompt that includes both the file content and the analysis request
        let combined_prompt = self.format_prompt_with_file_content(file_content, analysis_prompt);
        
        // Log the combined prompt length for debugging
        info!("Combined prompt length: {} chars", combined_prompt.len());
        
        // Use the standard invoke_model with the combined prompt
        self.invoke_model(model_id, &combined_prompt, max_tokens).await
    }

    /// Format a prompt that includes file content and analysis instructions
    fn format_prompt_with_file_content(&self, file_content: &str, analysis_prompt: &str) -> String {
        // Add debugging info about content length
        info!("Formatting prompt with file content length: {} chars", file_content.len());
        
        if file_content.trim().is_empty() {
            return format!(
                "ERROR: No file content provided. Unable to analyze empty content.\n\n\
                USER REQUEST: {}\n\n\
                Please ensure the file content is properly loaded before analysis.",
                analysis_prompt
            );
        }

        format!(
            "You are analyzing the content of a file. The file content is provided below, followed by the analysis request.\n\n\
            === FILE CONTENT START ===\n\
            {}\n\
            === FILE CONTENT END ===\n\n\
            === ANALYSIS REQUEST ===\n\
            {}\n\n\
            === INSTRUCTIONS ===\n\
            Please analyze the file content provided above and respond to the analysis request. \
            Base your response ONLY on the content shown between the FILE CONTENT START/END markers. \
            Do not provide generic guidance - analyze the specific content provided.",
            file_content, analysis_prompt
        )
    }

    /// Build request body based on model type
    fn build_request_body(&self, model_id: &str, prompt: &str, max_tokens: u32) -> Value {
        match model_id {
            // Amazon Nova models (working in eu-west-2)
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
                        "max_new_tokens": max_tokens,
                        "temperature": 0.1,
                        "top_p": 0.9
                    }
                })
            }
            // Anthropic Claude 3 models (working in eu-west-2)
            id if id.starts_with("anthropic.claude-3") => {
                json!({
                    "anthropic_version": "bedrock-2023-05-31",
                    "max_tokens": max_tokens,
                    "temperature": 0.1,
                    "top_p": 0.9,
                    "messages": [
                        {
                            "role": "user",
                            "content": prompt
                        }
                    ]
                })
            }
            // Mistral models (working in eu-west-2)
            id if id.starts_with("mistral.") => {
                json!({
                    "prompt": prompt,
                    "max_tokens": max_tokens,
                    "temperature": 0.1,
                    "top_p": 0.9
                })
            }
            // Meta Llama models (working in eu-west-2)
            id if id.starts_with("meta.llama") => {
                json!({
                    "prompt": prompt,
                    "max_gen_len": max_tokens,
                    "temperature": 0.1,
                    "top_p": 0.9
                })
            }
            // Fallback for older Claude models
            id if id.starts_with("anthropic.claude") => {
                json!({
                    "prompt": format!("\n\nHuman: {}\n\nAssistant:", prompt),
                    "max_tokens_to_sample": max_tokens,
                    "temperature": 0.1,
                    "top_p": 0.9,
                })
            }
            // Amazon Titan models (if needed)
            id if id.starts_with("amazon.titan") => {
                json!({
                    "inputText": prompt,
                    "textGenerationConfig": {
                        "maxTokenCount": max_tokens,
                        "temperature": 0.1,
                        "topP": 0.9
                    }
                })
            }
            // Default format for any other models
            _ => {
                json!({
                    "prompt": prompt,
                    "max_tokens": max_tokens,
                    "temperature": 0.1
                })
            }
        }
    }

    /// Extract output from response based on model type
    fn extract_output(&self, model_id: &str, response_body: &Value) -> Result<String> {
        let output = match model_id {
            // Amazon Nova models (working in eu-west-2)
            id if id.starts_with("amazon.nova") => {
                response_body["output"]["message"]["content"][0]["text"]
                    .as_str()
                    .unwrap_or("No content generated")
                    .to_string()
            }
            // Anthropic Claude 3 models (working in eu-west-2)
            id if id.starts_with("anthropic.claude-3") => {
                response_body["content"][0]["text"]
                    .as_str()
                    .unwrap_or("No content generated")
                    .to_string()
            }
            // Mistral models (working in eu-west-2)
            id if id.starts_with("mistral.") => {
                response_body["outputs"][0]["text"]
                    .as_str()
                    .unwrap_or("No content generated")
                    .to_string()
            }
            // Meta Llama models (working in eu-west-2)
            id if id.starts_with("meta.llama") => {
                response_body["generation"]
                    .as_str()
                    .unwrap_or("No content generated")
                    .to_string()
            }
            // Older Claude models
            id if id.starts_with("anthropic.claude") => {
                response_body["completion"]
                    .as_str()
                    .unwrap_or("No content generated")
                    .to_string()
            }
            // Amazon Titan models (if needed)
            id if id.starts_with("amazon.titan") => {
                response_body["results"][0]["outputText"]
                    .as_str()
                    .unwrap_or("No content generated")
                    .to_string()
            }
            // Fallback for other models - try multiple common response formats
            _ => {
                response_body["completion"]
                    .as_str()
                    .or_else(|| response_body["text"].as_str())
                    .or_else(|| response_body["outputs"][0]["text"].as_str())
                    .or_else(|| response_body["content"][0]["text"].as_str())
                    .or_else(|| response_body["generation"].as_str())
                    .or_else(|| response_body["results"][0]["outputText"].as_str())
                    .or_else(|| response_body["output"]["message"]["content"][0]["text"].as_str())
                    .unwrap_or("No content generated")
                    .to_string()
            }
        };

        if output.trim().is_empty() || output == "No content generated" {
            info!("Model response body for debugging: {}", serde_json::to_string_pretty(response_body).unwrap_or_else(|_| "Could not serialize response".to_string()));
        }

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

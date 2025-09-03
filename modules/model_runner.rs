use anyhow::{Context, Result};
use aws_sdk_bedrockruntime::{primitives::Blob, Client as BedrockClient};
use serde_json::{json, Value};
use std::{collections::HashMap, sync::Arc};
use tracing::info;

#[derive(Debug, Clone)]
pub struct ModelPricing {
    pub input: f64,
    pub output: f64,
}


#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct TokenUsage {
    pub input_tokens: u32,
    pub output_tokens: u32,
    pub total_tokens: u32,
}

impl std::fmt::Display for TokenUsage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} total ({} input, {} output)", 
               self.total_tokens, self.input_tokens, self.output_tokens)
    }
}

pub struct ModelRunner {
    bedrock_client: Arc<BedrockClient>,
}

impl ModelRunner {
    pub fn new(bedrock_client: Arc<BedrockClient>) -> Self {
        Self { bedrock_client }
    }

    /// Invoke a Bedrock model with the given prompt and model configuration
    pub async fn invoke_model(&self, model_id: &str, prompt: &str, max_tokens: u32) -> Result<(String, Option<TokenUsage>)> {
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

        info!("Model {:?} invoked successfully, {:?} tokens used", model_id, tokens_used.clone().unwrap());

        Ok((output, tokens_used))
    }

    /// Invoke a model with file content and analysis prompt
    pub async fn invoke_model_with_file_content(
        &self, 
        model_id: &str, 
        file_content: &str, 
        analysis_prompt: &str, 
        max_tokens: u32
    ) -> Result<(String, Option<TokenUsage>)> {
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

    pub fn get_model_pricing() -> HashMap<&'static str, ModelPricing> {
        let mut pricing = HashMap::new();
        pricing.insert("amazon.nova-micro-v1:0", ModelPricing { input: 0.00035, output: 0.0014 });
        pricing.insert("amazon.nova-lite-v1:0", ModelPricing { input: 0.0006, output: 0.0024 });
        pricing.insert("amazon.nova-pro-v1:0", ModelPricing { input: 0.008, output: 0.032 });
        pricing.insert("amazon.titan-text-lite-v1", ModelPricing { input: 0.0003, output: 0.0004 });
        pricing.insert("amazon.titan-text-express-v1", ModelPricing { input: 0.0008, output: 0.0016 });
        pricing.insert("anthropic.claude-3-haiku-20240307-v1:0", ModelPricing { input: 0.00025, output: 0.00125 });
        pricing.insert("anthropic.claude-3-sonnet-20240229-v1:0", ModelPricing { input: 0.003, output: 0.015 });
        pricing.insert("anthropic.claude-3-opus-20240229-v1:0", ModelPricing { input: 0.015, output: 0.075 });
        pricing.insert("anthropic.claude-3-5-sonnet-20240620-v1:0", ModelPricing { input: 0.003, output: 0.015 });
        pricing.insert("meta.llama3-8b-instruct-v1:0", ModelPricing { input: 0.0003, output: 0.0006 });
        pricing.insert("meta.llama3-70b-instruct-v1:0", ModelPricing { input: 0.00265, output: 0.0035 });
        pricing.insert("mistral.mistral-7b-instruct-v0:2", ModelPricing { input: 0.00015, output: 0.0002 });
        pricing.insert("mistral.mixtral-8x7b-instruct-v0:1", ModelPricing { input: 0.00045, output: 0.0007 });
        pricing.insert("mistral.mistral-large-2402-v1:0", ModelPricing { input: 0.004, output: 0.012 });
        pricing.insert("cohere.command-text-v14", ModelPricing { input: 0.0015, output: 0.002 });
        pricing.insert("cohere.command-light-text-v14", ModelPricing { input: 0.0003, output: 0.0006 });
        pricing.insert("ai21.j2-mid-v1", ModelPricing { input: 0.0125, output: 0.0125 });
        pricing.insert("ai21.j2-ultra-v1", ModelPricing { input: 0.0188, output: 0.0188 });
        pricing
    }

        /// Calculates cost estimate based on token usage and model pricing
    pub fn calculate_cost_estimate(token_usage: Option<TokenUsage>, model_id: &str) -> Option<f64> {
        token_usage.and_then(|usage| {
            let pricing_map = ModelRunner::get_model_pricing();
            pricing_map.get(model_id).map(|pricing| {
                (usage.input_tokens as f64 * pricing.input / 1000.0) + 
                (usage.output_tokens as f64 * pricing.output / 1000.0)
            })
        })
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
    fn extract_token_usage(&self, response_body: &Value) -> Option<TokenUsage> {
        // Try to extract separate input/output tokens first
        if let (Some(input), Some(output)) = (
            response_body["usage"]["inputTokens"].as_u64(),
            response_body["usage"]["outputTokens"].as_u64(),
        ) {
            return Some(TokenUsage {
                input_tokens: input as u32,
                output_tokens: output as u32,
                total_tokens: (input + output) as u32,
            });
        }

        // Fallback to total tokens if available
        if let Some(total) = response_body["usage"]["totalTokens"].as_u64()
            .or_else(|| response_body["usage"]["total_tokens"].as_u64())
        {
            // When only total is available, assume 70% input / 30% output split as rough estimate
            let input_estimate = (total as f64 * 0.7) as u32;
            let output_estimate = total as u32 - input_estimate;
            return Some(TokenUsage {
                input_tokens: input_estimate,
                output_tokens: output_estimate,
                total_tokens: total as u32,
            });
        }

        // Last resort: try Bedrock invocation metrics
        if let (Some(input), Some(output)) = (
            response_body["amazon-bedrock-invocationMetrics"]["inputTokenCount"].as_u64(),
            response_body["amazon-bedrock-invocationMetrics"]["outputTokenCount"].as_u64(),
        ) {
            return Some(TokenUsage {
                input_tokens: input as u32,
                output_tokens: output as u32,
                total_tokens: (input + output) as u32,
            });
        }

        None
    }
}

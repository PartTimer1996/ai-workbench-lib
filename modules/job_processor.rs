use anyhow::{Context, Result};
use aws_sdk_bedrockruntime::Client as BedrockClient;
use aws_sdk_s3::Client as S3Client;
use bytes::Bytes;
use futures::future::join_all;
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::collections::{HashMap, HashSet};
use std::path::Path;
use std::sync::Arc;
use std::time::Instant;
use tokio::io::AsyncReadExt;
use tracing::{error, info};

use super::{FileDiscovery, FileInfo, FileSplitter, SplitConfig, ModelRunner, FileType};

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct JobConfig {
    pub job_id: String,
    pub prompt: String,
    pub workspace_bucket: String,
    pub input_spec: String, // Can be single file, or folder (ending with /)
    pub output_prefix: String,
    pub model_id: String,
    pub workspace_id: String,
    pub user_id: String,
    pub chunk_size_mb: Option<f64>,
    pub max_parallel: Option<usize>,
    pub include_file_context: Option<bool>, // Whether to include file path and type in prompts
}

#[derive(Debug, Serialize, Deserialize)]
pub struct FileChunkResult {
    pub file_path: String,
    pub chunk_id: usize,
    pub output: String,
    pub tokens_used: Option<u32>,
    pub cost_estimate: Option<f64>,
    pub file_type: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct JobResult {
    pub job_id: String,
    pub output_s3_key: String,
    pub total_files_processed: usize,
    pub total_chunks: usize,
    pub total_tokens: Option<u32>,
    pub total_cost: Option<f64>,
    pub processing_time_ms: u64,
    pub files_summary: String,
    pub final_summary_tokens: Option<u32>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ProcessingMetadata {
    pub job_id: String,
    pub input_spec: String,
    pub processing_type: String, // "single_file" or or "folder"
    pub total_files: usize,
    pub files_processed: Vec<FileProcessingInfo>,
    pub errors: Vec<String>,
    pub processing_time_ms: u64,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct FileProcessingInfo {
    pub file_path: String,
    pub file_size_bytes: usize,
    pub file_type: String,
    pub chunks_created: usize,
    pub processing_successful: bool,
    pub error_message: Option<String>,
}

pub struct JobProcessor {
    s3_client: Arc<S3Client>,
    model_runner: ModelRunner,
    config: JobConfig,
}

impl JobProcessor {
    pub fn new(s3_client: Arc<S3Client>, bedrock_client: Arc<BedrockClient>, config: JobConfig) -> Self {
        let model_runner = ModelRunner::new(bedrock_client);
        
        Self {
            s3_client,
            model_runner,
            config,
        }
    }

    /// Discover all files to process
    pub async fn discover_files(&self) -> Result<Vec<FileInfo>> {
        let file_discovery = FileDiscovery::new(
            (*self.s3_client).clone(),
            self.config.workspace_bucket.clone()
        );
        
        file_discovery.discover_files(&self.config.input_spec).await
    }

    /// Download a single file from S3
    async fn download_file(&self, file_info: &FileInfo) -> Result<Bytes> {
        info!("Downloading file: s3://{}/{}", self.config.workspace_bucket, file_info.s3_key);
        
        let response = self
            .s3_client
            .get_object()
            .bucket(&self.config.workspace_bucket)
            .key(&file_info.s3_key)
            .send()
            .await
            .context(format!("Failed to download {} from S3", file_info.s3_key))?;

        let mut body = response.body.into_async_read();
        let mut buffer = Vec::new();
        body.read_to_end(&mut buffer).await
            .context(format!("Failed to read S3 object body for {}", file_info.s3_key))?;
        
        Ok(Bytes::from(buffer))
    }

    /// Split file data into chunks for processing
    fn chunk_file_data(&self, file_info: &FileInfo, data: &[u8]) -> Result<Vec<Vec<u8>>> {
        let chunk_size_mb = self.config.chunk_size_mb.unwrap_or(5.0);
        
        // Create file splitter with configuration
        let split_config = SplitConfig {
            chunk_size_mb,
            _preserve_boundaries: true,
            min_chunk_ratio: 0.1,
        };
        
        let file_splitter = FileSplitter::with_config(split_config);
        
        // Use file info path for type detection
        let file_path = Path::new(&file_info.s3_key);
        
        // Split the file using the intelligent splitter
        let file_chunks = file_splitter.split_file(file_path, data)
            .context(format!("Failed to split file data for {}", file_info.s3_key))?;
        
        info!(
            "Split {} into {} chunks using {:?} format detection", 
            file_info.s3_key,
            file_chunks.len(),
            file_chunks.first().map(|c| &c.metadata.file_type).unwrap_or(&FileType::Binary)
        );
        
        // Log chunk details
        for chunk in &file_chunks {
            info!(
                "File: {}, Chunk {}: {} bytes, {} units, headers: {}", 
                file_info.s3_key,
                chunk.chunk_id,
                chunk.metadata.size_bytes,
                chunk.metadata.unit_count.unwrap_or(0),
                chunk.metadata.has_headers
            );
        }
        
        // Convert FileChunk to Vec<u8> for compatibility with existing code
        Ok(file_chunks.into_iter().map(|chunk| chunk.data).collect())
    }

    async fn process_file_chunk(&self, file_info: &FileInfo, chunk_id: usize, chunk_data: &[u8]) -> Result<FileChunkResult> {
        let text_content = String::from_utf8_lossy(chunk_data);
        
        // Build prompt with optional file context
        let final_prompt = if self.config.include_file_context.unwrap_or(true) {
            let file_context = format!(
                "File: {}\nFile Type: {:?}\nChunk: {} of file\n\nContent:\n{}",
                file_info.relative_path,
                file_info.file_type,
                chunk_id + 1,
                text_content
            );
            self.config.prompt.replace("{{file}}", &file_context)
        } else {
            self.config.prompt.replace("{{file}}", &text_content)
        };

        info!("Processing file: {}, chunk {} ({} bytes)", file_info.relative_path, chunk_id, chunk_data.len());

        // Use ModelRunner to process the chunk
        let (output, tokens_used) = self.model_runner
            .invoke_model(&self.config.model_id, &final_prompt, 4000)
            .await
            .context(format!("Failed to process chunk {} for file {}", chunk_id, file_info.relative_path))?;

        info!("File: {}, chunk {} processed successfully, {} tokens used", 
               file_info.relative_path, chunk_id, tokens_used.unwrap_or(0));

        Ok(FileChunkResult {
            file_path: file_info.relative_path.clone(),
            chunk_id,
            output,
            tokens_used,
            cost_estimate: tokens_used.map(|t| t as f64 * 0.00001), // Rough estimate
            file_type: format!("{:?}", file_info.file_type),
        })
    }

    /// Process all chunks for a single file
    async fn process_file_chunks(&self, file_info: &FileInfo, chunks: Vec<Vec<u8>>) -> Result<Vec<FileChunkResult>> {
        let max_parallel = self.config.max_parallel.unwrap_or(4);
        let semaphore = Arc::new(tokio::sync::Semaphore::new(max_parallel));
        
        let tasks: Vec<_> = chunks
            .into_iter()
            .enumerate()
            .map(|(i, chunk)| {
                let semaphore = semaphore.clone();
                let file_info = file_info.clone();
                let processor = self;
                async move {
                    let _permit = semaphore.acquire().await.unwrap();
                    processor.process_file_chunk(&file_info, i, &chunk).await
                }
            })
            .collect();

        let results = join_all(tasks).await;
        let mut chunk_results = Vec::new();
        
        for result in results {
            match result {
                Ok(chunk_result) => chunk_results.push(chunk_result),
                Err(e) => {
                    error!("Chunk processing failed for {}: {}", file_info.relative_path, e);
                    return Err(e);
                }
            }
        }

        // Sort by chunk_id to maintain order
        chunk_results.sort_by_key(|r| r.chunk_id);
        Ok(chunk_results)
    }

    /// Generate final summary by passing combined results through the model
    async fn generate_final_summary(&self, combined_content: &str) -> Result<(String, Option<u32>)> {
        info!("Generating final summary through model analysis");

        // Create a summary prompt
        let summary_prompt = format!(
            "Please provide a comprehensive summary and analysis of the following processed content. \
            Identify key insights, patterns, and important findings:\n\n{}",
            combined_content
        );

        // Use ModelRunner to generate the final summary
        let (summary, tokens_used) = self.model_runner
            .invoke_model(&self.config.model_id, &summary_prompt, 4000)
            .await
            .context("Failed to generate final summary")?;

        info!("Final summary generated, {} tokens used", tokens_used.unwrap_or(0));

        Ok((summary, tokens_used))
    }

    async fn process_all_files(&self, files: Vec<FileInfo>) -> Result<Vec<FileChunkResult>> {
        let mut all_results = Vec::new();
        
        for file_info in files {
            info!("Processing file: {} ({} bytes, {:?})", 
                  file_info.relative_path, file_info.size_bytes, file_info.file_type);
            
            // Download file
            let file_data = self.download_file(&file_info).await?;
            
            // Split into chunks
            let chunks = self.chunk_file_data(&file_info, &file_data)?;
            
            if chunks.len() > 1 {
                info!("File {} split into {} chunks", file_info.relative_path, chunks.len());
            }
            
            // Process chunks
            let file_results = self.process_file_chunks(&file_info, chunks).await?;
            all_results.extend(file_results);
        }
        
        Ok(all_results)
    }

    async fn upload_results(&self, results: &[FileChunkResult], files_summary: &str, files: &[FileInfo], processing_time_ms: u64) -> Result<String> {
        // Group results by file for better organization
        let mut file_groups: HashMap<String, Vec<&FileChunkResult>> = HashMap::new();
        for result in results {
            file_groups.entry(result.file_path.clone()).or_default().push(result);
        }
        
        let mut combined_output = String::new();
        combined_output.push_str("=== ANALYSIS RESULTS ===\n\n");
        combined_output.push_str(files_summary);
        combined_output.push('\n');
        
        for (file_path, file_results) in &file_groups {
            combined_output.push_str(&format!("\n=== FILE: {} ({}) ===\n", file_path, file_results[0].file_type));
            
            for result in file_results {
                if file_results.len() > 1 {
                    combined_output.push_str(&format!("\n--- Chunk {} ---\n", result.chunk_id + 1));
                }
                combined_output.push_str(&result.output);
                combined_output.push('\n');
            }
        }

        // Generate final summary through model
        let (final_summary, final_summary_tokens) = self.generate_final_summary(&combined_output).await?;
        
        // Add final summary to output
        combined_output.push_str("\n\n=== FINAL ANALYSIS SUMMARY ===\n");
        combined_output.push_str(&final_summary);

        let total_tokens: u32 = results.iter().filter_map(|r| r.tokens_used).sum();
        let total_cost: f64 = results.iter().filter_map(|r| r.cost_estimate).sum();
        let unique_files: HashSet<_> = results.iter().map(|r| &r.file_path).collect();

        let final_result = JobResult {
            job_id: self.config.job_id.clone(),
            output_s3_key: format!("{}{}.json", self.config.output_prefix, self.config.job_id),
            total_files_processed: unique_files.len(),
            total_chunks: results.len(),
            total_tokens: Some(total_tokens + final_summary_tokens.unwrap_or(0)),
            total_cost: Some(total_cost),
            processing_time_ms,
            files_summary: files_summary.to_string(),
            final_summary_tokens,
        };

        // Upload main results
        let output_data = json!({
            "metadata": final_result,
            "content": combined_output
        });

        let output_key = format!("{}{}.json", self.config.output_prefix, self.config.job_id);
        
        self.s3_client
            .put_object()
            .bucket(&self.config.workspace_bucket)
            .key(&output_key)
            .body(output_data.to_string().into_bytes().into())
            .content_type("application/json")
            .send()
            .await
            .context("Failed to upload results to S3")?;

        // Upload detailed metadata separately
        let processing_type = if files.len() == 1 {
            "single_file".to_string()
        } else {
            "folder".to_string()
        };
        let files_processed: Vec<FileProcessingInfo> = files.iter().map(|file| {
            let file_results: Vec<&FileChunkResult> = results.iter()
                .filter(|r| r.file_path == file.relative_path)
                .collect();
            
            FileProcessingInfo {
                file_path: file.relative_path.clone(),
                file_size_bytes: file.size_bytes,
                file_type: format!("{:?}", file.file_type),
                chunks_created: file_results.len(),
                processing_successful: !file_results.is_empty(),
                error_message: None,
            }
        }).collect();

        let metadata = ProcessingMetadata {
            job_id: self.config.job_id.clone(),
            input_spec: self.config.input_spec.clone(),
            processing_type,
            total_files: files.len(),
            files_processed,
            errors: Vec::new(), // TODO: Collect actual errors during processing
            processing_time_ms
        };

        let metadata_key = format!("{}metadata_{}.json", self.config.output_prefix, self.config.job_id);
        
        self.s3_client
            .put_object()
            .bucket(&self.config.workspace_bucket)
            .key(&metadata_key)
            .body(serde_json::to_string_pretty(&metadata)?.into_bytes().into())
            .content_type("application/json")
            .send()
            .await
            .context("Failed to upload metadata to S3")?;

        info!("Results uploaded to s3://{}/{}", self.config.workspace_bucket, output_key);
        info!("Metadata uploaded to s3://{}/{}", self.config.workspace_bucket, metadata_key);
        
        Ok(output_key)
    }

    pub async fn run(&self) -> Result<String> {
        let start_time = Instant::now();
        
        // Discover all files to process
        info!("Discovering files from input spec: {}", self.config.input_spec);
        let files = self.discover_files().await?;
        
        if files.is_empty() {
            return Err(anyhow::anyhow!("No processable files found"));
        }
        
        // Generate processing summary
        let file_discovery = FileDiscovery::new(
            (*self.s3_client).clone(),
            self.config.workspace_bucket.clone()
        );
        let summary = file_discovery.generate_processing_summary(&files);
        let summary_text = summary.format_summary();
        
        info!("Processing summary:\n{}", summary_text);
        
        // Process all files (clone files to avoid move)
        let results = self.process_all_files(files.clone()).await?;
        
        let elapsed = start_time.elapsed();
        let processing_time_ms = elapsed.as_millis() as u64;
        
        // Upload combined results
        let output_key = self.upload_results(&results, &summary_text, &files, processing_time_ms).await?;
        
        info!("Job {} completed in {:?}", self.config.job_id, elapsed);
        
        Ok(output_key)
    }
}

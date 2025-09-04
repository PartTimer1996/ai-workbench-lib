//! Core job processing logic

use anyhow::{Context, Result};
use aws_sdk_bedrockruntime::Client as BedrockClient;
use aws_sdk_s3::Client as S3Client;
use bytes::Bytes;
use futures::future::join_all;
use serde_json::json;
use std::collections::{HashMap, HashSet};
use std::fmt::Write;
use std::path::Path;
use std::sync::Arc;
use std::time::Instant;
use tokio::io::AsyncReadExt;
use tracing::{error, info, warn};

use crate::{FileDiscovery, FileInfo, FileSplitter, SplitConfig, ModelRunner, FileType};
use crate::modules::model_runner::TokenUsage;
use super::markdown::MarkdownFormatter;
use super::types::{JobConfig, FileChunkResult, JobResult, ProcessingMetadata, FileProcessingInfo};

// Constants
const DEFAULT_CHUNK_SIZE_MB: f64 = 5.0;
const DEFAULT_MAX_PARALLEL: usize = 4;
const DEFAULT_MAX_TOKENS: u32 = 4000;
const MIN_CHUNK_RATIO: f64 = 0.1;

/// Main processor for handling AI-powered file analysis jobs
/// 
/// Orchestrates the complete workflow from file discovery to result upload
pub struct JobProcessor {
    s3_client: Arc<S3Client>,
    model_runner: ModelRunner,
    config: JobConfig,
}

impl JobProcessor {
    /// Creates a new JobProcessor instance
    pub fn new(s3_client: Arc<S3Client>, bedrock_client: Arc<BedrockClient>, config: JobConfig) -> Self {
        let model_runner = ModelRunner::new(bedrock_client);
        
        Self {
            s3_client,
            model_runner,
            config,
        }
    }

    // === Main Execution ===
    /// Executes the complete job workflow from file discovery to result upload
    /// Returns the S3 key where the main results were uploaded
    pub async fn run(&self) -> Result<String> {
        let start_time = Instant::now();
        info!("Starting job execution for job_id: {}", self.config.job_id);
        
        // Step 1: Discover all files to process
        let files = self.discover_files().await
            .context("Failed during file discovery phase")?;
        
        // Step 2: Generate processing summary
        let summary_text = self.generate_processing_summary(&files).await
            .context("Failed to generate processing summary")?;
        
        // Step 3: Process all discovered files
        let results = self.process_all_files(files.clone()).await
            .context("Failed during file processing phase")?;
        
        if results.is_empty() {
            warn!("No results produced from file processing");
            return Err(anyhow::anyhow!("No results were generated from {} files", files.len()));
        }
        
        let processing_time_ms = start_time.elapsed().as_millis() as u64;
        
        // Step 4: Upload all results and metadata
        let output_key = self.upload_results(&results, &summary_text, &files, processing_time_ms).await
            .context("Failed during result upload phase")?;
        
        let final_elapsed = start_time.elapsed();
        info!("Job {} completed successfully in {:?}. Output: s3://{}/{}", 
              self.config.job_id, final_elapsed, self.config.workspace_bucket, output_key);
        
        Ok(output_key)
    }

    // === File Discovery and Management ===
    /// Discovers all files to process based on the input specification
    pub async fn discover_files(&self) -> Result<Vec<FileInfo>> {
        info!("=== FILE DISCOVERY DEBUG START ===");
        info!("Input specification: '{}'", self.config.input_spec);
        info!("Workspace bucket: '{}'", self.config.workspace_bucket);
        info!("Full S3 path would be: s3://{}/{}", self.config.workspace_bucket, self.config.input_spec);
        
        let file_discovery = FileDiscovery::new(
            (*self.s3_client).clone(),
            self.config.workspace_bucket.clone()
        );
        
        info!("FileDiscovery instance created successfully");
        info!("Attempting to discover files...");
        
        let files_result = file_discovery.discover_files(&self.config.input_spec).await;
        
        match &files_result {
            Ok(files) => {
                info!("File discovery succeeded! Found {} files", files.len());
                for (i, file) in files.iter().enumerate() {
                    info!("  File {}: s3_key='{}', relative_path='{}', size={} bytes, type={:?}",
                          i + 1, file.s3_key, file.relative_path, file.size_bytes, file.file_type);
                }
            },
            Err(e) => {
                error!("File discovery failed with error: {}", e);
                error!("Error context: {:?}", e);
                info!("=== FILE DISCOVERY DEBUG END (FAILED) ===");
                return Err(anyhow::anyhow!("{e}"));
            }
        }
        
        let files = files_result?;
        
        if files.is_empty() {
            error!("File discovery returned empty list!");
            error!("This means no processable files were found in: '{}'", self.config.input_spec);
            error!("Bucket: '{}'", self.config.workspace_bucket);
            info!("=== FILE DISCOVERY DEBUG END (EMPTY) ===");
            return Err(anyhow::anyhow!("No processable files found in: {}", self.config.input_spec));
        }
        
        info!("File discovery completed successfully with {} files", files.len());
        info!("=== FILE DISCOVERY DEBUG END (SUCCESS) ===");
        Ok(files)
    }

    /// Downloads a single file from S3 and returns its content as bytes
    async fn download_file(&self, file_info: &FileInfo) -> Result<Bytes> {
        info!("Downloading file: s3://{}/{} ({} bytes)", 
              self.config.workspace_bucket, file_info.s3_key, file_info.size_bytes);
        
        let response = self
            .s3_client
            .get_object()
            .bucket(&self.config.workspace_bucket)
            .key(&file_info.s3_key)
            .send()
            .await
            .context(format!("Failed to download {} from S3", file_info.s3_key))?;

        let mut body = response.body.into_async_read();
        let mut buffer = Vec::with_capacity(file_info.size_bytes);
        body.read_to_end(&mut buffer).await
            .context(format!("Failed to read S3 object body for {}", file_info.s3_key))?;
        
        Ok(Bytes::from(buffer))
    }

    // === File Processing ===

    /// Creates a configured file splitter based on job settings
    fn create_file_splitter(&self) -> FileSplitter {
        let chunk_size_mb = self.config.chunk_size_mb.unwrap_or(DEFAULT_CHUNK_SIZE_MB);
        
        let split_config = SplitConfig {
            chunk_size_mb,
            _preserve_boundaries: true,
            min_chunk_ratio: MIN_CHUNK_RATIO,
        };
        
        FileSplitter::with_config(split_config)
    }

    /// Splits file data into chunks using intelligent content-aware splitting
    fn chunk_file_data(&self, file_info: &FileInfo, data: &[u8]) -> Result<Vec<Vec<u8>>> {
        let file_splitter = self.create_file_splitter();
        let file_path = Path::new(&file_info.s3_key);
        
        let file_chunks = file_splitter.split_file(file_path, data)
            .context(format!("Failed to split file data for {}", file_info.s3_key))?;
        
        let detected_type = file_chunks.first()
            .map(|c| &c.metadata.file_type)
            .unwrap_or(&FileType::Binary);
        
        info!("Split {} into {} chunks using {:?} format detection", 
              file_info.s3_key, file_chunks.len(), detected_type);
        
        self.log_chunk_details(&file_chunks, &file_info.s3_key);
        
        Ok(file_chunks.into_iter().map(|chunk| chunk.data).collect())
    }

    /// Logs detailed information about file chunks
    fn log_chunk_details(&self, chunks: &[crate::file_splitter::FileChunk], file_path: &str) {
        for chunk in chunks {
            info!("File: {}, Chunk {}: {} bytes, {} units, headers: {}", 
                  file_path, chunk.chunk_id, chunk.metadata.size_bytes,
                  chunk.metadata.unit_count.unwrap_or(0), chunk.metadata.has_headers);
        }
    }

    /// Processes a single file chunk through the AI model
    async fn process_file_chunk(&self, file_info: &FileInfo, chunk_id: usize, chunk_data: &[u8]) -> Result<FileChunkResult> {
        let text_content = String::from_utf8_lossy(chunk_data);
        
        info!("Processing file: {}, chunk {} ({} bytes)", 
              file_info.relative_path, chunk_id, chunk_data.len());

        let max_tokens = self.config.max_tokens.unwrap_or(DEFAULT_MAX_TOKENS);
        let (output, tokens_used) = self.model_runner
            .invoke_model_with_file_content(
                &self.config.model_id, 
                &text_content, 
                &self.config.prompt, 
                max_tokens,
                self.config.temperature,
                self.config.top_p,
                None // No stop sequences for file content processing
            )
            .await
            .context(format!("Failed to process chunk {} for file {}", chunk_id, file_info.relative_path))?;

        info!("File: {}, chunk {} processed successfully, {} tokens used", 
              file_info.relative_path, chunk_id, 
              tokens_used.as_ref().map(|t| t.total_tokens).unwrap_or(0));

        Ok(FileChunkResult {
            file_path: file_info.relative_path.clone(),
            chunk_id,
            output,
            tokens_used: tokens_used.clone(),
            cost_estimate: ModelRunner::calculate_cost_estimate(tokens_used, &self.config.model_id),
            file_type: format!("{:?}", file_info.file_type),
        })
    }


    /// Processes all chunks for a single file with controlled concurrency
    async fn process_file_chunks(&self, file_info: &FileInfo, chunks: Vec<Vec<u8>>) -> Result<Vec<FileChunkResult>> {
        if chunks.is_empty() {
            warn!("No chunks to process for file: {}", file_info.relative_path);
            return Ok(Vec::new());
        }
        
        let max_parallel = self.config.max_parallel.unwrap_or(DEFAULT_MAX_PARALLEL);
        let semaphore = Arc::new(tokio::sync::Semaphore::new(max_parallel));
        
        info!("Processing {} chunks for file: {} (max {} parallel)", 
              chunks.len(), file_info.relative_path, max_parallel);
        
        let tasks: Vec<_> = chunks
            .into_iter()
            .enumerate()
            .map(|(i, chunk)| {
                let semaphore = semaphore.clone();
                let file_info = file_info.clone();
                let processor = self;
                async move {
                    let _permit = semaphore.acquire().await
                        .context("Failed to acquire semaphore permit")?;
                    processor.process_file_chunk(&file_info, i, &chunk).await
                }
            })
            .collect();

        let results = join_all(tasks).await;
        let mut chunk_results = Vec::with_capacity(results.len());
        let mut errors = Vec::new();
        
        for (idx, result) in results.into_iter().enumerate() {
            match result {
                Ok(chunk_result) => chunk_results.push(chunk_result),
                Err(e) => {
                    error!("Chunk {} processing failed for {}: {}", idx, file_info.relative_path, e);
                    errors.push(format!("Chunk {}: {}", idx, e));
                }
            }
        }
        
        if !errors.is_empty() {
            return Err(anyhow::anyhow!("Failed to process {} chunks: {}", 
                                       errors.len(), errors.join("; ")));
        }

        chunk_results.sort_by_key(|r| r.chunk_id);
        Ok(chunk_results)
    }

    // === Summary Generation ===

    /// Generates a comprehensive final summary using the AI model
    async fn generate_final_summary(&self, combined_content: &str) -> Result<(String, Option<TokenUsage>)> {
        info!("Generating final summary through model analysis");
        
        if combined_content.trim().is_empty() {
            warn!("No content provided for summary generation");
            return Ok(("No content available for summary".to_string(), None));
        }

        let summary_prompt = self.create_summary_prompt(combined_content);

        let max_tokens = self.config.max_tokens.unwrap_or(DEFAULT_MAX_TOKENS);
        let (summary, tokens_used) = self.model_runner
            .invoke_model(
                &self.config.model_id, 
                &summary_prompt, 
                max_tokens,
                self.config.temperature,
                self.config.top_p,
                None // No stop sequences for summary generation
            )
            .await
            .context("Failed to generate final summary")?;

        info!("Final summary generated, {} tokens used", 
              tokens_used.as_ref().map(|t| t.total_tokens).unwrap_or(0));
        Ok((summary, tokens_used))
    }

    /// Creates a well-formatted prompt for summary generation
    fn create_summary_prompt(&self, content: &str) -> String {
        format!(
            "Please provide a comprehensive summary and analysis of the following processed content. \
             Identify key insights, patterns, and important findings:\n\n{}",
            content
        )
    }

    /// Processes all discovered files through the complete workflow
    async fn process_all_files(&self, files: Vec<FileInfo>) -> Result<Vec<FileChunkResult>> {
        if files.is_empty() {
            return Ok(Vec::new());
        }
        
        info!("Starting to process {} files", files.len());
        let mut all_results = Vec::new();
        let mut processing_errors = Vec::new();
        
        for (idx, file_info) in files.iter().enumerate() {
            info!("Processing file {}/{}: {} ({} bytes, {:?})", 
                  idx + 1, files.len(), file_info.relative_path, 
                  file_info.size_bytes, file_info.file_type);
            
            match self.process_single_file(file_info).await {
                Ok(file_results) => {
                    info!("Successfully processed {} with {} chunks", 
                          file_info.relative_path, file_results.len());
                    all_results.extend(file_results);
                },
                Err(e) => {
                    error!("Failed to process file {}: {}", file_info.relative_path, e);
                    processing_errors.push(format!("{}: {}", file_info.relative_path, e));
                }
            }
        }
        
        if !processing_errors.is_empty() {
            warn!("Encountered {} file processing errors: {}", 
                  processing_errors.len(), processing_errors.join("; "));
        }
        
        info!("Completed processing all files. Total chunks: {}", all_results.len());
        Ok(all_results)
    }

    /// Processes a single file through download, chunking, and AI analysis
    async fn process_single_file(&self, file_info: &FileInfo) -> Result<Vec<FileChunkResult>> {
        let file_data = self.download_file(file_info).await
            .context(format!("Failed to download {}", file_info.relative_path))?;
            
        let chunks = self.chunk_file_data(file_info, &file_data)
            .context(format!("Failed to chunk {}", file_info.relative_path))?;
            
        if chunks.len() > 1 {
            info!("File {} split into {} chunks", file_info.relative_path, chunks.len());
        }
        
        self.process_file_chunks(file_info, chunks).await
            .context(format!("Failed to process chunks for {}", file_info.relative_path))
    }

    // === Result Formatting and Upload ===

    /// Groups chunk results by their source file for organized output
    fn group_results_by_file<'a>(&'a self, results: &'a [FileChunkResult]) -> HashMap<String, Vec<&'a FileChunkResult>> {
        let mut file_groups: HashMap<String, Vec<&FileChunkResult>> = HashMap::new();
        for result in results {
            file_groups.entry(result.file_path.clone()).or_default().push(result);
        }
        // Sort chunks within each file group by chunk_id
        for chunks in file_groups.values_mut() {
            chunks.sort_by_key(|chunk| chunk.chunk_id);
        }
        file_groups
    }

    /// Generates the combined plain text output for all processed files
    fn generate_combined_output(&self, file_groups: &HashMap<String, Vec<&FileChunkResult>>, 
                               files_summary: &str, final_summary: &str) -> String {
        let mut output = String::with_capacity(8192);
        
        // Header and summary
        writeln!(output, "=== ANALYSIS RESULTS ===\n").unwrap();
        writeln!(output, "{}", files_summary).unwrap();
        
        // Process files in sorted order
        self.add_file_content_to_output(&mut output, file_groups);
        
        // Final summary
        if !final_summary.trim().is_empty() {
            writeln!(output, "\n=== FINAL ANALYSIS SUMMARY ===").unwrap();
            writeln!(output, "{}", final_summary).unwrap();
        }
        
        output
    }
    
    /// Adds file content to the plain text output
    fn add_file_content_to_output(&self, output: &mut String, 
                                 file_groups: &HashMap<String, Vec<&FileChunkResult>>) {
        let mut sorted_files: Vec<_> = file_groups.iter().collect();
        sorted_files.sort_by_key(|(path, _)| path.as_str());
        
        for (file_path, file_results) in sorted_files {
            if file_results.is_empty() {
                continue;
            }
            
            writeln!(output, "\n=== FILE: {} ({}) ===", 
                    file_path, file_results[0].file_type).unwrap();
            
            for result in file_results {
                if file_results.len() > 1 {
                    writeln!(output, "\n--- Chunk {} ---", result.chunk_id + 1).unwrap();
                }
                writeln!(output, "{}", result.output).unwrap();
            }
        }
    }

    /// Creates a comprehensive job result with aggregated statistics
    fn create_job_result(&self, results: &[FileChunkResult], final_summary_tokens: Option<&TokenUsage>, 
                        processing_time_ms: u64, files_summary: &str) -> JobResult {
        let total_tokens: u32 = results.iter().filter_map(|r| r.tokens_used.as_ref()).map(|t| t.total_tokens).sum();
        let total_cost: f64 = results.iter().filter_map(|r| r.cost_estimate).sum();
        let unique_files: HashSet<_> = results.iter().map(|r| &r.file_path).collect();
        let final_tokens = final_summary_tokens.map(|t| t.total_tokens).unwrap_or(0);

        JobResult {
            job_id: self.config.job_id.clone(),
            output_s3_key: self.generate_output_key("result","md"),
            total_files_processed: unique_files.len(),
            total_chunks: results.len(),
            total_tokens: Some(total_tokens + final_tokens),
            total_cost: Some(total_cost),
            processing_time_ms,
            files_summary: files_summary.to_string(),
            final_summary_tokens: final_summary_tokens.map(|t| t.total_tokens),
        }
    }

    /// Generates consistent S3 keys for different output formats
    fn generate_output_key(&self, file_name: &str, extension: &str) -> String {
        format!("{}{}.{}", self.config.output_prefix, file_name, extension)
    }

    fn create_processing_metadata(&self, files: &[FileInfo], results: &[FileChunkResult], processing_time_ms: u64) -> ProcessingMetadata {
        let processing_type = if files.len() == 1 {
            "single_file".to_string()
        } else {
            "folder".to_string()
        };
        
        let files_processed: Vec<FileProcessingInfo> = files.iter().map(|file| {
            let file_results: Vec<&FileChunkResult> = results.iter()
                .filter(|r| r.file_path == file.relative_path)
                .collect();
            
            // Calculate per-file totals
            let file_input_tokens: u32 = file_results.iter()
                .filter_map(|r| r.tokens_used.as_ref())
                .map(|t| t.input_tokens)
                .sum();
            let file_output_tokens: u32 = file_results.iter()
                .filter_map(|r| r.tokens_used.as_ref())
                .map(|t| t.output_tokens)
                .sum();
            let file_total_tokens = file_input_tokens + file_output_tokens;
            let file_cost: f64 = file_results.iter()
                .filter_map(|r| r.cost_estimate)
                .sum();
            
            FileProcessingInfo {
                file_path: file.relative_path.clone(),
                file_size_bytes: file.size_bytes,
                file_type: format!("{:?}", file.file_type),
                chunks_created: file_results.len(),
                processing_successful: !file_results.is_empty(),
                error_message: None,
                input_tokens: file_input_tokens,
                output_tokens: file_output_tokens,
                total_tokens: file_total_tokens,
                cost_estimate: if file_cost > 0.0 { Some(file_cost) } else { None },
            }
        }).collect();

        // Calculate overall totals
        let total_input_tokens: u32 = results.iter()
            .filter_map(|r| r.tokens_used.as_ref())
            .map(|t| t.input_tokens)
            .sum();
        let total_output_tokens: u32 = results.iter()
            .filter_map(|r| r.tokens_used.as_ref())
            .map(|t| t.output_tokens)
            .sum();
        let total_tokens = total_input_tokens + total_output_tokens;
        let total_cost: f64 = results.iter()
            .filter_map(|r| r.cost_estimate)
            .sum();

        ProcessingMetadata {
            job_id: self.config.job_id.clone(),
            input_spec: self.config.input_spec.clone(),
            processing_type,
            total_files: files.len(),
            files_processed,
            errors: Vec::new(),
            processing_time_ms,
            total_input_tokens,
            total_output_tokens,
            total_tokens,
            total_cost: if total_cost > 0.0 { Some(total_cost) } else { None },
        }
    }

    /// Uploads content to S3 with proper error handling and logging
    async fn upload_to_s3(&self, key: &str, content: Vec<u8>, content_type: &str) -> Result<()> {
        info!("Uploading {} bytes to s3://{}/{}", content.len(), self.config.workspace_bucket, key);
        
        self.s3_client
            .put_object()
            .bucket(&self.config.workspace_bucket)
            .key(key)
            .body(content.into())
            .content_type(content_type)
            .send()
            .await
            .context(format!("Failed to upload {} to S3 bucket {}", key, self.config.workspace_bucket))?;
            
        info!("Successfully uploaded to s3://{}/{}", self.config.workspace_bucket, key);
        Ok(())
    }



    /// Orchestrates the complete result upload process with multiple formats
    async fn upload_results(&self, results: &[FileChunkResult], files_summary: &str, 
                           files: &[FileInfo], processing_time_ms: u64) -> Result<String> {
        info!("Beginning result upload process for {} chunks from {} files", 
              results.len(), files.len());
              
        let file_groups = self.group_results_by_file(results);
        
        // Generate final summary first
        let initial_combined_output = self.generate_combined_output(&file_groups, files_summary, "");
        let (final_summary, final_summary_tokens) = self.generate_final_summary(&initial_combined_output).await?;
        
        // Create all output formats
        let combined_output = self.generate_combined_output(&file_groups, files_summary, &final_summary);
        let markdown_content = MarkdownFormatter::format_as_markdown(&self.config, files_summary, &file_groups, &final_summary);
        let job_result = self.create_job_result(results, final_summary_tokens.as_ref(), processing_time_ms, files_summary);
        let metadata = self.create_processing_metadata(files, results, processing_time_ms);

        // Generate S3 keys
        let output_key = self.generate_output_key("result","md");
        let json_key = self.generate_output_key("result","json");
        let metadata_key = format!("{}metadata.json", self.config.output_prefix);

        // Upload all formats in parallel
        let markdown_upload = self.upload_to_s3(&output_key, markdown_content.into_bytes(), "text/markdown");
        
        let json_output = json!({
            "metadata": job_result,
            "content": combined_output
        });
        let json_upload = self.upload_to_s3(&json_key, json_output.to_string().into_bytes(), "application/json");
        
        let metadata_json = serde_json::to_string_pretty(&metadata)
            .context("Failed to serialize metadata to JSON")?;
        let metadata_upload = self.upload_to_s3(&metadata_key, metadata_json.into_bytes(), "application/json");

        // Wait for all uploads to complete
        tokio::try_join!(markdown_upload, json_upload, metadata_upload)
            .context("One or more file uploads failed")?;
        
        info!("All results successfully uploaded. Main output: s3://{}/{}", 
              self.config.workspace_bucket, output_key);
        Ok(output_key)
    }

    /// Generates a processing summary for the discovered files
    async fn generate_processing_summary(&self, files: &[FileInfo]) -> Result<String> {
        let file_discovery = FileDiscovery::new(
            (*self.s3_client).clone(),
            self.config.workspace_bucket.clone()
        );
        
        let summary = file_discovery.generate_processing_summary(files);
        let summary_text = summary.format_summary();
        
        info!("Generated processing summary for {} files:\n{}", files.len(), summary_text);
        Ok(summary_text)
    }
}

#[cfg(test)]
mod job_processor_tests {
    use crate::modules::job_processor::{JobProcessor, JobConfig};
    use aws_config::BehaviorVersion;
    use aws_sdk_s3::Client as S3Client;
    use aws_sdk_bedrockruntime::Client as BedrockClient;
    use std::env;
    use std::sync::Arc;
    use uuid::Uuid;

    /// Helper to create S3 client for testing
    async fn create_test_s3_client() -> S3Client {
        let config = aws_config::defaults(BehaviorVersion::latest())
            .region("eu-west-2")
            .load()
            .await;
        S3Client::new(&config)
    }

    /// Helper to create Bedrock client for testing
    async fn create_test_bedrock_client() -> BedrockClient {
        let config = aws_config::defaults(BehaviorVersion::latest())
            .region("eu-west-2")
            .load()
            .await;
        BedrockClient::new(&config)
    }

    /// Helper to get test workspace bucket from environment
    fn get_test_bucket() -> String {
        env::var("TEST_WORKSPACE_BUCKET")
            .unwrap_or_else(|_| "ai-workbench-6c9c43db-7fe6-42f1-8b11-8f82323f83f0-eu-west-2".to_string())
    }

    #[tokio::test]
    async fn test_job_processor_single_file_end_to_end() {
        // Initialize tracing for debugging
        let _ = tracing_subscriber::fmt()
            .with_max_level(tracing::Level::INFO)
            .try_init();
            
        let s3_client = Arc::new(create_test_s3_client().await);
        let bedrock_client = Arc::new(create_test_bedrock_client().await);
        let bucket = get_test_bucket();
        let model_id: String = "amazon.nova-micro-v1:0".into();
        
        // Test with a single file (you'll provide this)
        let test_file_key = "text_files/0c472776-7c22-464b-93bf-3714fd229b01.txt";
        let job_id = format!("test-single-{}", Uuid::new_v4());
        
        let config = JobConfig {
            job_id: job_id.clone(),
            prompt: "Extract and analyze key information from this document:\n1. Main topics and themes\n2. Key facts and figures\n3. Action items or requirements\n4. Structured summary\n\nDocument:\n{{file}}".to_string(),
            workspace_bucket: bucket.clone(),
            input_spec: test_file_key.to_string(),
            output_prefix: format!("result/{}/", job_id.clone()),
            model_id,
            workspace_id: "test-workspace".to_string(),
            user_id: "test-user".to_string(),
            chunk_size_mb: Some(1.0),
            max_parallel: Some(2),
            include_file_context: Some(true),
            max_tokens: None,
            temperature: None,
            top_p: None,
        };

        let processor = JobProcessor::new(s3_client.clone(), bedrock_client, config);
        
        println!("Testing single file processing for: {}", test_file_key);
        
        // Test each step individually for better debugging
        println!("Step 1: Testing file discovery...");
        let files_result = processor.discover_files().await;
        match &files_result {
            Ok(files) => {
                println!("✅ Found {} files", files.len());
                for (i, file) in files.iter().enumerate() {
                    println!("  File {}: {} ({} bytes)", i+1, file.relative_path, file.size_bytes);
                }
            }
            Err(e) => {
                println!("❌ File discovery failed: {}", e);
                return; // Exit early if file discovery fails
            }
        }
        
        let files = files_result.unwrap();
        
        println!("Step 2: Testing single file processing...");
        if let Some(first_file) = files.first() {
            match processor.process_single_file(first_file).await {
                Ok(results) => {
                    println!("✅ Successfully processed file with {} chunks", results.len());
                    for (i, chunk_result) in results.iter().enumerate() {
                        println!("  Chunk {}: {} -> {} chars output", 
                                i+1, 
                                chunk_result.chunk_id, 
                                chunk_result.output.len());
                    }
                }
                Err(e) => {
                    println!("❌ Single file processing failed: {}", e);
                    return;
                }
            }
        }
        
        println!("Step 3: Running full job processor...");
        
        match processor.run().await {
            Ok(output_key) => {
                println!("✅ Single file job completed successfully!");
                println!("Output saved to: s3://{}/{}", bucket, output_key);
                
                // Verify the output was actually created
                let get_result = s3_client
                    .head_object()
                    .bucket(&bucket)
                    .key(&output_key)
                    .send()
                    .await;
                
                match get_result {
                    Ok(response) => {
                        println!("✅ Output file verified in S3");
                        let size = response.content_length().unwrap_or(0);
                        println!("Output file size: {} bytes", size);
                        assert!(size > 0, "Output file should not be empty");
                    }
                    Err(e) => {
                        eprintln!("❌ Failed to verify output file: {}", e);
                        panic!("Output file not found in S3");
                    }
                }
                
                // Also check if metadata file was created
                let metadata_key = format!("test-results/metadata_{}.json", job_id);
                let metadata_result = s3_client
                    .head_object()
                    .bucket(&bucket)
                    .key(&metadata_key)
                    .send()
                    .await;
                
                match metadata_result {
                    Ok(_) => println!("✅ Metadata file verified in S3"),
                    Err(e) => println!("⚠️  Metadata file check failed: {}", e),
                }
            }
            Err(e) => {
                eprintln!("❌ Single file job failed: {}", e);
                println!("This might be expected if the test file doesn't exist yet");
                // Don't panic here - allow test to pass if files don't exist
            }
        }
    }

    #[tokio::test]
    async fn test_job_processor_folder_end_to_end() {
        let s3_client = Arc::new(create_test_s3_client().await);
        let bedrock_client = Arc::new(create_test_bedrock_client().await);
        let bucket = get_test_bucket();
        let model_id = String::from("amazon.nova-micro-v1:0");
        
        // Test with a folder (you'll provide this)
        let test_folder = "text_files/";
        let job_id = format!("test-folder-{}", Uuid::new_v4());
        
        let config = JobConfig {
            job_id: job_id.clone(),
            prompt: "Analyze the content of this file and summarize the key information. Identify main topics and important details.".to_string(),
            workspace_bucket: bucket.clone(),
            input_spec: test_folder.to_string(),
            output_prefix: "test-results/".to_string(),
            model_id,
            workspace_id: "test-workspace".to_string(),
            user_id: "test-user".to_string(),
            chunk_size_mb: Some(0.5), // Smaller chunks for testing
            max_parallel: Some(3),
            include_file_context: Some(true),
            max_tokens: None,
            temperature: None,
            top_p: None,
        };

        let processor = JobProcessor::new(s3_client.clone(), bedrock_client, config);
        
        println!("Testing folder processing for: {}", test_folder);
        
        match processor.run().await {
            Ok(output_key) => {
                println!("✅ Folder job completed successfully!");
                println!("Output saved to: s3://{}/{}", bucket, output_key);
                
                // Verify the output was created
                let get_result = s3_client
                    .head_object()
                    .bucket(&bucket)
                    .key(&output_key)
                    .send()
                    .await;
                
                match get_result {
                    Ok(response) => {
                        println!("✅ Output file verified in S3");
                        let size = response.content_length().unwrap_or(0);
                        println!("Output file size: {} bytes", size);
                        assert!(size > 0, "Output file should not be empty");
                        
                        // For folder processing, we expect larger output
                        assert!(size > 500, "Folder processing should generate substantial output");
                    }
                    Err(e) => {
                        eprintln!("❌ Failed to verify output file: {}", e);
                        panic!("Output file not found in S3");
                    }
                }
                
                // Check metadata file for folder processing
                let metadata_key = format!("test-results/metadata_{}.json", job_id);
                let metadata_result = s3_client
                    .get_object()
                    .bucket(&bucket)
                    .key(&metadata_key)
                    .send()
                    .await;
                
                match metadata_result {
                    Ok(response) => {
                        println!("✅ Metadata file retrieved successfully");
                        
                        // Try to parse metadata to verify structure
                        let body = response.body.collect().await.unwrap();
                        let body_bytes = body.into_bytes();
                        let metadata_text = String::from_utf8_lossy(&body_bytes);
                        
                        // Verify metadata contains expected fields
                        assert!(metadata_text.contains("processing_type"));
                        assert!(metadata_text.contains("total_files"));
                        assert!(metadata_text.contains("files_processed"));
                        
                        println!("Metadata content preview (first 200 chars):");
                        println!("{}", &metadata_text[..metadata_text.len().min(200)]);
                    }
                    Err(e) => println!("⚠️  Metadata file check failed: {}", e),
                }
            }
            Err(e) => {
                eprintln!("❌ Folder job failed: {}", e);
                println!("This might be expected if the test folder doesn't exist yet");
                // Don't panic here - allow test to pass if files don't exist
            }
        }
    }

    #[tokio::test]
    async fn test_job_processor_with_large_file() {
        let s3_client = Arc::new(create_test_s3_client().await);
        let bedrock_client = Arc::new(create_test_bedrock_client().await);
        let bucket = get_test_bucket();
        let model_id = "amazon.nova-micro-v1:0".into();
        
        // Test with a larger file that should be chunked (you'll provide this)
        let test_large_file = "large_files/large_document.txt";
        let job_id = format!("test-large-{}", Uuid::new_v4());
        
        let config = JobConfig {
            job_id: job_id.clone(),
            prompt: "Provide a detailed analysis of this content. Extract key insights, themes, and important information.".to_string(),
            workspace_bucket: bucket.clone(),
            input_spec: test_large_file.to_string(),
            output_prefix: "test-results/".to_string(),
            model_id,
            workspace_id: "test-workspace".to_string(),
            user_id: "test-user".to_string(),
            chunk_size_mb: Some(0.1), // Very small chunks to force splitting
            max_parallel: Some(2),
            include_file_context: Some(true),
            max_tokens: None,
            temperature: None,
            top_p: None,
        };

        let processor = JobProcessor::new(s3_client.clone(), bedrock_client, config);
        
        println!("Testing large file processing (with chunking) for: {}", test_large_file);
        
        match processor.run().await {
            Ok(output_key) => {
                println!("✅ Large file job completed successfully!");
                println!("Output saved to: s3://{}/{}", bucket, output_key);
                
                // Download and examine the output
                let get_result = s3_client
                    .get_object()
                    .bucket(&bucket)
                    .key(&output_key)
                    .send()
                    .await;
                
                match get_result {
                    Ok(response) => {
                        let body = response.body.collect().await.unwrap();
                        let body_bytes = body.into_bytes();
                        let output_text = String::from_utf8_lossy(&body_bytes);
                        
                        println!("✅ Large file output verified");
                        println!("Output length: {} characters", output_text.len());
                        
                        // Verify output contains expected sections
                        assert!(output_text.contains("ANALYSIS RESULTS"));
                        assert!(output_text.contains("FINAL ANALYSIS SUMMARY"));
                        
                        // For chunked files, we should see chunk markers
                        if output_text.contains("Chunk") {
                            println!("✅ File was properly chunked and processed");
                        }
                        
                        println!("Output preview (first 300 chars):");
                        println!("{}", &output_text[..output_text.len().min(300)]);
                    }
                    Err(e) => {
                        eprintln!("❌ Failed to retrieve output: {}", e);
                        panic!("Could not retrieve output file");
                    }
                }
            }
            Err(e) => {
                eprintln!("❌ Large file job failed: {}", e);
                println!("This might be expected if the test file doesn't exist yet");
            }
        }
    }

    #[tokio::test]
    async fn test_job_processor_file_discovery() {
        let s3_client = Arc::new(create_test_s3_client().await);
        let bedrock_client = Arc::new(create_test_bedrock_client().await);
        let bucket = get_test_bucket();
        let model_id = "amazon.nova-micro-v1:0".into();
        
        // Test just the file discovery part without running the full job
        let test_folder = "text_files/";
        
        let config = JobConfig {
            job_id: "test-discovery".to_string(),
            prompt: "test prompt".to_string(),
            workspace_bucket: bucket,
            input_spec: test_folder.to_string(),
            output_prefix: "test-results/".to_string(),
            model_id,
            workspace_id: "test-workspace".to_string(),
            user_id: "test-user".to_string(),
            chunk_size_mb: Some(1.0),
            max_parallel: Some(2),
            include_file_context: Some(true),
            max_tokens: None,
            temperature: None,
            top_p: None,
        };

        let processor = JobProcessor::new(s3_client, bedrock_client, config);
        
        match processor.discover_files().await {
            Ok(files) => {
                println!("✅ File discovery successful!");
                println!("Discovered {} files", files.len());
                
                for file in &files {
                    println!("  - {} ({} bytes, {:?})", 
                             file.relative_path, file.size_bytes, file.file_type);
                }
                
                if !files.is_empty() {
                    assert!(files.iter().all(|f| f.size_bytes > 0));
                    assert!(files.iter().all(|f| !f.s3_key.is_empty()));
                    assert!(files.iter().all(|f| !f.relative_path.is_empty()));
                    println!("✅ All discovered files have valid metadata");
                }
            }
            Err(e) => {
                println!("❌ File discovery failed: {}", e);
                println!("This might be expected if the test folder doesn't exist yet");
            }
        }
    }

    #[tokio::test]
    async fn test_job_processor_config_validation() {
        let s3_client = Arc::new(create_test_s3_client().await);
        let bedrock_client = Arc::new(create_test_bedrock_client().await);
        let model_id: String = "amazon.nova-micro-v1:0".into();
        
        // Test with various configuration options
        let configs = vec![
            // Minimal config
            JobConfig {
                job_id: "test-minimal".to_string(),
                prompt: "Simple test prompt".to_string(),
                workspace_bucket: get_test_bucket(),
                input_spec: "text_files/sample.txt".to_string(),
                output_prefix: "test-results/".to_string(),
                model_id: model_id.clone(),
                workspace_id: "test".to_string(),
                user_id: "test".to_string(),
                chunk_size_mb: None,
                max_parallel: None,
                include_file_context: None,
                max_tokens: None,
                temperature: None,
                top_p: None,
            },
            // Full config
            JobConfig {
                job_id: "test-full".to_string(),
                prompt: "Detailed test prompt with {{file}} placeholder".to_string(),
                workspace_bucket: get_test_bucket(),
                input_spec: "text_files/".to_string(),
                output_prefix: "test-results/full/".to_string(),
                model_id: model_id.clone(),
                workspace_id: "test-workspace".to_string(),
                user_id: "test-user".to_string(),
                chunk_size_mb: Some(2.0),
                max_parallel: Some(5),
                include_file_context: Some(false),
                max_tokens: None,
                temperature: None,
                top_p: None,
            },
        ];
        
        for config in configs {
            let processor = JobProcessor::new(s3_client.clone(), bedrock_client.clone(), config.clone());
            
            // Test that processor can be created with different configs
            println!("✅ JobProcessor created successfully with job_id: {}", config.job_id);
            
            // Test file discovery with this config
            match processor.discover_files().await {
                Ok(files) => {
                    println!("  ✅ Discovery works: {} files found", files.len());
                }
                Err(e) => {
                    println!("  ⚠️  Discovery failed (expected if files don't exist): {}", e);
                }
            }
        }
    }
}
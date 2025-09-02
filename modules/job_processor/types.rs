//! Types and data structures for the job processor module

use serde::{Deserialize, Serialize};

/// Configuration for a processing job
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

/// Result from processing a single file chunk
#[derive(Debug, Serialize, Deserialize)]
pub struct FileChunkResult {
    pub file_path: String,
    pub chunk_id: usize,
    pub output: String,
    pub tokens_used: Option<u32>,
    pub cost_estimate: Option<f64>,
    pub file_type: String,
}

/// Complete result from a job execution
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

/// Metadata about the entire processing operation
#[derive(Debug, Serialize, Deserialize)]
pub struct ProcessingMetadata {
    pub job_id: String,
    pub input_spec: String,
    pub processing_type: String, // "single_file" or "folder"
    pub total_files: usize,
    pub files_processed: Vec<FileProcessingInfo>,
    pub errors: Vec<String>,
    pub processing_time_ms: u64,
}

/// Information about processing a single file
#[derive(Debug, Serialize, Deserialize)]
pub struct FileProcessingInfo {
    pub file_path: String,
    pub file_size_bytes: usize,
    pub file_type: String,
    pub chunks_created: usize,
    pub processing_successful: bool,
    pub error_message: Option<String>,
}
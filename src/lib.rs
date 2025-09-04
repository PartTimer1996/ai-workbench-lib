//! # AI Workbench Library
//! 
//! A comprehensive library for AI workbench operations including:
//! - File discovery and S3 operations
//! - Intelligent file splitting with type-aware chunking  
//! - Model runner for AWS Bedrock integration
//! - Job processor for orchestrating the entire workflow
//! 
//! ## Example Usage
//! 
//! ```rust
//! use ai_workbench_lib::{JobProcessor, JobConfig, FileDiscovery};
//! use aws_config::BehaviorVersion;
//! use aws_sdk_s3::Client as S3Client;
//! use aws_sdk_bedrockruntime::Client as BedrockClient;
//! use std::sync::Arc;
//! 
//! #[tokio::main]
//! async fn main() -> anyhow::Result<()> {
//!     // Initialize AWS clients
//!     let config = aws_config::load_defaults(BehaviorVersion::latest()).await;
//!     let s3_client = Arc::new(S3Client::new(&config));
//!     let bedrock_client = Arc::new(BedrockClient::new(&config));
//!     
//!     // Create job configuration
//!     let job_config = JobConfig {
//!         job_id: "test-job".to_string(),
//!         prompt: "Analyze this file: {{file}}".to_string(),
//!         workspace_bucket: "my-workspace-bucket".to_string(),
//!         input_spec: "path/to/file.txt".to_string(),
//!         output_prefix: "outputs/".to_string(),
//!         model_id: "amazon.nova-micro-v1:0".to_string(),
//!         workspace_id: "ws-123".to_string(),
//!         user_id: "user-123".to_string(),
//!         chunk_size_mb: Some(5.0),
//!         max_parallel: Some(4),
//!         include_file_context: Some(true),
//!         max_tokens: Some(4000),
//!         temperature: Some(0.1),
//!         top_p: Some(0.9),
//!     };
//!     
//!     // Create and run job processor
//!     let processor = JobProcessor::new(s3_client, bedrock_client, job_config);
//!     let (output_key, metrics) = processor.run().await?;
//!     
//!     println!("Job completed! Output: {}", output_key);
//!     println!("Processed {} files with {} tokens", metrics.files_processed, metrics.total_tokens);
//!     
//!     Ok(())
//! }
//! ```

// Include the modules from the modules directory
#[path = "../modules/mod.rs"]
pub mod modules;

// Re-export everything from modules for easy access
pub use modules::*;

// Re-export commonly used external types for convenience
pub use anyhow::{Result, Context};
pub use aws_sdk_s3::Client as S3Client;
pub use aws_sdk_bedrockruntime::Client as BedrockClient;
pub use serde::{Serialize, Deserialize};
pub use tokio;
pub use tracing;

/// Version information for the library
pub const VERSION: &str = env!("CARGO_PKG_VERSION");

/// Library information
pub const DESCRIPTION: &str = env!("CARGO_PKG_DESCRIPTION");
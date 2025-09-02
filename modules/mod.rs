//! Library for AI Workbench file processing
//! 
//! This library provides modules for:
//! - File discovery and S3 operations
//! - Intelligent file splitting with type-aware chunking
//! - Model runner for Bedrock integration
//! - Job processor for orchestrating the entire workflow

pub mod file_discovery;
pub mod file_splitter;
pub mod model_runner;
pub mod job_processor;

// Re-export commonly used types and structs
pub use file_discovery::{FileDiscovery, FileInfo};
pub use file_splitter::{FileSplitter, SplitConfig, FileType};
pub use model_runner::ModelRunner;
pub use job_processor::{
    JobProcessor, JobConfig
};

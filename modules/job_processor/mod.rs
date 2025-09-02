//! Job processor module for handling AI-powered file analysis workflows
//!
//! This module provides a complete pipeline for:
//! - Discovering files in S3
//! - Intelligent content-aware file splitting
//! - AI model processing of file chunks
//! - Result aggregation and markdown generation
//! - Multi-format output (Markdown, JSON, metadata)

pub mod types;
pub mod markdown;
pub mod core;

// Re-export main types and structs
pub use types::{
    JobConfig, 
    FileChunkResult, 
    JobResult, 
    ProcessingMetadata, 
    FileProcessingInfo
};

pub use markdown::{MarkdownBuilder, MarkdownFormatter};
pub use core::JobProcessor;
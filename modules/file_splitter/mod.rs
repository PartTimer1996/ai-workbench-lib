//! File Splitter Library
//! 
//! Provides intelligent file splitting capabilities for large documents based on file type.
//! Supports text files, CSV/tabular data, JSON, and source code with configurable
//! chunk sizes and logical boundary preservation.

pub mod types;
pub mod splitter;
pub mod splitter_types;

// Re-export main types and the FileSplitter
pub use types::*;
pub use splitter::FileSplitter;

#[cfg(test)]
mod tests;

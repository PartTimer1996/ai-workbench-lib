//! File splitter implementations for different file types
//! 
//! This module contains specific splitter implementations for various file formats:
//! - TextSplitter: For plain text files
//! - CsvSplitter: For CSV and tabular data
//! - JsonSplitter: For JSON and JSONL files
//! - CodeSplitter: For source code files with intelligent boundary detection

pub mod text_splitter;
pub mod csv_splitter;
pub mod json_splitter;
pub mod code_splitter;

// Re-export the splitter structs
pub use text_splitter::TextSplitter;
pub use csv_splitter::CsvSplitter;
pub use json_splitter::JsonSplitter;
pub use code_splitter::CodeSplitter;

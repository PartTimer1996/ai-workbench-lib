//! Markdown generation utilities for job processor output

use std::collections::HashMap;
use std::fmt::Write;

use crate::JobConfig;

use super::types::FileChunkResult;

/// A builder for creating structured markdown documents
pub struct MarkdownBuilder {
    content: String,
}

impl MarkdownBuilder {
    pub fn new() -> Self {
        Self {
            content: String::with_capacity(16384),
        }
    }
    
    pub fn heading(&mut self, level: u8, text: &str) -> &mut Self {
        let prefix = "#".repeat(level as usize);
        writeln!(self.content, "{} {}\n", prefix, text).unwrap();
        self
    }
    
    pub fn paragraph(&mut self, text: &str) -> &mut Self {
        writeln!(self.content, "{}\n", text).unwrap();
        self
    }
    
    #[allow(dead_code)]
    pub fn code_block(&mut self, text: &str) -> &mut Self {
        writeln!(self.content, "```\n{}\n```\n", text).unwrap();
        self
    }
    
    pub fn metadata(&mut self, key: &str, value: &str) -> &mut Self {
        writeln!(self.content, "**{}:** {}\n", key, value).unwrap();
        self
    }
    
    pub fn horizontal_rule(&mut self) -> &mut Self {
        writeln!(self.content, "---\n").unwrap();
        self
    }
    
    pub fn emoji_heading(&mut self, level: u8, emoji: &str, text: &str) -> &mut Self {
        let prefix = "#".repeat(level as usize);
        writeln!(self.content, "{} {} {}\n", prefix, emoji, text).unwrap();
        self
    }
    
    pub fn build(self) -> String {
        self.content
    }
}

impl Default for MarkdownBuilder {
    fn default() -> Self {
        Self::new()
    }
}

/// Utilities for formatting job processor output as markdown
pub struct MarkdownFormatter;

impl MarkdownFormatter {

    /// Formats the analysis results as well-structured Markdown using the builder
    pub fn format_as_markdown(config: &JobConfig, files_summary: &str, 
                         file_groups: &HashMap<String, Vec<&FileChunkResult>>, 
                         final_summary: &str) -> String {
        let mut builder = MarkdownBuilder::new();
        
        // Header with job metadata
        builder
            .heading(1, "File Analysis Results")
            .metadata("Job ID", &config.job_id)
            .metadata("Model", &config.model_id)
            .paragraph("");
        
        // Processing summary
        let clean_summary = MarkdownFormatter::clean_processing_summary(files_summary);
        builder
            .heading(2, "Processing Summary")
            .paragraph(&clean_summary)
            .horizontal_rule();
        
        // Individual file analyses
        MarkdownFormatter::add_file_analyses(&mut builder, file_groups);
        
        // Final summary
        if !final_summary.trim().is_empty() {
            builder
                .emoji_heading(1, "📊", "Final Analysis Summary")
                .paragraph(final_summary);
        }
        
        // Footer
        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
            
        builder
            .horizontal_rule()
            .paragraph(&format!("*Analysis completed: Unix timestamp {}*", timestamp));
        
        builder.build()
    }

    /// Formats file analyses section in markdown
    pub fn add_file_analyses(builder: &mut MarkdownBuilder, 
                            file_groups: &HashMap<String, Vec<&FileChunkResult>>) {
        // Sort files for consistent output
        let mut sorted_files: Vec<_> = file_groups.iter().collect();
        sorted_files.sort_by_key(|(path, _)| path.as_str());
        
        for (file_path, file_results) in sorted_files {
            if file_results.is_empty() {
                continue;
            }
            
            let file_type = &file_results[0].file_type;
            builder
                .emoji_heading(2, "📁", &format!("File: `{}`", file_path))
                .metadata("Type", file_type)
                .metadata("Chunks", &file_results.len().to_string())
                .paragraph("");
            
            for result in file_results {
                if file_results.len() > 1 {
                    builder.heading(3, &format!("Chunk {} (Tokens: {})", 
                                               result.chunk_id + 1, 
                                               result.tokens_used.as_ref().map(|t| t.total_tokens).unwrap_or(0)));
                }
                
                let cleaned_output = Self::clean_ai_output(&result.output);
                builder.paragraph(&cleaned_output);
            }
            
            builder.horizontal_rule();
        }
    }

    /// Cleans up AI output for better markdown formatting
    pub fn clean_ai_output(output: &str) -> String {
        output
            .replace("### ", "### ")
            .replace("#### ", "#### ")
            .replace("=== ", "### ")
            .replace("---", "\n---\n")
            .trim()
            .to_string()
    }

    /// Cleans the processing summary text
    pub fn clean_processing_summary(files_summary: &str) -> String {
        files_summary
            .replace("Processing Summary:", "")
            .trim()
            .to_string()
    }
}
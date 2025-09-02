use super::types::*;
use super::splitter_types::{TextSplitter, CsvSplitter, JsonSplitter, CodeSplitter};
use anyhow::Result;
use std::path::Path;

/// Main file splitter that automatically detects file type and applies appropriate strategy
pub struct FileSplitter {
    config: SplitConfig,
}

impl FileSplitter {
    /// Create a new file splitter with default configuration
    pub fn new() -> Self {
        Self {
            config: SplitConfig::default(),
        }
    }
    
    /// Create a new file splitter with custom configuration
    pub fn with_config(config: SplitConfig) -> Self {
        Self { config }
    }
    
    /// Split a file by detecting its type from the file path
    pub fn split_file(&self, file_path: &Path, data: &[u8]) -> Result<Vec<FileChunk>> {
        let file_type = FileType::from_extension(file_path);
        self.split_with_type(data, file_type)
    }
    
    /// Split data with explicit file type
    pub fn split_with_type(&self, data: &[u8], file_type: FileType) -> Result<Vec<FileChunk>> {
        if data.is_empty() {
            return Ok(vec![]);
        }

        let strategy: Box<dyn SplitStrategy> = match file_type {
            FileType::Csv => {
                let csv_splitter = CsvSplitter::new();
                // Validate CSV format before processing
                if !csv_splitter.validate_format(data)? {
                    // Fall back to text splitting if CSV validation fails
                    return self.split_with_type(data, FileType::Text);
                }
                Box::new(csv_splitter)
            },
            FileType::Tsv => {
                let tsv_splitter = CsvSplitter::new_tsv();
                // Validate TSV format before processing
                if !tsv_splitter.validate_format(data)? {
                    // Fall back to text splitting if TSV validation fails
                    return self.split_with_type(data, FileType::Text);
                }
                Box::new(tsv_splitter)
            },
            FileType::Json => {
                let json_splitter = JsonSplitter::new();
                // Try JSON splitting, fall back to text if validation fails
                match json_splitter.split(data, &self.config) {
                    Ok(chunks) => return Ok(chunks),
                    Err(_) => return self.split_with_type(data, FileType::Text),
                }
            },
            // Source code files get special treatment
            file_type if file_type.is_source_code() => {
                Box::new(CodeSplitter::new(file_type))
            },
            // For all other types (Text, Xml, Binary, config files), use text splitting
            _ => Box::new(TextSplitter),
        };

        strategy.split(data, &self.config)
    }
    
}

impl Default for FileSplitter {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::Path;

    #[test]
    fn test_file_type_detection_by_extension() {
        let splitter = FileSplitter::new();
        
        assert_eq!(splitter.split_file(Path::new("test.txt"), b"hello world").unwrap().len(), 1);
        assert_eq!(splitter.split_file(Path::new("data.csv"), b"a,b\n1,2").unwrap()[0].metadata.file_type, FileType::Csv);
        assert_eq!(splitter.split_file(Path::new("data.tsv"), b"a\tb\n1\t2").unwrap()[0].metadata.file_type, FileType::Tsv);
        assert_eq!(splitter.split_file(Path::new("logs.json"), br#"{"message": "test"}"#).unwrap()[0].metadata.file_type, FileType::Json);
    }

    #[test]
    fn test_json_file_splitting() {
        let splitter = FileSplitter::new();
        
        // JSON Lines format (common for logs)
        let json_lines = br#"{"timestamp": "2023-01-01", "level": "INFO", "message": "Application started"}
{"timestamp": "2023-01-01", "level": "ERROR", "message": "Database connection failed"}
{"timestamp": "2023-01-01", "level": "INFO", "message": "Retrying connection"}"#;
        
        let chunks = splitter.split_file(Path::new("app.log.json"), json_lines).unwrap();
        assert!(!chunks.is_empty());
        assert_eq!(chunks[0].metadata.file_type, FileType::Json);
        
        // Standard JSON object
        let json_object = br#"{"users": [{"name": "John", "age": 25}, {"name": "Jane", "age": 30}]}"#;
        let chunks = splitter.split_file(Path::new("users.json"), json_object).unwrap();
        assert!(!chunks.is_empty());
        assert_eq!(chunks[0].metadata.file_type, FileType::Json);
    }

    #[test]
    fn test_invalid_csv_fallback() {
        let splitter = FileSplitter::new();
        
        // This looks like CSV but has inconsistent structure
        let invalid_csv = "a,b,c\nthis is not csv\njust random text";
        let chunks = splitter.split_with_type(invalid_csv.as_bytes(), FileType::Csv).unwrap();
        
        // Should fall back to text splitting
        assert_eq!(chunks[0].metadata.file_type, FileType::Text);
    }

    #[test]
    fn test_binary_file_rejection() {
        let splitter = FileSplitter::new();
        
        // Binary data
        let binary_data = &[0xFF, 0xFE, 0x00, 0x01, 0x02, 0x03];
        let result = splitter.split_with_type(binary_data, FileType::Binary);
        
        assert!(result.is_err());
    }

    #[test]
    fn test_empty_file() {
        let splitter = FileSplitter::new();
        let chunks = splitter.split_file(Path::new("empty.txt"), b"").unwrap();
        assert!(chunks.is_empty());
    }

}

#[cfg(test)]
mod integration_tests {
    use super::*;
    use std::path::Path;

    #[test]
    fn test_large_text_file_splitting() {
        // Create a large text file
        let large_text = (0..10000)
            .map(|i| format!("This is line number {} with some additional content to make it longer", i))
            .collect::<Vec<_>>()
            .join("\n");
        
        let config = SplitConfig {
            chunk_size_mb: 0.1, // 100KB chunks
            ..Default::default()
        };
        
        let splitter = FileSplitter::with_config(config);
        let chunks = splitter.split_file(Path::new("large.txt"), large_text.as_bytes()).unwrap();
        
        assert!(chunks.len() > 1);
        
        // Verify all chunks are reasonably sized
        for chunk in &chunks {
            assert!(chunk.data.len() > 0);
            assert!(chunk.data.len() <= 120_000); // Allow some overhead
        }
        
        // Verify content integrity by reconstructing
        let reconstructed = chunks.iter()
            .map(|chunk| String::from_utf8_lossy(&chunk.data))
            .collect::<Vec<_>>()
            .join("\n");
        
        // Should contain all original lines (allowing for joining differences)
        let original_lines: std::collections::HashSet<&str> = large_text.lines().collect();
        let reconstructed_lines: std::collections::HashSet<&str> = reconstructed.lines().collect();
        
        // Most lines should be preserved (allowing for some edge cases in joining)
        let preserved_ratio = original_lines.intersection(&reconstructed_lines).count() as f64 / original_lines.len() as f64;
        assert!(preserved_ratio > 0.95);
    }

    #[test]
    fn test_large_csv_file_splitting() {
        // Create a large CSV
        let mut csv_data = "id,name,email,department,salary\n".to_string();
        for i in 0..5000 {
            csv_data.push_str(&format!(
                "{},Employee {},employee{}@company.com,Dept{},{}\n",
                i, i, i, i % 10, 50000 + (i % 1000) * 100
            ));
        }
        
        let config = SplitConfig {
            chunk_size_mb: 0.05, // 50KB chunks
            ..Default::default()
        };
        
        let splitter = FileSplitter::with_config(config);
        let chunks = splitter.split_file(Path::new("employees.csv"), csv_data.as_bytes()).unwrap();
        
        assert!(chunks.len() > 1);
        
        // Verify each chunk is valid CSV with header
        for chunk in &chunks {
            assert!(chunk.metadata.has_headers);
            let text = String::from_utf8_lossy(&chunk.data);
            let lines: Vec<&str> = text.lines().collect();
            assert!(lines.len() >= 1);
            assert_eq!(lines[0], "id,name,email,department,salary");
            
            // Verify each data line has correct number of fields
            for line in &lines[1..] {
                let fields: Vec<&str> = line.split(',').collect();
                assert_eq!(fields.len(), 5);
            }
        }
    }

    #[test]
    fn test_mixed_file_types() {
        let splitter = FileSplitter::new();
        
        let test_cases = vec![
            ("data.txt", "Simple text file\nwith multiple lines\nof content", FileType::Text),
            ("data.csv", "a,b,c\n1,2,3\n4,5,6", FileType::Csv),
            ("data.tsv", "a\tb\tc\n1\t2\t3\n4\t5\t6", FileType::Tsv),
            ("data.json", r#"{"name": "test", "value": 123}"#, FileType::Json),
            ("data.xml", "<root><item>value</item></root>", FileType::Xml),
        ];
        
        for (filename, content, expected_type) in test_cases {
            let chunks = splitter.split_file(Path::new(filename), content.as_bytes()).unwrap();
            assert_eq!(chunks.len(), 1); // Small files should be single chunk
            assert_eq!(chunks[0].metadata.file_type, expected_type);
            assert_eq!(chunks[0].chunk_id, 0);
        }
    }

    #[test]
    fn test_edge_cases() {
        let splitter = FileSplitter::new();
        
        // Very long single line
        let long_line = "a".repeat(1_000_000);
        let chunks = splitter.split_file(Path::new("long.txt"), long_line.as_bytes()).unwrap();
        assert_eq!(chunks.len(), 1); // Can't split a single line
        
        // File with only headers (CSV)
        let header_only = "name,age,city";
        let chunks = splitter.split_file(Path::new("headers.csv"), header_only.as_bytes()).unwrap();
        assert_eq!(chunks.len(), 1);
        assert!(chunks[0].metadata.has_headers);
        
        // File with empty lines
        let with_empty_lines = "line1\n\n\nline2\n\nline3";
        let chunks = splitter.split_file(Path::new("gaps.txt"), with_empty_lines.as_bytes()).unwrap();
        assert_eq!(chunks.len(), 1);
        assert_eq!(chunks[0].metadata.unit_count, Some(6)); // 6 lines including empty ones
    }

    #[test]
    fn test_performance_characteristics() {
        let splitter = FileSplitter::new();
        
        // Create a very large file to test performance
        let large_content = (0..50000)
            .map(|i| format!("Line {} with some content that makes it reasonably long", i))
            .collect::<Vec<_>>()
            .join("\n");
        
        let start = std::time::Instant::now();
        let chunks = splitter.split_file(Path::new("performance.txt"), large_content.as_bytes()).unwrap();
        let duration = start.elapsed();
        
        // Should complete in reasonable time (adjust threshold as needed)
        assert!(duration.as_secs() < 5);
        assert!(chunks.len() > 1);
        
        // Verify total size preservation
        let total_chunk_size: usize = chunks.iter().map(|c| c.data.len()).sum();
        let original_size = large_content.len();
        
        // Allow for some size difference due to line joining in chunks
        let size_ratio = total_chunk_size as f64 / original_size as f64;
        assert!(size_ratio > 0.95 && size_ratio < 1.05);
    }
}

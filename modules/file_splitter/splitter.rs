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

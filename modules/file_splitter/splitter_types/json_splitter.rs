use super::super::types::*;
use anyhow::{Result, Context};
use serde_json::Value;

/// JSON file splitting strategy that handles both standard JSON and JSON Lines format
pub struct JsonSplitter;

impl JsonSplitter {
    /// Create a new JSON splitter
    pub fn new() -> Self {
        Self
    }
    
    /// Validate that the data appears to be valid JSON
    pub fn validate_format(&self, data: &[u8]) -> Result<JsonFormat> {
        let text = std::str::from_utf8(data).context("Invalid UTF-8 in JSON data")?;
        let trimmed = text.trim();
        
        if trimmed.is_empty() {
            return Ok(JsonFormat::Empty);
        }
        
        // Check for JSON Lines format (one JSON object per line)
        if self.is_json_lines(text) {
            return Ok(JsonFormat::JsonLines);
        }
        
        // Check for standard JSON (single object or array)
        if self.is_standard_json(trimmed) {
            return Ok(JsonFormat::Standard);
        }
        
        Ok(JsonFormat::Invalid)
    }
    
    /// Check if content is JSON Lines format
    fn is_json_lines(&self, text: &str) -> bool {
        let lines: Vec<&str> = text.lines()
            .map(|line| line.trim())
            .filter(|line| !line.is_empty())
            .take(10) // Check first 10 non-empty lines
            .collect();
            
        if lines.is_empty() {
            return false;
        }
        
        // Most lines should parse as valid JSON objects
        let valid_json_lines = lines.iter()
            .filter(|line| serde_json::from_str::<Value>(line).is_ok())
            .count();
            
        valid_json_lines >= (lines.len() * 3 / 4) // At least 75% valid
    }
    
    /// Check if content is standard JSON
    fn is_standard_json(&self, text: &str) -> bool {
        serde_json::from_str::<Value>(text).is_ok()
    }
}

impl Default for JsonSplitter {
    fn default() -> Self {
        Self::new()
    }
}

impl SplitStrategy for JsonSplitter {
    fn split(&self, data: &[u8], config: &SplitConfig) -> Result<Vec<FileChunk>> {
        let format = self.validate_format(data)?;
        
        match format {
            JsonFormat::Empty => Ok(vec![]),
            JsonFormat::Invalid => {
                // Fall back to treating as text if JSON validation fails
                return Err(anyhow::anyhow!("Invalid JSON format"));
            },
            JsonFormat::Standard => self.split_standard_json(data, config),
            JsonFormat::JsonLines => self.split_json_lines(data, config),
        }
    }
    
    fn file_type(&self) -> FileType {
        FileType::Json
    }
}

impl JsonSplitter {
    /// Split standard JSON (single object or array)
    fn split_standard_json(&self, data: &[u8], config: &SplitConfig) -> Result<Vec<FileChunk>> {
        let chunk_size_bytes = (config.chunk_size_mb * 1024.0 * 1024.0) as usize;
        
        // For small JSON files, return as single chunk
        if data.len() <= chunk_size_bytes {
            return Ok(vec![FileChunk {
                chunk_id: 0,
                data: data.to_vec(),
                metadata: ChunkMetadata {
                    size_bytes: data.len(),
                    unit_count: Some(1), // Single JSON document
                    has_headers: false,
                    file_type: FileType::Json,

                },
            }]);
        }
        
        let text = std::str::from_utf8(data).context("Invalid UTF-8 in JSON data")?;
        
        // Try to parse as JSON array for intelligent splitting
        if let Ok(Value::Array(array)) = serde_json::from_str::<Value>(text) {
            return self.split_json_array(array, config);
        }
        
        // For large single JSON objects, split by lines (less intelligent but functional)
        self.split_by_lines(data, config)
    }
    
    /// Split JSON Lines format (one JSON object per line)
    fn split_json_lines(&self, data: &[u8], config: &SplitConfig) -> Result<Vec<FileChunk>> {
        let chunk_size_bytes = (config.chunk_size_mb * 1024.0 * 1024.0) as usize;
        let min_chunk_size = (chunk_size_bytes as f64 * config.min_chunk_ratio) as usize;
        
        let text = std::str::from_utf8(data).context("Invalid UTF-8 in JSON Lines data")?;
        let lines: Vec<&str> = text.lines().collect();
        
        if lines.is_empty() {
            return Ok(vec![]);
        }
        
        // For small files, return as single chunk
        if data.len() <= chunk_size_bytes {
            return Ok(vec![FileChunk {
                chunk_id: 0,
                data: data.to_vec(),
                metadata: ChunkMetadata {
                    size_bytes: data.len(),
                    unit_count: Some(lines.len()),
                    has_headers: false,
                    file_type: FileType::Json,
                },
            }]);
        }
        
        let mut chunks = Vec::new();
        let mut current_chunk_lines = Vec::new();
        let mut current_size = 0;
        let mut chunk_id = 0;
        
        for line in lines {
            let line_size = line.as_bytes().len() + 1; // +1 for newline
            
            // Check if adding this line would exceed chunk size
            if current_size + line_size > chunk_size_bytes && !current_chunk_lines.is_empty() {
                // Only create chunk if it meets minimum size requirement
                if current_size >= min_chunk_size {
                    let chunk_data = current_chunk_lines.join("\n").into_bytes();
                    chunks.push(FileChunk {
                        chunk_id,
                        data: chunk_data.clone(),
                        metadata: ChunkMetadata {
                            size_bytes: chunk_data.len(),
                            unit_count: Some(current_chunk_lines.len()),
                            has_headers: false,
                            file_type: FileType::Json,
                        },
                    });
                    chunk_id += 1;
                    current_chunk_lines.clear();
                    current_size = 0;
                }
            }
            
            current_chunk_lines.push(line);
            current_size += line_size;
        }
        
        // Add remaining lines as final chunk
        if !current_chunk_lines.is_empty() {
            let chunk_data = current_chunk_lines.join("\n").into_bytes();
            chunks.push(FileChunk {
                chunk_id,
                data: chunk_data.clone(),
                metadata: ChunkMetadata {
                    size_bytes: chunk_data.len(),
                    unit_count: Some(current_chunk_lines.len()),
                    has_headers: false,
                    file_type: FileType::Json,
                },
            });
        }
        
        Ok(chunks)
    }
    
    /// Split JSON array into chunks by grouping array elements
    fn split_json_array(&self, array: Vec<Value>, config: &SplitConfig) -> Result<Vec<FileChunk>> {
        let chunk_size_bytes = (config.chunk_size_mb * 1024.0 * 1024.0) as usize;
        let min_chunk_size = (chunk_size_bytes as f64 * config.min_chunk_ratio) as usize;
        
        let mut chunks = Vec::new();
        let mut current_chunk_items = Vec::new();
        let mut current_size = 0;
        let mut chunk_id = 0;
        
        for item in array {
            let item_json = serde_json::to_string(&item)
                .context("Failed to serialize JSON item")?;
            let item_size = item_json.as_bytes().len();
            
            // Check if adding this item would exceed chunk size
            if current_size + item_size > chunk_size_bytes && !current_chunk_items.is_empty() {
                // Only create chunk if it meets minimum size requirement
                if current_size >= min_chunk_size {
                    let chunk_json = serde_json::to_string(&current_chunk_items)
                        .context("Failed to serialize JSON chunk")?;
                    let chunk_data = chunk_json.into_bytes();
                    
                    chunks.push(FileChunk {
                        chunk_id,
                        data: chunk_data.clone(),
                        metadata: ChunkMetadata {
                            size_bytes: chunk_data.len(),
                            unit_count: Some(current_chunk_items.len()),
                            has_headers: false,
                            file_type: FileType::Json,
                        },
                    });
                    chunk_id += 1;
                    current_chunk_items.clear();
                    current_size = 0;
                }
            }
            
            current_chunk_items.push(item);
            current_size += item_size;
        }
        
        // Add remaining items as final chunk
        if !current_chunk_items.is_empty() {
            let chunk_json = serde_json::to_string(&current_chunk_items)
                .context("Failed to serialize final JSON chunk")?;
            let chunk_data = chunk_json.into_bytes();
            
            chunks.push(FileChunk {
                chunk_id,
                data: chunk_data.clone(),
                metadata: ChunkMetadata {
                    size_bytes: chunk_data.len(),
                    unit_count: Some(current_chunk_items.len()),
                    has_headers: false,
                    file_type: FileType::Json,
                },
            });
        }
        
        Ok(chunks)
    }
    
    /// Fallback: split by lines for complex JSON structures
    fn split_by_lines(&self, data: &[u8], config: &SplitConfig) -> Result<Vec<FileChunk>> {
        let chunk_size_bytes = (config.chunk_size_mb * 1024.0 * 1024.0) as usize;
        let min_chunk_size = (chunk_size_bytes as f64 * config.min_chunk_ratio) as usize;
        
        let text = std::str::from_utf8(data).context("Invalid UTF-8 in JSON data")?;
        let lines: Vec<&str> = text.lines().collect();
        
        let mut chunks = Vec::new();
        let mut current_chunk_lines = Vec::new();
        let mut current_size = 0;
        let mut chunk_id = 0;
        
        for line in lines {
            let line_size = line.as_bytes().len() + 1; // +1 for newline
            
            if current_size + line_size > chunk_size_bytes && !current_chunk_lines.is_empty() {
                if current_size >= min_chunk_size {
                    let chunk_data = current_chunk_lines.join("\n").into_bytes();
                    chunks.push(FileChunk {
                        chunk_id,
                        data: chunk_data.clone(),
                        metadata: ChunkMetadata {
                            size_bytes: chunk_data.len(),
                            unit_count: Some(current_chunk_lines.len()),
                            has_headers: false,
                            file_type: FileType::Json,
                        },
                    });
                    chunk_id += 1;
                    current_chunk_lines.clear();
                    current_size = 0;
                }
            }
            
            current_chunk_lines.push(line);
            current_size += line_size;
        }
        
        if !current_chunk_lines.is_empty() {
            let chunk_data = current_chunk_lines.join("\n").into_bytes();
            chunks.push(FileChunk {
                chunk_id,
                data: chunk_data.clone(),
                metadata: ChunkMetadata {
                    size_bytes: chunk_data.len(),
                    unit_count: Some(current_chunk_lines.len()),
                    has_headers: false,
                    file_type: FileType::Json,

                },
            });
        }
        
        Ok(chunks)
    }
}

/// JSON format types
#[derive(Debug, Clone, PartialEq)]
pub enum JsonFormat {
    /// Empty file
    Empty,
    /// Invalid JSON
    Invalid,
    /// Standard JSON (single object or array)
    Standard,
    /// JSON Lines format (one JSON object per line)
    JsonLines,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_json_lines_detection() {
        let splitter = JsonSplitter::new();
        
        let json_lines = r#"{"name": "John", "age": 25}
{"name": "Jane", "age": 30}
{"name": "Bob", "age": 35}"#;
        
        let format = splitter.validate_format(json_lines.as_bytes()).unwrap();
        assert_eq!(format, JsonFormat::JsonLines);
    }

    #[test]
    fn test_standard_json_detection() {
        let splitter = JsonSplitter::new();
        
        let standard_json = r#"{"users": [{"name": "John", "age": 25}, {"name": "Jane", "age": 30}]}"#;
        
        let format = splitter.validate_format(standard_json.as_bytes()).unwrap();
        assert_eq!(format, JsonFormat::Standard);
    }

    #[test]
    fn test_json_array_detection() {
        let splitter = JsonSplitter::new();
        
        let json_array = r#"[{"name": "John", "age": 25}, {"name": "Jane", "age": 30}]"#;
        
        let format = splitter.validate_format(json_array.as_bytes()).unwrap();
        assert_eq!(format, JsonFormat::Standard);
    }

    #[test]
    fn test_json_lines_splitting() {
        let splitter = JsonSplitter::new();
        let config = SplitConfig {
            chunk_size_mb: 0.0001, // Very small for testing
            ..Default::default()
        };
        
        let json_lines = r#"{"id": 1, "message": "First log entry"}
{"id": 2, "message": "Second log entry"}
{"id": 3, "message": "Third log entry"}
{"id": 4, "message": "Fourth log entry"}"#;
        
        let chunks = splitter.split(json_lines.as_bytes(), &config).unwrap();
        assert!(chunks.len() > 1);
        
        // Verify each chunk contains valid JSON lines
        for chunk in chunks {
            let text = String::from_utf8(chunk.data).unwrap();
            for line in text.lines() {
                if !line.trim().is_empty() {
                    assert!(serde_json::from_str::<Value>(line).is_ok());
                }
            }
        }
    }

    #[test]
    fn test_invalid_json_rejection() {
        let splitter = JsonSplitter::new();
        
        let invalid_json = "This is not JSON at all";
        let format = splitter.validate_format(invalid_json.as_bytes()).unwrap();
        assert_eq!(format, JsonFormat::Invalid);
    }
}

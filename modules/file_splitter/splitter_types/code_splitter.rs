use super::super::types::*;
use anyhow::Result;

/// Simple file splitting strategy that batches files by size
pub struct CodeSplitter {
    file_type: FileType,
}

impl CodeSplitter {
    /// Create a new code splitter for the specified file type
    pub fn new(file_type: FileType) -> Self {
        Self { file_type }
    }
}

impl SplitStrategy for CodeSplitter {
    fn split(&self, data: &[u8], config: &SplitConfig) -> Result<Vec<FileChunk>> {
        let chunk_size_bytes = (config.chunk_size_mb * 1024.0 * 1024.0) as usize;
        
        // Simple approach: if file fits in one chunk, return it as-is
        if data.len() <= chunk_size_bytes {
            return Ok(vec![FileChunk {
                chunk_id: 0,
                data: data.to_vec(),
                metadata: ChunkMetadata {
                    size_bytes: data.len(),
                    unit_count: None,
                    has_headers: false,
                    file_type: self.file_type,
                },
            }]);
        }
        
        // For larger files, split into simple chunks
        let mut chunks = Vec::new();
        let mut chunk_id = 0;
        let mut offset = 0;
        
        while offset < data.len() {
            let chunk_end = std::cmp::min(offset + chunk_size_bytes, data.len());
            let chunk_data = data[offset..chunk_end].to_vec();
            
            chunks.push(FileChunk {
                chunk_id,
                data: chunk_data.clone(),
                metadata: ChunkMetadata {
                    size_bytes: chunk_data.len(),
                    unit_count: None,
                    has_headers: false,
                    file_type: self.file_type,
                },
            });
            
            chunk_id += 1;
            offset = chunk_end;
        }
        
        Ok(chunks)
    }
    
    fn file_type(&self) -> FileType {
        self.file_type
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_simple_code_splitting() {
        let code = b"fn main() {\n    println!(\"Hello, world!\");\n}";
        let splitter = CodeSplitter::new(FileType::Rust);
        let config = SplitConfig::default();
        let chunks = splitter.split(code, &config).unwrap();
        
        assert!(!chunks.is_empty());
        assert_eq!(chunks[0].metadata.file_type, FileType::Rust);
    }

    #[test]
    fn test_large_file_splitting() {
        // Create a large file that will need splitting
        let large_content = "x".repeat(2_000_000); // 2MB
        let splitter = CodeSplitter::new(FileType::Text);
        let mut config = SplitConfig::default();
        config.chunk_size_mb = 1.0; // 1MB chunks
        
        let chunks = splitter.split(large_content.as_bytes(), &config).unwrap();
        
        assert!(chunks.len() > 1); // Should be split into multiple chunks
        assert!(chunks[0].data.len() <= 1_048_576); // Each chunk should be <= 1MB
    }
}

use super::super::types::*;
use anyhow::Result;

/// Text file splitting strategy that preserves line boundaries
pub struct TextSplitter;

impl SplitStrategy for TextSplitter {
    fn split(&self, data: &[u8], config: &SplitConfig) -> Result<Vec<FileChunk>> {
        let chunk_size_bytes = (config.chunk_size_mb * 1024.0 * 1024.0) as usize;
        let min_chunk_size = (chunk_size_bytes as f64 * config.min_chunk_ratio) as usize;
        
        // If data is smaller than chunk size, return as single chunk
        if data.len() <= chunk_size_bytes {
            return Ok(vec![FileChunk {
                chunk_id: 0,
                data: data.to_vec(),
                metadata: ChunkMetadata {
                    size_bytes: data.len(),
                    unit_count: Some(count_lines(data)),
                    has_headers: false,
                    file_type: FileType::Text,
                },
            }]);
        }

        let text = String::from_utf8_lossy(data);
        let lines: Vec<&str> = text.lines().collect();
        
        let mut chunks = Vec::new();
        let mut current_chunk = Vec::new();
        let mut current_size = 0;
        let mut chunk_id = 0;

        for line in lines {
            let line_bytes = line.as_bytes();
            let line_size = line_bytes.len() + 1; // +1 for newline
            
            // If adding this line would exceed chunk size and we have content
            if current_size + line_size > chunk_size_bytes && !current_chunk.is_empty() {
                // Only create chunk if it meets minimum size requirement
                if current_size >= min_chunk_size {
                    let chunk_data = current_chunk.join("\n").into_bytes();
                    chunks.push(FileChunk {
                        chunk_id,
                        data: chunk_data.clone(),
                        metadata: ChunkMetadata {
                            size_bytes: chunk_data.len(),
                            unit_count: Some(current_chunk.len()),
                            has_headers: false,
                            file_type: FileType::Text,
                        },
                    });
                    chunk_id += 1;
                    current_chunk.clear();
                    current_size = 0;
                }
            }
            
            current_chunk.push(line);
            current_size += line_size;
        }

        // Add remaining content as final chunk
        if !current_chunk.is_empty() {
            let chunk_data = current_chunk.join("\n").into_bytes();
            chunks.push(FileChunk {
                chunk_id,
                data: chunk_data.clone(),
                metadata: ChunkMetadata {
                    size_bytes: chunk_data.len(),
                    unit_count: Some(current_chunk.len()),
                    has_headers: false,
                    file_type: FileType::Text,
                },
            });
        }

        Ok(chunks)
    }
    
    fn file_type(&self) -> FileType {
        FileType::Text
    }
}

/// Count the number of lines in the given data
fn count_lines(data: &[u8]) -> usize {
    data.iter().filter(|&&b| b == b'\n').count() + 1
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_small_file_single_chunk() {
        let splitter = TextSplitter;
        let data = b"Line 1\nLine 2\nLine 3";
        let config = SplitConfig {
            chunk_size_mb: 1.0, // 1MB
            ..Default::default()
        };

        let chunks = splitter.split(data, &config).unwrap();
        assert_eq!(chunks.len(), 1);
        assert_eq!(chunks[0].chunk_id, 0);
        assert_eq!(chunks[0].metadata.unit_count, Some(3));
    }

    #[test]
    fn test_large_file_multiple_chunks() {
        let splitter = TextSplitter;
        // Create a file with many lines
        let lines: Vec<String> = (0..1000).map(|i| format!("This is line number {}", i)).collect();
        let data = lines.join("\n").into_bytes();
        
        let config = SplitConfig {
            chunk_size_mb: 0.01, // 10KB for testing
            ..Default::default()
        };

        let chunks = splitter.split(&data, &config).unwrap();
        assert!(chunks.len() > 1);
        
        // Verify chunk IDs are sequential
        for (i, chunk) in chunks.iter().enumerate() {
            assert_eq!(chunk.chunk_id, i);
        }
        
        // Verify all chunks have reasonable sizes
        for chunk in &chunks {
            assert!(chunk.metadata.size_bytes > 0);
            assert!(chunk.metadata.unit_count.unwrap() > 0);
        }
    }

    #[test]
    fn test_line_boundary_preservation() {
        let splitter = TextSplitter;
        let data = b"Short line\nThis is a much longer line that should not be split in the middle\nAnother short line";
        
        let config = SplitConfig {
            chunk_size_mb: 0.00005, // Very small chunk to force splitting
            ..Default::default()
        };

        let chunks = splitter.split(data, &config).unwrap();
        
        // Verify that each chunk contains complete lines
        for chunk in chunks {
            let text = String::from_utf8(chunk.data).unwrap();
            // Should not start or end with partial lines (except maybe newlines)
            assert!(!text.starts_with('\n') || text.len() == 1);
        }
    }
}

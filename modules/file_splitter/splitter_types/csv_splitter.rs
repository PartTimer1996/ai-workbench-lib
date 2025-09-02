use super::super::types::*;
use anyhow::{Result, Context};
use std::str;

/// CSV/TSV file splitting strategy that preserves record boundaries and headers
pub struct CsvSplitter {
    delimiter: u8,
}

impl CsvSplitter {
    /// Create a new CSV splitter with comma delimiter
    pub fn new() -> Self {
        Self { delimiter: b',' }
    }
    
    /// Create a new TSV splitter with tab delimiter
    pub fn new_tsv() -> Self {
        Self { delimiter: b'\t' }
    }
    
}

impl Default for CsvSplitter {
    fn default() -> Self {
        Self::new()
    }
}

impl SplitStrategy for CsvSplitter {
    fn split(&self, data: &[u8], config: &SplitConfig) -> Result<Vec<FileChunk>> {
        let chunk_size_bytes = (config.chunk_size_mb * 1024.0 * 1024.0) as usize;
        let min_chunk_size = (chunk_size_bytes as f64 * config.min_chunk_ratio) as usize;
        
        let text = str::from_utf8(data).context("Invalid UTF-8 in CSV data")?;
        let lines: Vec<&str> = text.lines().collect();
        
        if lines.is_empty() {
            return Ok(vec![]);
        }
        
        // If file is small enough, return as single chunk
        if data.len() <= chunk_size_bytes {
            return Ok(vec![FileChunk {
                chunk_id: 0,
                data: data.to_vec(),
                metadata: ChunkMetadata {
                    size_bytes: data.len(),
                    unit_count: Some(lines.len()),
                    has_headers: true,
                    file_type: self.file_type(),
                },
            }]);
        }

        let header_line = lines[0];
        let header_size = header_line.as_bytes().len() + 1; // +1 for newline
        let data_lines = &lines[1..];
        
        let mut chunks = Vec::new();
        let mut current_chunk_lines = Vec::new();
        let mut current_size = header_size; // Start with header size
        let mut chunk_id = 0;

        for line in data_lines {
            let line_size = line.as_bytes().len() + 1; // +1 for newline
            
            // Check if adding this line would exceed chunk size
            if current_size + line_size > chunk_size_bytes && !current_chunk_lines.is_empty() {
                // Only create chunk if it meets minimum size requirement
                if current_size >= min_chunk_size {
                    let chunk_data = self.create_chunk_with_header(header_line, &current_chunk_lines);
                    chunks.push(FileChunk {
                        chunk_id,
                        data: chunk_data.clone(),
                        metadata: ChunkMetadata {
                            size_bytes: chunk_data.len(),
                            unit_count: Some(current_chunk_lines.len() + 1), // +1 for header
                            has_headers: true,
                            file_type: self.file_type(),
                        },
                    });
                    chunk_id += 1;
                    current_chunk_lines.clear();
                    current_size = header_size; // Reset to header size
                }
            }
            
            current_chunk_lines.push(line);
            current_size += line_size;
        }

        // Add remaining data as final chunk
        if !current_chunk_lines.is_empty() {
            let chunk_data = self.create_chunk_with_header(header_line, &current_chunk_lines);
            chunks.push(FileChunk {
                chunk_id,
                data: chunk_data.clone(),
                metadata: ChunkMetadata {
                    size_bytes: chunk_data.len(),
                    unit_count: Some(current_chunk_lines.len() + 1), // +1 for header
                    has_headers: true,
                    file_type: self.file_type(),
                },
            });
        }

        Ok(chunks)
    }
    
    fn file_type(&self) -> FileType {
        match self.delimiter {
            b',' => FileType::Csv,
            b'\t' => FileType::Tsv,
            _ => FileType::Csv, // Default to CSV for custom delimiters
        }
    }
}

impl CsvSplitter {
    /// Create a chunk that includes the header and the specified data lines
    fn create_chunk_with_header(&self, header: &str, data_lines: &[&str]) -> Vec<u8> {
        let mut result = Vec::new();
        result.extend_from_slice(header.as_bytes());
        result.push(b'\n');
        
        for (i, line) in data_lines.iter().enumerate() {
            result.extend_from_slice(line.as_bytes());
            if i < data_lines.len() - 1 {
                result.push(b'\n');
            }
        }
        
        result
    }
    
    /// Validate that the data appears to be valid CSV/TSV
    pub fn validate_format(&self, data: &[u8]) -> Result<bool> {
        let text = str::from_utf8(data).context("Invalid UTF-8 in CSV data")?;
        let lines: Vec<&str> = text.lines().take(10).collect(); // Check first 10 lines
        
        if lines.is_empty() {
            return Ok(false);
        }
        
        // Check if header line has the expected delimiter
        let header_field_count = lines[0].bytes().filter(|&b| b == self.delimiter).count() + 1;
        
        // Verify that most lines have similar field counts (allowing some variance)
        let mut consistent_lines = 0;
        for line in &lines[1..] {
            let field_count = line.bytes().filter(|&b| b == self.delimiter).count() + 1;
            if field_count == header_field_count || field_count == header_field_count - 1 || field_count == header_field_count + 1 {
                consistent_lines += 1;
            }
        }
        
        // Consider valid if more than 70% of lines have consistent field counts
        Ok(consistent_lines as f64 / (lines.len() - 1) as f64 > 0.7)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_csv_single_chunk() {
        let splitter = CsvSplitter::new();
        let data = b"name,age,city\nJohn,25,NYC\nJane,30,LA";
        let config = SplitConfig {
            chunk_size_mb: 1.0,
            ..Default::default()
        };

        let chunks = splitter.split(data, &config).unwrap();
        assert_eq!(chunks.len(), 1);
        assert_eq!(chunks[0].metadata.unit_count, Some(3)); // header + 2 rows
        assert!(chunks[0].metadata.has_headers);
    }

    #[test]
    fn test_csv_multiple_chunks_with_headers() {
        let splitter = CsvSplitter::new();
        
        // Create a larger CSV
        let mut csv_data = "name,age,city\n".to_string();
        for i in 0..100 {
            csv_data.push_str(&format!("Person{},2{},City{}\n", i, i % 10, i % 5));
        }
        
        let config = SplitConfig {
            chunk_size_mb: 0.001, // Very small for testing
            ..Default::default()
        };

        let chunks = splitter.split(csv_data.as_bytes(), &config).unwrap();
        assert!(chunks.len() > 1);
        
        // Verify each chunk has headers
        for chunk in &chunks {
            assert!(chunk.metadata.has_headers);
            let text = String::from_utf8(chunk.data.clone()).unwrap();
            assert!(text.starts_with("name,age,city\n"));
        }
    }

    #[test]
    fn test_tsv_format() {
        let splitter = CsvSplitter::new_tsv();
        let data = b"name\tage\tcity\nJohn\t25\tNYC\nJane\t30\tLA";
        let config = SplitConfig::default();

        let chunks = splitter.split(data, &config).unwrap();
        assert_eq!(chunks.len(), 1);
        assert_eq!(chunks[0].metadata.file_type, FileType::Tsv);
    }

    #[test]
    fn test_csv_format_validation() {
        let splitter = CsvSplitter::new();
        
        // Valid CSV
        let valid_csv = b"a,b,c\n1,2,3\n4,5,6";
        assert!(splitter.validate_format(valid_csv).unwrap());
        
        // Invalid CSV (inconsistent fields)
        let invalid_csv = b"a,b,c\n1,2\n4,5,6,7,8";
        assert!(!splitter.validate_format(invalid_csv).unwrap());
        
        // Empty data
        let empty_csv = b"";
        assert!(!splitter.validate_format(empty_csv).unwrap());
    }

    #[test]
    fn test_record_boundary_preservation() {
        let splitter = CsvSplitter::new();
        let data = b"name,description\nJohn,A very long description that might cause issues if not handled properly\nJane,Short desc";
        
        let config = SplitConfig {
            chunk_size_mb: 0.00005, // Very small to force splitting
            ..Default::default()
        };

        let chunks = splitter.split(data, &config).unwrap();
        
        // Verify each chunk is valid CSV with header
        for chunk in chunks {
            let text = String::from_utf8(chunk.data).unwrap();
            let lines: Vec<&str> = text.lines().collect();
            assert!(lines.len() >= 1); // At least header
            assert_eq!(lines[0], "name,description"); // Correct header
        }
    }
}

use super::*;

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

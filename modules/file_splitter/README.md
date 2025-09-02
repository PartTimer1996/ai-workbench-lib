# File Splitter Library

A robust, intelligent file splitting library designed for AI workloads that need to process large files efficiently. The library automatically detects file formats and applies appropriate splitting strategies while preserving logical boundaries.

## Features

### 🎯 **Intelligent File Type Detection**
- **Extension-based**: Automatic detection from file extensions (.txt, .csv, .tsv, .json, .xml)
- **Content-based**: Fallback analysis when extensions are unclear or missing
- **Format validation**: Ensures detected formats are actually valid before processing

### 📊 **Specialized Splitting Strategies**

#### Text Files (.txt, .md, .markdown)
- Preserves line boundaries
- Configurable chunk sizes
- Handles edge cases like very long lines
- Maintains content integrity

#### CSV/TSV Files (.csv, .tsv)
- **Header preservation**: Every chunk includes the original header row
- **Record boundary respect**: Never splits in the middle of a data record
- **Format validation**: Verifies consistent field counts before processing
- **Custom delimiters**: Support for comma, tab, or custom delimiters

#### JSON Files (.json, .jsonl)
- Line-based splitting for JSON Lines format
- Object boundary preservation for standard JSON
- Future extensibility for streaming JSON parsing

#### XML Files (.xml, .html)
- Text-based splitting with future extensibility for XML parsing
- Maintains tag integrity where possible

### ⚙️ **Configurable Behavior**
```rust
let config = SplitConfig {
    chunk_size_mb: 5.0,           // Target chunk size in MB
    preserve_boundaries: true,     // Respect logical boundaries
    min_chunk_ratio: 0.1,        // Minimum chunk size (10% of target)
};
```

### 🔍 **Rich Metadata**
Each chunk includes comprehensive metadata:
- **Size information**: Exact byte count
- **Content analysis**: Line/record counts
- **Format details**: File type and structure info
- **Header information**: Whether chunk includes headers (for tabular data)

## Usage Examples

### Basic Usage
```rust
use file_splitter::{FileSplitter, SplitConfig};
use std::path::Path;

// Create splitter with default config
let splitter = FileSplitter::new();

// Split a file (type detected from extension)
let chunks = splitter.split_file(Path::new("data.csv"), file_data)?;

// Process each chunk
for chunk in chunks {
    println!("Chunk {}: {} bytes, {} records", 
        chunk.chunk_id, 
        chunk.metadata.size_bytes,
        chunk.metadata.unit_count.unwrap_or(0)
    );
}
```

### Custom Configuration
```rust
let config = SplitConfig {
    chunk_size_mb: 2.0,           // 2MB chunks
    preserve_boundaries: true,
    min_chunk_ratio: 0.15,        // 15% minimum
};

let splitter = FileSplitter::with_config(config);
let chunks = splitter.split_file(Path::new("large_dataset.csv"), data)?;
```

### Explicit Type Handling
```rust
use file_splitter::FileType;

// Force specific type handling
let chunks = splitter.split_with_type(data, FileType::Csv)?;

// Auto-detect from content
let detected_type = splitter.detect_file_type(data, Some("csv"));
```

### CSV-Specific Features
```rust
use file_splitter::CsvSplitter;

// Create CSV splitter with validation
let csv_splitter = CsvSplitter::new();

// Validate format before processing
if csv_splitter.validate_format(data)? {
    let chunks = csv_splitter.split(data, &config)?;
    
    // Each chunk includes headers automatically
    for chunk in chunks {
        assert!(chunk.metadata.has_headers);
        println!("Chunk {} has {} data rows", 
            chunk.chunk_id, 
            chunk.metadata.unit_count.unwrap() - 1  // -1 for header
        );
    }
}
```

## Architecture

### Single Responsibility Design
Each component has a focused responsibility:

- **`FileSplitter`**: Main orchestrator, handles type detection and strategy selection
- **`TextSplitter`**: Handles plain text files with line boundary preservation
- **`CsvSplitter`**: Handles tabular data with header preservation and record integrity
- **`SplitStrategy` trait**: Extensible interface for new file format support

### Error Handling
Comprehensive error handling with meaningful context:
```rust
// Graceful fallbacks
if csv_validation_fails {
    fallback_to_text_splitting();
}

// Rich error context
.context("Failed to split CSV data due to inconsistent field counts")
```

### Performance Characteristics
- **Memory efficient**: Streaming approach, doesn't load entire file into memory unnecessarily
- **Boundary aware**: Smart splitting that respects logical file structure
- **Configurable**: Tunable chunk sizes and behavior for different use cases

## Testing

Comprehensive test suite covering:

### Unit Tests
- Individual splitter strategies
- Boundary condition handling
- Format validation
- Configuration options

### Integration Tests
- Large file handling (10K+ lines)
- Mixed file type processing
- Performance benchmarks
- Memory usage validation

### Edge Case Testing
- Very long single lines
- Empty files and headers-only files
- Files with irregular structure
- Binary data rejection

### Run Tests
```bash
# Run all file splitter tests
cargo test file_splitter

# Run integration tests
cargo test integration_tests

# Run with verbose output
./test_file_splitter.sh
```

## Future Enhancements

### Planned Features
1. **JSON streaming parser**: Handle large JSON files more intelligently
2. **XML tree-aware splitting**: Respect XML element boundaries
3. **Binary format support**: Basic chunking for binary files
4. **Compression aware**: Handle compressed files transparently
5. **Memory mapping**: Support for extremely large files
6. **Parallel processing**: Multi-threaded splitting for very large files

### Extensibility
The library is designed for easy extension:
```rust
// Implement custom splitter
struct MyCustomSplitter;

impl SplitStrategy for MyCustomSplitter {
    fn split(&self, data: &[u8], config: &SplitConfig) -> Result<Vec<FileChunk>> {
        // Custom splitting logic
    }
    
    fn file_type(&self) -> FileType {
        FileType::Custom
    }
}
```

## Integration with AI Workbench

This library is specifically designed for AI processing workloads:

- **Large document processing**: Handle multi-gigabyte text files
- **Dataset processing**: Efficiently process large CSV datasets
- **Batch AI operations**: Split files for parallel AI model processing
- **Memory optimization**: Prevent out-of-memory errors with large files
- **Result tracking**: Rich metadata for processing pipeline monitoring

The library integrates seamlessly with the Bedrock Runner for distributed AI processing of large files across multiple containers.

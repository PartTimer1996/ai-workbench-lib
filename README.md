# AI Workbench Library

A comprehensive Rust library for AI workbench operations including file processing, intelligent splitting, and AWS Bedrock integration.

## Features

- **File Discovery**: S3-based file discovery and processing
- **Intelligent File Splitting**: Type-aware chunking for various file formats (text, CSV, JSON, code)
- **AWS Bedrock Integration**: Model runner for seamless AI model interactions
- **Job Processing**: Complete workflow orchestration for AI processing tasks

## Installation

Add this to your `Cargo.toml`:

```toml
[dependencies]
ai-workbench-lib = "0.1.0"
```

## Quick Start

```rust
use ai_workbench_lib::{JobProcessor, JobConfig};
use aws_config::BehaviorVersion;
use aws_sdk_s3::Client as S3Client;
use aws_sdk_bedrockruntime::Client as BedrockClient;
use std::sync::Arc;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Initialize AWS clients
    let config = aws_config::load_defaults(BehaviorVersion::latest()).await;
    let s3_client = Arc::new(S3Client::new(&config));
    let bedrock_client = Arc::new(BedrockClient::new(&config));
    
    // Create job configuration
    let job_config = JobConfig {
        job_id: "my-job".to_string(),
        prompt: "Analyze this file: {{file}}".to_string(),
        workspace_bucket: "my-workspace-bucket".to_string(),
        input_spec: "path/to/file.txt".to_string(),
        output_prefix: "outputs/".to_string(),
        model_id: "amazon.nova-micro-v1:0".to_string(),
        workspace_id: "ws-123".to_string(),
        user_id: "user-123".to_string(),
        chunk_size_mb: Some(5.0),
        max_parallel: Some(4),
        include_file_context: Some(true),
    };
    
    // Process the job
    let processor = JobProcessor::new(s3_client, bedrock_client, job_config);
    let (output_key, metrics) = processor.run().await?;
    
    println!("Job completed! Output: {}", output_key);
    println!("Processed {} files with {} tokens", 
             metrics.files_processed, metrics.total_tokens);
    
    Ok(())
}
```

## Components

### File Discovery

Discover and process files from S3 buckets:

```rust
use ai_workbench_lib::FileDiscovery;

let discovery = FileDiscovery::new(s3_client, "my-bucket".to_string());
let files = discovery.discover_files("path/to/files/").await?;
```

### File Splitting

Intelligent file splitting with type-aware chunking:

```rust
use ai_workbench_lib::{FileSplitter, SplitConfig};

let splitter = FileSplitter::with_config(SplitConfig {
    chunk_size_mb: 5.0,
    _preserve_boundaries: true,
    min_chunk_ratio: 0.1,
});

let chunks = splitter.split_file(&file_path, &file_data)?;
```

### Model Runner

Direct AWS Bedrock model interactions:

```rust
use ai_workbench_lib::ModelRunner;

let model_runner = ModelRunner::new(bedrock_client);
let (response, tokens) = model_runner
    .invoke_model("amazon.nova-micro-v1:0", "Your prompt here", 4000)
    .await?;
```

## File Type Support

The library automatically detects and optimally processes:

- **Text files**: Line-based chunking with context preservation
- **CSV/TSV files**: Row-based chunking with header preservation
- **JSON files**: Object/array-based intelligent splitting
- **Code files**: Syntax-aware chunking (Rust, Python, JavaScript, etc.)
- **Binary files**: Basic size-based chunking

## Requirements

- Rust 1.70+
- AWS credentials configured
- Tokio async runtime

## License

MIT License - see LICENSE file for details.

## Contributing

Contributions welcome! Please read our contributing guidelines and submit pull requests to our GitHub repository.

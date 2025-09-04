//! Example binary for AI Workbench Library

use ai_workbench_lib::JobConfig;
use anyhow::Result;

#[tokio::main]
async fn main() -> Result<()> {
    println!("AI Workbench Library v{}", ai_workbench_lib::VERSION);

    println!("AI Workbench Library Example");
    println!("This is a placeholder example binary.");
    println!("Use the library in your own projects for actual workbench operations.");

    // Example configuration (not executed)
    let _example_config = JobConfig {
        job_id: "example-job".to_string(),
        prompt: "Analyze this file".to_string(),
        workspace_bucket: "example-bucket".to_string(),
        input_spec: "example/file.txt".to_string(),
        output_prefix: "outputs/".to_string(),
        model_id: "amazon.nova-micro-v1:0".to_string(),
        workspace_id: "ws-example".to_string(),
        user_id: "user-example".to_string(),
        chunk_size_mb: Some(5.0),
        max_parallel: Some(4),
        include_file_context: Some(true),
        max_tokens: Some(4000),
        temperature: Some(0.1),
        top_p: Some(0.9),
    };

    println!("Example configuration created successfully!");
    Ok(())
}
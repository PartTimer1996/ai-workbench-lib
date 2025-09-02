use anyhow::{Context, Result};
use aws_sdk_s3::Client as S3Client;
use std::collections::HashMap;
use std::path::Path;
use super::file_splitter::types::FileType;
use tracing::info;

/// Handles discovery and processing of multiple files and folders
pub struct FileDiscovery {
    s3_client: S3Client,
    workspace_bucket: String,
}

impl FileDiscovery {
    pub fn new(s3_client: S3Client, workspace_bucket: String) -> Self {
        Self {
            s3_client,
            workspace_bucket,
        }
    }
    
    /// Discover all files to process from the input specification
    pub async fn discover_files(&self, input_spec: &str) -> Result<Vec<FileInfo>> {
         if input_spec.ends_with('/') {
            // Folder specified
            self.process_folder(input_spec).await
        } else {
            // Single file
            self.process_single_file(input_spec).await
        }
    }
    
    /// Process a single file
    async fn process_single_file(&self, file_key: &str) -> Result<Vec<FileInfo>> {
        let metadata = self.get_file_metadata(file_key).await?;
        
        Ok(vec![FileInfo {
            s3_key: file_key.to_string(),
            relative_path: file_key.to_string(),
            size_bytes: metadata.size_bytes,
            file_type: metadata.file_type,
        }])
    }
    
    
    /// Process a folder (recursively discover all files)
    async fn process_folder(&self, folder_prefix: &str) -> Result<Vec<FileInfo>> {
        info!("Discovering files in folder: s3://{}/{}", self.workspace_bucket, folder_prefix);
        
        let mut files = Vec::new();
        let mut continuation_token: Option<String> = None;
        
        loop {
            let mut request = self.s3_client
                .list_objects_v2()
                .bucket(&self.workspace_bucket)
                .prefix(folder_prefix)
                .max_keys(1000);
                
            if let Some(token) = &continuation_token {
                request = request.continuation_token(token);
            }
            
            let response = request.send().await
                .context("Failed to list objects in S3")?;
            
            let objects = response.contents();
            
            for object in objects {
                let key = object.key().unwrap_or_default();
                
                // Skip directories (keys ending with /)
                if key.ends_with('/') {
                    continue;
                }
                
                let metadata = FileMetadata::from_s3_object(object, key);
                
                if metadata.is_processable {
                    files.push(FileInfo {
                        s3_key: key.to_string(),
                        relative_path: key.strip_prefix(folder_prefix).unwrap_or(key).to_string(),
                        size_bytes: metadata.size_bytes,
                        file_type: metadata.file_type,
                    });
                } else {
                    info!("Skipping non-processable file: {}", key);
                }
            }
            
            continuation_token = response.next_continuation_token().map(|s| s.to_string());
            if continuation_token.is_none() {
                break;
            }
        }
        
        info!("Discovered {} processable files in folder", files.len());
        Ok(files)
    }
    
    /// Get metadata for a single file
    async fn get_file_metadata(&self, file_key: &str) -> Result<FileMetadata> {
        let response = self.s3_client
            .head_object()
            .bucket(&self.workspace_bucket)
            .key(file_key)
            .send()
            .await
            .context(format!("Failed to get metadata for {}", file_key))?;
        
        let size_bytes = response.content_length().unwrap_or(0) as usize;
        let path = Path::new(file_key);
        let file_type = super::file_splitter::types::FileType::from_extension(path);
        let is_processable = Self::is_file_processable(&file_type, size_bytes);
        
        Ok(FileMetadata {
            size_bytes,
            file_type,
            is_processable,
        })
    }
    
    /// Check if a file should be processed based on type and size
    fn is_file_processable(file_type: &FileType, size_bytes: usize) -> bool {
        
        // Skip binary files that are too large (> 100MB)
        if matches!(file_type, FileType::Binary) && size_bytes > 100 * 1024 * 1024 {
            return false;
        }
        
        // Skip empty files
        if size_bytes == 0 {
            return false;
        }
        
        // Process most text-based files
        !matches!(file_type, FileType::Binary)
    }
    
    /// Generate processing summary
    pub fn generate_processing_summary(&self, files: &[FileInfo]) -> ProcessingSummary {
        let mut summary = ProcessingSummary {
            total_files: files.len(),
            total_size_bytes: files.iter().map(|f| f.size_bytes).sum(),
            files_by_type: HashMap::new(),
            files_by_category: HashMap::new(),
        };
        
        for file in files {
            // Count by file type
            *summary.files_by_type.entry(file.file_type).or_insert(0) += 1;
            
            // Count by category
            let category = file.file_type.language_category();
            *summary.files_by_category.entry(category.to_string()).or_insert(0) += 1;
        }
        
        summary
    }
}

/// Information about a discovered file
#[derive(Debug, Clone,)]
pub struct FileInfo {
    pub s3_key: String,
    pub relative_path: String,
    pub size_bytes: usize,
    pub file_type: FileType,
}

/// File metadata
#[derive(Debug, Clone)]
struct FileMetadata {
    size_bytes: usize,
    pub file_type: FileType,
    is_processable: bool,
}

impl FileMetadata {
    fn from_s3_object(object: &aws_sdk_s3::types::Object, key: &str) -> Self {
        let size_bytes = object.size().unwrap_or(0) as usize;
        let path = Path::new(key);
        let file_type = FileType::from_extension(path);
        let is_processable = FileDiscovery::is_file_processable(&file_type, size_bytes);
        
        Self {
            size_bytes,
            file_type,
            is_processable,
        }
    }
}

/// Summary of files to be processed
#[derive(Debug, Clone)]
pub struct ProcessingSummary {
    pub total_files: usize,
    pub total_size_bytes: usize,
    pub files_by_type: HashMap<FileType, usize>,
    pub files_by_category: HashMap<String, usize>,
}

impl ProcessingSummary {
    pub fn format_summary(&self) -> String {
        let mut summary = format!(
            "Processing Summary:\n- Total files: {}\n- Total size: {:.2} MB\n",
            self.total_files,
            self.total_size_bytes as f64 / (1024.0 * 1024.0)
        );
        
        summary.push_str("\nFiles by category:\n");
        for (category, count) in &self.files_by_category {
            summary.push_str(&format!("- {}: {} files\n", category, count));
        }
        
        summary.push_str("\nFiles by type:\n");
        for (file_type, count) in &self.files_by_type {
            summary.push_str(&format!("- {:?}: {} files\n", file_type, count));
        }
        
        summary
    }
}

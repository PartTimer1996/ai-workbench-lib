use std::path::Path;
use anyhow::Result;

/// Configuration for file splitting operations
#[derive(Debug, Clone)]
pub struct SplitConfig {
    /// Target chunk size in megabytes
    pub chunk_size_mb: f64,
    /// Whether to preserve logical boundaries (lines, records, etc.)
    pub _preserve_boundaries: bool,
    /// Minimum chunk size as percentage of target (to avoid tiny trailing chunks)
    pub min_chunk_ratio: f64,
}

impl Default for SplitConfig {
    fn default() -> Self {
        Self {
            chunk_size_mb: 5.0,
            _preserve_boundaries: true,
            min_chunk_ratio: 0.1, // 10% minimum
        }
    }
}

/// Represents a chunk of data from a split file
#[derive(Debug, Clone)]
pub struct FileChunk {
    /// Unique identifier for this chunk within the file
    pub chunk_id: usize,
    /// The actual data bytes
    pub data: Vec<u8>,
    /// Metadata about this chunk
    pub metadata: ChunkMetadata,
}

/// Metadata associated with a file chunk
#[derive(Debug, Clone)]
pub struct ChunkMetadata {
    /// Size of the chunk in bytes
    pub size_bytes: usize,
    /// Number of logical units (lines, records, etc.) in this chunk
    pub unit_count: Option<usize>,
    /// Whether this chunk contains headers (for CSV/tabular data)
    pub has_headers: bool,
    /// File type that was detected
    pub file_type: FileType,
}


/// File type enumeration for different splitting strategies
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum FileType {
    // Text and Data formats
    Text,
    Csv,
    Tsv,
    Json,
    Xml,
    Yaml,
    Toml,
    
    // Programming Languages
    Rust,
    Python,
    JavaScript,
    TypeScript,
    Java,
    Cpp,
    C,
    CSharp,
    Go,
    Swift,
    Kotlin,
    Scala,
    Ruby,
    Php,
    Shell,
    
    // Web Technologies
    Html,
    Css,
    Scss,
    Less,
    
    // Configuration and Build files
    Dockerfile,
    Makefile,
    CMake,
    
    // Documentation
    Markdown,
    RestructuredText,
    
    // Database
    Sql,
    
    // Other
    Binary,
}

impl FileType {
    /// Detect file type from file extension
    pub fn from_extension(path: &Path) -> Self {
        match path.extension().and_then(|ext| ext.to_str()) {
            // Text and Data formats
            Some("txt") | Some("text") => FileType::Text,
            Some("csv") => FileType::Csv,
            Some("tsv") | Some("tab") => FileType::Tsv,
            Some("json") | Some("jsonl") | Some("ndjson") => FileType::Json,
            Some("xml") => FileType::Xml,
            Some("yaml") | Some("yml") => FileType::Yaml,
            Some("toml") => FileType::Toml,
            
            // Programming Languages
            Some("rs") => FileType::Rust,
            Some("py") | Some("pyw") | Some("pyi") => FileType::Python,
            Some("js") | Some("mjs") | Some("cjs") => FileType::JavaScript,
            Some("ts") | Some("tsx") => FileType::TypeScript,
            Some("java") => FileType::Java,
            Some("cpp") | Some("cxx") | Some("cc") | Some("c++") => FileType::Cpp,
            Some("c") | Some("h") => FileType::C,
            Some("cs") => FileType::CSharp,
            Some("go") => FileType::Go,
            Some("swift") => FileType::Swift,
            Some("kt") | Some("kts") => FileType::Kotlin,
            Some("scala") | Some("sc") => FileType::Scala,
            Some("rb") | Some("ruby") => FileType::Ruby,
            Some("php") | Some("phtml") => FileType::Php,
            Some("sh") | Some("bash") | Some("zsh") | Some("fish") => FileType::Shell,
            
            // Web Technologies
            Some("html") | Some("htm") => FileType::Html,
            Some("css") => FileType::Css,
            Some("scss") | Some("sass") => FileType::Scss,
            Some("less") => FileType::Less,
            
            // Configuration and Build files
            Some("dockerfile") => FileType::Dockerfile,
            Some("makefile") | Some("mk") => FileType::Makefile,
            Some("cmake") => FileType::CMake,
            
            // Documentation
            Some("md") | Some("markdown") => FileType::Markdown,
            Some("rst") => FileType::RestructuredText,
            
            // Database
            Some("sql") => FileType::Sql,
            
            // Handle files without extensions or by filename
            _ => {
                match path.file_name().and_then(|name| name.to_str()) {
                    Some("Dockerfile") | Some("dockerfile") => FileType::Dockerfile,
                    Some("Makefile") | Some("makefile") => FileType::Makefile,
                    Some("CMakeLists.txt") => FileType::CMake,
                    Some("package.json") | Some("tsconfig.json") | Some("settings.json") => FileType::Json,
                    Some("Cargo.toml") | Some("pyproject.toml") => FileType::Toml,
                    Some("requirements.txt") | Some("README") => FileType::Text,
                    _ => FileType::Binary,
                }
            }
        }
    }
    
    /// Get the language category for documentation purposes
    pub fn language_category(&self) -> &'static str {
        match self {
            FileType::Rust | FileType::C | FileType::Cpp | FileType::Go | FileType::Swift => "Systems Programming",
            FileType::Python | FileType::Ruby | FileType::Php => "Scripting Languages",
            FileType::JavaScript | FileType::TypeScript | FileType::Html | FileType::Css | FileType::Scss | FileType::Less => "Web Technologies",
            FileType::Java | FileType::CSharp | FileType::Kotlin | FileType::Scala => "JVM/Enterprise Languages",
            FileType::Shell => "Shell Scripts",
            FileType::Sql => "Database",
            FileType::Json | FileType::Yaml | FileType::Toml | FileType::Xml => "Configuration/Data",
            FileType::Markdown | FileType::RestructuredText => "Documentation",
            FileType::Dockerfile | FileType::Makefile | FileType::CMake => "Build/Deploy",
            FileType::Csv | FileType::Tsv => "Data Files",
            FileType::Text => "Plain Text",
            FileType::Binary => "Binary",
        }
    }
    
    /// Check if this file type represents source code
    pub fn is_source_code(&self) -> bool {
        matches!(self, 
            FileType::Rust | FileType::Python | FileType::JavaScript | FileType::TypeScript |
            FileType::Java | FileType::Cpp | FileType::C | FileType::CSharp | FileType::Go |
            FileType::Swift | FileType::Kotlin | FileType::Scala | FileType::Ruby | FileType::Php |
            FileType::Shell | FileType::Html | FileType::Css | FileType::Scss | FileType::Less |
            FileType::Sql
        )
    }
    
}

/// Trait for file splitting implementations
pub trait SplitStrategy {
    /// Split the given data into chunks according to the strategy
    fn split(&self, data: &[u8], config: &SplitConfig) -> Result<Vec<FileChunk>>;
    
    /// Get the file type this strategy handles
    fn file_type(&self) -> FileType;
}

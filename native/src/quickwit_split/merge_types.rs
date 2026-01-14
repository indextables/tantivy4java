use serde::{Deserialize, Serialize};

/// Configuration for split merge operations (for merge binary)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MergeSplitConfig {
    pub index_uid: String,
    pub source_id: String,
    pub node_id: String,
    pub aws_config: Option<MergeAwsConfig>,
}

/// AWS configuration for S3 access (for merge binary)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MergeAwsConfig {
    pub access_key: String,
    pub secret_key: String,
    pub session_token: Option<String>,
    pub region: String,
    pub endpoint_url: Option<String>,
    pub force_path_style: bool,
}

/// Information about a split that was skipped during merge
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SkippedSplit {
    pub url: String,       // The URL/path of the split that was skipped
    pub reason: String,    // Human-readable reason why it was skipped
}

impl SkippedSplit {
    pub fn new(url: impl Into<String>, reason: impl Into<String>) -> Self {
        Self {
            url: url.into(),
            reason: reason.into(),
        }
    }
}

/// Metadata about a split after merge
#[derive(Debug, Serialize, Deserialize)]
pub struct SplitMetadata {
    pub split_id: String,
    pub num_docs: usize,
    pub uncompressed_size_bytes: u64,
    pub time_range_start: Option<i64>,
    pub time_range_end: Option<i64>,
    pub create_timestamp: u64,
    pub footer_offsets: Option<(u64, u64)>,
    pub skipped_splits: Vec<SkippedSplit>,  // Splits that were skipped with reasons
}

impl SplitMetadata {
    pub fn get_num_docs(&self) -> usize {
        self.num_docs
    }

    pub fn get_uncompressed_size_bytes(&self) -> u64 {
        self.uncompressed_size_bytes
    }

    pub fn get_split_id(&self) -> &str {
        &self.split_id
    }
}
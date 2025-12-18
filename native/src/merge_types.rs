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
    pub skipped_splits: Vec<String>,  // URLs/paths of splits that were skipped due to corruption or errors
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
// parquet_storage.rs - Storage factory for parquet file access
//
// Creates a separate Storage instance for accessing parquet files,
// which may use different credentials than the split storage.

use std::sync::Arc;
use anyhow::Result;
use quickwit_storage::Storage;

use std::str::FromStr;

use crate::debug_println;

/// Configuration for parquet file storage access
#[derive(Debug, Clone)]
pub struct ParquetStorageConfig {
    /// AWS S3 config (if parquet files are on S3)
    pub aws_access_key: Option<String>,
    pub aws_secret_key: Option<String>,
    pub aws_session_token: Option<String>,
    pub aws_region: Option<String>,
    pub aws_endpoint: Option<String>,
    pub aws_force_path_style: bool,
    /// Azure config (if parquet files are on Azure)
    pub azure_account_name: Option<String>,
    pub azure_access_key: Option<String>,
    pub azure_bearer_token: Option<String>,
}

impl Default for ParquetStorageConfig {
    fn default() -> Self {
        Self {
            aws_access_key: None,
            aws_secret_key: None,
            aws_session_token: None,
            aws_region: None,
            aws_endpoint: None,
            aws_force_path_style: false,
            azure_account_name: None,
            azure_access_key: None,
            azure_bearer_token: None,
        }
    }
}

/// Create a Storage instance for accessing parquet files.
/// Reuses the existing storage resolver infrastructure and shares the L2 disk cache.
///
/// Returns `(wrapped_storage, prewarm_storage)`:
/// - `wrapped_storage`: Storage wrapped with L2 disk cache (for query-path reads)
/// - `prewarm_storage`: Prewarm-mode clone if cache wrapper was applied, None otherwise.
///   Uses blocking `put()` for guaranteed L2 writes and records prewarm metrics.
pub fn create_parquet_storage(
    config: &ParquetStorageConfig,
    table_root: &str,
) -> Result<(Arc<dyn Storage>, Option<Arc<dyn Storage>>)> {
    debug_println!("🔧 PARQUET_STORAGE: Creating storage for table_root: {}", table_root);

    let storage_resolver = if config.aws_access_key.is_some() && config.aws_secret_key.is_some() {
        let mut s3_config = quickwit_config::S3StorageConfig::default();
        s3_config.access_key_id = config.aws_access_key.clone();
        s3_config.secret_access_key = config.aws_secret_key.clone();
        s3_config.session_token = config.aws_session_token.clone();
        s3_config.region = config.aws_region.clone();
        s3_config.endpoint = config.aws_endpoint.clone();
        s3_config.force_path_style_access = config.aws_force_path_style;
        crate::global_cache::get_configured_storage_resolver(Some(s3_config), None)
    } else if config.azure_account_name.is_some() {
        let azure_config = quickwit_config::AzureStorageConfig {
            account_name: config.azure_account_name.clone(),
            access_key: config.azure_access_key.clone(),
            bearer_token: config.azure_bearer_token.clone(),
        };
        crate::global_cache::get_configured_storage_resolver(None, Some(azure_config))
    } else {
        crate::global_cache::get_configured_storage_resolver(None, None)
    };

    // Resolve storage for the table root URI
    let runtime = crate::runtime_manager::QuickwitRuntimeManager::global();
    let storage = runtime.handle().block_on(async {
        let uri = quickwit_common::uri::Uri::from_str(table_root)
            .map_err(|e| anyhow::anyhow!("Invalid table root URI '{}': {}", table_root, e))?;
        storage_resolver
            .resolve(&uri)
            .await
            .map_err(|e| anyhow::anyhow!("Failed to resolve storage for '{}': {}", table_root, e))
    })?;

    // Wrap with L2 disk cache if available (shares cache with split storage)
    // Create prewarm variant that uses blocking put() and prewarm metrics
    let (resolved_storage, prewarm_storage): (Arc<dyn Storage>, Option<Arc<dyn Storage>>) = match crate::global_cache::get_global_disk_cache() {
        Some(disk_cache) => {
            debug_println!("🔧 PARQUET_STORAGE: Wrapping with L2 disk cache");
            let wrapped = Arc::new(crate::persistent_cache_storage::StorageWithPersistentCache::with_disk_cache_only(
                storage,
                disk_cache,
                table_root.to_string(),
                format!("parquet-{}", table_root),
            ));
            let prewarm = wrapped.for_prewarm();
            (wrapped, Some(prewarm as Arc<dyn Storage>))
        }
        None => {
            debug_println!("🔧 PARQUET_STORAGE: No disk cache, using raw storage");
            (storage, None)
        }
    };

    Ok((resolved_storage, prewarm_storage))
}

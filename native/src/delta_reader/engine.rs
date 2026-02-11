// delta_reader/engine.rs - DefaultEngine construction with credentials
//
// Builds an object_store-backed DefaultEngine for Delta Lake access,
// supporting local, S3, and Azure storage with credential configuration.

use std::sync::Arc;
use anyhow::Result;
use url::Url;

use delta_kernel::engine::default::DefaultEngine;
use delta_kernel::engine::default::executor::tokio::TokioBackgroundExecutor;
use object_store::aws::AmazonS3Builder;
use object_store::azure::MicrosoftAzureBuilder;
use object_store::local::LocalFileSystem;
use object_store::ObjectStore;

use crate::debug_println;

/// Concrete engine type returned by create_engine.
pub type DeltaEngine = DefaultEngine<TokioBackgroundExecutor>;

/// Configuration for Delta table storage access.
/// Mirrors the ParquetStorageConfig pattern from parquet_companion.
#[derive(Debug, Clone, Default)]
pub struct DeltaStorageConfig {
    /// AWS S3 credentials
    pub aws_access_key: Option<String>,
    pub aws_secret_key: Option<String>,
    pub aws_session_token: Option<String>,
    pub aws_region: Option<String>,
    pub aws_endpoint: Option<String>,
    pub aws_force_path_style: bool,
    /// Azure credentials
    pub azure_account_name: Option<String>,
    pub azure_access_key: Option<String>,
    pub azure_bearer_token: Option<String>,
}

/// Create a DefaultEngine backed by the appropriate ObjectStore for the given URL.
pub fn create_engine(table_url: &Url, config: &DeltaStorageConfig) -> Result<DeltaEngine> {
    let scheme = table_url.scheme();
    debug_println!("🔧 DELTA_ENGINE: Creating engine for scheme={}, url={}", scheme, table_url);

    let store: Arc<dyn ObjectStore> = match scheme {
        "s3" | "s3a" => {
            let mut builder = AmazonS3Builder::new()
                .with_bucket_name(
                    table_url
                        .host_str()
                        .ok_or_else(|| anyhow::anyhow!("S3 URL missing bucket: {}", table_url))?,
                );

            if let Some(ref key) = config.aws_access_key {
                builder = builder.with_access_key_id(key);
            }
            if let Some(ref secret) = config.aws_secret_key {
                builder = builder.with_secret_access_key(secret);
            }
            if let Some(ref token) = config.aws_session_token {
                builder = builder.with_token(token);
            }
            if let Some(ref region) = config.aws_region {
                builder = builder.with_region(region);
            }
            if let Some(ref endpoint) = config.aws_endpoint {
                builder = builder.with_endpoint(endpoint);
            }
            if config.aws_force_path_style {
                builder = builder.with_virtual_hosted_style_request(false);
            }

            Arc::new(builder.build()?)
        }
        "az" | "azure" | "abfs" | "abfss" => {
            let mut builder = MicrosoftAzureBuilder::new()
                .with_container_name(
                    table_url
                        .host_str()
                        .ok_or_else(|| {
                            anyhow::anyhow!("Azure URL missing container: {}", table_url)
                        })?,
                );

            if let Some(ref account) = config.azure_account_name {
                builder = builder.with_account(account);
            }
            if let Some(ref key) = config.azure_access_key {
                builder = builder.with_access_key(key);
            }
            if let Some(ref token) = config.azure_bearer_token {
                builder = builder.with_bearer_token_authorization(token);
            }

            Arc::new(builder.build()?)
        }
        "file" | "" => {
            Arc::new(LocalFileSystem::new())
        }
        other => {
            return Err(anyhow::anyhow!("Unsupported URL scheme '{}' for Delta table: {}", other, table_url));
        }
    };

    debug_println!("🔧 DELTA_ENGINE: ObjectStore created, building DefaultEngine");
    let engine = DefaultEngine::new(store);
    Ok(engine)
}

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

/// Create an ObjectStore for the given URL and storage configuration.
///
/// Supports S3/S3a, Azure (az/abfs/abfss), and local file:// URLs.
/// Used by both delta_reader and parquet_schema_reader.
pub fn create_object_store(url: &Url, config: &DeltaStorageConfig) -> Result<Arc<dyn ObjectStore>> {
    let scheme = url.scheme();
    debug_println!("🔧 DELTA_ENGINE: Creating ObjectStore for scheme={}, url={}", scheme, url);

    let store: Arc<dyn ObjectStore> = match scheme {
        "s3" | "s3a" => {
            // from_env() picks up AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY /
            // AWS_SESSION_TOKEN / AWS_REGION etc., so deployments relying on
            // env-var credentials work; explicit config below overrides.
            let mut builder = AmazonS3Builder::from_env()
                .with_bucket_name(
                    url
                        .host_str()
                        .ok_or_else(|| anyhow::anyhow!("S3 URL missing bucket: {}", url))?,
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
            // Canonical ABFS URLs are abfss://<container>@<account>.dfs.core.windows.net/path:
            // the container is the USERNAME component and the host is the account
            // endpoint. For az://container/path the host is the container.
            let (container, account_from_url) = match scheme {
                "abfs" | "abfss" if !url.username().is_empty() => {
                    let account = url
                        .host_str()
                        .and_then(|h| h.split('.').next())
                        .map(|s| s.to_string());
                    (url.username().to_string(), account)
                }
                _ => (
                    url.host_str()
                        .ok_or_else(|| {
                            anyhow::anyhow!("Azure URL missing container: {}", url)
                        })?
                        .to_string(),
                    None,
                ),
            };

            // from_env() picks up AZURE_STORAGE_ACCOUNT_NAME / _KEY etc.;
            // explicit config below overrides.
            let mut builder = MicrosoftAzureBuilder::from_env()
                .with_container_name(&container);

            if let Some(ref account) = config.azure_account_name {
                builder = builder.with_account(account);
            } else if let Some(account) = account_from_url {
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
            return Err(anyhow::anyhow!("Unsupported URL scheme '{}': {}", other, url));
        }
    };

    Ok(store)
}

/// Create a DefaultEngine backed by the appropriate ObjectStore for the given URL.
pub fn create_engine(table_url: &Url, config: &DeltaStorageConfig) -> Result<DeltaEngine> {
    let store = create_object_store(table_url, config)?;
    debug_println!("🔧 DELTA_ENGINE: ObjectStore created, building DefaultEngine");
    let engine = DefaultEngine::new(store);
    Ok(engine)
}

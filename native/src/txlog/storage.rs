// txlog/storage.rs - Storage integration using existing StorageResolver + ObjectStore
//
// Reuses the credential-cached StorageResolver from global_cache/storage_resolver.rs.

use std::sync::Arc;
use bytes::Bytes;
use object_store::{ObjectStore, PutMode, PutOptions, PutPayload, path::Path as ObjectPath};
use url::Url;

use crate::delta_reader::engine::DeltaStorageConfig;
use crate::debug_println;

use super::error::{TxLogError, Result};

/// Credential-aware storage handle for transaction log operations.
pub struct TxLogStorage {
    store: Arc<dyn ObjectStore>,
    table_path: String,
    txlog_prefix: String,
}

impl TxLogStorage {
    /// Create from a table URL and credential config.
    ///
    /// The txlog lives at `<table_path>/_transaction_log/`.
    pub fn new(table_path: &str, config: &DeltaStorageConfig) -> Result<Self> {
        let url = Url::parse(table_path)
            .or_else(|_| Url::from_file_path(table_path).map_err(|_| {
                TxLogError::Storage(anyhow::anyhow!("Invalid table path: {}", table_path))
            }))?;
        let store = crate::delta_reader::engine::create_object_store(&url, config)
            .map_err(|e| TxLogError::Storage(e))?;
        let txlog_prefix = format!("{}/_transaction_log",
            url.path().trim_end_matches('/'));
        Ok(Self {
            store,
            table_path: table_path.to_string(),
            txlog_prefix,
        })
    }

    /// Full object path for a relative path under the txlog directory.
    fn full_path(&self, relative: &str) -> ObjectPath {
        let full = format!("{}/{}", self.txlog_prefix, relative);
        ObjectPath::from(full)
    }

    /// Read bytes at a path relative to txlog root.
    pub async fn get(&self, relative_path: &str) -> Result<Bytes> {
        let path = self.full_path(relative_path);
        debug_println!("📖 TXLOG_STORAGE: get {}", path);
        let result = self.store.get(&path).await
            .map_err(|e| TxLogError::Storage(anyhow::anyhow!("GET {}: {}", path, e)))?;
        result.bytes().await
            .map_err(|e| TxLogError::Storage(anyhow::anyhow!("Read bytes {}: {}", path, e)))
    }

    /// Write bytes atomically with if-not-exists semantics (for version files).
    /// Returns true if written, false if already exists.
    pub async fn put_if_absent(&self, relative_path: &str, data: Bytes) -> Result<bool> {
        let path = self.full_path(relative_path);
        debug_println!("📝 TXLOG_STORAGE: put_if_absent {}", path);
        let opts = PutOptions {
            mode: PutMode::Create,
            ..Default::default()
        };
        match self.store.put_opts(&path, PutPayload::from(data), opts).await {
            Ok(_) => Ok(true),
            Err(object_store::Error::AlreadyExists { .. }) => Ok(false),
            Err(e) => Err(TxLogError::Storage(anyhow::anyhow!("PUT {}: {}", path, e))),
        }
    }

    /// Unconditional write (for checkpoints, manifests).
    pub async fn put(&self, relative_path: &str, data: Bytes) -> Result<()> {
        let path = self.full_path(relative_path);
        debug_println!("📝 TXLOG_STORAGE: put {}", path);
        self.store.put(&path, PutPayload::from(data)).await
            .map_err(|e| TxLogError::Storage(anyhow::anyhow!("PUT {}: {}", path, e)))?;
        Ok(())
    }

    /// List entries under a prefix relative to txlog root.
    pub async fn list(&self, prefix: &str) -> Result<Vec<String>> {
        let full_prefix = format!("{}/{}", self.txlog_prefix, prefix);
        let path = ObjectPath::from(full_prefix.as_str());
        debug_println!("📂 TXLOG_STORAGE: list {}", path);

        let mut entries = Vec::new();
        let result = self.store.list(Some(&path));
        use futures::TryStreamExt;
        let items: Vec<_> = result.try_collect().await
            .map_err(|e| TxLogError::Storage(anyhow::anyhow!("LIST {}: {}", path, e)))?;
        // ObjectPath normalizes away leading '/', so we must compare
        // against the normalized prefix to handle file:// URLs correctly.
        let normalized_prefix = self.txlog_prefix.trim_start_matches('/');
        for meta in items {
            let full = meta.location.to_string();
            let relative = full.strip_prefix(&self.txlog_prefix)
                .or_else(|| full.strip_prefix(normalized_prefix))
                .map(|r| r.trim_start_matches('/').to_string())
                .unwrap_or(full);
            entries.push(relative);
        }
        Ok(entries)
    }

    /// Check existence of a path relative to txlog root.
    pub async fn exists(&self, relative_path: &str) -> Result<bool> {
        let path = self.full_path(relative_path);
        match self.store.head(&path).await {
            Ok(_) => Ok(true),
            Err(object_store::Error::NotFound { .. }) => Ok(false),
            Err(e) => Err(TxLogError::Storage(anyhow::anyhow!("HEAD {}: {}", path, e))),
        }
    }

    /// Delete a path (for GC only).
    pub async fn delete(&self, relative_path: &str) -> Result<()> {
        let path = self.full_path(relative_path);
        debug_println!("🗑️ TXLOG_STORAGE: delete {}", path);
        self.store.delete(&path).await
            .map_err(|e| TxLogError::Storage(anyhow::anyhow!("DELETE {}: {}", path, e)))?;
        Ok(())
    }

    /// Version file path: "00000000000000000042.json"
    pub fn version_path(version: i64) -> String {
        format!("{:020}.json", version)
    }

    /// State directory name: "state-v00000000000000000042"
    /// Uses 20-digit zero-padded format matching Scala's StateManifestIO.formatStateDir().
    pub fn state_dir_name(version: i64) -> String {
        format!("state-v{:020}", version)
    }

    /// Get table path.
    pub fn table_path(&self) -> &str {
        &self.table_path
    }

    /// List version files and return sorted version numbers.
    pub async fn list_versions(&self) -> Result<Vec<i64>> {
        let entries = self.list("").await?;
        let mut versions: Vec<i64> = entries
            .iter()
            .filter_map(|name| {
                let name = name.trim_start_matches('/');
                // Version files are named as 20-digit zero-padded numbers + ".json" = 25 chars
                // Example: "00000000000000000042.json"
                if name.ends_with(".json") && name.len() == 25 {
                    name[..20].parse::<i64>().ok()
                } else {
                    None
                }
            })
            .collect();
        versions.sort();
        Ok(versions)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_version_path() {
        assert_eq!(TxLogStorage::version_path(0), "00000000000000000000.json");
        assert_eq!(TxLogStorage::version_path(42), "00000000000000000042.json");
        assert_eq!(TxLogStorage::version_path(1234567890), "00000000001234567890.json");
    }

    #[test]
    fn test_state_dir_name() {
        assert_eq!(TxLogStorage::state_dir_name(42), "state-v00000000000000000042");
        assert_eq!(TxLogStorage::state_dir_name(0), "state-v00000000000000000000");
    }
}

// persistent_cache_storage.rs - Write-through persistent cache layer for Storage
//
// This module provides a Storage wrapper that adds a persistent LRU cache layer
// underneath Quickwit's ephemeral ByteRangeCache, providing seamless caching
// across multiple search operations while maintaining Quickwit's native behavior.

use std::fmt;
use std::ops::Range;
use std::path::Path;
use std::sync::Arc;

use async_trait::async_trait;
use quickwit_common::uri::Uri;
use quickwit_storage::{
    BulkDeleteError, OwnedBytes, Storage, StorageResult, MemorySizedCache
};
use tokio::io::AsyncRead;

use crate::debug_println;

/// Storage wrapper that provides write-through persistent caching
///
/// This sits between Quickwit's ephemeral cache and the actual storage backend,
/// providing a persistent cache layer that survives across search operations.
///
/// Architecture:
/// Search -> Ephemeral ByteRangeCache -> StorageWithPersistentCache -> Actual Storage
///                     ↓ (miss)                      ↓ (miss)              ↓
///                 This layer                 Persistent Cache          S3/File
///                     ↑                           ↑
///                 (write-through)          (write-through)
pub struct StorageWithPersistentCache {
    /// The underlying storage backend (S3, file system, etc.)
    pub storage: Arc<dyn Storage>,
    /// Persistent cache that survives across search operations
    pub persistent_cache: Arc<MemorySizedCache<quickwit_storage::cache::slice_address::SliceAddress>>,
}

impl StorageWithPersistentCache {
    pub fn new(
        storage: Arc<dyn Storage>,
        persistent_cache: Arc<MemorySizedCache<quickwit_storage::cache::slice_address::SliceAddress>>,
    ) -> Self {
        debug_println!("🔄 PERSISTENT_CACHE: Creating StorageWithPersistentCache wrapper");
        Self {
            storage,
            persistent_cache,
        }
    }
}

impl fmt::Debug for StorageWithPersistentCache {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("StorageWithPersistentCache").finish()
    }
}

#[async_trait]
impl Storage for StorageWithPersistentCache {
    async fn check_connectivity(&self) -> anyhow::Result<()> {
        self.storage.check_connectivity().await
    }

    async fn put(
        &self,
        path: &Path,
        payload: Box<dyn quickwit_storage::PutPayload>,
    ) -> StorageResult<()> {
        // Forward puts to underlying storage (we don't cache writes)
        self.storage.put(path, payload).await
    }

    async fn get_slice(&self, path: &Path, byte_range: Range<usize>) -> StorageResult<OwnedBytes> {
        debug_println!("🔍 PERSISTENT_CACHE: get_slice requested for {:?} range {:?}", path, byte_range);

        // Check persistent cache first
        if let Some(bytes) = self.persistent_cache.get_slice(path, byte_range.clone()) {
            debug_println!("✅ PERSISTENT_CACHE_HIT: Found in persistent cache for {:?}", path);
            return Ok(bytes);
        }

        debug_println!("❌ PERSISTENT_CACHE_MISS: Not in persistent cache, fetching from storage for {:?}", path);

        // Cache miss - fetch from underlying storage
        let bytes = self.storage.get_slice(path, byte_range.clone()).await?;

        // Write-through to persistent cache
        self.persistent_cache.put_slice(path.to_owned(), byte_range, bytes.clone());
        debug_println!("💾 PERSISTENT_CACHE_STORE: Stored in persistent cache for {:?}", path);

        Ok(bytes)
    }

    async fn get_slice_stream(
        &self,
        path: &Path,
        range: Range<usize>,
    ) -> StorageResult<Box<dyn AsyncRead + Send + Unpin>> {
        // For streaming, bypass cache and go directly to storage
        self.storage.get_slice_stream(path, range).await
    }

    async fn get_all(&self, path: &Path) -> StorageResult<OwnedBytes> {
        debug_println!("🔍 PERSISTENT_CACHE: get_all requested for {:?}", path);

        // For get_all, we can't easily determine the range, so just forward to storage
        // In practice, get_slice is used much more frequently for split operations
        self.storage.get_all(path).await
    }

    async fn delete(&self, path: &Path) -> StorageResult<()> {
        // Forward deletes and potentially invalidate cache entries
        // Note: We don't implement cache invalidation for simplicity
        self.storage.delete(path).await
    }

    async fn bulk_delete<'a>(&self, paths: &[&'a Path]) -> Result<(), BulkDeleteError> {
        // Forward bulk deletes
        self.storage.bulk_delete(paths).await
    }

    async fn exists(&self, path: &Path) -> StorageResult<bool> {
        self.storage.exists(path).await
    }

    async fn file_num_bytes(&self, path: &Path) -> StorageResult<u64> {
        self.storage.file_num_bytes(path).await
    }

    fn uri(&self) -> &Uri {
        self.storage.uri()
    }
}
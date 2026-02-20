// augmented_directory.rs - ParquetAugmentedDirectory (Phase 2)
//
// A tantivy Directory wrapper that intercepts .fast file reads and optionally
// serves fast field data from parquet files instead of (or in addition to)
// the native tantivy columnar data.
//
// How it works:
//   1. On prewarm (or first access), transcode parquet columns into tantivy columnar bytes
//   2. In hybrid mode, merge the native .fast bytes with the transcoded parquet bytes
//   3. Cache the result keyed by segment path (e.g. "{uuid}.fast")
//   4. On get_file_handle(".fast"), return cached OwnedBytes as FileHandle if available
//
// The cache is populated externally via insert_transcoded() — called from
// the prewarm JNI method or from a lazy-init path.

use std::collections::HashMap;
use std::ops::Range;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use quickwit_storage::Storage;

use tantivy::directory::{
    Directory, FileHandle, OwnedBytes, WatchCallback, WatchHandle,
    error::{DeleteError, OpenReadError, OpenWriteError},
};

use super::manifest::{FastFieldMode, ParquetManifest};
use super::transcode::{columns_to_transcode, merge_columnar_bytes,
                        transcode_columns_from_parquet};

use crate::debug_println;

/// A Directory wrapper that augments .fast file reads with parquet data.
///
/// Depending on the FastFieldMode:
/// - Disabled: All reads delegated to inner directory (no-op wrapper)
/// - Hybrid: Numeric fast fields from inner, string fast fields from parquet
/// - ParquetOnly: All fast fields decoded from parquet
#[derive(Clone)]
pub struct ParquetAugmentedDirectory {
    inner: Arc<dyn Directory>,
    mode: FastFieldMode,
    manifest: Arc<ParquetManifest>,
    storage: Arc<dyn Storage>,
    /// Storage for reading native .fast bytes from the split bundle (async-capable).
    /// In hybrid mode, we need to read native fast fields from the split file via
    /// the storage layer since HotDirectory's sync get_file_handle may not have them cached.
    split_storage: Arc<dyn Storage>,
    /// Byte ranges for each file within the split bundle.
    bundle_file_offsets: HashMap<PathBuf, Range<u64>>,
    /// Filename of the split within the storage (e.g. "mysplit.split").
    split_filename: PathBuf,
    /// Full URI of the split (e.g. "s3://bucket/path/mysplit.split").
    /// Used for L2 disk cache key derivation.
    split_uri: String,
    /// Cache of transcoded fast field bytes per segment path.
    /// Key: segment .fast file path (e.g. "abc123.fast")
    /// Value: complete columnar bytes (merged native+parquet in hybrid mode)
    pub(crate) transcoded_cache: Arc<Mutex<HashMap<PathBuf, OwnedBytes>>>,
}

impl ParquetAugmentedDirectory {
    pub fn new(
        inner: Arc<dyn Directory>,
        mode: FastFieldMode,
        manifest: Arc<ParquetManifest>,
        storage: Arc<dyn Storage>,
        split_storage: Arc<dyn Storage>,
        bundle_file_offsets: HashMap<PathBuf, Range<u64>>,
        split_filename: PathBuf,
        split_uri: String,
    ) -> Self {
        Self {
            inner,
            mode,
            manifest,
            storage,
            split_storage,
            bundle_file_offsets,
            split_filename,
            split_uri,
            transcoded_cache: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// Check if a path is a fast field file
    fn is_fast_file(path: &Path) -> bool {
        path.extension()
            .map(|ext| ext == "fast")
            .unwrap_or(false)
    }

    /// Insert pre-transcoded bytes into the cache for a segment.
    ///
    /// Called from prewarm code after transcoding + merging is complete.
    pub fn insert_transcoded(&self, path: PathBuf, bytes: OwnedBytes) {
        let mut cache = self.transcoded_cache.lock().unwrap();
        debug_println!(
            "📊 AUGMENTED_DIR: Caching transcoded fast fields for {:?} ({} bytes)",
            path, bytes.len()
        );
        cache.insert(path, bytes);
    }

    /// Check if transcoded bytes are cached for a given path.
    pub fn has_cached(&self, path: &Path) -> bool {
        let cache = self.transcoded_cache.lock().unwrap();
        cache.contains_key(path)
    }

    /// Get the fast field mode.
    pub fn mode(&self) -> FastFieldMode {
        self.mode
    }

    /// Get the manifest.
    pub fn manifest(&self) -> &ParquetManifest {
        &self.manifest
    }

    /// Get the storage.
    pub fn storage(&self) -> &Arc<dyn Storage> {
        &self.storage
    }

    /// Get the inner directory.
    pub fn inner(&self) -> &Arc<dyn Directory> {
        &self.inner
    }

    /// Transcode and cache fast fields for a segment.
    ///
    /// This reads parquet data, transcodes to columnar format, optionally merges
    /// with native fast fields, and caches the result.
    ///
    /// Supports L2 disk cache: transcoded bytes are persisted to the L2 disk cache
    /// (if configured) so that subsequent JVM restarts or SplitSearcher instances
    /// can reuse previously transcoded data without re-reading parquet files.
    ///
    /// # Arguments
    /// * `fast_file_path` - The .fast file path (e.g. "abc123.fast")
    /// * `requested_columns` - Specific columns to transcode, or None for all applicable
    pub async fn transcode_and_cache(
        &self,
        fast_file_path: &Path,
        requested_columns: Option<&[String]>,
    ) -> anyhow::Result<()> {
        let columns = columns_to_transcode(&self.manifest, self.mode, requested_columns);

        if columns.is_empty() && self.mode != FastFieldMode::Hybrid {
            debug_println!(
                "📊 AUGMENTED_DIR: No columns to transcode for {:?}",
                fast_file_path
            );
            return Ok(());
        }

        // Build L2 cache key components.
        // Use "parquet_transcoded" as component to distinguish from native bundle data.
        // Include the fast file path (segment ID) and a sorted column list hash
        // so that different column sets produce different cache entries.
        let disk_cache = crate::global_cache::get_global_disk_cache();
        let l2_cache_key = disk_cache.as_ref().map(|_| {
            let (storage_loc, split_id) = crate::prewarm::parse_split_uri(&self.split_uri);
            let fast_name = fast_file_path.file_name()
                .and_then(|n| n.to_str())
                .unwrap_or("unknown.fast");
            // Include sorted column names in the component key so that different
            // column sets produce different cache entries.
            let mut col_names: Vec<&str> = columns.iter()
                .map(|c| c.tantivy_name.as_str())
                .collect();
            col_names.sort();
            let col_hash = {
                use std::hash::{Hash, Hasher};
                let mut hasher = std::collections::hash_map::DefaultHasher::new();
                col_names.hash(&mut hasher);
                hasher.finish()
            };
            let component = format!("parquet_transcoded_{}_{:016x}", fast_name, col_hash);
            (storage_loc, split_id, component)
        });

        // Check L2 disk cache before transcoding
        if let (Some(ref cache), Some((ref storage_loc, ref split_id, ref component))) =
            (&disk_cache, &l2_cache_key)
        {
            if let Some(cached_bytes) = cache.get(storage_loc, split_id, component, None) {
                debug_println!(
                    "📊 AUGMENTED_DIR: L2 cache hit for {:?} ({} bytes), skipping transcoding",
                    fast_file_path, cached_bytes.len()
                );
                self.insert_transcoded(
                    fast_file_path.to_path_buf(),
                    cached_bytes,
                );
                return Ok(());
            } else {
                debug_println!(
                    "📊 AUGMENTED_DIR: L2 cache miss for {:?} (component='{}')",
                    fast_file_path, component
                );
            }
        }

        debug_println!(
            "📊 AUGMENTED_DIR: Transcoding {} columns for {:?}",
            columns.len(), fast_file_path
        );

        let num_docs: u32 = self.manifest.total_rows.try_into()
            .map_err(|_| anyhow::anyhow!("total_rows {} exceeds u32::MAX", self.manifest.total_rows))?;

        // Transcode parquet columns to columnar bytes
        let parquet_columnar_bytes = transcode_columns_from_parquet(
            &columns,
            &self.manifest,
            &self.storage,
            num_docs,
            None, // metadata cache populated via prewarm/doc retrieval paths
        ).await?;

        // Merge with native fast fields if in hybrid mode
        let final_bytes = match self.mode {
            FastFieldMode::Hybrid => {
                // Read the native .fast file from the split bundle via async storage.
                // We cannot use self.inner.get_file_handle() because the underlying
                // StorageDirectory only supports async reads and will fail on sync access.
                let native_bytes = if let Some(range) = self.bundle_file_offsets.get(fast_file_path) {
                    debug_println!(
                        "📊 AUGMENTED_DIR: Reading native .fast bytes from split bundle: {:?} (range {}..{})",
                        fast_file_path, range.start, range.end
                    );
                    let bytes = self.split_storage
                        .get_slice(&self.split_filename, range.start as usize..range.end as usize)
                        .await
                        .map_err(|e| anyhow::anyhow!(
                            "Failed to read native fast field bytes from split bundle: {}", e
                        ))?;
                    Some(bytes)
                } else {
                    debug_println!(
                        "⚠️ AUGMENTED_DIR: No bundle offset for {:?}, skipping native merge",
                        fast_file_path
                    );
                    None
                };

                match native_bytes {
                    Some(ref bytes) => merge_columnar_bytes(Some(bytes), &parquet_columnar_bytes, self.mode)?,
                    None => parquet_columnar_bytes,
                }
            }
            FastFieldMode::ParquetOnly => {
                // All from parquet
                parquet_columnar_bytes
            }
            FastFieldMode::Disabled => {
                // Should not reach here, but handle gracefully
                return Ok(());
            }
        };

        // Wrap the raw columnar bytes with a tantivy Footer (version + CRC + magic 1337).
        // Without this, tantivy's file reader rejects the data with "Footer magic byte mismatch".
        let wrapped_bytes = super::transcode::wrap_with_tantivy_footer(&final_bytes);

        debug_println!(
            "📊 AUGMENTED_DIR: Cached {} bytes (wrapped from {} raw) for {:?}",
            wrapped_bytes.len(), final_bytes.len(), fast_file_path
        );

        // Write to L2 disk cache for persistence across JVM restarts
        if let (Some(ref cache), Some((ref storage_loc, ref split_id, ref component))) =
            (&disk_cache, &l2_cache_key)
        {
            debug_println!(
                "📊 AUGMENTED_DIR: Writing {} bytes to L2 disk cache (component='{}')",
                wrapped_bytes.len(), component
            );
            cache.put(storage_loc, split_id, component, None, &wrapped_bytes);
        }

        self.insert_transcoded(
            fast_file_path.to_path_buf(),
            OwnedBytes::new(wrapped_bytes),
        );

        Ok(())
    }
}

impl std::fmt::Debug for ParquetAugmentedDirectory {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ParquetAugmentedDirectory")
            .field("mode", &self.mode)
            .field("inner", &"<Directory>")
            .finish()
    }
}

impl Directory for ParquetAugmentedDirectory {
    fn get_file_handle(
        &self,
        path: &Path,
    ) -> Result<Arc<dyn FileHandle>, OpenReadError> {
        if Self::is_fast_file(path) && self.mode != FastFieldMode::Disabled {
            // Check cache for pre-transcoded bytes
            let cache = self.transcoded_cache.lock().unwrap();
            if let Some(cached) = cache.get(path) {
                debug_println!(
                    "📊 AUGMENTED_DIR: Cache hit for {:?} ({} bytes)",
                    path, cached.len()
                );
                return Ok(Arc::new(cached.clone()));
            }

            debug_println!(
                "📊 AUGMENTED_DIR: Cache miss for {:?} — delegating to inner",
                path
            );
        }

        // Delegate to inner directory (native fast fields or non-.fast file)
        self.inner.get_file_handle(path)
    }

    fn exists(&self, path: &Path) -> Result<bool, OpenReadError> {
        // If we have cached transcoded data for a .fast file, it exists
        if Self::is_fast_file(path) && self.mode != FastFieldMode::Disabled {
            let cache = self.transcoded_cache.lock().unwrap();
            if cache.contains_key(path) {
                return Ok(true);
            }
        }
        self.inner.exists(path)
    }

    fn atomic_read(&self, path: &Path) -> Result<Vec<u8>, OpenReadError> {
        // Fast fields don't use atomic_read — they use get_file_handle
        self.inner.atomic_read(path)
    }

    fn atomic_write(&self, path: &Path, _data: &[u8]) -> std::io::Result<()> {
        // Read-only directory — accept writes silently for known internal files
        // (.managed.json written by ManagedDirectory during Index::open).
        // Log unexpected writes so they don't get silently lost.
        let name = path.file_name().and_then(|n| n.to_str()).unwrap_or("");
        if name != ".managed.json" && name != "meta.json" {
            crate::debug_println!(
                "⚠️ PARQUET_AUGMENTED_DIR: Discarding write to {} (read-only directory)",
                path.display()
            );
        }
        Ok(())
    }

    fn delete(&self, _path: &Path) -> Result<(), DeleteError> {
        Ok(()) // read-only, no-op
    }

    fn open_write(&self, path: &Path) -> Result<tantivy::directory::WritePtr, OpenWriteError> {
        Err(OpenWriteError::FileAlreadyExists(path.to_path_buf()))
    }

    fn sync_directory(&self) -> std::io::Result<()> {
        Ok(()) // read-only, no-op
    }

    fn watch(&self, _watch_callback: WatchCallback) -> tantivy::Result<WatchHandle> {
        Ok(WatchHandle::empty())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tantivy::directory::RamDirectory;

    #[test]
    fn test_is_fast_file() {
        assert!(ParquetAugmentedDirectory::is_fast_file(Path::new("abc123.fast")));
        assert!(ParquetAugmentedDirectory::is_fast_file(Path::new("some/path/uuid.fast")));
        assert!(!ParquetAugmentedDirectory::is_fast_file(Path::new("abc123.pos")));
        assert!(!ParquetAugmentedDirectory::is_fast_file(Path::new("abc123.store")));
        assert!(!ParquetAugmentedDirectory::is_fast_file(Path::new("fast")));
    }

    #[test]
    fn test_cache_insert_and_hit() {
        let ram_dir = RamDirectory::default();
        let manifest = Arc::new(make_test_manifest());
        // We need a Storage impl for the constructor; use a mock approach
        // For unit tests of the cache logic, we just test insert_transcoded + has_cached
        let dir = ParquetAugmentedDirectory {
            inner: Arc::new(ram_dir),
            mode: FastFieldMode::Hybrid,
            manifest,
            storage: Arc::new(MockStorage),
            split_storage: Arc::new(MockStorage),
            bundle_file_offsets: HashMap::new(),
            split_filename: PathBuf::from("test.split"),
            split_uri: "file:///tmp/test.split".to_string(),
            transcoded_cache: Arc::new(Mutex::new(HashMap::new())),
        };

        let path = PathBuf::from("abc123.fast");
        assert!(!dir.has_cached(&path));

        dir.insert_transcoded(path.clone(), OwnedBytes::new(vec![1, 2, 3, 4]));
        assert!(dir.has_cached(&path));
    }

    #[test]
    fn test_disabled_mode_delegates() {
        let ram_dir = RamDirectory::default();
        // Write a test file to the RAM directory
        ram_dir.atomic_write(Path::new("meta.json"), b"{}").unwrap();

        let manifest = Arc::new(make_test_manifest());
        let dir = ParquetAugmentedDirectory {
            inner: Arc::new(ram_dir),
            mode: FastFieldMode::Disabled,
            manifest,
            storage: Arc::new(MockStorage),
            split_storage: Arc::new(MockStorage),
            bundle_file_offsets: HashMap::new(),
            split_filename: PathBuf::from("test.split"),
            split_uri: "file:///tmp/test.split".to_string(),
            transcoded_cache: Arc::new(Mutex::new(HashMap::new())),
        };

        // atomic_read should delegate to inner
        let data = dir.atomic_read(Path::new("meta.json")).unwrap();
        assert_eq!(data, b"{}");
    }

    #[test]
    fn test_cached_fast_file_returned() {
        let ram_dir = RamDirectory::default();
        let manifest = Arc::new(make_test_manifest());
        let dir = ParquetAugmentedDirectory {
            inner: Arc::new(ram_dir),
            mode: FastFieldMode::ParquetOnly,
            manifest,
            storage: Arc::new(MockStorage),
            split_storage: Arc::new(MockStorage),
            bundle_file_offsets: HashMap::new(),
            split_filename: PathBuf::from("test.split"),
            split_uri: "file:///tmp/test.split".to_string(),
            transcoded_cache: Arc::new(Mutex::new(HashMap::new())),
        };

        // Create valid columnar bytes
        let mut writer = tantivy::columnar::ColumnarWriter::default();
        writer.record_numerical(0u32, "x", 42i64);
        let mut buf = Vec::new();
        writer.serialize(1u32, &mut buf).unwrap();

        let path = PathBuf::from("seg1.fast");
        dir.insert_transcoded(path.clone(), OwnedBytes::new(buf.clone()));

        // get_file_handle should return the cached bytes
        let handle = dir.get_file_handle(&path).unwrap();
        assert_eq!(handle.len(), buf.len());
    }

    fn make_test_manifest() -> ParquetManifest {
        use super::super::manifest::*;
        ParquetManifest {
            version: SUPPORTED_MANIFEST_VERSION,
            table_root: "/data".to_string(),
            fast_field_mode: FastFieldMode::Hybrid,
            segment_row_ranges: vec![],
            parquet_files: vec![],
            column_mapping: vec![],
            total_rows: 0,
            storage_config: None,
            metadata: std::collections::HashMap::new(),
        }
    }

    /// Minimal mock Storage for testing cache logic only.
    /// None of the storage methods are called in these tests.
    #[derive(Debug)]
    struct MockStorage;

    #[async_trait::async_trait]
    impl Storage for MockStorage {
        async fn check_connectivity(&self) -> anyhow::Result<()> { Ok(()) }

        async fn put(
            &self,
            _path: &std::path::Path,
            _payload: Box<dyn quickwit_storage::PutPayload>,
        ) -> quickwit_storage::StorageResult<()> {
            unimplemented!()
        }

        fn copy_to<'life0, 'life1, 'life2, 'async_trait>(
            &'life0 self,
            _path: &'life1 std::path::Path,
            _output: &'life2 mut dyn quickwit_storage::SendableAsync,
        ) -> ::core::pin::Pin<Box<dyn ::core::future::Future<Output = quickwit_storage::StorageResult<()>> + ::core::marker::Send + 'async_trait>>
        where
            'life0: 'async_trait,
            'life1: 'async_trait,
            'life2: 'async_trait,
            Self: 'async_trait,
        {
            Box::pin(async move { unimplemented!() })
        }

        async fn get_slice(
            &self,
            _path: &std::path::Path,
            _range: std::ops::Range<usize>,
        ) -> quickwit_storage::StorageResult<OwnedBytes> {
            unimplemented!()
        }

        async fn get_slice_stream(
            &self,
            _path: &std::path::Path,
            _range: std::ops::Range<usize>,
        ) -> quickwit_storage::StorageResult<Box<dyn tokio::io::AsyncRead + Send + Unpin>> {
            unimplemented!()
        }

        async fn delete(&self, _path: &std::path::Path) -> quickwit_storage::StorageResult<()> {
            unimplemented!()
        }

        async fn bulk_delete<'a>(
            &self,
            _paths: &[&'a std::path::Path],
        ) -> Result<(), quickwit_storage::BulkDeleteError> {
            unimplemented!()
        }

        async fn get_all(&self, _path: &std::path::Path) -> quickwit_storage::StorageResult<OwnedBytes> {
            unimplemented!()
        }

        async fn file_num_bytes(&self, _path: &std::path::Path) -> quickwit_storage::StorageResult<u64> {
            unimplemented!()
        }

        fn uri(&self) -> &quickwit_common::uri::Uri {
            unimplemented!()
        }
    }
}

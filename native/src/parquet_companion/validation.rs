// validation.rs - Staleness and missing file checks for parquet companion mode
//
// STATUS: Currently UNUSED / not wired into any read path.
//
// `FileValidator` and `MissingFilePolicy` are only re-exported from `mod.rs`; no
// retrieval or transcode path constructs a `FileValidator`, so the staleness /
// missing-file protection it implements is not yet enforced at query time. The
// module is retained (rather than deleted) because it is intended to be wired
// into the read path in the future. If you are extending the read path, this is
// the place that should be invoked before serving parquet-backed rows. Until
// then, treat this module as scaffolding.

use std::collections::HashMap;
use std::sync::Mutex;

use super::manifest::ParquetManifest;

/// Policy for handling missing parquet files at query time
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MissingFilePolicy {
    /// Fail the query immediately if any referenced file is missing or stale
    Fail,
    /// Log a warning and continue as if the file were valid.
    ///
    /// NOTE: Despite the name, this does NOT substitute empty/null values for the
    /// documents in a missing/stale file. `evaluate_result` returns `Result<(),
    /// String>`, which can only express "ok" or "error" — there is no third
    /// outcome to signal "present-but-substitute-empties". So `Warn` collapses to
    /// a logged no-op: the validation is skipped and the query proceeds, with any
    /// subsequent read of the actually-missing file failing on its own. Delivering
    /// true empty/null substitution would require a richer return type
    /// (e.g. an enum of Present/Substitute/Error) and cooperation from the reader.
    Warn,
}

impl Default for MissingFilePolicy {
    fn default() -> Self {
        MissingFilePolicy::Fail
    }
}

/// Result of a file validation check
#[derive(Debug, Clone)]
pub struct FileValidationResult {
    pub path: String,
    pub exists: bool,
    pub size_matches: bool,
    pub actual_size: Option<u64>,
    pub expected_size: u64,
}

/// Validates that referenced parquet files exist and haven't changed size.
/// Caches results to avoid repeated checks for the same file.
pub struct FileValidator {
    policy: MissingFilePolicy,
    cache: Mutex<HashMap<String, FileValidationResult>>,
}

impl FileValidator {
    pub fn new(policy: MissingFilePolicy) -> Self {
        Self {
            policy,
            cache: Mutex::new(HashMap::new()),
        }
    }

    /// Check if a file is valid (exists and size matches manifest).
    /// Results are cached per resolved path.
    pub async fn validate_file(
        &self,
        resolved_path: &str,
        expected_size: u64,
        storage: &dyn quickwit_storage::Storage,
    ) -> Result<(), String> {
        // Check cache first
        {
            let cache = self.cache.lock().unwrap();
            if let Some(cached) = cache.get(resolved_path) {
                return self.evaluate_result(cached);
            }
        }

        // Perform validation
        let result = match storage
            .file_num_bytes(std::path::Path::new(resolved_path))
            .await
        {
            Ok(actual_size) => FileValidationResult {
                path: resolved_path.to_string(),
                exists: true,
                size_matches: actual_size == expected_size,
                actual_size: Some(actual_size),
                expected_size,
            },
            // Only a definitive "not found" is a cacheable absence. Transient
            // storage failures (S3 503, timeouts, throttling, auth, I/O) must NOT
            // be cached as `exists: false` — doing so would poison the cache
            // forever (there is no TTL), permanently failing a file that was only
            // temporarily unreachable. Surface the error to the caller instead so
            // a later call can retry.
            Err(err) if err.kind() == quickwit_storage::StorageErrorKind::NotFound => {
                FileValidationResult {
                    path: resolved_path.to_string(),
                    exists: false,
                    size_matches: false,
                    actual_size: None,
                    expected_size,
                }
            }
            Err(err) => {
                return Err(format!(
                    "Transient storage error validating '{}': {} (not cached; retry)",
                    resolved_path, err
                ));
            }
        };

        let eval = self.evaluate_result(&result);

        // Cache the result
        {
            let mut cache = self.cache.lock().unwrap();
            cache.insert(resolved_path.to_string(), result);
        }

        eval
    }

    fn evaluate_result(&self, result: &FileValidationResult) -> Result<(), String> {
        if !result.exists {
            let msg = format!("Parquet file not found: '{}'", result.path);
            match self.policy {
                MissingFilePolicy::Fail => return Err(msg),
                MissingFilePolicy::Warn => {
                    crate::debug_println!("⚠️ PARQUET_VALIDATION: {}", msg);
                    return Ok(());
                }
            }
        }

        if !result.size_matches {
            let msg = format!(
                "Parquet file size mismatch for '{}': expected {} bytes, found {} bytes (file may be stale)",
                result.path,
                result.expected_size,
                result.actual_size.unwrap_or(0)
            );
            match self.policy {
                MissingFilePolicy::Fail => return Err(msg),
                MissingFilePolicy::Warn => {
                    crate::debug_println!("⚠️ PARQUET_VALIDATION: {}", msg);
                    return Ok(());
                }
            }
        }

        Ok(())
    }

    /// Validate all files in a manifest
    pub async fn validate_manifest(
        &self,
        manifest: &ParquetManifest,
        storage: &dyn quickwit_storage::Storage,
    ) -> Result<(), String> {
        for file_entry in &manifest.parquet_files {
            let resolved = manifest.resolve_path(&file_entry.relative_path);
            self.validate_file(&resolved, file_entry.file_size_bytes, storage)
                .await?;
        }
        Ok(())
    }
}

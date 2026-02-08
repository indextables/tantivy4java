// validation.rs - Staleness and missing file checks for parquet companion mode

use std::collections::HashMap;
use std::sync::Mutex;

use super::manifest::ParquetManifest;

/// Policy for handling missing parquet files at query time
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MissingFilePolicy {
    /// Fail the query immediately if any referenced file is missing or stale
    Fail,
    /// Log a warning and return empty/null values for documents in missing files
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
            Err(_) => FileValidationResult {
                path: resolved_path.to_string(),
                exists: false,
                size_matches: false,
                actual_size: None,
                expected_size,
            },
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

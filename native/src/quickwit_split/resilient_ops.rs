// resilient_ops.rs - Resilient operations for Tantivy index operations
// Handles transient file corruption with retry logic

use std::path::Path;
use std::time::Duration;
use anyhow::{anyhow, Result};
use quickwit_common::retry::Retryable;
use crate::debug_println;

// Debug logging macro
macro_rules! debug_log {
    ($($arg:tt)*) => {
        debug_println!("DEBUG: {}", format!($($arg)*))
    };
}

/// Maximum number of retry attempts for resilient operations
const MAX_RETRIES: usize = 3;

/// Get the maximum retry count based on environment (Databricks gets more retries)
fn get_max_retries() -> usize {
    if std::env::var("DATABRICKS_RUNTIME_VERSION").is_ok() { 8 } else { MAX_RETRIES }
}

/// Base delay between retries in milliseconds (exponential backoff)
const BASE_RETRY_DELAY_MS: u64 = 100;

/// Wrapper to make anyhow::Error implement Retryable for Quickwit's retry system
#[derive(Debug)]
struct RetryableAnyhowError(anyhow::Error);

impl std::fmt::Display for RetryableAnyhowError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

impl std::error::Error for RetryableAnyhowError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.0.source()
    }
}

impl Retryable for RetryableAnyhowError {
    fn is_retryable(&self) -> bool {
        true  // Retry ALL errors - let the retry logic handle all failure cases
    }
}

/// Resilient wrapper for index.load_metas() with retry logic for transient corruption
pub fn resilient_load_metas(index: &tantivy::Index) -> Result<tantivy::IndexMeta> {
    resilient_operation("load_metas", || {
        index.load_metas()
    })
}

/// Resilient wrapper for directory operations that may encounter transient I/O issues
pub fn resilient_directory_read(directory: &dyn tantivy::Directory, path: &Path) -> Result<Vec<u8>> {
    resilient_io_operation("directory_read", || {
        directory.atomic_read(path)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))
    })
}

/// Extract column number from serde_json error messages like "line 1, column: 2291"
pub fn extract_column_number(error_msg: &str) -> Option<usize> {
    // Simple string parsing approach to avoid regex dependency complexity
    // Look for patterns like "line 1, column: 2291" or "line 1, column 2291"
    if let Some(column_start) = error_msg.find("column") {
        let after_column = &error_msg[column_start + 6..]; // Skip "column"
        // Skip any whitespace and optional colon
        let trimmed = after_column.trim_start_matches(|c: char| c.is_whitespace() || c == ':');
        // Extract the number
        let number_str = trimmed.split_whitespace().next()?;
        number_str.parse::<usize>().ok()
    } else {
        None
    }
}

/// Special resilient operation wrapper for JSON column truncation errors
/// This provides enhanced debugging and potential recovery for consistent JSON truncation issues
#[allow(dead_code)]
fn resilient_operation_with_column_2291_fix<T, F>(operation_name: &str, mut operation: F) -> Result<T>
where
    F: FnMut() -> tantivy::Result<T>,
{
    let max_retries = get_max_retries();
    let mut last_error = None;

    for attempt in 0..max_retries {
        if attempt > 0 {
            // Add extra delay for column 2291 errors since they might be timing-related
            let delay_ms = BASE_RETRY_DELAY_MS * (1 << (attempt - 1)) + 50; // Extra 50ms
            debug_log!("🔧 Column 2291 Fix: Retry attempt {} for {} after {} ms delay", attempt + 1, operation_name, delay_ms);
            std::thread::sleep(Duration::from_millis(delay_ms));
        }

        match operation() {
            Ok(result) => {
                if attempt > 0 {
                    debug_log!("✅ Column 2291 Fix: {} succeeded on attempt {}", operation_name, attempt + 1);
                }
                return Ok(result);
            }
            Err(err) => {
                let error_msg = err.to_string();

                // Special handling for JSON column truncation errors
                if error_msg.contains("EOF while parsing") {
                    if let Some(column_num) = extract_column_number(&error_msg) {
                        debug_log!("🚨 JSON Column Fix: DETECTED JSON TRUNCATION ERROR in {} at column {}: {}", operation_name, column_num, error_msg);
                        debug_log!("🚨 JSON Column Fix: This indicates systematic JSON truncation at byte position {}", column_num);
                        debug_log!("🚨 JSON Column Fix: Attempt {} of {} - will retry with enhanced delay", attempt + 1, max_retries);

                        // For consistent column truncation errors, add extra logging
                        if attempt == max_retries - 1 {
                            return Err(anyhow!("PERSISTENT JSON COLUMN {} CORRUPTION: {} consistently fails with JSON truncation at column {} after {} attempts. This indicates a systematic file reading issue in the BundleDirectory or FileSlice operations.", column_num, operation_name, column_num, max_retries));
                        }

                        last_error = Some(err);
                        continue;
                    }
                }

                // Check if this is a transient corruption error that we should retry
                let is_transient_error = error_msg.contains("EOF while parsing") ||
                                       error_msg.contains("Data corrupted") ||
                                       error_msg.contains("Managed file cannot be deserialized") ||
                                       error_msg.contains("unexpected end of file") ||
                                       error_msg.contains("invalid json");

                if is_transient_error {
                    debug_log!("🔧 Resilient operation: {} failed with transient error (attempt {}): {}", operation_name, attempt + 1, error_msg);
                    last_error = Some(err);

                    if attempt == get_max_retries() - 1 {
                        break;
                    }
                } else {
                    // Non-transient error, fail immediately
                    return Err(anyhow!("Operation {} failed with non-transient error: {}", operation_name, err));
                }
            }
        }
    }

    match last_error {
        Some(err) => Err(anyhow!("Operation {} failed after {} attempts. Last error: {}", operation_name, get_max_retries(), err)),
        None => Err(anyhow!("Operation {} failed after {} attempts with unknown error", operation_name, get_max_retries())),
    }
}

/// Generic resilient operation wrapper with exponential backoff retry for Tantivy operations
fn resilient_operation<T, F>(operation_name: &str, mut operation: F) -> Result<T>
where
    F: FnMut() -> tantivy::Result<T>,
{
    let mut last_error = None;

    for attempt in 0..get_max_retries() {
        match operation() {
            Ok(result) => {
                if attempt > 0 {
                    debug_log!("✅ RESILIENCE: {} succeeded on attempt {} after transient failure", operation_name, attempt + 1);
                }
                return Ok(result);
            }
            Err(err) => {
                let error_msg = err.to_string();

                // Check if this is a transient corruption error that we should retry
                let is_transient_error = error_msg.contains("EOF while parsing") ||
                                       error_msg.contains("Data corrupted") ||
                                       error_msg.contains("Managed file cannot be deserialized") ||
                                       error_msg.contains("unexpected end of file") ||
                                       error_msg.contains("invalid json");

                if !is_transient_error || attempt == get_max_retries() - 1 {
                    // Not a transient error or final attempt - don't retry
                    return Err(anyhow!("{} failed: {}", operation_name, err));
                }

                debug_log!("⚠️ RESILIENCE: {} attempt {} failed with transient error: {} (retrying...)",
                          operation_name, attempt + 1, error_msg);

                last_error = Some(err);

                // Exponential backoff: 100ms, 200ms, 400ms
                let delay_ms = BASE_RETRY_DELAY_MS * (1 << attempt);
                std::thread::sleep(Duration::from_millis(delay_ms));
            }
        }
    }

    // All retries exhausted
    Err(anyhow!("{} failed after {} attempts: {:?}", operation_name, get_max_retries(), last_error))
}

/// Generic resilient operation wrapper with exponential backoff retry for I/O operations
fn resilient_io_operation<T, F>(operation_name: &str, mut operation: F) -> Result<T>
where
    F: FnMut() -> std::io::Result<T>,
{
    let mut last_error = None;

    for attempt in 0..get_max_retries() {
        match operation() {
            Ok(result) => {
                if attempt > 0 {
                    debug_log!("✅ RESILIENCE: {} succeeded on attempt {} after transient I/O failure", operation_name, attempt + 1);
                }
                return Ok(result);
            }
            Err(err) => {
                let error_msg = err.to_string();

                // Check if this is a transient I/O error that we should retry
                let is_transient_error = error_msg.contains("EOF while parsing") ||
                                       error_msg.contains("Data corrupted") ||
                                       error_msg.contains("Managed file cannot be deserialized") ||
                                       error_msg.contains("unexpected end of file") ||
                                       error_msg.contains("invalid json") ||
                                       error_msg.contains("Resource temporarily unavailable") ||
                                       error_msg.contains("Interrupted system call");

                if !is_transient_error || attempt == get_max_retries() - 1 {
                    // Not a transient error or final attempt - don't retry
                    return Err(anyhow!("{} I/O failed: {}", operation_name, err));
                }

                debug_log!("⚠️ RESILIENCE: {} attempt {} failed with transient I/O error: {} (retrying...)",
                          operation_name, attempt + 1, error_msg);

                last_error = Some(err);

                // Exponential backoff: 100ms, 200ms, 400ms
                let delay_ms = BASE_RETRY_DELAY_MS * (1 << attempt);
                std::thread::sleep(Duration::from_millis(delay_ms));
            }
        }
    }

    // All retries exhausted
    Err(anyhow!("{} I/O failed after {} attempts: {:?}", operation_name, get_max_retries(), last_error))
}

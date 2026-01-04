// temp_management.rs - Temporary directory management and split extraction
// Extracted from mod.rs during refactoring
// Contains: extract_split_to_directory_impl, create_temp_directory functions, ExtractedSplit

use std::path::{Path, PathBuf};
use tempfile as temp;
use anyhow::{anyhow, Result};
use tantivy::directory::Directory;

use crate::debug_println;
use quickwit_indexing::open_index;
use super::split_utils::{create_quickwit_tokenizer_manager, get_tantivy_directory_from_split_bundle_full_access};
use super::resilient_ops;

// Debug logging macro - controlled by TANTIVY4JAVA_DEBUG environment variable
macro_rules! debug_log {
    ($($arg:tt)*) => {
        debug_println!("DEBUG: {}", format!($($arg)*))
    };
}

/// Represents an extracted split with its temporary directory and metadata
#[derive(Debug)]
pub struct ExtractedSplit {
    pub temp_dir: temp::TempDir,
    pub temp_path: PathBuf,
    pub split_url: String,
    pub split_index: usize,
}

/// Extract split file contents to a directory (avoiding read-only BundleDirectory issues)
/// CRITICAL: This function is used by merge operations and must NOT use lazy loading
/// to prevent range assertion failures in the native layer
pub fn extract_split_to_directory_impl(split_path: &Path, output_dir: &Path) -> Result<()> {
    use tantivy::directory::MmapDirectory;

    debug_log!("🔧 MERGE EXTRACTION: Extracting split {:?} to directory {:?}", split_path, output_dir);
    debug_log!("🚨 MERGE SAFETY: Using full file access (NO lazy loading) to prevent native crashes");

    // Create output directory
    std::fs::create_dir_all(output_dir)?;

    // Open the bundle directory (read-only) - MERGE OPERATIONS MUST USE FULL ACCESS
    let split_path_str = split_path.to_string_lossy().to_string();
    debug_log!("🔧 MERGE EXTRACTION: Opening bundle directory with full file access (no lazy loading)");

    // ✅ CORRUPTION RESILIENCE: Catch panics from corrupted split files during directory access
    let bundle_directory = match std::panic::catch_unwind(|| {
        get_tantivy_directory_from_split_bundle_full_access(&split_path_str)
    }) {
        Ok(Ok(directory)) => directory,
        Ok(Err(e)) => {
            debug_log!("🚨 SPLIT ACCESS ERROR: Failed to open split directory for '{}': {}", split_path_str, e);
            return Err(anyhow!("Failed to open split directory: {}", e));
        },
        Err(_panic_info) => {
            debug_log!("🚨 PANIC CAUGHT: Split file '{}' caused panic during directory access - file is corrupted", split_path_str);
            return Err(anyhow!("Split file is corrupted and caused panic during access"));
        }
    };

    // Open the output directory (writable)
    let output_directory = MmapDirectory::open(output_dir)?;

    // ✅ CORRUPTION RESILIENCE: Try to open index and load metadata with error handling
    // Note: We can't use catch_unwind here due to interior mutability in Directory types
    let tokenizer_manager = create_quickwit_tokenizer_manager();
    let temp_bundle_index = match open_index(bundle_directory.box_clone(), &tokenizer_manager) {
        Ok(index) => index,
        Err(e) => {
            debug_log!("🚨 INDEX OPEN ERROR: Failed to open index for '{}': {}", split_path_str, e);
            return Err(anyhow!("Failed to open index: {}", e));
        }
    };

    let index_meta = match resilient_ops::resilient_load_metas(&temp_bundle_index) {
        Ok(meta) => meta,
        Err(e) => {
            debug_log!("🚨 METADATA LOAD ERROR: Failed to load metadata for '{}': {}", split_path_str, e);
            return Err(anyhow!("Failed to load index metadata: {}", e));
        }
    };

    // Copy all segment files and meta.json
    let mut copied_files = 0;

    // Copy meta.json
    if bundle_directory.exists(Path::new("meta.json"))? {
        debug_log!("Copying meta.json");
        let meta_data = bundle_directory.atomic_read(Path::new("meta.json"))?;
        output_directory.atomic_write(Path::new("meta.json"), &meta_data)?;
        copied_files += 1;
    }

    // Copy all segment-related files
    for segment_meta in &index_meta.segments {
        let segment_id = segment_meta.id().uuid_string();
        debug_log!("Copying files for segment: {}", segment_id);

        // Copy common segment files (this is a simplified approach - in practice Tantivy has many file types)
        let file_patterns = vec![
            format!("{}.store", segment_id),
            format!("{}.pos", segment_id),
            format!("{}.idx", segment_id),
            format!("{}.term", segment_id),
            format!("{}.fieldnorm", segment_id),
            format!("{}.fast", segment_id),
        ];

        for file_pattern in file_patterns {
            let file_path = Path::new(&file_pattern);
            if bundle_directory.exists(file_path)? {
                debug_log!("Copying file: {}", file_pattern);
                let file_data = bundle_directory.atomic_read(file_path)?;
                output_directory.atomic_write(file_path, &file_data)?;
                copied_files += 1;
            }
        }
    }

    debug_log!("Successfully extracted split to directory (copied {} files)", copied_files);
    Ok(())
}

/// ✅ REENTRANCY FIX: Create temporary directory with enhanced collision avoidance
/// Uses retry logic and collision detection to prevent race conditions
pub fn create_temp_directory_with_base(prefix: &str, base_path: Option<&str>) -> Result<temp::TempDir> {
    create_temp_directory_with_retries(prefix, base_path, 5)
}

/// ✅ REENTRANCY FIX: Create temporary directory with retry logic for collision avoidance
pub fn create_temp_directory_with_retries(prefix: &str, base_path: Option<&str>, max_retries: usize) -> Result<temp::TempDir> {
    let mut last_error = None;

    for attempt in 0..max_retries {
        // Add random suffix to prefix for additional collision avoidance
        let random_suffix = uuid::Uuid::new_v4().simple().to_string()[..8].to_string();
        let enhanced_prefix = format!("{}_{}_", prefix, random_suffix);

        let mut builder = temp::Builder::new();
        builder.prefix(&enhanced_prefix);

        let result = if let Some(custom_base) = base_path {
            // Validate base path on each attempt (could change between attempts)
            let base_path_buf = PathBuf::from(custom_base);
            if !base_path_buf.exists() {
                return Err(anyhow!("Custom temp directory base path does not exist: {}", custom_base));
            }
            if !base_path_buf.is_dir() {
                return Err(anyhow!("Custom temp directory base path is not a directory: {}", custom_base));
            }

            debug_log!("🏗️ REENTRANCY FIX: Using custom temp directory base: {} (attempt {})", custom_base, attempt + 1);
            builder.tempdir_in(&base_path_buf)
        } else {
            debug_log!("🏗️ REENTRANCY FIX: Using system temp directory (attempt {})", attempt + 1);
            builder.tempdir()
        };

        match result {
            Ok(temp_dir) => {
                if attempt > 0 {
                    debug_log!("✅ REENTRANCY FIX: Successfully created temp directory on attempt {}", attempt + 1);
                }
                return Ok(temp_dir);
            }
            Err(e) => {
                debug_log!("⚠️ REENTRANCY FIX: Temp directory creation failed on attempt {}: {}", attempt + 1, e);
                last_error = Some(e);

                // Add exponential backoff delay for retries
                if attempt < max_retries - 1 {
                    let delay_ms = 10u64 * (1 << attempt); // 10ms, 20ms, 40ms, 80ms
                    std::thread::sleep(std::time::Duration::from_millis(delay_ms));
                }
            }
        }
    }

    match last_error {
        Some(e) => Err(anyhow!("Failed to create temp directory with prefix '{}' after {} attempts: {}", prefix, max_retries, e)),
        None => Err(anyhow!("Failed to create temp directory with prefix '{}' after {} attempts: unknown error", prefix, max_retries)),
    }
}

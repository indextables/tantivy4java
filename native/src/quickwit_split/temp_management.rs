// temp_management.rs - Temporary directory management and split extraction
// Extracted from mod.rs during refactoring
// Contains: extract_split_to_directory_impl, create_temp_directory functions, ExtractedSplit

use std::path::{Path, PathBuf};
use std::io::Write;
use tempfile as temp;
use anyhow::{anyhow, Result};
use tantivy::directory::Directory;
use tantivy::HasLen;  // Required for FileSlice::len()

use crate::debug_println;

/// Chunk size for streaming file copies - 16MB provides good balance between
/// memory usage and I/O efficiency for large splits (avg 4GB).
/// For a 4GB file: 256 read operations vs 4096 with 1MB chunks.
/// Quickwit uses 100MB chunks for S3, but we use smaller for local extraction.
const STREAMING_CHUNK_SIZE: usize = 16_777_216;  // 16MB

/// Threshold below which we use atomic read/write (faster for small files)
/// Most segment metadata files (meta.json, .fieldnorm) are under 1MB
const SMALL_FILE_THRESHOLD: usize = 1_048_576;  // 1MB
use quickwit_indexing::open_index;
use super::split_utils::{create_quickwit_tokenizer_manager, get_tantivy_directory_from_split_bundle_full_access};
use super::resilient_ops;

// Debug logging macro - controlled by TANTIVY4JAVA_DEBUG environment variable
macro_rules! debug_log {
    ($($arg:tt)*) => {
        debug_println!("DEBUG: {}", format!($($arg)*))
    };
}

/// Stream-copy a file from source directory to destination directory.
/// Uses bounded memory chunks (1MB) instead of loading entire file into memory.
///
/// For small files (< 64KB), uses atomic read/write for efficiency.
/// For large files, streams in chunks to prevent memory spikes.
///
/// Returns the number of bytes copied.
fn streaming_copy_file(
    source_dir: &dyn Directory,
    dest_dir: &dyn Directory,
    file_path: &Path,
) -> Result<u64> {
    // Open source file slice (lazy - no data loaded yet)
    let file_slice = source_dir.open_read(file_path)?;
    let file_len = file_slice.len() as usize;

    // For small files, use atomic operations (simpler and faster)
    if file_len <= SMALL_FILE_THRESHOLD {
        debug_log!("📄 Small file ({} bytes), using atomic copy for {:?}", file_len, file_path);
        let data = source_dir.atomic_read(file_path)?;
        dest_dir.atomic_write(file_path, &data)?;
        return Ok(file_len as u64);
    }

    // For large files, stream in chunks to bound memory usage
    debug_log!("📦 Large file ({} bytes), streaming in {} byte chunks for {:?}",
               file_len, STREAMING_CHUNK_SIZE, file_path);

    // Open destination file for writing
    let mut writer = dest_dir.open_write(file_path)?;

    let mut bytes_copied = 0u64;
    let mut offset = 0usize;

    while offset < file_len {
        let chunk_end = std::cmp::min(offset + STREAMING_CHUNK_SIZE, file_len);

        // Read chunk using FileSlice::read_bytes_slice()
        // This returns OwnedBytes which is a lightweight view into the underlying data
        let chunk_data = file_slice.read_bytes_slice(offset..chunk_end)?;

        // Write chunk to destination
        writer.write_all(chunk_data.as_slice())?;

        bytes_copied += (chunk_end - offset) as u64;
        offset = chunk_end;
    }

    writer.flush()?;
    // WritePtr auto-terminates on drop

    debug_log!("✅ Streamed {} bytes for {:?}", bytes_copied, file_path);
    Ok(bytes_copied)
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

    // Copy all segment files and meta.json using streaming to avoid memory spikes
    let mut copied_files = 0;
    let mut total_bytes = 0u64;

    // Copy meta.json (small file, will use atomic copy internally)
    if bundle_directory.exists(Path::new("meta.json"))? {
        debug_log!("Copying meta.json");
        let bytes = streaming_copy_file(
            bundle_directory.as_ref(),
            &output_directory,
            Path::new("meta.json")
        )?;
        total_bytes += bytes;
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
                // Use streaming copy to avoid memory spikes for large segment files
                let bytes = streaming_copy_file(
                    bundle_directory.as_ref(),
                    &output_directory,
                    file_path
                )?;
                total_bytes += bytes;
                copied_files += 1;
            }
        }
    }

    // Copy parquet companion manifest if present
    let manifest_path = Path::new(crate::parquet_companion::manifest_io::MANIFEST_FILENAME);
    if bundle_directory.exists(manifest_path)? {
        debug_log!("📦 PARQUET_COMPANION: Copying {} from split bundle", crate::parquet_companion::manifest_io::MANIFEST_FILENAME);
        let bytes = streaming_copy_file(
            bundle_directory.as_ref(),
            &output_directory,
            manifest_path
        )?;
        total_bytes += bytes;
        copied_files += 1;
    }

    debug_log!("✅ Successfully extracted split to directory (copied {} files, {} bytes total)", copied_files, total_bytes);
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

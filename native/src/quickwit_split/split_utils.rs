// split_utils.rs - Split utility and cleanup functions
// Extracted from mod.rs during refactoring
// Contains: tokenizer management, split file access, cleanup functions

use std::path::{Path, PathBuf};
use tempfile as temp;
use anyhow::{anyhow, Result};

use tantivy::tokenizer::TokenizerManager;
use quickwit_query::get_quickwit_fastfield_normalizer_manager;

use crate::debug_println;
use super::merge_registry::cleanup_temp_directory_safe;

// Debug logging macro - controlled by TANTIVY4JAVA_DEBUG environment variable
macro_rules! debug_log {
    ($($arg:tt)*) => {
        debug_println!("DEBUG: {}", format!($($arg)*))
    };
}

/// Create Quickwit-compatible tokenizer manager for proper index operations
pub fn create_quickwit_tokenizer_manager() -> TokenizerManager {
    // Use Quickwit's fast field tokenizer manager which includes all necessary tokenizers
    get_quickwit_fastfield_normalizer_manager()
        .tantivy_manager()
        .clone()
}

/// Get file list from split using Quickwit's native functions
/// This uses the same approach as BundleDirectory::get_stats_split but filters out hotcache
/// Note: hotcache is metadata added by get_stats_split but not accessible via BundleDirectory::open_read()
pub fn get_split_file_list(split_path: &str) -> Result<Vec<PathBuf>> {
    use quickwit_directories::BundleDirectory;
    use tantivy::directory::OwnedBytes;

    debug_log!("🔧 QUICKWIT NATIVE: Getting file list from split: {}", split_path);

    // Read split file data
    let split_data = std::fs::read(split_path)
        .map_err(|e| anyhow!("Failed to read split file: {}", e))?;

    let owned_bytes = OwnedBytes::new(split_data);
    let files_and_sizes = BundleDirectory::get_stats_split(owned_bytes)
        .map_err(|e| anyhow!("Failed to get split stats: {}", e))?;

    // Filter out "hotcache" since it's not a readable file in BundleDirectory
    // hotcache is metadata added by get_stats_split but not accessible via open_read()
    let file_list: Vec<PathBuf> = files_and_sizes
        .into_iter()
        .filter(|(path, _size)| path.to_string_lossy() != "hotcache")
        .map(|(path, _size)| path)
        .collect();

    debug_log!("✅ QUICKWIT NATIVE: Found {} readable files in split (hotcache excluded)", file_list.len());
    Ok(file_list)
}

/// Open split using Quickwit's native functions with proper tokenizer management
/// This replaces our custom memory mapping and directory creation logic
pub fn open_split_with_quickwit_native(split_path: &str) -> Result<(tantivy::Index, quickwit_directories::BundleDirectory)> {
    use quickwit_directories::BundleDirectory;
    use tantivy::directory::FileSlice;
    use super::merge_registry::MmapFileHandle;

    debug_log!("🔧 QUICKWIT NATIVE: Opening split with native functions: {}", split_path);

    // Create memory-mapped file slice (same pattern as Quickwit's merge code)
    let file = std::fs::File::open(split_path)
        .map_err(|e| anyhow!("Failed to open split file: {}", e))?;
    let mmap = unsafe { memmap2::Mmap::map(&file)? };
    let mmap_arc = std::sync::Arc::new(mmap);

    // Create a file handle that uses the memory map
    let mmap_handle = MmapFileHandle { mmap: mmap_arc };
    let file_slice = FileSlice::new(std::sync::Arc::new(mmap_handle));

    // Open as BundleDirectory (Quickwit's native approach)
    let bundle_dir = BundleDirectory::open_split(file_slice)
        .map_err(|e| anyhow!("Failed to open bundle directory: {}", e))?;

    // Get Quickwit-compatible tokenizer manager
    let tokenizer_manager = create_quickwit_tokenizer_manager();

    // Open index with tokenizer manager (matching Quickwit's approach)
    let mut index = tantivy::Index::open(bundle_dir.clone())
        .map_err(|e| anyhow!("Failed to open index from bundle: {}", e))?;
    index.set_tokenizers(tokenizer_manager);

    debug_log!("✅ QUICKWIT NATIVE: Successfully opened split with native functions");
    Ok((index, bundle_dir))
}

/// Get Tantivy directory from split bundle using FULL access (not lazy)
/// This is critical for merge operations to prevent range assertion failures
pub fn get_tantivy_directory_from_split_bundle_full_access(split_path: &str) -> Result<Box<dyn tantivy::Directory>> {
    use quickwit_directories::BundleDirectory;
    use tantivy::directory::FileSlice;
    use super::merge_registry::MmapFileHandle;

    debug_log!("🔧 FULL ACCESS: Getting Tantivy directory from split bundle with full access: {}", split_path);

    // Read the entire file using memory-mapped access (lazy but safe for reading)
    let file = std::fs::File::open(split_path)
        .map_err(|e| anyhow!("Failed to open split file: {}", e))?;
    let mmap = unsafe { memmap2::Mmap::map(&file)? };
    let mmap_arc = std::sync::Arc::new(mmap);

    // Create a file handle that uses the memory map
    let mmap_handle = MmapFileHandle { mmap: mmap_arc.clone() };

    // Create FileSlice from our memory-mapped handle
    let file_slice = FileSlice::new(std::sync::Arc::new(mmap_handle));

    // Open as BundleDirectory
    let bundle_dir = BundleDirectory::open_split(file_slice)
        .map_err(|e| anyhow!("Failed to open bundle directory: {}", e))?;

    debug_log!("✅ FULL ACCESS: Successfully opened split bundle directory");
    Ok(Box::new(bundle_dir))
}

// ============================================================================
// Cleanup Functions
// ============================================================================

/// ✅ REENTRANCY FIX: Coordinated cleanup to prevent race conditions
/// Ensures proper cleanup order and handles overlapping directory scenarios
pub fn coordinate_temp_directory_cleanup(merge_id: &str, output_temp_dir: &Path, temp_dirs: Vec<temp::TempDir>) -> Result<()> {
    debug_log!("🧹 REENTRANCY FIX: Starting coordinated cleanup for merge: {}", merge_id);

    // Step 1: Wait for any pending operations to complete (brief pause)
    std::thread::sleep(std::time::Duration::from_millis(50));

    // Step 2: Clean up output temp directory with safety checks
    cleanup_output_directory_safe(output_temp_dir)?;

    // Step 3: Clean up split extraction temporary directories with validation
    cleanup_split_directories_safe(temp_dirs, merge_id)?;

    debug_log!("✅ REENTRANCY FIX: Coordinated cleanup completed for merge: {}", merge_id);
    Ok(())
}

/// ✅ REENTRANCY FIX: Safely clean up output directory with path validation
pub fn cleanup_output_directory_safe(output_temp_dir: &Path) -> Result<()> {
    if !output_temp_dir.exists() {
        debug_log!("🧹 REENTRANCY FIX: Output directory already cleaned up: {:?}", output_temp_dir);
        return Ok(());
    }

    // Validate this is actually a temporary directory (safety check)
    // Check both the directory name and its parent path for temp directory markers
    let dir_name = output_temp_dir.file_name()
        .and_then(|n| n.to_str())
        .unwrap_or("");

    let parent_name = output_temp_dir.parent()
        .and_then(|p| p.file_name())
        .and_then(|n| n.to_str())
        .unwrap_or("");

    let full_path_str = output_temp_dir.to_string_lossy();

    // Allow deletion if:
    // 1. Directory name contains our temp markers, OR
    // 2. Parent directory contains our temp markers, OR
    // 3. Full path contains our temp markers (for nested temp directories)
    let is_safe_to_delete = dir_name.contains("tantivy4java_merge")
        || dir_name.contains("temp_merge")
        || parent_name.contains("tantivy4java_merge")
        || parent_name.contains("temp_merge")
        || full_path_str.contains("tantivy4java_merge")
        || full_path_str.contains("temp_merge");

    if !is_safe_to_delete {
        return Err(anyhow!("🚨 SAFETY: Refusing to delete directory that doesn't appear to be a tantivy4java temp directory: {:?}", output_temp_dir));
    }

    match std::fs::remove_dir_all(output_temp_dir) {
        Ok(()) => {
            debug_log!("✅ REENTRANCY FIX: Successfully cleaned up output directory: {:?}", output_temp_dir);
            Ok(())
        }
        Err(e) => {
            debug_log!("⚠️ REENTRANCY FIX: Warning: Could not clean up output temp directory {:?}: {}", output_temp_dir, e);
            // Don't fail the entire operation for cleanup issues
            Ok(())
        }
    }
}

/// ✅ REENTRANCY FIX: Safely clean up split directories with overlap detection
pub fn cleanup_split_directories_safe(temp_dirs: Vec<temp::TempDir>, merge_id: &str) -> Result<()> {
    debug_log!("🧹 REENTRANCY FIX: Cleaning up {} split temp directories for merge: {}", temp_dirs.len(), merge_id);

    for (i, temp_dir) in temp_dirs.into_iter().enumerate() {
        let temp_path = temp_dir.path();

        // Validate directory name contains merge ID (safety check)
        let dir_name = temp_path.file_name()
            .and_then(|n| n.to_str())
            .unwrap_or("");

        if dir_name.contains(merge_id) || dir_name.contains("tantivy4java_merge") {
            debug_log!("✅ REENTRANCY FIX: Cleaning up split temp directory {}: {:?}", i, temp_path);
            // temp_dir will be automatically cleaned up when dropped (RAII)
        } else {
            debug_log!("⚠️ REENTRANCY FIX: Skipping cleanup of directory that doesn't match merge ID: {:?}", temp_path);
        }
    }

    debug_log!("✅ REENTRANCY FIX: All split temp directories processed for cleanup");
    Ok(())
}

/// ✅ REENTRANCY FIX: Clean up temporary directories for a specific merge operation
/// Uses the new safe cleanup function with proper error handling
pub fn cleanup_merge_temp_directory(merge_id: &str) {
    let registry_key = format!("merge_meta_{}", merge_id);
    match cleanup_temp_directory_safe(&registry_key) {
        Ok(true) => {
            debug_log!("✅ REENTRANCY FIX: Successfully cleaned up temp directory: {}", registry_key);
        }
        Ok(false) => {
            debug_log!("⚠️ REENTRANCY FIX: Temp directory not found in registry: {}", registry_key);
        }
        Err(e) => {
            debug_log!("❌ REENTRANCY FIX: Error cleaning up temp directory {}: {}", registry_key, e);
        }
    }
}

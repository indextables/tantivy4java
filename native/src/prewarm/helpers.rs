// helpers.rs - Utility functions for prewarm module
// Extracted from mod.rs during refactoring

use jni::sys::jlong;

use crate::debug_println;
use crate::global_cache::clear_global_l1_cache;

/// Clear global L1 ByteRangeCache after prewarm to prevent memory exhaustion.
///
/// This can be called after prewarm operations complete to release memory.
/// The prewarmed data is safely persisted in L2 disk cache, so clearing L1 is safe.
/// Subsequent queries will populate L1 on-demand from L2 (disk cache hits).
///
/// Note: Since L1 is now bounded with auto-eviction (256MB default), this is optional
/// but still useful for reclaiming memory immediately after large prewarm operations.
#[allow(dead_code)] // Public API for optional JNI use
pub fn clear_l1_cache_after_prewarm(_searcher_ptr: jlong) {
    // Clear the global shared L1 cache (no longer per-searcher)
    clear_global_l1_cache();
    debug_println!("🧹 PREWARM_L1_CLEAR: Global L1 ByteRangeCache cleared after prewarm");
}

/// Parse split URI into (storage_loc, split_id) for L2 disk cache keys.
///
/// **CRITICAL**: These keys MUST match how `StorageWithPersistentCache` constructs its keys,
/// otherwise prewarm data will never be found during queries!
///
/// StorageWithPersistentCache uses:
/// - `storage_loc` = full split URI (e.g., `"s3://bucket/path/abc123.split"`)
/// - `split_id` = filename without `.split` extension (e.g., `"abc123"`)
pub(crate) fn parse_split_uri(split_uri: &str) -> (String, String) {
    // storage_loc = full URI (matches StorageWithPersistentCache constructor)
    let storage_loc = split_uri.to_string();

    // split_id = filename without .split extension (matches split_id_for_cache logic)
    let split_filename = if let Some(last_slash_pos) = split_uri.rfind('/') {
        &split_uri[last_slash_pos + 1..]
    } else {
        split_uri
    };

    let split_id = if split_filename.ends_with(".split") {
        split_filename[..split_filename.len() - 6].to_string()
    } else {
        split_filename.to_string()
    };

    debug_println!(
        "🔑 PREWARM_CACHE_KEY: storage_loc='{}', split_id='{}'",
        storage_loc,
        split_id
    );
    (storage_loc, split_id)
}

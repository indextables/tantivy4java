// path_helpers.rs - Path generation for disk cache
// Extracted from mod.rs during refactoring

#![allow(dead_code)]

use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::ops::Range;
use std::path::{Path, PathBuf};

use super::types::CompressionAlgorithm;

/// Subdirectory name for the disk cache within the root path
pub const CACHE_SUBDIR: &str = "tantivy4java_slicecache";

/// Get the cache directory path (root_path/tantivy4java_slicecache)
pub fn cache_dir(root_path: &Path) -> PathBuf {
    root_path.join(CACHE_SUBDIR)
}

/// Generate storage location hash for directory naming
pub fn storage_loc_hash(storage_loc: &str) -> String {
    // Parse storage_loc to extract scheme and bucket
    let (scheme, rest) = if storage_loc.starts_with("s3://") {
        ("s3", &storage_loc[5..])
    } else if storage_loc.starts_with("azure://") {
        ("azure", &storage_loc[8..])
    } else if storage_loc.starts_with("file://") {
        ("file", &storage_loc[7..])
    } else {
        ("local", storage_loc)
    };

    let bucket = rest.split('/').next().unwrap_or("default");

    let mut hasher = DefaultHasher::new();
    storage_loc.hash(&mut hasher);
    let hash = hasher.finish();

    format!("{}_{}__{:08x}", scheme, bucket, hash as u32)
}

/// Get the directory path for a split
pub fn split_dir(root_path: &Path, storage_loc: &str, split_id: &str) -> PathBuf {
    cache_dir(root_path)
        .join(storage_loc_hash(storage_loc))
        .join(split_id)
}

/// Get the file path for a cached component
pub fn component_path(
    root_path: &Path,
    storage_loc: &str,
    split_id: &str,
    component: &str,
    byte_range: Option<Range<u64>>,
    compression: CompressionAlgorithm,
) -> PathBuf {
    let dir = split_dir(root_path, storage_loc, split_id);
    let base_name = match byte_range {
        Some(range) => format!("{}_{}-{}", component, range.start, range.end),
        None => format!("{}_full", component),
    };
    let ext = match compression {
        CompressionAlgorithm::None => "cache",
        CompressionAlgorithm::Lz4 => "cache.lz4",
        CompressionAlgorithm::Zstd => "cache.zst",
    };
    dir.join(format!("{}.{}", base_name, ext))
}

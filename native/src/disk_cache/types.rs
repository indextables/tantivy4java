// types.rs - Core types and configuration for disk cache
// Extracted from mod.rs during refactoring

#![allow(dead_code)]

use std::io;
use std::path::PathBuf;

use fs2::available_space;
use serde::{Deserialize, Serialize};

/// Default maximum number of memory-mapped files to keep open.
pub const DEFAULT_MMAP_CACHE_SIZE: usize = 1024;

/// Compression algorithm for cached data
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
pub enum CompressionAlgorithm {
    /// No compression - use for already-compressed or small data
    None,
    /// LZ4 compression - fast, good for index data (default)
    #[default]
    Lz4,
    /// Zstd compression - better ratio, slower (for cold data)
    Zstd,
}

impl CompressionAlgorithm {
    /// Convert from Java enum ordinal
    pub fn from_ordinal(ordinal: i32) -> Self {
        match ordinal {
            0 => CompressionAlgorithm::None,
            1 => CompressionAlgorithm::Lz4,
            2 => CompressionAlgorithm::Zstd,
            _ => CompressionAlgorithm::Lz4, // Default
        }
    }
}

/// Configuration for the L2 disk cache
#[derive(Debug, Clone)]
pub struct DiskCacheConfig {
    /// Root directory for cache storage
    pub root_path: PathBuf,
    /// Maximum cache size in bytes (0 = auto: 2/3 of available space)
    pub max_size_bytes: u64,
    /// Compression algorithm to use
    pub compression: CompressionAlgorithm,
    /// Minimum data size to consider compression (bytes)
    pub min_compress_size: usize,
    /// Sync manifest every N seconds (0 = on every write)
    pub manifest_sync_interval_secs: u64,
    /// Maximum number of memory-mapped files to cache (0 = use default)
    /// Higher values use more file descriptors but improve random access performance.
    /// Production systems with high fd limits can safely use 2048-4096.
    pub mmap_cache_size: usize,
}

impl Default for DiskCacheConfig {
    fn default() -> Self {
        Self {
            root_path: PathBuf::from("/tmp/tantivy4java_cache"),
            max_size_bytes: 0, // Auto-detect
            compression: CompressionAlgorithm::Lz4,
            min_compress_size: 4096, // 4KB
            manifest_sync_interval_secs: 30,
            mmap_cache_size: DEFAULT_MMAP_CACHE_SIZE,
        }
    }
}

impl DiskCacheConfig {
    pub fn new(root_path: impl Into<PathBuf>) -> Self {
        Self {
            root_path: root_path.into(),
            ..Default::default()
        }
    }

    /// Calculate actual max size (auto-detect if 0)
    pub fn effective_max_size(&self) -> io::Result<u64> {
        if self.max_size_bytes > 0 {
            return Ok(self.max_size_bytes);
        }

        // Auto-detect: 2/3 of available space on the cache partition
        let available = available_space(&self.root_path).unwrap_or_else(|_| {
            // If path doesn't exist yet, check parent
            self.root_path
                .parent()
                .and_then(|p| available_space(p).ok())
                .unwrap_or(10 * 1024 * 1024 * 1024) // 10GB fallback
        });

        Ok((available * 2) / 3)
    }
}

/// Metadata for a cached component entry
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ComponentEntry {
    /// File path relative to split directory
    pub file_name: String,
    /// Original uncompressed size
    pub uncompressed_size_bytes: u64,
    /// Size on disk (after compression if any)
    pub disk_size_bytes: u64,
    /// Compression used (None if uncompressed)
    pub compression: CompressionAlgorithm,
    /// Component type (term, idx, pos, etc.)
    pub component: String,
    /// Byte range if this is a slice (None for full component)
    pub byte_range: Option<(u64, u64)>,
    /// Creation timestamp (unix epoch seconds)
    pub created_at: u64,
}

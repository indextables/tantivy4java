// compression.rs - Compression logic for disk cache
// Extracted from mod.rs during refactoring

#![allow(dead_code)]

use std::io;
use lz4_flex::{compress_prepend_size, decompress_size_prepended};
use super::types::{CompressionAlgorithm, DiskCacheConfig};

/// Check if compression should be applied for this component and size
pub fn should_compress(config: &DiskCacheConfig, component: &str, data_size: usize) -> bool {
    if config.compression == CompressionAlgorithm::None {
        return false;
    }

    // Skip small data - compression overhead not worth it
    if data_size < config.min_compress_size {
        return false;
    }

    // Component-aware compression decisions based on internal Tantivy encoding
    // AND access patterns:
    //
    // Already compressed (skip - would waste CPU):
    //   - .store: LZ4/Zstd compressed in 16KB blocks
    //   - .term:  Zstd-compressed sstable blocks
    //
    // Block-access patterns (skip - decompressing whole file for block access is terrible):
    //   - .idx/.pos: Postings accessed in 128-doc blocks via skip lists
    //                With millions of docs, a 10MB file would need full decompression
    //                just to read a 512-byte block. Bitpacking already compacts the data.
    //
    // Random-access patterns:
    //   - .fast: Accessed by doc_id, same decompression problem as postings
    //
    match component {
        // Small/hot data - skip compression for access speed
        "footer" | "metadata" | "fieldnorm" => false,
        // Already compressed by Tantivy - don't double compress
        "store" | "term" => false,
        // Block/random access patterns - whole-file decompression would kill performance
        "idx" | "pos" | "fast" => false,
        // Default: NO compression - unknown components (like split file byte ranges) are
        // likely already compressed or have random-access patterns. Safe default is no
        // compression to avoid decompression overhead on sub-range reads.
        _ => false,
    }
}

/// Compress data if appropriate
pub fn compress_data(config: &DiskCacheConfig, component: &str, data: &[u8]) -> (Vec<u8>, CompressionAlgorithm) {
    if !should_compress(config, component, data.len()) {
        return (data.to_vec(), CompressionAlgorithm::None);
    }

    match config.compression {
        CompressionAlgorithm::None => (data.to_vec(), CompressionAlgorithm::None),
        CompressionAlgorithm::Lz4 => {
            let compressed = compress_prepend_size(data);
            // Only use compression if it actually saves space
            if compressed.len() < data.len() {
                (compressed, CompressionAlgorithm::Lz4)
            } else {
                (data.to_vec(), CompressionAlgorithm::None)
            }
        }
        CompressionAlgorithm::Zstd => {
            // Zstd not currently available as direct dependency
            // Fall back to LZ4 which provides good compression with better speed
            let compressed = compress_prepend_size(data);
            if compressed.len() < data.len() {
                (compressed, CompressionAlgorithm::Lz4)
            } else {
                (data.to_vec(), CompressionAlgorithm::None)
            }
        }
    }
}

/// Decompress data
pub fn decompress_data(data: &[u8], compression: CompressionAlgorithm) -> io::Result<Vec<u8>> {
    match compression {
        CompressionAlgorithm::None => Ok(data.to_vec()),
        CompressionAlgorithm::Lz4 => decompress_size_prepended(data)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e.to_string())),
        CompressionAlgorithm::Zstd => {
            // Zstd data would have been stored as LZ4 (see compress_data fallback)
            decompress_size_prepended(data)
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e.to_string()))
        }
    }
}

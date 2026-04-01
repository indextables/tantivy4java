// txlog/compression.rs - Gzip compression for Scala-compatible version files

use std::io::{Read, Write};
use flate2::read::GzDecoder;
use flate2::write::GzEncoder;
use flate2::Compression;

/// Gzip compress bytes (Scala-compatible, level 6 default).
pub fn gzip_compress(data: &[u8]) -> std::io::Result<Vec<u8>> {
    let mut encoder = GzEncoder::new(Vec::new(), Compression::new(6));
    encoder.write_all(data)?;
    encoder.finish()
}

/// Maximum decompressed size to prevent OOM from gzip bombs.
const MAX_DECOMPRESSED_SIZE: usize = 512 * 1024 * 1024; // 512MB

/// Gzip decompress bytes with size limit to prevent gzip bomb attacks.
pub fn gzip_decompress(data: &[u8]) -> std::io::Result<Vec<u8>> {
    let mut decoder = GzDecoder::new(data);
    let mut buf = Vec::new();
    let mut total_read = 0usize;
    let mut chunk = [0u8; 8192];
    loop {
        let n = decoder.read(&mut chunk)?;
        if n == 0 { break; }
        total_read += n;
        if total_read > MAX_DECOMPRESSED_SIZE {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("Decompressed size exceeds {}MB limit", MAX_DECOMPRESSED_SIZE / 1024 / 1024),
            ));
        }
        buf.extend_from_slice(&chunk[..n]);
    }
    Ok(buf)
}

/// Detect if bytes are gzip-compressed (magic bytes 0x1f 0x8b).
pub fn is_gzip(data: &[u8]) -> bool {
    data.len() >= 2 && data[0] == 0x1f && data[1] == 0x8b
}

/// Detect if bytes have a 2-byte compression indicator prefix followed by gzip data.
/// Scala writes version files with a 2-byte prefix (e.g., 0x01 0x01) before the gzip stream.
fn is_prefixed_gzip(data: &[u8]) -> bool {
    data.len() >= 4 && data[2] == 0x1f && data[3] == 0x8b
}

/// Transparently decompress data if gzip, otherwise return as-is.
/// Handles both raw gzip and Scala's 2-byte-prefixed gzip format.
pub fn maybe_decompress(data: &[u8]) -> std::io::Result<Vec<u8>> {
    if is_gzip(data) {
        gzip_decompress(data)
    } else if is_prefixed_gzip(data) {
        // Skip the 2-byte compression indicator prefix
        gzip_decompress(&data[2..])
    } else {
        Ok(data.to_vec())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_roundtrip() {
        let original = b"hello world, this is a test of gzip compression";
        let compressed = gzip_compress(original).unwrap();
        assert!(is_gzip(&compressed));
        let decompressed = gzip_decompress(&compressed).unwrap();
        assert_eq!(decompressed, original);
    }

    #[test]
    fn test_not_gzip() {
        assert!(!is_gzip(b"plain text"));
        assert!(!is_gzip(b""));
        assert!(!is_gzip(b"\x1f"));
    }

    #[test]
    fn test_maybe_decompress_plain() {
        let plain = b"not compressed";
        let result = maybe_decompress(plain).unwrap();
        assert_eq!(result, plain);
    }

    #[test]
    fn test_maybe_decompress_gzip() {
        let original = b"compressed data";
        let compressed = gzip_compress(original).unwrap();
        let result = maybe_decompress(&compressed).unwrap();
        assert_eq!(result, original);
    }
}

// manifest_io.rs - Serialize/deserialize the ParquetManifest with optional zstd compression

use anyhow::{Context, Result};

use super::manifest::{ParquetManifest, SUPPORTED_MANIFEST_VERSION};

/// Filename of the manifest within the split bundle
pub const MANIFEST_FILENAME: &str = "_parquet_manifest.json";

/// Zstd magic bytes: 0x28B52FFD (little-endian)
const ZSTD_MAGIC: [u8; 4] = [0x28, 0xB5, 0x2F, 0xFD];

/// Zstd compression level (3 = good balance of speed and ratio)
const ZSTD_COMPRESSION_LEVEL: i32 = 3;

/// Serialize a ParquetManifest to bytes.
/// Uses JSON, then applies zstd compression if it results in a smaller payload.
pub fn serialize_manifest(manifest: &ParquetManifest) -> Result<Vec<u8>> {
    let json_bytes = serde_json::to_vec(manifest)
        .context("Failed to serialize ParquetManifest to JSON")?;

    // Try zstd compression
    let compressed = zstd::encode_all(json_bytes.as_slice(), ZSTD_COMPRESSION_LEVEL)
        .context("Failed to zstd-compress manifest")?;

    // Use compressed only if it's actually smaller
    if compressed.len() < json_bytes.len() {
        Ok(compressed)
    } else {
        Ok(json_bytes)
    }
}

/// Deserialize a ParquetManifest from bytes.
/// Auto-detects zstd compression via magic bytes.
pub fn deserialize_manifest(data: &[u8]) -> Result<ParquetManifest> {
    let json_bytes = if is_zstd_compressed(data) {
        zstd::decode_all(data)
            .context("Failed to zstd-decompress manifest")?
    } else {
        data.to_vec()
    };

    let manifest: ParquetManifest = serde_json::from_slice(&json_bytes)
        .context("Failed to parse ParquetManifest JSON")?;

    // Version check
    if manifest.version != SUPPORTED_MANIFEST_VERSION {
        anyhow::bail!(
            "Unsupported manifest version: {} (expected {})",
            manifest.version, SUPPORTED_MANIFEST_VERSION
        );
    }

    Ok(manifest)
}

/// Check if data starts with zstd magic bytes
fn is_zstd_compressed(data: &[u8]) -> bool {
    data.len() >= 4 && data[..4] == ZSTD_MAGIC
}

#[cfg(test)]
mod tests {
    use super::*;
    use super::super::manifest::*;
    use std::collections::HashMap;

    fn make_test_manifest() -> ParquetManifest {
        ParquetManifest {
            version: SUPPORTED_MANIFEST_VERSION,
            table_root: "s3://bucket/table".to_string(),
            fast_field_mode: FastFieldMode::Disabled,
            segment_row_ranges: vec![
                SegmentRowRange { segment_ord: 0, row_offset: 0, num_rows: 500 },
            ],
            parquet_files: vec![
                ParquetFileEntry {
                    relative_path: "part-0001.parquet".to_string(),
                    file_size_bytes: 1024,
                    row_offset: 0,
                    num_rows: 500,
                    has_offset_index: false,
                    row_groups: vec![],
                },
            ],
            column_mapping: vec![],
            total_rows: 500,
            storage_config: None,
            metadata: HashMap::new(),
            string_hash_fields: HashMap::new(),
            string_indexing_modes: HashMap::new(),
            companion_hash_fields: HashMap::new(),
        }
    }

    #[test]
    fn test_roundtrip_json() {
        let manifest = make_test_manifest();
        let bytes = serde_json::to_vec(&manifest).unwrap();
        // Small manifests may not compress well, so we may get raw JSON
        let serialized = serialize_manifest(&manifest).unwrap();
        let deserialized = deserialize_manifest(&serialized).unwrap();
        assert_eq!(deserialized.total_rows, 500);
        assert_eq!(deserialized.table_root, "s3://bucket/table");
    }

    #[test]
    fn test_roundtrip_zstd() {
        // Create a large manifest that will definitely compress
        let mut manifest = make_test_manifest();
        for i in 0..100 {
            manifest.column_mapping.push(ColumnMapping {
                tantivy_field_name: format!("field_{}", i),
                parquet_column_name: format!("column_{}", i),
                physical_ordinal: i,
                parquet_type: "BYTE_ARRAY".to_string(),
                tantivy_type: "Str".to_string(),
                field_id: Some(i as i32),
                fast_field_tokenizer: None,
            });
        }
        manifest.metadata.insert("description".to_string(), "A".repeat(1000));

        let serialized = serialize_manifest(&manifest).unwrap();
        // Should be compressed (zstd magic)
        assert!(is_zstd_compressed(&serialized), "Large manifest should be zstd-compressed");

        let deserialized = deserialize_manifest(&serialized).unwrap();
        assert_eq!(deserialized.column_mapping.len(), 100);
    }

    #[test]
    fn test_version_check() {
        let mut manifest = make_test_manifest();
        manifest.version = 999;
        let json_bytes = serde_json::to_vec(&manifest).unwrap();
        let result = deserialize_manifest(&json_bytes);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("Unsupported manifest version"));
    }

    #[test]
    fn test_detect_zstd() {
        assert!(is_zstd_compressed(&[0x28, 0xB5, 0x2F, 0xFD, 0x00]));
        assert!(!is_zstd_compressed(&[0x7B, 0x22])); // JSON '{' '"'
        assert!(!is_zstd_compressed(&[0x28, 0xB5])); // Too short
    }
}

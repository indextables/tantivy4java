// merge.rs - Manifest merge logic (Phase 3)
//
// When merging splits that have parquet manifests, the manifests must be
// combined: file lists concatenated, row offsets adjusted, segment row
// ranges rebuilt.

use std::path::Path;
use anyhow::{Context, Result};

use super::manifest::*;
use super::manifest_io::{serialize_manifest, deserialize_manifest, MANIFEST_FILENAME};

use crate::debug_println;

/// Combine parquet manifests from multiple source splits during merge.
///
/// # Rules:
/// - All or no splits must have manifests (mixing is not allowed)
/// - No deletions in any source (identity mapping requirement)
/// - Same fast_field_mode across all sources
/// - Row offsets are adjusted based on cumulative row count
/// - Segment row ranges are rebuilt for the merged single segment
///
/// Returns None if no splits have manifests.
pub fn combine_parquet_manifests(
    source_dirs: &[&Path],
    output_dir: &Path,
) -> Result<Option<()>> {
    // Read manifests from source directories
    let mut manifests: Vec<Option<ParquetManifest>> = Vec::new();

    for dir in source_dirs {
        let manifest_path = dir.join(MANIFEST_FILENAME);
        if manifest_path.exists() {
            let data = std::fs::read(&manifest_path)
                .with_context(|| format!("Failed to read manifest from {:?}", manifest_path))?;
            let manifest = deserialize_manifest(&data)?;
            manifests.push(Some(manifest));
        } else {
            manifests.push(None);
        }
    }

    // Check: all or none
    let has_manifest_count = manifests.iter().filter(|m| m.is_some()).count();
    if has_manifest_count == 0 {
        return Ok(None);
    }
    if has_manifest_count != manifests.len() {
        anyhow::bail!(
            "Cannot merge: {} of {} splits have parquet manifests (must be all or none)",
            has_manifest_count,
            manifests.len()
        );
    }

    let manifests: Vec<ParquetManifest> = manifests.into_iter().map(|m| m.unwrap()).collect();

    // Validate: same fast_field_mode
    let mode = manifests[0].fast_field_mode;
    for (i, m) in manifests.iter().enumerate().skip(1) {
        if m.fast_field_mode != mode {
            anyhow::bail!(
                "Cannot merge: fast_field_mode mismatch between split[0] ({:?}) and split[{}] ({:?})",
                mode, i, m.fast_field_mode
            );
        }
    }

    // Validate: no deletions in any source split.
    // While fast-field-based resolution (__pq_file_hash / __pq_row_in_file) survives
    // segment merges, split-level merges with deletions are still rejected because
    // the deleted docs' fast field data would be discarded during compaction,
    // creating mismatches if those docs are referenced by other structures.
    for (i, dir) in source_dirs.iter().enumerate() {
        let meta_path = dir.join("meta.json");
        if !meta_path.exists() {
            crate::debug_println!(
                "⚠️ PARQUET_MERGE: Missing meta.json in source split[{}] ({:?}), \
                 skipping deletion check",
                i, dir
            );
            continue;
        }
        let meta_bytes = std::fs::read(&meta_path)
            .with_context(|| format!("Failed to read meta.json from {:?}", meta_path))?;
        let meta_str = std::str::from_utf8(&meta_bytes)
            .context("meta.json is not valid UTF-8")?;
        let meta: serde_json::Value = serde_json::from_str(meta_str)
            .context("Failed to parse meta.json")?;

        if let Some(segments) = meta.get("segments").and_then(|s| s.as_array()) {
            for seg in segments {
                let num_deleted = seg.get("num_deleted_docs")
                    .and_then(|n| n.as_u64())
                    .unwrap_or(0);
                if num_deleted > 0 {
                    anyhow::bail!(
                        "Cannot merge parquet companion splits with deletions: \
                         split[{}] has {} deleted docs. Identity doc ID mapping \
                         requires zero deletions in all source splits.",
                        i, num_deleted
                    );
                }
            }
        }
    }

    // Validate: all manifests have compatible column_mappings.
    // Mismatched column_mappings mean the splits were built from different parquet schemas,
    // which would cause data corruption after merge.
    if manifests.len() > 1 {
        let base_mapping = &manifests[0].column_mapping;
        for (i, manifest) in manifests.iter().enumerate().skip(1) {
            if manifest.column_mapping != *base_mapping {
                anyhow::bail!(
                    "Cannot merge splits with incompatible column_mappings: \
                     split[0] has {} mappings, split[{}] has {} mappings. \
                     All splits must be built from the same parquet schema.",
                    base_mapping.len(), i, manifest.column_mapping.len()
                );
            }
        }
    }

    // Build combined manifest
    let mut combined_files: Vec<ParquetFileEntry> = Vec::new();
    let mut cumulative_rows: u64 = 0;

    for manifest in &manifests {
        for file in &manifest.parquet_files {
            let mut adjusted_file = file.clone();
            adjusted_file.row_offset = cumulative_rows + file.row_offset;
            combined_files.push(adjusted_file);
        }
        cumulative_rows += manifest.total_rows;
    }

    // Rebuild segment_row_ranges: merged = single segment covering all rows
    let merged_segment_ranges = vec![SegmentRowRange {
        segment_ord: 0,
        row_offset: 0,
        num_rows: cumulative_rows,
    }];

    // Union string_hash_fields from all manifests (they should be identical for same-schema splits).
    let mut combined_hash_fields = manifests[0].string_hash_fields.clone();
    for (i, manifest) in manifests.iter().enumerate().skip(1) {
        for (k, v) in &manifest.string_hash_fields {
            if let Some(existing) = combined_hash_fields.get(k) {
                debug_assert_eq!(existing, v,
                    "string_hash_fields mismatch during merge for field {}", k);
            }
            combined_hash_fields.insert(k.clone(), v.clone());
        }
        if manifest.string_hash_fields != manifests[0].string_hash_fields {
            debug_println!(
                "⚠️ MERGE_MANIFEST: string_hash_fields differ between split[0] and split[{}]; \
                 using union (this may indicate schema inconsistency)",
                i
            );
        }
    }

    // Union string_indexing_modes from all manifests with mismatch validation.
    let mut combined_indexing_modes = manifests[0].string_indexing_modes.clone();
    for (i, manifest) in manifests.iter().enumerate().skip(1) {
        for (field, mode) in &manifest.string_indexing_modes {
            if let Some(existing) = combined_indexing_modes.get(field) {
                if existing != mode {
                    anyhow::bail!(
                        "Cannot merge: string_indexing_modes mismatch for field '{}' \
                         between split[0] ({:?}) and split[{}] ({:?})",
                        field, existing, i, mode
                    );
                }
            }
            combined_indexing_modes.insert(field.clone(), mode.clone());
        }
    }

    // Union companion_hash_fields from all manifests.
    let mut combined_companion_fields = manifests[0].companion_hash_fields.clone();
    for (_i, manifest) in manifests.iter().enumerate().skip(1) {
        for (k, v) in &manifest.companion_hash_fields {
            combined_companion_fields.insert(k.clone(), v.clone());
        }
    }

    // Use first manifest's metadata as base
    let combined = ParquetManifest {
        version: SUPPORTED_MANIFEST_VERSION,
        table_root: String::new(), // Not persisted — provided at read time via config
        fast_field_mode: mode,
        segment_row_ranges: merged_segment_ranges,
        parquet_files: combined_files,
        column_mapping: manifests[0].column_mapping.clone(),
        total_rows: cumulative_rows,
        storage_config: manifests[0].storage_config.clone(),
        metadata: manifests[0].metadata.clone(),
        string_hash_fields: combined_hash_fields,
        string_indexing_modes: combined_indexing_modes,
        companion_hash_fields: combined_companion_fields,
    };

    // Validate combined manifest
    combined.validate().map_err(|e| anyhow::anyhow!("Combined manifest validation failed: {}", e))?;

    // Write to output directory
    let output_path = output_dir.join(MANIFEST_FILENAME);
    let serialized = serialize_manifest(&combined)?;
    std::fs::write(&output_path, &serialized)
        .with_context(|| format!("Failed to write combined manifest to {:?}", output_path))?;

    debug_println!(
        "🔗 MERGE_MANIFEST: Combined {} manifests → {} files, {} total rows",
        manifests.len(),
        combined.parquet_files.len(),
        cumulative_rows
    );

    Ok(Some(()))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_test_manifest(table_root: &str, files: Vec<(&str, u64, u64)>) -> ParquetManifest {
        let mut parquet_files = Vec::new();
        let mut offset = 0u64;
        let mut total = 0u64;

        for (path, num_rows, size) in &files {
            parquet_files.push(ParquetFileEntry {
                relative_path: path.to_string(),
                file_size_bytes: *size,
                row_offset: offset,
                num_rows: *num_rows,
                has_offset_index: false,
                row_groups: vec![],
            });
            offset += num_rows;
            total += num_rows;
        }

        ParquetManifest {
            version: SUPPORTED_MANIFEST_VERSION,
            table_root: table_root.to_string(),
            fast_field_mode: FastFieldMode::Disabled,
            segment_row_ranges: vec![SegmentRowRange {
                segment_ord: 0,
                row_offset: 0,
                num_rows: total,
            }],
            parquet_files,
            column_mapping: vec![],
            total_rows: total,
            storage_config: None,
            metadata: std::collections::HashMap::new(),
            string_hash_fields: std::collections::HashMap::new(),
            string_indexing_modes: std::collections::HashMap::new(),
            companion_hash_fields: std::collections::HashMap::new(),
        }
    }

    #[test]
    fn test_combine_no_manifests() {
        let dir1 = tempfile::tempdir().unwrap();
        let dir2 = tempfile::tempdir().unwrap();
        let output = tempfile::tempdir().unwrap();

        let result = combine_parquet_manifests(
            &[dir1.path(), dir2.path()],
            output.path(),
        ).unwrap();

        assert!(result.is_none());
    }

    #[test]
    fn test_combine_mixed_manifests_fails() {
        let dir1 = tempfile::tempdir().unwrap();
        let dir2 = tempfile::tempdir().unwrap();
        let output = tempfile::tempdir().unwrap();

        // Write manifest to only dir1
        let m = make_test_manifest("s3://bucket/table", vec![("part1.parquet", 100, 1024)]);
        let serialized = serialize_manifest(&m).unwrap();
        std::fs::write(dir1.path().join(MANIFEST_FILENAME), &serialized).unwrap();

        let result = combine_parquet_manifests(
            &[dir1.path(), dir2.path()],
            output.path(),
        );

        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("must be all or none"));
    }

    #[test]
    fn test_combine_mode_mismatch_fails() {
        let dir1 = tempfile::tempdir().unwrap();
        let dir2 = tempfile::tempdir().unwrap();
        let output = tempfile::tempdir().unwrap();

        let mut m1 = make_test_manifest("s3://bucket", vec![("p1.parquet", 100, 1024)]);
        m1.fast_field_mode = FastFieldMode::Disabled;
        let mut m2 = make_test_manifest("s3://bucket", vec![("p2.parquet", 200, 2048)]);
        m2.fast_field_mode = FastFieldMode::Hybrid;

        std::fs::write(dir1.path().join(MANIFEST_FILENAME), serialize_manifest(&m1).unwrap()).unwrap();
        std::fs::write(dir2.path().join(MANIFEST_FILENAME), serialize_manifest(&m2).unwrap()).unwrap();

        let result = combine_parquet_manifests(
            &[dir1.path(), dir2.path()],
            output.path(),
        );

        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("fast_field_mode mismatch"));
    }

    #[test]
    fn test_combine_two_manifests_ok() {
        let dir1 = tempfile::tempdir().unwrap();
        let dir2 = tempfile::tempdir().unwrap();
        let output = tempfile::tempdir().unwrap();

        let m1 = make_test_manifest("s3://bucket", vec![("p1.parquet", 100, 1024)]);
        let m2 = make_test_manifest("s3://bucket", vec![("p2.parquet", 200, 2048)]);

        std::fs::write(dir1.path().join(MANIFEST_FILENAME), serialize_manifest(&m1).unwrap()).unwrap();
        std::fs::write(dir2.path().join(MANIFEST_FILENAME), serialize_manifest(&m2).unwrap()).unwrap();

        let result = combine_parquet_manifests(
            &[dir1.path(), dir2.path()],
            output.path(),
        ).unwrap();

        assert!(result.is_some());

        // Read back the combined manifest
        let combined_data = std::fs::read(output.path().join(MANIFEST_FILENAME)).unwrap();
        let combined = deserialize_manifest(&combined_data).unwrap();

        assert_eq!(combined.total_rows, 300);
        assert_eq!(combined.parquet_files.len(), 2);
        assert_eq!(combined.parquet_files[0].row_offset, 0);
        assert_eq!(combined.parquet_files[1].row_offset, 100);
        assert_eq!(combined.segment_row_ranges.len(), 1);
        assert_eq!(combined.segment_row_ranges[0].num_rows, 300);
    }

    #[test]
    fn test_combine_three_manifests_row_offsets() {
        let dir1 = tempfile::tempdir().unwrap();
        let dir2 = tempfile::tempdir().unwrap();
        let dir3 = tempfile::tempdir().unwrap();
        let output = tempfile::tempdir().unwrap();

        let m1 = make_test_manifest("s3://b", vec![("a.parquet", 50, 512)]);
        let m2 = make_test_manifest("s3://b", vec![("b.parquet", 30, 256), ("c.parquet", 20, 128)]);
        let m3 = make_test_manifest("s3://b", vec![("d.parquet", 100, 1024)]);

        std::fs::write(dir1.path().join(MANIFEST_FILENAME), serialize_manifest(&m1).unwrap()).unwrap();
        std::fs::write(dir2.path().join(MANIFEST_FILENAME), serialize_manifest(&m2).unwrap()).unwrap();
        std::fs::write(dir3.path().join(MANIFEST_FILENAME), serialize_manifest(&m3).unwrap()).unwrap();

        let result = combine_parquet_manifests(
            &[dir1.path(), dir2.path(), dir3.path()],
            output.path(),
        ).unwrap();

        assert!(result.is_some());

        let combined_data = std::fs::read(output.path().join(MANIFEST_FILENAME)).unwrap();
        let combined = deserialize_manifest(&combined_data).unwrap();

        assert_eq!(combined.total_rows, 200);
        assert_eq!(combined.parquet_files.len(), 4);
        // m1: file a at offset 0
        assert_eq!(combined.parquet_files[0].relative_path, "a.parquet");
        assert_eq!(combined.parquet_files[0].row_offset, 0);
        // m2: file b at offset 50 (after m1's 50 rows)
        assert_eq!(combined.parquet_files[1].relative_path, "b.parquet");
        assert_eq!(combined.parquet_files[1].row_offset, 50);
        // m2: file c at offset 80 (50 + 30)
        assert_eq!(combined.parquet_files[2].relative_path, "c.parquet");
        assert_eq!(combined.parquet_files[2].row_offset, 80);
        // m3: file d at offset 100 (50 + 50)
        assert_eq!(combined.parquet_files[3].relative_path, "d.parquet");
        assert_eq!(combined.parquet_files[3].row_offset, 100);
    }

    #[test]
    fn test_combine_rejects_splits_with_deletions() {
        let dir1 = tempfile::tempdir().unwrap();
        let dir2 = tempfile::tempdir().unwrap();
        let output = tempfile::tempdir().unwrap();

        let m1 = make_test_manifest("s3://bucket", vec![("p1.parquet", 100, 1024)]);
        let m2 = make_test_manifest("s3://bucket", vec![("p2.parquet", 200, 2048)]);

        std::fs::write(dir1.path().join(MANIFEST_FILENAME), serialize_manifest(&m1).unwrap()).unwrap();
        std::fs::write(dir2.path().join(MANIFEST_FILENAME), serialize_manifest(&m2).unwrap()).unwrap();

        // Write a meta.json with deletions in dir2
        let meta_with_deletions = serde_json::json!({
            "segments": [{
                "segment_id": "abc123",
                "max_doc": 200,
                "num_deleted_docs": 5
            }]
        });
        std::fs::write(
            dir2.path().join("meta.json"),
            serde_json::to_string(&meta_with_deletions).unwrap(),
        ).unwrap();

        let result = combine_parquet_manifests(
            &[dir1.path(), dir2.path()],
            output.path(),
        );

        assert!(result.is_err());
        let err_msg = result.unwrap_err().to_string();
        assert!(err_msg.contains("deletions"), "Error should mention deletions: {}", err_msg);
        assert!(err_msg.contains("split[1]"), "Error should identify split[1]: {}", err_msg);
    }

    #[test]
    fn test_combine_allows_zero_deletions() {
        let dir1 = tempfile::tempdir().unwrap();
        let dir2 = tempfile::tempdir().unwrap();
        let output = tempfile::tempdir().unwrap();

        let m1 = make_test_manifest("s3://bucket", vec![("p1.parquet", 100, 1024)]);
        let m2 = make_test_manifest("s3://bucket", vec![("p2.parquet", 200, 2048)]);

        std::fs::write(dir1.path().join(MANIFEST_FILENAME), serialize_manifest(&m1).unwrap()).unwrap();
        std::fs::write(dir2.path().join(MANIFEST_FILENAME), serialize_manifest(&m2).unwrap()).unwrap();

        // Write meta.json with zero deletions — should pass
        let meta_no_deletions = serde_json::json!({
            "segments": [{
                "segment_id": "abc123",
                "max_doc": 200,
                "num_deleted_docs": 0
            }]
        });
        std::fs::write(
            dir1.path().join("meta.json"),
            serde_json::to_string(&meta_no_deletions).unwrap(),
        ).unwrap();
        std::fs::write(
            dir2.path().join("meta.json"),
            serde_json::to_string(&meta_no_deletions).unwrap(),
        ).unwrap();

        let result = combine_parquet_manifests(
            &[dir1.path(), dir2.path()],
            output.path(),
        );

        assert!(result.is_ok());
        assert!(result.unwrap().is_some());
    }

    #[test]
    fn test_combine_rejects_incompatible_column_mappings() {
        let dir1 = tempfile::tempdir().unwrap();
        let dir2 = tempfile::tempdir().unwrap();
        let output = tempfile::tempdir().unwrap();

        let mut m1 = make_test_manifest("s3://bucket", vec![("p1.parquet", 100, 1024)]);
        m1.column_mapping = vec![ColumnMapping {
            tantivy_field_name: "id".to_string(),
            parquet_column_name: "id".to_string(),
            physical_ordinal: 0,
            parquet_type: "INT64".to_string(),
            tantivy_type: "I64".to_string(),
            field_id: None,
            fast_field_tokenizer: None,
        }];

        let mut m2 = make_test_manifest("s3://bucket", vec![("p2.parquet", 200, 2048)]);
        m2.column_mapping = vec![ColumnMapping {
            tantivy_field_name: "id".to_string(),
            parquet_column_name: "identifier".to_string(),  // different!
            physical_ordinal: 0,
            parquet_type: "INT64".to_string(),
            tantivy_type: "I64".to_string(),
            field_id: None,
            fast_field_tokenizer: None,
        }];

        std::fs::write(dir1.path().join(MANIFEST_FILENAME), serialize_manifest(&m1).unwrap()).unwrap();
        std::fs::write(dir2.path().join(MANIFEST_FILENAME), serialize_manifest(&m2).unwrap()).unwrap();

        let result = combine_parquet_manifests(
            &[dir1.path(), dir2.path()],
            output.path(),
        );

        assert!(result.is_err());
        let err_msg = result.unwrap_err().to_string();
        assert!(err_msg.contains("column_mapping"), "Error should mention column_mapping: {}", err_msg);
    }

    #[test]
    fn test_combine_string_indexing_modes() {
        use super::super::string_indexing::{StringIndexingMode, CompanionFieldInfo, UUID_REGEX};

        let dir1 = tempfile::tempdir().unwrap();
        let dir2 = tempfile::tempdir().unwrap();
        let output = tempfile::tempdir().unwrap();

        let mut m1 = make_test_manifest("s3://bucket", vec![("p1.parquet", 100, 1024)]);
        m1.string_indexing_modes.insert("trace_id".to_string(), StringIndexingMode::ExactOnly);
        m1.string_indexing_modes.insert("msg".to_string(), StringIndexingMode::TextUuidExactonly);
        m1.companion_hash_fields.insert("msg__uuids".to_string(), CompanionFieldInfo {
            original_field_name: "msg".to_string(),
            regex_pattern: UUID_REGEX.to_string(),
        });

        let mut m2 = make_test_manifest("s3://bucket", vec![("p2.parquet", 200, 2048)]);
        m2.string_indexing_modes.insert("trace_id".to_string(), StringIndexingMode::ExactOnly);
        m2.string_indexing_modes.insert("msg".to_string(), StringIndexingMode::TextUuidExactonly);
        m2.companion_hash_fields.insert("msg__uuids".to_string(), CompanionFieldInfo {
            original_field_name: "msg".to_string(),
            regex_pattern: UUID_REGEX.to_string(),
        });

        std::fs::write(dir1.path().join(MANIFEST_FILENAME), serialize_manifest(&m1).unwrap()).unwrap();
        std::fs::write(dir2.path().join(MANIFEST_FILENAME), serialize_manifest(&m2).unwrap()).unwrap();

        let result = combine_parquet_manifests(
            &[dir1.path(), dir2.path()],
            output.path(),
        ).unwrap();
        assert!(result.is_some());

        let combined_data = std::fs::read(output.path().join(MANIFEST_FILENAME)).unwrap();
        let combined = deserialize_manifest(&combined_data).unwrap();

        assert_eq!(combined.string_indexing_modes.len(), 2);
        assert_eq!(combined.string_indexing_modes["trace_id"], StringIndexingMode::ExactOnly);
        assert_eq!(combined.companion_hash_fields.len(), 1);
        assert!(combined.companion_hash_fields.contains_key("msg__uuids"));
    }

    #[test]
    fn test_combine_rejects_mismatched_string_indexing_modes() {
        use super::super::string_indexing::StringIndexingMode;

        let dir1 = tempfile::tempdir().unwrap();
        let dir2 = tempfile::tempdir().unwrap();
        let output = tempfile::tempdir().unwrap();

        let mut m1 = make_test_manifest("s3://bucket", vec![("p1.parquet", 100, 1024)]);
        m1.string_indexing_modes.insert("field_a".to_string(), StringIndexingMode::ExactOnly);

        let mut m2 = make_test_manifest("s3://bucket", vec![("p2.parquet", 200, 2048)]);
        m2.string_indexing_modes.insert("field_a".to_string(), StringIndexingMode::TextUuidStrip);

        std::fs::write(dir1.path().join(MANIFEST_FILENAME), serialize_manifest(&m1).unwrap()).unwrap();
        std::fs::write(dir2.path().join(MANIFEST_FILENAME), serialize_manifest(&m2).unwrap()).unwrap();

        let result = combine_parquet_manifests(
            &[dir1.path(), dir2.path()],
            output.path(),
        );
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("string_indexing_modes mismatch"));
    }

    #[test]
    fn test_combine_compatible_column_mappings_ok() {
        let dir1 = tempfile::tempdir().unwrap();
        let dir2 = tempfile::tempdir().unwrap();
        let output = tempfile::tempdir().unwrap();

        let mapping = vec![ColumnMapping {
            tantivy_field_name: "id".to_string(),
            parquet_column_name: "id".to_string(),
            physical_ordinal: 0,
            parquet_type: "INT64".to_string(),
            tantivy_type: "I64".to_string(),
            field_id: None,
            fast_field_tokenizer: None,
        }];

        let mut m1 = make_test_manifest("s3://bucket", vec![("p1.parquet", 100, 1024)]);
        m1.column_mapping = mapping.clone();
        let mut m2 = make_test_manifest("s3://bucket", vec![("p2.parquet", 200, 2048)]);
        m2.column_mapping = mapping;

        std::fs::write(dir1.path().join(MANIFEST_FILENAME), serialize_manifest(&m1).unwrap()).unwrap();
        std::fs::write(dir2.path().join(MANIFEST_FILENAME), serialize_manifest(&m2).unwrap()).unwrap();

        let result = combine_parquet_manifests(
            &[dir1.path(), dir2.path()],
            output.path(),
        );

        assert!(result.is_ok());
        assert!(result.unwrap().is_some());
    }
}

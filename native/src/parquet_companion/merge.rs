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
/// - Row offsets are adjusted based on cumulative row count, following the
///   same segment ordering quickwit's `combine_index_meta` produces for the
///   merged index (the LAST source split's docs come first).
/// - Segment row ranges are rebuilt for the merged single segment
///
/// `expected_merged_docs`, when provided, is asserted to equal the combined
/// row count as a backstop against ordering/deletion bugs (F1/F2/M7).
///
/// Returns None if no splits have manifests.
pub fn combine_parquet_manifests(
    source_dirs: &[&Path],
    output_dir: &Path,
    expected_merged_docs: Option<u64>,
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
            // A companion split always carries a meta.json; its absence means
            // the extracted split is corrupt. Skipping the deletion check here
            // could let a split with deletions through and corrupt doc→row
            // mapping, so fail instead of silently continuing (L11).
            anyhow::bail!(
                "Cannot merge: source split[{}] ({:?}) is missing meta.json; \
                 unable to verify it has no deletions.",
                i, dir
            );
        }
        let meta_bytes = std::fs::read(&meta_path)
            .with_context(|| format!("Failed to read meta.json from {:?}", meta_path))?;
        let meta_str = std::str::from_utf8(&meta_bytes)
            .context("meta.json is not valid UTF-8")?;
        let meta: serde_json::Value = serde_json::from_str(meta_str)
            .context("Failed to parse meta.json")?;

        if let Some(segments) = meta.get("segments").and_then(|s| s.as_array()) {
            for seg in segments {
                // Tantivy nests deletions under `deletes: Option<DeleteMeta>`
                // (InnerSegmentMeta, no serde flatten). The real shape is
                // {"segment_id":…,"max_doc":100,"deletes":{"num_deleted_docs":5,…}},
                // so a top-level `num_deleted_docs` lookup would always miss.
                let num_deleted = seg.get("deletes")
                    .and_then(|d| d.get("num_deleted_docs"))
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

    // Validate: all manifests share the same storage_config (M8).
    // The combined manifest keeps only split[0]'s storage_config; if split[1]
    // was built over a different bucket/endpoint, its relative paths would then
    // resolve against the wrong storage after merge (wrong rows if a same-named
    // file exists there). Reject the merge rather than silently mis-resolve.
    if manifests.len() > 1 {
        let base_storage = &manifests[0].storage_config;
        for (i, manifest) in manifests.iter().enumerate().skip(1) {
            if manifest.storage_config != *base_storage {
                anyhow::bail!(
                    "Cannot merge splits with incompatible storage_config: split[0] \
                     and split[{}] were built over different storage backends. Only \
                     split[0]'s storage_config is retained after merge, so relative \
                     paths from other splits would resolve against the wrong backend.",
                    i
                );
            }
        }
    }

    // Build combined manifest.
    //
    // CRITICAL: the merged doc order is NOT the input order. Quickwit's
    // `combine_index_meta` pops the LAST index_meta as the base then extends
    // with the rest in order, so the merged segment/doc order is
    // `[n-1, 0, 1, …, n-2]`. Positional consumers of the manifest (transcode
    // assigning doc_id = global_row + row, legacy positional retrieval, hash
    // touch-up fallback) require row_offsets that match that merged order, so
    // we concatenate manifests in the same order rather than input order.
    let n = manifests.len();
    let merge_order: Vec<usize> = std::iter::once(n - 1).chain(0..n - 1).collect();

    let mut combined_files: Vec<ParquetFileEntry> = Vec::new();
    let mut cumulative_rows: u64 = 0;

    for &idx in &merge_order {
        let manifest = &manifests[idx];
        for file in &manifest.parquet_files {
            let mut adjusted_file = file.clone();
            adjusted_file.row_offset = cumulative_rows + file.row_offset;
            combined_files.push(adjusted_file);
        }
        cumulative_rows += manifest.total_rows;
    }

    // Backstop (M7): the combined row count must equal the actual merged doc
    // count. A mismatch means an ordering bug, an unguarded deletion, or a
    // manifest whose total_rows drifted from its file rows — all of which would
    // silently map doc_ids to wrong parquet rows.
    if let Some(merged_docs) = expected_merged_docs {
        if cumulative_rows != merged_docs {
            anyhow::bail!(
                "Combined parquet manifest row count ({}) does not match merged \
                 document count ({}). This indicates deletions, an ordering bug, \
                 or corrupt manifest metadata; refusing to produce a split that \
                 would map documents to wrong parquet rows.",
                cumulative_rows, merged_docs
            );
        }
    }

    // Reject duplicate relative paths across the merged manifests. Because
    // table_root is not persisted, two splits built over different tables whose
    // files share a relative name would collide in `build_file_hash_index` (a
    // HashMap keyed by path hash keeps only the last entry), silently resolving
    // documents to the wrong parquet file.
    {
        let mut seen = std::collections::HashSet::with_capacity(combined_files.len());
        for file in &combined_files {
            if !seen.insert(file.relative_path.as_str()) {
                anyhow::bail!(
                    "Cannot merge splits: duplicate parquet file relative path '{}' \
                     across source manifests. Relative paths must be unique because \
                     table_root is not persisted and doc→parquet resolution keys on \
                     the path hash.",
                    file.relative_path
                );
            }
        }
    }

    // Rebuild segment_row_ranges: merged = single segment covering all rows
    let merged_segment_ranges = vec![SegmentRowRange {
        segment_ord: 0,
        row_offset: 0,
        num_rows: cumulative_rows,
    }];

    // Union string_hash_fields from all manifests. A hash-field-name mismatch for
    // the same tantivy field means the splits hash that field differently, which
    // would corrupt hash-based aggregation reads on the merged split — reject it
    // with a hard error rather than a release-mode no-op debug_assert (D3).
    let mut combined_hash_fields = manifests[0].string_hash_fields.clone();
    for (i, manifest) in manifests.iter().enumerate().skip(1) {
        for (k, v) in &manifest.string_hash_fields {
            if let Some(existing) = combined_hash_fields.get(k) {
                if existing != v {
                    anyhow::bail!(
                        "Cannot merge: string_hash_fields mismatch for field '{}' \
                         between split[0] ({}) and split[{}] ({})",
                        k, existing, i, v
                    );
                }
            }
            combined_hash_fields.insert(k.clone(), v.clone());
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

    // Union companion_hash_fields from all manifests with mismatch validation.
    let mut combined_companion_fields = manifests[0].companion_hash_fields.clone();
    for (i, manifest) in manifests.iter().enumerate().skip(1) {
        for (k, v) in &manifest.companion_hash_fields {
            if let Some(existing) = combined_companion_fields.get(k) {
                if existing != v {
                    anyhow::bail!(
                        "Cannot merge: companion_hash_fields mismatch for field '{}' \
                         between split[0] ({:?}) and split[{}] ({:?})",
                        k, existing, i, v
                    );
                }
            }
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

    /// Write a minimal no-deletion meta.json into a source split dir, matching
    /// what a real extracted companion split always carries. Required because
    /// combine_parquet_manifests now hard-fails on a missing meta.json (L11).
    fn write_no_deletion_meta(dir: &Path) {
        let meta = serde_json::json!({
            "segments": [{ "segment_id": "seg", "max_doc": 1, "deletes": serde_json::Value::Null }]
        });
        std::fs::write(dir.join("meta.json"), serde_json::to_string(&meta).unwrap()).unwrap();
    }

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
        None,
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
        write_no_deletion_meta(dir1.path());

        let result = combine_parquet_manifests(
            &[dir1.path(), dir2.path()],
            output.path(),
        None,
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
        write_no_deletion_meta(dir1.path());
        std::fs::write(dir2.path().join(MANIFEST_FILENAME), serialize_manifest(&m2).unwrap()).unwrap();
        write_no_deletion_meta(dir2.path());

        let result = combine_parquet_manifests(
            &[dir1.path(), dir2.path()],
            output.path(),
        None,
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
        write_no_deletion_meta(dir1.path());
        std::fs::write(dir2.path().join(MANIFEST_FILENAME), serialize_manifest(&m2).unwrap()).unwrap();
        write_no_deletion_meta(dir2.path());

        let result = combine_parquet_manifests(
            &[dir1.path(), dir2.path()],
            output.path(),
        None,
        ).unwrap();

        assert!(result.is_some());

        // Read back the combined manifest
        let combined_data = std::fs::read(output.path().join(MANIFEST_FILENAME)).unwrap();
        let combined = deserialize_manifest(&combined_data).unwrap();

        // Merged doc order mirrors quickwit's combine_index_meta: the LAST
        // split (m2) comes first, so p2 is at offset 0 and p1 follows at 200.
        assert_eq!(combined.total_rows, 300);
        assert_eq!(combined.parquet_files.len(), 2);
        assert_eq!(combined.parquet_files[0].relative_path, "p2.parquet");
        assert_eq!(combined.parquet_files[0].row_offset, 0);
        assert_eq!(combined.parquet_files[1].relative_path, "p1.parquet");
        assert_eq!(combined.parquet_files[1].row_offset, 200);
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
        write_no_deletion_meta(dir1.path());
        std::fs::write(dir2.path().join(MANIFEST_FILENAME), serialize_manifest(&m2).unwrap()).unwrap();
        write_no_deletion_meta(dir2.path());
        std::fs::write(dir3.path().join(MANIFEST_FILENAME), serialize_manifest(&m3).unwrap()).unwrap();
        write_no_deletion_meta(dir3.path());

        let result = combine_parquet_manifests(
            &[dir1.path(), dir2.path(), dir3.path()],
            output.path(),
        None,
        ).unwrap();

        assert!(result.is_some());

        let combined_data = std::fs::read(output.path().join(MANIFEST_FILENAME)).unwrap();
        let combined = deserialize_manifest(&combined_data).unwrap();

        // Merged order mirrors combine_index_meta: [m3, m1, m2] (last split
        // first). So: d (m3, 100 rows) → a (m1, 50) → b,c (m2, 30+20).
        assert_eq!(combined.total_rows, 200);
        assert_eq!(combined.parquet_files.len(), 4);
        // m3: file d at offset 0 (last split becomes the base)
        assert_eq!(combined.parquet_files[0].relative_path, "d.parquet");
        assert_eq!(combined.parquet_files[0].row_offset, 0);
        // m1: file a at offset 100 (after m3's 100 rows)
        assert_eq!(combined.parquet_files[1].relative_path, "a.parquet");
        assert_eq!(combined.parquet_files[1].row_offset, 100);
        // m2: file b at offset 150 (100 + 50)
        assert_eq!(combined.parquet_files[2].relative_path, "b.parquet");
        assert_eq!(combined.parquet_files[2].row_offset, 150);
        // m2: file c at offset 180 (150 + 30)
        assert_eq!(combined.parquet_files[3].relative_path, "c.parquet");
        assert_eq!(combined.parquet_files[3].row_offset, 180);
    }

    #[test]
    fn test_combine_rejects_splits_with_deletions() {
        let dir1 = tempfile::tempdir().unwrap();
        let dir2 = tempfile::tempdir().unwrap();
        let output = tempfile::tempdir().unwrap();

        let m1 = make_test_manifest("s3://bucket", vec![("p1.parquet", 100, 1024)]);
        let m2 = make_test_manifest("s3://bucket", vec![("p2.parquet", 200, 2048)]);

        std::fs::write(dir1.path().join(MANIFEST_FILENAME), serialize_manifest(&m1).unwrap()).unwrap();
        write_no_deletion_meta(dir1.path());
        std::fs::write(dir2.path().join(MANIFEST_FILENAME), serialize_manifest(&m2).unwrap()).unwrap();
        write_no_deletion_meta(dir2.path());

        // Write a meta.json with deletions in dir2, using tantivy's real shape:
        // deletions are nested under `deletes: {num_deleted_docs, opstamp}`.
        let meta_with_deletions = serde_json::json!({
            "segments": [{
                "segment_id": "abc123",
                "max_doc": 200,
                "deletes": { "num_deleted_docs": 5, "opstamp": 7 }
            }]
        });
        std::fs::write(
            dir2.path().join("meta.json"),
            serde_json::to_string(&meta_with_deletions).unwrap(),
        ).unwrap();

        let result = combine_parquet_manifests(
            &[dir1.path(), dir2.path()],
            output.path(),
        None,
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
        write_no_deletion_meta(dir1.path());
        std::fs::write(dir2.path().join(MANIFEST_FILENAME), serialize_manifest(&m2).unwrap()).unwrap();
        write_no_deletion_meta(dir2.path());

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
        None,
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
        write_no_deletion_meta(dir1.path());
        std::fs::write(dir2.path().join(MANIFEST_FILENAME), serialize_manifest(&m2).unwrap()).unwrap();
        write_no_deletion_meta(dir2.path());

        let result = combine_parquet_manifests(
            &[dir1.path(), dir2.path()],
            output.path(),
        None,
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
        write_no_deletion_meta(dir1.path());
        std::fs::write(dir2.path().join(MANIFEST_FILENAME), serialize_manifest(&m2).unwrap()).unwrap();
        write_no_deletion_meta(dir2.path());

        let result = combine_parquet_manifests(
            &[dir1.path(), dir2.path()],
            output.path(),
        None,
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
        write_no_deletion_meta(dir1.path());
        std::fs::write(dir2.path().join(MANIFEST_FILENAME), serialize_manifest(&m2).unwrap()).unwrap();
        write_no_deletion_meta(dir2.path());

        let result = combine_parquet_manifests(
            &[dir1.path(), dir2.path()],
            output.path(),
        None,
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
        write_no_deletion_meta(dir1.path());
        std::fs::write(dir2.path().join(MANIFEST_FILENAME), serialize_manifest(&m2).unwrap()).unwrap();
        write_no_deletion_meta(dir2.path());

        let result = combine_parquet_manifests(
            &[dir1.path(), dir2.path()],
            output.path(),
        None,
        );

        assert!(result.is_ok());
        assert!(result.unwrap().is_some());
    }
}

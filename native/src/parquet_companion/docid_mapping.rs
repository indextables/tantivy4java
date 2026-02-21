// docid_mapping.rs - DocId-to-parquet-row translation
//
// Translates tantivy (segment_ord, local_doc_id) addresses to global row indices,
// then locates which parquet file and row within that file contains the data.

use std::collections::HashMap;
use super::manifest::ParquetManifest;
use super::indexing::hash_parquet_path;

/// Result of locating a row in a specific parquet file
#[derive(Debug, Clone)]
pub struct FileRowLocation {
    /// Index into manifest.parquet_files
    pub file_idx: usize,
    /// Row index within that file (0-based)
    pub row_in_file: u64,
}

/// Translate a tantivy (segment_ord, local_doc_id) to a global row index.
///
/// The global row is computed as: segment_row_ranges[segment_ord].row_offset + local_doc_id
pub fn translate_to_global_row(
    segment_ord: u32,
    local_doc_id: u32,
    manifest: &ParquetManifest,
) -> Result<u64, String> {
    let range = manifest
        .segment_row_ranges
        .iter()
        .find(|r| r.segment_ord == segment_ord)
        .ok_or_else(|| format!("Segment ordinal {} not found in manifest", segment_ord))?;

    let global_row = range.row_offset + local_doc_id as u64;

    if local_doc_id as u64 >= range.num_rows {
        return Err(format!(
            "local_doc_id {} exceeds segment {} row count {}",
            local_doc_id, segment_ord, range.num_rows
        ));
    }

    Ok(global_row)
}

/// Locate which parquet file contains a given global row, using binary search.
///
/// Returns the file index and the row offset within that file.
pub fn locate_row_in_file(
    global_row: u64,
    manifest: &ParquetManifest,
) -> Result<FileRowLocation, String> {
    if manifest.parquet_files.is_empty() {
        return Err("No parquet files in manifest".to_string());
    }

    if global_row >= manifest.total_rows {
        return Err(format!(
            "Global row {} exceeds total_rows {}",
            global_row, manifest.total_rows
        ));
    }

    // Binary search: find the last file whose row_offset <= global_row
    let file_idx = match manifest
        .parquet_files
        .binary_search_by_key(&global_row, |f| f.row_offset)
    {
        Ok(idx) => idx,
        Err(idx) => {
            // idx is where global_row would be inserted; the file is at idx - 1
            if idx == 0 {
                return Err(format!(
                    "Global row {} is before first file offset {}",
                    global_row, manifest.parquet_files[0].row_offset
                ));
            }
            idx - 1
        }
    };

    let file = &manifest.parquet_files[file_idx];
    let row_in_file = global_row - file.row_offset;

    debug_assert!(
        row_in_file < file.num_rows,
        "row_in_file {} >= num_rows {} for file[{}]",
        row_in_file,
        file.num_rows,
        file_idx
    );

    Ok(FileRowLocation {
        file_idx,
        row_in_file,
    })
}

/// Group a set of (segment_ord, doc_id) addresses by their target parquet file.
///
/// Returns a map from file_idx to a list of (original_index, row_in_file).
/// The original_index preserves the caller's ordering for result reassembly.
pub fn group_doc_addresses_by_file(
    addresses: &[(u32, u32)], // (segment_ord, doc_id)
    manifest: &ParquetManifest,
) -> Result<HashMap<usize, Vec<(usize, u64)>>, String> {
    let mut groups: HashMap<usize, Vec<(usize, u64)>> = HashMap::new();

    for (idx, &(seg_ord, doc_id)) in addresses.iter().enumerate() {
        let global_row = translate_to_global_row(seg_ord, doc_id, manifest)?;
        let location = locate_row_in_file(global_row, manifest)?;
        groups
            .entry(location.file_idx)
            .or_default()
            .push((idx, location.row_in_file));
    }

    // Sort each file's rows for sequential access
    for rows in groups.values_mut() {
        rows.sort_by_key(|&(_, row)| row);
    }

    Ok(groups)
}

/// Build a lookup table from parquet file path hash → index into manifest.parquet_files.
///
/// This enables O(1) resolution from the __pq_file_hash fast field to the file entry.
/// Should be built once at searcher creation time and reused across all retrievals.
pub fn build_file_hash_index(manifest: &ParquetManifest) -> HashMap<u64, usize> {
    manifest
        .parquet_files
        .iter()
        .enumerate()
        .map(|(idx, entry)| (hash_parquet_path(&entry.relative_path), idx))
        .collect()
}

/// Resolve a document's parquet location using fast field values.
///
/// Instead of the old segment_ord → global_row → file lookup, this uses
/// the __pq_file_hash and __pq_row_in_file values stored as fast fields
/// in each tantivy document. These values survive all merge operations.
pub fn resolve_via_fast_fields(
    file_hash: u64,
    row_in_file: u64,
    file_hash_index: &HashMap<u64, usize>,
) -> Result<FileRowLocation, String> {
    let file_idx = *file_hash_index
        .get(&file_hash)
        .ok_or_else(|| format!(
            "No parquet file found for hash {} — the split may reference \
             a file not in the current manifest", file_hash
        ))?;
    Ok(FileRowLocation { file_idx, row_in_file })
}

/// Group pre-resolved fast field locations by their target parquet file.
///
/// Each entry in `locations` is (original_index, file_hash, row_in_file).
/// Returns a map from file_idx to a list of (original_index, row_in_file),
/// sorted by row_in_file for sequential access within each file.
pub fn group_resolved_locations_by_file(
    locations: &[(usize, u64, u64)], // (original_idx, file_hash, row_in_file)
    file_hash_index: &HashMap<u64, usize>,
) -> Result<HashMap<usize, Vec<(usize, u64)>>, String> {
    let mut groups: HashMap<usize, Vec<(usize, u64)>> = HashMap::new();

    for &(orig_idx, file_hash, row_in_file) in locations {
        let file_idx = *file_hash_index
            .get(&file_hash)
            .ok_or_else(|| format!(
                "No parquet file found for hash {} at index {}",
                file_hash, orig_idx
            ))?;
        groups.entry(file_idx).or_default().push((orig_idx, row_in_file));
    }

    // Sort each file's rows for sequential access
    for rows in groups.values_mut() {
        rows.sort_by_key(|&(_, row)| row);
    }

    Ok(groups)
}

#[cfg(test)]
mod tests {
    use super::*;
    use super::super::manifest::*;
    use std::collections::HashMap as StdHashMap;

    fn make_multi_file_manifest() -> ParquetManifest {
        ParquetManifest {
            version: SUPPORTED_MANIFEST_VERSION,
            table_root: "/data".to_string(),
            fast_field_mode: FastFieldMode::Disabled,
            segment_row_ranges: vec![
                SegmentRowRange { segment_ord: 0, row_offset: 0, num_rows: 3000 },
            ],
            parquet_files: vec![
                ParquetFileEntry {
                    relative_path: "part-0001.parquet".to_string(),
                    file_size_bytes: 1024,
                    row_offset: 0,
                    num_rows: 1000,
                    has_offset_index: false,
                    row_groups: vec![],
                },
                ParquetFileEntry {
                    relative_path: "part-0002.parquet".to_string(),
                    file_size_bytes: 2048,
                    row_offset: 1000,
                    num_rows: 1000,
                    has_offset_index: false,
                    row_groups: vec![],
                },
                ParquetFileEntry {
                    relative_path: "part-0003.parquet".to_string(),
                    file_size_bytes: 2048,
                    row_offset: 2000,
                    num_rows: 1000,
                    has_offset_index: false,
                    row_groups: vec![],
                },
            ],
            column_mapping: vec![],
            total_rows: 3000,
            storage_config: None,
            metadata: StdHashMap::new(),
            string_hash_fields: StdHashMap::new(),
        }
    }

    #[test]
    fn test_translate_to_global_row() {
        let manifest = make_multi_file_manifest();
        assert_eq!(translate_to_global_row(0, 0, &manifest).unwrap(), 0);
        assert_eq!(translate_to_global_row(0, 500, &manifest).unwrap(), 500);
        assert_eq!(translate_to_global_row(0, 2999, &manifest).unwrap(), 2999);
    }

    #[test]
    fn test_translate_out_of_range() {
        let manifest = make_multi_file_manifest();
        assert!(translate_to_global_row(0, 3000, &manifest).is_err());
        assert!(translate_to_global_row(1, 0, &manifest).is_err()); // no segment 1
    }

    #[test]
    fn test_locate_first_file() {
        let manifest = make_multi_file_manifest();
        let loc = locate_row_in_file(0, &manifest).unwrap();
        assert_eq!(loc.file_idx, 0);
        assert_eq!(loc.row_in_file, 0);

        let loc = locate_row_in_file(999, &manifest).unwrap();
        assert_eq!(loc.file_idx, 0);
        assert_eq!(loc.row_in_file, 999);
    }

    #[test]
    fn test_locate_second_file() {
        let manifest = make_multi_file_manifest();
        let loc = locate_row_in_file(1000, &manifest).unwrap();
        assert_eq!(loc.file_idx, 1);
        assert_eq!(loc.row_in_file, 0);

        let loc = locate_row_in_file(1500, &manifest).unwrap();
        assert_eq!(loc.file_idx, 1);
        assert_eq!(loc.row_in_file, 500);
    }

    #[test]
    fn test_locate_last_file() {
        let manifest = make_multi_file_manifest();
        let loc = locate_row_in_file(2999, &manifest).unwrap();
        assert_eq!(loc.file_idx, 2);
        assert_eq!(loc.row_in_file, 999);
    }

    #[test]
    fn test_locate_out_of_range() {
        let manifest = make_multi_file_manifest();
        assert!(locate_row_in_file(3000, &manifest).is_err());
    }

    #[test]
    fn test_group_by_file() {
        let manifest = make_multi_file_manifest();
        let addresses = vec![
            (0u32, 500u32),  // file 0, row 500
            (0, 1500),       // file 1, row 500
            (0, 2500),       // file 2, row 500
            (0, 100),        // file 0, row 100
        ];
        let groups = group_doc_addresses_by_file(&addresses, &manifest).unwrap();

        // File 0 should have indices 0 and 3 (sorted by row)
        let file0 = &groups[&0];
        assert_eq!(file0.len(), 2);
        assert_eq!(file0[0], (3, 100));  // original idx 3, row 100
        assert_eq!(file0[1], (0, 500));  // original idx 0, row 500

        // File 1 should have index 1
        let file1 = &groups[&1];
        assert_eq!(file1.len(), 1);
        assert_eq!(file1[0], (1, 500));

        // File 2 should have index 2
        let file2 = &groups[&2];
        assert_eq!(file2.len(), 1);
        assert_eq!(file2[0], (2, 500));
    }

    #[test]
    fn test_build_file_hash_index() {
        let manifest = make_multi_file_manifest();
        let index = build_file_hash_index(&manifest);

        assert_eq!(index.len(), 3);
        assert_eq!(index[&hash_parquet_path("part-0001.parquet")], 0);
        assert_eq!(index[&hash_parquet_path("part-0002.parquet")], 1);
        assert_eq!(index[&hash_parquet_path("part-0003.parquet")], 2);
    }

    #[test]
    fn test_resolve_via_fast_fields() {
        let manifest = make_multi_file_manifest();
        let index = build_file_hash_index(&manifest);

        let hash1 = hash_parquet_path("part-0001.parquet");
        let loc = resolve_via_fast_fields(hash1, 42, &index).unwrap();
        assert_eq!(loc.file_idx, 0);
        assert_eq!(loc.row_in_file, 42);

        let hash3 = hash_parquet_path("part-0003.parquet");
        let loc = resolve_via_fast_fields(hash3, 999, &index).unwrap();
        assert_eq!(loc.file_idx, 2);
        assert_eq!(loc.row_in_file, 999);

        // Unknown hash should fail
        assert!(resolve_via_fast_fields(12345, 0, &index).is_err());
    }

    #[test]
    fn test_group_resolved_locations_by_file() {
        let manifest = make_multi_file_manifest();
        let index = build_file_hash_index(&manifest);

        let hash0 = hash_parquet_path("part-0001.parquet");
        let hash1 = hash_parquet_path("part-0002.parquet");
        let hash2 = hash_parquet_path("part-0003.parquet");

        let locations = vec![
            (0, hash0, 500u64),  // file 0, row 500
            (1, hash1, 500),     // file 1, row 500
            (2, hash2, 500),     // file 2, row 500
            (3, hash0, 100),     // file 0, row 100
        ];

        let groups = group_resolved_locations_by_file(&locations, &index).unwrap();

        // File 0 should have indices 0 and 3 (sorted by row)
        let file0 = &groups[&0];
        assert_eq!(file0.len(), 2);
        assert_eq!(file0[0], (3, 100));  // original idx 3, row 100
        assert_eq!(file0[1], (0, 500));  // original idx 0, row 500

        // File 1 should have index 1
        let file1 = &groups[&1];
        assert_eq!(file1.len(), 1);
        assert_eq!(file1[0], (1, 500));

        // File 2 should have index 2
        let file2 = &groups[&2];
        assert_eq!(file2.len(), 1);
        assert_eq!(file2[0], (2, 500));
    }
}

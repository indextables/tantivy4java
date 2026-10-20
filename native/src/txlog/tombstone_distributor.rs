// txlog/tombstone_distributor.rs - Tombstone filtering and compaction triggering
//
// Matches Scala's TombstoneDistributor: filters tombstoned entries out of a live
// set and decides when a state manifest needs compaction.
//
// NOTE: the partition-aware *selective* compaction helpers
// (`distribute_tombstones_to_manifests`, `selective_partition`,
// `is_selective_compaction_beneficial`) were removed when the auto-checkpoint path
// collapsed selective compaction into full compaction (TXLOG_MODULE_REVIEW E3):
// the selective branch read every manifest and produced a checkpoint identical to
// full compaction, so it bought nothing. Reintroduce them only alongside a real
// selective writer that reuses kept manifest refs.

use std::collections::HashSet;

use super::actions::FileEntry;

/// Default tombstone ratio threshold for triggering compaction.
pub const COMPACTION_TOMBSTONE_THRESHOLD: f64 = 0.10;

/// Default max manifest count before fragmentation compaction.
pub const COMPACTION_MAX_MANIFESTS: usize = 20;

/// Filter out entries whose paths appear in the removed set.
pub fn filter_tombstoned_entries(
    entries: &[FileEntry],
    removed_paths: &HashSet<String>,
) -> Vec<FileEntry> {
    entries.iter()
        .filter(|e| !removed_paths.contains(&e.add.path))
        .cloned()
        .collect()
}

// Keep the old name as an alias for backward compatibility
pub fn distribute_tombstones(
    entries: &[FileEntry],
    removed_paths: &HashSet<String>,
    _partition_columns: &[String],
) -> Vec<FileEntry> {
    filter_tombstoned_entries(entries, removed_paths)
}

/// Check if a state manifest needs compaction.
///
/// Compaction is triggered when:
/// 1. Tombstone ratio exceeds threshold (default 10%)
/// 2. Too many manifests (fragmentation, default 20)
pub fn needs_compaction(
    manifest: &super::actions::StateManifest,
    new_removes: usize,
) -> bool {
    let total_tombstones = manifest.tombstones.len() + new_removes;

    if manifest.total_file_count <= 0 {
        return true;
    }

    let tombstone_ratio = total_tombstones as f64 / manifest.total_file_count as f64;
    let manifest_count = manifest.manifests.len();

    tombstone_ratio > COMPACTION_TOMBSTONE_THRESHOLD || manifest_count > COMPACTION_MAX_MANIFESTS
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::txlog::actions::{AddAction, FileEntry, ManifestInfo};
    use std::collections::HashMap;

    fn make_entry(path: &str) -> FileEntry {
        FileEntry {
            add: AddAction {
                path: path.to_string(),
                partition_values: HashMap::new(),
                size: 100,
                modification_time: 0,
                data_change: true,
                stats: None, min_values: None, max_values: None, num_records: None,
                footer_start_offset: None, footer_end_offset: None, has_footer_offsets: None, delete_opstamp: None,
                split_tags: None, num_merge_ops: None,
                doc_mapping_json: None, doc_mapping_ref: None,
                uncompressed_size_bytes: None,
                time_range_start: None, time_range_end: None,
                companion_source_files: None, companion_delta_version: None,
                companion_fast_field_mode: None,
            },
            added_at_version: 1,
            added_at_timestamp: 0,
        }
    }

    fn make_entry_with_partition(path: &str, pv: HashMap<String, String>) -> FileEntry {
        let mut entry = make_entry(path);
        entry.add.partition_values = pv;
        entry
    }

    #[test]
    fn test_no_removals() {
        let entries = vec![make_entry("a.split"), make_entry("b.split"), make_entry("c.split")];
        let removed = HashSet::new();
        let result = distribute_tombstones(&entries, &removed, &[]);
        assert_eq!(result.len(), 3);
    }

    #[test]
    fn test_some_removals() {
        let entries = vec![make_entry("a.split"), make_entry("b.split"), make_entry("c.split")];
        let mut removed = HashSet::new();
        removed.insert("b.split".to_string());
        let result = distribute_tombstones(&entries, &removed, &[]);
        assert_eq!(result.len(), 2);
        let paths: Vec<&str> = result.iter().map(|e| e.add.path.as_str()).collect();
        assert!(paths.contains(&"a.split"));
        assert!(paths.contains(&"c.split"));
        assert!(!paths.contains(&"b.split"));
    }

    #[test]
    fn test_all_removed() {
        let entries = vec![make_entry("a.split"), make_entry("b.split")];
        let mut removed = HashSet::new();
        removed.insert("a.split".to_string());
        removed.insert("b.split".to_string());
        let result = distribute_tombstones(&entries, &removed, &[]);
        assert!(result.is_empty());
    }

    #[test]
    fn test_removal_of_nonexistent_path() {
        let entries = vec![make_entry("a.split"), make_entry("b.split")];
        let mut removed = HashSet::new();
        removed.insert("nonexistent.split".to_string());
        let result = distribute_tombstones(&entries, &removed, &[]);
        assert_eq!(result.len(), 2);
    }

    #[test]
    fn test_empty_entries() {
        let entries: Vec<FileEntry> = vec![];
        let mut removed = HashSet::new();
        removed.insert("a.split".to_string());
        let result = distribute_tombstones(&entries, &removed, &[]);
        assert!(result.is_empty());
    }

    #[test]
    fn test_partition_columns_ignored() {
        // partition_columns parameter exists for future use but is currently unused
        let mut pv = HashMap::new();
        pv.insert("year".to_string(), "2024".to_string());
        let entries = vec![
            make_entry_with_partition("a.split", pv.clone()),
            make_entry_with_partition("b.split", pv),
        ];
        let mut removed = HashSet::new();
        removed.insert("a.split".to_string());
        let result = distribute_tombstones(&entries, &removed, &["year".to_string()]);
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].add.path, "b.split");
    }

    #[test]
    fn test_preserves_entry_metadata() {
        let mut entry = make_entry("keep.split");
        entry.add.size = 42;
        entry.add.num_records = Some(999);
        entry.added_at_version = 7;
        let entries = vec![entry];
        let removed = HashSet::new();
        let result = distribute_tombstones(&entries, &removed, &[]);
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].add.size, 42);
        assert_eq!(result[0].add.num_records, Some(999));
        assert_eq!(result[0].added_at_version, 7);
    }

    #[test]
    fn test_needs_compaction_high_tombstones() {
        use crate::txlog::actions::StateManifest;
        let manifest = StateManifest {
            version: 1,
            manifests: vec![],
            partition_bounds: None,
            created_time: 0,
            total_file_count: 100,
            format: String::new(),
            protocol_json: None,
            metadata: None,
            schema_registry: HashMap::new(),
            tombstones: vec!["a".to_string(); 15], // 15 existing
            format_version: 1,
            total_bytes: 0,
            protocol_version: 4,
        };
        // 15 existing + 0 new = 15% > 10% threshold
        assert!(needs_compaction(&manifest, 0));
    }

    #[test]
    fn test_needs_compaction_too_many_manifests() {
        use crate::txlog::actions::StateManifest;
        let manifest = StateManifest {
            version: 1,
            manifests: (0..25).map(|i| ManifestInfo {
                path: format!("m{}.avro", i),
                file_count: 10,
                ..Default::default()
            }).collect(),
            partition_bounds: None,
            created_time: 0,
            total_file_count: 250,
            format: String::new(),
            protocol_json: None,
            metadata: None,
            schema_registry: HashMap::new(),
            tombstones: vec![],
            format_version: 1,
            total_bytes: 0,
            protocol_version: 4,
        };
        // 25 manifests > 20 threshold
        assert!(needs_compaction(&manifest, 0));
    }
}

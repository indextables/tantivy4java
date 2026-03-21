// txlog/tombstone_distributor.rs - Distribute tombstones during checkpoint creation
//
// When creating a checkpoint, removed files should be excluded from the
// checkpoint entirely (matching Scala behavior). This module provides
// the filtering logic.

use std::collections::HashSet;

use super::actions::FileEntry;

/// Filter out entries whose paths appear in the removed set.
///
/// When creating a checkpoint, distribute RemoveAction paths across manifests
/// so that manifest-level partition pruning can skip manifests that only contain
/// files that were later removed.
///
/// Simple strategy (matching Scala): exclude removed files from the checkpoint
/// entirely, since the checkpoint represents the current live file set.
pub fn distribute_tombstones(
    entries: &[FileEntry],
    removed_paths: &HashSet<String>,
    _partition_columns: &[String],
) -> Vec<FileEntry> {
    entries.iter()
        .filter(|e| !removed_paths.contains(&e.add.path))
        .cloned()
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::txlog::actions::{AddAction, FileEntry};
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
                footer_start_offset: None, footer_end_offset: None, has_footer_offsets: None,
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
}

// txlog/streaming.rs - Streaming version filtering for incremental reads
//
// Matches Scala's StreamingStateReader: filters manifests and entries by
// minAddedAtVersion/maxAddedAtVersion to efficiently handle incremental
// processing without rescanning existing data.

use std::collections::HashSet;

use crate::txlog::actions::{FileEntry, ManifestInfo, StateManifest};

/// Result of a streaming changes query.
#[derive(Debug, Clone)]
pub struct ChangeSet {
    pub adds: Vec<FileEntry>,
    pub removes: Vec<String>,
    pub new_version: i64,
}

/// Filter manifests to only those that may contain entries added after `since_version`.
///
/// A manifest is relevant if its `max_added_at_version > since_version`,
/// meaning it contains at least one entry added after the requested version.
/// Manifests with `max_added_at_version == 0` are always included (version info unknown).
pub fn filter_manifests_by_version(
    manifests: &[ManifestInfo],
    since_version: i64,
) -> Vec<&ManifestInfo> {
    manifests.iter().filter(|m| {
        // Include if max version is unknown (0) or exceeds since_version
        m.max_added_at_version == 0 || m.max_added_at_version > since_version
    }).collect()
}

/// Filter manifests to only those that may contain entries at a specific version.
///
/// A manifest is relevant if `min_added_at_version <= version <= max_added_at_version`.
pub fn filter_manifests_for_version(
    manifests: &[ManifestInfo],
    version: i64,
) -> Vec<&ManifestInfo> {
    manifests.iter().filter(|m| {
        // Include if version range is unknown (0) or contains the target version
        (m.min_added_at_version == 0 && m.max_added_at_version == 0)
            || (m.min_added_at_version <= version && m.max_added_at_version >= version)
    }).collect()
}

/// Filter file entries to only those added in a specific version range.
///
/// Returns entries where `from_version < added_at_version <= to_version`,
/// excluding tombstoned paths.
pub fn filter_entries_by_version_range(
    entries: &[FileEntry],
    from_version: i64,
    to_version: i64,
    tombstones: &HashSet<String>,
) -> Vec<FileEntry> {
    entries.iter()
        .filter(|e| e.added_at_version > from_version && e.added_at_version <= to_version)
        .filter(|e| !tombstones.contains(&e.add.path))
        .cloned()
        .collect()
}

/// Filter file entries to only those added at a specific version.
pub fn filter_entries_at_version(
    entries: &[FileEntry],
    version: i64,
) -> Vec<FileEntry> {
    entries.iter()
        .filter(|e| e.added_at_version == version)
        .cloned()
        .collect()
}

/// Compute a ChangeSet between two versions from a StateManifest and its entries.
///
/// This is the core streaming query: given a manifest with all entries loaded,
/// return only the changes between `from_version` and `to_version`.
pub fn compute_changes(
    manifest: &StateManifest,
    all_entries: &[FileEntry],
    from_version: i64,
    to_version: i64,
) -> ChangeSet {
    let tombstone_set: HashSet<String> = manifest.tombstones.iter().cloned().collect();

    let adds = filter_entries_by_version_range(all_entries, from_version, to_version, &tombstone_set);

    // For removes, tombstones don't carry version info — return all tombstones
    // that reference entries present in the manifest
    let all_paths: HashSet<&str> = all_entries.iter().map(|e| e.add.path.as_str()).collect();
    let removes: Vec<String> = manifest.tombstones.iter()
        .filter(|path| all_paths.contains(path.as_str()))
        .cloned()
        .collect();

    ChangeSet {
        adds,
        removes,
        new_version: to_version,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::txlog::actions::{AddAction, ManifestInfo};
    use std::collections::HashMap;

    fn make_manifest(path: &str, min_v: i64, max_v: i64, count: i64) -> ManifestInfo {
        ManifestInfo {
            path: path.to_string(),
            file_count: count,
            partition_bounds: None,
            min_added_at_version: min_v,
            max_added_at_version: max_v,
            tombstone_count: 0,
            live_entry_count: count,
        }
    }

    fn make_entry(path: &str, version: i64) -> FileEntry {
        FileEntry {
            add: AddAction {
                path: path.to_string(),
                partition_values: HashMap::new(),
                size: 1000, modification_time: 0, data_change: true,
                stats: None, min_values: None, max_values: None, num_records: None,
                footer_start_offset: None, footer_end_offset: None, has_footer_offsets: None,
                delete_opstamp: None, split_tags: None, num_merge_ops: None,
                doc_mapping_json: None, doc_mapping_ref: None, uncompressed_size_bytes: None,
                time_range_start: None, time_range_end: None,
                companion_source_files: None, companion_delta_version: None,
                companion_fast_field_mode: None,
            },
            added_at_version: version,
            added_at_timestamp: 0,
        }
    }

    #[test]
    fn test_filter_manifests_by_version() {
        let manifests = vec![
            make_manifest("m1.avro", 1, 10, 100),
            make_manifest("m2.avro", 11, 20, 100),
            make_manifest("m3.avro", 21, 30, 100),
        ];

        // since_version=15: should include m2 (max=20>15) and m3 (max=30>15)
        let filtered = filter_manifests_by_version(&manifests, 15);
        assert_eq!(filtered.len(), 2);
        assert_eq!(filtered[0].path, "m2.avro");
        assert_eq!(filtered[1].path, "m3.avro");
    }

    #[test]
    fn test_filter_manifests_by_version_includes_unknown() {
        let manifests = vec![
            make_manifest("m1.avro", 0, 0, 100), // unknown version info
            make_manifest("m2.avro", 1, 5, 50),
        ];

        // Unknown version manifests (0,0) are always included
        let filtered = filter_manifests_by_version(&manifests, 10);
        assert_eq!(filtered.len(), 1);
        assert_eq!(filtered[0].path, "m1.avro");
    }

    #[test]
    fn test_filter_manifests_for_version() {
        let manifests = vec![
            make_manifest("m1.avro", 1, 10, 100),
            make_manifest("m2.avro", 11, 20, 100),
            make_manifest("m3.avro", 21, 30, 100),
        ];

        // version=15: only m2 contains it (11<=15<=20)
        let filtered = filter_manifests_for_version(&manifests, 15);
        assert_eq!(filtered.len(), 1);
        assert_eq!(filtered[0].path, "m2.avro");
    }

    #[test]
    fn test_filter_entries_by_version_range() {
        let entries = vec![
            make_entry("a.split", 5),
            make_entry("b.split", 10),
            make_entry("c.split", 15),
            make_entry("d.split", 20),
        ];
        let tombstones = HashSet::new();

        // from=8, to=18: should include b(10) and c(15)
        let filtered = filter_entries_by_version_range(&entries, 8, 18, &tombstones);
        assert_eq!(filtered.len(), 2);
        assert_eq!(filtered[0].add.path, "b.split");
        assert_eq!(filtered[1].add.path, "c.split");
    }

    #[test]
    fn test_filter_entries_excludes_tombstones() {
        let entries = vec![
            make_entry("a.split", 5),
            make_entry("dead.split", 10),
            make_entry("c.split", 15),
        ];
        let mut tombstones = HashSet::new();
        tombstones.insert("dead.split".to_string());

        let filtered = filter_entries_by_version_range(&entries, 0, 20, &tombstones);
        assert_eq!(filtered.len(), 2);
        assert!(!filtered.iter().any(|e| e.add.path == "dead.split"));
    }

    #[test]
    fn test_filter_entries_at_version() {
        let entries = vec![
            make_entry("a.split", 5),
            make_entry("b.split", 10),
            make_entry("c.split", 10),
            make_entry("d.split", 15),
        ];

        let filtered = filter_entries_at_version(&entries, 10);
        assert_eq!(filtered.len(), 2);
        assert_eq!(filtered[0].add.path, "b.split");
        assert_eq!(filtered[1].add.path, "c.split");
    }
}

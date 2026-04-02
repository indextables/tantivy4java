// txlog/tombstone_distributor.rs - Partition-aware tombstone distribution and selective compaction
//
// Matches Scala's TombstoneDistributor: distributes tombstones to manifests
// based on partition bounds, enabling selective compaction that only rewrites
// dirty manifests while keeping clean ones intact.

use std::collections::HashSet;

use super::actions::{FileEntry, ManifestInfo, PartitionBounds};
use super::partition_pruning::compare_values;

/// Default tombstone ratio threshold for triggering compaction.
pub const COMPACTION_TOMBSTONE_THRESHOLD: f64 = 0.10;

/// Default max manifest count before fragmentation compaction.
pub const COMPACTION_MAX_MANIFESTS: usize = 20;

/// Minimum savings ratio for selective compaction to be beneficial.
const SELECTIVE_COMPACTION_SAVINGS_RATIO: f64 = 0.10;

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

/// Distribute tombstones to manifests based on partition bounds.
///
/// For each manifest, counts how many tombstones fall within its partition bounds.
/// Returns manifests with updated `tombstone_count` and `live_entry_count`.
pub fn distribute_tombstones_to_manifests(
    manifests: &[ManifestInfo],
    tombstones: &HashSet<String>,
    partition_columns: &[String],
) -> Vec<ManifestInfo> {
    if tombstones.is_empty() {
        return manifests.iter().map(|m| {
            let mut updated = m.clone();
            updated.tombstone_count = 0;
            updated.live_entry_count = m.file_count;
            updated
        }).collect();
    }

    // Parse partition values from tombstone paths
    let tombstone_partitions: Vec<(&str, std::collections::HashMap<String, String>)> = tombstones.iter()
        .map(|path| (path.as_str(), extract_partition_values(path, partition_columns)))
        .collect();

    manifests.iter().map(|manifest| {
        let count = tombstone_partitions.iter()
            .filter(|(_, pv)| values_within_bounds(pv, &manifest.partition_bounds))
            .count();

        // Cap at file_count
        let capped = std::cmp::min(count as i64, manifest.file_count);
        let live = manifest.file_count - capped;

        let mut updated = manifest.clone();
        updated.tombstone_count = capped;
        updated.live_entry_count = live;
        updated
    }).collect()
}

/// Check if a set of partition values falls within a manifest's partition bounds.
fn values_within_bounds(
    partition_values: &std::collections::HashMap<String, String>,
    bounds: &Option<PartitionBounds>,
) -> bool {
    match bounds {
        None => partition_values.is_empty(),
        Some(bounds) if bounds.min_values.is_empty() && bounds.max_values.is_empty() => {
            partition_values.is_empty()
        }
        Some(bounds) => {
            // Check each column in bounds
            bounds.min_values.keys()
                .chain(bounds.max_values.keys())
                .collect::<HashSet<_>>()
                .iter()
                .all(|col| {
                    match partition_values.get(*col) {
                        None => true, // Conservative: could match
                        Some(value) => {
                            let within_min = bounds.min_values.get(*col)
                                .map_or(true, |min| compare_values(value, min) != std::cmp::Ordering::Less);
                            let within_max = bounds.max_values.get(*col)
                                .map_or(true, |max| compare_values(value, max) != std::cmp::Ordering::Greater);
                            within_min && within_max
                        }
                    }
                })
        }
    }
}

/// Extract partition values from a file path.
///
/// Parses paths like `date=2024-01-01/region=us-east/file.split`
/// into `{"date": "2024-01-01", "region": "us-east"}`.
fn extract_partition_values(
    path: &str,
    _partition_columns: &[String],
) -> std::collections::HashMap<String, String> {
    let mut values = std::collections::HashMap::new();
    for segment in path.split('/') {
        if let Some(eq_pos) = segment.find('=') {
            let key = &segment[..eq_pos];
            let val = &segment[eq_pos + 1..];
            if !key.is_empty() && !val.is_empty() {
                values.insert(key.to_string(), val.to_string());
            }
        }
    }
    values
}

/// Partition manifests into keep (clean) and rewrite (dirty) based on tombstone ratio.
///
/// Returns `(keep, rewrite)` where:
/// - `keep`: manifests with tombstone ratio <= threshold (reused as-is)
/// - `rewrite`: manifests with tombstone ratio > threshold (need rewriting)
pub fn selective_partition(
    manifests: &[ManifestInfo],
    threshold: f64,
) -> (Vec<ManifestInfo>, Vec<ManifestInfo>) {
    let mut keep = Vec::new();
    let mut rewrite = Vec::new();

    for m in manifests {
        if m.file_count == 0 {
            keep.push(m.clone()); // Empty manifests — nothing to compact
        } else {
            let ratio = m.tombstone_count as f64 / m.file_count as f64;
            if ratio <= threshold {
                keep.push(m.clone());
            } else {
                rewrite.push(m.clone());
            }
        }
    }

    (keep, rewrite)
}

/// Check if selective compaction is beneficial (saves at least 10% of entries).
pub fn is_selective_compaction_beneficial(
    keep: &[ManifestInfo],
    rewrite: &[ManifestInfo],
) -> bool {
    if keep.is_empty() || rewrite.is_empty() {
        return false;
    }

    let entries_to_keep: i64 = keep.iter().map(|m| m.file_count).sum();
    let entries_to_rewrite: i64 = rewrite.iter().map(|m| m.file_count).sum();
    let total = entries_to_keep + entries_to_rewrite;

    if total == 0 {
        return false;
    }

    let savings_ratio = entries_to_keep as f64 / total as f64;
    savings_ratio >= SELECTIVE_COMPACTION_SAVINGS_RATIO
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

    // ========================================================================
    // Selective compaction tests
    // ========================================================================

    fn make_manifest_info(path: &str, count: i64, bounds: Option<PartitionBounds>) -> ManifestInfo {
        ManifestInfo {
            path: path.to_string(),
            file_count: count,
            partition_bounds: bounds,
            ..Default::default()
        }
    }

    fn make_bounds(col: &str, min: &str, max: &str) -> PartitionBounds {
        let mut min_values = HashMap::new();
        min_values.insert(col.to_string(), min.to_string());
        let mut max_values = HashMap::new();
        max_values.insert(col.to_string(), max.to_string());
        PartitionBounds { min_values, max_values }
    }

    #[test]
    fn test_distribute_tombstones_to_manifests_empty() {
        let manifests = vec![make_manifest_info("m1.avro", 100, None)];
        let tombstones = HashSet::new();
        let result = distribute_tombstones_to_manifests(&manifests, &tombstones, &[]);
        assert_eq!(result[0].tombstone_count, 0);
        assert_eq!(result[0].live_entry_count, 100);
    }

    #[test]
    fn test_distribute_tombstones_with_partition_bounds() {
        let manifests = vec![
            make_manifest_info("m-2023.avro", 100,
                Some(make_bounds("year", "2023", "2023"))),
            make_manifest_info("m-2024.avro", 100,
                Some(make_bounds("year", "2024", "2024"))),
        ];
        let mut tombstones = HashSet::new();
        tombstones.insert("year=2024/file1.split".to_string());
        tombstones.insert("year=2024/file2.split".to_string());
        tombstones.insert("year=2023/file3.split".to_string());

        let result = distribute_tombstones_to_manifests(&manifests, &tombstones, &["year".to_string()]);
        assert_eq!(result[0].tombstone_count, 1); // year=2023
        assert_eq!(result[1].tombstone_count, 2); // year=2024
    }

    #[test]
    fn test_selective_partition() {
        let manifests = vec![
            ManifestInfo { tombstone_count: 0, live_entry_count: 100, file_count: 100, ..Default::default() },
            ManifestInfo { tombstone_count: 50, live_entry_count: 50, file_count: 100, ..Default::default() },
            ManifestInfo { tombstone_count: 5, live_entry_count: 95, file_count: 100, ..Default::default() },
        ];

        let (keep, rewrite) = selective_partition(&manifests, 0.10);
        assert_eq!(keep.len(), 2); // 0% and 5%
        assert_eq!(rewrite.len(), 1); // 50%
    }

    #[test]
    fn test_selective_compaction_beneficial() {
        let keep = vec![
            ManifestInfo { file_count: 1000, ..Default::default() },
        ];
        let rewrite = vec![
            ManifestInfo { file_count: 100, ..Default::default() },
        ];
        // 1000 / 1100 = 90.9% savings — beneficial
        assert!(is_selective_compaction_beneficial(&keep, &rewrite));
    }

    #[test]
    fn test_selective_compaction_not_beneficial_all_dirty() {
        let keep: Vec<ManifestInfo> = vec![];
        let rewrite = vec![ManifestInfo { file_count: 100, ..Default::default() }];
        assert!(!is_selective_compaction_beneficial(&keep, &rewrite));
    }

    #[test]
    fn test_extract_partition_values() {
        let values = extract_partition_values("year=2024/month=01/file.split", &[]);
        assert_eq!(values.get("year"), Some(&"2024".to_string()));
        assert_eq!(values.get("month"), Some(&"01".to_string()));
        assert_eq!(values.len(), 2);
    }

    #[test]
    fn test_extract_partition_values_no_partitions() {
        let values = extract_partition_values("plain/file.split", &[]);
        assert!(values.is_empty());
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

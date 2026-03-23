// txlog/log_replay.rs - Action replay to compute the live file set
//
// Latest action per path wins. Remove supersedes Add. SkipActions tracked separately.

use std::collections::HashMap;
use super::actions::{Action, AddAction, FileEntry, SkipAction};

/// Result of replaying all actions.
pub struct ReplayResult {
    pub files: Vec<FileEntry>,
    pub skips: Vec<SkipAction>,
}

/// Replay checkpoint entries + post-checkpoint actions to compute the live file set.
///
/// `post_checkpoint_actions` is a sorted list of `(version, actions)` tuples.
pub fn replay(
    checkpoint_entries: Vec<FileEntry>,
    post_checkpoint_actions: Vec<(i64, Vec<Action>)>,
) -> ReplayResult {
    let mut file_map: HashMap<String, FileEntry> = HashMap::new();
    let mut skip_map: HashMap<String, SkipAction> = HashMap::new();

    // Start with checkpoint entries
    for entry in checkpoint_entries {
        file_map.insert(entry.add.path.clone(), entry);
    }

    // Apply post-checkpoint actions in version order
    for (version, actions) in post_checkpoint_actions {
        for action in actions {
            match action {
                Action::Add(add) => {
                    // Use the file's own modification_time as the added_at_timestamp
                    let timestamp = add.modification_time;
                    let entry = FileEntry {
                        add,
                        added_at_version: version,
                        added_at_timestamp: timestamp,
                    };
                    file_map.insert(entry.add.path.clone(), entry);
                }
                Action::Remove(remove) => {
                    file_map.remove(&remove.path);
                }
                Action::MergeSkip(skip) => {
                    skip_map.insert(skip.path.clone(), skip);
                }
                Action::Protocol(_) | Action::MetaData(_) => {
                    // Protocol/Metadata tracked separately by caller
                }
            }
        }
    }

    // Sort by path for deterministic output (HashMap iteration order is arbitrary)
    let mut files: Vec<FileEntry> = file_map.into_values().collect();
    files.sort_by(|a, b| a.add.path.cmp(&b.add.path));
    let mut skips: Vec<SkipAction> = skip_map.into_values().collect();
    skips.sort_by(|a, b| a.path.cmp(&b.path));

    ReplayResult { files, skips }
}

/// Extract the latest Protocol and Metadata actions from a list of actions.
pub fn extract_metadata(
    checkpoint_actions: &[Action],
    post_checkpoint_actions: &[(i64, Vec<Action>)],
) -> (Option<super::actions::ProtocolAction>, Option<super::actions::MetadataAction>) {
    let mut protocol = None;
    let mut metadata = None;

    // Check checkpoint
    for action in checkpoint_actions {
        match action {
            Action::Protocol(p) => protocol = Some(p.clone()),
            Action::MetaData(m) => metadata = Some(m.clone()),
            _ => {}
        }
    }

    // Post-checkpoint overrides
    for (_version, actions) in post_checkpoint_actions {
        for action in actions {
            match action {
                Action::Protocol(p) => protocol = Some(p.clone()),
                Action::MetaData(m) => metadata = Some(m.clone()),
                _ => {}
            }
        }
    }

    (protocol, metadata)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::txlog::actions::*;

    fn make_file_entry(path: &str, version: i64) -> FileEntry {
        FileEntry {
            add: AddAction {
                path: path.to_string(),
                partition_values: HashMap::new(),
                size: 100,
                modification_time: 0,
                data_change: true,
                stats: None, min_values: None, max_values: None,
                num_records: None,
                footer_start_offset: None, footer_end_offset: None, has_footer_offsets: None, delete_opstamp: None,
                split_tags: None, num_merge_ops: None,
                doc_mapping_json: None, doc_mapping_ref: None,
                uncompressed_size_bytes: None,
                time_range_start: None, time_range_end: None,
                companion_source_files: None, companion_delta_version: None,
                companion_fast_field_mode: None,
            },
            added_at_version: version,
            added_at_timestamp: 0,
        }
    }

    #[test]
    fn test_replay_empty() {
        let result = replay(vec![], vec![]);
        assert!(result.files.is_empty());
        assert!(result.skips.is_empty());
    }

    #[test]
    fn test_replay_checkpoint_only() {
        let entries = vec![
            make_file_entry("a.split", 1),
            make_file_entry("b.split", 2),
        ];
        let result = replay(entries, vec![]);
        assert_eq!(result.files.len(), 2);
    }

    #[test]
    fn test_replay_remove_supersedes_add() {
        let entries = vec![
            make_file_entry("a.split", 1),
            make_file_entry("b.split", 1),
        ];
        let post = vec![
            (2, vec![Action::Remove(RemoveAction {
                path: "a.split".to_string(),
                deletion_timestamp: Some(1700000000000),
                data_change: true,
                partition_values: None,
                size: None,
            })]),
        ];
        let result = replay(entries, post);
        assert_eq!(result.files.len(), 1);
        assert_eq!(result.files[0].add.path, "b.split");
    }

    #[test]
    fn test_replay_skip_tracked() {
        let post = vec![
            (1, vec![
                Action::Add(AddAction {
                    path: "a.split".to_string(),
                    partition_values: HashMap::new(),
                    size: 100, modification_time: 0, data_change: true,
                    stats: None, min_values: None, max_values: None, num_records: None,
                    footer_start_offset: None, footer_end_offset: None, has_footer_offsets: None, delete_opstamp: None,
                    split_tags: None, num_merge_ops: None,
                    doc_mapping_json: None, doc_mapping_ref: None,
                    uncompressed_size_bytes: None,
                    time_range_start: None, time_range_end: None,
                    companion_source_files: None, companion_delta_version: None,
                    companion_fast_field_mode: None,
                }),
                Action::MergeSkip(SkipAction {
                    path: "bad.split".to_string(),
                    skip_timestamp: 0,
                    reason: "merge".to_string(),
                    operation: None, partition_values: None, size: None,
                    retry_after: None, skip_count: Some(1),
                }),
            ]),
        ];
        let result = replay(vec![], post);
        assert_eq!(result.files.len(), 1);
        assert_eq!(result.skips.len(), 1);
        assert_eq!(result.skips[0].reason, "merge");
    }

    #[test]
    fn test_replay_latest_add_wins() {
        let entries = vec![make_file_entry("a.split", 1)];
        let mut new_add = make_file_entry("a.split", 3).add;
        new_add.size = 999;
        let post = vec![
            (3, vec![Action::Add(new_add)]),
        ];
        let result = replay(entries, post);
        assert_eq!(result.files.len(), 1);
        assert_eq!(result.files[0].add.size, 999);
        assert_eq!(result.files[0].added_at_version, 3);
    }
}

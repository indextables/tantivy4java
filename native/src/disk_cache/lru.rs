// lru.rs - LRU tracking for split eviction
// Extracted from mod.rs during refactoring

use std::collections::HashMap;
use std::time::{SystemTime, UNIX_EPOCH};

fn now_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

/// Per-split LRU bookkeeping.
struct LruEntry {
    size_bytes: u64,
    /// Wall-clock seconds of the last access (used as the primary LRU key).
    last_accessed: u64,
    /// Monotonic tiebreaker so accesses within the same second still order
    /// deterministically (seconds granularity alone would be arbitrary).
    seq: u64,
}

/// LRU table for tracking split access patterns.
///
/// Keyed by split_key (`storage_loc/split_id`) for O(1) touch/remove on the hot
/// read path. Eviction candidates are produced by sorting on demand, which only
/// happens when the cache is over its high-water mark.
pub(crate) struct SplitLruTable {
    entries: HashMap<String, LruEntry>,
    next_seq: u64,
}

impl SplitLruTable {
    pub fn new() -> Self {
        Self {
            entries: HashMap::new(),
            next_seq: 0,
        }
    }

    fn bump_seq(&mut self) -> u64 {
        let seq = self.next_seq;
        self.next_seq += 1;
        seq
    }

    /// Record an access to a split, updating its recency and size. O(1).
    pub fn touch(&mut self, key: &str, size_bytes: u64) {
        let now = now_secs();
        let seq = self.bump_seq();
        match self.entries.get_mut(key) {
            Some(entry) => {
                entry.last_accessed = now;
                entry.size_bytes = size_bytes;
                entry.seq = seq;
            }
            None => {
                self.entries.insert(
                    key.to_string(),
                    LruEntry {
                        size_bytes,
                        last_accessed: now,
                        seq,
                    },
                );
            }
        }
    }

    /// Seed an entry restored from the persisted manifest at startup, preserving
    /// its recorded `last_accessed` timestamp so cold splits from prior runs remain
    /// visible to eviction. Never overwrites a live entry.
    pub fn seed(&mut self, key: &str, size_bytes: u64, last_accessed: u64) {
        if self.entries.contains_key(key) {
            return;
        }
        let seq = self.bump_seq();
        self.entries.insert(
            key.to_string(),
            LruEntry {
                size_bytes,
                last_accessed,
                seq,
            },
        );
    }

    pub fn remove(&mut self, key: &str) {
        self.entries.remove(key);
    }

    /// Get splits to evict to reach target size, ordered by LRU (oldest first).
    pub fn get_eviction_candidates(&self, current_bytes: u64, target_bytes: u64) -> Vec<String> {
        if current_bytes <= target_bytes {
            return Vec::new();
        }

        // Sort by (last_accessed, seq) so oldest — and, within a second, least
        // recently touched — come first.
        let mut sorted: Vec<(&String, &LruEntry)> = self.entries.iter().collect();
        sorted.sort_by_key(|(_, e)| (e.last_accessed, e.seq));

        let mut to_evict = Vec::new();
        let mut freed = 0u64;
        let need_to_free = current_bytes - target_bytes;

        for (key, entry) in sorted {
            if freed >= need_to_free {
                break;
            }
            to_evict.push(key.clone());
            freed += entry.size_bytes;
        }

        to_evict
    }
}

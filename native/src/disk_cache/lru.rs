// lru.rs - LRU tracking for split eviction
// Extracted from mod.rs during refactoring

use std::time::{SystemTime, UNIX_EPOCH};

/// LRU tracking entry for a split
pub(crate) struct SplitLruEntry {
    pub key: String,
    pub size_bytes: u64,
    pub last_accessed: u64,
}

/// LRU table for tracking split access patterns
pub(crate) struct SplitLruTable {
    entries: Vec<SplitLruEntry>,
}

impl SplitLruTable {
    pub fn new() -> Self {
        Self { entries: Vec::new() }
    }

    pub fn touch(&mut self, key: &str, size_bytes: u64) {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        if let Some(entry) = self.entries.iter_mut().find(|e| e.key == key) {
            entry.last_accessed = now;
            entry.size_bytes = size_bytes;
        } else {
            self.entries.push(SplitLruEntry {
                key: key.to_string(),
                size_bytes,
                last_accessed: now,
            });
        }
    }

    pub fn remove(&mut self, key: &str) {
        self.entries.retain(|e| e.key != key);
    }

    /// Get splits to evict to reach target size, ordered by LRU
    pub fn get_eviction_candidates(&self, current_bytes: u64, target_bytes: u64) -> Vec<String> {
        if current_bytes <= target_bytes {
            return Vec::new();
        }

        // Sort by last_accessed (oldest first)
        let mut sorted: Vec<_> = self.entries.iter().collect();
        sorted.sort_by_key(|e| e.last_accessed);

        let mut to_evict = Vec::new();
        let mut freed = 0u64;
        let need_to_free = current_bytes - target_bytes;

        for entry in sorted {
            if freed >= need_to_free {
                break;
            }
            to_evict.push(entry.key.clone());
            freed += entry.size_bytes;
        }

        to_evict
    }
}

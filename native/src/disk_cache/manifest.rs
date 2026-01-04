// manifest.rs - Cache manifest and split state management
// Extracted from mod.rs during refactoring

use std::collections::HashMap;
use std::ops::Range;

use serde::{Deserialize, Serialize};

use super::types::ComponentEntry;
use super::range_index::{CachedRange, RangeIndex};

/// Metadata for a cached split
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SplitEntry {
    /// Split ID
    pub split_id: String,
    /// Storage location identifier
    pub storage_loc: String,
    /// Total size of all components
    pub total_size_bytes: u64,
    /// Last access timestamp (unix epoch seconds)
    pub last_accessed: u64,
    /// Components in this split
    pub components: HashMap<String, ComponentEntry>,
}

/// In-memory state for a split (includes range index for fast coalescing)
#[derive(Debug)]
pub struct SplitState {
    /// Range indices per component (component name -> RangeIndex)
    pub(crate) range_indices: HashMap<String, RangeIndex>,
}

impl SplitState {
    pub fn new() -> Self {
        Self {
            range_indices: HashMap::new(),
        }
    }

    /// Rebuild from manifest entry
    pub fn from_entry(entry: &SplitEntry) -> Self {
        let mut state = Self::new();

        // Group components by their base component name
        for (key, comp_entry) in &entry.components {
            if comp_entry.byte_range.is_some() {
                let index = state.range_indices
                    .entry(comp_entry.component.clone())
                    .or_insert_with(RangeIndex::new);

                if let Some((start, end)) = comp_entry.byte_range {
                    index.insert(CachedRange {
                        start,
                        end,
                        cache_key: key.clone(),
                        compression: comp_entry.compression,
                    });
                }
            }
        }

        state
    }

    /// Get or create range index for a component
    pub fn get_or_create_index(&mut self, component: &str) -> &mut RangeIndex {
        self.range_indices
            .entry(component.to_string())
            .or_insert_with(RangeIndex::new)
    }
}

/// Cache manifest - persisted to disk
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct CacheManifest {
    /// Version for future compatibility
    pub version: u32,
    /// Total bytes used by cache
    pub total_bytes: u64,
    /// Splits by storage_loc/split_id key
    pub splits: HashMap<String, SplitEntry>,
    /// Last sync timestamp
    pub last_sync: u64,
}

impl CacheManifest {
    pub const VERSION: u32 = 1;

    pub fn new() -> Self {
        Self {
            version: Self::VERSION,
            ..Default::default()
        }
    }

    /// Generate key for a split
    pub fn split_key(storage_loc: &str, split_id: &str) -> String {
        format!("{}/{}", storage_loc, split_id)
    }

    /// Generate key for a component
    pub fn component_key(component: &str, byte_range: Option<Range<u64>>) -> String {
        match byte_range {
            Some(range) => format!("{}_{}-{}", component, range.start, range.end),
            None => format!("{}_full", component),
        }
    }
}

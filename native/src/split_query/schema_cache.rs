// schema_cache.rs - Global schema caches and mapping functions
// Extracted from split_query.rs during refactoring

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use jni::sys::jlong;
use once_cell::sync::Lazy;

use crate::debug_println;

// Global cache mapping split URI to schema for parseQuery field extraction
pub static SPLIT_SCHEMA_CACHE: Lazy<Arc<Mutex<HashMap<String, tantivy::schema::Schema>>>> =
    Lazy::new(|| Arc::new(Mutex::new(HashMap::new())));

// Direct mapping from searcher pointer to schema pointer for reliable schema access
pub static SEARCHER_SCHEMA_MAPPING: Lazy<Arc<Mutex<HashMap<jlong, jlong>>>> =
    Lazy::new(|| Arc::new(Mutex::new(HashMap::new())));

/// Store schema clone for a split URI
pub fn store_split_schema(split_uri: &str, schema: tantivy::schema::Schema) {
    debug_println!(
        "RUST DEBUG: *** STORE_SPLIT_SCHEMA CALLED WITH URI: {}",
        split_uri
    );
    debug_println!(
        "RUST DEBUG: Storing schema clone in cache for split: {}",
        split_uri
    );
    let mut cache = SPLIT_SCHEMA_CACHE.lock().unwrap();
    cache.insert(split_uri.to_string(), schema);
    debug_println!(
        "RUST DEBUG: Schema cache now contains {} entries",
        cache.len()
    );
    debug_println!("RUST DEBUG: *** STORE_SPLIT_SCHEMA COMPLETED");
}

/// Retrieve schema clone for a split URI
pub fn get_split_schema(split_uri: &str) -> Option<tantivy::schema::Schema> {
    let cache = SPLIT_SCHEMA_CACHE.lock().unwrap();
    if let Some(schema) = cache.get(split_uri) {
        debug_println!(
            "RUST DEBUG: ✅ Retrieved schema from cache for split: {}",
            split_uri
        );
        Some(schema.clone())
    } else {
        debug_println!(
            "RUST DEBUG: ❌ Schema not found in cache for split: {}",
            split_uri
        );
        debug_println!(
            "RUST DEBUG: Available cache entries: {:?}",
            cache.keys().collect::<Vec<_>>()
        );
        None
    }
}

/// Store direct mapping from searcher pointer to schema pointer
pub fn store_searcher_schema(searcher_ptr: jlong, schema_ptr: jlong) {
    debug_println!(
        "RUST DEBUG: Storing searcher->schema mapping: {} -> {}",
        searcher_ptr,
        schema_ptr
    );
    let mut mapping = SEARCHER_SCHEMA_MAPPING.lock().unwrap();
    mapping.insert(searcher_ptr, schema_ptr);
    debug_println!(
        "RUST DEBUG: Searcher schema mapping now contains {} entries",
        mapping.len()
    );
}

/// Retrieve schema pointer for a searcher pointer
pub fn get_searcher_schema(searcher_ptr: jlong) -> Option<jlong> {
    let mapping = SEARCHER_SCHEMA_MAPPING.lock().unwrap();
    if let Some(&schema_ptr) = mapping.get(&searcher_ptr) {
        debug_println!(
            "RUST DEBUG: ✅ Found schema pointer {} for searcher {}",
            schema_ptr,
            searcher_ptr
        );
        Some(schema_ptr)
    } else {
        debug_println!(
            "RUST DEBUG: ❌ No schema mapping found for searcher {}",
            searcher_ptr
        );
        None
    }
}

/// Remove schema mapping when searcher is closed
pub fn remove_searcher_schema(searcher_ptr: jlong) -> bool {
    let mut mapping = SEARCHER_SCHEMA_MAPPING.lock().unwrap();
    if let Some(schema_ptr) = mapping.remove(&searcher_ptr) {
        debug_println!(
            "RUST DEBUG: ✅ Removed schema mapping: {} -> {}",
            searcher_ptr,
            schema_ptr
        );
        // Also release the schema Arc to prevent memory leaks
        crate::utils::release_arc(schema_ptr);
        debug_println!("RUST DEBUG: ✅ Released schema Arc: {}", schema_ptr);
        true
    } else {
        debug_println!(
            "RUST DEBUG: ❌ No schema mapping found to remove for searcher {}",
            searcher_ptr
        );
        false
    }
}

/// Clear all cached schemas - called when last cache manager is closed
/// This is critical for test isolation to prevent schema data from leaking between tests
pub fn clear_split_schema_cache() {
    debug_println!("RUST DEBUG: 🧹 CLEAR_SPLIT_SCHEMA_CACHE: Clearing all cached schemas");

    // Clear split schema cache
    {
        let mut cache = SPLIT_SCHEMA_CACHE.lock().unwrap();
        let count = cache.len();
        cache.clear();
        debug_println!("RUST DEBUG: 🧹 Cleared {} entries from SPLIT_SCHEMA_CACHE", count);
    }

    // Clear searcher schema mapping
    {
        let mut mapping = SEARCHER_SCHEMA_MAPPING.lock().unwrap();
        let count = mapping.len();
        // Release all schema Arcs before clearing
        for (_searcher_ptr, schema_ptr) in mapping.iter() {
            crate::utils::release_arc(*schema_ptr);
        }
        mapping.clear();
        debug_println!("RUST DEBUG: 🧹 Cleared {} entries from SEARCHER_SCHEMA_MAPPING", count);
    }

    debug_println!("RUST DEBUG: 🧹 CLEAR_SPLIT_SCHEMA_CACHE: Complete");
}

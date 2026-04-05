// schema_cache.rs - Global schema caches and mapping functions
// Extracted from split_query.rs during refactoring

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use once_cell::sync::Lazy;

use crate::debug_println;

// Global cache mapping split URI to schema for parseQuery field extraction
pub static SPLIT_SCHEMA_CACHE: Lazy<Arc<Mutex<HashMap<String, tantivy::schema::Schema>>>> =
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

/// Clear all cached schemas - called when last cache manager is closed
/// This is critical for test isolation to prevent schema data from leaking between tests
pub fn clear_split_schema_cache() {
    debug_println!("RUST DEBUG: 🧹 CLEAR_SPLIT_SCHEMA_CACHE: Clearing all cached schemas");

    let mut cache = SPLIT_SCHEMA_CACHE.lock().unwrap();
    let count = cache.len();
    cache.clear();
    debug_println!("RUST DEBUG: 🧹 Cleared {} entries from SPLIT_SCHEMA_CACHE", count);
    debug_println!("RUST DEBUG: 🧹 CLEAR_SPLIT_SCHEMA_CACHE: Complete");
}

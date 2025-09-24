// cache_debug.rs - Enhanced cache debugging and monitoring utilities

use std::sync::Arc;
use quickwit_storage::{MemorySizedCache, STORAGE_METRICS};
use crate::debug_println;

/// Cache statistics for debug output
pub struct CacheStats {
    pub current_items: u64,
    pub max_items: u64,
    pub hit_rate_percent: u64,
    pub total_hits: u64,
    pub total_misses: u64,
    pub current_bytes: u64,
    pub max_bytes: u64,
}

/// Get comprehensive cache statistics from Quickwit metrics
pub fn get_footer_cache_stats() -> CacheStats {
    let metrics = &STORAGE_METRICS.split_footer_cache;

    let hits = metrics.hits_num_items.get();
    let misses = metrics.misses_num_items.get();
    let total_requests = hits + misses;
    let hit_rate = if total_requests > 0 {
        (hits * 100) / total_requests
    } else {
        0
    };

    CacheStats {
        current_items: metrics.in_cache_count.get() as u64,
        max_items: 0, // MemorySizedCache doesn't expose max items directly
        hit_rate_percent: hit_rate,
        total_hits: hits,
        total_misses: misses,
        current_bytes: metrics.in_cache_num_bytes.get() as u64,
        max_bytes: 0, // Would need to track this separately
    }
}

/// Enhanced cache operation logging
pub fn debug_cache_operation(operation: &str, split_id: &str, cache_name: &str) {
    if std::env::var("TANTIVY4JAVA_DEBUG").map(|v| v == "1" || v.to_lowercase() == "true").unwrap_or(false) {
        let stats = get_footer_cache_stats();
        debug_println!("🔍 CACHE DEBUG: {} for split '{}' in cache '{}' - Items: {}, Bytes: {} KB, Hit Rate: {}% ({}/{} total requests)",
                     operation, split_id, cache_name,
                     stats.current_items,
                     stats.current_bytes / 1024,
                     stats.hit_rate_percent,
                     stats.total_hits,
                     stats.total_hits + stats.total_misses);
    }
}

/// Verify cache instance identity across calls
pub fn debug_cache_identity<T: std::hash::Hash + Eq>(cache: &MemorySizedCache<T>, cache_name: &str) {
    if std::env::var("TANTIVY4JAVA_DEBUG").map(|v| v == "1" || v.to_lowercase() == "true").unwrap_or(false) {
        let cache_ptr = cache as *const _ as usize;
        debug_println!("🔍 CACHE IDENTITY: Using {} instance at address: 0x{:x}", cache_name, cache_ptr);
    }
}

/// Verify Arc cache identity for shared cache instances
pub fn debug_arc_cache_identity<T: std::hash::Hash + Eq>(cache: &Arc<MemorySizedCache<T>>, cache_name: &str) {
    if std::env::var("TANTIVY4JAVA_DEBUG").map(|v| v == "1" || v.to_lowercase() == "true").unwrap_or(false) {
        let cache_ptr = Arc::as_ptr(cache) as usize;
        let arc_count = Arc::strong_count(cache);
        debug_println!("🔍 CACHE IDENTITY: Using shared {} Arc at address: 0x{:x}, ref_count: {}",
                     cache_name, cache_ptr, arc_count);
    }
}

/// Specific version for String cache which is our main use case
pub fn debug_string_cache_identity(cache: &MemorySizedCache<String>, cache_name: &str) {
    if std::env::var("TANTIVY4JAVA_DEBUG").map(|v| v == "1" || v.to_lowercase() == "true").unwrap_or(false) {
        let cache_ptr = cache as *const _ as usize;
        debug_println!("🔍 CACHE IDENTITY: Using {} instance at address: 0x{:x}", cache_name, cache_ptr);
    }
}

/// Specific version for Arc<MemorySizedCache<String>> which is our main use case
pub fn debug_arc_string_cache_identity(cache: &Arc<MemorySizedCache<String>>, cache_name: &str) {
    if std::env::var("TANTIVY4JAVA_DEBUG").map(|v| v == "1" || v.to_lowercase() == "true").unwrap_or(false) {
        let cache_ptr = Arc::as_ptr(cache) as usize;
        let arc_count = Arc::strong_count(cache);
        debug_println!("🔍 CACHE IDENTITY: Using shared {} Arc at address: 0x{:x}, ref_count: {}",
                     cache_name, cache_ptr, arc_count);
    }
}

/// Debug cache hit/miss with detailed context
pub fn debug_cache_lookup(operation: &str, split_id: &str, found: bool) {
    if std::env::var("TANTIVY4JAVA_DEBUG").map(|v| v == "1" || v.to_lowercase() == "true").unwrap_or(false) {
        let status = if found { "✅ HIT" } else { "❌ MISS" };
        debug_println!("🔍 CACHE LOOKUP: {} - {} for split '{}'", status, operation, split_id);

        if !found {
            // Additional debug info for cache misses
            let stats = get_footer_cache_stats();
            debug_println!("🔍 CACHE STATE: {} items in cache, {} KB total",
                         stats.current_items, stats.current_bytes / 1024);
        }
    }
}

/// Debug cache operation timing
pub struct CacheTimer {
    operation: String,
    split_id: String,
    start: std::time::Instant,
}

impl CacheTimer {
    pub fn new(operation: &str, split_id: &str) -> Self {
        debug_println!("⏱️ CACHE TIMER: Starting {} for split '{}'", operation, split_id);
        Self {
            operation: operation.to_string(),
            split_id: split_id.to_string(),
            start: std::time::Instant::now(),
        }
    }
}

impl Drop for CacheTimer {
    fn drop(&mut self) {
        if std::env::var("TANTIVY4JAVA_DEBUG").map(|v| v == "1" || v.to_lowercase() == "true").unwrap_or(false) {
            let duration = self.start.elapsed();
            debug_println!("⏱️ CACHE TIMER: {} for split '{}' completed in {:?}",
                         self.operation, self.split_id, duration);
        }
    }
}

/// Summary of all cache statistics for periodic reporting
pub fn debug_cache_summary() {
    if std::env::var("TANTIVY4JAVA_DEBUG").map(|v| v == "1" || v.to_lowercase() == "true").unwrap_or(false) {
        let footer_stats = get_footer_cache_stats();

        debug_println!("📊 CACHE SUMMARY:");
        debug_println!("  Footer Cache: {} items, {} KB, {}% hit rate",
                     footer_stats.current_items,
                     footer_stats.current_bytes / 1024,
                     footer_stats.hit_rate_percent);

        // Byte range cache stats
        let byte_range = &STORAGE_METRICS.shortlived_cache;
        let br_hits = byte_range.hits_num_items.get();
        let br_misses = byte_range.misses_num_items.get();
        let br_total = br_hits + br_misses;
        let br_hit_rate = if br_total > 0 { (br_hits * 100) / br_total } else { 0 };

        debug_println!("  Byte Range Cache: {} items, {} KB, {}% hit rate",
                     byte_range.in_cache_count.get(),
                     byte_range.in_cache_num_bytes.get() / 1024,
                     br_hit_rate);

        // Fast field cache stats
        let ff_cache = &STORAGE_METRICS.fast_field_cache;
        let ff_hits = ff_cache.hits_num_items.get();
        let ff_misses = ff_cache.misses_num_items.get();
        let ff_total = ff_hits + ff_misses;
        let ff_hit_rate = if ff_total > 0 { (ff_hits * 100) / ff_total } else { 0 };

        debug_println!("  Fast Field Cache: {} items, {} KB, {}% hit rate",
                     ff_cache.in_cache_count.get(),
                     ff_cache.in_cache_num_bytes.get() / 1024,
                     ff_hit_rate);
    }
}
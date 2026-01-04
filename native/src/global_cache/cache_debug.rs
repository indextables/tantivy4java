// cache_debug.rs - Enhanced cache debugging and monitoring utilities

use std::sync::Arc;
use quickwit_storage::{MemorySizedCache, STORAGE_METRICS};
use crate::debug_println;

/// Cache statistics for debug output
pub struct CacheStats {
    pub current_items: u64,
    pub hit_rate_percent: u64,
    pub current_bytes: u64,
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
        hit_rate_percent: hit_rate,
        current_bytes: metrics.in_cache_num_bytes.get() as u64,
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

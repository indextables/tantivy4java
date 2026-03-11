// memory_pool/mod.rs - Unified memory management for JVM-coordinated native allocations
//
// This module provides a memory pool that coordinates Rust-side memory allocations
// with an external JVM memory manager (e.g., Spark's TaskMemoryManager), following
// the pattern established by DataFusion Comet's CometUnifiedMemoryPool.
//
// Key design:
// - MemoryPool trait with try_acquire/release + category tracking
// - JvmMemoryPool uses high/low watermark batching to minimize JNI round-trips
// - UnlimitedMemoryPool for backward compatibility (no JVM coordination)
// - MemoryReservation as RAII guard for automatic cleanup
// - Global pool: lazy default (UnlimitedMemoryPool) that can be replaced once
//   by set_global_pool before or after first use

mod pool;
mod reservation;
mod jvm_pool;
mod jni_bridge;
mod disk_cache_budget;

pub use pool::{MemoryPool, UnlimitedMemoryPool, MemoryError};
#[cfg(test)]
pub use pool::LimitedMemoryPool;
pub use reservation::MemoryReservation;
pub use jvm_pool::JvmMemoryPool;
pub use jni_bridge::*;
pub use disk_cache_budget::DiskCacheMemoryBudget;

use std::sync::{Arc, RwLock};
use std::sync::atomic::{AtomicBool, Ordering};

use once_cell::sync::Lazy;

/// Global memory pool state.
/// - Starts as UnlimitedMemoryPool (lazy default).
/// - Can be replaced exactly once via set_global_pool() (before or after first use).
/// - After explicit set, further set calls are rejected.
static GLOBAL_MEMORY_POOL: Lazy<RwLock<Arc<dyn MemoryPool>>> =
    Lazy::new(|| RwLock::new(Arc::new(UnlimitedMemoryPool::default())));

/// Tracks whether set_global_pool has been called (prevents double-set).
static EXPLICITLY_CONFIGURED: AtomicBool = AtomicBool::new(false);

/// Get the global memory pool.
pub fn global_pool() -> Arc<dyn MemoryPool> {
    GLOBAL_MEMORY_POOL.read().unwrap_or_else(|e| e.into_inner()).clone()
}

/// Set the global memory pool. Can be called once to replace the default.
/// Returns Err if already explicitly set via a prior call to set_global_pool.
pub fn set_global_pool(pool: Arc<dyn MemoryPool>) -> Result<(), Arc<dyn MemoryPool>> {
    // Atomically check-and-set the configured flag
    if EXPLICITLY_CONFIGURED.compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst).is_err() {
        return Err(pool);
    }
    *GLOBAL_MEMORY_POOL.write().unwrap() = pool;
    Ok(())
}

/// Check if a custom (non-default) memory pool has been explicitly configured.
pub fn is_pool_configured() -> bool {
    EXPLICITLY_CONFIGURED.load(Ordering::SeqCst)
}

/// Global search arena reservation.
///
/// Instead of reserving 16MB per SplitSearcher (which could mean 16GB for 1000
/// cached searchers), we reserve `max_concurrency × 16MB` once. This correctly
/// reflects that only `max_concurrency` searchers execute simultaneously, even
/// if many more are cached.
///
/// Initialized lazily on first searcher creation. Fails fast if the pool denies.
static SEARCH_ARENA_RESERVATION: std::sync::OnceLock<std::sync::Mutex<Option<MemoryReservation>>> =
    std::sync::OnceLock::new();

/// Size of each search arena slot (16MB).
pub const SEARCH_ARENA_SLOT_SIZE: usize = 16 * 1024 * 1024;

/// Initialize the global search arena reservation if not already done.
/// Reserves `max_concurrency × 16MB` from the pool.
/// Returns Err if the pool denies the reservation.
pub fn init_search_arena() -> Result<(), MemoryError> {
    let holder = SEARCH_ARENA_RESERVATION.get_or_init(|| std::sync::Mutex::new(None));
    let mut guard = holder.lock().unwrap();
    if guard.is_some() {
        return Ok(()); // Already initialized
    }
    let max_threads = crate::split_searcher::cache_config::get_max_java_threads();
    let total_arena = max_threads * SEARCH_ARENA_SLOT_SIZE;
    let reservation = MemoryReservation::try_new(
        &global_pool(),
        total_arena,
        "search_results",
    )?;
    crate::debug_println!(
        "📊 SEARCH_ARENA: Reserved {} MB for {} concurrent search slots ({} MB each)",
        total_arena / 1024 / 1024, max_threads, SEARCH_ARENA_SLOT_SIZE / 1024 / 1024
    );
    *guard = Some(reservation);
    Ok(())
}

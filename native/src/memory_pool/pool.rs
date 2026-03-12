// memory_pool/pool.rs - Core MemoryPool trait and UnlimitedMemoryPool implementation

use std::collections::HashMap;
use std::fmt;
use std::sync::atomic::{AtomicUsize, Ordering::Relaxed};
use std::sync::Mutex;

/// Error type for memory pool operations.
#[derive(Debug, Clone)]
pub enum MemoryError {
    /// Memory request was denied by the external manager.
    Denied {
        requested: usize,
        available: usize,
        category: String,
    },
    /// JNI call to the memory manager failed.
    JniError(String),
}

impl fmt::Display for MemoryError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            MemoryError::Denied { requested, available, category } => {
                write!(
                    f,
                    "Memory request denied: requested {} bytes for '{}', available {} bytes",
                    requested, category, available
                )
            }
            MemoryError::JniError(msg) => write!(f, "JNI memory pool error: {}", msg),
        }
    }
}

impl std::error::Error for MemoryError {}

/// Trait for memory pools that coordinate native memory allocations.
///
/// Implementations must be thread-safe (Send + Sync).
pub trait MemoryPool: Send + Sync + fmt::Debug {
    /// Try to acquire `size` bytes in the given category.
    /// Returns Ok(()) if granted, Err(MemoryError) if denied.
    fn try_acquire(&self, size: usize, category: &'static str) -> Result<(), MemoryError>;

    /// Release `size` bytes back to the pool in the given category.
    fn release(&self, size: usize, category: &'static str);

    /// Current total memory held by this pool across all categories.
    fn used(&self) -> usize;

    /// Peak memory usage observed since creation or last reset.
    fn peak(&self) -> usize;

    /// Reset peak usage counter to current usage. Returns the old peak value.
    fn reset_peak(&self) -> usize;

    /// Total bytes granted by the external manager (for JVM pools) or usize::MAX (for unlimited).
    fn granted(&self) -> usize;

    /// Per-category memory breakdown.
    fn category_breakdown(&self) -> HashMap<String, usize>;

    /// Per-category peak memory breakdown. Returns the maximum bytes each category
    /// has held since creation or last reset, even if currently zero.
    fn category_peak_breakdown(&self) -> HashMap<String, usize>;

    /// Signal that the JVM is shutting down. Subsequent release() calls skip
    /// JNI callbacks to avoid calling releaseMemory() outside of task context.
    /// Default implementation is a no-op (for non-JVM pools).
    fn shutdown(&self) {}
}

/// Per-category tracking with current and peak usage.
#[derive(Debug)]
pub(super) struct CategoryTracker {
    pub(super) current: AtomicUsize,
    pub(super) peak: AtomicUsize,
}

impl CategoryTracker {
    pub(super) fn new() -> Self {
        Self {
            current: AtomicUsize::new(0),
            peak: AtomicUsize::new(0),
        }
    }
}

/// An unlimited memory pool that always grants requests.
/// Used as the default when no external memory manager is configured.
/// Provides local tracking for statistics without any JNI overhead.
#[derive(Debug)]
pub struct UnlimitedMemoryPool {
    used: AtomicUsize,
    peak: AtomicUsize,
    categories: Mutex<HashMap<&'static str, CategoryTracker>>,
}

impl Default for UnlimitedMemoryPool {
    fn default() -> Self {
        Self {
            used: AtomicUsize::new(0),
            peak: AtomicUsize::new(0),
            categories: Mutex::new(HashMap::new()),
        }
    }
}

impl UnlimitedMemoryPool {
    fn update_category(&self, category: &'static str, delta: isize) {
        let mut cats = self.categories.lock().unwrap();
        let tracker = cats
            .entry(category)
            .or_insert_with(CategoryTracker::new);
        if delta > 0 {
            let new_val = tracker.current.fetch_add(delta as usize, Relaxed) + delta as usize;
            // Update per-category peak
            let mut old_peak = tracker.peak.load(Relaxed);
            while new_val > old_peak {
                match tracker.peak.compare_exchange_weak(old_peak, new_val, Relaxed, Relaxed) {
                    Ok(_) => break,
                    Err(actual) => old_peak = actual,
                }
            }
        } else {
            tracker.current.fetch_sub((-delta) as usize, Relaxed);
        }
    }

    fn update_peak(&self) {
        let current = self.used.load(Relaxed);
        let mut old_peak = self.peak.load(Relaxed);
        while current > old_peak {
            match self.peak.compare_exchange_weak(old_peak, current, Relaxed, Relaxed) {
                Ok(_) => break,
                Err(actual) => old_peak = actual,
            }
        }
    }
}

impl MemoryPool for UnlimitedMemoryPool {
    fn try_acquire(&self, size: usize, category: &'static str) -> Result<(), MemoryError> {
        self.used.fetch_add(size, Relaxed);
        self.update_category(category, size as isize);
        self.update_peak();
        Ok(())
    }

    fn release(&self, size: usize, category: &'static str) {
        self.used.fetch_sub(size, Relaxed);
        self.update_category(category, -(size as isize));
    }

    fn used(&self) -> usize {
        self.used.load(Relaxed)
    }

    fn peak(&self) -> usize {
        self.peak.load(Relaxed)
    }

    fn reset_peak(&self) -> usize {
        let current = self.used.load(Relaxed);
        self.peak.swap(current, Relaxed)
    }

    fn granted(&self) -> usize {
        usize::MAX
    }

    fn category_breakdown(&self) -> HashMap<String, usize> {
        let cats = self.categories.lock().unwrap();
        cats.iter()
            .map(|(k, v)| (k.to_string(), v.current.load(Relaxed)))
            .filter(|(_, v)| *v > 0)
            .collect()
    }

    fn category_peak_breakdown(&self) -> HashMap<String, usize> {
        let cats = self.categories.lock().unwrap();
        cats.iter()
            .map(|(k, v)| (k.to_string(), v.peak.load(Relaxed)))
            .filter(|(_, v)| *v > 0)
            .collect()
    }
}

/// A memory pool with a hard capacity limit. Used for testing fail-fast behavior.
/// Returns `MemoryError::Denied` when a request would exceed the capacity.
#[cfg(test)]
#[derive(Debug)]
pub struct LimitedMemoryPool {
    capacity: usize,
    used: AtomicUsize,
    peak: AtomicUsize,
    categories: Mutex<HashMap<&'static str, CategoryTracker>>,
}

#[cfg(test)]
impl LimitedMemoryPool {
    pub fn new(capacity: usize) -> Self {
        Self {
            capacity,
            used: AtomicUsize::new(0),
            peak: AtomicUsize::new(0),
            categories: Mutex::new(HashMap::new()),
        }
    }

    fn update_category(&self, category: &'static str, delta: isize) {
        let mut cats = self.categories.lock().unwrap();
        let tracker = cats.entry(category).or_insert_with(CategoryTracker::new);
        if delta > 0 {
            let new_val = tracker.current.fetch_add(delta as usize, Relaxed) + delta as usize;
            let mut old_peak = tracker.peak.load(Relaxed);
            while new_val > old_peak {
                match tracker.peak.compare_exchange_weak(old_peak, new_val, Relaxed, Relaxed) {
                    Ok(_) => break,
                    Err(actual) => old_peak = actual,
                }
            }
        } else {
            tracker.current.fetch_sub((-delta) as usize, Relaxed);
        }
    }

    fn update_peak(&self) {
        let current = self.used.load(Relaxed);
        let mut old_peak = self.peak.load(Relaxed);
        while current > old_peak {
            match self.peak.compare_exchange_weak(old_peak, current, Relaxed, Relaxed) {
                Ok(_) => break,
                Err(actual) => old_peak = actual,
            }
        }
    }
}

#[cfg(test)]
impl MemoryPool for LimitedMemoryPool {
    fn try_acquire(&self, size: usize, category: &'static str) -> Result<(), MemoryError> {
        if size == 0 {
            return Ok(());
        }
        let current = self.used.load(Relaxed);
        if current + size > self.capacity {
            return Err(MemoryError::Denied {
                requested: size,
                available: self.capacity.saturating_sub(current),
                category: category.to_string(),
            });
        }
        self.used.fetch_add(size, Relaxed);
        self.update_category(category, size as isize);
        self.update_peak();
        Ok(())
    }

    fn release(&self, size: usize, category: &'static str) {
        self.used.fetch_sub(size, Relaxed);
        self.update_category(category, -(size as isize));
    }

    fn used(&self) -> usize { self.used.load(Relaxed) }
    fn peak(&self) -> usize { self.peak.load(Relaxed) }
    fn reset_peak(&self) -> usize {
        let current = self.used.load(Relaxed);
        self.peak.swap(current, Relaxed)
    }
    fn granted(&self) -> usize { self.capacity }
    fn category_breakdown(&self) -> HashMap<String, usize> {
        let cats = self.categories.lock().unwrap();
        cats.iter()
            .map(|(k, v)| (k.to_string(), v.current.load(Relaxed)))
            .filter(|(_, v)| *v > 0)
            .collect()
    }
    fn category_peak_breakdown(&self) -> HashMap<String, usize> {
        let cats = self.categories.lock().unwrap();
        cats.iter()
            .map(|(k, v)| (k.to_string(), v.peak.load(Relaxed)))
            .filter(|(_, v)| *v > 0)
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_unlimited_pool_basic() {
        let pool = UnlimitedMemoryPool::default();
        assert_eq!(pool.used(), 0);
        assert_eq!(pool.peak(), 0);

        pool.try_acquire(1000, "test").unwrap();
        assert_eq!(pool.used(), 1000);
        assert_eq!(pool.peak(), 1000);

        pool.try_acquire(2000, "test").unwrap();
        assert_eq!(pool.used(), 3000);
        assert_eq!(pool.peak(), 3000);

        pool.release(1500, "test");
        assert_eq!(pool.used(), 1500);
        assert_eq!(pool.peak(), 3000); // peak unchanged

        pool.release(1500, "test");
        assert_eq!(pool.used(), 0);
    }

    #[test]
    fn test_unlimited_pool_categories() {
        let pool = UnlimitedMemoryPool::default();

        pool.try_acquire(100, "index_writer").unwrap();
        pool.try_acquire(200, "l1_cache").unwrap();
        pool.try_acquire(50, "index_writer").unwrap();

        let breakdown = pool.category_breakdown();
        assert_eq!(*breakdown.get("index_writer").unwrap(), 150);
        assert_eq!(*breakdown.get("l1_cache").unwrap(), 200);
        assert_eq!(pool.used(), 350);

        pool.release(100, "index_writer");
        let breakdown = pool.category_breakdown();
        assert_eq!(*breakdown.get("index_writer").unwrap(), 50);

        // Release everything — current breakdown should be empty
        pool.release(50, "index_writer");
        pool.release(200, "l1_cache");
        let breakdown = pool.category_breakdown();
        assert!(breakdown.is_empty(), "Current breakdown should be empty after full release");

        // But peak breakdown should still show historical maximums
        let peak_breakdown = pool.category_peak_breakdown();
        assert_eq!(*peak_breakdown.get("index_writer").unwrap(), 150);
        assert_eq!(*peak_breakdown.get("l1_cache").unwrap(), 200);
    }

    #[test]
    fn test_unlimited_pool_reset_peak() {
        let pool = UnlimitedMemoryPool::default();

        pool.try_acquire(5000, "test").unwrap();
        assert_eq!(pool.peak(), 5000);

        pool.release(3000, "test");
        assert_eq!(pool.used(), 2000);
        assert_eq!(pool.peak(), 5000);

        // Reset peak — returns old peak, sets to current used
        let old_peak = pool.reset_peak();
        assert_eq!(old_peak, 5000);
        assert_eq!(pool.peak(), 2000);

        // New acquire below old peak — peak tracks correctly from reset point
        pool.try_acquire(1000, "test").unwrap();
        assert_eq!(pool.peak(), 3000);
    }

    #[test]
    fn test_unlimited_pool_always_grants() {
        let pool = UnlimitedMemoryPool::default();
        // Even very large requests succeed
        assert!(pool.try_acquire(1_000_000_000_000, "huge").is_ok());
        assert_eq!(pool.granted(), usize::MAX);
    }

    #[test]
    fn test_unlimited_pool_thread_safety() {
        use std::sync::Arc;
        use std::thread;

        static THREAD_CATS: [&str; 10] = [
            "thread_0", "thread_1", "thread_2", "thread_3", "thread_4",
            "thread_5", "thread_6", "thread_7", "thread_8", "thread_9",
        ];

        let pool = Arc::new(UnlimitedMemoryPool::default());
        let mut handles = vec![];

        for i in 0..10 {
            let pool = pool.clone();
            handles.push(thread::spawn(move || {
                let cat = THREAD_CATS[i];
                for _ in 0..100 {
                    pool.try_acquire(1000, cat).unwrap();
                }
                for _ in 0..100 {
                    pool.release(1000, cat);
                }
            }));
        }

        for h in handles {
            h.join().unwrap();
        }

        assert_eq!(pool.used(), 0);
    }

    // ========================================================================
    // LimitedMemoryPool tests — validate fail-fast denial behavior
    // ========================================================================

    #[test]
    fn test_limited_pool_grants_within_capacity() {
        let pool = LimitedMemoryPool::new(1000);
        assert!(pool.try_acquire(500, "test").is_ok());
        assert!(pool.try_acquire(500, "test").is_ok());
        assert_eq!(pool.used(), 1000);
        assert_eq!(pool.granted(), 1000);
    }

    #[test]
    fn test_limited_pool_denies_over_capacity() {
        let pool = LimitedMemoryPool::new(1000);
        pool.try_acquire(800, "test").unwrap();

        // Request that would exceed capacity
        let result = pool.try_acquire(300, "test");
        assert!(result.is_err());
        match result.unwrap_err() {
            MemoryError::Denied { requested, available, category } => {
                assert_eq!(requested, 300);
                assert_eq!(available, 200);
                assert_eq!(category, "test");
            }
            other => panic!("Expected Denied, got: {:?}", other),
        }
        // Used should not have changed (no partial allocation)
        assert_eq!(pool.used(), 800);
    }

    #[test]
    fn test_limited_pool_release_frees_capacity() {
        let pool = LimitedMemoryPool::new(1000);
        pool.try_acquire(1000, "test").unwrap();
        assert!(pool.try_acquire(1, "test").is_err()); // Full

        pool.release(500, "test");
        assert!(pool.try_acquire(500, "test").is_ok()); // Now fits
        assert_eq!(pool.used(), 1000);
    }

    #[test]
    fn test_limited_pool_zero_capacity_denies_everything() {
        let pool = LimitedMemoryPool::new(0);
        assert!(pool.try_acquire(1, "test").is_err());
        // Zero-size requests still succeed
        assert!(pool.try_acquire(0, "test").is_ok());
    }
}

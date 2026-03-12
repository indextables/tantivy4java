// memory_pool/disk_cache_budget.rs - Staircase-up/cliff-down memory budget for L2 disk cache
//
// Pattern:
// - Start: acquire `base_grant` (configured max) from pool
// - Grow: when queue needs more, acquire `grow_increment` (500MB) chunks
// - Cliff-down: when queue drains to 0, release all overflow above base_grant
// - Drop: release everything including base_grant
//
// This minimizes JNI round-trips by keeping the base grant permanently and only
// making JNI calls when growing or when the queue empties completely.

use std::sync::{Arc, Mutex};
use std::sync::atomic::{AtomicUsize, Ordering};

use crate::debug_println;
use super::pool::MemoryPool;

/// Default grow increment: 500MB
const DEFAULT_GROW_INCREMENT: usize = 500 * 1024 * 1024;

/// Default max grant multiplier: 8x the base grant
const DEFAULT_MAX_GRANT_MULTIPLIER: usize = 8;

/// Memory budget for the L2 disk cache write queue.
///
/// Manages a staircase-up/cliff-down grant pattern:
/// - Acquires `base_grant` upfront at creation
/// - Grows in `grow_increment` steps when needed (up to `max_grant` cap)
/// - Releases overflow (above base) when queue drains completely
/// - Releases base grant only on Drop
pub struct DiskCacheMemoryBudget {
    pool: Arc<dyn MemoryPool>,
    /// Base grant that is retained permanently (configured max write queue size)
    base_grant: usize,
    /// Current total granted from pool (base + overflow increments)
    total_granted: AtomicUsize,
    /// Size of each growth increment
    grow_increment: usize,
    /// Maximum total grant (hard cap). Growth beyond this is denied.
    max_grant: usize,
    /// Serializes ensure_capacity and on_queue_drained to prevent races
    /// between concurrent grow + drain operations on total_granted.
    op_lock: Mutex<()>,
}

impl DiskCacheMemoryBudget {
    /// Create a new budget, acquiring `base_grant` bytes from the pool.
    /// Max grant defaults to 8x the base grant.
    ///
    /// If the pool denies the base grant, returns a budget with 0 base
    /// (best-effort operation continues without memory tracking).
    pub fn new(pool: &Arc<dyn MemoryPool>, base_grant: usize) -> Self {
        Self::with_config(pool, base_grant, DEFAULT_GROW_INCREMENT, 0)
    }

    /// Create with a custom grow increment. Max grant defaults to 8x the base grant.
    pub fn with_increment(
        pool: &Arc<dyn MemoryPool>,
        base_grant: usize,
        grow_increment: usize,
    ) -> Self {
        Self::with_config(pool, base_grant, grow_increment, 0)
    }

    /// Create with custom grow increment and max grant cap.
    /// If `max_grant` is 0, defaults to `DEFAULT_MAX_GRANT_MULTIPLIER * base_grant`.
    pub fn with_config(
        pool: &Arc<dyn MemoryPool>,
        base_grant: usize,
        grow_increment: usize,
        max_grant: usize,
    ) -> Self {
        let actual_base = if base_grant > 0 {
            match pool.try_acquire(base_grant, "l2_write_queue") {
                Ok(()) => {
                    debug_println!(
                        "📊 DiskCacheMemoryBudget: Acquired base grant of {} MB",
                        base_grant / 1024 / 1024
                    );
                    base_grant
                }
                Err(e) => {
                    debug_println!(
                        "⚠️ DiskCacheMemoryBudget: Pool denied base grant of {} MB: {}. Operating untracked.",
                        base_grant / 1024 / 1024, e
                    );
                    0
                }
            }
        } else {
            0
        };

        let effective_max = if max_grant > 0 {
            max_grant.max(actual_base) // max_grant must be >= base
        } else {
            actual_base.saturating_mul(DEFAULT_MAX_GRANT_MULTIPLIER)
        };

        debug_println!(
            "📊 DiskCacheMemoryBudget: base={} MB, max={} MB, increment={} MB",
            actual_base / 1024 / 1024,
            effective_max / 1024 / 1024,
            grow_increment / 1024 / 1024
        );

        Self {
            pool: Arc::clone(pool),
            base_grant: actual_base,
            total_granted: AtomicUsize::new(actual_base),
            grow_increment,
            max_grant: effective_max,
            op_lock: Mutex::new(()),
        }
    }

    /// Ensure there is enough grant for `needed_bytes`.
    /// If current grant is insufficient, acquires more in `grow_increment` chunks,
    /// up to the `max_grant` cap. Returns true if sufficient grant is available, false if denied.
    pub fn ensure_capacity(&self, needed_bytes: usize) -> bool {
        let _guard = self.op_lock.lock().unwrap();

        let current = self.total_granted.load(Ordering::Acquire);
        if current >= needed_bytes {
            return true;
        }

        // Check if we've already hit the cap
        if current >= self.max_grant {
            debug_println!(
                "⚠️ DiskCacheMemoryBudget: At max grant cap ({} MB), cannot grow",
                self.max_grant / 1024 / 1024
            );
            return false;
        }

        // Need to grow — calculate how many increments, clamped to max_grant
        let deficit = needed_bytes - current;
        let increments = (deficit + self.grow_increment - 1) / self.grow_increment;
        let mut grow_amount = increments * self.grow_increment;

        // Clamp so total_granted + grow_amount <= max_grant
        let headroom = self.max_grant - current;
        if grow_amount > headroom {
            grow_amount = headroom;
        }

        // If clamped growth won't satisfy the need, don't bother growing
        if grow_amount == 0 || current + grow_amount < needed_bytes {
            debug_println!(
                "⚠️ DiskCacheMemoryBudget: Cannot satisfy {} MB (max cap {} MB)",
                needed_bytes / 1024 / 1024,
                self.max_grant / 1024 / 1024
            );
            return false;
        }

        match self.pool.try_acquire(grow_amount, "l2_write_queue") {
            Ok(()) => {
                self.total_granted.fetch_add(grow_amount, Ordering::Release);
                debug_println!(
                    "📊 DiskCacheMemoryBudget: Grew by {} MB (total now {} MB, max {} MB)",
                    grow_amount / 1024 / 1024,
                    (current + grow_amount) / 1024 / 1024,
                    self.max_grant / 1024 / 1024
                );
                true
            }
            Err(_) => {
                debug_println!(
                    "⚠️ DiskCacheMemoryBudget: Pool denied growth of {} MB",
                    grow_amount / 1024 / 1024
                );
                false
            }
        }
    }

    /// Called when the write queue drains completely (queued_bytes == 0).
    /// Releases all overflow above the base grant back to the pool.
    pub fn on_queue_drained(&self) {
        let _guard = self.op_lock.lock().unwrap();

        let current = self.total_granted.load(Ordering::Acquire);
        if current > self.base_grant {
            let overflow = current - self.base_grant;
            self.pool.release(overflow, "l2_write_queue");
            self.total_granted.store(self.base_grant, Ordering::Release);
            debug_println!(
                "📊 DiskCacheMemoryBudget: Queue drained, released {} MB overflow (retained {} MB base)",
                overflow / 1024 / 1024,
                self.base_grant / 1024 / 1024
            );
        }
    }

    /// Current total granted bytes.
    pub fn total_granted(&self) -> usize {
        self.total_granted.load(Ordering::Relaxed)
    }

    /// Base grant bytes (permanent floor).
    pub fn base_grant(&self) -> usize {
        self.base_grant
    }

    /// Maximum grant cap (hard ceiling for growth).
    pub fn max_grant(&self) -> usize {
        self.max_grant
    }
}

impl Drop for DiskCacheMemoryBudget {
    fn drop(&mut self) {
        let total = self.total_granted.load(Ordering::Acquire);
        if total > 0 {
            self.pool.release(total, "l2_write_queue");
            debug_println!(
                "📊 DiskCacheMemoryBudget: Released all {} MB on shutdown",
                total / 1024 / 1024
            );
        }
    }
}

impl std::fmt::Debug for DiskCacheMemoryBudget {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DiskCacheMemoryBudget")
            .field("base_grant", &self.base_grant)
            .field("total_granted", &self.total_granted.load(Ordering::Relaxed))
            .field("max_grant", &self.max_grant)
            .field("grow_increment", &self.grow_increment)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::memory_pool::UnlimitedMemoryPool;

    #[test]
    fn test_budget_basic_lifecycle() {
        let pool: Arc<dyn MemoryPool> = Arc::new(UnlimitedMemoryPool::default());
        let base = 100 * 1024 * 1024; // 100MB

        {
            let budget = DiskCacheMemoryBudget::new(&pool, base);
            assert_eq!(budget.base_grant(), base);
            assert_eq!(budget.total_granted(), base);
            assert_eq!(pool.used(), base);
        }
        // Dropped — all released
        assert_eq!(pool.used(), 0);
    }

    #[test]
    fn test_budget_grow_and_drain() {
        let pool: Arc<dyn MemoryPool> = Arc::new(UnlimitedMemoryPool::default());
        let base = 100 * 1024 * 1024;
        let increment = 50 * 1024 * 1024;

        let budget = DiskCacheMemoryBudget::with_increment(&pool, base, increment);
        assert_eq!(pool.used(), base);

        // Need more than base
        let needed = base + 30 * 1024 * 1024;
        assert!(budget.ensure_capacity(needed));
        assert_eq!(budget.total_granted(), base + increment); // Grew by one increment
        assert_eq!(pool.used(), base + increment);

        // Queue drains — release overflow back to base
        budget.on_queue_drained();
        assert_eq!(budget.total_granted(), base);
        assert_eq!(pool.used(), base);
    }

    #[test]
    fn test_budget_multiple_grows() {
        let pool: Arc<dyn MemoryPool> = Arc::new(UnlimitedMemoryPool::default());
        let base = 50 * 1024 * 1024;
        let increment = 25 * 1024 * 1024;

        let budget = DiskCacheMemoryBudget::with_increment(&pool, base, increment);

        // Need 120MB total — should grow by 3 increments (75MB)
        assert!(budget.ensure_capacity(120 * 1024 * 1024));
        assert_eq!(budget.total_granted(), base + 3 * increment);

        // Drain — back to base
        budget.on_queue_drained();
        assert_eq!(budget.total_granted(), base);
        assert_eq!(pool.used(), base);
    }

    #[test]
    fn test_budget_drain_at_base_is_noop() {
        let pool: Arc<dyn MemoryPool> = Arc::new(UnlimitedMemoryPool::default());
        let base = 100 * 1024 * 1024;

        let budget = DiskCacheMemoryBudget::new(&pool, base);
        budget.on_queue_drained(); // No overflow to release
        assert_eq!(budget.total_granted(), base);
        assert_eq!(pool.used(), base);
    }

    #[test]
    fn test_budget_zero_base() {
        let pool: Arc<dyn MemoryPool> = Arc::new(UnlimitedMemoryPool::default());
        let budget = DiskCacheMemoryBudget::new(&pool, 0);
        assert_eq!(budget.base_grant(), 0);
        assert_eq!(budget.total_granted(), 0);
        assert_eq!(pool.used(), 0);
    }

    // ========================================================================
    // Fail-fast denial tests
    // ========================================================================

    #[test]
    fn test_budget_base_denied_when_pool_full() {
        use crate::memory_pool::LimitedMemoryPool;
        let pool: Arc<dyn MemoryPool> = Arc::new(LimitedMemoryPool::new(50 * 1024 * 1024));

        // Request base grant larger than pool capacity
        let budget = DiskCacheMemoryBudget::new(&pool, 100 * 1024 * 1024);
        // Budget should fall back to 0 base when pool denies
        assert_eq!(budget.base_grant(), 0);
        assert_eq!(budget.total_granted(), 0);
        assert_eq!(pool.used(), 0);
    }

    #[test]
    fn test_budget_ensure_capacity_denied_returns_false() {
        use crate::memory_pool::LimitedMemoryPool;
        // Pool has 200MB capacity
        let pool: Arc<dyn MemoryPool> = Arc::new(LimitedMemoryPool::new(200 * 1024 * 1024));
        let base = 100 * 1024 * 1024;
        let increment = 50 * 1024 * 1024;

        let budget = DiskCacheMemoryBudget::with_increment(&pool, base, increment);
        assert_eq!(budget.base_grant(), base);

        // Need 250MB — pool only has 200MB total (100MB already used for base)
        // Growth would need 150MB (3 increments) but only 100MB available
        let result = budget.ensure_capacity(250 * 1024 * 1024);
        assert!(!result, "Should return false when pool denies growth");

        // total_granted should not have changed (no partial growth)
        assert_eq!(budget.total_granted(), base);
        assert_eq!(pool.used(), base);
    }

    // ========================================================================
    // Max grant cap tests
    // ========================================================================

    #[test]
    fn test_budget_default_max_grant_is_8x_base() {
        let pool: Arc<dyn MemoryPool> = Arc::new(UnlimitedMemoryPool::default());
        let base = 100 * 1024 * 1024; // 100MB

        let budget = DiskCacheMemoryBudget::new(&pool, base);
        assert_eq!(budget.max_grant(), base * 8); // Default: 8x
    }

    #[test]
    fn test_budget_custom_max_grant() {
        let pool: Arc<dyn MemoryPool> = Arc::new(UnlimitedMemoryPool::default());
        let base = 100 * 1024 * 1024;
        let max = 300 * 1024 * 1024; // 300MB cap

        let budget = DiskCacheMemoryBudget::with_config(
            &pool, base, 50 * 1024 * 1024, max,
        );
        assert_eq!(budget.max_grant(), max);
    }

    #[test]
    fn test_budget_growth_clamped_by_max_grant() {
        let pool: Arc<dyn MemoryPool> = Arc::new(UnlimitedMemoryPool::default());
        let base = 100 * 1024 * 1024;     // 100MB
        let increment = 50 * 1024 * 1024;  // 50MB
        let max = 200 * 1024 * 1024;       // 200MB cap

        let budget = DiskCacheMemoryBudget::with_config(&pool, base, increment, max);
        assert_eq!(budget.base_grant(), base);
        assert_eq!(budget.max_grant(), max);

        // Request 250MB — exceeds max_grant (200MB). Cannot satisfy, no growth occurs.
        let result = budget.ensure_capacity(250 * 1024 * 1024);
        assert!(!result);
        // No partial growth — budget stays at base
        assert_eq!(budget.total_granted(), base);
        assert_eq!(pool.used(), base);

        // Request exactly 200MB (at the cap) — succeeds with 100MB growth
        assert!(budget.ensure_capacity(200 * 1024 * 1024));
        assert_eq!(budget.total_granted(), max);
        assert_eq!(pool.used(), max);
    }

    #[test]
    fn test_budget_growth_stops_at_max_grant() {
        let pool: Arc<dyn MemoryPool> = Arc::new(UnlimitedMemoryPool::default());
        let base = 50 * 1024 * 1024;      // 50MB
        let increment = 25 * 1024 * 1024;  // 25MB
        let max = 100 * 1024 * 1024;       // 100MB cap

        let budget = DiskCacheMemoryBudget::with_config(&pool, base, increment, max);

        // Grow to 75MB (one increment)
        assert!(budget.ensure_capacity(75 * 1024 * 1024));
        assert_eq!(budget.total_granted(), 75 * 1024 * 1024);

        // Grow to 100MB (another increment, hits cap exactly)
        assert!(budget.ensure_capacity(100 * 1024 * 1024));
        assert_eq!(budget.total_granted(), 100 * 1024 * 1024);

        // Try to grow beyond cap — denied
        assert!(!budget.ensure_capacity(101 * 1024 * 1024));
        assert_eq!(budget.total_granted(), 100 * 1024 * 1024);
    }

    #[test]
    fn test_budget_drain_resets_then_can_grow_again() {
        let pool: Arc<dyn MemoryPool> = Arc::new(UnlimitedMemoryPool::default());
        let base = 50 * 1024 * 1024;
        let increment = 25 * 1024 * 1024;
        let max = 100 * 1024 * 1024;

        let budget = DiskCacheMemoryBudget::with_config(&pool, base, increment, max);

        // Grow to 100MB (cap)
        assert!(budget.ensure_capacity(100 * 1024 * 1024));
        assert_eq!(budget.total_granted(), max);

        // Drain — back to base
        budget.on_queue_drained();
        assert_eq!(budget.total_granted(), base);
        assert_eq!(pool.used(), base);

        // Can grow again after drain
        assert!(budget.ensure_capacity(75 * 1024 * 1024));
        assert_eq!(budget.total_granted(), 75 * 1024 * 1024);
    }

    #[test]
    fn test_budget_max_grant_at_least_base() {
        let pool: Arc<dyn MemoryPool> = Arc::new(UnlimitedMemoryPool::default());
        let base = 100 * 1024 * 1024;
        // Set max_grant smaller than base — should be raised to base
        let budget = DiskCacheMemoryBudget::with_config(
            &pool, base, 50 * 1024 * 1024, 50 * 1024 * 1024,
        );
        assert_eq!(budget.max_grant(), base); // Raised to base
    }

    #[test]
    fn test_budget_zero_base_zero_max() {
        let pool: Arc<dyn MemoryPool> = Arc::new(UnlimitedMemoryPool::default());
        let budget = DiskCacheMemoryBudget::with_config(&pool, 0, 50 * 1024 * 1024, 0);
        assert_eq!(budget.base_grant(), 0);
        assert_eq!(budget.max_grant(), 0); // 8 * 0 = 0
        // Can't grow at all
        assert!(!budget.ensure_capacity(1));
    }
}

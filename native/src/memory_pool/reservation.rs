// memory_pool/reservation.rs - RAII memory reservation guard

use std::sync::Arc;

use crate::debug_println;
use super::pool::{MemoryError, MemoryPool};

/// RAII guard that holds a memory reservation from a MemoryPool.
/// Automatically releases the reserved memory when dropped.
///
/// # Example
/// ```ignore
/// let reservation = MemoryReservation::try_new(&pool, 1024, "index_writer")?;
/// // ... use the memory ...
/// // reservation is automatically released when it goes out of scope
/// ```
pub struct MemoryReservation {
    pool: Arc<dyn MemoryPool>,
    size: usize,
    category: &'static str,
}

impl MemoryReservation {
    /// Try to create a new reservation, acquiring `size` bytes from the pool.
    /// Returns Err if the pool denies the request.
    pub fn try_new(
        pool: &Arc<dyn MemoryPool>,
        size: usize,
        category: &'static str,
    ) -> Result<Self, MemoryError> {
        if size == 0 {
            return Ok(Self {
                pool: Arc::clone(pool),
                size: 0,
                category,
            });
        }
        pool.try_acquire(size, category)?;
        debug_println!(
            "📊 MEMORY_POOL: Reserved {} bytes for '{}' (pool total: {} bytes)",
            size, category, pool.used()
        );
        Ok(Self {
            pool: Arc::clone(pool),
            size,
            category,
        })
    }

    /// Create a reservation that doesn't track any memory (zero-cost no-op).
    pub fn empty(pool: &Arc<dyn MemoryPool>, category: &'static str) -> Self {
        Self {
            pool: Arc::clone(pool),
            size: 0,
            category,
        }
    }

    /// Resize this reservation. Acquires more if growing, releases if shrinking.
    pub fn resize(&mut self, new_size: usize) -> Result<(), MemoryError> {
        if new_size == self.size {
            return Ok(());
        }
        if new_size > self.size {
            let additional = new_size - self.size;
            self.pool.try_acquire(additional, self.category)?;
        } else {
            let decrease = self.size - new_size;
            self.pool.release(decrease, self.category);
        }
        self.size = new_size;
        Ok(())
    }

    /// Grow this reservation by `additional` bytes.
    pub fn grow(&mut self, additional: usize) -> Result<(), MemoryError> {
        self.resize(self.size + additional)
    }

    /// Shrink this reservation by `amount` bytes.
    pub fn shrink(&mut self, amount: usize) {
        let new_size = self.size.saturating_sub(amount);
        let _ = self.resize(new_size);
    }

    /// Current size of this reservation in bytes.
    pub fn size(&self) -> usize {
        self.size
    }

    /// Category this reservation belongs to.
    pub fn category(&self) -> &'static str {
        self.category
    }

    /// Release all memory and reset to zero.
    pub fn release_all(&mut self) {
        if self.size > 0 {
            self.pool.release(self.size, self.category);
            self.size = 0;
        }
    }
}

impl Drop for MemoryReservation {
    fn drop(&mut self) {
        if self.size > 0 {
            self.pool.release(self.size, self.category);
            debug_println!(
                "📊 MEMORY_POOL: Released {} bytes for '{}' (pool total: {} bytes)",
                self.size, self.category, self.pool.used()
            );
        }
    }
}

impl std::fmt::Debug for MemoryReservation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MemoryReservation")
            .field("size", &self.size)
            .field("category", &self.category)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::memory_pool::UnlimitedMemoryPool;

    #[test]
    fn test_reservation_basic_lifecycle() {
        let pool: Arc<dyn MemoryPool> = Arc::new(UnlimitedMemoryPool::default());

        {
            let reservation = MemoryReservation::try_new(&pool, 1024, "test").unwrap();
            assert_eq!(reservation.size(), 1024);
            assert_eq!(pool.used(), 1024);
        }
        // Dropped — memory released
        assert_eq!(pool.used(), 0);
    }

    #[test]
    fn test_reservation_resize_grow() {
        let pool: Arc<dyn MemoryPool> = Arc::new(UnlimitedMemoryPool::default());
        let mut reservation = MemoryReservation::try_new(&pool, 1000, "test").unwrap();
        assert_eq!(pool.used(), 1000);

        reservation.resize(2000).unwrap();
        assert_eq!(reservation.size(), 2000);
        assert_eq!(pool.used(), 2000);
    }

    #[test]
    fn test_reservation_resize_shrink() {
        let pool: Arc<dyn MemoryPool> = Arc::new(UnlimitedMemoryPool::default());
        let mut reservation = MemoryReservation::try_new(&pool, 2000, "test").unwrap();

        reservation.resize(500).unwrap();
        assert_eq!(reservation.size(), 500);
        assert_eq!(pool.used(), 500);
    }

    #[test]
    fn test_reservation_grow_and_shrink() {
        let pool: Arc<dyn MemoryPool> = Arc::new(UnlimitedMemoryPool::default());
        let mut reservation = MemoryReservation::try_new(&pool, 1000, "test").unwrap();

        reservation.grow(500).unwrap();
        assert_eq!(reservation.size(), 1500);
        assert_eq!(pool.used(), 1500);

        reservation.shrink(300);
        assert_eq!(reservation.size(), 1200);
        assert_eq!(pool.used(), 1200);
    }

    #[test]
    fn test_reservation_release_all() {
        let pool: Arc<dyn MemoryPool> = Arc::new(UnlimitedMemoryPool::default());
        let mut reservation = MemoryReservation::try_new(&pool, 5000, "test").unwrap();
        assert_eq!(pool.used(), 5000);

        reservation.release_all();
        assert_eq!(reservation.size(), 0);
        assert_eq!(pool.used(), 0);

        // Drop should be no-op since already released
        drop(reservation);
        assert_eq!(pool.used(), 0);
    }

    #[test]
    fn test_reservation_zero_size() {
        let pool: Arc<dyn MemoryPool> = Arc::new(UnlimitedMemoryPool::default());
        let reservation = MemoryReservation::try_new(&pool, 0, "test").unwrap();
        assert_eq!(reservation.size(), 0);
        assert_eq!(pool.used(), 0);
        drop(reservation);
        assert_eq!(pool.used(), 0);
    }

    #[test]
    fn test_reservation_empty() {
        let pool: Arc<dyn MemoryPool> = Arc::new(UnlimitedMemoryPool::default());
        let reservation = MemoryReservation::empty(&pool, "test");
        assert_eq!(reservation.size(), 0);
        assert_eq!(pool.used(), 0);
    }

    #[test]
    fn test_multiple_reservations_same_pool() {
        let pool: Arc<dyn MemoryPool> = Arc::new(UnlimitedMemoryPool::default());

        let r1 = MemoryReservation::try_new(&pool, 100, "index_writer").unwrap();
        let r2 = MemoryReservation::try_new(&pool, 200, "l1_cache").unwrap();
        let r3 = MemoryReservation::try_new(&pool, 300, "merge").unwrap();

        assert_eq!(pool.used(), 600);

        drop(r2);
        assert_eq!(pool.used(), 400);

        drop(r1);
        drop(r3);
        assert_eq!(pool.used(), 0);
    }

    #[test]
    fn test_reservation_category_tracking() {
        let pool: Arc<dyn MemoryPool> = Arc::new(UnlimitedMemoryPool::default());

        let _r1 = MemoryReservation::try_new(&pool, 100, "index_writer").unwrap();
        let _r2 = MemoryReservation::try_new(&pool, 200, "l1_cache").unwrap();

        let breakdown = pool.category_breakdown();
        assert_eq!(*breakdown.get("index_writer").unwrap(), 100);
        assert_eq!(*breakdown.get("l1_cache").unwrap(), 200);
    }

    // ========================================================================
    // Fail-fast denial tests — validates that MemoryReservation propagates errors
    // ========================================================================

    #[test]
    fn test_reservation_denied_when_pool_full() {
        use crate::memory_pool::LimitedMemoryPool;
        let pool: Arc<dyn MemoryPool> = Arc::new(LimitedMemoryPool::new(1000));

        // First reservation succeeds
        let _r1 = MemoryReservation::try_new(&pool, 800, "test").unwrap();

        // Second reservation should be denied (would exceed capacity)
        let result = MemoryReservation::try_new(&pool, 300, "test");
        assert!(result.is_err(), "Should fail when pool is nearly full");

        // Pool used should still be only from r1 (no partial allocation)
        assert_eq!(pool.used(), 800);
    }

    #[test]
    fn test_reservation_denial_does_not_leak_memory() {
        use crate::memory_pool::LimitedMemoryPool;
        let pool: Arc<dyn MemoryPool> = Arc::new(LimitedMemoryPool::new(500));

        // Denied reservation should not leak any memory
        let result = MemoryReservation::try_new(&pool, 1000, "big");
        assert!(result.is_err());
        assert_eq!(pool.used(), 0, "No memory should be reserved after denial");

        // Pool should still be fully usable after denial
        let _r = MemoryReservation::try_new(&pool, 500, "fits").unwrap();
        assert_eq!(pool.used(), 500);
    }

    #[test]
    fn test_reservation_grow_denied_when_pool_full() {
        use crate::memory_pool::LimitedMemoryPool;
        let pool: Arc<dyn MemoryPool> = Arc::new(LimitedMemoryPool::new(1000));

        let mut r = MemoryReservation::try_new(&pool, 800, "test").unwrap();

        // Grow should fail if it would exceed capacity
        let result = r.grow(300);
        assert!(result.is_err());
        assert_eq!(r.size(), 800, "Reservation size unchanged after failed grow");
        assert_eq!(pool.used(), 800, "Pool used unchanged after failed grow");
    }
}

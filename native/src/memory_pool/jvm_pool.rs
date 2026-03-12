// memory_pool/jvm_pool.rs - JVM-backed memory pool with high/low watermark batching

use std::collections::HashMap;
use std::fmt;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering::Relaxed};
use std::sync::Mutex;

use jni::objects::{GlobalRef, JMethodID, JValue};
use jni::signature::ReturnType;
use jni::JNIEnv;

use super::pool::{MemoryError, MemoryPool};
use crate::debug_println;

/// Default high watermark: acquire more from JVM when usage exceeds 90% of grant.
const DEFAULT_HIGH_WATERMARK: f64 = 0.90;
/// Default low watermark: release excess to JVM when usage drops below 25% of grant.
const DEFAULT_LOW_WATERMARK: f64 = 0.25;
/// Default minimum JNI acquire chunk: 64MB.
const DEFAULT_ACQUIRE_INCREMENT: usize = 64 * 1024 * 1024;
/// Default minimum amount to release back: 64MB.
const DEFAULT_MIN_RELEASE_AMOUNT: usize = 64 * 1024 * 1024;

/// Configuration for JvmMemoryPool watermark behavior.
#[derive(Debug, Clone)]
pub struct JvmPoolConfig {
    pub high_watermark: f64,
    pub low_watermark: f64,
    pub acquire_increment: usize,
    pub min_release_amount: usize,
}

impl Default for JvmPoolConfig {
    fn default() -> Self {
        Self {
            high_watermark: DEFAULT_HIGH_WATERMARK,
            low_watermark: DEFAULT_LOW_WATERMARK,
            acquire_increment: DEFAULT_ACQUIRE_INCREMENT,
            min_release_amount: DEFAULT_MIN_RELEASE_AMOUNT,
        }
    }
}

/// A memory pool that coordinates with a Java NativeMemoryAccountant via JNI.
///
/// Uses high/low watermark batching to minimize JNI round-trips:
/// - Most reserve/release calls are pure atomic operations (zero JNI).
/// - JNI calls happen only when usage crosses watermark thresholds.
///
/// Thread-safe: all state is atomic or behind Mutex.
pub struct JvmMemoryPool {
    /// Reference to the Java NativeMemoryAccountant object.
    jvm_ref: GlobalRef,
    /// Cached JNI method ID for acquireMemory(long) -> long.
    acquire_mid: JMethodID,
    /// Cached JNI method ID for releaseMemory(long) -> void.
    release_mid: JMethodID,

    // Authoritative state
    /// Total bytes the JVM has granted us.
    jvm_granted: AtomicUsize,
    /// Total bytes Rust code has reserved from us.
    rust_used: AtomicUsize,
    /// Peak usage observed.
    peak: AtomicUsize,

    // Per-category tracking (current + peak)
    categories: Mutex<HashMap<&'static str, super::pool::CategoryTracker>>,

    // Watermark configuration
    config: JvmPoolConfig,

    // Mutex to serialize JNI calls (JNI method IDs are not Send in some impls)
    jni_lock: Mutex<()>,

    /// When true, skip JNI release callbacks. Set during JVM shutdown to avoid
    /// calling releaseMemory() outside of a task context (e.g., on shutdown hook
    /// threads where Spark's TaskContext is unavailable).
    shutting_down: AtomicBool,
}

// Safety: JMethodID is a pointer that is valid for the lifetime of the JVM.
// GlobalRef prevents the Java object from being garbage collected.
// We serialize JNI calls through jni_lock.
unsafe impl Send for JvmMemoryPool {}
unsafe impl Sync for JvmMemoryPool {}

impl fmt::Debug for JvmMemoryPool {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("JvmMemoryPool")
            .field("jvm_granted", &self.jvm_granted.load(Relaxed))
            .field("rust_used", &self.rust_used.load(Relaxed))
            .field("peak", &self.peak.load(Relaxed))
            .field("config", &self.config)
            .finish()
    }
}

impl JvmMemoryPool {
    /// Create a new JvmMemoryPool from a Java NativeMemoryAccountant object.
    ///
    /// # Arguments
    /// * `env` - JNI environment (used only during construction to cache method IDs)
    /// * `accountant` - GlobalRef to Java NativeMemoryAccountant object
    /// * `config` - Watermark configuration
    pub fn new(
        env: &mut JNIEnv,
        accountant: GlobalRef,
        config: JvmPoolConfig,
    ) -> Result<Self, MemoryError> {
        // Cache method IDs for acquireMemory and releaseMemory
        let class = env
            .get_object_class(&accountant)
            .map_err(|e| MemoryError::JniError(format!("Failed to get accountant class: {}", e)))?;

        let acquire_mid = env
            .get_method_id(&class, "acquireMemory", "(J)J")
            .map_err(|e| {
                MemoryError::JniError(format!("Failed to find acquireMemory method: {}", e))
            })?;

        let release_mid = env
            .get_method_id(&class, "releaseMemory", "(J)V")
            .map_err(|e| {
                MemoryError::JniError(format!("Failed to find releaseMemory method: {}", e))
            })?;

        Ok(Self {
            jvm_ref: accountant,
            acquire_mid,
            release_mid,
            jvm_granted: AtomicUsize::new(0),
            rust_used: AtomicUsize::new(0),
            peak: AtomicUsize::new(0),
            categories: Mutex::new(HashMap::new()),
            config,
            jni_lock: Mutex::new(()),
            shutting_down: AtomicBool::new(false),
        })
    }

    /// Execute a closure with the JNI environment for the current thread.
    ///
    /// This keeps the `JNIEnv` borrow scoped to the closure, avoiding the need
    /// for an unsafe lifetime transmute. The `jni_lock` is held for the duration,
    /// serializing all JNI calls.
    fn with_jni_env<R>(&self, f: impl FnOnce(&mut JNIEnv) -> R) -> Result<R, MemoryError> {
        let _lock = self.jni_lock.lock().unwrap();

        let jvm = crate::utils::get_jvm().ok_or_else(|| {
            MemoryError::JniError("JavaVM not available".to_string())
        })?;

        let mut env = jvm.attach_current_thread_permanently().map_err(|e| {
            MemoryError::JniError(format!("Failed to attach thread: {}", e))
        })?;

        Ok(f(&mut env))
    }

    /// Call Java acquireMemory(bytes) → returns actual bytes granted.
    fn jni_acquire(&self, bytes: usize) -> Result<usize, MemoryError> {
        self.with_jni_env(|env| {
            let result = unsafe {
                env.call_method_unchecked(
                    &self.jvm_ref,
                    self.acquire_mid,
                    ReturnType::Primitive(jni::signature::Primitive::Long),
                    &[JValue::Long(bytes as i64).as_jni()],
                )
            };

            match result {
                Ok(val) => {
                    if env.exception_check().unwrap_or(false) {
                        env.exception_clear().ok();
                        return Err(MemoryError::JniError(
                            "Java exception during acquireMemory".to_string(),
                        ));
                    }
                    let acquired = val.j().map_err(|e| {
                        MemoryError::JniError(format!("Failed to extract long result: {}", e))
                    })?;
                    Ok(acquired as usize)
                }
                Err(e) => {
                    env.exception_clear().ok();
                    Err(MemoryError::JniError(format!(
                        "JNI acquireMemory call failed: {}",
                        e
                    )))
                }
            }
        })?
    }

    /// Call Java releaseMemory(bytes).
    fn jni_release(&self, bytes: usize) -> Result<(), MemoryError> {
        self.with_jni_env(|env| {
            let result = unsafe {
                env.call_method_unchecked(
                    &self.jvm_ref,
                    self.release_mid,
                    ReturnType::Primitive(jni::signature::Primitive::Void),
                    &[JValue::Long(bytes as i64).as_jni()],
                )
            };

            match result {
                Ok(_) => {
                    if env.exception_check().unwrap_or(false) {
                        env.exception_clear().ok();
                        return Err(MemoryError::JniError(
                            "Java exception during releaseMemory".to_string(),
                        ));
                    }
                    Ok(())
                }
                Err(e) => {
                    env.exception_clear().ok();
                    Err(MemoryError::JniError(format!(
                        "JNI releaseMemory call failed: {}",
                        e
                    )))
                }
            }
        })?
    }

    fn update_category(&self, category: &'static str, delta: isize) {
        let mut cats = self.categories.lock().unwrap();
        let tracker = cats
            .entry(category)
            .or_insert_with(super::pool::CategoryTracker::new);
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
        let current = self.rust_used.load(Relaxed);
        let mut old_peak = self.peak.load(Relaxed);
        // CAS loop converges quickly: each retry means another thread updated
        // peak to a higher value, and once old_peak >= current the loop exits.
        while current > old_peak {
            match self.peak.compare_exchange_weak(old_peak, current, Relaxed, Relaxed) {
                Ok(_) => break,
                Err(actual) => old_peak = actual,
            }
        }
    }

    /// Check if we need to acquire more from JVM (usage crossed high watermark).
    fn needs_jvm_acquire(&self, new_used: usize) -> bool {
        let granted = self.jvm_granted.load(Relaxed);
        if granted == 0 {
            return true; // No grant yet, must acquire
        }
        new_used as f64 > granted as f64 * self.config.high_watermark
    }

    /// Check if we should release excess to JVM (usage crossed low watermark).
    fn should_jvm_release(&self) -> Option<usize> {
        let granted = self.jvm_granted.load(Relaxed);
        let used = self.rust_used.load(Relaxed);

        if granted == 0 {
            return None;
        }

        // When usage drops to zero, release the entire grant back to JVM
        if used == 0 {
            if granted >= self.config.min_release_amount {
                return Some(granted);
            }
            return None;
        }

        if (used as f64) < (granted as f64 * self.config.low_watermark) {
            // Calculate how much to keep: enough headroom above current usage
            let target_grant = if self.config.low_watermark > 0.0 {
                (used as f64 / self.config.low_watermark) as usize
            } else {
                used
            };
            let excess = granted.saturating_sub(target_grant);
            if excess >= self.config.min_release_amount {
                return Some(excess);
            }
        }
        None
    }
}

impl MemoryPool for JvmMemoryPool {
    fn try_acquire(&self, size: usize, category: &'static str) -> Result<(), MemoryError> {
        if size == 0 {
            return Ok(());
        }

        // Optimistic update: increment used
        let new_used = self.rust_used.fetch_add(size, Relaxed) + size;

        // Check if we need more from JVM
        if self.needs_jvm_acquire(new_used) {
            let want = std::cmp::max(size, self.config.acquire_increment);

            match self.jni_acquire(want) {
                Ok(acquired) if acquired >= size => {
                    self.jvm_granted.fetch_add(acquired, Relaxed);
                }
                Ok(acquired) => {
                    // JVM gave us less than we need — release what we got, undo, fail
                    if acquired > 0 {
                        let _ = self.jni_release(acquired);
                    }
                    self.rust_used.fetch_sub(size, Relaxed);
                    return Err(MemoryError::Denied {
                        requested: size,
                        available: acquired,
                        category: category.to_string(),
                    });
                }
                Err(e) => {
                    self.rust_used.fetch_sub(size, Relaxed);
                    return Err(e);
                }
            }
        }

        self.update_category(category, size as isize);
        self.update_peak();
        Ok(())
    }

    fn release(&self, size: usize, category: &'static str) {
        if size == 0 {
            return;
        }

        self.rust_used.fetch_sub(size, Relaxed);
        self.update_category(category, -(size as isize));

        // Check if we should release excess to JVM.
        // Skip JNI release during shutdown — the JVM is exiting and the
        // accountant's task context may no longer be available.
        if self.shutting_down.load(Relaxed) {
            return;
        }

        // Cap the release to `size` (the amount being freed by this call) to
        // prevent releasing more to the JVM accountant than what the current
        // thread's reservation held. The global pool batches acquisitions across
        // threads, so the excess can be larger than any single thread's total.
        // Without capping, a per-task accountant (e.g., Spark's ExecutionMemoryPool)
        // would see releaseMemory(X) where X exceeds what that task acquired.
        // Any remaining excess stays in jvm_granted and will be released by
        // subsequent operations or on pool shutdown.
        if let Some(excess) = self.should_jvm_release() {
            let to_release = excess.min(size);
            if to_release > 0 {
                if let Ok(()) = self.jni_release(to_release) {
                    self.jvm_granted.fetch_sub(to_release, Relaxed);
                }
            }
            // If JNI release fails, we just keep the grant — no harm done
        }
    }

    fn used(&self) -> usize {
        self.rust_used.load(Relaxed)
    }

    fn peak(&self) -> usize {
        self.peak.load(Relaxed)
    }

    fn reset_peak(&self) -> usize {
        let current = self.rust_used.load(Relaxed);
        self.peak.swap(current, Relaxed)
    }

    fn granted(&self) -> usize {
        self.jvm_granted.load(Relaxed)
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

    fn shutdown(&self) {
        self.shutting_down.store(true, Relaxed);
    }
}

impl Drop for JvmMemoryPool {
    fn drop(&mut self) {
        // Skip JNI release during shutdown — the JVM is exiting and the
        // accountant's task context may no longer be available.
        if self.shutting_down.load(Relaxed) {
            return;
        }
        // Release all remaining grant back to JVM
        let granted = self.jvm_granted.swap(0, Relaxed);
        if granted > 0 {
            if let Err(e) = self.jni_release(granted) {
                debug_println!(
                    "MEMORY_POOL: Failed to release {} bytes back to JVM during drop: {}",
                    granted, e
                );
            }
        }
    }
}

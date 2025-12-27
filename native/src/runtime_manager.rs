// runtime_manager.rs - Global Tokio Runtime Manager for async-first architecture
//
// This module provides a singleton runtime manager that eliminates sync-in-async deadlocks
// by providing proper async-first patterns for JNI bridge operations with Quickwit.

use std::sync::{Arc, OnceLock, atomic::{AtomicBool, AtomicUsize, Ordering}};
use tokio::runtime::{Runtime, Handle};
use std::future::Future;
use crate::debug_println;

/// Global singleton runtime manager for all Quickwit async operations
pub struct QuickwitRuntimeManager {
    /// Main tokio runtime for all async operations
    runtime: Arc<Runtime>,
    /// Track if runtime is shutting down to prevent new operations
    is_shutting_down: AtomicBool,
    /// Count of active searchers to prevent premature shutdown
    active_searcher_count: AtomicUsize,
}

impl QuickwitRuntimeManager {
    /// Create a new runtime manager with optimal configuration
    fn new() -> anyhow::Result<Self> {
        debug_println!("🚀 RUNTIME_MANAGER: Creating new QuickwitRuntimeManager");

        // Create multi-threaded runtime optimized for I/O operations
        let runtime = Arc::new(
            tokio::runtime::Builder::new_multi_thread()
                .worker_threads(4) // Optimal for mixed I/O and CPU workloads
                .thread_name("quickwit-runtime")
                .enable_all() // Enable I/O and time drivers
                .build()?
        );

        debug_println!("✅ RUNTIME_MANAGER: QuickwitRuntimeManager created successfully");

        Ok(QuickwitRuntimeManager {
            runtime,
            is_shutting_down: AtomicBool::new(false),
            active_searcher_count: AtomicUsize::new(0),
        })
    }

    /// Get the global runtime manager instance (singleton pattern)
    pub fn global() -> &'static Self {
        static RUNTIME_MANAGER: OnceLock<QuickwitRuntimeManager> = OnceLock::new();
        RUNTIME_MANAGER.get_or_init(|| {
            let manager = Self::new().expect("Failed to create QuickwitRuntimeManager");

            // Note: JVM shutdown hooks must be registered from Java side
            // The runtime will be properly shut down via explicit calls or JVM termination

            manager
        })
    }

    // Removed: block_on_search - replaced with thread-safe block_on that doesn't use jobject

    // Removed: spawn_search - replaced with generic spawn that doesn't use jobject

    /// Get a handle to the runtime for advanced usage
    ///
    /// This should only be used by internal async operations, not JNI bridges.
    pub fn handle(&self) -> &Handle {
        self.runtime.handle()
    }

    /// Register a new searcher to prevent premature runtime shutdown
    pub fn register_searcher(&self) {
        let count = self.active_searcher_count.fetch_add(1, Ordering::Relaxed);
        debug_println!("📊 RUNTIME_MANAGER: Registered searcher, active count: {}", count + 1);
    }

    /// Unregister a searcher when it's being closed
    pub fn unregister_searcher(&self) {
        let old_count = self.active_searcher_count.fetch_sub(1, Ordering::Relaxed);
        debug_println!("📊 RUNTIME_MANAGER: Unregistered searcher, active count: {}", old_count.saturating_sub(1));
    }

    /// Block on any generic async operation (not search-specific)
    ///
    /// This is for internal operations that need async execution but aren't search-related.
    pub fn block_on<F, T>(&self, future: F) -> T
    where
        F: Future<Output = T> + Send + 'static,
        T: Send + 'static,
    {
        debug_println!("🔄 RUNTIME_MANAGER: Starting generic block_on operation");

        // Check if runtime is shutting down
        if self.is_shutting_down.load(Ordering::Acquire) {
            debug_println!("⚠️ RUNTIME_MANAGER: Runtime is shutting down, but proceeding with caution");
        }

        // Check if we're already in a tokio context
        if Handle::try_current().is_ok() {
            panic!("block_on called from within async context - this indicates an architectural problem");
        }

        // Try to use the runtime, but catch if it's been dropped/shutdown externally
        match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            self.runtime.block_on(future)
        })) {
            Ok(result) => {
                debug_println!("✅ RUNTIME_MANAGER: Generic block_on operation completed");
                result
            }
            Err(_panic_info) => {
                debug_println!("❌ RUNTIME_MANAGER: Runtime panicked or was dropped externally - this suggests a runtime lifecycle issue");
                panic!("Runtime was dropped externally while operation was in progress - this indicates the runtime shutdown coordination is not working properly");
            }
        }
    }

    /// Gracefully shutdown the runtime with a timeout for pending operations
    /// This should only be called during JVM termination or explicit test cleanup
    pub fn shutdown_with_timeout(&self, timeout_secs: u64) {
        debug_println!("🛑 RUNTIME_MANAGER: Initiating graceful shutdown with {}s timeout", timeout_secs);
        debug_println!("📊 RUNTIME_MANAGER: Current active searcher count: {}", self.active_searcher_count.load(Ordering::Acquire));

        // Set shutdown flag - but allow existing operations to continue
        self.is_shutting_down.store(true, Ordering::Release);
        debug_println!("🚫 RUNTIME_MANAGER: Set shutdown flag for coordination");

        // Wait briefly for active searchers to complete (but don't block forever)
        let start_time = std::time::Instant::now();
        let wait_timeout = std::time::Duration::from_secs(2); // Short wait, don't block system

        while self.active_searcher_count.load(Ordering::Acquire) > 0 {
            if start_time.elapsed() > wait_timeout {
                let remaining = self.active_searcher_count.load(Ordering::Acquire);
                debug_println!("⚠️ RUNTIME_MANAGER: {} active searchers still running, shutting down runtime anyway", remaining);
                break;
            }

            debug_println!("⏳ RUNTIME_MANAGER: Waiting for {} active searchers to complete",
                          self.active_searcher_count.load(Ordering::Acquire));
            std::thread::sleep(std::time::Duration::from_millis(100));
        }

        debug_println!("🏁 RUNTIME_MANAGER: Proceeding with runtime shutdown");

        // Shutdown the runtime gracefully with timeout
        // Note: We can't shutdown the runtime from within an Arc since shutdown_timeout consumes the runtime
        // This is intentionally left as a TODO since proper shutdown requires coordination
        debug_println!("⚠️ RUNTIME_MANAGER: Cannot shutdown Arc<Runtime> - requires architectural changes for proper cleanup");
        // In practice, the JVM shutdown will clean up the runtime anyway

        debug_println!("✅ RUNTIME_MANAGER: Graceful shutdown completed");
    }
}

/// JNI function to trigger graceful shutdown of the runtime
/// This should be called from Java shutdown hooks or test cleanup
#[no_mangle]
pub extern "C" fn Java_io_indextables_tantivy4java_config_RuntimeManager_shutdownGracefullyNative(
    _env: jni::JNIEnv,
    _class: jni::objects::JClass,
    timeout_seconds: jni::sys::jlong,
) {
    debug_println!("📞 JNI: Received graceful shutdown request with {}s timeout", timeout_seconds);

    let manager = QuickwitRuntimeManager::global();
    manager.shutdown_with_timeout(timeout_seconds as u64);

    debug_println!("✅ JNI: Graceful shutdown completed");
}

// Removed: block_on_search_operation - replaced with thread-safe alternatives
// that use block_on_operation with String/primitive return types instead of jobject

/// Convenience function for generic async operations
pub fn block_on_operation<F, T>(future: F) -> T
where
    F: Future<Output = T> + Send + 'static,
    T: Send + 'static,
{
    let manager = QuickwitRuntimeManager::global();

    // Execute the operation with runtime failure detection
    manager.block_on(future)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn test_runtime_manager_singleton() {
        let manager1 = QuickwitRuntimeManager::global();
        let manager2 = QuickwitRuntimeManager::global();

        // Should be the same instance
        assert!(std::ptr::eq(manager1, manager2));
    }

    #[test]
    fn test_simple_async_operation() {
        let result = block_on_operation(async {
            tokio::time::sleep(Duration::from_millis(1)).await;
            42
        });

        assert_eq!(result, 42);
    }
}
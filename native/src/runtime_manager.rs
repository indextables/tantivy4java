// runtime_manager.rs - Global Tokio Runtime Manager for async-first architecture
//
// This module provides a singleton runtime manager that eliminates sync-in-async deadlocks
// by providing proper async-first patterns for JNI bridge operations with Quickwit.

use std::sync::{Arc, OnceLock};
use tokio::runtime::{Runtime, Handle};
use tokio::task::JoinHandle;
use std::future::Future;
use jni::sys::jobject;
use quickwit_common::thread_pool::ThreadPool;
use crate::debug_println;

/// Global singleton runtime manager for all Quickwit async operations
pub struct QuickwitRuntimeManager {
    /// Main tokio runtime for all async operations
    runtime: Arc<Runtime>,
    /// Quickwit-compatible thread pool for search operations
    thread_pool: Arc<ThreadPool>,
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

        // Create Quickwit-compatible thread pool for search operations
        let thread_pool = Arc::new(ThreadPool::new("quickwit-search", None));

        debug_println!("✅ RUNTIME_MANAGER: QuickwitRuntimeManager created successfully");

        Ok(QuickwitRuntimeManager {
            runtime,
            thread_pool,
        })
    }

    /// Get the global runtime manager instance (singleton pattern)
    pub fn global() -> &'static Self {
        static RUNTIME_MANAGER: OnceLock<QuickwitRuntimeManager> = OnceLock::new();
        RUNTIME_MANAGER.get_or_init(|| {
            Self::new().expect("Failed to create QuickwitRuntimeManager")
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

    /// Get the search thread pool for Quickwit compatibility
    pub fn search_thread_pool(&self) -> &ThreadPool {
        &self.thread_pool
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

        // Check if we're already in a tokio context
        if Handle::try_current().is_ok() {
            panic!("block_on called from within async context - this indicates an architectural problem");
        }

        let result = self.runtime.block_on(future);
        debug_println!("✅ RUNTIME_MANAGER: Generic block_on operation completed");
        result
    }

    /// Check if we're currently in the runtime context
    pub fn is_in_runtime_context(&self) -> bool {
        Handle::try_current().is_ok()
    }
}

// Removed: block_on_search_operation - replaced with thread-safe alternatives
// that use block_on_operation with String/primitive return types instead of jobject

/// Convenience function for generic async operations
pub fn block_on_operation<F, T>(future: F) -> T
where
    F: Future<Output = T> + Send + 'static,
    T: Send + 'static,
{
    QuickwitRuntimeManager::global().block_on(future)
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

    #[test]
    fn test_runtime_context_detection() {
        let manager = QuickwitRuntimeManager::global();

        // Should not be in runtime context initially
        assert!(!manager.is_in_runtime_context());

        // This test can't easily test the positive case without more complex setup
    }
}
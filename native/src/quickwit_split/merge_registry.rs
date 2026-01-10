// merge_registry.rs - Merge ID generation and temp directory registry
// Extracted from mod.rs during refactoring
// Contains: collision-resistant merge ID generation, temp directory registry, mmap file handle

use std::cell::Cell;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::sync::LazyLock;
use std::sync::atomic::{AtomicU64, Ordering};
use tempfile as temp;
use anyhow::{anyhow, Result};
use tantivy::directory::FileHandle;

use crate::debug_println;

// Debug logging macro - controlled by TANTIVY4JAVA_DEBUG environment variable
macro_rules! debug_log {
    ($($arg:tt)*) => {
        debug_println!("DEBUG: {}", format!($($arg)*))
    };
}

// ✅ REENTRANCY FIX: Enhanced merge ID generation system to prevent collisions
// Global atomic counter for merge operations (process-wide uniqueness)
pub(crate) static GLOBAL_MERGE_COUNTER: AtomicU64 = AtomicU64::new(0);

// Thread-local counter for additional collision resistance within threads
thread_local! {
    pub(crate) static THREAD_MERGE_COUNTER: Cell<u64> = Cell::new(0);
}

// Registry state tracking for better collision detection and debugging
#[derive(Debug, Clone)]
pub(crate) struct RegistryEntry {
    pub temp_dir: Arc<temp::TempDir>,
    pub created_at: std::time::Instant,
    pub merge_id: String,
    pub operation_type: String,
}

// Enhanced registry with state tracking for collision detection
pub(crate) static TEMP_DIR_REGISTRY: LazyLock<Mutex<HashMap<String, RegistryEntry>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

// Global semaphore for concurrent downloads across ALL merge operations
// This prevents overwhelming the system when multiple merges run simultaneously
pub(crate) static GLOBAL_DOWNLOAD_SEMAPHORE: LazyLock<tokio::sync::Semaphore> = LazyLock::new(|| {
    let max_global_downloads = std::env::var("TANTIVY4JAVA_MAX_DOWNLOADS")
        .ok()
        .and_then(|s| s.parse::<usize>().ok())
        .unwrap_or_else(|| {
            // Default to reasonable parallelism across the process (4-16 permits)
            num_cpus::get().clamp(4, 8) * 2
        });

    tokio::sync::Semaphore::new(max_global_downloads)
});

/// Wrapper around Mmap that implements StableDeref for use with OwnedBytes
///
/// SAFETY: Mmap's backing memory remains at a stable address for its entire lifetime.
/// The pointer returned by Deref remains valid and stable as long as the Mmap exists.
struct StableMmap(memmap2::Mmap);

impl std::ops::Deref for StableMmap {
    type Target = [u8];
    fn deref(&self) -> &[u8] {
        &self.0
    }
}

// SAFETY: Mmap guarantees that the memory address remains stable for its entire lifetime.
// The kernel maps the file into a fixed virtual address range that doesn't move.
unsafe impl stable_deref_trait::StableDeref for StableMmap {}

// Debug impl for StableMmap
impl std::fmt::Debug for StableMmap {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StableMmap")
            .field("len", &self.0.len())
            .finish()
    }
}

/// Memory-efficient FileHandle implementation that provides lazy access to memory-mapped files
/// This avoids loading the entire file into a Vec, significantly reducing memory usage
///
/// ✅ MEMORY OPTIMIZATION: Returns OwnedBytes that directly reference the mmap without copying.
/// For a 7GB store segment, this means ZERO heap allocation instead of 7GB.
#[derive(Debug)]
pub(crate) struct MmapFileHandle {
    /// The memory-mapped file wrapped in OwnedBytes for zero-copy slicing
    owned_bytes: tantivy::directory::OwnedBytes,
}

impl MmapFileHandle {
    /// Create a new MmapFileHandle from a Mmap
    /// The OwnedBytes wraps the mmap directly - no data is copied
    pub fn new(mmap: memmap2::Mmap) -> Self {
        // Wrap in StableMmap to satisfy StableDeref trait requirement
        let stable_mmap = StableMmap(mmap);
        // OwnedBytes::new takes ownership of the StableMmap without copying any data
        // The Mmap stays alive inside OwnedBytes as long as this handle exists
        let owned_bytes = tantivy::directory::OwnedBytes::new(stable_mmap);
        Self { owned_bytes }
    }
}

impl tantivy::HasLen for MmapFileHandle {
    fn len(&self) -> usize {
        self.owned_bytes.len()
    }
}

impl FileHandle for MmapFileHandle {
    fn read_bytes(&self, range: std::ops::Range<usize>) -> std::io::Result<tantivy::directory::OwnedBytes> {
        if range.end > self.owned_bytes.len() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!("Range end {} exceeds file size {}", range.end, self.owned_bytes.len()),
            ));
        }

        // ✅ ZERO-COPY: OwnedBytes::slice returns a view into the mmap without copying
        // This is the critical fix - for a 7GB range, NO heap allocation occurs
        Ok(self.owned_bytes.slice(range))
    }
}

/// ✅ REENTRANCY FIX: Generate collision-resistant merge ID
/// Uses multiple entropy sources to prevent merge ID collisions in high-concurrency scenarios
pub fn generate_collision_resistant_merge_id() -> String {
    // Get global atomic counter (process-wide uniqueness)
    let global_counter = GLOBAL_MERGE_COUNTER.fetch_add(1, Ordering::SeqCst);

    // Get thread-local counter (thread-specific uniqueness)
    let thread_counter = THREAD_MERGE_COUNTER.with(|c| {
        let current = c.get();
        c.set(current.wrapping_add(1));
        current
    });

    // Get current thread info
    let thread_id = format!("{:?}", std::thread::current().id())
        .replace("ThreadId(", "").replace(")", "");

    // Get high-resolution timestamp
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);

    // Get memory address for additional uniqueness (different per call)
    let stack_addr = &thread_counter as *const u64 as usize;

    // Generate cryptographically random UUID
    let uuid = uuid::Uuid::new_v4();

    // Combine all entropy sources for maximum collision resistance
    format!("{}_{}_{}_{}_{}_{}_{}",
        std::process::id(),    // Process ID
        thread_id,            // Thread ID
        global_counter,       // Global atomic counter
        thread_counter,       // Thread-local counter
        nanos,               // Nanosecond timestamp
        stack_addr,          // Stack memory address
        uuid                 // Cryptographic UUID
    )
}

/// ✅ REENTRANCY FIX: Safely cleanup temporary directory with state validation
pub fn cleanup_temp_directory_safe(registry_key: &str) -> Result<bool> {
    let mut registry = TEMP_DIR_REGISTRY.lock().map_err(|e| {
        anyhow!("Failed to acquire registry lock for cleanup: {}", e)
    })?;

    if let Some(entry) = registry.remove(registry_key) {
        let ref_count = Arc::strong_count(&entry.temp_dir);
        let age = entry.created_at.elapsed();

        debug_log!("✅ REGISTRY CLEANUP: Removed temp directory: {} (merge_id: {}, operation: {}, age: {:?}, ref_count: {})",
                   registry_key, entry.merge_id, entry.operation_type, age, ref_count);

        // entry.temp_dir will be dropped here, automatic cleanup if ref_count == 1
        Ok(true)
    } else {
        debug_log!("⚠️ REGISTRY CLEANUP: Temp directory not found: {}", registry_key);
        Ok(false)
    }
}

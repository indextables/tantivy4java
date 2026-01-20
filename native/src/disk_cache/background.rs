// background.rs - Background writer and manifest sync
// Extracted from mod.rs during refactoring

use std::ops::Range;
use std::sync::Arc;
use std::time::Duration;

use crate::debug_println;

use super::L2DiskCache;

/// Background write request
pub(crate) enum WriteRequest {
    Put {
        storage_loc: String,
        split_id: String,
        component: String,
        byte_range: Option<Range<u64>>,
        data: Vec<u8>,
    },
    Evict {
        storage_loc: String,
        split_id: String,
    },
    SyncManifest,
    /// Flush all pending writes and signal completion via the oneshot channel
    Flush(tokio::sync::oneshot::Sender<()>),
    Shutdown,
}

impl L2DiskCache {
    /// Background writer thread with parallel async I/O
    ///
    /// Uses std::sync::mpsc::Receiver for bounded channel with backpressure.
    /// Spawns async write tasks on a tokio runtime with num_cpus worker threads.
    /// A semaphore limits concurrent writes to num_cpus for optimal parallelism.
    ///
    /// Backpressure flow:
    /// 1. Sender calls put() → sync_channel.send() blocks if queue full
    /// 2. This thread recv()s requests from the bounded queue
    /// 3. Before spawning write task, acquires semaphore permit
    /// 4. If all writers busy, blocks on semaphore → recv loop stalls
    /// 5. Bounded channel fills up → senders block
    pub(crate) fn background_writer_static(
        rx: std::sync::mpsc::Receiver<WriteRequest>,
        cache_weak: std::sync::Weak<Self>,
    ) {
        let num_writers = num_cpus::get();
        debug_println!("🚀 L2DiskCache: Starting {} parallel async writers", num_writers);

        // Create tokio runtime with num_cpus worker threads for parallel async I/O
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(num_writers)
            .thread_name("disk-cache-writer")
            .enable_all()
            .build()
            .expect("Failed to create tokio runtime for disk cache");

        // Semaphore limits concurrent writes to num_cpus
        let semaphore = Arc::new(tokio::sync::Semaphore::new(num_writers));

        // Process requests - blocking recv is fine since this is a dedicated thread
        while let Ok(req) = rx.recv() {
            let cache = match cache_weak.upgrade() {
                Some(c) => c,
                None => break,
            };

            match req {
                WriteRequest::Put {
                    storage_loc,
                    split_id,
                    component,
                    byte_range,
                    data,
                } => {
                    // Acquire permit BEFORE spawning - blocks if all writers busy
                    // This provides backpressure: when blocked here, recv loop stalls,
                    // bounded channel fills, and senders block on put()
                    let permit = match runtime.block_on(semaphore.clone().acquire_owned()) {
                        Ok(p) => p,
                        Err(_) => continue, // Semaphore closed
                    };

                    // Spawn async write task
                    runtime.spawn(async move {
                        match cache
                            .do_put_async(
                                &storage_loc,
                                &split_id,
                                &component,
                                byte_range.map(|r| r.start..r.end),
                                &data,
                            )
                            .await
                        {
                            Ok(()) => {
                                // Mark manifest dirty so timer will sync it
                                cache.mark_dirty();
                            }
                            Err(e) => {
                                debug_println!("Disk cache write error: {}", e);
                            }
                        }
                        drop(permit); // Release permit when write completes
                    });
                }
                WriteRequest::Evict {
                    storage_loc,
                    split_id,
                } => {
                    // Eviction can run in parallel too
                    let permit = match runtime.block_on(semaphore.clone().acquire_owned()) {
                        Ok(p) => p,
                        Err(_) => continue,
                    };

                    runtime.spawn(async move {
                        match cache.do_evict_async(&storage_loc, &split_id).await {
                            Ok(()) => {
                                cache.mark_dirty();
                            }
                            Err(e) => {
                                debug_println!("Disk cache evict error: {}", e);
                            }
                        }
                        drop(permit);
                    });
                }
                WriteRequest::SyncManifest => {
                    // Manifest sync is quick, run synchronously
                    if let Err(e) = cache.do_sync_manifest() {
                        debug_println!("Disk cache manifest sync error: {}", e);
                    }
                }
                WriteRequest::Flush(tx) => {
                    // Wait for ALL pending writes to complete by acquiring all permits
                    runtime.block_on(async {
                        let _all_permits = semaphore
                            .acquire_many(num_writers as u32)
                            .await
                            .expect("Semaphore closed during flush");

                        // Now all writes are complete, sync manifest
                        if let Err(e) = cache.do_sync_manifest() {
                            debug_println!("Disk cache manifest sync error during flush: {}", e);
                        }

                        // Signal completion
                        let _ = tx.send(());
                    });
                }
                WriteRequest::Shutdown => break,
            }
        }

        // Graceful shutdown: wait for all pending writes to complete
        runtime.block_on(async {
            let _ = semaphore.acquire_many(num_writers as u32).await;
        });
        debug_println!("🛑 L2DiskCache: Writer thread shutdown complete");
    }

    /// Manifest sync timer - checks every second, syncs if dirty (non-blocking)
    ///
    /// This replaces the old interval-based approach with a dirty-flag approach:
    /// - Checks every 1 second (fast response to changes)
    /// - Only syncs if manifest has uncommitted changes
    /// - Non-blocking: doesn't wait for pending writes
    pub(crate) fn manifest_sync_timer_static(
        cache_weak: std::sync::Weak<Self>,
        shutdown_flag: Arc<std::sync::atomic::AtomicBool>,
        dirty_flag: Arc<std::sync::atomic::AtomicBool>,
    ) {
        use std::sync::atomic::Ordering;

        loop {
            // Sleep for 1 second (check in 100ms increments for faster shutdown response)
            for _ in 0..10 {
                if shutdown_flag.load(Ordering::Relaxed) {
                    return;
                }
                std::thread::sleep(Duration::from_millis(100));
            }

            // Check if manifest is dirty
            if dirty_flag.swap(false, Ordering::AcqRel) {
                // Try to upgrade weak reference - if cache is dropped, exit
                if let Some(cache) = cache_weak.upgrade() {
                    debug_println!("📝 MANIFEST_SYNC: Dirty flag set, syncing manifest...");
                    if let Err(e) = cache.do_sync_manifest() {
                        debug_println!("Manifest sync timer error: {}", e);
                    } else {
                        debug_println!("✅ MANIFEST_SYNC: Manifest synced successfully");
                    }
                } else {
                    break;
                }
            }
        }
    }
}

// txlog/metrics.rs - Write retries, cache stats, timing

use std::sync::atomic::{AtomicU64, Ordering};

#[derive(Debug, Default)]
pub struct TxLogMetrics {
    // Write metrics
    pub write_attempts: AtomicU64,
    pub write_conflicts: AtomicU64,
    pub write_successes: AtomicU64,
    pub total_retry_delay_ms: AtomicU64,

    // Read metrics
    pub versions_read: AtomicU64,
    pub manifests_read: AtomicU64,
    pub file_entries_loaded: AtomicU64,

    // Timing
    pub last_list_files_ms: AtomicU64,
    pub last_write_ms: AtomicU64,
    pub last_checkpoint_ms: AtomicU64,
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct TxLogMetricsSnapshot {
    pub write_attempts: u64,
    pub write_conflicts: u64,
    pub write_successes: u64,
    pub avg_retry_delay_ms: f64,
    pub versions_read: u64,
    pub manifests_read: u64,
    pub file_entries_loaded: u64,
    pub last_list_files_ms: u64,
    pub last_write_ms: u64,
    pub last_checkpoint_ms: u64,
}

impl TxLogMetrics {
    pub fn snapshot(&self) -> TxLogMetricsSnapshot {
        let attempts = self.write_attempts.load(Ordering::Relaxed);
        let total_delay = self.total_retry_delay_ms.load(Ordering::Relaxed);
        let conflicts = self.write_conflicts.load(Ordering::Relaxed);
        TxLogMetricsSnapshot {
            write_attempts: attempts,
            write_conflicts: conflicts,
            write_successes: self.write_successes.load(Ordering::Relaxed),
            avg_retry_delay_ms: if conflicts > 0 { total_delay as f64 / conflicts as f64 } else { 0.0 },
            versions_read: self.versions_read.load(Ordering::Relaxed),
            manifests_read: self.manifests_read.load(Ordering::Relaxed),
            file_entries_loaded: self.file_entries_loaded.load(Ordering::Relaxed),
            last_list_files_ms: self.last_list_files_ms.load(Ordering::Relaxed),
            last_write_ms: self.last_write_ms.load(Ordering::Relaxed),
            last_checkpoint_ms: self.last_checkpoint_ms.load(Ordering::Relaxed),
        }
    }

    pub fn reset(&self) {
        self.write_attempts.store(0, Ordering::Relaxed);
        self.write_conflicts.store(0, Ordering::Relaxed);
        self.write_successes.store(0, Ordering::Relaxed);
        self.total_retry_delay_ms.store(0, Ordering::Relaxed);
        self.versions_read.store(0, Ordering::Relaxed);
        self.manifests_read.store(0, Ordering::Relaxed);
        self.file_entries_loaded.store(0, Ordering::Relaxed);
        self.last_list_files_ms.store(0, Ordering::Relaxed);
        self.last_write_ms.store(0, Ordering::Relaxed);
        self.last_checkpoint_ms.store(0, Ordering::Relaxed);
    }
}

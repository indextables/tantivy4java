// adaptive_tuning.rs - Adaptive Tuning Engine for Batch Optimization
//
// This module provides automatic parameter tuning based on observed batch performance.
// It monitors consolidation ratios and adjusts gap_tolerance to optimize for different workloads.
//
// Priority 5: Adaptive Tuning Implementation
//
// Key Features:
// - Monitors batch performance in real-time
// - Gradually adjusts parameters based on observed behavior
// - Learns optimal settings for specific workloads
// - Safety limits prevent extreme configurations

use std::collections::VecDeque;
use std::sync::{Arc, Mutex};
use crate::simple_batch_optimization::SimpleBatchConfig;
use crate::debug_println;

/// Safety limits for adaptive tuning
const MIN_GAP_TOLERANCE: usize = 64 * 1024;      // 64KB minimum
const MAX_GAP_TOLERANCE: usize = 2 * 1024 * 1024; // 2MB maximum
const MIN_MAX_RANGE_SIZE: usize = 2 * 1024 * 1024; // 2MB minimum
const MAX_MAX_RANGE_SIZE: usize = 32 * 1024 * 1024; // 32MB maximum

/// Target consolidation ratio ranges
const TARGET_CONSOLIDATION_MIN: f64 = 10.0;  // Aim for at least 10x consolidation
const TARGET_CONSOLIDATION_MAX: f64 = 50.0;  // Above 50x may be wasting bandwidth

/// Adjustment factors
const ADJUSTMENT_FACTOR_INCREASE: f64 = 1.15; // 15% increase when consolidation is low
const ADJUSTMENT_FACTOR_DECREASE: f64 = 0.92; // 8% decrease when consolidation is high

/// Maximum number of recent batches to track
const MAX_HISTORY_SIZE: usize = 20;

/// Batch performance metrics tracked by adaptive tuning
#[derive(Debug, Clone)]
pub struct BatchMetrics {
    /// Number of documents in batch
    pub document_count: usize,

    /// Number of consolidated ranges created
    pub consolidated_ranges: usize,

    /// Total bytes fetched
    pub bytes_fetched: usize,

    /// Total bytes actually used (document data)
    pub bytes_used: usize,

    /// Batch processing time in milliseconds
    pub latency_ms: u64,
}

impl BatchMetrics {
    /// Calculate consolidation ratio (documents per range)
    pub fn consolidation_ratio(&self) -> f64 {
        if self.consolidated_ranges == 0 {
            0.0
        } else {
            self.document_count as f64 / self.consolidated_ranges as f64
        }
    }

    /// Calculate waste ratio (unused data / total data)
    pub fn waste_ratio(&self) -> f64 {
        if self.bytes_fetched == 0 {
            0.0
        } else {
            let bytes_wasted = self.bytes_fetched.saturating_sub(self.bytes_used);
            bytes_wasted as f64 / self.bytes_fetched as f64
        }
    }
}

/// Adaptive tuning engine that automatically adjusts batch optimization parameters
pub struct AdaptiveTuningEngine {
    /// Current configuration (mutable via tuning)
    config: SimpleBatchConfig,

    /// History of recent batch performance
    recent_batches: VecDeque<BatchMetrics>,

    /// Whether adaptive tuning is enabled
    enabled: bool,

    /// Number of batches processed since last adjustment
    batches_since_adjustment: usize,

    /// Minimum batches before making adjustments
    min_batches_for_adjustment: usize,
}

impl AdaptiveTuningEngine {
    /// Create a new adaptive tuning engine with default configuration
    pub fn new() -> Self {
        Self {
            config: SimpleBatchConfig::default(),
            recent_batches: VecDeque::with_capacity(MAX_HISTORY_SIZE),
            enabled: true,
            batches_since_adjustment: 0,
            min_batches_for_adjustment: 5, // Need at least 5 batches to see patterns
        }
    }

    /// Create with specific initial configuration
    pub fn with_config(config: SimpleBatchConfig) -> Self {
        Self {
            config,
            recent_batches: VecDeque::with_capacity(MAX_HISTORY_SIZE),
            enabled: true,
            batches_since_adjustment: 0,
            min_batches_for_adjustment: 5,
        }
    }

    /// Enable or disable adaptive tuning
    pub fn set_enabled(&mut self, enabled: bool) {
        self.enabled = enabled;
        if enabled {
            debug_println!("[ADAPTIVE_TUNING] Adaptive tuning enabled");
        } else {
            debug_println!("[ADAPTIVE_TUNING] Adaptive tuning disabled");
        }
    }

    /// Check if adaptive tuning is enabled
    pub fn is_enabled(&self) -> bool {
        self.enabled
    }

    /// Get current configuration (possibly adjusted)
    pub fn config(&self) -> &SimpleBatchConfig {
        &self.config
    }

    /// Record batch performance metrics
    pub fn record_batch(&mut self, metrics: BatchMetrics) {
        debug_println!(
            "[ADAPTIVE_TUNING] Recording batch: {} docs, {} ranges, ratio: {:.1}x, waste: {:.1}%",
            metrics.document_count,
            metrics.consolidated_ranges,
            metrics.consolidation_ratio(),
            metrics.waste_ratio() * 100.0
        );

        // Add to history (maintain max size)
        if self.recent_batches.len() >= MAX_HISTORY_SIZE {
            self.recent_batches.pop_front();
        }
        self.recent_batches.push_back(metrics);

        // Increment counter
        self.batches_since_adjustment += 1;

        // Consider adjustments if enabled
        if self.enabled && self.batches_since_adjustment >= self.min_batches_for_adjustment {
            self.consider_adjustments();
            self.batches_since_adjustment = 0;
        }
    }

    /// Calculate average consolidation ratio from recent batches
    fn calculate_avg_consolidation(&self) -> f64 {
        if self.recent_batches.is_empty() {
            return 0.0;
        }

        let sum: f64 = self.recent_batches
            .iter()
            .map(|m| m.consolidation_ratio())
            .sum();

        sum / self.recent_batches.len() as f64
    }

    /// Calculate average waste ratio from recent batches
    fn calculate_avg_waste(&self) -> f64 {
        if self.recent_batches.is_empty() {
            return 0.0;
        }

        let sum: f64 = self.recent_batches
            .iter()
            .map(|m| m.waste_ratio())
            .sum();

        sum / self.recent_batches.len() as f64
    }

    /// Consider whether to adjust parameters based on recent performance
    fn consider_adjustments(&mut self) {
        let avg_consolidation = self.calculate_avg_consolidation();
        let avg_waste = self.calculate_avg_waste();

        debug_println!(
            "[ADAPTIVE_TUNING] Performance check: avg_consolidation={:.1}x, avg_waste={:.1}%, gap_tolerance={}KB",
            avg_consolidation,
            avg_waste * 100.0,
            self.config.gap_tolerance / 1024
        );

        // Low consolidation → increase gap tolerance to consolidate more
        if avg_consolidation < TARGET_CONSOLIDATION_MIN {
            let old_gap = self.config.gap_tolerance;
            self.config.gap_tolerance =
                ((self.config.gap_tolerance as f64) * ADJUSTMENT_FACTOR_INCREASE) as usize;
            self.config.gap_tolerance = self.config.gap_tolerance.clamp(
                MIN_GAP_TOLERANCE,
                MAX_GAP_TOLERANCE
            );

            if self.config.gap_tolerance != old_gap {
                debug_println!(
                    "[ADAPTIVE_TUNING] Low consolidation ({:.1}x < {:.1}x target): increasing gap_tolerance from {}KB to {}KB",
                    avg_consolidation,
                    TARGET_CONSOLIDATION_MIN,
                    old_gap / 1024,
                    self.config.gap_tolerance / 1024
                );
            }
        }
        // High consolidation AND high waste → decrease gap tolerance to reduce waste
        else if avg_consolidation > TARGET_CONSOLIDATION_MAX && avg_waste > 0.15 {
            let old_gap = self.config.gap_tolerance;
            self.config.gap_tolerance =
                ((self.config.gap_tolerance as f64) * ADJUSTMENT_FACTOR_DECREASE) as usize;
            self.config.gap_tolerance = self.config.gap_tolerance.clamp(
                MIN_GAP_TOLERANCE,
                MAX_GAP_TOLERANCE
            );

            if self.config.gap_tolerance != old_gap {
                debug_println!(
                    "[ADAPTIVE_TUNING] High consolidation ({:.1}x > {:.1}x) with high waste ({:.1}%): decreasing gap_tolerance from {}KB to {}KB",
                    avg_consolidation,
                    TARGET_CONSOLIDATION_MAX,
                    avg_waste * 100.0,
                    old_gap / 1024,
                    self.config.gap_tolerance / 1024
                );
            }
        }

        // Adjust max_range_size based on consolidation patterns
        self.adjust_max_range_size(avg_consolidation);
    }

    /// Adjust maximum range size based on consolidation patterns
    fn adjust_max_range_size(&mut self, avg_consolidation: f64) {
        let old_max_range = self.config.max_range_size;

        // If we're consolidating well but ranges are small, we can increase max_range_size
        if avg_consolidation > TARGET_CONSOLIDATION_MIN * 1.5 {
            self.config.max_range_size =
                ((self.config.max_range_size as f64) * 1.1) as usize;
            self.config.max_range_size = self.config.max_range_size.clamp(
                MIN_MAX_RANGE_SIZE,
                MAX_MAX_RANGE_SIZE
            );

            if self.config.max_range_size != old_max_range {
                debug_println!(
                    "[ADAPTIVE_TUNING] Good consolidation ({:.1}x): increasing max_range_size from {}MB to {}MB",
                    avg_consolidation,
                    old_max_range / (1024 * 1024),
                    self.config.max_range_size / (1024 * 1024)
                );
            }
        }
        // If consolidation is poor, decrease max_range_size to reduce memory pressure
        else if avg_consolidation < TARGET_CONSOLIDATION_MIN * 0.5 {
            self.config.max_range_size =
                ((self.config.max_range_size as f64) * 0.9) as usize;
            self.config.max_range_size = self.config.max_range_size.clamp(
                MIN_MAX_RANGE_SIZE,
                MAX_MAX_RANGE_SIZE
            );

            if self.config.max_range_size != old_max_range {
                debug_println!(
                    "[ADAPTIVE_TUNING] Poor consolidation ({:.1}x): decreasing max_range_size from {}MB to {}MB",
                    avg_consolidation,
                    old_max_range / (1024 * 1024),
                    self.config.max_range_size / (1024 * 1024)
                );
            }
        }
    }

    /// Get summary statistics for debugging/monitoring
    pub fn get_stats(&self) -> AdaptiveStats {
        AdaptiveStats {
            enabled: self.enabled,
            batches_tracked: self.recent_batches.len(),
            avg_consolidation: self.calculate_avg_consolidation(),
            avg_waste: self.calculate_avg_waste(),
            current_gap_tolerance: self.config.gap_tolerance,
            current_max_range_size: self.config.max_range_size,
        }
    }

    /// Reset adaptive tuning history (useful for testing or major workload changes)
    pub fn reset(&mut self) {
        self.recent_batches.clear();
        self.batches_since_adjustment = 0;
        debug_println!("[ADAPTIVE_TUNING] Adaptive tuning history reset");
    }
}

/// Statistics snapshot for adaptive tuning
#[derive(Debug, Clone)]
pub struct AdaptiveStats {
    pub enabled: bool,
    pub batches_tracked: usize,
    pub avg_consolidation: f64,
    pub avg_waste: f64,
    pub current_gap_tolerance: usize,
    pub current_max_range_size: usize,
}

impl AdaptiveStats {
    /// Format as human-readable string
    pub fn to_string(&self) -> String {
        format!(
            "AdaptiveStats {{ enabled: {}, batches: {}, avg_consolidation: {:.1}x, avg_waste: {:.1}%, gap_tolerance: {}KB, max_range: {}MB }}",
            self.enabled,
            self.batches_tracked,
            self.avg_consolidation,
            self.avg_waste * 100.0,
            self.current_gap_tolerance / 1024,
            self.current_max_range_size / (1024 * 1024)
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_adaptive_tuning_increases_gap_tolerance_on_low_consolidation() {
        let mut engine = AdaptiveTuningEngine::new();
        let initial_gap = engine.config().gap_tolerance;

        // Record several batches with low consolidation (2x ratio)
        for _ in 0..5 {
            engine.record_batch(BatchMetrics {
                document_count: 100,
                consolidated_ranges: 50,  // 2x consolidation
                bytes_fetched: 1_000_000,
                bytes_used: 900_000,
                latency_ms: 100,
            });
        }

        // Gap tolerance should have increased
        assert!(engine.config().gap_tolerance > initial_gap,
            "Gap tolerance should increase when consolidation is low");
    }

    #[test]
    fn test_adaptive_tuning_decreases_gap_tolerance_on_high_waste() {
        let mut engine = AdaptiveTuningEngine::new();
        let initial_gap = engine.config().gap_tolerance;

        // Record several batches with high consolidation (60x) but high waste (20%)
        for _ in 0..5 {
            engine.record_batch(BatchMetrics {
                document_count: 600,
                consolidated_ranges: 10,  // 60x consolidation
                bytes_fetched: 10_000_000,
                bytes_used: 8_000_000,    // 20% waste
                latency_ms: 100,
            });
        }

        // Gap tolerance should have decreased
        assert!(engine.config().gap_tolerance < initial_gap,
            "Gap tolerance should decrease when consolidation is high with waste");
    }

    #[test]
    fn test_safety_limits_enforced() {
        let mut engine = AdaptiveTuningEngine::new();

        // Try to push gap tolerance very high
        for _ in 0..50 {
            engine.record_batch(BatchMetrics {
                document_count: 100,
                consolidated_ranges: 100,  // 1x consolidation (very low)
                bytes_fetched: 1_000_000,
                bytes_used: 900_000,
                latency_ms: 100,
            });
        }

        // Should be clamped to maximum
        assert!(engine.config().gap_tolerance <= MAX_GAP_TOLERANCE,
            "Gap tolerance should not exceed maximum safety limit");
    }

    #[test]
    fn test_can_disable_adaptive_tuning() {
        let mut engine = AdaptiveTuningEngine::new();
        let initial_gap = engine.config().gap_tolerance;

        // Disable adaptive tuning
        engine.set_enabled(false);
        assert!(!engine.is_enabled());

        // Record batches with low consolidation
        for _ in 0..10 {
            engine.record_batch(BatchMetrics {
                document_count: 100,
                consolidated_ranges: 50,
                bytes_fetched: 1_000_000,
                bytes_used: 900_000,
                latency_ms: 100,
            });
        }

        // Gap tolerance should NOT have changed
        assert_eq!(engine.config().gap_tolerance, initial_gap,
            "Gap tolerance should not change when adaptive tuning is disabled");
    }
}

// ============================================================================
// JNI Integration - Expose Adaptive Tuning to Java
// ============================================================================

use jni::JNIEnv;
use jni::objects::{JClass, JObject};
use jni::sys::{jlong, jboolean, jint, jdouble};
use std::sync::Mutex as StdMutex;

// Global registry for adaptive tuning engines
lazy_static::lazy_static! {
    static ref ADAPTIVE_ENGINES: StdMutex<Vec<Arc<StdMutex<AdaptiveTuningEngine>>>> =
        StdMutex::new(Vec::new());
}

/// Create a new adaptive tuning engine
#[no_mangle]
pub extern "system" fn Java_io_indextables_tantivy4java_split_AdaptiveTuning_nativeCreate(
    _env: JNIEnv,
    _class: JClass,
    enabled: jboolean,
    min_batches_for_adjustment: jint,
) -> jlong {
    let mut engine = AdaptiveTuningEngine::new();
    engine.set_enabled(enabled != 0);

    if min_batches_for_adjustment > 0 {
        engine.min_batches_for_adjustment = min_batches_for_adjustment as usize;
    }

    let arc = Arc::new(StdMutex::new(engine));
    let ptr = Arc::as_ptr(&arc) as jlong;

    // Store in registry
    let mut engines = ADAPTIVE_ENGINES.lock().unwrap();
    engines.push(arc);

    debug_println!("[ADAPTIVE_JNI] Created adaptive tuning engine at ptr={}", ptr);
    ptr
}

/// Record batch performance metrics
#[no_mangle]
pub extern "system" fn Java_io_indextables_tantivy4java_split_AdaptiveTuning_nativeRecordBatch(
    _env: JNIEnv,
    _class: JClass,
    ptr: jlong,
    document_count: jint,
    consolidated_ranges: jint,
    bytes_fetched: jlong,
    bytes_used: jlong,
    latency_ms: jlong,
) {
    let engines = ADAPTIVE_ENGINES.lock().unwrap();
    if let Some(arc) = engines.iter().find(|e| Arc::as_ptr(e) as jlong == ptr) {
        let mut engine = arc.lock().unwrap();

        let metrics = BatchMetrics {
            document_count: document_count as usize,
            consolidated_ranges: consolidated_ranges as usize,
            bytes_fetched: bytes_fetched as usize,
            bytes_used: bytes_used as usize,
            latency_ms: latency_ms as u64,
        };

        engine.record_batch(metrics);
    }
}

/// Get adaptive tuning statistics
#[no_mangle]
pub extern "system" fn Java_io_indextables_tantivy4java_split_AdaptiveTuning_nativeGetStats<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass,
    ptr: jlong,
) -> JObject<'local> {
    let engines = ADAPTIVE_ENGINES.lock().unwrap();
    if let Some(arc) = engines.iter().find(|e| Arc::as_ptr(e) as jlong == ptr) {
        let engine = arc.lock().unwrap();
        let stats = engine.get_stats();

        // Create AdaptiveTuningStats Java object
        let stats_class = env.find_class("io/indextables/tantivy4java/split/AdaptiveTuningStats")
            .expect("Failed to find AdaptiveTuningStats class");

        let stats_obj = env.new_object(
            stats_class,
            "(ZIDDDJJ)V",
            &[
                (stats.enabled as jboolean).into(),
                (stats.batches_tracked as jint).into(),
                stats.avg_consolidation.into(),
                stats.avg_waste.into(),
                (stats.current_gap_tolerance as jlong).into(),
                (stats.current_max_range_size as jlong).into(),
            ],
        ).expect("Failed to create AdaptiveTuningStats object");

        return stats_obj;
    }

    // Return null if engine not found
    JObject::null()
}

/// Enable or disable adaptive tuning
#[no_mangle]
pub extern "system" fn Java_io_indextables_tantivy4java_split_AdaptiveTuning_nativeSetEnabled(
    _env: JNIEnv,
    _class: JClass,
    ptr: jlong,
    enabled: jboolean,
) {
    let engines = ADAPTIVE_ENGINES.lock().unwrap();
    if let Some(arc) = engines.iter().find(|e| Arc::as_ptr(e) as jlong == ptr) {
        let mut engine = arc.lock().unwrap();
        engine.set_enabled(enabled != 0);
    }
}

/// Check if adaptive tuning is enabled
#[no_mangle]
pub extern "system" fn Java_io_indextables_tantivy4java_split_AdaptiveTuning_nativeIsEnabled(
    _env: JNIEnv,
    _class: JClass,
    ptr: jlong,
) -> jboolean {
    let engines = ADAPTIVE_ENGINES.lock().unwrap();
    if let Some(arc) = engines.iter().find(|e| Arc::as_ptr(e) as jlong == ptr) {
        let engine = arc.lock().unwrap();
        return engine.is_enabled() as jboolean;
    }
    0
}

/// Get current gap tolerance
#[no_mangle]
pub extern "system" fn Java_io_indextables_tantivy4java_split_AdaptiveTuning_nativeGetGapTolerance(
    _env: JNIEnv,
    _class: JClass,
    ptr: jlong,
) -> jlong {
    let engines = ADAPTIVE_ENGINES.lock().unwrap();
    if let Some(arc) = engines.iter().find(|e| Arc::as_ptr(e) as jlong == ptr) {
        let engine = arc.lock().unwrap();
        return engine.config().gap_tolerance as jlong;
    }
    0
}

/// Get current max range size
#[no_mangle]
pub extern "system" fn Java_io_indextables_tantivy4java_split_AdaptiveTuning_nativeGetMaxRangeSize(
    _env: JNIEnv,
    _class: JClass,
    ptr: jlong,
) -> jlong {
    let engines = ADAPTIVE_ENGINES.lock().unwrap();
    if let Some(arc) = engines.iter().find(|e| Arc::as_ptr(e) as jlong == ptr) {
        let engine = arc.lock().unwrap();
        return engine.config().max_range_size as jlong;
    }
    0
}

/// Reset adaptive tuning history
#[no_mangle]
pub extern "system" fn Java_io_indextables_tantivy4java_split_AdaptiveTuning_nativeReset(
    _env: JNIEnv,
    _class: JClass,
    ptr: jlong,
) {
    let engines = ADAPTIVE_ENGINES.lock().unwrap();
    if let Some(arc) = engines.iter().find(|e| Arc::as_ptr(e) as jlong == ptr) {
        let mut engine = arc.lock().unwrap();
        engine.reset();
    }
}

/// Destroy adaptive tuning engine and free resources
#[no_mangle]
pub extern "system" fn Java_io_indextables_tantivy4java_split_AdaptiveTuning_nativeDestroy(
    _env: JNIEnv,
    _class: JClass,
    ptr: jlong,
) {
    let mut engines = ADAPTIVE_ENGINES.lock().unwrap();
    engines.retain(|arc| Arc::as_ptr(arc) as jlong != ptr);
    debug_println!("[ADAPTIVE_JNI] Destroyed adaptive tuning engine at ptr={}", ptr);
}

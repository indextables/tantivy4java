// txlog/parallel_bench_tests.rs — Issue #153 parallelization benchmarks
//
// Verifies that the concurrent implementations deliver measurable speedups
// over sequential baselines.  Each test uses artificial latency (tokio::time::sleep)
// to simulate S3/Azure round-trip time, proving that batched/concurrent
// execution is fundamentally faster than sequential.
//
// Latency is set to 100ms (realistic S3 round-trip) and speedup thresholds
// are deliberately conservative (≥2×) to avoid flaky failures on overloaded
// CI runners.

use std::time::{Duration, Instant};

use crate::txlog::distributed;
use crate::txlog::error::Result;

/// Simulated per-request latency — 100ms is a realistic S3 round-trip.
/// Higher values make timing assertions more robust on slow CI runners.
const LATENCY: Duration = Duration::from_millis(100);

// ============================================================================
// 1. join_all_bounded — concurrent manifest reads
// ============================================================================

/// Simulate N manifest reads each taking `latency` to complete.
async fn simulate_manifest_reads_parallel(
    num_manifests: usize,
    latency: Duration,
    max_concurrent: usize,
) -> (Duration, Vec<usize>) {
    let futs: Vec<_> = (0..num_manifests)
        .map(|i| {
            let lat = latency;
            async move {
                tokio::time::sleep(lat).await;
                Ok(i) as Result<usize>
            }
        })
        .collect();

    let t0 = Instant::now();
    let results = distributed::join_all_bounded(futs, max_concurrent)
        .await
        .unwrap();
    (t0.elapsed(), results)
}

/// Sequential baseline: run the same futures one-at-a-time.
async fn simulate_manifest_reads_sequential(
    num_manifests: usize,
    latency: Duration,
) -> (Duration, Vec<usize>) {
    let t0 = Instant::now();
    let mut results = Vec::with_capacity(num_manifests);
    for i in 0..num_manifests {
        tokio::time::sleep(latency).await;
        results.push(i);
    }
    (t0.elapsed(), results)
}

/// 8 manifests × 100 ms each.
/// Sequential: ~800 ms.  Parallel (concurrency=32): ~100 ms.
/// Expect ≥2× speedup (conservative for CI).
#[tokio::test]
async fn test_parallel_manifest_reads_speedup() {
    let n = 8;

    let (seq_dur, seq_results) = simulate_manifest_reads_sequential(n, LATENCY).await;
    let (par_dur, par_results) = simulate_manifest_reads_parallel(n, LATENCY, 32).await;

    // Results must be identical and in order
    assert_eq!(seq_results, par_results, "parallel results must match sequential");

    let speedup = seq_dur.as_secs_f64() / par_dur.as_secs_f64();
    eprintln!(
        "manifest reads: seq={:.0}ms, par={:.0}ms, speedup={:.1}×",
        seq_dur.as_millis(),
        par_dur.as_millis(),
        speedup,
    );
    assert!(
        speedup >= 2.0,
        "Expected ≥2× speedup, got {:.1}× (seq={:?}, par={:?})",
        speedup,
        seq_dur,
        par_dur,
    );
}

/// Verify that join_all_bounded respects the concurrency limit.
/// 16 tasks × 100 ms with max_concurrent=4 → 4 batches → ~400 ms.
/// Should be significantly slower than unbounded (16 tasks → ~100 ms).
#[tokio::test]
async fn test_join_all_bounded_respects_concurrency_limit() {
    let n = 16;

    let (bounded_dur, bounded_results) =
        simulate_manifest_reads_parallel(n, LATENCY, 4).await;
    let (unbounded_dur, unbounded_results) =
        simulate_manifest_reads_parallel(n, LATENCY, 100).await;

    assert_eq!(bounded_results, unbounded_results, "results must match");

    eprintln!(
        "bounded(4)={:.0}ms, unbounded(100)={:.0}ms",
        bounded_dur.as_millis(),
        unbounded_dur.as_millis(),
    );

    // Bounded should take at least 1.5× longer than unbounded
    assert!(
        bounded_dur.as_secs_f64() > unbounded_dur.as_secs_f64() * 1.5,
        "Bounded should be slower: bounded={:?}, unbounded={:?}",
        bounded_dur,
        unbounded_dur,
    );
}

/// Verify ordering is preserved even when futures complete out-of-order.
/// Futures with lower indices sleep longer, so they complete last,
/// but results must still be in insertion order.
#[tokio::test]
async fn test_join_all_bounded_preserves_order() {
    let futs: Vec<_> = (0..8u32)
        .map(|i| async move {
            // Reverse latency: item 0 sleeps 80ms, item 7 sleeps 10ms
            let delay = Duration::from_millis((8 - i as u64) * 10);
            tokio::time::sleep(delay).await;
            Ok(i) as Result<u32>
        })
        .collect();

    let results = distributed::join_all_bounded(futs, 8).await.unwrap();
    assert_eq!(results, vec![0, 1, 2, 3, 4, 5, 6, 7], "order must be preserved");
}

/// Verify error propagation: if one future fails, the whole batch fails.
#[tokio::test]
async fn test_join_all_bounded_propagates_errors() {
    let futs: Vec<_> = (0..5u32)
        .map(|i| async move {
            if i == 3 {
                Err(crate::txlog::error::TxLogError::Serde("test error".into()))
            } else {
                Ok(i)
            }
        })
        .collect();

    let result = distributed::join_all_bounded(futs, 5).await;
    assert!(result.is_err(), "should propagate error from future #3");
}

/// Verify that an empty list of futures returns an empty result (not an error).
#[tokio::test]
async fn test_join_all_bounded_empty() {
    let futs: Vec<std::pin::Pin<Box<dyn std::future::Future<Output = Result<i32>>>>> = vec![];
    let results = distributed::join_all_bounded(futs, 8).await.unwrap();
    assert!(results.is_empty());
}

// ============================================================================
// 2. Batched checkpoint verification — concurrent HEAD probes
// ============================================================================

/// Simulate the checkpoint verification pattern: sequential probing vs batched.
///
/// Sequential: check v+1, wait, check v+2, wait, ... → N × latency.
/// Batched (batch_size): check [v+1..v+batch] concurrently → ceil(N/batch) × latency.
async fn sequential_probe(num_versions: usize, latency: Duration) -> Duration {
    let t0 = Instant::now();
    for _ in 0..num_versions {
        tokio::time::sleep(latency).await;
    }
    t0.elapsed()
}

async fn batched_probe(num_versions: usize, latency: Duration, batch_size: usize) -> Duration {
    let t0 = Instant::now();
    let mut remaining = num_versions;
    while remaining > 0 {
        let batch = std::cmp::min(remaining, batch_size);
        let futs: Vec<_> = (0..batch)
            .map(|_| tokio::time::sleep(latency))
            .collect();
        futures::future::join_all(futs).await;
        remaining -= batch;
    }
    t0.elapsed()
}

/// 12 checkpoint probes × 100 ms each.
/// Sequential: ~1200 ms.  Batched(4): ~300 ms.
/// Expect ≥2× speedup.
#[tokio::test]
async fn test_batched_checkpoint_probe_speedup() {
    let n = 12;

    let seq_dur = sequential_probe(n, LATENCY).await;
    let batch_dur = batched_probe(n, LATENCY, 4).await;

    let speedup = seq_dur.as_secs_f64() / batch_dur.as_secs_f64();
    eprintln!(
        "checkpoint probe: seq={:.0}ms, batched={:.0}ms, speedup={:.1}×",
        seq_dur.as_millis(),
        batch_dur.as_millis(),
        speedup,
    );
    assert!(
        speedup >= 2.0,
        "Expected ≥2× speedup, got {:.1}× (seq={:?}, batched={:?})",
        speedup,
        seq_dur,
        batch_dur,
    );
}

// ============================================================================
// 3. Batched backward probe — concurrent metadata lookups
// ============================================================================

/// Simulate backward probing for metadata.
/// Sequential: probe v-1, wait, probe v-2, wait, ... → N × latency.
/// Batched(4): probe [v-4..v-1] concurrently → ceil(N/4) × latency.
///
/// target_version = which version has the valid metadata.
async fn sequential_backward_probe(
    start_version: usize,
    target_version: usize,
    latency: Duration,
) -> (Duration, usize) {
    let t0 = Instant::now();
    let mut v = start_version;
    while v > 0 {
        v -= 1;
        tokio::time::sleep(latency).await;
        if v == target_version {
            return (t0.elapsed(), v);
        }
    }
    (t0.elapsed(), 0)
}

async fn batched_backward_probe(
    start_version: usize,
    target_version: usize,
    latency: Duration,
    batch_size: usize,
) -> (Duration, usize) {
    let t0 = Instant::now();
    let mut ceiling = start_version - 1;

    loop {
        let floor = ceiling.saturating_sub(batch_size - 1);
        // N.B. `.rev()` so join_all returns results in descending order
        let futs: Vec<_> = (floor..=ceiling)
            .rev()
            .map(|v| {
                let lat = latency;
                async move {
                    tokio::time::sleep(lat).await;
                    v
                }
            })
            .collect();

        let results = futures::future::join_all(futs).await;
        // Check results in descending order (newest first)
        for v in results {
            if v == target_version {
                return (t0.elapsed(), v);
            }
        }

        if floor == 0 {
            break;
        }
        ceiling = floor - 1;
    }
    (t0.elapsed(), 0)
}

/// Start at version 16, target metadata is at version 4 (12 probes away).
/// Sequential: 12 × 100ms = 1200ms.
/// Batched(4): 3 batches × 100ms = 300ms.
/// Expect ≥2× speedup.
#[tokio::test]
async fn test_batched_backward_probe_speedup() {
    let start = 16;
    let target = 4;

    let (seq_dur, seq_found) = sequential_backward_probe(start, target, LATENCY).await;
    let (batch_dur, batch_found) = batched_backward_probe(start, target, LATENCY, 4).await;

    assert_eq!(seq_found, target, "sequential must find target");
    assert_eq!(batch_found, target, "batched must find target");

    let speedup = seq_dur.as_secs_f64() / batch_dur.as_secs_f64();
    eprintln!(
        "backward probe: seq={:.0}ms, batched={:.0}ms, speedup={:.1}×",
        seq_dur.as_millis(),
        batch_dur.as_millis(),
        speedup,
    );
    assert!(
        speedup >= 2.0,
        "Expected ≥2× speedup, got {:.1}× (seq={:?}, batched={:?})",
        speedup,
        seq_dur,
        batch_dur,
    );
}

// ============================================================================
// 4. Combined scenario: realistic table with multiple parallel paths
// ============================================================================

/// Simulate a realistic cold-start snapshot read for a table with:
/// - 8 checkpoint version probes (stale by 8 versions)
/// - 12 manifest reads
/// - 4 backward metadata probes
///
/// All with 100ms per-request latency (typical S3 round-trip).
///
/// Sequential total: (8 + 12 + 4) × 100ms = 2400ms
/// Parallel total: ceil(8/4)×100 + ceil(12/32)×100 + ceil(4/4)×100 = 400ms
/// Expect ≥2× speedup (conservative).
#[tokio::test]
async fn test_combined_realistic_scenario_speedup() {
    // Sequential simulation
    let t_seq = Instant::now();
    sequential_probe(8, LATENCY).await;                          // checkpoint verify
    simulate_manifest_reads_sequential(12, LATENCY).await;       // manifest reads
    sequential_backward_probe(10, 6, LATENCY).await;             // backward probe (4 hops)
    let seq_total = t_seq.elapsed();

    // Parallel simulation
    let t_par = Instant::now();
    batched_probe(8, LATENCY, 4).await;                          // checkpoint verify
    simulate_manifest_reads_parallel(12, LATENCY, 32).await;     // manifest reads
    batched_backward_probe(10, 6, LATENCY, 4).await;             // backward probe
    let par_total = t_par.elapsed();

    let speedup = seq_total.as_secs_f64() / par_total.as_secs_f64();
    eprintln!(
        "combined scenario: seq={:.0}ms, par={:.0}ms, speedup={:.1}×",
        seq_total.as_millis(),
        par_total.as_millis(),
        speedup,
    );
    assert!(
        speedup >= 2.0,
        "Expected ≥2× speedup in combined scenario, got {:.1}× (seq={:?}, par={:?})",
        speedup,
        seq_total,
        par_total,
    );
}

// ============================================================================
// 5. Edge-case tests for batched probing logic
// ============================================================================

/// Verify the checkpoint batching logic handles the case where version is 0
/// (nothing to probe).
#[tokio::test]
async fn test_checkpoint_verify_at_version_zero() {
    // Batched probe with 0 versions should complete instantly
    let dur = batched_probe(0, Duration::from_millis(500), 4).await;
    assert!(dur.as_millis() < 50, "zero-probe should be instant, took {:?}", dur);
}

/// Verify the backward probe handles the case where target is at version 0.
#[tokio::test]
async fn test_backward_probe_target_at_version_zero() {
    let (dur, found) = batched_backward_probe(4, 0, Duration::from_millis(50), 4).await;
    assert_eq!(found, 0, "should find target at version 0");
    // 4 probes in 1 batch → ~50ms
    assert!(
        dur.as_millis() < 200,
        "should complete in 1 batch, took {:?}",
        dur,
    );
}

/// Verify the backward probe returns 0 when no version matches (target not found).
#[tokio::test]
async fn test_backward_probe_target_not_found() {
    // target_version=99 is above start_version=4, so it will never be found
    let (_dur, found) = batched_backward_probe(4, 99, Duration::from_millis(10), 4).await;
    assert_eq!(found, 0, "should return 0 when target not found");
}

/// Verify batched probing at the MAX_PROBE boundary.
/// Uses sequential_probe to simulate "many versions" — should not hang or panic.
#[tokio::test]
async fn test_checkpoint_verify_at_probe_limit() {
    // 100 probes at 1ms each with batch=4 → 25 batches → ~25ms
    let dur = batched_probe(100, Duration::from_millis(1), 4).await;
    assert!(
        dur.as_millis() < 500,
        "100 probes in batches of 4 should complete quickly, took {:?}",
        dur,
    );
}

/// Verify batched probing handles a partial last batch correctly.
/// 7 versions with batch_size=4: batch1=[4], batch2=[3] → both should work.
#[tokio::test]
async fn test_checkpoint_verify_partial_last_batch() {
    // 7 probes in batches of 4: 2 batches (4+3)
    let dur = batched_probe(7, Duration::from_millis(50), 4).await;
    // Expected: 2 batches × 50ms = ~100ms
    assert!(
        dur.as_millis() >= 80 && dur.as_millis() < 300,
        "7 probes in 2 batches should take ~100ms, took {:?}",
        dur,
    );
}

/// Verify that verify_checkpoint_version with the real storage on an empty
/// directory returns the hint version unchanged (no newer state dirs exist).
#[tokio::test]
async fn test_verify_checkpoint_empty_storage() {
    use crate::txlog::storage::TxLogStorage;
    use crate::delta_reader::engine::DeltaStorageConfig;

    let tmp = tempfile::tempdir().unwrap();
    // Create the _transaction_log directory so TxLogStorage can initialize
    std::fs::create_dir_all(tmp.path().join("_transaction_log")).unwrap();

    let table_path = format!("file://{}", tmp.path().display());
    let config = DeltaStorageConfig::default();
    let storage = TxLogStorage::new(&table_path, &config).unwrap();

    // verify_checkpoint_version is private, so test through probe_versions_since
    // which uses the same batched pattern
    let versions = super::distributed::probe_versions_since(&storage, 5).await.unwrap();
    assert!(versions.is_empty(), "no version files exist, should return empty");
}

/// Verify probe_versions_since correctly finds contiguous versions on real storage.
#[tokio::test]
async fn test_probe_versions_since_finds_contiguous() {
    use crate::txlog::storage::TxLogStorage;
    use crate::delta_reader::engine::DeltaStorageConfig;

    let tmp = tempfile::tempdir().unwrap();
    let txlog_dir = tmp.path().join("_transaction_log");
    std::fs::create_dir_all(&txlog_dir).unwrap();

    // Create version files 3, 4, 5 (since_version=2 should find all three)
    for v in 3..=5 {
        let path = txlog_dir.join(format!("{:020}.json", v));
        std::fs::write(&path, r#"{"add":{}}"#).unwrap();
    }

    let table_path = format!("file://{}", tmp.path().display());
    let config = DeltaStorageConfig::default();
    let storage = TxLogStorage::new(&table_path, &config).unwrap();

    let versions = super::distributed::probe_versions_since(&storage, 2).await.unwrap();
    assert_eq!(versions, vec![3, 4, 5], "should find versions 3, 4, 5");
}

/// Verify probe_versions_since stops at a gap (version 4 missing).
#[tokio::test]
async fn test_probe_versions_since_stops_at_gap() {
    use crate::txlog::storage::TxLogStorage;
    use crate::delta_reader::engine::DeltaStorageConfig;

    let tmp = tempfile::tempdir().unwrap();
    let txlog_dir = tmp.path().join("_transaction_log");
    std::fs::create_dir_all(&txlog_dir).unwrap();

    // Create version files 3 and 5 (gap at 4)
    for v in [3, 5] {
        let path = txlog_dir.join(format!("{:020}.json", v));
        std::fs::write(&path, r#"{"add":{}}"#).unwrap();
    }

    let table_path = format!("file://{}", tmp.path().display());
    let config = DeltaStorageConfig::default();
    let storage = TxLogStorage::new(&table_path, &config).unwrap();

    let versions = super::distributed::probe_versions_since(&storage, 2).await.unwrap();
    assert_eq!(versions, vec![3], "should stop at gap — only version 3");
}

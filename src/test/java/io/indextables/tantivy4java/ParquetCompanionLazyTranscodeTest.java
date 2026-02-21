/*
 * Reproduces and verifies production bugs with parquet companion string aggregations:
 *
 *  Bug 1 — "count is always zero":
 *    When transcode_and_cache fails (e.g. due to OOM on large data), the error was
 *    silently swallowed. SplitOverrides gets empty fast_field_data, so Quickwit reads
 *    the native .fast file (which has no string columns in HYBRID mode), causing every
 *    TermsAggregation to return count=0.
 *    Fix: propagate errors from transcode_and_cache so callers receive an exception.
 *
 *  Bug 3a — redundant re-transcode after prewarm:
 *    After a successful prewarm, transcoded_fast_columns was not updated.  The first
 *    aggregation found all columns "missing" and repeated the full parquet read.
 *    Fix: update transcoded_fast_columns after successful prewarm.
 *
 * Run:
 *   mvn test -pl . -Dtest=ParquetCompanionLazyTranscodeTest
 */
package io.indextables.tantivy4java;

import io.indextables.tantivy4java.aggregation.*;
import io.indextables.tantivy4java.result.*;
import io.indextables.tantivy4java.split.*;
import io.indextables.tantivy4java.split.merge.*;

import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.*;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class ParquetCompanionLazyTranscodeTest {

    private static SplitCacheManager cacheManager;

    @BeforeAll
    static void setupCache() {
        SplitCacheManager.CacheConfig config =
                new SplitCacheManager.CacheConfig("pq-lazy-test-" + System.nanoTime());
        cacheManager = SplitCacheManager.getInstance(config);
    }

    @AfterAll
    static void teardownCache() {
        if (cacheManager != null) cacheManager.close();
    }

    // ─────────────────────────────────────────────────────────────
    //  Bug 1 reproduction: missing parquet file → must throw, not count=0
    // ─────────────────────────────────────────────────────────────

    /**
     * Reproduces Bug 1: "count is always zero" when transcoding fails silently.
     *
     * Before fix: deleting the parquet file after split creation causes transcode to fail.
     * The error was swallowed, SplitOverrides got empty fast_field_data, and every
     * TermsAggregation returned count=0 instead of raising an error.
     *
     * After fix: the error propagates as a RuntimeException.
     */
    @Test @Order(1)
    @DisplayName("Bug 1: transcode error propagates as exception instead of returning count=0")
    void transcodeErrorPropagatesNotSilent(@TempDir Path dir) throws Exception {
        Path parquetFile = dir.resolve("bug1.parquet");
        Path splitFile   = dir.resolve("bug1.split");

        QuickwitSplit.nativeWriteTestParquet(parquetFile.toString(), 20, 0);

        // Disable string hash optimization: with hash-opt ON (default), terms on "name"
        // would be redirected to the native _phash_name U64 field, which doesn't need parquet
        // at all. This test specifically validates the transcode error propagation path.
        ParquetCompanionConfig config = new ParquetCompanionConfig(dir.toString())
                .withFastFieldMode(ParquetCompanionConfig.FastFieldMode.HYBRID)
                .withStringHashOptimization(false);

        QuickwitSplit.SplitMetadata metadata = QuickwitSplit.createFromParquet(
                Collections.singletonList(parquetFile.toString()),
                splitFile.toString(), config);

        // Delete the parquet file so that transcoding will fail with a storage error
        Files.delete(parquetFile);

        String splitUrl = "file://" + splitFile.toAbsolutePath();
        try (SplitSearcher searcher = cacheManager.createSplitSearcher(splitUrl, metadata, dir.toString())) {
            TermsAggregation agg = new TermsAggregation("name_terms", "name", 50, 0);

            // Before fix: silently returns TermsResult with count=0 for all buckets.
            // After fix: throws RuntimeException because the parquet file is gone.
            assertThrows(RuntimeException.class, () ->
                    searcher.search(new SplitMatchAllQuery(), 0, "terms", agg),
                    "Should throw when parquet file is missing, not silently return count=0");
        }
    }

    /**
     * Same bug, PARQUET_ONLY mode.
     */
    @Test @Order(2)
    @DisplayName("Bug 1 (PARQUET_ONLY): transcode error propagates as exception")
    void transcodeErrorPropagatesParquetOnly(@TempDir Path dir) throws Exception {
        Path parquetFile = dir.resolve("bug1pq.parquet");
        Path splitFile   = dir.resolve("bug1pq.split");

        QuickwitSplit.nativeWriteTestParquet(parquetFile.toString(), 20, 0);

        ParquetCompanionConfig config = new ParquetCompanionConfig(dir.toString())
                .withFastFieldMode(ParquetCompanionConfig.FastFieldMode.PARQUET_ONLY);

        QuickwitSplit.SplitMetadata metadata = QuickwitSplit.createFromParquet(
                Collections.singletonList(parquetFile.toString()),
                splitFile.toString(), config);

        Files.delete(parquetFile);

        String splitUrl = "file://" + splitFile.toAbsolutePath();
        try (SplitSearcher searcher = cacheManager.createSplitSearcher(splitUrl, metadata, dir.toString())) {
            StatsAggregation agg = new StatsAggregation("id_stats", "id");
            assertThrows(RuntimeException.class, () ->
                    searcher.search(new SplitMatchAllQuery(), 0, "stats", agg),
                    "Should throw when parquet file is missing");
        }
    }

    // ─────────────────────────────────────────────────────────────
    //  Lazy transcoding (no explicit prewarm) correctness
    // ─────────────────────────────────────────────────────────────

    /**
     * Verifies that TermsAggregation on a string field works WITHOUT an explicit prewarm
     * call, exercising the lazy transcoding path end-to-end (HYBRID mode).
     *
     * In production users sometimes skip prewarm. The lazy path must transcode on demand
     * and return correct (non-zero) counts.
     */
    @Test @Order(3)
    @DisplayName("Lazy transcode (no prewarm): TermsAgg correct counts — HYBRID")
    void lazyTranscodeTermsAggHybrid(@TempDir Path dir) throws Exception {
        Path parquetFile = dir.resolve("lazy_hyb.parquet");
        Path splitFile   = dir.resolve("lazy_hyb.split");

        int numRows = 100;
        QuickwitSplit.nativeWriteTestParquet(parquetFile.toString(), numRows, 0);

        ParquetCompanionConfig config = new ParquetCompanionConfig(dir.toString())
                .withFastFieldMode(ParquetCompanionConfig.FastFieldMode.HYBRID);

        QuickwitSplit.SplitMetadata metadata = QuickwitSplit.createFromParquet(
                Collections.singletonList(parquetFile.toString()),
                splitFile.toString(), config);

        String splitUrl = "file://" + splitFile.toAbsolutePath();
        // Intentionally no preloadParquetFastFields() call — use lazy path
        try (SplitSearcher searcher = cacheManager.createSplitSearcher(splitUrl, metadata, dir.toString())) {
            TermsAggregation agg = new TermsAggregation("name_terms", "name", 200, 0);
            SearchResult result = searcher.search(new SplitMatchAllQuery(), 0, "terms", agg);

            assertTrue(result.hasAggregations(), "Should have aggregation results");
            TermsResult terms = (TermsResult) result.getAggregation("terms");
            assertNotNull(terms);

            // name = "item_0".."item_99" — all unique, each bucket doc_count=1
            List<TermsResult.TermsBucket> buckets = terms.getBuckets();
            assertFalse(buckets.isEmpty(),
                    "Buckets must not be empty — before fix, count was always 0 when lazy transcode silently failed");
            assertEquals(numRows, buckets.size(),
                    "Should have one bucket per unique name (lazy transcode must return complete data)");
            for (TermsResult.TermsBucket b : buckets) {
                assertEquals(1, b.getDocCount(), "Each name is unique, expected doc_count=1");
            }
        }
    }

    /**
     * Same lazy-transcode test for PARQUET_ONLY mode.
     */
    @Test @Order(4)
    @DisplayName("Lazy transcode (no prewarm): TermsAgg correct counts — PARQUET_ONLY")
    void lazyTranscodeTermsAggParquetOnly(@TempDir Path dir) throws Exception {
        Path parquetFile = dir.resolve("lazy_pq.parquet");
        Path splitFile   = dir.resolve("lazy_pq.split");

        int numRows = 100;
        QuickwitSplit.nativeWriteTestParquet(parquetFile.toString(), numRows, 0);

        ParquetCompanionConfig config = new ParquetCompanionConfig(dir.toString())
                .withFastFieldMode(ParquetCompanionConfig.FastFieldMode.PARQUET_ONLY);

        QuickwitSplit.SplitMetadata metadata = QuickwitSplit.createFromParquet(
                Collections.singletonList(parquetFile.toString()),
                splitFile.toString(), config);

        String splitUrl = "file://" + splitFile.toAbsolutePath();
        try (SplitSearcher searcher = cacheManager.createSplitSearcher(splitUrl, metadata, dir.toString())) {
            TermsAggregation agg = new TermsAggregation("name_terms", "name", 200, 0);
            SearchResult result = searcher.search(new SplitMatchAllQuery(), 0, "terms", agg);

            assertTrue(result.hasAggregations());
            TermsResult terms = (TermsResult) result.getAggregation("terms");
            assertNotNull(terms);

            List<TermsResult.TermsBucket> buckets = terms.getBuckets();
            assertFalse(buckets.isEmpty(), "Lazy PARQUET_ONLY transcode must produce non-empty buckets");
            assertEquals(numRows, buckets.size(), "All unique names should produce one bucket each");
        }
    }

    /**
     * Verifies that consecutive lazy aggregations on the same field reuse cached data
     * (transcoded_fast_columns prevents redundant re-transcoding).
     */
    @Test @Order(5)
    @DisplayName("Consecutive lazy aggregations: second call uses L1 cache")
    void consecutiveLazyAggregationsUseCache(@TempDir Path dir) throws Exception {
        Path parquetFile = dir.resolve("consec.parquet");
        Path splitFile   = dir.resolve("consec.split");

        int numRows = 30;
        QuickwitSplit.nativeWriteTestParquet(parquetFile.toString(), numRows, 0);

        ParquetCompanionConfig config = new ParquetCompanionConfig(dir.toString())
                .withFastFieldMode(ParquetCompanionConfig.FastFieldMode.PARQUET_ONLY);

        QuickwitSplit.SplitMetadata metadata = QuickwitSplit.createFromParquet(
                Collections.singletonList(parquetFile.toString()),
                splitFile.toString(), config);

        String splitUrl = "file://" + splitFile.toAbsolutePath();
        try (SplitSearcher searcher = cacheManager.createSplitSearcher(splitUrl, metadata, dir.toString())) {
            TermsAggregation agg = new TermsAggregation("name_terms", "name", 100, 0);

            // First call: lazy transcode
            SearchResult r1 = searcher.search(new SplitMatchAllQuery(), 0, "terms", agg);
            TermsResult t1 = (TermsResult) r1.getAggregation("terms");
            assertEquals(numRows, t1.getBuckets().size(), "First lazy agg: correct count");

            // Second call: must use cached transcoded data, same result
            SearchResult r2 = searcher.search(new SplitMatchAllQuery(), 0, "terms", agg);
            TermsResult t2 = (TermsResult) r2.getAggregation("terms");
            assertEquals(t1.getBuckets().size(), t2.getBuckets().size(),
                    "Second aggregation must return identical count (no redundant re-transcode)");
        }
    }

    // ─────────────────────────────────────────────────────────────
    //  Bug 3a: prewarm should suppress redundant transcode
    // ─────────────────────────────────────────────────────────────

    /**
     * Verifies that after prewarm, aggregations return correct results and are served
     * from the prewarm cache (transcoded_fast_columns updated by prewarm).
     *
     * Before Bug 3a fix: transcoded_fast_columns was not updated after prewarm, so the
     * first aggregation repeated the entire parquet read unnecessarily (wasted work,
     * and with large data — OOM).
     *
     * After fix: transcoded_fast_columns is populated after prewarm, so the first
     * aggregation skips the transcode loop and uses the L1 cache directly.
     */
    @Test @Order(6)
    @DisplayName("Bug 3a: after prewarm, aggregation uses cached data (no re-transcode)")
    void prewarmThenAggregationUsesCache(@TempDir Path dir) throws Exception {
        Path parquetFile = dir.resolve("prewarm_agg.parquet");
        Path splitFile   = dir.resolve("prewarm_agg.split");

        int numRows = 50;
        QuickwitSplit.nativeWriteTestParquet(parquetFile.toString(), numRows, 0);

        ParquetCompanionConfig config = new ParquetCompanionConfig(dir.toString())
                .withFastFieldMode(ParquetCompanionConfig.FastFieldMode.HYBRID);

        QuickwitSplit.SplitMetadata metadata = QuickwitSplit.createFromParquet(
                Collections.singletonList(parquetFile.toString()),
                splitFile.toString(), config);

        String splitUrl = "file://" + splitFile.toAbsolutePath();
        try (SplitSearcher searcher = cacheManager.createSplitSearcher(splitUrl, metadata, dir.toString())) {
            // Explicit prewarm of the string column
            searcher.preloadParquetFastFields("name").join();

            // Now delete the parquet file — if the fix is correct, the first aggregation
            // should use the prewarm cache and NOT re-read parquet.
            // Before fix: would try to re-transcode, fail, and silently return count=0.
            Files.delete(parquetFile);

            TermsAggregation agg = new TermsAggregation("name_terms", "name", 100, 0);
            SearchResult result = searcher.search(new SplitMatchAllQuery(), 0, "terms", agg);

            assertTrue(result.hasAggregations());
            TermsResult terms = (TermsResult) result.getAggregation("terms");
            assertNotNull(terms);

            // With the fix, prewarm cached the data and transcoded_fast_columns was
            // updated, so no re-transcode is attempted despite the missing parquet file.
            List<TermsResult.TermsBucket> buckets = terms.getBuckets();
            assertFalse(buckets.isEmpty(),
                    "After prewarm, aggregation must use cached data even if parquet is gone " +
                    "(before fix, redundant re-transcode would fail silently → count=0)");
            assertEquals(numRows, buckets.size(),
                    "Prewarm cache must contain complete data for all rows");
        }
    }

    /**
     * Same prewarm-suppresses-retranscode test for PARQUET_ONLY mode.
     */
    @Test @Order(7)
    @DisplayName("Bug 3a (PARQUET_ONLY): after prewarm, aggregation uses cached data")
    void prewarmThenAggregationParquetOnly(@TempDir Path dir) throws Exception {
        Path parquetFile = dir.resolve("prewarm_pq.parquet");
        Path splitFile   = dir.resolve("prewarm_pq.split");

        int numRows = 40;
        QuickwitSplit.nativeWriteTestParquet(parquetFile.toString(), numRows, 0);

        ParquetCompanionConfig config = new ParquetCompanionConfig(dir.toString())
                .withFastFieldMode(ParquetCompanionConfig.FastFieldMode.PARQUET_ONLY);

        QuickwitSplit.SplitMetadata metadata = QuickwitSplit.createFromParquet(
                Collections.singletonList(parquetFile.toString()),
                splitFile.toString(), config);

        String splitUrl = "file://" + splitFile.toAbsolutePath();
        try (SplitSearcher searcher = cacheManager.createSplitSearcher(splitUrl, metadata, dir.toString())) {
            searcher.preloadParquetFastFields("id", "score", "name", "active").join();

            // Delete parquet — fix must ensure prewarm cache is used
            Files.delete(parquetFile);

            TermsAggregation agg = new TermsAggregation("name_terms", "name", 100, 0);
            SearchResult result = searcher.search(new SplitMatchAllQuery(), 0, "terms", agg);

            assertTrue(result.hasAggregations());
            TermsResult terms = (TermsResult) result.getAggregation("terms");
            assertNotNull(terms);
            assertEquals(numRows, terms.getBuckets().size(),
                    "PARQUET_ONLY: prewarm cache must cover all data");
        }
    }

    // ─────────────────────────────────────────────────────────────
    //  L2 disk cache: lazy-transcoded bytes must survive L1 eviction
    // ─────────────────────────────────────────────────────────────

    /**
     * Confirms that dynamically transcoded string columns are written to the L2 disk
     * cache so they don't have to be re-transcoded from parquet when the in-memory (L1)
     * data is lost (searcher closed / evicted).
     *
     * Proof: after the first lazy aggregation with a disk cache configured,
     *   1. Close the searcher (L1 lost).
     *   2. Delete the parquet file (re-transcode from parquet would fail).
     *   3. Open a new searcher backed by the same disk cache.
     *   4. Run the same aggregation — must succeed from L2 with correct counts.
     *
     * HYBRID mode: native numerics in .fast + parquet-derived string column.
     */
    @Test @Order(8)
    @DisplayName("L2 cache: lazy-transcoded string column survives searcher close — HYBRID")
    void lazyTranscodeWritesToL2Cache_Hybrid(@TempDir Path dir) throws Exception {
        Path parquetFile = dir.resolve("l2_hyb.parquet");
        Path splitFile   = dir.resolve("l2_hyb.split");
        Path diskCacheDir = dir.resolve("l2cache");
        int numRows = 50;

        QuickwitSplit.nativeWriteTestParquet(parquetFile.toString(), numRows, 0);

        ParquetCompanionConfig pqConfig = new ParquetCompanionConfig(dir.toString())
                .withFastFieldMode(ParquetCompanionConfig.FastFieldMode.HYBRID);

        QuickwitSplit.SplitMetadata metadata = QuickwitSplit.createFromParquet(
                Collections.singletonList(parquetFile.toString()),
                splitFile.toString(), pqConfig);

        String splitUrl = "file://" + splitFile.toAbsolutePath();

        // ── Phase 1: lazy transcode → populates L2 disk cache ──────────────
        SplitCacheManager.TieredCacheConfig tiered = new SplitCacheManager.TieredCacheConfig()
                .withDiskCachePath(diskCacheDir.toString())
                .withMaxDiskSize(100_000_000);

        try (SplitCacheManager cm1 = SplitCacheManager.getInstance(
                new SplitCacheManager.CacheConfig("l2-hyb-phase1-" + System.nanoTime())
                        .withMaxCacheSize(50_000_000)
                        .withTieredCache(tiered))) {

            try (SplitSearcher s1 = cm1.createSplitSearcher(splitUrl, metadata, dir.toString())) {
                // No prewarm — use lazy path
                TermsAggregation agg = new TermsAggregation("n", "name", 100, 0);
                SearchResult r = s1.search(new SplitMatchAllQuery(), 0, "terms", agg);
                assertEquals(numRows,
                        ((TermsResult) r.getAggregation("terms")).getBuckets().size(),
                        "Phase 1: lazy transcode must produce correct count");
            }
            // searcher closed → L1 in-memory cache dropped
        }
        // cache manager closed → but disk cache directory persists

        // ── Phase 2: delete parquet, open new searcher, aggregate from L2 ──
        Files.delete(parquetFile);

        try (SplitCacheManager cm2 = SplitCacheManager.getInstance(
                new SplitCacheManager.CacheConfig("l2-hyb-phase2-" + System.nanoTime())
                        .withMaxCacheSize(50_000_000)
                        .withTieredCache(tiered))) {   // same diskCacheDir

            try (SplitSearcher s2 = cm2.createSplitSearcher(splitUrl, metadata, dir.toString())) {
                TermsAggregation agg = new TermsAggregation("n", "name", 100, 0);
                SearchResult r = s2.search(new SplitMatchAllQuery(), 0, "terms", agg);

                TermsResult terms = (TermsResult) r.getAggregation("terms");
                assertNotNull(terms);
                assertEquals(numRows, terms.getBuckets().size(),
                        "Phase 2: L2 disk cache must serve transcoded data after parquet is deleted " +
                        "(re-transcoding would have failed — parquet is gone)");
            }
        }
    }

    /**
     * Same L2 disk cache confirmation for PARQUET_ONLY mode.
     */
    @Test @Order(9)
    @DisplayName("L2 cache: lazy-transcoded string column survives searcher close — PARQUET_ONLY")
    void lazyTranscodeWritesToL2Cache_ParquetOnly(@TempDir Path dir) throws Exception {
        Path parquetFile = dir.resolve("l2_pq.parquet");
        Path splitFile   = dir.resolve("l2_pq.split");
        Path diskCacheDir = dir.resolve("l2cache");
        int numRows = 50;

        QuickwitSplit.nativeWriteTestParquet(parquetFile.toString(), numRows, 0);

        ParquetCompanionConfig pqConfig = new ParquetCompanionConfig(dir.toString())
                .withFastFieldMode(ParquetCompanionConfig.FastFieldMode.PARQUET_ONLY);

        QuickwitSplit.SplitMetadata metadata = QuickwitSplit.createFromParquet(
                Collections.singletonList(parquetFile.toString()),
                splitFile.toString(), pqConfig);

        String splitUrl = "file://" + splitFile.toAbsolutePath();

        SplitCacheManager.TieredCacheConfig tiered = new SplitCacheManager.TieredCacheConfig()
                .withDiskCachePath(diskCacheDir.toString())
                .withMaxDiskSize(100_000_000);

        // Phase 1: lazy transcode → L2 populated
        try (SplitCacheManager cm1 = SplitCacheManager.getInstance(
                new SplitCacheManager.CacheConfig("l2-pq-phase1-" + System.nanoTime())
                        .withMaxCacheSize(50_000_000)
                        .withTieredCache(tiered))) {

            try (SplitSearcher s1 = cm1.createSplitSearcher(splitUrl, metadata, dir.toString())) {
                TermsAggregation agg = new TermsAggregation("n", "name", 100, 0);
                SearchResult r = s1.search(new SplitMatchAllQuery(), 0, "terms", agg);
                assertEquals(numRows,
                        ((TermsResult) r.getAggregation("terms")).getBuckets().size(),
                        "Phase 1: correct count from lazy transcode");
            }
        }

        // Phase 2: delete parquet, reopen with same disk cache
        Files.delete(parquetFile);

        try (SplitCacheManager cm2 = SplitCacheManager.getInstance(
                new SplitCacheManager.CacheConfig("l2-pq-phase2-" + System.nanoTime())
                        .withMaxCacheSize(50_000_000)
                        .withTieredCache(tiered))) {

            try (SplitSearcher s2 = cm2.createSplitSearcher(splitUrl, metadata, dir.toString())) {
                TermsAggregation agg = new TermsAggregation("n", "name", 100, 0);
                SearchResult r = s2.search(new SplitMatchAllQuery(), 0, "terms", agg);

                TermsResult terms = (TermsResult) r.getAggregation("terms");
                assertNotNull(terms);
                assertEquals(numRows, terms.getBuckets().size(),
                        "Phase 2: L2 disk cache must serve transcoded data after parquet is deleted");
            }
        }
    }
}

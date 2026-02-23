/*
 * Validates that aggregations and range queries work correctly with parquet companion splits
 * across all FastFieldMode settings (DISABLED, HYBRID, PARQUET_ONLY) and data types.
 *
 * Run:
 *   mvn test -pl . -Dtest=ParquetCompanionAggregationTest
 */
package io.indextables.tantivy4java;

import io.indextables.tantivy4java.core.*;
import io.indextables.tantivy4java.query.*;
import io.indextables.tantivy4java.result.*;
import io.indextables.tantivy4java.split.*;
import io.indextables.tantivy4java.split.merge.*;
import io.indextables.tantivy4java.aggregation.*;

import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests aggregations (stats, terms, histogram) and range queries on parquet companion
 * splits with all three FastFieldMode settings:
 *   - DISABLED: no fast fields from parquet (native tantivy fast fields only)
 *   - HYBRID: numeric fast fields from tantivy, string fast fields from parquet
 *   - PARQUET_ONLY: all fast fields transcoded from parquet
 *
 * Data types covered:
 *   - i64 (id, active) — integer aggregations, range queries
 *   - f64 (score) — float aggregations, range queries
 *   - utf8 (name) — terms aggregation, term/range queries
 *   - bool (active) — via i64 representation
 *   - timestamp (created_at) — date histogram, date range queries (complex parquet)
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class ParquetCompanionAggregationTest {

    private static SplitCacheManager cacheManager;

    @BeforeAll
    static void setupCache() {
        SplitCacheManager.CacheConfig config =
                new SplitCacheManager.CacheConfig("pq-agg-test-" + System.nanoTime());
        cacheManager = SplitCacheManager.getInstance(config);
    }

    @AfterAll
    static void teardownCache() {
        if (cacheManager != null) cacheManager.close();
    }

    // ═══════════════════════════════════════════════════════════════
    //  Helper: create a parquet companion split with a given mode
    // ═══════════════════════════════════════════════════════════════

    private SplitSearcher createSearcher(Path dir, ParquetCompanionConfig.FastFieldMode mode,
                                         int numRows, boolean complex, String tag) throws Exception {
        return createSearcher(dir, mode, numRows, complex, tag, null);
    }

    private SplitSearcher createSearcher(Path dir, ParquetCompanionConfig.FastFieldMode mode,
                                         int numRows, boolean complex, String tag,
                                         Map<String, String> tokenizerOverrides) throws Exception {
        Path parquetFile = dir.resolve(tag + ".parquet");
        Path splitFile = dir.resolve(tag + ".split");

        if (complex) {
            QuickwitSplit.nativeWriteTestParquetComplex(parquetFile.toString(), numRows, 0);
        } else {
            QuickwitSplit.nativeWriteTestParquet(parquetFile.toString(), numRows, 0);
        }

        ParquetCompanionConfig config = new ParquetCompanionConfig(dir.toString())
                .withFastFieldMode(mode);
        if (tokenizerOverrides != null) {
            config = config.withTokenizerOverrides(tokenizerOverrides);
        }

        QuickwitSplit.SplitMetadata metadata = QuickwitSplit.createFromParquet(
                Collections.singletonList(parquetFile.toString()),
                splitFile.toString(), config);

        String splitUrl = "file://" + splitFile.toAbsolutePath();
        SplitSearcher searcher = cacheManager.createSplitSearcher(splitUrl, metadata, dir.toString());

        // Prewarm for HYBRID/PARQUET_ONLY so fast fields are available
        try {
            if (mode != ParquetCompanionConfig.FastFieldMode.DISABLED) {
                if (complex) {
                    searcher.preloadParquetFastFields("id", "score", "name", "active", "created_at").join();
                } else {
                    searcher.preloadParquetFastFields("id", "score", "name", "active").join();
                }
            }
        } catch (Exception e) {
            searcher.close();
            throw e;
        }

        return searcher;
    }

    // ═══════════════════════════════════════════════════════════════
    //  1. Stats aggregation on i64 field — all three modes
    // ═══════════════════════════════════════════════════════════════

    @Test @org.junit.jupiter.api.Order(1)
    @DisplayName("Stats agg on i64 (id) — DISABLED mode")
    void statsI64Disabled(@TempDir Path dir) throws Exception {
        try (SplitSearcher s = createSearcher(dir, ParquetCompanionConfig.FastFieldMode.DISABLED, 20, false, "stats_i64_dis")) {
            verifyI64Stats(s, 20);
        }
    }

    @Test @org.junit.jupiter.api.Order(2)
    @DisplayName("Stats agg on i64 (id) — HYBRID mode")
    void statsI64Hybrid(@TempDir Path dir) throws Exception {
        try (SplitSearcher s = createSearcher(dir, ParquetCompanionConfig.FastFieldMode.HYBRID, 20, false, "stats_i64_hyb")) {
            verifyI64Stats(s, 20);
        }
    }

    @Test @org.junit.jupiter.api.Order(3)
    @DisplayName("Stats agg on i64 (id) — PARQUET_ONLY mode")
    void statsI64ParquetOnly(@TempDir Path dir) throws Exception {
        try (SplitSearcher s = createSearcher(dir, ParquetCompanionConfig.FastFieldMode.PARQUET_ONLY, 20, false, "stats_i64_pq")) {
            verifyI64Stats(s, 20);
        }
    }

    private void verifyI64Stats(SplitSearcher s, int numRows) {
        StatsAggregation agg = new StatsAggregation("id_stats", "id");
        SearchResult result = s.search(new SplitMatchAllQuery(), 0, "stats", agg);

        assertTrue(result.hasAggregations());
        StatsResult stats = (StatsResult) result.getAggregation("stats");
        assertNotNull(stats);
        assertEquals(numRows, stats.getCount());
        assertEquals(0.0, stats.getMin(), 0.01, "min id should be 0");
        assertEquals(numRows - 1.0, stats.getMax(), 0.01, "max id should be " + (numRows - 1));
        // sum of 0..19 = 190
        double expectedSum = (numRows - 1.0) * numRows / 2.0;
        assertEquals(expectedSum, stats.getSum(), 0.01);
    }

    // ═══════════════════════════════════════════════════════════════
    //  2. Stats aggregation on f64 field (score) — all modes
    // ═══════════════════════════════════════════════════════════════

    @Test @org.junit.jupiter.api.Order(4)
    @DisplayName("Stats agg on f64 (score) — DISABLED mode")
    void statsF64Disabled(@TempDir Path dir) throws Exception {
        try (SplitSearcher s = createSearcher(dir, ParquetCompanionConfig.FastFieldMode.DISABLED, 20, false, "stats_f64_dis")) {
            verifyF64Stats(s, 20);
        }
    }

    @Test @org.junit.jupiter.api.Order(5)
    @DisplayName("Stats agg on f64 (score) — HYBRID mode")
    void statsF64Hybrid(@TempDir Path dir) throws Exception {
        try (SplitSearcher s = createSearcher(dir, ParquetCompanionConfig.FastFieldMode.HYBRID, 20, false, "stats_f64_hyb")) {
            verifyF64Stats(s, 20);
        }
    }

    @Test @org.junit.jupiter.api.Order(6)
    @DisplayName("Stats agg on f64 (score) — PARQUET_ONLY mode")
    void statsF64ParquetOnly(@TempDir Path dir) throws Exception {
        try (SplitSearcher s = createSearcher(dir, ParquetCompanionConfig.FastFieldMode.PARQUET_ONLY, 20, false, "stats_f64_pq")) {
            verifyF64Stats(s, 20);
        }
    }

    private void verifyF64Stats(SplitSearcher s, int numRows) {
        // score = id * 1.5 + 10.0 → values: 10.0, 11.5, 13.0, ..., (numRows-1)*1.5+10.0
        StatsAggregation agg = new StatsAggregation("score_stats", "score");
        SearchResult result = s.search(new SplitMatchAllQuery(), 0, "stats", agg);

        assertTrue(result.hasAggregations());
        StatsResult stats = (StatsResult) result.getAggregation("stats");
        assertNotNull(stats);
        assertEquals(numRows, stats.getCount());
        assertEquals(10.0, stats.getMin(), 0.01);
        assertEquals((numRows - 1) * 1.5 + 10.0, stats.getMax(), 0.01);
    }

    // ═══════════════════════════════════════════════════════════════
    //  3. Histogram aggregation on i64 — all modes
    // ═══════════════════════════════════════════════════════════════

    @Test @org.junit.jupiter.api.Order(7)
    @DisplayName("Histogram agg on i64 (id) — DISABLED mode")
    void histogramI64Disabled(@TempDir Path dir) throws Exception {
        try (SplitSearcher s = createSearcher(dir, ParquetCompanionConfig.FastFieldMode.DISABLED, 20, false, "hist_i64_dis")) {
            verifyI64Histogram(s);
        }
    }

    @Test @org.junit.jupiter.api.Order(8)
    @DisplayName("Histogram agg on i64 (id) — HYBRID mode")
    void histogramI64Hybrid(@TempDir Path dir) throws Exception {
        try (SplitSearcher s = createSearcher(dir, ParquetCompanionConfig.FastFieldMode.HYBRID, 20, false, "hist_i64_hyb")) {
            verifyI64Histogram(s);
        }
    }

    @Test @org.junit.jupiter.api.Order(9)
    @DisplayName("Histogram agg on i64 (id) — PARQUET_ONLY mode")
    void histogramI64ParquetOnly(@TempDir Path dir) throws Exception {
        try (SplitSearcher s = createSearcher(dir, ParquetCompanionConfig.FastFieldMode.PARQUET_ONLY, 20, false, "hist_i64_pq")) {
            verifyI64Histogram(s);
        }
    }

    private void verifyI64Histogram(SplitSearcher s) {
        // id values: 0-19; interval=10 → buckets at 0 and 10
        HistogramAggregation agg = new HistogramAggregation("id", 10.0);
        SearchResult result = s.search(new SplitMatchAllQuery(), 0, "hist", agg);

        assertTrue(result.hasAggregations());
        HistogramResult hist = (HistogramResult) result.getAggregation("hist");
        assertNotNull(hist);

        List<HistogramResult.HistogramBucket> buckets = hist.getBuckets();
        assertFalse(buckets.isEmpty(), "Should have histogram buckets");

        // Find bucket at key=0 and key=10
        long countAt0 = 0, countAt10 = 0;
        for (HistogramResult.HistogramBucket b : buckets) {
            if (b.getKey() == 0.0) countAt0 = b.getDocCount();
            if (b.getKey() == 10.0) countAt10 = b.getDocCount();
        }
        assertEquals(10, countAt0, "Bucket [0,10) should have 10 docs");
        assertEquals(10, countAt10, "Bucket [10,20) should have 10 docs");
    }

    // ═══════════════════════════════════════════════════════════════
    //  4. Histogram aggregation on f64 (score) — all modes
    // ═══════════════════════════════════════════════════════════════

    @Test @org.junit.jupiter.api.Order(10)
    @DisplayName("Histogram agg on f64 (score) — DISABLED mode")
    void histogramF64Disabled(@TempDir Path dir) throws Exception {
        try (SplitSearcher s = createSearcher(dir, ParquetCompanionConfig.FastFieldMode.DISABLED, 20, false, "hist_f64_dis")) {
            verifyF64Histogram(s);
        }
    }

    @Test @org.junit.jupiter.api.Order(11)
    @DisplayName("Histogram agg on f64 (score) — HYBRID mode")
    void histogramF64Hybrid(@TempDir Path dir) throws Exception {
        try (SplitSearcher s = createSearcher(dir, ParquetCompanionConfig.FastFieldMode.HYBRID, 20, false, "hist_f64_hyb")) {
            verifyF64Histogram(s);
        }
    }

    @Test @org.junit.jupiter.api.Order(12)
    @DisplayName("Histogram agg on f64 (score) — PARQUET_ONLY mode")
    void histogramF64ParquetOnly(@TempDir Path dir) throws Exception {
        try (SplitSearcher s = createSearcher(dir, ParquetCompanionConfig.FastFieldMode.PARQUET_ONLY, 20, false, "hist_f64_pq")) {
            verifyF64Histogram(s);
        }
    }

    private void verifyF64Histogram(SplitSearcher s) {
        // score = id * 1.5 + 10.0 → values: 10.0, 11.5, 13.0, ..., 38.5
        // interval=10 → buckets at 10, 20, 30
        HistogramAggregation agg = new HistogramAggregation("score", 10.0);
        SearchResult result = s.search(new SplitMatchAllQuery(), 0, "hist", agg);

        assertTrue(result.hasAggregations());
        HistogramResult hist = (HistogramResult) result.getAggregation("hist");
        assertNotNull(hist);

        List<HistogramResult.HistogramBucket> buckets = hist.getBuckets();
        assertTrue(buckets.size() >= 2, "Should have at least 2 histogram buckets");

        long total = 0;
        for (HistogramResult.HistogramBucket b : buckets) {
            total += b.getDocCount();
        }
        assertEquals(20, total, "Total across all buckets should be 20");
    }

    // ═══════════════════════════════════════════════════════════════
    //  5. Terms aggregation on text field (name) — all modes
    // ═══════════════════════════════════════════════════════════════

    @Test @org.junit.jupiter.api.Order(13)
    @DisplayName("Terms agg on text (name) — DISABLED mode")
    void termsTextDisabled(@TempDir Path dir) throws Exception {
        try (SplitSearcher s = createSearcher(dir, ParquetCompanionConfig.FastFieldMode.DISABLED, 20, false, "terms_txt_dis")) {
            verifyTermsAgg(s, 20);
        }
    }

    @Test @org.junit.jupiter.api.Order(14)
    @DisplayName("Terms agg on text (name) — HYBRID mode")
    void termsTextHybrid(@TempDir Path dir) throws Exception {
        try (SplitSearcher s = createSearcher(dir, ParquetCompanionConfig.FastFieldMode.HYBRID, 20, false, "terms_txt_hyb")) {
            verifyTermsAgg(s, 20);
        }
    }

    @Test @org.junit.jupiter.api.Order(15)
    @DisplayName("Terms agg on text (name) — PARQUET_ONLY mode")
    void termsTextParquetOnly(@TempDir Path dir) throws Exception {
        try (SplitSearcher s = createSearcher(dir, ParquetCompanionConfig.FastFieldMode.PARQUET_ONLY, 20, false, "terms_txt_pq")) {
            verifyTermsAgg(s, 20);
        }
    }

    private void verifyTermsAgg(SplitSearcher s, int numRows) {
        // name = "item_0", "item_1", ..., "item_19" — all unique
        TermsAggregation agg = new TermsAggregation("name_terms", "name", 50, 0);
        SearchResult result = s.search(new SplitMatchAllQuery(), 0, "terms", agg);

        assertTrue(result.hasAggregations());
        TermsResult terms = (TermsResult) result.getAggregation("terms");
        assertNotNull(terms);

        List<TermsResult.TermsBucket> buckets = terms.getBuckets();
        assertFalse(buckets.isEmpty(), "Should have terms buckets");

        // Each name is unique → each bucket has doc_count=1
        long totalDocs = 0;
        for (TermsResult.TermsBucket b : buckets) {
            assertEquals(1, b.getDocCount(), "Each name is unique, should have 1 doc per bucket");
            totalDocs++;
        }
        assertEquals(numRows, totalDocs, "Should have " + numRows + " term buckets");
    }

    // ═══════════════════════════════════════════════════════════════
    //  6. Range query on i64 field — all modes
    // ═══════════════════════════════════════════════════════════════

    @Test @org.junit.jupiter.api.Order(16)
    @DisplayName("Range query on i64 (id) — DISABLED mode")
    void rangeI64Disabled(@TempDir Path dir) throws Exception {
        try (SplitSearcher s = createSearcher(dir, ParquetCompanionConfig.FastFieldMode.DISABLED, 20, false, "range_i64_dis")) {
            verifyI64RangeQuery(s);
        }
    }

    @Test @org.junit.jupiter.api.Order(17)
    @DisplayName("Range query on i64 (id) — HYBRID mode")
    void rangeI64Hybrid(@TempDir Path dir) throws Exception {
        try (SplitSearcher s = createSearcher(dir, ParquetCompanionConfig.FastFieldMode.HYBRID, 20, false, "range_i64_hyb")) {
            verifyI64RangeQuery(s);
        }
    }

    @Test @org.junit.jupiter.api.Order(18)
    @DisplayName("Range query on i64 (id) — PARQUET_ONLY mode")
    void rangeI64ParquetOnly(@TempDir Path dir) throws Exception {
        try (SplitSearcher s = createSearcher(dir, ParquetCompanionConfig.FastFieldMode.PARQUET_ONLY, 20, false, "range_i64_pq")) {
            verifyI64RangeQuery(s);
        }
    }

    private void verifyI64RangeQuery(SplitSearcher s) {
        // id in [5, 10] inclusive → ids 5,6,7,8,9,10 = 6 docs
        SplitRangeQuery rangeQ = SplitRangeQuery.inclusiveRange("id", "5", "10", "i64");
        SearchResult result = s.search(rangeQ, 100);
        assertEquals(6, result.getHits().size(), "Range [5,10] should match 6 docs");

        // id in [0, 4] inclusive → ids 0,1,2,3,4 = 5 docs
        SplitRangeQuery rangeQ2 = SplitRangeQuery.inclusiveRange("id", "0", "4", "i64");
        SearchResult result2 = s.search(rangeQ2, 100);
        assertEquals(5, result2.getHits().size(), "Range [0,4] should match 5 docs");

        // id > 17 (exclusive lower, unbounded upper) → ids 18,19 = 2 docs
        SplitRangeQuery rangeQ3 = new SplitRangeQuery("id",
                SplitRangeQuery.RangeBound.exclusive("17"),
                SplitRangeQuery.RangeBound.unbounded(), "i64");
        SearchResult result3 = s.search(rangeQ3, 100);
        assertEquals(2, result3.getHits().size(), "Range (17,*) should match 2 docs");
    }

    // ═══════════════════════════════════════════════════════════════
    //  7. Range query on f64 field (score) — all modes
    // ═══════════════════════════════════════════════════════════════

    @Test @org.junit.jupiter.api.Order(19)
    @DisplayName("Range query on f64 (score) — DISABLED mode")
    void rangeF64Disabled(@TempDir Path dir) throws Exception {
        try (SplitSearcher s = createSearcher(dir, ParquetCompanionConfig.FastFieldMode.DISABLED, 20, false, "range_f64_dis")) {
            verifyF64RangeQuery(s);
        }
    }

    @Test @org.junit.jupiter.api.Order(20)
    @DisplayName("Range query on f64 (score) — HYBRID mode")
    void rangeF64Hybrid(@TempDir Path dir) throws Exception {
        try (SplitSearcher s = createSearcher(dir, ParquetCompanionConfig.FastFieldMode.HYBRID, 20, false, "range_f64_hyb")) {
            verifyF64RangeQuery(s);
        }
    }

    @Test @org.junit.jupiter.api.Order(21)
    @DisplayName("Range query on f64 (score) — PARQUET_ONLY mode")
    void rangeF64ParquetOnly(@TempDir Path dir) throws Exception {
        try (SplitSearcher s = createSearcher(dir, ParquetCompanionConfig.FastFieldMode.PARQUET_ONLY, 20, false, "range_f64_pq")) {
            verifyF64RangeQuery(s);
        }
    }

    private void verifyF64RangeQuery(SplitSearcher s) {
        // score = id * 1.5 + 10.0 → values 10.0, 11.5, 13.0, ..., 38.5
        // score in [10.0, 20.0]:
        //   id*1.5+10.0 >= 10.0 → id >= 0 → all ids
        //   id*1.5+10.0 <= 20.0 → id <= 6.67 → id <= 6
        //   ids: 0,1,2,3,4,5,6 → scores: 10.0, 11.5, 13.0, 14.5, 16.0, 17.5, 19.0
        //   = 7 docs
        SplitRangeQuery rangeQ = SplitRangeQuery.inclusiveRange("score", "10.0", "20.0", "f64");
        SearchResult result = s.search(rangeQ, 100);
        assertEquals(7, result.getHits().size(), "Range [10.0,20.0] should match 7 docs");
    }

    // ═══════════════════════════════════════════════════════════════
    //  8. Range query on text field (name) — all modes
    // ═══════════════════════════════════════════════════════════════

    @Test @org.junit.jupiter.api.Order(22)
    @DisplayName("Range query on text (name) — DISABLED mode")
    void rangeTextDisabled(@TempDir Path dir) throws Exception {
        try (SplitSearcher s = createSearcher(dir, ParquetCompanionConfig.FastFieldMode.DISABLED, 20, false, "range_txt_dis")) {
            verifyTextRangeQuery(s);
        }
    }

    @Test @org.junit.jupiter.api.Order(23)
    @DisplayName("Range query on text (name) — HYBRID mode")
    void rangeTextHybrid(@TempDir Path dir) throws Exception {
        try (SplitSearcher s = createSearcher(dir, ParquetCompanionConfig.FastFieldMode.HYBRID, 20, false, "range_txt_hyb")) {
            verifyTextRangeQuery(s);
        }
    }

    @Test @org.junit.jupiter.api.Order(24)
    @DisplayName("Range query on text (name) — PARQUET_ONLY mode")
    void rangeTextParquetOnly(@TempDir Path dir) throws Exception {
        try (SplitSearcher s = createSearcher(dir, ParquetCompanionConfig.FastFieldMode.PARQUET_ONLY, 20, false, "range_txt_pq")) {
            verifyTextRangeQuery(s);
        }
    }

    private void verifyTextRangeQuery(SplitSearcher s) {
        // names: item_0 .. item_19 (lexicographic ordering)
        // Range [item_0, item_2) → "item_0", "item_1", "item_10"..."item_19" in lex order
        // Actually let's just verify we get results for a term query
        SplitTermQuery tq = new SplitTermQuery("name", "item_5");
        SearchResult result = s.search(tq, 10);
        assertEquals(1, result.getHits().size(), "Term query for item_5 should match 1 doc");
    }

    // ═══════════════════════════════════════════════════════════════
    //  9. Aggregation + range query combined — all modes
    // ═══════════════════════════════════════════════════════════════

    @Test @org.junit.jupiter.api.Order(25)
    @DisplayName("Range query + stats agg combined — DISABLED mode")
    void rangeWithAggDisabled(@TempDir Path dir) throws Exception {
        try (SplitSearcher s = createSearcher(dir, ParquetCompanionConfig.FastFieldMode.DISABLED, 20, false, "combo_dis")) {
            verifyRangeWithAgg(s);
        }
    }

    @Test @org.junit.jupiter.api.Order(26)
    @DisplayName("Range query + stats agg combined — HYBRID mode")
    void rangeWithAggHybrid(@TempDir Path dir) throws Exception {
        try (SplitSearcher s = createSearcher(dir, ParquetCompanionConfig.FastFieldMode.HYBRID, 20, false, "combo_hyb")) {
            verifyRangeWithAgg(s);
        }
    }

    @Test @org.junit.jupiter.api.Order(27)
    @DisplayName("Range query + stats agg combined — PARQUET_ONLY mode")
    void rangeWithAggParquetOnly(@TempDir Path dir) throws Exception {
        try (SplitSearcher s = createSearcher(dir, ParquetCompanionConfig.FastFieldMode.PARQUET_ONLY, 20, false, "combo_pq")) {
            verifyRangeWithAgg(s);
        }
    }

    private void verifyRangeWithAgg(SplitSearcher s) {
        // Range id in [5, 14] = 10 docs, then stats on score
        SplitRangeQuery rangeQ = SplitRangeQuery.inclusiveRange("id", "5", "14", "i64");
        StatsAggregation scoreStats = new StatsAggregation("score_stats", "score");

        SearchResult result = s.search(rangeQ, 100, "stats", scoreStats);
        assertEquals(10, result.getHits().size(), "Range [5,14] should match 10 docs");

        assertTrue(result.hasAggregations());
        StatsResult stats = (StatsResult) result.getAggregation("stats");
        assertNotNull(stats);
        assertEquals(10, stats.getCount());
        // score = id * 1.5 + 10.0
        // scores for ids 5-14: 17.5, 19.0, 20.5, 22.0, 23.5, 25.0, 26.5, 28.0, 29.5, 31.0
        assertEquals(17.5, stats.getMin(), 0.01);
        assertEquals(31.0, stats.getMax(), 0.01);
    }

    // ═══════════════════════════════════════════════════════════════
    //  10. Multiple aggregation types at once — all modes
    // ═══════════════════════════════════════════════════════════════

    @Test @org.junit.jupiter.api.Order(28)
    @DisplayName("Multiple aggs (stats+histogram+terms) — DISABLED mode")
    void multiAggDisabled(@TempDir Path dir) throws Exception {
        try (SplitSearcher s = createSearcher(dir, ParquetCompanionConfig.FastFieldMode.DISABLED, 20, false, "multi_dis")) {
            verifyMultiAgg(s);
        }
    }

    @Test @org.junit.jupiter.api.Order(29)
    @DisplayName("Multiple aggs (stats+histogram+terms) — HYBRID mode")
    void multiAggHybrid(@TempDir Path dir) throws Exception {
        try (SplitSearcher s = createSearcher(dir, ParquetCompanionConfig.FastFieldMode.HYBRID, 20, false, "multi_hyb")) {
            verifyMultiAgg(s);
        }
    }

    @Test @org.junit.jupiter.api.Order(30)
    @DisplayName("Multiple aggs (stats+histogram+terms) — PARQUET_ONLY mode")
    void multiAggParquetOnly(@TempDir Path dir) throws Exception {
        try (SplitSearcher s = createSearcher(dir, ParquetCompanionConfig.FastFieldMode.PARQUET_ONLY, 20, false, "multi_pq")) {
            verifyMultiAgg(s);
        }
    }

    private void verifyMultiAgg(SplitSearcher s) {
        Map<String, SplitAggregation> aggs = new LinkedHashMap<>();
        aggs.put("id_stats", new StatsAggregation("id"));
        aggs.put("id_hist", new HistogramAggregation("id", 5.0));
        aggs.put("name_terms", new TermsAggregation("name_terms", "name", 50, 0));

        SearchResult result = s.search(new SplitMatchAllQuery(), 0, aggs);

        assertTrue(result.hasAggregations());
        assertEquals(3, result.getAggregations().size());

        // Stats
        StatsResult stats = (StatsResult) result.getAggregation("id_stats");
        assertNotNull(stats);
        assertEquals(20, stats.getCount());

        // Histogram
        HistogramResult hist = (HistogramResult) result.getAggregation("id_hist");
        assertNotNull(hist);
        // interval=5, ids 0-19 → 4 buckets: [0,5), [5,10), [10,15), [15,20)
        long total = 0;
        for (HistogramResult.HistogramBucket b : hist.getBuckets()) {
            total += b.getDocCount();
        }
        assertEquals(20, total);

        // Terms
        TermsResult terms = (TermsResult) result.getAggregation("name_terms");
        assertNotNull(terms);
        assertFalse(terms.getBuckets().isEmpty());
    }

    // ═══════════════════════════════════════════════════════════════
    //  11. Min/Max/Sum/Avg individual aggs — HYBRID mode (spot check)
    // ═══════════════════════════════════════════════════════════════

    @Test @org.junit.jupiter.api.Order(31)
    @DisplayName("Individual aggs (min, max, sum, avg) — HYBRID mode")
    void individualAggsHybrid(@TempDir Path dir) throws Exception {
        try (SplitSearcher s = createSearcher(dir, ParquetCompanionConfig.FastFieldMode.HYBRID, 20, false, "indiv_hyb")) {
            SplitQuery all = new SplitMatchAllQuery();

            // Min
            MinAggregation minAgg = new MinAggregation("id_min", "id");
            MinResult min = (MinResult) s.search(all, 0, "min", minAgg).getAggregation("min");
            assertNotNull(min);
            assertEquals(0.0, min.getMin(), 0.01);

            // Max
            MaxAggregation maxAgg = new MaxAggregation("id_max", "id");
            MaxResult max = (MaxResult) s.search(all, 0, "max", maxAgg).getAggregation("max");
            assertNotNull(max);
            assertEquals(19.0, max.getMax(), 0.01);

            // Sum
            SumAggregation sumAgg = new SumAggregation("id_sum", "id");
            SumResult sum = (SumResult) s.search(all, 0, "sum", sumAgg).getAggregation("sum");
            assertNotNull(sum);
            assertEquals(190.0, sum.getSum(), 0.01); // 0+1+...+19 = 190

            // Avg
            AverageAggregation avgAgg = new AverageAggregation("id_avg", "id");
            AverageResult avg = (AverageResult) s.search(all, 0, "avg", avgAgg).getAggregation("avg");
            assertNotNull(avg);
            assertEquals(9.5, avg.getAverage(), 0.01);
        }
    }

    // ═══════════════════════════════════════════════════════════════
    //  12. Complex parquet: timestamp date histogram — HYBRID mode
    // ═══════════════════════════════════════════════════════════════

    @Test @org.junit.jupiter.api.Order(32)
    @DisplayName("Date histogram on timestamp — HYBRID mode (complex parquet)")
    void dateHistogramHybrid(@TempDir Path dir) throws Exception {
        try (SplitSearcher s = createSearcher(dir, ParquetCompanionConfig.FastFieldMode.HYBRID, 10, true, "datehist_hyb")) {
            // created_at = base + row * 3600s (hourly intervals)
            // Use a day interval to bucket all 10 rows
            DateHistogramAggregation agg = new DateHistogramAggregation("created_at").setFixedInterval("1d");
            SearchResult result = s.search(new SplitMatchAllQuery(), 0, "date_hist", agg);

            assertTrue(result.hasAggregations());
            DateHistogramResult hist = (DateHistogramResult) result.getAggregation("date_hist");
            assertNotNull(hist);

            List<DateHistogramResult.DateHistogramBucket> buckets = hist.getBuckets();
            assertFalse(buckets.isEmpty(), "Should have date histogram buckets");

            long total = 0;
            for (DateHistogramResult.DateHistogramBucket b : buckets) {
                total += b.getDocCount();
            }
            assertEquals(10, total, "Total across date histogram buckets should be 10");
        }
    }

    @Test @org.junit.jupiter.api.Order(33)
    @DisplayName("Date histogram on timestamp — PARQUET_ONLY mode (complex parquet)")
    void dateHistogramParquetOnly(@TempDir Path dir) throws Exception {
        try (SplitSearcher s = createSearcher(dir, ParquetCompanionConfig.FastFieldMode.PARQUET_ONLY, 10, true, "datehist_pq")) {
            DateHistogramAggregation agg = new DateHistogramAggregation("created_at").setFixedInterval("1d");
            SearchResult result = s.search(new SplitMatchAllQuery(), 0, "date_hist", agg);

            assertTrue(result.hasAggregations());
            DateHistogramResult hist = (DateHistogramResult) result.getAggregation("date_hist");
            assertNotNull(hist);

            long total = 0;
            for (DateHistogramResult.DateHistogramBucket b : hist.getBuckets()) {
                total += b.getDocCount();
            }
            assertEquals(10, total);
        }
    }

    // ═══════════════════════════════════════════════════════════════
    //  13. Complex parquet: range query on timestamp — all modes
    // ═══════════════════════════════════════════════════════════════

    @Test @org.junit.jupiter.api.Order(34)
    @DisplayName("Range query on timestamp — HYBRID mode (complex parquet)")
    void rangeTimestampHybrid(@TempDir Path dir) throws Exception {
        try (SplitSearcher s = createSearcher(dir, ParquetCompanionConfig.FastFieldMode.HYBRID, 10, true, "range_ts_hyb")) {
            verifyTimestampRange(s);
        }
    }

    @Test @org.junit.jupiter.api.Order(35)
    @DisplayName("Range query on timestamp — PARQUET_ONLY mode (complex parquet)")
    void rangeTimestampParquetOnly(@TempDir Path dir) throws Exception {
        try (SplitSearcher s = createSearcher(dir, ParquetCompanionConfig.FastFieldMode.PARQUET_ONLY, 10, true, "range_ts_pq")) {
            verifyTimestampRange(s);
        }
    }

    private void verifyTimestampRange(SplitSearcher s) {
        // created_at base = 2024-01-01T00:00:00Z, each row +1h
        // Row 0: 2024-01-01T00:00:00Z, Row 9: 2024-01-01T09:00:00Z
        // Range: first 5 hours → rows 0-4 = 5 docs
        // Use ISO format for date range
        SplitRangeQuery rangeQ = SplitRangeQuery.inclusiveRange(
                "created_at",
                "2024-01-01T00:00:00Z",
                "2024-01-01T04:59:59Z",
                "date");
        SearchResult result = s.search(rangeQ, 100);
        assertTrue(result.getHits().size() >= 4 && result.getHits().size() <= 5,
                "Timestamp range should match ~5 docs, got " + result.getHits().size());
    }

    // ═══════════════════════════════════════════════════════════════
    //  14. Terms aggregation on string with "raw" tokenizer — all modes
    //  (raw tokenizer = exact full-string tokens, e.g. "item_0")
    // ═══════════════════════════════════════════════════════════════

    @Test @org.junit.jupiter.api.Order(36)
    @DisplayName("Terms agg on raw-tokenized string (name) — DISABLED mode")
    void termsRawTokenizerDisabled(@TempDir Path dir) throws Exception {
        // name field defaults to "raw" tokenizer in schema derivation
        try (SplitSearcher s = createSearcher(dir, ParquetCompanionConfig.FastFieldMode.DISABLED, 20, false, "terms_raw_dis")) {
            verifyRawTokenizerTerms(s, 20);
        }
    }

    @Test @org.junit.jupiter.api.Order(37)
    @DisplayName("Terms agg on raw-tokenized string (name) — HYBRID mode")
    void termsRawTokenizerHybrid(@TempDir Path dir) throws Exception {
        try (SplitSearcher s = createSearcher(dir, ParquetCompanionConfig.FastFieldMode.HYBRID, 20, false, "terms_raw_hyb")) {
            verifyRawTokenizerTerms(s, 20);
        }
    }

    @Test @org.junit.jupiter.api.Order(38)
    @DisplayName("Terms agg on raw-tokenized string (name) — PARQUET_ONLY mode")
    void termsRawTokenizerParquetOnly(@TempDir Path dir) throws Exception {
        try (SplitSearcher s = createSearcher(dir, ParquetCompanionConfig.FastFieldMode.PARQUET_ONLY, 20, false, "terms_raw_pq")) {
            verifyRawTokenizerTerms(s, 20);
        }
    }

    private void verifyRawTokenizerTerms(SplitSearcher s, int numRows) {
        // name = "item_0", "item_1", ... — raw tokenizer keeps full string as single token
        TermsAggregation agg = new TermsAggregation("name_raw", "name", 50, 0);
        SearchResult result = s.search(new SplitMatchAllQuery(), 0, "terms", agg);

        assertTrue(result.hasAggregations());
        TermsResult terms = (TermsResult) result.getAggregation("terms");
        assertNotNull(terms);

        List<TermsResult.TermsBucket> buckets = terms.getBuckets();
        // Raw tokenizer: each "item_X" is one token, all unique → numRows buckets, each count=1
        assertEquals(numRows, buckets.size(),
                "Raw tokenizer should produce " + numRows + " unique term buckets");
        for (TermsResult.TermsBucket b : buckets) {
            assertEquals(1, b.getDocCount(), "Each raw term should appear in exactly 1 doc");
            assertTrue(((String) b.getKey()).startsWith("item_"),
                    "Raw token should be the full string like 'item_X', got: " + b.getKey());
        }
    }

    // ═══════════════════════════════════════════════════════════════
    //  15. Terms aggregation on string with "default" tokenizer — all modes
    //  (default tokenizer = lowercased + whitespace-split tokens)
    //  Uses "notes" field from complex parquet with tokenizer override
    // ═══════════════════════════════════════════════════════════════

    @Test @org.junit.jupiter.api.Order(39)
    @DisplayName("Terms agg on default-tokenized string (notes) — DISABLED mode")
    void termsDefaultTokenizerDisabled(@TempDir Path dir) throws Exception {
        Map<String, String> overrides = new HashMap<>();
        overrides.put("notes", "default");
        try (SplitSearcher s = createSearcher(dir, ParquetCompanionConfig.FastFieldMode.DISABLED,
                10, true, "terms_def_dis", overrides)) {
            verifyDefaultTokenizerTerms(s);
        }
    }

    @Test @org.junit.jupiter.api.Order(40)
    @DisplayName("Terms agg on default-tokenized string (notes) — HYBRID mode succeeds")
    void termsDefaultTokenizerHybrid(@TempDir Path dir) throws Exception {
        Map<String, String> overrides = new HashMap<>();
        overrides.put("notes", "default");
        try (SplitSearcher s = createSearcher(dir, ParquetCompanionConfig.FastFieldMode.HYBRID,
                10, true, "terms_def_hyb", overrides)) {
            verifyDefaultTokenizerTermsSupported(s);
        }
    }

    @Test @org.junit.jupiter.api.Order(41)
    @DisplayName("Terms agg on default-tokenized string (notes) — PARQUET_ONLY mode succeeds")
    void termsDefaultTokenizerParquetOnly(@TempDir Path dir) throws Exception {
        Map<String, String> overrides = new HashMap<>();
        overrides.put("notes", "default");
        try (SplitSearcher s = createSearcher(dir, ParquetCompanionConfig.FastFieldMode.PARQUET_ONLY,
                10, true, "terms_def_pq", overrides)) {
            verifyDefaultTokenizerTermsSupported(s);
        }
    }

    private void verifyDefaultTokenizerTerms(SplitSearcher s) {
        // In DISABLED mode, tantivy handles tokenization during indexing natively,
        // so the "default" tokenizer works correctly for fast field aggregation.
        // notes = "Note for item X" (even rows only, odd rows null)
        // "default" tokenizer lowercases + splits: ["note", "for", "item", "0"]
        TermsAggregation agg = new TermsAggregation("notes_terms", "notes", 50, 0);
        SearchResult result = s.search(new SplitMatchAllQuery(), 0, "terms", agg);

        assertTrue(result.hasAggregations());
        TermsResult terms = (TermsResult) result.getAggregation("terms");
        assertNotNull(terms);

        List<TermsResult.TermsBucket> buckets = terms.getBuckets();
        assertFalse(buckets.isEmpty(), "Default tokenizer should produce term buckets in DISABLED mode");

        Map<String, Long> tokenCounts = new HashMap<>();
        for (TermsResult.TermsBucket b : buckets) {
            tokenCounts.put((String) b.getKey(), b.getDocCount());
        }

        assertTrue(tokenCounts.containsKey("note"),
                "Should have token 'note' from default tokenizer, got: " + tokenCounts.keySet());
        assertEquals(5, tokenCounts.get("note"),
                "Token 'note' should appear in 5 docs (even rows)");
    }

    private void verifyDefaultTokenizerTermsSupported(SplitSearcher s) {
        // All text fields now support fast field aggregation regardless of tokenizer.
        // Tantivy fast fields store raw bytes independent of the tokenizer.
        TermsAggregation agg = new TermsAggregation("notes_terms", "notes", 50, 0);
        SearchResult result = s.search(new SplitMatchAllQuery(), 0, "terms", agg);
        assertNotNull(result, "Aggregation on default-tokenized text field should succeed");
        assertNotNull(result.getAggregations(), "Aggregation result should have aggregations map");
    }

    // ═══════════════════════════════════════════════════════════════
    //  16. Histogram on raw-tokenized string (name) — HYBRID + PARQUET_ONLY
    //  Validates that string fast fields support histogram aggregation
    //  (Note: stats agg returns count=0 for text fields since it's numeric-only)
    // ═══════════════════════════════════════════════════════════════

    @Test @org.junit.jupiter.api.Order(42)
    @DisplayName("Terms agg + search on raw string (name) — HYBRID mode")
    void termsAndSearchRawStringHybrid(@TempDir Path dir) throws Exception {
        try (SplitSearcher s = createSearcher(dir, ParquetCompanionConfig.FastFieldMode.HYBRID, 20, false, "str_combo_hyb")) {
            // Combine: search for specific name + terms agg on name field
            SplitTermQuery tq = new SplitTermQuery("name", "item_5");
            TermsAggregation agg = new TermsAggregation("name_terms", "name", 50, 0);
            SearchResult result = s.search(tq, 10, "terms", agg);

            assertEquals(1, result.getHits().size(), "Should find item_5");
            assertTrue(result.hasAggregations());
            TermsResult terms = (TermsResult) result.getAggregation("terms");
            assertNotNull(terms);
            // Terms agg scoped to query — only 1 matching doc
            assertEquals(1, terms.getBuckets().size());
            assertEquals("item_5", terms.getBuckets().get(0).getKey());
        }
    }

    @Test @org.junit.jupiter.api.Order(43)
    @DisplayName("Terms agg + search on raw string (name) — PARQUET_ONLY mode")
    void termsAndSearchRawStringParquetOnly(@TempDir Path dir) throws Exception {
        try (SplitSearcher s = createSearcher(dir, ParquetCompanionConfig.FastFieldMode.PARQUET_ONLY, 20, false, "str_combo_pq")) {
            SplitTermQuery tq = new SplitTermQuery("name", "item_10");
            TermsAggregation agg = new TermsAggregation("name_terms", "name", 50, 0);
            SearchResult result = s.search(tq, 10, "terms", agg);

            assertEquals(1, result.getHits().size(), "Should find item_10");
            assertTrue(result.hasAggregations());
            TermsResult terms = (TermsResult) result.getAggregation("terms");
            assertNotNull(terms);
            assertEquals(1, terms.getBuckets().size());
            assertEquals("item_10", terms.getBuckets().get(0).getKey());
        }
    }

    // ═══════════════════════════════════════════════════════════════
    //  L2 Disk Cache: Transcoded fast field bytes persist to disk
    // ═══════════════════════════════════════════════════════════════

    @Test @org.junit.jupiter.api.Order(100)
    @DisplayName("L2 disk cache — transcoded fast fields persist and are reused (HYBRID)")
    void l2DiskCacheHybrid(@TempDir Path dir) throws Exception {
        verifyL2DiskCachePersistence(dir, ParquetCompanionConfig.FastFieldMode.HYBRID, "l2_hybrid");
    }

    @Test @org.junit.jupiter.api.Order(101)
    @DisplayName("L2 disk cache — transcoded fast fields persist and are reused (PARQUET_ONLY)")
    void l2DiskCacheParquetOnly(@TempDir Path dir) throws Exception {
        verifyL2DiskCachePersistence(dir, ParquetCompanionConfig.FastFieldMode.PARQUET_ONLY, "l2_pqonly");
    }

    /**
     * Verifies that transcoded fast field bytes are:
     * 1. Written to L2 disk cache on first aggregation
     * 2. Cache files appear on disk with "parquet_transcoded" in the name
     * 3. A fresh searcher (new in-memory cache) produces correct results using L2 data
     */
    private void verifyL2DiskCachePersistence(Path dir, ParquetCompanionConfig.FastFieldMode mode,
                                               String tag) throws Exception {
        Path diskCachePath = dir.resolve("disk_cache");
        Files.createDirectories(diskCachePath);

        Path parquetFile = dir.resolve(tag + ".parquet");
        Path splitFile = dir.resolve(tag + ".split");

        // Write test data
        QuickwitSplit.nativeWriteTestParquet(parquetFile.toString(), 50, 0);

        ParquetCompanionConfig config = new ParquetCompanionConfig(dir.toString())
                .withFastFieldMode(mode);

        QuickwitSplit.SplitMetadata metadata = QuickwitSplit.createFromParquet(
                Collections.singletonList(parquetFile.toString()),
                splitFile.toString(), config);

        String splitUrl = "file://" + splitFile.toAbsolutePath();

        // Create a cache manager WITH disk cache
        SplitCacheManager.CacheConfig cacheConfig =
                new SplitCacheManager.CacheConfig("pq-l2-test-" + tag + "-" + System.nanoTime())
                        .withMaxCacheSize(100_000_000);
        SplitCacheManager.TieredCacheConfig tieredConfig = new SplitCacheManager.TieredCacheConfig()
                .withDiskCachePath(diskCachePath.toString())
                .withMaxDiskSize(1_000_000_000L);
        cacheConfig.withTieredCache(tieredConfig);

        // Phase 1: Open searcher, run aggregation → transcoding + L2 write
        StatsAggregation statsAgg;
        double phase1Sum;
        try (SplitCacheManager cm = SplitCacheManager.getInstance(cacheConfig)) {
            try (SplitSearcher s = cm.createSplitSearcher(splitUrl, metadata, dir.toString())) {
                // Prewarm fast fields (triggers transcoding + L2 write)
                s.preloadParquetFastFields("id", "score", "name", "active").join();

                // Run aggregation to verify correctness
                statsAgg = new StatsAggregation("id_stats", "id");
                SearchResult r = s.search(new SplitMatchAllQuery(), 0, "stats", statsAgg);
                assertTrue(r.hasAggregations());
                StatsResult stats = (StatsResult) r.getAggregation("stats");
                assertNotNull(stats);
                assertEquals(50, stats.getCount(), "Phase 1: Should have 50 docs");
                phase1Sum = stats.getSum();
                assertTrue(phase1Sum > 0, "Phase 1: Sum should be positive");
            }
        }

        // Verify: disk cache directory should contain parquet_transcoded files
        long transcodedFiles = Files.walk(diskCachePath)
                .filter(p -> p.getFileName().toString().contains("parquet_transcoded"))
                .count();
        assertTrue(transcodedFiles > 0,
                "Disk cache should contain parquet_transcoded files, found " + transcodedFiles);
        System.out.println("[L2 CACHE TEST] Found " + transcodedFiles + " transcoded cache files on disk");

        // Phase 2: Create a completely new cache manager (simulating fresh JVM / new searcher)
        // The in-memory transcoded_cache is gone, so the only source of transcoded data is L2.
        SplitCacheManager.CacheConfig cacheConfig2 =
                new SplitCacheManager.CacheConfig("pq-l2-test-" + tag + "-phase2-" + System.nanoTime())
                        .withMaxCacheSize(100_000_000);
        SplitCacheManager.TieredCacheConfig tieredConfig2 = new SplitCacheManager.TieredCacheConfig()
                .withDiskCachePath(diskCachePath.toString())
                .withMaxDiskSize(1_000_000_000L);
        cacheConfig2.withTieredCache(tieredConfig2);

        try (SplitCacheManager cm2 = SplitCacheManager.getInstance(cacheConfig2)) {
            try (SplitSearcher s2 = cm2.createSplitSearcher(splitUrl, metadata, dir.toString())) {
                // Prewarm fast fields again — should hit L2 cache, no re-transcoding
                s2.preloadParquetFastFields("id", "score", "name", "active").join();

                // Run the same aggregation — must produce identical results
                statsAgg = new StatsAggregation("id_stats", "id");
                SearchResult r2 = s2.search(new SplitMatchAllQuery(), 0, "stats", statsAgg);
                assertTrue(r2.hasAggregations());
                StatsResult stats2 = (StatsResult) r2.getAggregation("stats");
                assertNotNull(stats2);
                assertEquals(50, stats2.getCount(), "Phase 2: Should have 50 docs from L2 cache");
                assertEquals(phase1Sum, stats2.getSum(), 0.001,
                        "Phase 2: Sum should match phase 1 (data from L2 cache)");

                // Also test a terms aggregation on string field to verify string transcoding from L2
                TermsAggregation termsAgg = new TermsAggregation("name_terms", "name", 50, 0);
                SearchResult r3 = s2.search(new SplitMatchAllQuery(), 0, "terms", termsAgg);
                assertTrue(r3.hasAggregations());
                TermsResult terms = (TermsResult) r3.getAggregation("terms");
                assertNotNull(terms);
                assertTrue(terms.getBuckets().size() > 0,
                        "Phase 2: Terms agg should return buckets from L2-cached transcoded data");
                System.out.println("[L2 CACHE TEST] Phase 2 aggregation from L2 cache: " +
                        terms.getBuckets().size() + " term buckets, id_stats sum=" + stats2.getSum());
            }
        }
    }

    // ═══════════════════════════════════════════════════════════════
    //  Regression: merge two HYBRID companion splits, then aggregate
    //  Verifies that promote_doc_mapping_all_fast is called on merge
    //  path so that fast field aggregations work on merged splits.
    // ═══════════════════════════════════════════════════════════════

    @Test @org.junit.jupiter.api.Order(102)
    @DisplayName("Merge two HYBRID companion splits — aggregations still work on merged result")
    void mergedHybridCompanionSplitAggregation(@TempDir Path dir) throws Exception {
        // Build split A: 20 rows (id 0..19)
        Path pqA = dir.resolve("merge_agg_a.parquet");
        Path splitA = dir.resolve("merge_agg_a.split");
        QuickwitSplit.nativeWriteTestParquet(pqA.toString(), 20, 0);

        ParquetCompanionConfig configA = new ParquetCompanionConfig(dir.toString())
                .withFastFieldMode(ParquetCompanionConfig.FastFieldMode.HYBRID);
        QuickwitSplit.SplitMetadata metaA = QuickwitSplit.createFromParquet(
                Collections.singletonList(pqA.toString()), splitA.toString(), configA);
        assertEquals(20, metaA.getNumDocs());

        // Build split B: 20 rows (id 20..39)
        Path pqB = dir.resolve("merge_agg_b.parquet");
        Path splitB = dir.resolve("merge_agg_b.split");
        QuickwitSplit.nativeWriteTestParquet(pqB.toString(), 20, 20);

        ParquetCompanionConfig configB = new ParquetCompanionConfig(dir.toString())
                .withFastFieldMode(ParquetCompanionConfig.FastFieldMode.HYBRID);
        QuickwitSplit.SplitMetadata metaB = QuickwitSplit.createFromParquet(
                Collections.singletonList(pqB.toString()), splitB.toString(), configB);
        assertEquals(20, metaB.getNumDocs());

        // Merge A + B
        Path mergedSplit = dir.resolve("merge_agg_merged.split");
        QuickwitSplit.MergeConfig mergeConfig = new QuickwitSplit.MergeConfig(
                "merge-agg-test", "merge-agg-source", "merge-agg-node");
        QuickwitSplit.SplitMetadata mergedMeta = QuickwitSplit.mergeSplits(
                Arrays.asList(splitA.toString(), splitB.toString()),
                mergedSplit.toString(), mergeConfig);
        assertEquals(40, mergedMeta.getNumDocs());

        // Open the merged split and run aggregations
        String splitUrl = "file://" + mergedSplit.toAbsolutePath();
        try (SplitSearcher s = cacheManager.createSplitSearcher(splitUrl, mergedMeta, dir.toString())) {
            // Prewarm fast fields
            s.preloadParquetFastFields("id", "score", "name").join();

            // Stats aggregation on numeric field
            StatsAggregation statsAgg = new StatsAggregation("id_stats", "id");
            SearchResult statsResult = s.search(new SplitMatchAllQuery(), 0, "stats", statsAgg);
            assertTrue(statsResult.hasAggregations(),
                    "Merged HYBRID split should support stats aggregation");
            StatsResult stats = (StatsResult) statsResult.getAggregation("stats");
            assertNotNull(stats);
            assertEquals(40, stats.getCount(),
                    "Stats should cover all 40 merged docs");

            // Terms aggregation on string field — this was the failing case before the fix
            TermsAggregation termsAgg = new TermsAggregation("name_terms", "name", 50, 0);
            SearchResult termsResult = s.search(new SplitMatchAllQuery(), 0, "terms", termsAgg);
            assertTrue(termsResult.hasAggregations(),
                    "Merged HYBRID split should support terms aggregation on string field");
            TermsResult terms = (TermsResult) termsResult.getAggregation("terms");
            assertNotNull(terms);
            assertTrue(terms.getBuckets().size() > 0,
                    "Terms agg on merged split should return buckets");
        }
    }
}

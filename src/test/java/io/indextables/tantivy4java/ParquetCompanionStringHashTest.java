/*
 * Validates the string-hash fast-field optimisation for HYBRID-mode parquet companion splits.
 *
 * In HYBRID mode the indexer pre-computes xxHash64("item_X") and stores it as a native
 * tantivy U64 fast field named `_phash_name`.  At query time the aggregation rewriter
 * transparently substitutes the cheap U64 field for the expensive parquet string column
 * transcoding, then Phase-3 resolves hash bucket keys back to the original strings.
 *
 * Scenarios covered:
 *   1. withStringHashOptimization(false) — parquet-transcode fallback still works
 *   2. withStringHashOptimization(true)  — bucket keys resolved to "item_X" strings
 *   3. hash-on vs hash-off give identical bucket key sets
 *   4. value_count on string field redirected via hash (hash-on)
 *   5. value_count on string field without hash opt (parquet path)
 *   6. terms + numeric sub-aggregation (hash resolution + sub-agg correctness)
 *   7. min_doc_count:0 — not hash-redirected; falls back to parquet transcode
 *   8. include filter — filter values hashed; resolved keys appear in result
 *   9. mixed request (terms + stats + value_count) — hash and non-hash in one shot
 *  10. exclude filter — excluded string values absent from result after hash resolution
 *  11. f64 (score) stats alongside hash-opt string terms — float data type not affected
 *  12. bool (active) value_count alongside hash-opt string terms — bool not redirected
 *  13. order:_key — buckets re-sorted alphabetically after hash resolution
 *  14. exists query on string field — FieldPresence redirected to _phash_name
 *
 * Run:
 *   mvn test -pl . -Dtest=ParquetCompanionStringHashTest
 */
package io.indextables.tantivy4java;

import io.indextables.tantivy4java.aggregation.*;
import io.indextables.tantivy4java.query.*;
import io.indextables.tantivy4java.result.*;
import io.indextables.tantivy4java.split.*;
import io.indextables.tantivy4java.split.merge.*;

import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class ParquetCompanionStringHashTest {

    private static SplitCacheManager cacheManager;

    @BeforeAll
    static void setupCache() {
        SplitCacheManager.CacheConfig config =
                new SplitCacheManager.CacheConfig("pq-hash-test-" + System.nanoTime());
        cacheManager = SplitCacheManager.getInstance(config);
    }

    @AfterAll
    static void teardownCache() {
        if (cacheManager != null) cacheManager.close();
    }

    // ═══════════════════════════════════════════════════════════════
    //  Helper: create a HYBRID-mode split with hash opt on or off
    // ═══════════════════════════════════════════════════════════════

    private SplitSearcher createSearcher(Path dir, boolean hashOpt, int numRows, String tag)
            throws Exception {
        Path parquetFile = dir.resolve(tag + ".parquet");
        Path splitFile   = dir.resolve(tag + ".split");

        QuickwitSplit.nativeWriteTestParquet(parquetFile.toString(), numRows, 0);

        ParquetCompanionConfig config = new ParquetCompanionConfig(dir.toString())
                .withFastFieldMode(ParquetCompanionConfig.FastFieldMode.HYBRID)
                .withStringHashOptimization(hashOpt);

        QuickwitSplit.SplitMetadata metadata = QuickwitSplit.createFromParquet(
                Collections.singletonList(parquetFile.toString()),
                splitFile.toString(), config);

        String splitUrl = "file://" + splitFile.toAbsolutePath();
        SplitSearcher searcher = cacheManager.createSplitSearcher(splitUrl, metadata, dir.toString());
        try {
            searcher.preloadParquetFastFields("id", "score", "name", "active").join();
        } catch (Exception e) {
            searcher.close();
            throw e;
        }
        return searcher;
    }

    /** Collect bucket keys as a sorted list of strings. */
    private static List<String> bucketKeys(TermsResult terms) {
        List<String> keys = new ArrayList<>();
        for (TermsResult.TermsBucket b : terms.getBuckets()) {
            keys.add(b.getKey().toString());
        }
        Collections.sort(keys);
        return keys;
    }

    // ═══════════════════════════════════════════════════════════════
    //  1. Hash optimization disabled — parquet-transcode fallback
    // ═══════════════════════════════════════════════════════════════

    @Test @org.junit.jupiter.api.Order(1)
    @DisplayName("Terms agg — hash opt disabled, parquet-transcode fallback produces correct keys")
    void termsAggHashOptDisabled(@TempDir Path dir) throws Exception {
        try (SplitSearcher s = createSearcher(dir, false, 20, "hash_off")) {
            TermsAggregation agg = new TermsAggregation("name_terms", "name", 50, 0);
            SearchResult result = s.search(new SplitMatchAllQuery(), 0, "terms", agg);

            assertTrue(result.hasAggregations());
            TermsResult terms = (TermsResult) result.getAggregation("terms");
            assertNotNull(terms);

            List<TermsResult.TermsBucket> buckets = terms.getBuckets();
            assertEquals(20, buckets.size(),
                    "Parquet-transcode fallback should return all 20 unique name buckets");
            for (TermsResult.TermsBucket b : buckets) {
                assertEquals(1, b.getDocCount(),
                        "Each name is unique so doc_count should be 1");
                assertTrue(b.getKey() instanceof String,
                        "Bucket key should be a String: " + b.getKey());
                assertTrue(((String) b.getKey()).startsWith("item_"),
                        "Key should match 'item_X' pattern: " + b.getKey());
            }
        }
    }

    // ═══════════════════════════════════════════════════════════════
    //  2. Hash optimization enabled — keys resolved back to strings
    // ═══════════════════════════════════════════════════════════════

    @Test @org.junit.jupiter.api.Order(2)
    @DisplayName("Terms agg — hash opt enabled, bucket keys resolved to original strings")
    void termsAggHashOptEnabled(@TempDir Path dir) throws Exception {
        try (SplitSearcher s = createSearcher(dir, true, 20, "hash_on")) {
            TermsAggregation agg = new TermsAggregation("name_terms", "name", 50, 0);
            SearchResult result = s.search(new SplitMatchAllQuery(), 0, "terms", agg);

            assertTrue(result.hasAggregations());
            TermsResult terms = (TermsResult) result.getAggregation("terms");
            assertNotNull(terms);

            List<TermsResult.TermsBucket> buckets = terms.getBuckets();
            assertEquals(20, buckets.size(),
                    "Hash-opt should return 20 resolved buckets (one per unique name)");
            for (TermsResult.TermsBucket b : buckets) {
                assertEquals(1, b.getDocCount());
                assertTrue(b.getKey() instanceof String,
                        "Phase-3 resolution should produce a String key, not a raw hash number: "
                                + b.getKey());
                assertTrue(((String) b.getKey()).startsWith("item_"),
                        "Resolved key should be the original 'item_X' value: " + b.getKey());
            }
        }
    }

    // ═══════════════════════════════════════════════════════════════
    //  3. Hash-on and hash-off produce identical bucket key sets
    // ═══════════════════════════════════════════════════════════════

    @Test @org.junit.jupiter.api.Order(3)
    @DisplayName("Terms agg — hash-on and hash-off return identical bucket key sets")
    void termsAggHashOnOffIdentical(@TempDir Path dir) throws Exception {
        int numRows = 15;
        try (SplitSearcher sOn  = createSearcher(dir, true,  numRows, "cmp_on");
             SplitSearcher sOff = createSearcher(dir, false, numRows, "cmp_off")) {

            TermsAggregation agg = new TermsAggregation("name_terms", "name", 50, 0);

            TermsResult onTerms  = (TermsResult)
                    sOn .search(new SplitMatchAllQuery(), 0, "terms", agg).getAggregation("terms");
            TermsResult offTerms = (TermsResult)
                    sOff.search(new SplitMatchAllQuery(), 0, "terms", agg).getAggregation("terms");

            assertNotNull(onTerms);
            assertNotNull(offTerms);

            List<String> onKeys  = bucketKeys(onTerms);
            List<String> offKeys = bucketKeys(offTerms);

            assertEquals(numRows, onKeys.size(),
                    "Hash-on should produce " + numRows + " buckets");
            assertEquals(offKeys, onKeys,
                    "Hash-on and hash-off must produce the same sorted key list");

            // Sanity: doc counts should also match
            Map<String, Long> onCounts  = new LinkedHashMap<>();
            Map<String, Long> offCounts = new LinkedHashMap<>();
            for (TermsResult.TermsBucket b : onTerms .getBuckets()) onCounts .put(b.getKey().toString(), b.getDocCount());
            for (TermsResult.TermsBucket b : offTerms.getBuckets()) offCounts.put(b.getKey().toString(), b.getDocCount());
            assertEquals(offCounts, onCounts, "Doc counts should match between hash-on and hash-off");
        }
    }

    // ═══════════════════════════════════════════════════════════════
    //  4. value_count on string field — hash redirect in HYBRID mode
    // ═══════════════════════════════════════════════════════════════

    @Test @org.junit.jupiter.api.Order(4)
    @DisplayName("value_count on string field — hash-opt redirects to _phash_name, count is correct")
    void valueCountStringFieldHashOn(@TempDir Path dir) throws Exception {
        try (SplitSearcher s = createSearcher(dir, true, 20, "vc_on")) {
            CountAggregation agg = new CountAggregation("name_vc", "name");
            SearchResult result = s.search(new SplitMatchAllQuery(), 0, "vc", agg);

            assertTrue(result.hasAggregations());
            CountResult count = (CountResult) result.getAggregation("vc");
            assertNotNull(count);
            assertEquals(20, count.getCount(),
                    "value_count via hash redirect should count all 20 non-null name values");
        }
    }

    // ═══════════════════════════════════════════════════════════════
    //  5. value_count on string field — parquet-transcode fallback
    // ═══════════════════════════════════════════════════════════════

    @Test @org.junit.jupiter.api.Order(5)
    @DisplayName("value_count on string field — hash opt off, parquet-transcode gives same count")
    void valueCountStringFieldHashOff(@TempDir Path dir) throws Exception {
        try (SplitSearcher s = createSearcher(dir, false, 20, "vc_off")) {
            CountAggregation agg = new CountAggregation("name_vc", "name");
            SearchResult result = s.search(new SplitMatchAllQuery(), 0, "vc", agg);

            assertTrue(result.hasAggregations());
            CountResult count = (CountResult) result.getAggregation("vc");
            assertNotNull(count);
            assertEquals(20, count.getCount(),
                    "value_count without hash opt should also return 20 (parquet transcode path)");
        }
    }

    // ═══════════════════════════════════════════════════════════════
    //  6. Terms with sub-aggregation: hash resolution + inner agg
    // ═══════════════════════════════════════════════════════════════

    @Test @org.junit.jupiter.api.Order(6)
    @DisplayName("Terms + sub-agg (stats on score) — outer keys resolved, inner stats correct")
    void termsAggWithSubAggregation(@TempDir Path dir) throws Exception {
        int numRows = 10;
        try (SplitSearcher s = createSearcher(dir, true, numRows, "subagg")) {
            // Outer: terms on "name" (redirected to _phash_name)
            // Inner: stats on "score" (numeric, not redirected)
            TermsAggregation agg = new TermsAggregation("name_terms", "name", 50, 0)
                    .addSubAggregation("score_stats", new StatsAggregation("score_stats", "score"));
            SearchResult result = s.search(new SplitMatchAllQuery(), 0, "terms", agg);

            assertTrue(result.hasAggregations());
            TermsResult terms = (TermsResult) result.getAggregation("terms");
            assertNotNull(terms);
            assertEquals(numRows, terms.getBuckets().size(),
                    "Should have " + numRows + " buckets with resolved string keys");

            for (TermsResult.TermsBucket bucket : terms.getBuckets()) {
                // Outer key must be a resolved string
                assertTrue(bucket.getKey() instanceof String,
                        "Outer bucket key should be a resolved string: " + bucket.getKey());
                assertTrue(((String) bucket.getKey()).startsWith("item_"),
                        "Resolved key should be 'item_X': " + bucket.getKey());
                assertEquals(1, bucket.getDocCount(),
                        "Each name is unique so doc_count should be 1");

                // Inner sub-agg must be present and correct
                AggregationResult subResult = bucket.getSubAggregation("score_stats");
                assertNotNull(subResult,
                        "sub-aggregation 'score_stats' should be present for each bucket");
                assertTrue(subResult instanceof StatsResult,
                        "Sub-agg should be a StatsResult");
                StatsResult subStats = (StatsResult) subResult;

                // score = id * 1.5 + 10.0; ids 0..9 → scores 10.0..23.5
                assertEquals(1, subStats.getCount(),
                        "Each name bucket covers 1 doc, so sub-stats count should be 1");
                assertTrue(subStats.getMin() >= 10.0 && subStats.getMin() <= 23.5,
                        "Sub-agg score should be in valid range [10.0, 23.5]: " + subStats.getMin());
            }
        }
    }

    // ═══════════════════════════════════════════════════════════════
    //  7. min_doc_count: 0 — hash rewriter skips, falls back to transcode
    // ═══════════════════════════════════════════════════════════════

    @Test @org.junit.jupiter.api.Order(7)
    @DisplayName("Terms with min_doc_count:0 — not hash-redirected, parquet transcode fallback")
    void termsAggMinDocCountZeroFallback(@TempDir Path dir) throws Exception {
        try (SplitSearcher s = createSearcher(dir, true, 20, "mdc0")) {
            // TermsAggregation doesn't expose min_doc_count, so we use a custom aggregation
            SplitAggregation minDocCount0Agg = new SplitAggregation("name_mdc0") {
                @Override
                public String toAggregationJson() {
                    // min_doc_count: 0 prevents hash redirection (needs full dictionary)
                    return "{\"terms\": {\"field\": \"name\", \"size\": 50, \"min_doc_count\": 0}}";
                }
                @Override public String getFieldName()      { return "name"; }
                @Override public String getAggregationType() { return "terms"; }
            };

            SearchResult result = s.search(new SplitMatchAllQuery(), 0, "terms", minDocCount0Agg);

            assertTrue(result.hasAggregations());
            TermsResult terms = (TermsResult) result.getAggregation("terms");
            assertNotNull(terms);

            // With 20 unique names all having doc_count >= 1, min_doc_count: 0 returns same set
            assertEquals(20, terms.getBuckets().size(),
                    "min_doc_count:0 via parquet transcode should still return all 20 buckets");
            for (TermsResult.TermsBucket b : terms.getBuckets()) {
                assertTrue(b.getKey() instanceof String,
                        "Keys should be strings from parquet transcode path: " + b.getKey());
                assertTrue(((String) b.getKey()).startsWith("item_"),
                        "Keys should be 'item_X': " + b.getKey());
            }
        }
    }

    // ═══════════════════════════════════════════════════════════════
    //  8. include filter — filter values get hashed; keys resolved back
    // ═══════════════════════════════════════════════════════════════

    @Test @org.junit.jupiter.api.Order(8)
    @DisplayName("Terms with include filter — include values hashed, resolved keys returned")
    void termsAggIncludeFilter(@TempDir Path dir) throws Exception {
        try (SplitSearcher s = createSearcher(dir, true, 20, "include")) {
            // Request only item_3 and item_7 via include filter.
            // The rewriter hashes ["item_3","item_7"] to U64 values and puts them
            // in the include array of the _phash_name terms agg.
            // Phase 3 resolves the resulting buckets back to strings.
            SplitAggregation includeAgg = new SplitAggregation("name_include") {
                @Override
                public String toAggregationJson() {
                    return "{\"terms\": {\"field\": \"name\", \"size\": 50, "
                            + "\"include\": [\"item_3\", \"item_7\"]}}";
                }
                @Override public String getFieldName()      { return "name"; }
                @Override public String getAggregationType() { return "terms"; }
            };

            SearchResult result = s.search(new SplitMatchAllQuery(), 0, "terms", includeAgg);

            assertTrue(result.hasAggregations());
            TermsResult terms = (TermsResult) result.getAggregation("terms");
            assertNotNull(terms);

            // Only item_3 and item_7 should appear after include filter + hash resolution
            List<TermsResult.TermsBucket> buckets = terms.getBuckets();
            assertEquals(2, buckets.size(),
                    "include filter [item_3, item_7] should produce exactly 2 buckets, got: "
                            + bucketKeys(terms));

            Set<String> keys = new HashSet<>();
            for (TermsResult.TermsBucket b : buckets) {
                assertTrue(b.getKey() instanceof String,
                        "Key should be a resolved string: " + b.getKey());
                keys.add((String) b.getKey());
                assertEquals(1, b.getDocCount(), "Each name is unique");
            }
            assertTrue(keys.contains("item_3"),
                    "Result should contain 'item_3' after hash resolution");
            assertTrue(keys.contains("item_7"),
                    "Result should contain 'item_7' after hash resolution");
        }
    }

    // ═══════════════════════════════════════════════════════════════
    //  9. Mixed aggregations — hash-redirected and numeric in one request
    // ═══════════════════════════════════════════════════════════════

    @Test @org.junit.jupiter.api.Order(9)
    @DisplayName("Mixed aggs: terms(hash) + stats(i64) + value_count(hash) in one request")
    void mixedAggHashAndNumeric(@TempDir Path dir) throws Exception {
        try (SplitSearcher s = createSearcher(dir, true, 20, "mixed")) {
            Map<String, SplitAggregation> aggs = new LinkedHashMap<>();
            aggs.put("name_terms", new TermsAggregation("name_terms", "name", 50, 0));
            aggs.put("id_stats",   new StatsAggregation("id_stats", "id"));
            aggs.put("name_count", new CountAggregation("name_count", "name"));

            SearchResult result = s.search(new SplitMatchAllQuery(), 0, aggs);

            assertTrue(result.hasAggregations());
            assertEquals(3, result.getAggregations().size());

            // Hash-redirected terms agg — keys must be resolved strings
            TermsResult terms = (TermsResult) result.getAggregation("name_terms");
            assertNotNull(terms);
            assertEquals(20, terms.getBuckets().size(),
                    "Mixed request: terms agg should return 20 hash-resolved buckets");
            for (TermsResult.TermsBucket b : terms.getBuckets()) {
                assertTrue(b.getKey() instanceof String,
                        "Key should be a resolved string: " + b.getKey());
                assertTrue(((String) b.getKey()).startsWith("item_"));
            }

            // Numeric stats agg — not redirected, should still work correctly
            StatsResult stats = (StatsResult) result.getAggregation("id_stats");
            assertNotNull(stats);
            assertEquals(20, stats.getCount());
            assertEquals(0.0,  stats.getMin(), 0.01, "min id should be 0");
            assertEquals(19.0, stats.getMax(), 0.01, "max id should be 19");
            assertEquals(190.0, stats.getSum(), 0.01, "sum 0+1+...+19 = 190");

            // Hash-redirected value_count — count should be 20
            CountResult count = (CountResult) result.getAggregation("name_count");
            assertNotNull(count);
            assertEquals(20, count.getCount(),
                    "Mixed request: value_count via hash redirect should return 20");
        }
    }

    // ═══════════════════════════════════════════════════════════════
    // 10. Exclude filter — excluded strings absent after hash resolution
    // ═══════════════════════════════════════════════════════════════

    @Test @org.junit.jupiter.api.Order(10)
    @DisplayName("Terms with exclude filter — excluded string values absent from resolved result")
    void termsAggExcludeFilter(@TempDir Path dir) throws Exception {
        try (SplitSearcher s = createSearcher(dir, true, 20, "exclude")) {
            SplitAggregation excludeAgg = new SplitAggregation("name_exclude") {
                @Override
                public String toAggregationJson() {
                    return "{\"terms\": {\"field\": \"name\", \"size\": 50, "
                            + "\"exclude\": [\"item_0\", \"item_1\", \"item_2\"]}}";
                }
                @Override public String getFieldName()       { return "name"; }
                @Override public String getAggregationType() { return "terms"; }
            };

            SearchResult result = s.search(new SplitMatchAllQuery(), 0, "terms", excludeAgg);

            assertTrue(result.hasAggregations());
            TermsResult terms = (TermsResult) result.getAggregation("terms");
            assertNotNull(terms);

            List<TermsResult.TermsBucket> buckets = terms.getBuckets();
            // 20 total - 3 excluded = 17 buckets
            assertEquals(17, buckets.size(),
                    "Exclude filter [item_0, item_1, item_2] should produce 17 buckets, got: "
                            + bucketKeys(terms));

            Set<String> keys = new HashSet<>();
            for (TermsResult.TermsBucket b : buckets) {
                assertTrue(b.getKey() instanceof String,
                        "Key should be a resolved string: " + b.getKey());
                keys.add((String) b.getKey());
            }
            assertFalse(keys.contains("item_0"), "Excluded 'item_0' must not appear in result");
            assertFalse(keys.contains("item_1"), "Excluded 'item_1' must not appear in result");
            assertFalse(keys.contains("item_2"), "Excluded 'item_2' must not appear in result");
            // All remaining items should be present
            for (int i = 3; i < 20; i++) {
                assertTrue(keys.contains("item_" + i),
                        "Non-excluded 'item_" + i + "' should be present");
            }
        }
    }

    // ═══════════════════════════════════════════════════════════════
    // 11. f64 data type: stats on score alongside hash-opt string terms
    //     Validates float field is not affected by the hash rewriter.
    // ═══════════════════════════════════════════════════════════════

    @Test @org.junit.jupiter.api.Order(11)
    @DisplayName("f64 stats on score alongside hash-opt terms on name — float data type unaffected")
    void f64StatsAlongsideHashOptTerms(@TempDir Path dir) throws Exception {
        try (SplitSearcher s = createSearcher(dir, true, 20, "f64_mix")) {
            Map<String, SplitAggregation> aggs = new LinkedHashMap<>();
            // Hash-redirected string agg
            aggs.put("name_terms", new TermsAggregation("name_terms", "name", 50, 0));
            // Native f64 fast field agg — must NOT be redirected to a hash field
            aggs.put("score_stats", new StatsAggregation("score_stats", "score"));

            SearchResult result = s.search(new SplitMatchAllQuery(), 0, aggs);

            assertTrue(result.hasAggregations());
            assertEquals(2, result.getAggregations().size());

            // String terms — hash resolved
            TermsResult terms = (TermsResult) result.getAggregation("name_terms");
            assertNotNull(terms);
            assertEquals(20, terms.getBuckets().size());
            for (TermsResult.TermsBucket b : terms.getBuckets()) {
                assertTrue(((String) b.getKey()).startsWith("item_"),
                        "Hash-resolved key should be 'item_X': " + b.getKey());
            }

            // Float stats — native fast field, hash opt must not corrupt values
            // score = id * 1.5 + 10.0, ids 0..19 → min=10.0, max=38.5, count=20
            StatsResult stats = (StatsResult) result.getAggregation("score_stats");
            assertNotNull(stats);
            assertEquals(20, stats.getCount(), "All 20 docs should be counted in score stats");
            assertEquals(10.0, stats.getMin(), 0.01, "min score should be 10.0 (id=0)");
            assertEquals(38.5, stats.getMax(), 0.01, "max score should be 38.5 (id=19)");
        }
    }

    // ═══════════════════════════════════════════════════════════════
    // 12. bool data type: value_count on active alongside hash-opt terms
    //     The bool field is a native tantivy fast field in HYBRID mode;
    //     the hash rewriter must not redirect it (it has no _phash_active field).
    // ═══════════════════════════════════════════════════════════════

    @Test @org.junit.jupiter.api.Order(12)
    @DisplayName("bool value_count on active alongside hash-opt terms on name — bool not redirected")
    void boolValueCountAlongsideHashOptTerms(@TempDir Path dir) throws Exception {
        try (SplitSearcher s = createSearcher(dir, true, 20, "bool_mix")) {
            Map<String, SplitAggregation> aggs = new LinkedHashMap<>();
            // Hash-redirected string agg
            aggs.put("name_terms", new TermsAggregation("name_terms", "name", 50, 0));
            // Bool field value_count — active is Boolean, must use native fast field path
            aggs.put("active_count", new CountAggregation("active_count", "active"));

            SearchResult result = s.search(new SplitMatchAllQuery(), 0, aggs);

            assertTrue(result.hasAggregations());
            assertEquals(2, result.getAggregations().size());

            // String terms — hash resolved
            TermsResult terms = (TermsResult) result.getAggregation("name_terms");
            assertNotNull(terms);
            assertEquals(20, terms.getBuckets().size());

            // Bool value_count — active is non-null for all 20 rows
            // (active = i % 2 == 0; all rows have a value, none null)
            CountResult count = (CountResult) result.getAggregation("active_count");
            assertNotNull(count, "value_count on bool field should return a result");
            assertEquals(20, count.getCount(),
                    "value_count on non-nullable bool field should be 20");
        }
    }

    // ═══════════════════════════════════════════════════════════════
    // 13. order:_key — buckets re-sorted alphabetically after hash resolution
    //     Without the fix, Tantivy sorts by U64 hash value (effectively random).
    //     With needs_resort, the result is re-sorted by resolved string key.
    // ═══════════════════════════════════════════════════════════════

    @Test @org.junit.jupiter.api.Order(13)
    @DisplayName("Terms with order:_key — buckets sorted alphabetically by resolved string key")
    void termsAggOrderByKey(@TempDir Path dir) throws Exception {
        try (SplitSearcher s = createSearcher(dir, true, 20, "order_key")) {
            SplitAggregation orderKeyAgg = new SplitAggregation("name_ordered") {
                @Override
                public String toAggregationJson() {
                    return "{\"terms\": {\"field\": \"name\", \"size\": 50, "
                            + "\"order\": {\"_key\": \"asc\"}}}";
                }
                @Override public String getFieldName()       { return "name"; }
                @Override public String getAggregationType() { return "terms"; }
            };

            SearchResult result = s.search(new SplitMatchAllQuery(), 0, "terms", orderKeyAgg);

            assertTrue(result.hasAggregations());
            TermsResult terms = (TermsResult) result.getAggregation("terms");
            assertNotNull(terms);

            List<TermsResult.TermsBucket> buckets = terms.getBuckets();
            assertEquals(20, buckets.size(),
                    "order:_key should return all 20 buckets");

            // Verify keys are in ascending alphabetical order.
            // "item_0", "item_1", "item_10", "item_11", ..., "item_19", "item_2", ..., "item_9"
            List<String> actualKeys = new ArrayList<>();
            for (TermsResult.TermsBucket b : buckets) {
                assertTrue(b.getKey() instanceof String,
                        "Key should be a resolved string: " + b.getKey());
                actualKeys.add((String) b.getKey());
            }

            List<String> expectedKeys = new ArrayList<>(actualKeys);
            Collections.sort(expectedKeys);

            assertEquals(expectedKeys, actualKeys,
                    "With order:{_key:asc}, buckets must be sorted alphabetically by resolved string key");
        }
    }

    // ═══════════════════════════════════════════════════════════════
    //  14. Exists query on hash-optimized string field
    //     SplitExistsQuery("name") should match all docs — the query
    //     is transparently redirected to FieldPresence("_phash_name")
    //     which avoids parquet string transcoding.
    // ═══════════════════════════════════════════════════════════════

    @Test @org.junit.jupiter.api.Order(14)
    @DisplayName("Exists query on hash-opt string field — redirected to _phash_name")
    void existsQueryOnHashField(@TempDir Path dir) throws Exception {
        int numRows = 20;

        // First verify exists query works on a numeric field (score)
        // which is always a native fast field and doesn't involve hash rewriting
        try (SplitSearcher s = createSearcher(dir, true, numRows, "exists_hash_num")) {
            SplitExistsQuery existsScore = new SplitExistsQuery("score");
            SearchResult numResult = s.search(existsScore, numRows);
            assertEquals(numRows, numResult.getHits().size(),
                    "Exists query on numeric field 'score' should match all " + numRows + " docs");
        }

        // Now test the hash-optimized path: exists on string field "name" is
        // rewritten to FieldPresence("_phash_name") — same result, no parquet transcode
        try (SplitSearcher s = createSearcher(dir, true, numRows, "exists_hash_str")) {
            SplitExistsQuery existsQuery = new SplitExistsQuery("name");
            SearchResult result = s.search(existsQuery, numRows);

            assertEquals(numRows, result.getHits().size(),
                    "Exists query on name should match all " + numRows + " docs");
        }
    }
}

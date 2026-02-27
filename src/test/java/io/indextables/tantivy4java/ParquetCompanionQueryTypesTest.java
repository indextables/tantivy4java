/*
 * Tests boolean, wildcard, phrase, and timestamp range query types against
 * parquet companion splits, plus the 4-arg createSplitSearcher overload.
 *
 * Run:
 *   mvn test -pl . -Dtest=ParquetCompanionQueryTypesTest
 */
package io.indextables.tantivy4java;

import io.indextables.tantivy4java.core.*;
import io.indextables.tantivy4java.result.*;
import io.indextables.tantivy4java.split.*;
import io.indextables.tantivy4java.split.merge.*;

import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class ParquetCompanionQueryTypesTest {

    private static SplitCacheManager cacheManager;

    @BeforeAll
    static void setupCache() {
        SplitCacheManager.CacheConfig config =
                new SplitCacheManager.CacheConfig("pq-qtypes-test-" + System.nanoTime());
        cacheManager = SplitCacheManager.getInstance(config);
    }

    @AfterAll
    static void teardownCache() {
        if (cacheManager != null) cacheManager.close();
    }

    // ═══════════════════════════════════════════════════════════════
    //  Helpers
    // ═══════════════════════════════════════════════════════════════

    /**
     * Create a companion split with "raw" tokenizer (default).
     * 50 rows: id 0..49, name "item_0".."item_49", score, active (0/1).
     */
    private SplitSearcher createSearcher(Path dir, String tag) throws Exception {
        Path parquetFile = dir.resolve(tag + ".parquet");
        Path splitFile = dir.resolve(tag + ".split");

        QuickwitSplit.nativeWriteTestParquet(parquetFile.toString(), 50, 0);

        ParquetCompanionConfig config = new ParquetCompanionConfig(dir.toString())
                .withStatisticsFields("id");

        QuickwitSplit.SplitMetadata metadata = QuickwitSplit.createFromParquet(
                Collections.singletonList(parquetFile.toString()),
                splitFile.toString(), config);

        String splitUrl = "file://" + splitFile.toAbsolutePath();
        return cacheManager.createSplitSearcher(splitUrl, metadata, dir.toString());
    }

    /**
     * Create a companion split with "default" tokenizer on the "name" field.
     * The default tokenizer splits "item_0" into tokens ["item", "0"],
     * enabling phrase queries.
     */
    private SplitSearcher createSearcherWithDefaultTokenizer(Path dir, String tag) throws Exception {
        Path parquetFile = dir.resolve(tag + ".parquet");
        Path splitFile = dir.resolve(tag + ".split");

        QuickwitSplit.nativeWriteTestParquet(parquetFile.toString(), 50, 0);

        ParquetCompanionConfig config = new ParquetCompanionConfig(dir.toString())
                .withTokenizerOverrides(Collections.singletonMap("name", "default"))
                .withStatisticsFields("id");

        QuickwitSplit.SplitMetadata metadata = QuickwitSplit.createFromParquet(
                Collections.singletonList(parquetFile.toString()),
                splitFile.toString(), config);

        String splitUrl = "file://" + splitFile.toAbsolutePath();
        return cacheManager.createSplitSearcher(splitUrl, metadata, dir.toString());
    }

    // ═══════════════════════════════════════════════════════════════
    //  Boolean queries (raw tokenizer)
    // ═══════════════════════════════════════════════════════════════

    @Test @Order(1)
    @DisplayName("Boolean MUST: term + range intersection")
    void testBooleanMustCombination(@TempDir Path dir) throws Exception {
        try (SplitSearcher s = createSearcher(dir, "bool_must")) {
            // item_0 has id=0, which is in [0, 10]
            SplitQuery termQ = new SplitTermQuery("name", "item_0");
            SplitQuery rangeQ = SplitRangeQuery.inclusiveRange("id", "0", "10", "i64");

            SplitBooleanQuery boolQ = new SplitBooleanQuery()
                    .addMust(termQ)
                    .addMust(rangeQ);

            SearchResult result = s.search(boolQ, 100);
            assertEquals(1, result.getHits().size(),
                    "Only item_0 matches both the term and the range");
        }
    }

    @Test @Order(2)
    @DisplayName("Boolean MUST_NOT: all except one")
    void testBooleanMustNot(@TempDir Path dir) throws Exception {
        try (SplitSearcher s = createSearcher(dir, "bool_mustnot")) {
            SplitBooleanQuery boolQ = new SplitBooleanQuery()
                    .addMust(new SplitMatchAllQuery())
                    .addMustNot(new SplitTermQuery("name", "item_5"));

            SearchResult result = s.search(boolQ, 100);
            assertEquals(49, result.getHits().size(),
                    "All 50 docs minus the one excluded");
        }
    }

    @Test @Order(3)
    @DisplayName("Boolean SHOULD with minShouldMatch=1")
    void testBooleanShouldWithMinShouldMatch(@TempDir Path dir) throws Exception {
        try (SplitSearcher s = createSearcher(dir, "bool_should")) {
            SplitBooleanQuery boolQ = new SplitBooleanQuery()
                    .addShould(new SplitTermQuery("name", "item_0"))
                    .addShould(new SplitTermQuery("name", "item_1"))
                    .setMinimumShouldMatch(1);

            SearchResult result = s.search(boolQ, 100);
            assertEquals(2, result.getHits().size(),
                    "Exactly item_0 and item_1 should match");
        }
    }

    // ═══════════════════════════════════════════════════════════════
    //  Wildcard queries (raw tokenizer — whole value is one token)
    // ═══════════════════════════════════════════════════════════════

    @Test @Order(4)
    @DisplayName("Wildcard prefix: item_1*")
    void testWildcardPrefix(@TempDir Path dir) throws Exception {
        try (SplitSearcher s = createSearcher(dir, "wc_prefix")) {
            SplitQuery q = new SplitWildcardQuery("name", "item_1*");
            SearchResult result = s.search(q, 100);
            // item_1, item_10, item_11, ..., item_19 = 11 hits
            assertEquals(11, result.getHits().size(),
                    "item_1 plus item_10..item_19");
        }
    }

    @Test @Order(5)
    @DisplayName("Wildcard single char: item_?")
    void testWildcardSingleChar(@TempDir Path dir) throws Exception {
        try (SplitSearcher s = createSearcher(dir, "wc_single")) {
            SplitQuery q = new SplitWildcardQuery("name", "item_?");
            SearchResult result = s.search(q, 100);
            // item_0 through item_9 = 10 hits
            assertEquals(10, result.getHits().size(),
                    "item_0 through item_9");
        }
    }

    @Test @Order(6)
    @DisplayName("Wildcard leading: *_49")
    void testWildcardLeading(@TempDir Path dir) throws Exception {
        try (SplitSearcher s = createSearcher(dir, "wc_leading")) {
            SplitQuery q = new SplitWildcardQuery("name", "*_49");
            SearchResult result = s.search(q, 100);
            assertEquals(1, result.getHits().size(),
                    "Only item_49 ends with _49");
        }
    }

    // ═══════════════════════════════════════════════════════════════
    //  Phrase queries (default tokenizer splits "item_0" → ["item","0"])
    // ═══════════════════════════════════════════════════════════════

    @Test @Order(7)
    @DisplayName("Phrase query match: \"item\" \"0\"")
    void testPhraseQueryMatch(@TempDir Path dir) throws Exception {
        try (SplitSearcher s = createSearcherWithDefaultTokenizer(dir, "phrase_match")) {
            SplitQuery q = new SplitPhraseQuery("name", "item", "0");
            SearchResult result = s.search(q, 100);
            assertEquals(1, result.getHits().size(),
                    "Only item_0 tokenizes to [item, 0]");
        }
    }

    @Test @Order(8)
    @DisplayName("Phrase query no match: \"item\" \"99\"")
    void testPhraseQueryNoMatch(@TempDir Path dir) throws Exception {
        try (SplitSearcher s = createSearcherWithDefaultTokenizer(dir, "phrase_nomatch")) {
            SplitQuery q = new SplitPhraseQuery("name", "item", "99");
            SearchResult result = s.search(q, 100);
            assertEquals(0, result.getHits().size(),
                    "No item_99 in 50-row dataset");
        }
    }

    // ═══════════════════════════════════════════════════════════════
    //  4-arg createSplitSearcher overload
    // ═══════════════════════════════════════════════════════════════

    @Test @Order(9)
    @DisplayName("4-arg createSplitSearcher with null storageConfig")
    void testFourArgCreateSplitSearcher(@TempDir Path dir) throws Exception {
        Path parquetFile = dir.resolve("fourarg.parquet");
        Path splitFile = dir.resolve("fourarg.split");

        QuickwitSplit.nativeWriteTestParquet(parquetFile.toString(), 50, 0);

        ParquetCompanionConfig config = new ParquetCompanionConfig(dir.toString());
        QuickwitSplit.SplitMetadata metadata = QuickwitSplit.createFromParquet(
                Collections.singletonList(parquetFile.toString()),
                splitFile.toString(), config);

        String splitUrl = "file://" + splitFile.toAbsolutePath();

        // Use the 4-arg overload: (splitUrl, metadata, tableRoot, storageConfig)
        try (SplitSearcher s = cacheManager.createSplitSearcher(
                splitUrl, metadata, dir.toString(), null)) {

            assertTrue(s.hasParquetCompanion(), "Should have parquet companion");

            SplitQuery q = new SplitTermQuery("name", "item_25");
            SearchResult result = s.search(q, 10);
            assertEquals(1, result.getHits().size(),
                    "4-arg searcher should find item_25");
        }
    }

    // ═══════════════════════════════════════════════════════════════
    //  Timestamp range queries (all-types parquet with ts_val & event_time)
    //
    //  ts_val:     base 2024-01-01T00:00:00Z + i hours, null at i%8==7
    //  event_time: base 2024-02-01T00:00:00Z + i minutes, null at i%10==9
    //
    //  50 rows, so ts_val spans 2024-01-01 00:00 .. 2024-01-03 01:00 (49 hours)
    //  ts_val null at: 7, 15, 23, 31, 39, 47 (6 nulls, 44 non-null)
    //  event_time spans 2024-02-01 00:00 .. 2024-02-01 00:49 (49 minutes)
    //  event_time null at: 9, 19, 29, 39, 49 (5 nulls, 45 non-null)
    // ═══════════════════════════════════════════════════════════════

    private SplitSearcher createAllTypesSearcher(Path dir, String tag) throws Exception {
        Path parquetFile = dir.resolve(tag + ".parquet");
        Path splitFile = dir.resolve(tag + ".split");

        QuickwitSplit.nativeWriteTestParquetAllTypes(parquetFile.toString(), 50, 0);

        ParquetCompanionConfig config = new ParquetCompanionConfig(dir.toString())
                .withIpAddressFields("ip_val", "src_ip");

        QuickwitSplit.SplitMetadata metadata = QuickwitSplit.createFromParquet(
                Collections.singletonList(parquetFile.toString()),
                splitFile.toString(), config);

        String splitUrl = "file://" + splitFile.toAbsolutePath();
        return cacheManager.createSplitSearcher(splitUrl, metadata, dir.toString());
    }

    @Test @Order(10)
    @DisplayName("Timestamp range: all ts_val on 2024-01-01")
    void testTimestampRangeDay1(@TempDir Path dir) throws Exception {
        // ts_val rows 0-23 span 2024-01-01 00:00 to 2024-01-01 23:00
        // Non-null rows on Jan 1: 0,1,2,3,4,5,6, 8,9,10,11,12,13,14, 16,17,18,19,20,21,22 = 21 rows
        // (rows 7, 15, 23 are null; row 23 = 2024-01-01 23:00 is also null)
        try (SplitSearcher s = createAllTypesSearcher(dir, "ts_range_day1")) {
            SplitRangeQuery q = SplitRangeQuery.inclusiveRange(
                    "ts_val", "2024-01-01T00:00:00Z", "2024-01-01T23:59:59Z", "date");
            SearchResult result = s.search(q, 100);
            System.out.println("ts_val on 2024-01-01: " + result.getHits().size() + " hits");
            // Rows 0-23: 24 total, minus nulls at 7 and 15 = 22 non-null
            // Row 23 (2024-01-01 23:00) is null so only 21 non-null
            assertEquals(21, result.getHits().size(),
                    "Should find 21 non-null ts_val rows on 2024-01-01");
        }
    }

    @Test @Order(11)
    @DisplayName("Timestamp range: ts_val on 2024-01-02")
    void testTimestampRangeDay2(@TempDir Path dir) throws Exception {
        // ts_val rows 24-47 span 2024-01-02 00:00 to 2024-01-02 23:00
        // Non-null: 24 rows minus nulls at 31, 39, 47 = 21 non-null
        try (SplitSearcher s = createAllTypesSearcher(dir, "ts_range_day2")) {
            SplitRangeQuery q = SplitRangeQuery.inclusiveRange(
                    "ts_val", "2024-01-02T00:00:00Z", "2024-01-02T23:59:59Z", "date");
            SearchResult result = s.search(q, 100);
            System.out.println("ts_val on 2024-01-02: " + result.getHits().size() + " hits");
            assertEquals(21, result.getHits().size(),
                    "Should find 21 non-null ts_val rows on 2024-01-02");
        }
    }

    @Test @Order(12)
    @DisplayName("Timestamp range: ts_val narrow window (first 6 hours)")
    void testTimestampRangeNarrow(@TempDir Path dir) throws Exception {
        // ts_val rows 0-5 are 00:00 to 05:00 — all non-null (row 7 is first null)
        try (SplitSearcher s = createAllTypesSearcher(dir, "ts_range_narrow")) {
            SplitRangeQuery q = SplitRangeQuery.inclusiveRange(
                    "ts_val", "2024-01-01T00:00:00Z", "2024-01-01T05:59:59Z", "date");
            SearchResult result = s.search(q, 100);
            System.out.println("ts_val first 6 hours: " + result.getHits().size() + " hits");
            assertEquals(6, result.getHits().size(),
                    "Should find 6 rows in first 6 hours (all non-null)");
        }
    }

    @Test @Order(13)
    @DisplayName("Timestamp range: ts_val empty range (before data)")
    void testTimestampRangeEmptyBeforeData(@TempDir Path dir) throws Exception {
        try (SplitSearcher s = createAllTypesSearcher(dir, "ts_range_empty")) {
            SplitRangeQuery q = SplitRangeQuery.inclusiveRange(
                    "ts_val", "2023-01-01T00:00:00Z", "2023-12-31T23:59:59Z", "date");
            SearchResult result = s.search(q, 100);
            System.out.println("ts_val in 2023 (before data): " + result.getHits().size() + " hits");
            assertEquals(0, result.getHits().size(),
                    "No ts_val data exists in 2023");
        }
    }

    @Test @Order(14)
    @DisplayName("Timestamp range: ts_val unbounded upper")
    void testTimestampRangeUnboundedUpper(@TempDir Path dir) throws Exception {
        // From 2024-01-02 onwards: rows 24-49, minus nulls at 31, 39, 47 = 23 non-null
        // Row 48 = 2024-01-03 00:00, row 49 = 2024-01-03 01:00 (both non-null)
        try (SplitSearcher s = createAllTypesSearcher(dir, "ts_range_unbounded")) {
            SplitRangeQuery q = new SplitRangeQuery(
                    "ts_val",
                    SplitRangeQuery.RangeBound.inclusive("2024-01-02T00:00:00Z"),
                    SplitRangeQuery.RangeBound.unbounded(),
                    "date");
            SearchResult result = s.search(q, 100);
            System.out.println("ts_val from 2024-01-02 onwards: " + result.getHits().size() + " hits");
            // Rows 24-49 = 26 rows, nulls at 31, 39, 47 = 23 non-null
            assertEquals(23, result.getHits().size(),
                    "Should find 23 non-null rows from 2024-01-02 onwards");
        }
    }

    @Test @Order(15)
    @DisplayName("Timestamp range: event_time full range")
    void testEventTimeFullRange(@TempDir Path dir) throws Exception {
        // event_time: all 50 rows, null at 9,19,29,39,49 = 45 non-null
        try (SplitSearcher s = createAllTypesSearcher(dir, "event_time_full")) {
            SplitRangeQuery q = SplitRangeQuery.inclusiveRange(
                    "event_time", "2024-02-01T00:00:00Z", "2024-02-01T23:59:59Z", "date");
            SearchResult result = s.search(q, 100);
            System.out.println("event_time full range: " + result.getHits().size() + " hits");
            assertEquals(45, result.getHits().size(),
                    "Should find 45 non-null event_time rows");
        }
    }

    @Test @Order(16)
    @DisplayName("Timestamp range: event_time narrow (first 10 minutes)")
    void testEventTimeNarrowRange(@TempDir Path dir) throws Exception {
        // event_time rows 0-9: 00:00 to 00:09, null at row 9 = 9 non-null
        try (SplitSearcher s = createAllTypesSearcher(dir, "event_time_narrow")) {
            SplitRangeQuery q = SplitRangeQuery.inclusiveRange(
                    "event_time", "2024-02-01T00:00:00Z", "2024-02-01T00:09:59Z", "date");
            SearchResult result = s.search(q, 100);
            System.out.println("event_time first 10 min: " + result.getHits().size() + " hits");
            assertEquals(9, result.getHits().size(),
                    "Should find 9 non-null event_time rows in first 10 minutes");
        }
    }

    @Test @Order(17)
    @DisplayName("Timestamp range: ts_val with parseQuery syntax")
    void testTimestampRangeViaParseQuery(@TempDir Path dir) throws Exception {
        // parseQuery with range syntax: ts_val:[2024-01-01 TO 2024-01-01T05:59:59Z]
        // This tests the fix_range_query_types path for date fields
        try (SplitSearcher s = createAllTypesSearcher(dir, "ts_parsequery")) {
            SplitQuery q = s.parseQuery("ts_val:[2024-01-01T00:00:00Z TO 2024-01-01T05:59:59Z]");
            SearchResult result = s.search(q, 100);
            System.out.println("ts_val via parseQuery: " + result.getHits().size() + " hits");
            // Same as test 12: rows 0-5 = 6 non-null
            assertEquals(6, result.getHits().size(),
                    "parseQuery range should find same 6 rows as SplitRangeQuery");
        }
    }

    @Test @Order(18)
    @DisplayName("Timestamp range: ts_val with numeric micros bounds")
    void testTimestampRangeWithNumericMicros(@TempDir Path dir) throws Exception {
        // Test using raw microsecond timestamps as range bounds
        // ts_val row 0 = 1704067200000000 (2024-01-01T00:00:00Z)
        // ts_val row 5 = 1704067200000000 + 5*3600000000 = 1704085200000000 (2024-01-01T05:00:00Z)
        try (SplitSearcher s = createAllTypesSearcher(dir, "ts_micros")) {
            SplitRangeQuery q = SplitRangeQuery.inclusiveRange(
                    "ts_val", "1704067200000000", "1704085200000000", "date");
            SearchResult result = s.search(q, 100);
            System.out.println("ts_val with numeric micros: " + result.getHits().size() + " hits");
            // Rows 0-5 = 6 non-null rows (row 7 is first null, row 6 = 06:00 is outside range)
            assertEquals(6, result.getHits().size(),
                    "Numeric micros range should find 6 rows");
        }
    }
}

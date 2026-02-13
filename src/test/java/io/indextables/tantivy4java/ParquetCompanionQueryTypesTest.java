/*
 * Tests boolean, wildcard, and phrase query types against parquet companion splits,
 * plus the 4-arg createSplitSearcher overload.
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
}

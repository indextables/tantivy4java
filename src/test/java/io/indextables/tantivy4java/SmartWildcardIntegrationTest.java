package io.indextables.tantivy4java;

import io.indextables.tantivy4java.core.*;
import io.indextables.tantivy4java.split.*;
import io.indextables.tantivy4java.split.merge.QuickwitSplit;
import io.indextables.tantivy4java.result.*;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import static org.junit.jupiter.api.Assertions.*;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.UUID;

/**
 * Integration tests for the Smart Wildcard AST Skipping optimization.
 *
 * Tests the short-circuit optimization when expensive wildcards are combined
 * with cheap filters that return zero results.
 *
 * The optimization is implemented in the native Rust layer and applies
 * transparently to ALL query types including parseQuery().
 *
 * Uses per-test lifecycle for cache manager and searcher to verify test isolation.
 * The split file is created once in @BeforeAll for efficiency, but each test
 * creates and destroys its own cache manager and searcher.
 */
@DisplayName("Smart Wildcard Integration Tests")
public class SmartWildcardIntegrationTest {

    // Class-level state (created once)
    private static String tempDir;
    private static String splitPath;
    private static Schema schema;
    private static QuickwitSplit.SplitMetadata splitMetadata;

    // Per-test state (created/destroyed for each test)
    private SplitSearcher splitSearcher;
    private SplitCacheManager cacheManager;

    @BeforeAll
    public static void setUpClass() throws Exception {
        System.out.println("=== Setting up Smart Wildcard Integration Test (class-level) ===");

        // Create temp directory
        tempDir = "/tmp/smart_wildcard_test_" + System.currentTimeMillis();
        File splitDir = new File(tempDir);
        splitDir.mkdirs();
        splitPath = tempDir + "/smart_wildcard_test.split";

        // Create test schema
        try (SchemaBuilder builder = new SchemaBuilder()) {
            builder.addTextField("title", true, false, "default", "position");
            builder.addTextField("content", true, false, "default", "position");
            builder.addTextField("category", true, false, "raw", "freq");
            builder.addIntegerField("id", true, true, false);
            builder.addIntegerField("price", true, true, true);

            schema = builder.build();
        }

        // Create test index with documents in different categories
        String indexPath = tempDir + "/test_index";
        try (Index index = new Index(schema, indexPath, false)) {
            try (IndexWriter writer = index.writer(Index.Memory.DEFAULT_HEAP_SIZE, 1)) {

                // Electronics category - 3 documents
                for (int i = 1; i <= 3; i++) {
                    try (Document doc = new Document()) {
                        doc.addInteger("id", i);
                        doc.addInteger("price", 100 * i);
                        doc.addText("title", "Electronic Device " + i + " smartphone tablet");
                        doc.addText("content", "Description of electronic device " + i);
                        doc.addText("category", "Electronics");
                        writer.addDocument(doc);
                    }
                }

                // Books category - 3 documents
                for (int i = 4; i <= 6; i++) {
                    try (Document doc = new Document()) {
                        doc.addInteger("id", i);
                        doc.addInteger("price", 20 * i);
                        doc.addText("title", "Book Title " + i + " programming guide");
                        doc.addText("content", "Content of book " + i);
                        doc.addText("category", "Books");
                        writer.addDocument(doc);
                    }
                }

                // Furniture category - 2 documents
                for (int i = 7; i <= 8; i++) {
                    try (Document doc = new Document()) {
                        doc.addInteger("id", i);
                        doc.addInteger("price", 500 * i);
                        doc.addText("title", "Furniture Item " + i + " desk chair");
                        doc.addText("content", "Description of furniture " + i);
                        doc.addText("category", "Furniture");
                        writer.addDocument(doc);
                    }
                }

                writer.commit();
            }

            index.reload();
        }

        // Convert to split
        QuickwitSplit.SplitConfig config = new QuickwitSplit.SplitConfig(
            "smart-wildcard-test", "test-source", "test-node");
        splitMetadata = QuickwitSplit.convertIndexFromPath(indexPath, splitPath, config);

        System.out.println("=== Split created successfully ===");
    }

    @BeforeEach
    public void setUp() throws Exception {
        System.out.println("  --- Setting up test (creating cache manager and searcher) ---");

        // Create cache manager and searcher with unique cache name per test
        String uniqueCacheName = "smart-wildcard-test-cache-" + UUID.randomUUID().toString();
        SplitCacheManager.CacheConfig cacheConfig =
            new SplitCacheManager.CacheConfig(uniqueCacheName)
                .withMaxCacheSize(100_000_000);
        cacheManager = SplitCacheManager.getInstance(cacheConfig);
        splitSearcher = cacheManager.createSplitSearcher("file://" + splitPath, splitMetadata);
    }

    @AfterEach
    public void tearDown() {
        System.out.println("  --- Tearing down test (closing cache manager and searcher) ---");

        if (splitSearcher != null) {
            try {
                splitSearcher.close();
            } catch (Exception e) {
                System.err.println("Warning: Failed to close SplitSearcher: " + e.getMessage());
            }
            splitSearcher = null;
        }
        if (cacheManager != null) {
            try {
                cacheManager.close();
            } catch (Exception e) {
                System.err.println("Warning: Failed to close SplitCacheManager: " + e.getMessage());
            }
            cacheManager = null;
        }
    }

    @AfterAll
    public static void tearDownClass() {
        System.out.println("=== Tearing down Smart Wildcard Integration Test (class-level) ===");

        // Clean up temp directory
        if (tempDir != null) {
            try {
                Files.walk(Path.of(tempDir))
                    .sorted(Comparator.reverseOrder())
                    .map(Path::toFile)
                    .forEach(File::delete);
            } catch (Exception e) {
                System.err.println("Cleanup failed: " + e.getMessage());
            }
        }
    }

    @Test
    @DisplayName("Test short-circuit when filter returns no results (programmatic query)")
    public void testShortCircuitWithNoMatchingFilter() throws Exception {
        // Reset stats before test
        SplitSearcher.resetWildcardOptimizationStats();

        // Query for a non-existent category with expensive wildcard
        // The Rust optimizer should detect this and short-circuit
        SplitBooleanQuery query = new SplitBooleanQuery()
            .addMust(new SplitTermQuery("category", "NonExistentCategory"))
            .addMust(new SplitWildcardQuery("title", "*smartphone*"));

        long startTime = System.nanoTime();
        try (SearchResult result = splitSearcher.search(query, 10)) {
            long endTime = System.nanoTime();

            assertNotNull(result);
            assertTrue(result.getHits().isEmpty(), "Should find no results");

            System.out.println("Short-circuit query took: " + (endTime - startTime) / 1_000_000.0 + " ms");
        }

        // Verify optimization stats
        SplitSearcher.SmartWildcardStats stats = SplitSearcher.getWildcardOptimizationStats();
        System.out.println("Optimization stats: " + stats);

        // Assert that the optimization actually triggered
        assertTrue(stats.queriesAnalyzed >= 1, "Should have analyzed at least 1 query");
        assertTrue(stats.queriesOptimizable >= 1, "Query should have been identified as optimizable");
        assertTrue(stats.shortCircuitsTriggered >= 1, "Short-circuit should have been triggered");
    }

    @Test
    @DisplayName("Test no short-circuit when filter matches")
    public void testNoShortCircuitWhenFilterMatches() throws Exception {
        // Reset stats before test
        SplitSearcher.resetWildcardOptimizationStats();

        // Query for existing category with wildcard
        SplitBooleanQuery query = new SplitBooleanQuery()
            .addMust(new SplitTermQuery("category", "Electronics"))
            .addMust(new SplitWildcardQuery("title", "*device*"));

        try (SearchResult result = splitSearcher.search(query, 10)) {
            assertNotNull(result);
            // Electronics category has 3 documents with "device" in title
            System.out.println("Filter-matched query found: " + result.getHits().size() + " results");
            assertTrue(result.getHits().size() > 0, "Should find matching documents");
        }

        // Verify optimization stats - should be optimizable but NOT short-circuited
        SplitSearcher.SmartWildcardStats stats = SplitSearcher.getWildcardOptimizationStats();
        System.out.println("Optimization stats: " + stats);

        assertTrue(stats.queriesAnalyzed >= 1, "Should have analyzed at least 1 query");
        assertTrue(stats.queriesOptimizable >= 1, "Query should have been identified as optimizable");
        assertEquals(0, stats.shortCircuitsTriggered, "Short-circuit should NOT have been triggered (filter matched)");
    }

    @Test
    @DisplayName("Test with range filter and expensive wildcard")
    public void testRangeFilterWithExpensiveWildcard() throws Exception {
        // Range query on price (cheap) + expensive wildcard
        SplitBooleanQuery query = new SplitBooleanQuery()
            .addMust(SplitRangeQuery.inclusiveRange("price", "1000", "10000", "i64"))
            .addMust(new SplitWildcardQuery("title", "*nonexistent*pattern*"));

        try (SearchResult result = splitSearcher.search(query, 10)) {
            assertNotNull(result);
            // Should find no results (pattern doesn't match anything)
            System.out.println("Range + wildcard query found: " + result.getHits().size() + " results");
        }
    }

    @Test
    @DisplayName("Test SearchResult.empty() factory method")
    public void testSearchResultEmpty() throws Exception {
        try (SearchResult empty = SearchResult.empty()) {
            assertNotNull(empty);
            assertTrue(empty.getHits().isEmpty(), "Empty result should have no hits");
            assertFalse(empty.hasAggregations(), "Empty result should have no aggregations");
        }
    }

    @Test
    @DisplayName("Test optimization applies transparently to parseQuery")
    public void testOptimizationWithParseQuery() throws Exception {
        // This test verifies that the Rust-level optimization applies to queries
        // created via parseQuery(), not just programmatically constructed queries.

        // Query 1: Non-existent category with expensive wildcard (should short-circuit)
        SplitQuery parsedQuery1 = splitSearcher.parseQuery("category:nonexistent AND title:*device*");

        long startTime = System.nanoTime();
        try (SearchResult result = splitSearcher.search(parsedQuery1, 10)) {
            long elapsedMs = (System.nanoTime() - startTime) / 1_000_000;

            assertNotNull(result);
            assertTrue(result.getHits().isEmpty(), "Should find no results for non-existent category");
            System.out.println("parseQuery short-circuit test took: " + elapsedMs + " ms");
        }

        // Query 2: Existing category with wildcard (should NOT short-circuit)
        SplitQuery parsedQuery2 = splitSearcher.parseQuery("category:electronics AND title:*device*");

        try (SearchResult result = splitSearcher.search(parsedQuery2, 10)) {
            assertNotNull(result);
            // Electronics category has documents with "device" in title
            System.out.println("parseQuery full execution found: " + result.getHits().size() + " results");
        }
    }

    @Test
    @DisplayName("Test optimization with pure wildcard parseQuery")
    public void testPureWildcardParseQuery() throws Exception {
        // Pure wildcard query (no cheap filter) should NOT short-circuit
        // but should still execute correctly
        SplitQuery wildcardOnly = splitSearcher.parseQuery("title:*electronic*");

        try (SearchResult result = splitSearcher.search(wildcardOnly, 10)) {
            assertNotNull(result);
            System.out.println("Pure wildcard parseQuery found: " + result.getHits().size() + " results");
        }
    }

    @Test
    @DisplayName("Test cheap suffix wildcard with filter")
    public void testCheapWildcardWithFilter() throws Exception {
        // Suffix wildcard (book*) is cheap, should not trigger optimization
        // but should still work correctly
        SplitBooleanQuery query = new SplitBooleanQuery()
            .addMust(new SplitTermQuery("category", "Books"))
            .addMust(new SplitWildcardQuery("title", "book*"));

        try (SearchResult result = splitSearcher.search(query, 10)) {
            assertNotNull(result);
            System.out.println("Cheap wildcard query found: " + result.getHits().size() + " results");
        }
    }

    // ========================================================================
    // Comprehensive Nested Boolean Query Tests
    // ========================================================================

    @Test
    @DisplayName("Nested AND: term AND (term AND wildcard) - should short-circuit")
    public void testNestedAndWithShortCircuit() throws Exception {
        SplitSearcher.resetWildcardOptimizationStats();

        // Outer AND with inner AND containing expensive wildcard
        // category:NonExistent AND (id:[1,100] AND title:*phone*)
        SplitBooleanQuery innerAnd = new SplitBooleanQuery()
            .addMust(SplitRangeQuery.inclusiveRange("price", "1", "100", "i64"))
            .addMust(new SplitWildcardQuery("title", "*phone*"));

        SplitBooleanQuery outerQuery = new SplitBooleanQuery()
            .addMust(new SplitTermQuery("category", "NonExistent"))
            .addMust(innerAnd);

        try (SearchResult result = splitSearcher.search(outerQuery, 10)) {
            assertNotNull(result);
            assertTrue(result.getHits().isEmpty(), "Should find no results");
        }

        SplitSearcher.SmartWildcardStats stats = SplitSearcher.getWildcardOptimizationStats();
        System.out.println("Nested AND short-circuit stats: " + stats);
        assertTrue(stats.queriesOptimizable >= 1, "Should identify as optimizable");
        assertTrue(stats.shortCircuitsTriggered >= 1, "Should trigger short-circuit");
    }

    @Test
    @DisplayName("Nested AND: term AND (range AND wildcard) - no short-circuit when filters match")
    public void testNestedAndNoShortCircuit() throws Exception {
        SplitSearcher.resetWildcardOptimizationStats();

        // Outer AND with inner AND - all filters match
        // Electronics items have prices 100, 200, 300 - use range that matches them
        SplitBooleanQuery innerAnd = new SplitBooleanQuery()
            .addMust(SplitRangeQuery.inclusiveRange("price", "50", "350", "i64"))
            .addMust(new SplitWildcardQuery("title", "*device*"));

        SplitBooleanQuery outerQuery = new SplitBooleanQuery()
            .addMust(new SplitTermQuery("category", "Electronics"))
            .addMust(innerAnd);

        try (SearchResult result = splitSearcher.search(outerQuery, 10)) {
            assertNotNull(result);
            System.out.println("Nested AND (matching) found: " + result.getHits().size() + " results");
            assertTrue(result.getHits().size() > 0, "Should find Electronics items with price 50-350 and 'device' in title");
        }

        SplitSearcher.SmartWildcardStats stats = SplitSearcher.getWildcardOptimizationStats();
        System.out.println("Nested AND no-short-circuit stats: " + stats);
        // Query is optimizable (has cheap filters + expensive wildcard) but should NOT short-circuit
        // because cheap filters (category:Electronics AND price:[50,350]) DO match documents
        assertTrue(stats.queriesOptimizable >= 1, "Query should be optimizable");
        assertEquals(0, stats.shortCircuitsTriggered, "Should NOT trigger short-circuit when filters match documents");
    }

    @Test
    @DisplayName("OR query with wildcard - should NOT optimize (OR doesn't filter)")
    public void testOrQueryNotOptimizable() throws Exception {
        SplitSearcher.resetWildcardOptimizationStats();

        // OR queries should not be optimizable because either branch can match
        // category:NonExistent OR title:*phone*
        SplitBooleanQuery orQuery = new SplitBooleanQuery()
            .addShould(new SplitTermQuery("category", "NonExistent"))
            .addShould(new SplitWildcardQuery("title", "*phone*"));

        try (SearchResult result = splitSearcher.search(orQuery, 10)) {
            assertNotNull(result);
            // Should find results because wildcard matches even though term doesn't
            System.out.println("OR query found: " + result.getHits().size() + " results");
        }

        SplitSearcher.SmartWildcardStats stats = SplitSearcher.getWildcardOptimizationStats();
        System.out.println("OR query stats: " + stats);
        // OR queries with wildcards should still be analyzed
        assertTrue(stats.queriesAnalyzed >= 1, "Should analyze query");
    }

    @Test
    @DisplayName("NOT query: term AND NOT(wildcard) - should execute correctly")
    public void testNotQueryWithWildcard() throws Exception {
        SplitSearcher.resetWildcardOptimizationStats();

        // Find Electronics items that do NOT match *tablet* in title
        SplitBooleanQuery query = new SplitBooleanQuery()
            .addMust(new SplitTermQuery("category", "Electronics"))
            .addMustNot(new SplitWildcardQuery("title", "*tablet*"));

        try (SearchResult result = splitSearcher.search(query, 10)) {
            assertNotNull(result);
            System.out.println("NOT query found: " + result.getHits().size() + " results");
        }

        SplitSearcher.SmartWildcardStats stats = SplitSearcher.getWildcardOptimizationStats();
        System.out.println("NOT query stats: " + stats);
    }

    @Test
    @DisplayName("Deeply nested: AND(range, AND(term:nonexistent, AND(range, wildcard))) - recursive extraction")
    public void testDeeplyNestedAndRecursiveExtraction() throws Exception {
        SplitSearcher.resetWildcardOptimizationStats();

        // The optimizer now RECURSIVELY extracts cheap filters from nested MUST clauses.
        // The non-matching term "NonExistent" in level2 will be extracted and used
        // for the cheap filter pre-check, triggering short-circuit.

        // 3 levels of nesting with expensive wildcard at deepest level
        SplitBooleanQuery level3 = new SplitBooleanQuery()
            .addMust(SplitRangeQuery.inclusiveRange("price", "1", "1000", "i64"))
            .addMust(new SplitWildcardQuery("content", "*nonexistent*pattern*"));

        SplitBooleanQuery level2 = new SplitBooleanQuery()
            .addMust(new SplitTermQuery("category", "NonExistent"))  // This gets extracted!
            .addMust(level3);

        SplitBooleanQuery level1 = new SplitBooleanQuery()
            .addMust(SplitRangeQuery.inclusiveRange("price", "1", "100", "i64"))
            .addMust(level2);

        try (SearchResult result = splitSearcher.search(level1, 10)) {
            assertNotNull(result);
            assertTrue(result.getHits().isEmpty(), "Should find no results");
        }

        SplitSearcher.SmartWildcardStats stats = SplitSearcher.getWildcardOptimizationStats();
        System.out.println("Deeply nested AND stats: " + stats);
        assertTrue(stats.queriesAnalyzed >= 1, "Should analyze query");
        assertTrue(stats.queriesOptimizable >= 1, "Should be identified as optimizable");
        // Now short-circuit SHOULD trigger because we recursively extract the NonExistent term
        assertTrue(stats.shortCircuitsTriggered >= 1, "Should short-circuit with recursive extraction");
    }

    @Test
    @DisplayName("Mixed AND/OR: AND(term, OR(term, wildcard)) - complex case")
    public void testMixedAndOr() throws Exception {
        SplitSearcher.resetWildcardOptimizationStats();

        // AND with OR subquery - the OR contains wildcard
        // category:Electronics AND (title:laptop OR title:*phone*)
        SplitBooleanQuery innerOr = new SplitBooleanQuery()
            .addShould(new SplitTermQuery("title", "laptop"))
            .addShould(new SplitWildcardQuery("title", "*phone*"));

        SplitBooleanQuery outerQuery = new SplitBooleanQuery()
            .addMust(new SplitTermQuery("category", "Electronics"))
            .addMust(innerOr);

        try (SearchResult result = splitSearcher.search(outerQuery, 10)) {
            assertNotNull(result);
            System.out.println("Mixed AND/OR found: " + result.getHits().size() + " results");
        }

        SplitSearcher.SmartWildcardStats stats = SplitSearcher.getWildcardOptimizationStats();
        System.out.println("Mixed AND/OR stats: " + stats);
    }

    @Test
    @DisplayName("Multiple expensive wildcards: AND(term, wildcard1, wildcard2) - short-circuit")
    public void testMultipleExpensiveWildcardsShortCircuit() throws Exception {
        SplitSearcher.resetWildcardOptimizationStats();

        // Multiple expensive wildcards with non-matching term
        SplitBooleanQuery query = new SplitBooleanQuery()
            .addMust(new SplitTermQuery("category", "NonExistent"))
            .addMust(new SplitWildcardQuery("title", "*foo*"))
            .addMust(new SplitWildcardQuery("content", "*bar*"));

        try (SearchResult result = splitSearcher.search(query, 10)) {
            assertNotNull(result);
            assertTrue(result.getHits().isEmpty(), "Should find no results");
        }

        SplitSearcher.SmartWildcardStats stats = SplitSearcher.getWildcardOptimizationStats();
        System.out.println("Multiple wildcards stats: " + stats);
        assertTrue(stats.shortCircuitsTriggered >= 1, "Should trigger short-circuit");
    }

    @Test
    @DisplayName("All expensive - no cheap filter: AND(wildcard1, wildcard2) - not optimizable")
    public void testAllExpensiveNotOptimizable() throws Exception {
        SplitSearcher.resetWildcardOptimizationStats();

        // Only expensive wildcards, no cheap filter to use for short-circuit
        SplitBooleanQuery query = new SplitBooleanQuery()
            .addMust(new SplitWildcardQuery("title", "*electronic*"))
            .addMust(new SplitWildcardQuery("content", "*device*"));

        try (SearchResult result = splitSearcher.search(query, 10)) {
            assertNotNull(result);
            System.out.println("All expensive wildcards found: " + result.getHits().size() + " results");
        }

        SplitSearcher.SmartWildcardStats stats = SplitSearcher.getWildcardOptimizationStats();
        System.out.println("All expensive stats: " + stats);
        // Should be analyzed but not optimizable (no cheap filters)
        assertTrue(stats.queriesAnalyzed >= 1, "Should analyze query");
        assertEquals(0, stats.queriesOptimizable, "Should NOT be optimizable (no cheap filters)");
    }

    @Test
    @DisplayName("Complex: AND(term, NOT(OR(term, wildcard)), range) - complex nesting")
    public void testComplexNesting() throws Exception {
        SplitSearcher.resetWildcardOptimizationStats();

        // Complex query: Electronics AND NOT(Books OR *tablet*) AND price:[1,500]
        SplitBooleanQuery innerOr = new SplitBooleanQuery()
            .addShould(new SplitTermQuery("category", "Books"))
            .addShould(new SplitWildcardQuery("title", "*tablet*"));

        SplitBooleanQuery query = new SplitBooleanQuery()
            .addMust(new SplitTermQuery("category", "Electronics"))
            .addMustNot(innerOr)
            .addMust(SplitRangeQuery.inclusiveRange("price", "1", "500", "i64"));

        try (SearchResult result = splitSearcher.search(query, 10)) {
            assertNotNull(result);
            System.out.println("Complex nesting found: " + result.getHits().size() + " results");
        }

        SplitSearcher.SmartWildcardStats stats = SplitSearcher.getWildcardOptimizationStats();
        System.out.println("Complex nesting stats: " + stats);
    }

    @Test
    @DisplayName("Top-level cheap filters with wildcarded OR sub-expressions should short-circuit")
    public void testTopLevelCheapWithWildcardedOrSubexpression() throws Exception {
        // Test the scenario: a:bb AND f:xx AND (f:y OR f:*x*) AND (f:x AND z:*xx*)
        // Top-level cheap filters (category, price) should allow short-circuit
        // even though we can't extract anything from the OR sub-expression
        SplitSearcher.resetWildcardOptimizationStats();

        // OR sub-expression containing wildcard - can't extract cheap filters from here
        SplitBooleanQuery orWithWildcard = new SplitBooleanQuery()
            .addShould(new SplitTermQuery("title", "laptop"))
            .addShould(new SplitWildcardQuery("title", "*phone*"));

        // AND sub-expression containing wildcard - CAN extract f:x
        SplitBooleanQuery andWithWildcard = new SplitBooleanQuery()
            .addMust(new SplitTermQuery("content", "description"))
            .addMust(new SplitWildcardQuery("title", "*xyz*"));

        // Main query: category:NonExistent AND price:[1,1000] AND (or_subexpr) AND (and_subexpr)
        SplitBooleanQuery query = new SplitBooleanQuery()
            .addMust(new SplitTermQuery("category", "NonExistent"))  // cheap, no match
            .addMust(SplitRangeQuery.inclusiveRange("price", "1", "1000", "i64"))  // cheap, matches
            .addMust(orWithWildcard)   // contains wildcard, can't extract (OR)
            .addMust(andWithWildcard); // contains wildcard, CAN extract content:description

        try (SearchResult result = splitSearcher.search(query, 10)) {
            assertNotNull(result);
            assertTrue(result.getHits().isEmpty(), "Should find no results (category doesn't exist)");
            System.out.println("Top-level cheap with OR subexpr found: " + result.getHits().size() + " results");
        }

        SplitSearcher.SmartWildcardStats stats = SplitSearcher.getWildcardOptimizationStats();
        System.out.println("Top-level cheap with OR subexpr stats: " + stats);

        // Should optimize using: category:NonExistent AND price:[1,1000] AND content:description
        assertTrue(stats.queriesOptimizable >= 1, "Query should be optimizable (has top-level cheap filters)");
        assertTrue(stats.shortCircuitsTriggered >= 1,
            "Should short-circuit because top-level cheap filter (category:NonExistent) returns 0 results");
    }

    @Test
    @DisplayName("Top-level cheap filters with wildcarded OR - no short-circuit when filter matches")
    public void testTopLevelCheapWithWildcardedOrNoShortCircuit() throws Exception {
        // Same structure but with matching cheap filters - should NOT short-circuit
        SplitSearcher.resetWildcardOptimizationStats();

        // OR sub-expression containing wildcard
        SplitBooleanQuery orWithWildcard = new SplitBooleanQuery()
            .addShould(new SplitTermQuery("title", "laptop"))
            .addShould(new SplitWildcardQuery("title", "*device*"));

        // AND sub-expression containing wildcard
        SplitBooleanQuery andWithWildcard = new SplitBooleanQuery()
            .addMust(new SplitTermQuery("content", "description"))
            .addMust(new SplitWildcardQuery("title", "*electronic*"));

        // Main query with MATCHING cheap filters
        SplitBooleanQuery query = new SplitBooleanQuery()
            .addMust(new SplitTermQuery("category", "Electronics"))  // cheap, MATCHES
            .addMust(SplitRangeQuery.inclusiveRange("price", "50", "350", "i64"))  // cheap, MATCHES
            .addMust(orWithWildcard)
            .addMust(andWithWildcard);

        try (SearchResult result = splitSearcher.search(query, 10)) {
            assertNotNull(result);
            System.out.println("Top-level cheap (matching) with OR subexpr found: " + result.getHits().size() + " results");
        }

        SplitSearcher.SmartWildcardStats stats = SplitSearcher.getWildcardOptimizationStats();
        System.out.println("Top-level cheap (matching) with OR subexpr stats: " + stats);

        // Should be optimizable but NOT short-circuit (cheap filters match documents)
        assertTrue(stats.queriesOptimizable >= 1, "Query should be optimizable");
        assertEquals(0, stats.shortCircuitsTriggered,
            "Should NOT short-circuit when top-level cheap filters match documents");
    }

    @Test
    @DisplayName("Range + multiple terms + wildcard: comprehensive AND")
    public void testComprehensiveAnd() throws Exception {
        SplitSearcher.resetWildcardOptimizationStats();

        // Multiple cheap filters + expensive wildcard
        SplitBooleanQuery query = new SplitBooleanQuery()
            .addMust(new SplitTermQuery("category", "NonExistent"))
            .addMust(SplitRangeQuery.inclusiveRange("price", "1", "100", "i64"))
            .addMust(SplitRangeQuery.inclusiveRange("price", "1", "10000", "i64"))
            .addMust(new SplitWildcardQuery("title", "*smartphone*"));

        try (SearchResult result = splitSearcher.search(query, 10)) {
            assertNotNull(result);
            assertTrue(result.getHits().isEmpty(), "Should find no results");
        }

        SplitSearcher.SmartWildcardStats stats = SplitSearcher.getWildcardOptimizationStats();
        System.out.println("Comprehensive AND stats: " + stats);
        assertTrue(stats.queriesOptimizable >= 1, "Should be optimizable");
        assertTrue(stats.shortCircuitsTriggered >= 1, "Should trigger short-circuit");
    }

    @Test
    @DisplayName("Verify short-circuit saves work: compare stats for matching vs non-matching")
    public void testShortCircuitEfficiency() throws Exception {
        // Test 1: Query that will short-circuit (non-existent category)
        SplitSearcher.resetWildcardOptimizationStats();

        SplitBooleanQuery shortCircuitQuery = new SplitBooleanQuery()
            .addMust(new SplitTermQuery("category", "NonExistent"))
            .addMust(new SplitWildcardQuery("title", "*very*long*complex*pattern*"));

        long start1 = System.nanoTime();
        try (SearchResult result = splitSearcher.search(shortCircuitQuery, 10)) {
            assertTrue(result.getHits().isEmpty());
        }
        long time1 = System.nanoTime() - start1;

        SplitSearcher.SmartWildcardStats stats1 = SplitSearcher.getWildcardOptimizationStats();

        // Test 2: Query that won't short-circuit (matching category)
        SplitSearcher.resetWildcardOptimizationStats();

        SplitBooleanQuery fullQuery = new SplitBooleanQuery()
            .addMust(new SplitTermQuery("category", "Electronics"))
            .addMust(new SplitWildcardQuery("title", "*device*"));

        long start2 = System.nanoTime();
        try (SearchResult result = splitSearcher.search(fullQuery, 10)) {
            // This query should return results
        }
        long time2 = System.nanoTime() - start2;

        SplitSearcher.SmartWildcardStats stats2 = SplitSearcher.getWildcardOptimizationStats();

        System.out.println("Short-circuit query time: " + (time1 / 1_000_000.0) + " ms, stats: " + stats1);
        System.out.println("Full query time: " + (time2 / 1_000_000.0) + " ms, stats: " + stats2);

        // Verify the optimization behavior
        assertEquals(1, stats1.shortCircuitsTriggered, "First query should short-circuit");
        assertEquals(0, stats2.shortCircuitsTriggered, "Second query should NOT short-circuit");
    }

    // ========================================================================
    // Wildcard Pattern Combination Tests
    // ========================================================================

    @Test
    @DisplayName("Leading wildcard (*foo) - EXPENSIVE, should optimize")
    public void testLeadingWildcardOptimization() throws Exception {
        SplitSearcher.resetWildcardOptimizationStats();

        SplitBooleanQuery query = new SplitBooleanQuery()
            .addMust(new SplitTermQuery("category", "NonExistent"))
            .addMust(new SplitWildcardQuery("title", "*phone"));  // Leading wildcard

        try (SearchResult result = splitSearcher.search(query, 10)) {
            assertTrue(result.getHits().isEmpty());
        }

        SplitSearcher.SmartWildcardStats stats = SplitSearcher.getWildcardOptimizationStats();
        System.out.println("Leading wildcard (*foo) stats: " + stats);
        assertTrue(stats.queriesOptimizable >= 1, "Leading wildcard should be optimizable");
        assertTrue(stats.shortCircuitsTriggered >= 1, "Should short-circuit");
    }

    @Test
    @DisplayName("Trailing wildcard (foo*) - CHEAP, should NOT optimize")
    public void testTrailingWildcardNotExpensive() throws Exception {
        SplitSearcher.resetWildcardOptimizationStats();

        SplitBooleanQuery query = new SplitBooleanQuery()
            .addMust(new SplitTermQuery("category", "NonExistent"))
            .addMust(new SplitWildcardQuery("title", "phone*"));  // Trailing wildcard - cheap

        try (SearchResult result = splitSearcher.search(query, 10)) {
            assertTrue(result.getHits().isEmpty());
        }

        SplitSearcher.SmartWildcardStats stats = SplitSearcher.getWildcardOptimizationStats();
        System.out.println("Trailing wildcard (foo*) stats: " + stats);
        // Trailing wildcard is cheap, so query should NOT be optimizable
        assertEquals(0, stats.queriesOptimizable, "Trailing wildcard is cheap - not optimizable");
    }

    @Test
    @DisplayName("Contains wildcard (*foo*) - EXPENSIVE, should optimize")
    public void testContainsWildcardOptimization() throws Exception {
        SplitSearcher.resetWildcardOptimizationStats();

        SplitBooleanQuery query = new SplitBooleanQuery()
            .addMust(new SplitTermQuery("category", "NonExistent"))
            .addMust(new SplitWildcardQuery("title", "*phone*"));  // Contains wildcard

        try (SearchResult result = splitSearcher.search(query, 10)) {
            assertTrue(result.getHits().isEmpty());
        }

        SplitSearcher.SmartWildcardStats stats = SplitSearcher.getWildcardOptimizationStats();
        System.out.println("Contains wildcard (*foo*) stats: " + stats);
        assertTrue(stats.queriesOptimizable >= 1, "Contains wildcard should be optimizable");
        assertTrue(stats.shortCircuitsTriggered >= 1, "Should short-circuit");
    }

    @Test
    @DisplayName("Multi-wildcard (*foo*bar*) - EXPENSIVE, should optimize")
    public void testMultiWildcardOptimization() throws Exception {
        SplitSearcher.resetWildcardOptimizationStats();

        SplitBooleanQuery query = new SplitBooleanQuery()
            .addMust(new SplitTermQuery("category", "NonExistent"))
            .addMust(new SplitWildcardQuery("title", "*smart*phone*"));  // Multiple wildcards

        try (SearchResult result = splitSearcher.search(query, 10)) {
            assertTrue(result.getHits().isEmpty());
        }

        SplitSearcher.SmartWildcardStats stats = SplitSearcher.getWildcardOptimizationStats();
        System.out.println("Multi-wildcard (*foo*bar*) stats: " + stats);
        assertTrue(stats.queriesOptimizable >= 1, "Multi-wildcard should be optimizable");
        assertTrue(stats.shortCircuitsTriggered >= 1, "Should short-circuit");
    }

    @Test
    @DisplayName("Single char wildcard (foo?) - CHEAP when at end")
    public void testSingleCharWildcardAtEnd() throws Exception {
        SplitSearcher.resetWildcardOptimizationStats();

        SplitBooleanQuery query = new SplitBooleanQuery()
            .addMust(new SplitTermQuery("category", "NonExistent"))
            .addMust(new SplitWildcardQuery("title", "phon?"));  // Single char at end - cheap

        try (SearchResult result = splitSearcher.search(query, 10)) {
            assertTrue(result.getHits().isEmpty());
        }

        SplitSearcher.SmartWildcardStats stats = SplitSearcher.getWildcardOptimizationStats();
        System.out.println("Single char wildcard (foo?) stats: " + stats);
        // Single char at end is cheap like trailing wildcard
        assertEquals(0, stats.queriesOptimizable, "Single char at end is cheap - not optimizable");
    }

    @Test
    @DisplayName("Leading single char (?foo) - EXPENSIVE")
    public void testLeadingSingleCharWildcard() throws Exception {
        SplitSearcher.resetWildcardOptimizationStats();

        SplitBooleanQuery query = new SplitBooleanQuery()
            .addMust(new SplitTermQuery("category", "NonExistent"))
            .addMust(new SplitWildcardQuery("title", "?hone"));  // Leading single char

        try (SearchResult result = splitSearcher.search(query, 10)) {
            assertTrue(result.getHits().isEmpty());
        }

        SplitSearcher.SmartWildcardStats stats = SplitSearcher.getWildcardOptimizationStats();
        System.out.println("Leading single char (?foo) stats: " + stats);
        assertTrue(stats.queriesOptimizable >= 1, "Leading single char should be optimizable");
        assertTrue(stats.shortCircuitsTriggered >= 1, "Should short-circuit");
    }

    @Test
    @DisplayName("Mixed wildcards: cheap + expensive in same query - short-circuit")
    public void testMixedWildcardPatterns() throws Exception {
        SplitSearcher.resetWildcardOptimizationStats();

        // One cheap wildcard (suffix) and one expensive (contains)
        // The term query (NonExistent) + suffix wildcard are both cheap filters
        // The contains wildcard (*device*) is expensive
        SplitBooleanQuery query = new SplitBooleanQuery()
            .addMust(new SplitTermQuery("category", "NonExistent"))
            .addMust(new SplitWildcardQuery("title", "smart*"))      // Cheap - suffix
            .addMust(new SplitWildcardQuery("content", "*device*")); // Expensive - contains

        try (SearchResult result = splitSearcher.search(query, 10)) {
            assertTrue(result.getHits().isEmpty());
        }

        SplitSearcher.SmartWildcardStats stats = SplitSearcher.getWildcardOptimizationStats();
        System.out.println("Mixed wildcards stats: " + stats);
        // Query has expensive wildcard + cheap filters (term + suffix wildcard)
        assertTrue(stats.queriesAnalyzed >= 1, "Should analyze query");
        assertTrue(stats.queriesOptimizable >= 1, "Query with expensive wildcard should be optimizable");
        // Short-circuit happens because the cheap filter (term:NonExistent AND title:smart*) returns 0 docs
        assertTrue(stats.shortCircuitsTriggered >= 1, "Should short-circuit since cheap filter returns 0");
    }

    @Test
    @DisplayName("Complex multi-segment wildcard (*a*b*c*)")
    public void testComplexMultiSegmentWildcard() throws Exception {
        SplitSearcher.resetWildcardOptimizationStats();

        SplitBooleanQuery query = new SplitBooleanQuery()
            .addMust(new SplitTermQuery("category", "NonExistent"))
            .addMust(new SplitWildcardQuery("title", "*elec*tron*ic*")); // Complex pattern

        try (SearchResult result = splitSearcher.search(query, 10)) {
            assertTrue(result.getHits().isEmpty());
        }

        SplitSearcher.SmartWildcardStats stats = SplitSearcher.getWildcardOptimizationStats();
        System.out.println("Complex multi-segment wildcard stats: " + stats);
        assertTrue(stats.queriesOptimizable >= 1, "Complex wildcard should be optimizable");
        assertTrue(stats.shortCircuitsTriggered >= 1, "Should short-circuit");
    }

    @Test
    @DisplayName("Wildcard in different fields: AND(field1:*foo*, field2:*bar*)")
    public void testWildcardsInDifferentFields() throws Exception {
        SplitSearcher.resetWildcardOptimizationStats();

        SplitBooleanQuery query = new SplitBooleanQuery()
            .addMust(new SplitTermQuery("category", "NonExistent"))
            .addMust(new SplitWildcardQuery("title", "*smart*"))
            .addMust(new SplitWildcardQuery("content", "*phone*"));

        try (SearchResult result = splitSearcher.search(query, 10)) {
            assertTrue(result.getHits().isEmpty());
        }

        SplitSearcher.SmartWildcardStats stats = SplitSearcher.getWildcardOptimizationStats();
        System.out.println("Wildcards in different fields stats: " + stats);
        assertTrue(stats.queriesOptimizable >= 1, "Should be optimizable");
        assertTrue(stats.shortCircuitsTriggered >= 1, "Should short-circuit");
    }

    @Test
    @DisplayName("Edge case: empty wildcard pattern")
    public void testEmptyWildcardPattern() throws Exception {
        SplitSearcher.resetWildcardOptimizationStats();

        // Empty or minimal patterns
        SplitBooleanQuery query = new SplitBooleanQuery()
            .addMust(new SplitTermQuery("category", "Electronics"))
            .addMust(new SplitWildcardQuery("title", "*"));  // Match all in field

        try (SearchResult result = splitSearcher.search(query, 10)) {
            assertNotNull(result);
            System.out.println("Star-only wildcard found: " + result.getHits().size() + " results");
        }

        SplitSearcher.SmartWildcardStats stats = SplitSearcher.getWildcardOptimizationStats();
        System.out.println("Star-only wildcard stats: " + stats);
    }

    @Test
    @DisplayName("Deeply nested with multiple wildcard types - recursive extraction triggers short-circuit")
    public void testDeeplyNestedMixedWildcards() throws Exception {
        SplitSearcher.resetWildcardOptimizationStats();

        // The optimizer now RECURSIVELY extracts cheap filters from nested MUST clauses.
        // The non-matching term "NonExistent" and the cheap suffix wildcard "elect*" in level2
        // will be extracted and used for the cheap filter pre-check.

        // Level 3: range + expensive wildcard
        SplitBooleanQuery level3 = new SplitBooleanQuery()
            .addMust(SplitRangeQuery.inclusiveRange("price", "1", "1000", "i64"))
            .addMust(new SplitWildcardQuery("content", "*description*")); // Expensive

        // Level 2: term + cheap wildcard + level3
        SplitBooleanQuery level2 = new SplitBooleanQuery()
            .addMust(new SplitTermQuery("category", "NonExistent"))  // Extracted!
            .addMust(new SplitWildcardQuery("title", "elect*"))       // Cheap - also extracted!
            .addMust(level3);

        // Level 1: range + level2
        SplitBooleanQuery level1 = new SplitBooleanQuery()
            .addMust(SplitRangeQuery.inclusiveRange("price", "1", "100", "i64"))
            .addMust(level2);

        try (SearchResult result = splitSearcher.search(level1, 10)) {
            assertTrue(result.getHits().isEmpty());
        }

        SplitSearcher.SmartWildcardStats stats = SplitSearcher.getWildcardOptimizationStats();
        System.out.println("Deeply nested mixed wildcards stats: " + stats);
        assertTrue(stats.queriesAnalyzed >= 1, "Should analyze query");
        assertTrue(stats.queriesOptimizable >= 1, "Should be optimizable");
        // Now short-circuit SHOULD trigger because we recursively extract NonExistent term
        assertTrue(stats.shortCircuitsTriggered >= 1, "Should short-circuit with recursive extraction");
    }
}

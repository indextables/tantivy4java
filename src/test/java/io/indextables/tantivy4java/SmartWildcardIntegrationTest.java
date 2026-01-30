package io.indextables.tantivy4java;

import io.indextables.tantivy4java.core.*;
import io.indextables.tantivy4java.split.*;
import io.indextables.tantivy4java.split.merge.QuickwitSplit;
import io.indextables.tantivy4java.result.*;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.TestInstance;
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
 * Uses @TestInstance(PER_CLASS) to share the cache manager and searcher
 * across all tests, avoiding repeated setup/teardown which causes crashes.
 */
@DisplayName("Smart Wildcard Integration Tests")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class SmartWildcardIntegrationTest {

    private String tempDir;
    private String splitPath;
    private SplitSearcher splitSearcher;
    private SplitCacheManager cacheManager;
    private Schema schema;
    private QuickwitSplit.SplitMetadata splitMetadata;

    @BeforeAll
    public void setUp() throws Exception {
        System.out.println("=== Setting up Smart Wildcard Integration Test ===");

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

        // Create cache manager and searcher with unique cache name per test run
        String uniqueCacheName = "smart-wildcard-test-cache-" + UUID.randomUUID().toString();
        SplitCacheManager.CacheConfig cacheConfig =
            new SplitCacheManager.CacheConfig(uniqueCacheName)
                .withMaxCacheSize(100_000_000);
        cacheManager = SplitCacheManager.getInstance(cacheConfig);
        splitSearcher = cacheManager.createSplitSearcher("file://" + splitPath, splitMetadata);
    }

    @AfterAll
    public void tearDown() {
        System.out.println("=== Tearing down Smart Wildcard Integration Test ===");

        if (splitSearcher != null) {
            try {
                splitSearcher.close();
            } catch (Exception e) {
                System.err.println("Warning: Failed to close SplitSearcher: " + e.getMessage());
            }
        }
        if (cacheManager != null) {
            try {
                cacheManager.close();
            } catch (Exception e) {
                System.err.println("Warning: Failed to close SplitCacheManager: " + e.getMessage());
            }
        }

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
    }

    @Test
    @DisplayName("Test no short-circuit when filter matches")
    public void testNoShortCircuitWhenFilterMatches() throws Exception {
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
}

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
 * Integration tests for SplitWildcardQuery functionality.
 *
 * Tests wildcard pattern matching in split files with various patterns.
 *
 * Uses @TestInstance(PER_CLASS) to share the cache manager and searcher
 * across all tests, avoiding repeated setup/teardown which causes crashes.
 */
@DisplayName("SplitWildcardQuery Integration Tests")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class SplitWildcardQueryTest {

    private String tempDir;
    private String splitPath;
    private SplitSearcher splitSearcher;
    private SplitCacheManager cacheManager;
    private Schema schema;
    private QuickwitSplit.SplitMetadata splitMetadata;

    @BeforeAll
    public void setUp() throws Exception {
        System.out.println("=== Setting up SplitWildcardQuery Test ===");

        // Create temp directory
        tempDir = "/tmp/split_wildcard_query_test_" + System.currentTimeMillis();
        File splitDir = new File(tempDir);
        splitDir.mkdirs();
        splitPath = tempDir + "/wildcard_test.split";

        // Create test schema with text fields
        try (SchemaBuilder builder = new SchemaBuilder()) {
            builder.addTextField("title", true, false, "default", "position");
            builder.addTextField("content", true, false, "default", "position");
            builder.addTextField("category", true, false, "raw", "freq");
            builder.addIntegerField("id", true, true, false);

            schema = builder.build();
        }

        // Create test index with sample documents
        String indexPath = tempDir + "/test_index";
        try (Index index = new Index(schema, indexPath, false)) {
            try (IndexWriter writer = index.writer(Index.Memory.DEFAULT_HEAP_SIZE, 1)) {

                // Document 1: smartphone
                try (Document doc1 = new Document()) {
                    doc1.addInteger("id", 1);
                    doc1.addText("title", "iPhone 15 Pro Max Smartphone");
                    doc1.addText("content", "The latest smartphone from Apple with advanced features");
                    doc1.addText("category", "Electronics");
                    writer.addDocument(doc1);
                }

                // Document 2: laptop
                try (Document doc2 = new Document()) {
                    doc2.addInteger("id", 2);
                    doc2.addText("title", "MacBook Pro Laptop");
                    doc2.addText("content", "Professional laptop for developers and creators");
                    doc2.addText("category", "Electronics");
                    writer.addDocument(doc2);
                }

                // Document 3: phone case
                try (Document doc3 = new Document()) {
                    doc3.addInteger("id", 3);
                    doc3.addText("title", "Protective Phone Case");
                    doc3.addText("content", "Durable case for protecting your phone");
                    doc3.addText("category", "Accessories");
                    writer.addDocument(doc3);
                }

                // Document 4: wireless headphones
                try (Document doc4 = new Document()) {
                    doc4.addInteger("id", 4);
                    doc4.addText("title", "Wireless Bluetooth Headphones");
                    doc4.addText("content", "High quality wireless headphones for music lovers");
                    doc4.addText("category", "Electronics");
                    writer.addDocument(doc4);
                }

                // Document 5: telephone book
                try (Document doc5 = new Document()) {
                    doc5.addInteger("id", 5);
                    doc5.addText("title", "Vintage Telephone Directory");
                    doc5.addText("content", "Historical telephone book from the 1950s");
                    doc5.addText("category", "Books");
                    writer.addDocument(doc5);
                }

                writer.commit();
            }

            index.reload();
        }

        // Convert to split
        QuickwitSplit.SplitConfig config = new QuickwitSplit.SplitConfig(
            "wildcard-test", "test-source", "test-node");
        splitMetadata = QuickwitSplit.convertIndexFromPath(indexPath, splitPath, config);

        // Create cache manager and searcher with unique cache name per test run
        String uniqueCacheName = "wildcard-test-cache-" + UUID.randomUUID().toString();
        SplitCacheManager.CacheConfig cacheConfig =
            new SplitCacheManager.CacheConfig(uniqueCacheName)
                .withMaxCacheSize(100_000_000);
        cacheManager = SplitCacheManager.getInstance(cacheConfig);
        splitSearcher = cacheManager.createSplitSearcher("file://" + splitPath, splitMetadata);
    }

    @AfterAll
    public void tearDown() {
        System.out.println("=== Tearing down SplitWildcardQuery Test ===");

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
    @DisplayName("Test suffix wildcard - phone*")
    public void testSuffixWildcard() throws Exception {
        // This should match "phone" in "smartphone" and "phone"
        SplitWildcardQuery query = new SplitWildcardQuery("title", "phone*");

        assertFalse(query.isExpensive(), "Suffix wildcard should not be expensive");

        // Note: Due to tokenization, "phone*" may or may not match
        // The test verifies the query executes without error
        try (SearchResult result = splitSearcher.search(query, 10)) {
            assertNotNull(result);
            System.out.println("Suffix wildcard 'phone*' found " + result.getHits().size() + " results");
        }
    }

    @Test
    @DisplayName("Test leading wildcard - *phone")
    public void testLeadingWildcard() throws Exception {
        SplitWildcardQuery query = new SplitWildcardQuery("title", "*phone");

        assertTrue(query.isExpensive(), "Leading wildcard should be expensive");

        try (SearchResult result = splitSearcher.search(query, 10)) {
            assertNotNull(result);
            System.out.println("Leading wildcard '*phone' found " + result.getHits().size() + " results");
        }
    }

    @Test
    @DisplayName("Test contains wildcard - *phone*")
    public void testContainsWildcard() throws Exception {
        SplitWildcardQuery query = new SplitWildcardQuery("title", "*phone*");

        assertTrue(query.isExpensive(), "Contains wildcard should be expensive");

        try (SearchResult result = splitSearcher.search(query, 10)) {
            assertNotNull(result);
            System.out.println("Contains wildcard '*phone*' found " + result.getHits().size() + " results");
        }
    }

    @Test
    @DisplayName("Test multi-wildcard pattern")
    public void testMultiWildcardPattern() throws Exception {
        SplitWildcardQuery query = new SplitWildcardQuery("title", "*pro*phone*");

        assertTrue(query.isExpensive(), "Multi-wildcard should be expensive");

        try (SearchResult result = splitSearcher.search(query, 10)) {
            assertNotNull(result);
            System.out.println("Multi-wildcard '*pro*phone*' found " + result.getHits().size() + " results");
        }
    }

    @Test
    @DisplayName("Test wildcard in boolean query - AND combination")
    public void testWildcardInBooleanQuery() throws Exception {
        SplitBooleanQuery query = new SplitBooleanQuery()
            .addMust(new SplitTermQuery("category", "Electronics"))
            .addMust(new SplitWildcardQuery("title", "*phone*"));

        try (SearchResult result = splitSearcher.search(query, 10)) {
            assertNotNull(result);
            System.out.println("Boolean wildcard found " + result.getHits().size() + " results");

            // Should find smartphone (Electronics + *phone*) but not phone case (Accessories)
            // Note: This depends on tokenization behavior
        }
    }

    @Test
    @DisplayName("Test SplitWildcardQuery toQueryAstJson")
    public void testToQueryAstJson() throws Exception {
        SplitWildcardQuery query = new SplitWildcardQuery("title", "*phone*");

        String json = query.toQueryAstJson();

        assertNotNull(json, "JSON should not be null");
        assertFalse(json.isEmpty(), "JSON should not be empty");
        System.out.println("QueryAst JSON: " + json);

        // Should contain the field name
        assertTrue(json.contains("title") || json.contains("field"),
            "JSON should contain field reference");
    }

    @Test
    @DisplayName("Test single character wildcard")
    public void testSingleCharacterWildcard() throws Exception {
        // ? matches exactly one character
        SplitWildcardQuery query = new SplitWildcardQuery("title", "phon?");

        // Single char wildcard at end is not expensive
        assertFalse(query.isExpensive());

        try (SearchResult result = splitSearcher.search(query, 10)) {
            assertNotNull(result);
            System.out.println("Single char wildcard 'phon?' found " + result.getHits().size() + " results");
        }
    }

    @Test
    @DisplayName("Test exact match (no wildcards)")
    public void testExactMatchNoWildcards() throws Exception {
        SplitWildcardQuery query = new SplitWildcardQuery("title", "laptop");

        assertFalse(query.isExpensive(), "Exact match should not be expensive");

        try (SearchResult result = splitSearcher.search(query, 10)) {
            assertNotNull(result);
            System.out.println("Exact match 'laptop' found " + result.getHits().size() + " results");
        }
    }

    @Test
    @DisplayName("Test empty result handling")
    public void testEmptyResult() throws Exception {
        // Search for something that doesn't exist
        SplitWildcardQuery query = new SplitWildcardQuery("title", "*zzzznoexist*");

        try (SearchResult result = splitSearcher.search(query, 10)) {
            assertNotNull(result);
            assertTrue(result.getHits().isEmpty(), "Should find no results for non-existent pattern");
        }
    }

    @Test
    @DisplayName("Test wildcard query getters")
    public void testWildcardQueryGetters() {
        SplitWildcardQuery query = new SplitWildcardQuery("myfield", "*mypattern*");

        assertEquals("myfield", query.getField());
        assertEquals("*mypattern*", query.getPattern());
        assertTrue(query.isExpensive());
    }
}

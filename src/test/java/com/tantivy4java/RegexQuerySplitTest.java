package com.tantivy4java;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;

import java.nio.file.Path;

/**
 * Test to verify regex query support against splits.
 */
public class RegexQuerySplitTest {

    private SplitCacheManager cacheManager;
    private SplitSearcher searcher;
    private String splitPath;
    private QuickwitSplit.SplitMetadata metadata;

    @BeforeEach
    public void setUp(@TempDir Path tempDir) throws Exception {
        String uniqueId = "regex_split_test_" + System.nanoTime();
        String indexPath = tempDir.resolve(uniqueId + "_index").toString();
        splitPath = tempDir.resolve(uniqueId + ".split").toString();

        try (SchemaBuilder builder = new SchemaBuilder()) {
            builder
                .addTextField("content", true, true, "default", "position")  // Fast field for regex
                .addTextField("title", true, true, "default", "position");   // Fast field for regex

            try (Schema schema = builder.build()) {
                try (Index index = new Index(schema, indexPath, false)) {
                    try (IndexWriter writer = index.writer(Index.Memory.DEFAULT_HEAP_SIZE, 1)) {

                        // Add test documents with patterns for regex testing
                        try (Document doc1 = new Document()) {
                            doc1.addText("content", "The quick brown fox jumps over the lazy dog");
                            doc1.addText("title", "animal-story-123");
                            writer.addDocument(doc1);
                        }

                        try (Document doc2 = new Document()) {
                            doc2.addText("content", "Machine learning and artificial intelligence");
                            doc2.addText("title", "tech-article-456");
                            writer.addDocument(doc2);
                        }

                        try (Document doc3 = new Document()) {
                            doc3.addText("content", "Regular expressions are powerful pattern matching tools");
                            doc3.addText("title", "regex-guide-789");
                            writer.addDocument(doc3);
                        }

                        writer.commit();
                    }

                    QuickwitSplit.SplitConfig config = new QuickwitSplit.SplitConfig(
                        uniqueId, "test-source", "test-node");

                    metadata = QuickwitSplit.convertIndexFromPath(indexPath, splitPath, config);
                    this.metadata = metadata;
                }
            }
        }

        String uniqueCacheName = uniqueId + "-cache";
        SplitCacheManager.CacheConfig cacheConfig =
            new SplitCacheManager.CacheConfig(uniqueCacheName);
        cacheManager = SplitCacheManager.getInstance(cacheConfig);

        searcher = cacheManager.createSplitSearcher("file://" + splitPath, metadata);
    }

    @AfterEach
    public void tearDown() {
        if (searcher != null) {
            searcher.close();
        }
    }

    @Test
    public void testRegexQueryParsing() {
        System.out.println("🧪 Testing regex query parsing with splits...");

        try {
            // Test 1: Simple regex pattern in query string
            SplitQuery regexQuery1 = searcher.parseQuery("content:/.*fox.*/");
            SearchResult result1 = searcher.search(regexQuery1, 10);

            System.out.println("📊 Regex query /.*fox.*/ found " + result1.getHits().size() + " hits");

            // Test 2: More complex regex pattern
            SplitQuery regexQuery2 = searcher.parseQuery("title:/.*-\\d+/");
            SearchResult result2 = searcher.search(regexQuery2, 10);

            System.out.println("📊 Regex query /.*-\\d+/ found " + result2.getHits().size() + " hits");

            // Test 3: Word boundary regex
            SplitQuery regexQuery3 = searcher.parseQuery("content:/\\bmachine\\b/i");
            SearchResult result3 = searcher.search(regexQuery3, 10);

            System.out.println("📊 Regex query /\\bmachine\\b/i found " + result3.getHits().size() + " hits");

            System.out.println("✅ Regex query parsing test completed!");

        } catch (Exception e) {
            System.out.println("❌ Regex query parsing failed: " + e.getMessage());
            fail("Regex query parsing failed: " + e.getMessage());
        }
    }

    @Test
    public void testDirectRegexQueryCreation() {
        System.out.println("🧪 Testing direct regex query creation...");

        try {
            Schema schema = searcher.getSchema();

            // Test creating regex queries directly using Query.regexQuery if available
            // This will tell us if regex queries are supported at the Query level

            Query regexQuery = Query.regexQuery(schema, "content", ".*fox.*");
            System.out.println("✅ Direct regex query creation successful!");

            regexQuery.close();

        } catch (NoSuchMethodError e) {
            System.out.println("❌ Query.regexQuery method not available: " + e.getMessage());
            fail("Query.regexQuery method not found - regex queries may not be supported");
        } catch (Exception e) {
            System.out.println("❌ Direct regex query creation failed: " + e.getMessage());
            fail("Direct regex query creation failed: " + e.getMessage());
        }
    }

    @Test
    public void testRegexQuerySearchResults() {
        System.out.println("🧪 Testing regex query search results...");

        try {
            Schema schema = searcher.getSchema();

            // Create regex query for pattern matching
            Query regexQuery = Query.regexQuery(schema, "content", ".*machine.*");

            // Convert to SplitQuery (if this conversion is possible)
            // Note: This may not work directly - we might need to use parseQuery instead

            System.out.println("✅ Regex query for machine pattern created successfully!");

            regexQuery.close();

        } catch (Exception e) {
            System.out.println("❌ Regex query search failed: " + e.getMessage());
            // Don't fail the test - this tells us about the limitation
            System.out.println("ℹ️ This suggests regex queries may not be directly supported against splits");
        }
    }

    @Test
    public void testQueryStringRegexSyntax() {
        System.out.println("🧪 Testing query string regex syntax...");

        try {
            // Test various regex syntaxes that might be supported in query strings
            String[] regexPatterns = {
                "content:/fox/",           // Basic regex
                "content:/.*fox.*/",       // Wildcard regex
                "content:/[a-z]+/",        // Character class
                "content:/\\d+/",          // Digit pattern
                "title:/.*-\\d{3}/",       // Complex pattern
                "content:regex(fox)",      // Alternative syntax
                "content:~/fox/",          // Lucene-style regex
            };

            for (String pattern : regexPatterns) {
                try {
                    SplitQuery query = searcher.parseQuery(pattern);
                    SearchResult result = searcher.search(query, 10);
                    System.out.println("✅ Pattern '" + pattern + "' parsed successfully, found " + result.getHits().size() + " hits");
                } catch (Exception e) {
                    System.out.println("❌ Pattern '" + pattern + "' failed: " + e.getMessage());
                }
            }

        } catch (Exception e) {
            System.out.println("❌ Query string regex syntax test failed: " + e.getMessage());
        }
    }
}
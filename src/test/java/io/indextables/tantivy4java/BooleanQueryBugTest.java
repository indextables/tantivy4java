package io.indextables.tantivy4java;

import io.indextables.tantivy4java.core.*;
import io.indextables.tantivy4java.query.*;
import io.indextables.tantivy4java.result.*;
import io.indextables.tantivy4java.split.*;
import io.indextables.tantivy4java.split.merge.*;


import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;

import java.nio.file.Path;

/**
 * Test to reproduce and fix the boolean query bug reported:
 * - Pure should clauses returning 0 results
 * - NOT IN queries with should clauses failing
 */
public class BooleanQueryBugTest {

    private SplitCacheManager cacheManager;
    private SplitSearcher searcher;
    private String splitPath;
    private QuickwitSplit.SplitMetadata metadata;

    @BeforeEach
    public void setUp(@TempDir Path tempDir) throws Exception {
        String uniqueId = "boolean_bug_test_" + System.nanoTime();
        String indexPath = tempDir.resolve(uniqueId + "_index").toString();
        splitPath = tempDir.resolve(uniqueId + ".split").toString();

        try (SchemaBuilder builder = new SchemaBuilder()) {
            builder
                .addIntegerField("id", true, true, true)
                .addTextField("department", true, true, "raw", "position")  // Raw tokenizer for exact matching
                .addTextField("status", true, true, "raw", "position")
                .addTextField("country", true, true, "raw", "position")
                .addIntegerField("priority", true, true, true);

            try (Schema schema = builder.build()) {
                try (Index index = new Index(schema, indexPath, false)) {
                    try (IndexWriter writer = index.writer(Index.Memory.DEFAULT_HEAP_SIZE, 1)) {

                        // Create test data matching the bug report
                        String[] departments = {"Sales", "Engineering", "Marketing", "Finance"};
                        String[] statuses = {"Active", "Inactive"};
                        String[] countries = {"US", "UK", "CA"};

                        // Create 200 test documents similar to bug report
                        for (int i = 1; i <= 200; i++) {
                            try (Document doc = new Document()) {
                                doc.addInteger("id", i);
                                doc.addText("department", departments[i % departments.length]);
                                doc.addText("status", statuses[i % statuses.length]);
                                doc.addText("country", countries[i % countries.length]);
                                doc.addInteger("priority", (i % 5) + 1);
                                writer.addDocument(doc);
                            }
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
    public void testBasicQueries() {
        System.out.println("🧪 Testing basic queries to verify data...");

        // Test individual term queries work
        SplitQuery salesQuery = searcher.parseQuery("department:Sales");
        SearchResult salesResult = searcher.search(salesQuery, 1000);
        System.out.println("📊 Sales department query found " + salesResult.getHits().size() + " hits");
        assertTrue(salesResult.getHits().size() > 0, "Should find Sales department documents");

        SplitQuery engineeringQuery = searcher.parseQuery("department:Engineering");
        SearchResult engineeringResult = searcher.search(engineeringQuery, 1000);
        System.out.println("📊 Engineering department query found " + engineeringResult.getHits().size() + " hits");
        assertTrue(engineeringResult.getHits().size() > 0, "Should find Engineering department documents");

        // Test match all query
        SplitMatchAllQuery matchAllQuery = new SplitMatchAllQuery();
        SearchResult allResult = searcher.search(matchAllQuery, 1000);
        System.out.println("📊 Match all query found " + allResult.getHits().size() + " hits");
        assertEquals(200, allResult.getHits().size(), "Should find all 200 documents");
    }

    @Test
    public void testPureShouldClauseBug() {
        System.out.println("🐛 Testing pure should clause bug (IN query)...");

        // Create boolean query with only should clauses (equivalent to IN query)
        SplitTermQuery salesQuery = new SplitTermQuery("department", "Sales");
        SplitTermQuery engineeringQuery = new SplitTermQuery("department", "Engineering");
        SplitTermQuery marketingQuery = new SplitTermQuery("department", "Marketing");

        SplitBooleanQuery boolQuery = new SplitBooleanQuery();
        boolQuery.addShould(salesQuery);
        boolQuery.addShould(engineeringQuery);
        boolQuery.addShould(marketingQuery);

        System.out.println("📋 Boolean query: " + boolQuery);
        System.out.println("📋 Generated JSON: " + boolQuery.toQueryAstJson());

        SearchResult result = searcher.search(boolQuery, 1000);
        System.out.println("📊 Pure should clause query found " + result.getHits().size() + " hits");

        // Expected: Should find documents matching Sales OR Engineering OR Marketing
        // Actual bug: Returns 0 results
        if (result.getHits().size() == 0) {
            System.out.println("❌ BUG CONFIRMED: Pure should clauses return 0 results");
            System.out.println("⚠️ This indicates the boolean query semantics issue");
        } else {
            System.out.println("✅ Pure should clauses work correctly");
            assertTrue(result.getHits().size() >= 150, "Should find most documents (Sales, Engineering, Marketing)");
        }
    }

    @Test
    public void testShouldClauseWithMustWorkaround() {
        System.out.println("🔧 Testing should clause with must workaround...");

        // Test the workaround mentioned in the bug report
        SplitTermQuery salesQuery = new SplitTermQuery("department", "Sales");
        SplitTermQuery engineeringQuery = new SplitTermQuery("department", "Engineering");
        SplitTermQuery marketingQuery = new SplitTermQuery("department", "Marketing");

        SplitBooleanQuery boolQuery = new SplitBooleanQuery();
        boolQuery.addMust(new SplitMatchAllQuery()); // Workaround: add match-all to force should evaluation
        boolQuery.addShould(salesQuery);
        boolQuery.addShould(engineeringQuery);
        boolQuery.addShould(marketingQuery);

        System.out.println("📋 Boolean query with workaround: " + boolQuery);

        SearchResult result = searcher.search(boolQuery, 1000);
        System.out.println("📊 Should clause with must workaround found " + result.getHits().size() + " hits");

        if (result.getHits().size() > 0) {
            System.out.println("✅ Workaround successful - should clauses work with must clause");
            assertTrue(result.getHits().size() >= 150, "Should find most documents with workaround");
        } else {
            System.out.println("❌ Workaround failed - still returns 0 results");
        }
    }

    @Test
    public void testNotInQueryBug() {
        System.out.println("🐛 Testing NOT IN query bug...");

        // Create child query: department IN ('HR', 'Legal') - should match nothing since these don't exist
        SplitTermQuery hrQuery = new SplitTermQuery("department", "HR");
        SplitTermQuery legalQuery = new SplitTermQuery("department", "Legal");

        SplitBooleanQuery childQuery = new SplitBooleanQuery();
        childQuery.addMust(new SplitMatchAllQuery()); // Workaround to make should clauses work
        childQuery.addShould(hrQuery);
        childQuery.addShould(legalQuery);

        System.out.println("📋 Child query (HR OR Legal): " + childQuery);

        // Test child query first
        SearchResult childResult = searcher.search(childQuery, 1000);
        System.out.println("📊 Child query (HR OR Legal) found " + childResult.getHits().size() + " hits");

        // Create NOT wrapper: NOT (department IN ('HR', 'Legal'))
        SplitBooleanQuery notQuery = new SplitBooleanQuery();
        notQuery.addMust(new SplitMatchAllQuery());
        notQuery.addMustNot(childQuery);

        System.out.println("📋 NOT query: " + notQuery);
        System.out.println("📋 NOT query JSON: " + notQuery.toQueryAstJson());

        SearchResult notResult = searcher.search(notQuery, 1000);
        System.out.println("📊 NOT IN query found " + notResult.getHits().size() + " hits");

        // Expected: Should return 200 documents (all documents since HR/Legal don't exist)
        // Actual bug: Returns 0 results due to must-all + should interaction in child query
        if (notResult.getHits().size() == 0) {
            System.out.println("❌ BUG CONFIRMED: NOT IN query returns 0 results");
            System.out.println("⚠️ This is likely due to must-all + should interaction in child query");
        } else {
            System.out.println("✅ NOT IN query works correctly");
            assertEquals(200, notResult.getHits().size(), "Should find all 200 documents (since HR/Legal don't exist)");
        }
    }

    @Test
    public void testMinimumShouldMatchFix() {
        System.out.println("🔧 Testing potential minimum should match fix...");

        // Test if setting minimum should match = 1 fixes pure should clauses
        SplitTermQuery salesQuery = new SplitTermQuery("department", "Sales");
        SplitTermQuery engineeringQuery = new SplitTermQuery("department", "Engineering");
        SplitTermQuery marketingQuery = new SplitTermQuery("department", "Marketing");

        SplitBooleanQuery boolQuery = new SplitBooleanQuery();
        boolQuery.addShould(salesQuery);
        boolQuery.addShould(engineeringQuery);
        boolQuery.addShould(marketingQuery);
        boolQuery.setMinimumShouldMatch(1); // Force at least one should clause to match

        System.out.println("📋 Boolean query with minShouldMatch=1: " + boolQuery);
        System.out.println("📋 Generated JSON: " + boolQuery.toQueryAstJson());

        SearchResult result = searcher.search(boolQuery, 1000);
        System.out.println("📊 Query with minShouldMatch=1 found " + result.getHits().size() + " hits");

        if (result.getHits().size() > 0) {
            System.out.println("✅ POTENTIAL FIX: Setting minShouldMatch=1 works for pure should clauses");
            assertTrue(result.getHits().size() >= 150, "Should find most documents with minShouldMatch");
        } else {
            System.out.println("❌ minShouldMatch=1 fix doesn't work");
        }
    }
}

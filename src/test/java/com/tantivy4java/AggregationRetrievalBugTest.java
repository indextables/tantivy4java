package com.tantivy4java;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;

import java.nio.file.Path;
import java.util.Map;
import java.util.HashMap;

/**
 * Test to reproduce the aggregation retrieval bug from TANTIVY4JAVA_AGGREGATION_RETRIEVAL_BUG_REPORT.md
 *
 * The bug: SearchResult.getAggregation(name) returns null despite successful Rust-side processing
 * and hasAggregations() returning true.
 */
public class AggregationRetrievalBugTest {

    private SplitCacheManager cacheManager;
    private SplitSearcher searcher;
    private String splitPath;
    private QuickwitSplit.SplitMetadata metadata;

    @BeforeEach
    public void setUp(@TempDir Path tempDir) throws Exception {
        String uniqueId = "agg_bug_test_" + System.nanoTime();
        String indexPath = tempDir.resolve(uniqueId + "_index").toString();
        splitPath = tempDir.resolve(uniqueId + ".split").toString();

        try (SchemaBuilder builder = new SchemaBuilder()) {
            builder
                .addTextField("status", true, true, "default", "position")  // Set fast=true for aggregations
                .addIntegerField("id", true, true, true);

            try (Schema schema = builder.build()) {
                try (Index index = new Index(schema, indexPath, false)) {
                    try (IndexWriter writer = index.writer(Index.Memory.DEFAULT_HEAP_SIZE, 1)) {

                        // Add test documents
                        try (Document doc1 = new Document()) {
                            doc1.addInteger("id", 1);
                            doc1.addText("status", "active");
                            writer.addDocument(doc1);
                        }

                        try (Document doc2 = new Document()) {
                            doc2.addInteger("id", 2);
                            doc2.addText("status", "active");
                            writer.addDocument(doc2);
                        }

                        try (Document doc3 = new Document()) {
                            doc3.addInteger("id", 3);
                            doc3.addText("status", "inactive");
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
    public void testAggregationRetrievalBug() {
        System.out.println("🐛 Reproducing aggregation retrieval bug...");

        // Create TermsAggregation exactly as described in bug report
        TermsAggregation termsAgg = new TermsAggregation("status", 1000);
        SplitQuery matchAllQuery = new SplitMatchAllQuery();

        // Execute search with aggregation using "agg_0" name as in bug report
        SearchResult result = searcher.search(matchAllQuery, 1000000, "agg_0", termsAgg);

        System.out.println("🔍 hasAggregations(): " + result.hasAggregations());
        System.out.println("🔍 getAggregations() map size: " + result.getAggregations().size());
        System.out.println("🔍 getAggregations() keys: " + result.getAggregations().keySet());

        // These should not both be true simultaneously (as per bug report)
        assertTrue(result.hasAggregations(), "hasAggregations() should return true");

        // Try to get aggregation by "agg_0" name
        TermsResult agg0Result = (TermsResult) result.getAggregation("agg_0");
        System.out.println("🔍 getAggregation('agg_0'): " + agg0Result);

        // Try to get aggregation by "terms" name (inner structure)
        TermsResult termsResult = (TermsResult) result.getAggregation("terms");
        System.out.println("🔍 getAggregation('terms'): " + termsResult);

        // Try to get aggregation by "status" name (field name)
        TermsResult statusResult = (TermsResult) result.getAggregation("status");
        System.out.println("🔍 getAggregation('status'): " + statusResult);

        // THIS IS THE BUG: Both should not fail simultaneously
        if (agg0Result == null && termsResult == null && statusResult == null) {
            fail("BUG REPRODUCED: getAggregation() returns null for all possible names " +
                 "despite hasAggregations()=true. Available keys: " + result.getAggregations().keySet());
        }

        // If we get here, the bug is fixed
        System.out.println("✅ Aggregation retrieval working correctly!");
    }

    @Test
    public void testMultipleAggregationsMap() {
        System.out.println("🔍 Testing multiple aggregations map approach...");

        // Test with Map approach (like SplitSearcherAggregationTest)
        Map<String, SplitAggregation> aggregations = new HashMap<>();
        aggregations.put("status_terms", new TermsAggregation("status", 100));
        aggregations.put("count_total", new CountAggregation("id"));

        SplitQuery matchAllQuery = new SplitMatchAllQuery();
        SearchResult result = searcher.search(matchAllQuery, 10, aggregations);

        System.out.println("🔍 hasAggregations(): " + result.hasAggregations());
        System.out.println("🔍 getAggregations() map size: " + result.getAggregations().size());
        System.out.println("🔍 getAggregations() keys: " + result.getAggregations().keySet());

        assertTrue(result.hasAggregations(), "Should have aggregations");

        // Try to retrieve by the exact names we put in
        TermsResult statusTerms = (TermsResult) result.getAggregation("status_terms");
        CountResult countTotal = (CountResult) result.getAggregation("count_total");

        System.out.println("🔍 getAggregation('status_terms'): " + statusTerms);
        System.out.println("🔍 getAggregation('count_total'): " + countTotal);

        if (statusTerms == null || countTotal == null) {
            fail("Map-based aggregation retrieval failed. status_terms=" + statusTerms +
                 ", count_total=" + countTotal +
                 ". Available keys: " + result.getAggregations().keySet());
        }

        System.out.println("✅ Map-based aggregation retrieval working!");
    }
}
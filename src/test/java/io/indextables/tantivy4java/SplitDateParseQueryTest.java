package io.indextables.tantivy4java;

import io.indextables.tantivy4java.core.*;
import io.indextables.tantivy4java.result.*;
import io.indextables.tantivy4java.split.*;
import io.indextables.tantivy4java.split.merge.*;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;

import java.nio.file.Path;
import java.time.LocalDateTime;

/**
 * Test that Tantivy's query parser correctly handles RFC3339 datetime strings
 * for DATE fields using parseQuery() instead of SplitRangeQuery.
 *
 * This test answers the research question:
 * "Does SplitSearcher.parseQuery() correctly handle RFC3339 datetime strings
 *  in range queries for DATE fields in split searches?"
 */
public class SplitDateParseQueryTest {

    private SplitCacheManager cacheManager;
    private SplitSearcher searcher;
    private String splitPath;

    @BeforeEach
    public void setUp(@TempDir Path tempDir) throws Exception {
        String uniqueId = "date_parse_test_" + System.nanoTime();
        String indexPath = tempDir.resolve(uniqueId + "_index").toString();
        splitPath = tempDir.resolve(uniqueId + ".split").toString();

        System.out.println("\n=== DATE Field parseQuery Test ===");

        try (SchemaBuilder builder = new SchemaBuilder()) {
            // Build schema with fast date field (enables microsecond precision)
            builder.addIntegerField("id", true, true, true)
                   .addDateField("timestamp", true, true, true);  // stored, indexed, FAST

            try (Schema schema = builder.build()) {
                try (Index index = new Index(schema, indexPath, false)) {
                    // Write documents with LocalDateTime
                    try (IndexWriter writer = index.writer(Index.Memory.DEFAULT_HEAP_SIZE, 1)) {
                        LocalDateTime dt1 = LocalDateTime.of(2025, 11, 7, 5, 0, 0);
                        LocalDateTime dt2 = LocalDateTime.of(2025, 11, 7, 6, 0, 0);
                        LocalDateTime dt3 = LocalDateTime.of(2025, 11, 7, 7, 0, 0);

                        try (Document doc1 = new Document()) {
                            doc1.addInteger("id", 1);
                            doc1.addDate("timestamp", dt1);
                            writer.addDocument(doc1);
                        }

                        try (Document doc2 = new Document()) {
                            doc2.addInteger("id", 2);
                            doc2.addDate("timestamp", dt2);
                            writer.addDocument(doc2);
                        }

                        try (Document doc3 = new Document()) {
                            doc3.addInteger("id", 3);
                            doc3.addDate("timestamp", dt3);
                            writer.addDocument(doc3);
                        }

                        writer.commit();
                        System.out.println("✓ Indexed 3 documents with timestamps");
                        System.out.println("  - Doc 1: 2025-11-07 05:00:00");
                        System.out.println("  - Doc 2: 2025-11-07 06:00:00");
                        System.out.println("  - Doc 3: 2025-11-07 07:00:00");
                    }

                    // Create split from index
                    System.out.println("Creating split at: " + splitPath);
                    QuickwitSplit.SplitConfig config = new QuickwitSplit.SplitConfig(
                        uniqueId, "test-source", "test-node");

                    QuickwitSplit.SplitMetadata metadata = QuickwitSplit.convertIndexFromPath(
                        indexPath, splitPath, config);
                    System.out.println("✓ Created split file with " + metadata.getNumDocs() + " documents");

                    // Create cache manager and searcher
                    String uniqueCacheName = uniqueId + "-cache";
                    SplitCacheManager.CacheConfig cacheConfig =
                        new SplitCacheManager.CacheConfig(uniqueCacheName);
                    cacheManager = SplitCacheManager.getInstance(cacheConfig);

                    searcher = cacheManager.createSplitSearcher("file://" + splitPath, metadata);
                    System.out.println("✓ Created split searcher");
                }
            }
        }
    }

    @AfterEach
    public void tearDown() {
        if (searcher != null) {
            searcher.close();
        }
    }

    @Test
    @DisplayName("Test parseQuery with RFC3339 datetime strings for DATE fields")
    public void testDateTimeParseQuery() {
        System.out.println("\n=== Testing parseQuery with RFC3339 datetime strings ===");

        // Test 1: Equality using range syntax (covers one second) - WITH Z suffix
        String query1 = "timestamp:[2025-11-07T05:00:00Z TO 2025-11-07T05:00:01Z}";
        System.out.println("\n📋 Test 1: Equality range with Z suffix");
        System.out.println("Query: " + query1);

        SplitQuery q1 = searcher.parseQuery(query1);
        SearchResult result1 = searcher.search(q1, 10);
        int hits1 = result1.getHits().size();
        System.out.println("  Hits: " + hits1);
        System.out.println("  Expected: 1 (document at 05:00:00)");
        assertEquals(1, hits1, "Should match document at 05:00:00 with Z suffix");

        // Test 2: Range query spanning multiple hours - WITH Z suffix
        String query2 = "timestamp:[2025-11-07T05:30:00Z TO 2025-11-07T06:30:00Z]";
        System.out.println("\n📋 Test 2: Range spanning hours with Z suffix");
        System.out.println("Query: " + query2);

        SplitQuery q2 = searcher.parseQuery(query2);
        SearchResult result2 = searcher.search(q2, 10);
        int hits2 = result2.getHits().size();
        System.out.println("  Hits: " + hits2);
        System.out.println("  Expected: 1 (document at 06:00:00)");
        assertEquals(1, hits2, "Should match document at 06:00:00 with Z suffix");

        // Test 3: Open-ended range (all documents >= 06:00:00)
        String query3 = "timestamp:[2025-11-07T06:00:00Z TO *]";
        System.out.println("\n📋 Test 3: Open-ended range with Z suffix");
        System.out.println("Query: " + query3);

        SplitQuery q3 = searcher.parseQuery(query3);
        SearchResult result3 = searcher.search(q3, 10);
        int hits3 = result3.getHits().size();
        System.out.println("  Hits: " + hits3);
        System.out.println("  Expected: 2 (documents at 06:00:00 and 07:00:00)");
        assertEquals(2, hits3, "Should match documents at 06:00:00 and 07:00:00");

        System.out.println("\n✓ All parseQuery tests with Z suffix passed!");
    }

    @Test
    @DisplayName("Test parseQuery with RFC3339 datetime strings WITHOUT Z suffix")
    public void testDateTimeParseQueryWithoutZSuffix() {
        System.out.println("\n=== Testing parseQuery WITHOUT Z suffix ===");

        // Test 4: Equality using range syntax - WITHOUT Z suffix
        String query4 = "timestamp:[2025-11-07T05:00:00 TO 2025-11-07T05:00:01}";
        System.out.println("\n📋 Test 4: Equality range without Z suffix");
        System.out.println("Query: " + query4);

        SplitQuery q4 = searcher.parseQuery(query4);
        SearchResult result4 = searcher.search(q4, 10);
        int hits4 = result4.getHits().size();
        System.out.println("  Hits: " + hits4);
        System.out.println("  Expected: 1 (document at 05:00:00)");
        assertEquals(1, hits4, "Should match document at 05:00:00 without Z suffix");

        // Test 5: Range query - WITHOUT Z suffix
        String query5 = "timestamp:[2025-11-07T05:30:00 TO 2025-11-07T06:30:00]";
        System.out.println("\n📋 Test 5: Range query without Z suffix");
        System.out.println("Query: " + query5);

        SplitQuery q5 = searcher.parseQuery(query5);
        SearchResult result5 = searcher.search(q5, 10);
        int hits5 = result5.getHits().size();
        System.out.println("  Hits: " + hits5);
        System.out.println("  Expected: 1 (document at 06:00:00)");
        assertEquals(1, hits5, "Should match document at 06:00:00 without Z suffix");

        System.out.println("\n✓ All parseQuery tests without Z suffix passed!");
    }

    @Test
    @DisplayName("Test parseQuery with document retrieval to verify correct results")
    public void testDateTimeParseQueryWithDocumentRetrieval() {
        System.out.println("\n=== Verifying parseQuery results with document retrieval ===");

        // Query for all documents in the time range
        String query = "timestamp:[2025-11-07T05:00:00Z TO 2025-11-07T08:00:00Z]";
        System.out.println("Query: " + query);

        SplitQuery q = searcher.parseQuery(query);
        SearchResult result = searcher.search(q, 10);

        System.out.println("Found " + result.getHits().size() + " documents");
        assertEquals(3, result.getHits().size(), "Should find all 3 documents");

        // Retrieve and verify each document
        for (int i = 0; i < result.getHits().size(); i++) {
            SearchResult.Hit hit = result.getHits().get(i);

            try (Document doc = searcher.doc(hit.getDocAddress())) {
                Number idNumber = (Number) doc.getFirst("id");
                LocalDateTime timestamp = (LocalDateTime) doc.getFirst("timestamp");

                int id = idNumber.intValue();
                System.out.println("  Doc " + id + ": timestamp = " + timestamp);

                assertNotNull(timestamp, "Timestamp should not be null");
                assertEquals(2025, timestamp.getYear(), "Year should be 2025");
                assertEquals(11, timestamp.getMonthValue(), "Month should be November");
                assertEquals(7, timestamp.getDayOfMonth(), "Day should be 7");
            }
        }

        System.out.println("✓ Document retrieval verification passed!");
    }
}

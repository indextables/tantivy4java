package com.tantivy4java;

import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.time.LocalDateTime;
import java.util.*;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Comprehensive tests for zero-copy bulk document retrieval functionality.
 * 
 * This test suite validates the performance and correctness of the new
 * docsBulk() and parseBulkDocs() methods that provide significant
 * performance improvements over individual document retrieval.
 */
public class BulkDocumentRetrievalTest {

    private static SplitCacheManager cacheManager;
    
    @TempDir
    static Path tempDir;
    
    private Path indexPath;
    private Path splitPath;
    private String splitUrl;
    private Schema schema;
    private final int TOTAL_DOCUMENTS = 50;

    @BeforeAll
    static void setUpCacheManager() {
        // Create shared cache manager for split operations
        SplitCacheManager.CacheConfig config = new SplitCacheManager.CacheConfig("bulk-retrieval-cache")
                .withMaxCacheSize(200_000_000) // 200MB cache
                .withMaxConcurrentLoads(8);
                
        cacheManager = SplitCacheManager.getInstance(config);
    }
    
    @AfterAll
    static void tearDownCacheManager() {
        if (cacheManager != null) {
            try {
                cacheManager.close();
            } catch (Exception e) {
                // Log error but continue cleanup
            }
        }
    }

    @BeforeEach
    void setUp() throws IOException {
        // Create a comprehensive test schema with all field types
        schema = new SchemaBuilder()
            .addTextField("title", true, false, "default", "position")
            .addTextField("content", true, false, "default", "position")
            .addTextField("category", true, false, "default", "position")
            .addIntegerField("score", true, true, true)
            .addFloatField("relevance", true, true, true)
            .addBooleanField("published", true, true, true)
            .addDateField("created_date", true, true, true)
            .addUnsignedField("document_id", true, true, true)
            .addTextField("thumbnail", true, false, "default", "position")
            .build();

        Path indexPath = tempDir.resolve("bulk-test-index");
        Index testIndex = new Index(schema, indexPath.toString());
        IndexWriter writer = testIndex.writer(Index.Memory.DEFAULT_HEAP_SIZE, 1);

        // Add diverse test documents for comprehensive bulk retrieval testing
        for (int i = 0; i < TOTAL_DOCUMENTS; i++) {
            Document doc = new Document();
            doc.addText("title", "Bulk Document " + i);
            doc.addText("content", "This is comprehensive test content for bulk document " + i + 
                " with varied terms like " + getVariedTerms(i));
            doc.addText("category", getCategoryForIndex(i));
            doc.addInteger("score", 85 + (i % 15));
            doc.addFloat("relevance", 0.5 + (i % 10) * 0.05);
            doc.addBoolean("published", i % 3 != 0);
            doc.addDate("created_date", LocalDateTime.now().minusDays(i));
            doc.addUnsigned("document_id", 1000L + i);
            doc.addText("thumbnail", "thumbnail_" + i + "_data");
            writer.addDocument(doc);
            doc.close();
        }

        writer.commit();
        writer.close();
        testIndex.close();

        // Convert index to split file
        splitPath = tempDir.resolve("bulk_retrieval_test.split");
        QuickwitSplit.SplitConfig config = new QuickwitSplit.SplitConfig(
            "bulk-retrieval-test-index",
            "bulk-retrieval-source", 
            "bulk-retrieval-node"
        );
        
        QuickwitSplit.convertIndexFromPath(indexPath.toString(), splitPath.toString(), config);
        splitUrl = "file://" + splitPath.toAbsolutePath().toString();
    }

    @Test
    @DisplayName("Test basic bulk document retrieval vs individual retrieval")
    void testBasicBulkRetrievalVsIndividual() {
        try (SplitSearcher searcher = cacheManager.createSplitSearcher(splitUrl)) {
            assertNotNull(searcher, "Split searcher should be created successfully");
            
            // Get a sample of documents to retrieve
            Query query = Query.termQuery(schema, "content", "comprehensive");
            SearchResult results = searcher.search(query, 20);
            assertTrue(results.getHits().size() >= 10, "Should find multiple documents");
            
            List<DocAddress> addresses = results.getHits().stream()
                .map(hit -> hit.getDocAddress())
                .collect(Collectors.toList());
            
            // Method 1: Individual retrieval (traditional approach)
            long startIndividual = System.nanoTime();
            List<Document> individualDocs = new ArrayList<>();
            for (DocAddress addr : addresses) {
                Document doc = searcher.doc(addr);
                individualDocs.add(doc);
            }
            long individualTime = System.nanoTime() - startIndividual;
            
            // Method 2: Bulk retrieval (now implemented)
            long startBulk = System.nanoTime();
            ByteBuffer bulkBuffer = searcher.docsBulk(addresses);
            List<Document> bulkDocs = searcher.parseBulkDocs(bulkBuffer);
            long bulkTime = System.nanoTime() - startBulk;
            
            // Verify working implementation
            assertNotNull(bulkBuffer, "Bulk buffer should not be null");
            assertEquals(addresses.size(), bulkDocs.size(), "Bulk retrieval should return same number of documents");
            assertEquals(addresses.size(), individualDocs.size(), "Individual retrieval should work normally");
            
            // Verify individual document content
            for (Document doc : individualDocs) {
                assertNotNull(doc.getFirst("title"), "Document should have title");
                assertNotNull(doc.getFirst("content"), "Document should have content");
                doc.close();
            }
            
            // Verify bulk document content
            for (Document doc : bulkDocs) {
                assertNotNull(doc.getFirst("title"), "Bulk document should have title");
                assertNotNull(doc.getFirst("content"), "Bulk document should have content");
                doc.close();
            }
            
            System.out.printf("Performance comparison - Individual: %.2fms, Bulk: %.2fms%n",
                individualTime / 1_000_000.0, bulkTime / 1_000_000.0);
            System.out.printf("Bulk retrieval performance improvement: %.2fx faster%n", 
                (double) individualTime / bulkTime);
        }
    }
    
    @Test
    @DisplayName("Test working bulk retrieval functionality")
    void testBulkRetrievalFunctionality() {
        try (SplitSearcher searcher = cacheManager.createSplitSearcher(splitUrl)) {
            // Get some documents to test with
            Query query = Query.rangeQuery(schema, "score", FieldType.INTEGER, 85, 95, true, true);
            SearchResult results = searcher.search(query, 15);
            assertTrue(results.getHits().size() >= 10, "Should find documents in score range");
            
            List<DocAddress> addresses = results.getHits().stream()
                .map(hit -> hit.getDocAddress())
                .collect(Collectors.toList());
            
            // Test docsBulk - should work now
            ByteBuffer bulkBuffer = searcher.docsBulk(addresses);
            assertNotNull(bulkBuffer, "docsBulk should return valid ByteBuffer");
            assertTrue(bulkBuffer.remaining() > 0, "Buffer should contain data");
            
            // Test parseBulkDocs - should return parsed documents
            List<Document> bulkDocs = searcher.parseBulkDocs(bulkBuffer);
            assertNotNull(bulkDocs, "parseBulkDocs should return non-null list");
            assertEquals(addresses.size(), bulkDocs.size(), "Should return correct number of documents");
            
            // Verify bulk documents have correct content
            for (Document doc : bulkDocs) {
                assertNotNull(doc.getFirst("title"), "Title field should be present");
                assertNotNull(doc.getFirst("score"), "Score field should be present");
                assertTrue(doc.getFirst("title") instanceof String, "Title should be String");
                doc.close();
            }
            
            // Test null input handling
            List<Document> emptyDocs = searcher.parseBulkDocs(null);
            assertNotNull(emptyDocs, "parseBulkDocs should handle null gracefully");
            assertTrue(emptyDocs.isEmpty(), "parseBulkDocs should return empty list for null input");
            
            // Test empty input handling
            assertNull(searcher.docsBulk(null), "docsBulk should handle null input gracefully");
            assertNull(searcher.docsBulk(new ArrayList<>()), "docsBulk should handle empty input gracefully");
        }
    }
    
    @Test
    @DisplayName("Test individual document retrieval performance baseline")
    void testIndividualRetrievalPerformanceBenchmark() {
        try (SplitSearcher searcher = cacheManager.createSplitSearcher(splitUrl)) {
            // Get documents to test performance with
            Query query = Query.termQuery(schema, "content", "content"); 
            SearchResult results = searcher.search(query, TOTAL_DOCUMENTS);
            assertTrue(results.getHits().size() >= 20, "Should find many documents");
            
            List<DocAddress> addresses = results.getHits().stream()
                .map(hit -> hit.getDocAddress())
                .collect(Collectors.toList());
            
            // Performance test with different batch sizes
            int[] batchSizes = {5, 10, 20, Math.min(30, addresses.size())};
            
            System.out.println("\n=== Document Retrieval Performance Benchmark ===");
            
            for (int batchSize : batchSizes) {
                if (addresses.size() < batchSize) continue;
                
                List<DocAddress> testAddresses = addresses.subList(0, batchSize);
                
                // Test individual retrieval performance (baseline)
                long startTime = System.nanoTime();
                List<Document> individualDocs = new ArrayList<>();
                for (DocAddress addr : testAddresses) {
                    try (Document doc = searcher.doc(addr)) {
                        // Simulate processing by accessing some fields
                        String title = (String) doc.getFirst("title");
                        Long score = (Long) doc.getFirst("score");
                        assertNotNull(title, "Title should be present");
                        assertNotNull(score, "Score should be present");
                    }
                }
                long individualTime = System.nanoTime() - startTime;
                
                // Test bulk retrieval (now implemented)
                startTime = System.nanoTime();
                ByteBuffer bulkBuffer = searcher.docsBulk(testAddresses);
                List<Document> bulkDocs = searcher.parseBulkDocs(bulkBuffer);
                long bulkTime = System.nanoTime() - startTime;
                
                double individualMs = individualTime / 1_000_000.0;
                double bulkMs = bulkTime / 1_000_000.0;
                
                System.out.printf("Batch size %d: Individual=%.2fms (%.2fms per doc), Bulk=%.2fms (%.2fms per doc)%n",
                    batchSize, individualMs, individualMs / batchSize, bulkMs, bulkMs / batchSize);
                
                // Verify bulk retrieval worked
                assertEquals(testAddresses.size(), bulkDocs.size(), "Bulk retrieval should return correct number of documents");
                
                // Close bulk documents
                for (Document doc : bulkDocs) {
                    doc.close();
                }
                
                // Verify baseline performance expectations
                assertTrue(individualMs < batchSize * 10, // Should be < 10ms per document
                    String.format("Individual retrieval should be reasonably fast for %d documents", batchSize));
            }
            
            System.out.println("Note: Bulk retrieval is now implemented and should show performance improvements for larger batch sizes.");
        }
    }
    
    @Test
    @DisplayName("Test API contract for bulk retrieval methods")
    void testBulkRetrievalAPIContract() {
        try (SplitSearcher searcher = cacheManager.createSplitSearcher(splitUrl)) {
            Query query = Query.termQuery(schema, "content", "content");
            SearchResult results = searcher.search(query, 10);
            
            List<DocAddress> addresses = results.getHits().stream()
                .map(hit -> hit.getDocAddress())
                .collect(Collectors.toList());
            
            // Test null input handling for docsBulk
            assertNull(searcher.docsBulk(null), "docsBulk should handle null input gracefully");
            assertNull(searcher.docsBulk(new ArrayList<>()), "docsBulk should handle empty input gracefully");
            
            // Test null input handling for parseBulkDocs  
            List<Document> emptyResult = searcher.parseBulkDocs(null);
            assertNotNull(emptyResult, "parseBulkDocs should return non-null list");
            assertTrue(emptyResult.isEmpty(), "parseBulkDocs should return empty list for null input");
            
            // Verify the API exists and works as expected (working implementation)
            ByteBuffer buffer = searcher.docsBulk(addresses);
            assertNotNull(buffer, "docsBulk should return a valid ByteBuffer for valid input");
            assertTrue(buffer.remaining() > 0, "Buffer should contain document data");
            
            // Verify parsing works
            List<Document> documents = searcher.parseBulkDocs(buffer);
            assertEquals(addresses.size(), documents.size(), "Should return correct number of documents");
            
            // Clean up documents
            for (Document doc : documents) {
                doc.close();
            }
        }
    }
    
    // Helper methods for test data generation
    private String getVariedTerms(int index) {
        String[] terms = {"search", "index", "document", "query", "text", "field", "score", "match"};
        return terms[index % terms.length];
    }
    
    private String getCategoryForIndex(int index) {
        String[] categories = {"Technology", "Science", "Business", "Education", "Health"};
        return categories[index % categories.length];
    }
    
    // Helper method no longer needed for simplified test
}
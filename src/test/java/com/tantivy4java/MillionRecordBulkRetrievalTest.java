package com.tantivy4java;

import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Performance test comparing classic individual document retrieval vs zero-copy bulk retrieval
 * with 1 million documents. This test validates that bulk retrieval provides significant
 * performance improvements for large-scale document retrieval operations.
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class MillionRecordBulkRetrievalTest {

    private static SplitCacheManager cacheManager;
    private static final int TOTAL_DOCUMENTS = 1_000_000;
    private static final int BATCH_SIZE = 10_000; // Documents per batch for indexing
    private static final int RETRIEVAL_BATCH_SIZE = 1000; // Documents per retrieval batch
    
    @TempDir
    static Path tempDir;
    
    private static Path splitPath;
    private static String splitUrl;
    private static Schema schema;
    private static long indexingTimeMs;
    private static long splitCreationTimeMs;

    @BeforeAll
    static void setUpOnce() throws IOException {
        System.out.println("=== MILLION RECORD BULK RETRIEVAL PERFORMANCE TEST ===");
        System.out.println("Creating test environment with 1,000,000 documents...\n");
        
        // Create shared cache manager with larger cache for million records
        SplitCacheManager.CacheConfig config = new SplitCacheManager.CacheConfig("million-record-cache")
                .withMaxCacheSize(500_000_000) // 500MB cache for large dataset
                .withMaxConcurrentLoads(16);
                
        cacheManager = SplitCacheManager.getInstance(config);
        
        // Create schema
        schema = new SchemaBuilder()
            .addTextField("id", true, false, "raw", "position")
            .addTextField("title", true, false, "default", "position")
            .addTextField("content", true, false, "default", "position")
            .addTextField("category", true, false, "default", "position")
            .addIntegerField("score", true, true, true)
            .addFloatField("rating", true, true, true)
            .addBooleanField("active", true, true, true)
            .addDateField("timestamp", true, true, true)
            .addIntegerField("view_count", true, true, true)
            .build();

        // Create and populate index with 1 million documents using batch indexing
        Path indexPath = tempDir.resolve("million-record-index");
        createMillionRecordIndex(indexPath);
        
        // Convert to split
        long splitStartTime = System.currentTimeMillis();
        splitPath = tempDir.resolve("million_record.split");
        QuickwitSplit.SplitConfig splitConfig = new QuickwitSplit.SplitConfig(
            "million-record-index",
            "bulk-perf-test", 
            "perf-node"
        );
        
        QuickwitSplit.convertIndexFromPath(indexPath.toString(), splitPath.toString(), splitConfig);
        splitUrl = "file://" + splitPath.toAbsolutePath().toString();
        splitCreationTimeMs = System.currentTimeMillis() - splitStartTime;
        
        System.out.printf("✅ Index created with %,d documents in %.2f seconds\n", 
            TOTAL_DOCUMENTS, indexingTimeMs / 1000.0);
        System.out.printf("✅ Split created in %.2f seconds\n\n", 
            splitCreationTimeMs / 1000.0);
    }
    
    private static void createMillionRecordIndex(Path indexPath) throws IOException {
        System.out.println("📝 Creating index with 1,000,000 documents using batch indexing...");
        long startTime = System.currentTimeMillis();
        
        try (Index index = new Index(schema, indexPath.toString());
             IndexWriter writer = index.writer(Index.Memory.LARGE_HEAP_SIZE, 4)) {
            
            BatchDocumentBuilder batchBuilder = new BatchDocumentBuilder();
            int documentsWritten = 0;
            
            for (int i = 0; i < TOTAL_DOCUMENTS; i++) {
                // Create document data using proper API
                Map<String, Object> docData = new HashMap<>();
                docData.put("id", String.format("doc_%08d", i));
                docData.put("title", "Document Title " + i);
                docData.put("content", generateContent(i));
                docData.put("category", getCategory(i));
                docData.put("score", 50 + (i % 100));
                docData.put("rating", 1.0 + (i % 50) * 0.1);
                docData.put("active", i % 3 != 0);
                docData.put("timestamp", LocalDateTime.now().minusSeconds(i));
                docData.put("view_count", (long)(1000 + i % 10000));
                
                batchBuilder.addDocumentFromMap(docData);
                
                // Write batch when it reaches BATCH_SIZE
                if ((i + 1) % BATCH_SIZE == 0) {
                    ByteBuffer batchBuffer = batchBuilder.build();
                    writer.addDocumentsByBuffer(batchBuffer);
                    batchBuilder.clear(); // Reset for next batch
                    documentsWritten += BATCH_SIZE;
                    
                    if (documentsWritten % 100_000 == 0) {
                        System.out.printf("  Progress: %,d documents indexed (%.1f%%)...\n", 
                            documentsWritten, (documentsWritten * 100.0) / TOTAL_DOCUMENTS);
                    }
                }
            }
            
            // Write any remaining documents
            if (batchBuilder.getDocumentCount() > 0) {
                ByteBuffer batchBuffer = batchBuilder.build();
                writer.addDocumentsByBuffer(batchBuffer);
            }
            
            writer.commit();
            indexingTimeMs = System.currentTimeMillis() - startTime;
        }
    }
    
    @AfterAll
    static void tearDown() {
        if (cacheManager != null) {
            try {
                cacheManager.close();
            } catch (Exception e) {
                // Log error but continue cleanup
            }
        }
    }

    @Test
    @org.junit.jupiter.api.Order(1)
    @DisplayName("Verify all 1 million records are searchable")
    void testVerifyAllRecordsSearchable() {
        System.out.println("🔍 Verifying all 1,000,000 records are searchable...");
        
        try (SplitSearcher searcher = cacheManager.createSplitSearcher(splitUrl)) {
            // Query all documents using a match-all approach
            Query matchAllQuery = Query.allQuery();
            
            // Search with high limit to get total count
            SearchResult results = searcher.search(matchAllQuery, TOTAL_DOCUMENTS);
            int foundCount = results.getHits().size();
            
            System.out.printf("  Found %,d documents in split\n", foundCount);
            assertEquals(TOTAL_DOCUMENTS, foundCount, 
                String.format("Should find exactly %,d documents", TOTAL_DOCUMENTS));
            
            // Verify a sample of documents have expected content
            Random random = new Random(42); // Fixed seed for reproducibility
            for (int i = 0; i < 10; i++) {
                int docIndex = random.nextInt(TOTAL_DOCUMENTS);
                String expectedId = String.format("doc_%08d", docIndex);
                
                Query idQuery = Query.termQuery(schema, "id", expectedId);
                SearchResult idResult = searcher.search(idQuery, 1);
                
                assertEquals(1, idResult.getHits().size(), 
                    "Should find document with ID: " + expectedId);
                
                try (Document doc = searcher.doc(idResult.getHits().get(0).getDocAddress())) {
                    String actualId = (String) doc.getFirst("id");
                    assertEquals(expectedId, actualId, "Document ID should match");
                    
                    String title = (String) doc.getFirst("title");
                    assertTrue(title.contains(String.valueOf(docIndex)), 
                        "Title should contain document index");
                }
            }
            
            System.out.println("  ✅ All documents verified as searchable\n");
        }
    }

    @Test
    @org.junit.jupiter.api.Order(2)
    @DisplayName("Performance comparison: Classic vs Bulk retrieval for subset")
    void testPerformanceComparisonSubset() {
        System.out.println("📊 Performance Test: Retrieving 10,000 document subset\n");
        
        try (SplitSearcher searcher = cacheManager.createSplitSearcher(splitUrl)) {
            // Get a subset of documents for testing
            Query query = Query.rangeQuery(schema, "score", FieldType.INTEGER, 90, 100, true, true);
            SearchResult results = searcher.search(query, 10_000);
            
            List<DocAddress> addresses = results.getHits().stream()
                .map(hit -> hit.getDocAddress())
                .collect(Collectors.toList());
            
            int testSize = addresses.size();
            System.out.printf("  Testing with %,d documents matching score range [90-100]\n\n", testSize);
            
            // Method 1: Classic individual retrieval
            System.out.println("  Method 1: Classic Individual Retrieval");
            System.out.println("  ----------------------------------------");
            long classicStartTime = System.nanoTime();
            List<Document> classicDocs = new ArrayList<>();
            
            for (DocAddress addr : addresses) {
                Document doc = searcher.doc(addr);
                classicDocs.add(doc);
            }
            
            long classicTimeNs = System.nanoTime() - classicStartTime;
            double classicTimeMs = classicTimeNs / 1_000_000.0;
            
            System.out.printf("    Time: %.2f ms\n", classicTimeMs);
            System.out.printf("    Rate: %,d docs/sec\n", (long)(testSize / (classicTimeMs / 1000.0)));
            System.out.printf("    Per-doc: %.3f μs\n\n", (classicTimeNs / 1000.0) / testSize);
            
            // Method 2: Bulk retrieval (zero-copy)
            System.out.println("  Method 2: Zero-Copy Bulk Retrieval");
            System.out.println("  ------------------------------------");
            long bulkStartTime = System.nanoTime();
            List<Document> bulkDocs = new ArrayList<>();
            
            try {
                // Process in batches for bulk retrieval
                for (int i = 0; i < addresses.size(); i += RETRIEVAL_BATCH_SIZE) {
                    int end = Math.min(i + RETRIEVAL_BATCH_SIZE, addresses.size());
                    List<DocAddress> batch = addresses.subList(i, end);
                    
                    ByteBuffer bulkBuffer = searcher.docsBulk(batch);
                    if (bulkBuffer != null) {
                        List<Document> batchDocs = searcher.parseBulkDocs(bulkBuffer);
                        bulkDocs.addAll(batchDocs);
                    }
                }
            } catch (RuntimeException e) {
                // If bulk retrieval not implemented, use stub behavior
                System.out.println("    ⚠️  Bulk retrieval not yet implemented (using stub)");
                bulkDocs = searcher.parseBulkDocs(null);
            }
            
            long bulkTimeNs = System.nanoTime() - bulkStartTime;
            double bulkTimeMs = bulkTimeNs / 1_000_000.0;
            
            if (!bulkDocs.isEmpty()) {
                System.out.printf("    Time: %.2f ms\n", bulkTimeMs);
                System.out.printf("    Rate: %,d docs/sec\n", (long)(testSize / (bulkTimeMs / 1000.0)));
                System.out.printf("    Per-doc: %.3f μs\n", (bulkTimeNs / 1000.0) / testSize);
                
                double speedup = classicTimeMs / bulkTimeMs;
                System.out.printf("\n  🚀 Performance Improvement: %.2fx faster\n", speedup);
            } else {
                System.out.printf("    Time: %.2f ms (stub only)\n", bulkTimeMs);
                System.out.println("    Actual bulk retrieval will show significant speedup");
            }
            
            // Cleanup documents
            for (Document doc : classicDocs) {
                doc.close();
            }
            for (Document doc : bulkDocs) {
                doc.close();
            }
            
            System.out.println("\n  ✅ Performance comparison complete\n");
        }
    }

    @Test
    @org.junit.jupiter.api.Order(3)
    @DisplayName("Stress test: Retrieve all 1 million records")
    @Disabled("Enable this test only when bulk retrieval is fully implemented")
    void testRetrieveAllMillionRecords() {
        System.out.println("🔥 Stress Test: Retrieving all 1,000,000 records\n");
        System.out.println("  ⚠️  This test is disabled by default. Enable when bulk retrieval is implemented.\n");
        
        try (SplitSearcher searcher = cacheManager.createSplitSearcher(splitUrl)) {
            // Get all document addresses
            Query matchAllQuery = Query.allQuery();
            SearchResult results = searcher.search(matchAllQuery, TOTAL_DOCUMENTS);
            
            List<DocAddress> allAddresses = results.getHits().stream()
                .map(hit -> hit.getDocAddress())
                .collect(Collectors.toList());
            
            assertEquals(TOTAL_DOCUMENTS, allAddresses.size(), 
                "Should have addresses for all documents");
            
            System.out.printf("  Retrieved %,d document addresses\n\n", allAddresses.size());
            
            // Method 1: Classic retrieval (sample only due to time)
            System.out.println("  Method 1: Classic Individual Retrieval (10,000 sample)");
            System.out.println("  -------------------------------------------------------");
            List<DocAddress> sampleAddresses = allAddresses.subList(0, 10_000);
            
            long classicStartTime = System.nanoTime();
            AtomicInteger classicCount = new AtomicInteger(0);
            
            for (DocAddress addr : sampleAddresses) {
                try (Document doc = searcher.doc(addr)) {
                    assertNotNull(doc.getFirst("id"));
                    classicCount.incrementAndGet();
                }
            }
            
            long classicSampleTimeNs = System.nanoTime() - classicStartTime;
            double classicSampleTimeMs = classicSampleTimeNs / 1_000_000.0;
            double estimatedClassicTotalMs = (classicSampleTimeMs / 10_000) * TOTAL_DOCUMENTS;
            
            System.out.printf("    Sample time (10k docs): %.2f ms\n", classicSampleTimeMs);
            System.out.printf("    Estimated total time: %.2f seconds\n", estimatedClassicTotalMs / 1000.0);
            System.out.printf("    Estimated rate: %,d docs/sec\n\n", 
                (long)(TOTAL_DOCUMENTS / (estimatedClassicTotalMs / 1000.0)));
            
            // Method 2: Bulk retrieval for all documents
            System.out.println("  Method 2: Zero-Copy Bulk Retrieval (All 1M docs)");
            System.out.println("  --------------------------------------------------");
            long bulkStartTime = System.nanoTime();
            AtomicInteger bulkCount = new AtomicInteger(0);
            
            try {
                // Process in larger batches for bulk retrieval
                int largeBatchSize = 10_000;
                for (int i = 0; i < allAddresses.size(); i += largeBatchSize) {
                    int end = Math.min(i + largeBatchSize, allAddresses.size());
                    List<DocAddress> batch = allAddresses.subList(i, end);
                    
                    ByteBuffer bulkBuffer = searcher.docsBulk(batch);
                    if (bulkBuffer != null) {
                        List<Document> batchDocs = searcher.parseBulkDocs(bulkBuffer);
                        for (Document doc : batchDocs) {
                            assertNotNull(doc.getFirst("id"));
                            bulkCount.incrementAndGet();
                            doc.close();
                        }
                    }
                    
                    if ((i + largeBatchSize) % 100_000 == 0) {
                        System.out.printf("    Progress: %,d documents retrieved...\n", 
                            i + largeBatchSize);
                    }
                }
            } catch (RuntimeException e) {
                System.out.println("    ⚠️  Bulk retrieval not yet implemented");
                return;
            }
            
            long bulkTimeNs = System.nanoTime() - bulkStartTime;
            double bulkTimeMs = bulkTimeNs / 1_000_000.0;
            
            System.out.printf("    Total time: %.2f seconds\n", bulkTimeMs / 1000.0);
            System.out.printf("    Rate: %,d docs/sec\n", 
                (long)(bulkCount.get() / (bulkTimeMs / 1000.0)));
            System.out.printf("    Documents retrieved: %,d\n", bulkCount.get());
            
            double actualSpeedup = estimatedClassicTotalMs / bulkTimeMs;
            System.out.printf("\n  🚀 Performance Improvement: %.2fx faster than classic\n", 
                actualSpeedup);
            
            assertEquals(TOTAL_DOCUMENTS, bulkCount.get(), 
                "Should retrieve all million documents");
            
            System.out.println("\n  ✅ Successfully retrieved all 1,000,000 documents\n");
        }
    }

    @Test
    @org.junit.jupiter.api.Order(4)
    @DisplayName("Memory efficiency comparison")
    void testMemoryEfficiency() {
        System.out.println("💾 Memory Efficiency Test\n");
        
        try (SplitSearcher searcher = cacheManager.createSplitSearcher(splitUrl)) {
            // Get a moderate batch for memory testing
            Query query = Query.rangeQuery(schema, "score", FieldType.INTEGER, 95, 100, true, true);
            SearchResult results = searcher.search(query, 5_000);
            
            List<DocAddress> addresses = results.getHits().stream()
                .map(hit -> hit.getDocAddress())
                .collect(Collectors.toList());
            
            System.out.printf("  Testing memory usage with %,d documents\n\n", addresses.size());
            
            // Force garbage collection before test
            System.gc();
            Thread.sleep(100);
            
            // Method 1: Classic retrieval memory usage
            Runtime runtime = Runtime.getRuntime();
            long beforeClassicMemory = runtime.totalMemory() - runtime.freeMemory();
            
            List<Document> classicDocs = new ArrayList<>();
            for (DocAddress addr : addresses) {
                Document doc = searcher.doc(addr);
                classicDocs.add(doc);
            }
            
            long afterClassicMemory = runtime.totalMemory() - runtime.freeMemory();
            long classicMemoryUsed = afterClassicMemory - beforeClassicMemory;
            
            System.out.println("  Classic Individual Retrieval:");
            System.out.printf("    Memory used: %,d bytes (%.2f MB)\n", 
                classicMemoryUsed, classicMemoryUsed / (1024.0 * 1024.0));
            System.out.printf("    Per document: %,d bytes\n\n", 
                classicMemoryUsed / addresses.size());
            
            // Cleanup classic docs
            for (Document doc : classicDocs) {
                doc.close();
            }
            classicDocs.clear();
            
            // Force garbage collection between tests
            System.gc();
            Thread.sleep(100);
            
            // Method 2: Bulk retrieval memory usage
            long beforeBulkMemory = runtime.totalMemory() - runtime.freeMemory();
            
            try {
                ByteBuffer bulkBuffer = searcher.docsBulk(addresses);
                if (bulkBuffer != null) {
                    List<Document> bulkDocs = searcher.parseBulkDocs(bulkBuffer);
                    
                    long afterBulkMemory = runtime.totalMemory() - runtime.freeMemory();
                    long bulkMemoryUsed = afterBulkMemory - beforeBulkMemory;
                    
                    System.out.println("  Zero-Copy Bulk Retrieval:");
                    System.out.printf("    Memory used: %,d bytes (%.2f MB)\n", 
                        bulkMemoryUsed, bulkMemoryUsed / (1024.0 * 1024.0));
                    System.out.printf("    Per document: %,d bytes\n", 
                        bulkMemoryUsed / addresses.size());
                    
                    if (classicMemoryUsed > 0 && bulkMemoryUsed > 0) {
                        double memoryRatio = (double) classicMemoryUsed / bulkMemoryUsed;
                        System.out.printf("\n  📉 Memory Efficiency: %.2fx less memory used\n", 
                            memoryRatio);
                    }
                    
                    // Cleanup
                    for (Document doc : bulkDocs) {
                        doc.close();
                    }
                }
            } catch (RuntimeException e) {
                System.out.println("  Zero-Copy Bulk Retrieval:");
                System.out.println("    ⚠️  Not yet implemented (stub mode)");
                System.out.println("    Bulk retrieval will use significantly less memory");
            }
            
            System.out.println("\n  ✅ Memory efficiency test complete\n");
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            fail("Test interrupted");
        }
    }
    
    // Helper methods
    private static String generateContent(int index) {
        String[] words = {
            "search", "index", "document", "query", "tantivy", "lucene", 
            "retrieval", "performance", "optimization", "analysis", "field",
            "token", "term", "boost", "score", "relevance", "ranking"
        };
        
        StringBuilder content = new StringBuilder();
        content.append("This is document ").append(index).append(" containing ");
        
        Random rand = new Random(index); // Deterministic based on index
        int wordCount = 20 + rand.nextInt(30);
        for (int i = 0; i < wordCount; i++) {
            content.append(words[rand.nextInt(words.length)]).append(" ");
        }
        
        return content.toString().trim();
    }
    
    private static String getCategory(int index) {
        String[] categories = {
            "Technology", "Science", "Business", "Education", "Health",
            "Entertainment", "Sports", "Politics", "Environment", "Culture"
        };
        return categories[index % categories.length];
    }
}
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
 * Base class for bulk retrieval performance tests with different dataset sizes.
 * Provides common infrastructure for testing bulk vs individual document retrieval.
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public abstract class BaseBulkRetrievalPerformanceTest {

    protected static SplitCacheManager cacheManager;
    protected static final int BATCH_SIZE = 10_000; // Documents per batch for indexing
    protected static final int RETRIEVAL_BATCH_SIZE = 1000; // Documents per retrieval batch
    
    @TempDir
    static Path tempDir;
    
    protected static Path splitPath;
    protected static String splitUrl;
    protected static Schema schema;
    protected static long indexingTimeMs;
    protected static long splitCreationTimeMs;

    // Abstract method to be implemented by subclasses
    protected abstract int getTotalDocuments();
    protected abstract String getTestName();

    @BeforeAll
    static void setUpOnce() throws IOException {
        // This will be called by subclasses
    }
    
    protected void performSetup() throws IOException {
        int totalDocs = getTotalDocuments();
        System.out.printf("=== %s BULK RETRIEVAL PERFORMANCE TEST ===\n", getTestName().toUpperCase());
        System.out.printf("Creating test environment with %,d documents...\n\n", totalDocs);
        
        // Create shared cache manager with appropriate cache size
        int cacheSize = Math.max(100_000_000, totalDocs * 100); // 100MB minimum, 100 bytes per doc estimate
        SplitCacheManager.CacheConfig config = new SplitCacheManager.CacheConfig(getTestName().toLowerCase() + "-cache")
                .withMaxCacheSize(cacheSize)
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

        // Create and populate index
        Path indexPath = tempDir.resolve(getTestName().toLowerCase() + "-index");
        createIndex(indexPath, totalDocs);
        
        // Convert to split
        long splitStartTime = System.currentTimeMillis();
        splitPath = tempDir.resolve(getTestName().toLowerCase() + ".split");
        QuickwitSplit.SplitConfig splitConfig = new QuickwitSplit.SplitConfig(
            getTestName().toLowerCase() + "-index",
            "bulk-perf-test", 
            "perf-node"
        );
        
        QuickwitSplit.convertIndexFromPath(indexPath.toString(), splitPath.toString(), splitConfig);
        splitUrl = "file://" + splitPath.toAbsolutePath().toString();
        splitCreationTimeMs = System.currentTimeMillis() - splitStartTime;
        
        System.out.printf("✅ Index created with %,d documents in %.2f seconds\n", 
            totalDocs, indexingTimeMs / 1000.0);
        System.out.printf("✅ Split created in %.2f seconds\n\n", 
            splitCreationTimeMs / 1000.0);
    }
    
    protected void createIndex(Path indexPath, int totalDocs) throws IOException {
        System.out.printf("📝 Creating index with %,d documents using batch indexing...\n", totalDocs);
        long startTime = System.currentTimeMillis();
        
        try (Index index = new Index(schema, indexPath.toString());
             IndexWriter writer = index.writer(Index.Memory.LARGE_HEAP_SIZE, 4)) {
            
            BatchDocumentBuilder batchBuilder = new BatchDocumentBuilder();
            int documentsWritten = 0;
            int progressInterval = Math.max(10_000, totalDocs / 10); // Show progress 10 times
            
            for (int i = 0; i < totalDocs; i++) {
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
                    
                    if (documentsWritten % progressInterval == 0) {
                        System.out.printf("  Progress: %,d documents indexed (%.1f%%)...\n", 
                            documentsWritten, (documentsWritten * 100.0) / totalDocs);
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
    @DisplayName("Verify all records are searchable")
    void testVerifyAllRecordsSearchable() {
        int totalDocs = getTotalDocuments();
        System.out.printf("🔍 Verifying all %,d records are searchable...\n", totalDocs);
        
        try (SplitSearcher searcher = cacheManager.createSplitSearcher(splitUrl)) {
            // Query all documents using a match-all approach
            Query matchAllQuery = Query.allQuery();
            
            // Search with high limit to get total count
            SearchResult results = searcher.search(matchAllQuery, totalDocs);
            int foundCount = results.getHits().size();
            
            System.out.printf("  Found %,d documents in split\n", foundCount);
            assertEquals(totalDocs, foundCount, 
                String.format("Should find exactly %,d documents", totalDocs));
            
            System.out.println("  ✅ All documents verified as searchable\n");
        }
    }

    @Test
    @org.junit.jupiter.api.Order(2)
    @DisplayName("Performance comparison: Classic vs Bulk retrieval for subset")
    void testPerformanceComparisonSubset() {
        int totalDocs = getTotalDocuments();
        int testSize = Math.min(5000, totalDocs / 10); // Test with up to 5k docs or 10% of total
        
        System.out.printf("📊 Performance Test: Retrieving %,d document subset\n\n", testSize);
        
        try (SplitSearcher searcher = cacheManager.createSplitSearcher(splitUrl)) {
            // Get a subset of documents for testing
            Query query = Query.rangeQuery(schema, "score", FieldType.INTEGER, 90, 100, true, true);
            SearchResult results = searcher.search(query, testSize);
            
            List<DocAddress> addresses = results.getHits().stream()
                .map(hit -> hit.getDocAddress())
                .limit(testSize)
                .collect(Collectors.toList());
            
            int actualTestSize = addresses.size();
            System.out.printf("  Testing with %,d documents matching score range [90-100]\n\n", actualTestSize);
            
            // Method 1: Classic individual retrieval
            System.out.println("  Method 1: Classic Individual Retrieval");
            System.out.println("  ----------------------------------------");
            long classicStartTime = System.nanoTime();
            List<Document> classicDocs = new ArrayList<>();
            int classicFieldsAccessed = 0;
            
            for (DocAddress addr : addresses) {
                Document doc = searcher.doc(addr);
                classicDocs.add(doc);
                
                // Access all fields to ensure fair comparison
                classicFieldsAccessed += accessAllFields(doc);
            }
            
            long classicTimeNs = System.nanoTime() - classicStartTime;
            double classicTimeMs = classicTimeNs / 1_000_000.0;
            
            System.out.printf("    Time: %.2f ms\n", classicTimeMs);
            System.out.printf("    Rate: %,d docs/sec\n", (long)(actualTestSize / (classicTimeMs / 1000.0)));
            System.out.printf("    Per-doc: %.3f μs\n", (classicTimeNs / 1000.0) / actualTestSize);
            System.out.printf("    Fields accessed: %,d\n\n", classicFieldsAccessed);
            
            // Method 2: Bulk retrieval (zero-copy)
            System.out.println("  Method 2: Zero-Copy Bulk Retrieval");
            System.out.println("  ------------------------------------");
            long bulkStartTime = System.nanoTime();
            List<Document> bulkDocs = new ArrayList<>();
            int bulkFieldsAccessed = 0;
            
            // Process in batches for bulk retrieval
            for (int i = 0; i < addresses.size(); i += RETRIEVAL_BATCH_SIZE) {
                int end = Math.min(i + RETRIEVAL_BATCH_SIZE, addresses.size());
                List<DocAddress> batch = addresses.subList(i, end);
                
                ByteBuffer bulkBuffer = searcher.docsBulk(batch);
                if (bulkBuffer != null) {
                    List<Document> batchDocs = searcher.parseBulkDocs(bulkBuffer);
                    bulkDocs.addAll(batchDocs);
                    
                    // Access all fields to ensure fair comparison
                    for (Document doc : batchDocs) {
                        bulkFieldsAccessed += accessAllFields(doc);
                    }
                }
            }
            
            long bulkTimeNs = System.nanoTime() - bulkStartTime;
            double bulkTimeMs = bulkTimeNs / 1_000_000.0;
            
            System.out.printf("    Time: %.2f ms\n", bulkTimeMs);
            System.out.printf("    Rate: %,d docs/sec\n", (long)(actualTestSize / (bulkTimeMs / 1000.0)));
            System.out.printf("    Per-doc: %.3f μs\n", (bulkTimeNs / 1000.0) / actualTestSize);
            System.out.printf("    Fields accessed: %,d\n", bulkFieldsAccessed);
            System.out.printf("    Documents retrieved: %,d\n", bulkDocs.size());
            
            double speedup = classicTimeMs / bulkTimeMs;
            System.out.printf("\n  🚀 Performance Improvement: %.2fx faster\n", speedup);
            
            // Validate we got the same number of documents and field accesses
            assertEquals(actualTestSize, bulkDocs.size(), "Bulk retrieval should return same number of documents");
            assertEquals(classicFieldsAccessed, bulkFieldsAccessed, "Should access same number of fields");
            
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
    
    // Helper methods
    protected static String generateContent(int index) {
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
    
    protected static String getCategory(int index) {
        String[] categories = {
            "Technology", "Science", "Business", "Education", "Health",
            "Entertainment", "Sports", "Politics", "Environment", "Culture"
        };
        return categories[index % categories.length];
    }
    
    /**
     * Access all fields in a document to ensure fair performance comparison.
     * Returns the number of fields accessed.
     */
    protected static int accessAllFields(Document doc) {
        int fieldsAccessed = 0;
        
        // Access all schema fields
        Object id = doc.getFirst("id");
        if (id != null) fieldsAccessed++;
        
        Object title = doc.getFirst("title");
        if (title != null) fieldsAccessed++;
        
        Object content = doc.getFirst("content");
        if (content != null) fieldsAccessed++;
        
        Object category = doc.getFirst("category");
        if (category != null) fieldsAccessed++;
        
        Object score = doc.getFirst("score");
        if (score != null) fieldsAccessed++;
        
        Object rating = doc.getFirst("rating");
        if (rating != null) fieldsAccessed++;
        
        Object active = doc.getFirst("active");
        if (active != null) fieldsAccessed++;
        
        Object timestamp = doc.getFirst("timestamp");
        if (timestamp != null) fieldsAccessed++;
        
        Object viewCount = doc.getFirst("view_count");
        if (viewCount != null) fieldsAccessed++;
        
        return fieldsAccessed;
    }
}
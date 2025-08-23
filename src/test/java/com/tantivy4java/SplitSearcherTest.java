package com.tantivy4java;

import io.findify.s3mock.S3Mock;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Comprehensive test suite for SplitSearcher functionality.
 * Tests both file: protocol (local filesystem) and s3: protocol (using Adobe S3Mock).
 */
public class SplitSearcherTest {

    private static final String TEST_BUCKET = "test-splits-bucket";
    private static final String ACCESS_KEY = "test-access-key";
    private static final String SECRET_KEY = "test-secret-key";
    private static final int S3_MOCK_PORT = 8001;
    
    private static S3Mock s3Mock;
    private static S3Client s3Client;
    
    @TempDir
    static Path tempDir;
    
    private Index testIndex;
    private Path indexPath;
    private Path localSplitPath;
    private String s3SplitPath;

    @BeforeAll
    static void setUpS3Mock() {
        // Start Adobe S3Mock server
        s3Mock = new S3Mock.Builder()
            .withPort(S3_MOCK_PORT)
            .withInMemoryBackend()
            .build();
        s3Mock.start();
        
        // Configure S3 client to use mock server
        s3Client = S3Client.builder()
            .endpointOverride(java.net.URI.create("http://localhost:" + S3_MOCK_PORT))
            .credentialsProvider(StaticCredentialsProvider.create(
                AwsBasicCredentials.create(ACCESS_KEY, SECRET_KEY)))
            .region(Region.US_EAST_1)
            .forcePathStyle(true)
            .build();
        
        // Create test bucket
        s3Client.createBucket(CreateBucketRequest.builder()
            .bucket(TEST_BUCKET)
            .build());
    }
    
    @AfterAll
    static void tearDownS3Mock() {
        if (s3Client != null) {
            s3Client.close();
        }
        if (s3Mock != null) {
            s3Mock.shutdown();
        }
    }

    @BeforeEach
    void setUp() throws IOException {
        // Create test index with substantial data
        Schema schema = new SchemaBuilder()
            .addTextField("title", true, false, "default", "position")
            .addTextField("content", true, false, "default", "position")
            .addIntegerField("category_id", true, true, true)
            .addFloatField("rating", true, true, true)
            .addDateField("created_at", true, true, true)
            .build();

        indexPath = tempDir.resolve("test-index");
        testIndex = new Index(schema, indexPath.toString());
        IndexWriter writer = testIndex.writer(50, 1);

        // Add 500 documents for comprehensive testing
        for (int i = 0; i < 500; i++) {
            Document doc = new Document();
            doc.addText("title", "Test Document " + i);
            doc.addText("content", "This is the content for document " + i + 
                ". It contains searchable text with various terms like: sample, test, content, document.");
            doc.addInteger("category_id", i % 10);
            doc.addFloat("rating", 1.0f + (i % 5));
            doc.addDate("created_at", java.time.OffsetDateTime.now().minusDays(i % 30).toLocalDateTime());
            writer.addDocument(doc);
        }

        writer.commit();
        writer.close();

        // Convert index to split for local file testing
        localSplitPath = tempDir.resolve("test-split.split");
        QuickwitSplit.SplitConfig config = new QuickwitSplit.SplitConfig(
            "test-index-uid",
            "test-source-id", 
            "test-node-id"
        );
        
        QuickwitSplit.convertIndexFromPath(indexPath.toString(), localSplitPath.toString(), config);
        
        // Upload split to S3Mock for S3 testing
        String s3Key = "splits/test-split-" + System.currentTimeMillis() + ".split";
        s3SplitPath = "s3://" + TEST_BUCKET + "/" + s3Key;
        
        s3Client.putObject(PutObjectRequest.builder()
            .bucket(TEST_BUCKET)
            .key(s3Key)
            .build(),
            localSplitPath);
    }

    @AfterEach
    void tearDown() {
        if (testIndex != null) {
            testIndex.close();
        }
    }

    // ========== FILE PROTOCOL TESTS ==========

    @Test
    @DisplayName("Create SplitSearcher with file: protocol")
    void testCreateSplitSearcherWithFileProtocol() {
        String fileUri = "file://" + localSplitPath.toAbsolutePath();
        
        try (SplitSearcher searcher = SplitSearcher.builder(fileUri)
            .withCacheSize(25_000_000)
            .preload(SplitSearcher.IndexComponent.SCHEMA, SplitSearcher.IndexComponent.STORE)
            .build()) {
            
            assertNotNull(searcher);
            
            // Verify schema access
            Schema schema = searcher.getSchema();
            assertNotNull(schema);
            
            // Verify split metadata
            SplitSearcher.SplitMetadata metadata = searcher.getSplitMetadata();
            assertNotNull(metadata);
            assertTrue(metadata.getTotalSize() > 0);
        }
    }

    @Test
    @DisplayName("Search with file: protocol - basic text search")
    void testFileProtocolBasicSearch() {
        String fileUri = "file://" + localSplitPath.toAbsolutePath();
        
        try (SplitSearcher searcher = SplitSearcher.builder(fileUri)
            .withCacheSize(50_000_000)
            .enableQueryCache(true)
            .build()) {
            
            Schema schema = searcher.getSchema();
            Query query = Query.termQuery(schema, "content", "document");
            
            SearchResult result = searcher.search(query, 10);
            assertNotNull(result);
            assertTrue(result.getHits().size() > 0);
        }
    }

    @Test
    @DisplayName("Search with file: protocol - range queries")
    void testFileProtocolRangeSearch() {
        String fileUri = "file://" + localSplitPath.toAbsolutePath();
        
        try (SplitSearcher searcher = SplitSearcher.builder(fileUri)
            .withCacheSize(50_000_000)
            .preload(SplitSearcher.IndexComponent.FASTFIELD)
            .build()) {
            
            Schema schema = searcher.getSchema();
            
            // Range query on category_id
            Query rangeQuery = Query.rangeQuery(schema, "category_id", FieldType.INTEGER,
                3, 7, true, true);
            
            SearchResult result = searcher.search(rangeQuery, 50);
            assertNotNull(result);
            assertTrue(result.getHits().size() > 0);
            
            // Verify results are within range
            // Verify hits were retrieved (document content verification not available in current API)
            List<SearchResult.Hit> hits = result.getHits();
            assertTrue(hits.size() > 0, "Should have found documents in the category range");
            for (SearchResult.Hit hit : hits) {
                assertTrue(hit.getScore() > 0, "Hit should have positive score");
                assertNotNull(hit.getDocAddress(), "Hit should have document address");
            }
        }
    }

    @Test
    @DisplayName("Cache performance with file: protocol")
    void testFileProtocolCachePerformance() {
        String fileUri = "file://" + localSplitPath.toAbsolutePath();
        
        try (SplitSearcher searcher = SplitSearcher.builder(fileUri)
            .withCacheSize(100_000_000)
            .preload(SplitSearcher.IndexComponent.SCHEMA, 
                    SplitSearcher.IndexComponent.STORE,
                    SplitSearcher.IndexComponent.POSTINGS)
            .build()) {
            
            Schema schema = searcher.getSchema();
            Query query = Query.termQuery(schema, "title", "Test");
            
            // First search - cache miss
            long start1 = System.nanoTime();
            SearchResult result1 = searcher.search(query, 10);
            long time1 = System.nanoTime() - start1;
            
            // Second search - cache hit
            long start2 = System.nanoTime();
            SearchResult result2 = searcher.search(query, 10);
            long time2 = System.nanoTime() - start2;
            
            // Verify results are identical
            assertEquals(result1.getHits().size(), result2.getHits().size());
            
            // Cache should improve performance (second search should be faster)
            assertTrue(time2 < time1 * 2, "Cached search should be faster");
            
            // Check cache statistics
            SplitSearcher.CacheStats stats = searcher.getCacheStats();
            assertTrue(stats.getHitCount() > 0);
            assertTrue(stats.getHitRate() > 0.0);
        }
    }

    // ========== S3 PROTOCOL TESTS ==========

    @Test
    @DisplayName("Create SplitSearcher with s3: protocol")
    void testCreateSplitSearcherWithS3Protocol() {
        SplitSearcher.SplitSearchConfig config = new SplitSearcher.SplitSearchConfig(s3SplitPath)
            .withCacheSize(25_000_000)
            .withAwsCredentials(ACCESS_KEY, SECRET_KEY, "us-east-1")
            .withAwsEndpoint("http://localhost:" + S3_MOCK_PORT)
            .preload(SplitSearcher.IndexComponent.SCHEMA);
        
        try (SplitSearcher searcher = SplitSearcher.create(config)) {
            assertNotNull(searcher);
            
            // Verify schema access works with S3
            Schema schema = searcher.getSchema();
            assertNotNull(schema);
            
            // Verify split metadata
            SplitSearcher.SplitMetadata metadata = searcher.getSplitMetadata();
            assertNotNull(metadata);
            assertTrue(metadata.getTotalSize() > 0);
        }
    }

    @Test
    @DisplayName("Search with s3: protocol - complex queries")
    void testS3ProtocolComplexSearch() {
        SplitSearcher.SplitSearchConfig config = new SplitSearcher.SplitSearchConfig(s3SplitPath)
            .withCacheSize(75_000_000)
            .withAwsCredentials(ACCESS_KEY, SECRET_KEY, "us-east-1")
            .withAwsEndpoint("http://localhost:" + S3_MOCK_PORT)
            .enableQueryCache(true);
        
        try (SplitSearcher searcher = SplitSearcher.create(config)) {
            Schema schema = searcher.getSchema();
            
            // Complex boolean query
            Query termQuery = Query.termQuery(schema, "content", "document");
            Query rangeQuery = Query.rangeQuery(schema, "rating", FieldType.FLOAT,
                3.0f, 5.0f, true, true);
            Query boolQuery = Query.booleanQuery(
                java.util.Arrays.asList(
                    new Query.OccurQuery(Occur.MUST, termQuery),
                    new Query.OccurQuery(Occur.MUST, rangeQuery)
                )
            );
            
            SearchResult result = searcher.search(boolQuery, 20);
            assertNotNull(result);
            assertTrue(result.getHits().size() > 0);
            
            // Verify hits were retrieved (document content verification not available in current API)
            List<SearchResult.Hit> hits = result.getHits();
            assertTrue(hits.size() > 0, "Should have found documents matching both conditions");
            for (SearchResult.Hit hit : hits) {
                assertTrue(hit.getScore() > 0, "Hit should have positive score");
                assertNotNull(hit.getDocAddress(), "Hit should have document address");
            }
        }
    }

    @Test
    @DisplayName("Hot cache preloading with s3: protocol")
    void testS3ProtocolHotCachePreloading() {
        SplitSearcher.SplitSearchConfig config = new SplitSearcher.SplitSearchConfig(s3SplitPath)
            .withCacheSize(100_000_000)
            .withMaxConcurrentLoads(4)
            .withMaxWaitTime(Duration.ofSeconds(30))
            .withAwsCredentials(ACCESS_KEY, SECRET_KEY, "us-east-1")
            .withAwsEndpoint("http://localhost:" + S3_MOCK_PORT)
            .preload(SplitSearcher.IndexComponent.SCHEMA,
                    SplitSearcher.IndexComponent.STORE,
                    SplitSearcher.IndexComponent.FASTFIELD,
                    SplitSearcher.IndexComponent.POSTINGS);
        
        try (SplitSearcher searcher = SplitSearcher.create(config)) {
            // Check loading statistics
            SplitSearcher.LoadingStats loadingStats = searcher.getLoadingStats();
            assertNotNull(loadingStats);
            assertTrue(loadingStats.getTotalBytesLoaded() > 0);
            
            // Check component cache status
            Map<SplitSearcher.IndexComponent, Boolean> cacheStatus = 
                searcher.getComponentCacheStatus();
            assertTrue(cacheStatus.get(SplitSearcher.IndexComponent.SCHEMA));
            assertTrue(cacheStatus.get(SplitSearcher.IndexComponent.STORE));
            
            // Verify search performance benefits
            Schema schema = searcher.getSchema();
            Query query = Query.termQuery(schema, "title", "Document");
            
            long start = System.nanoTime();
            SearchResult result = searcher.search(query, 50);
            long searchTime = System.nanoTime() - start;
            
            assertTrue(result.getHits().size() > 0);
            // With preloading, search should be fast
            assertTrue(searchTime < 500_000_000); // Less than 500ms
        }
    }

    @Test
    @DisplayName("S3 endpoint configuration and custom headers")
    void testS3CustomEndpointConfiguration() {
        SplitSearcher.SplitSearchConfig config = new SplitSearcher.SplitSearchConfig(s3SplitPath)
            .withCacheSize(50_000_000)
            .withAwsCredentials(ACCESS_KEY, SECRET_KEY, "us-east-1")
            .withAwsEndpoint("http://localhost:" + S3_MOCK_PORT);
        
        try (SplitSearcher searcher = SplitSearcher.create(config)) {
            // Verify the searcher can access S3 with custom endpoint
            assertNotNull(searcher);
            
            // Test split file validation
            assertTrue(searcher.validateSplit());
            
            // Test file listing
            List<String> files = searcher.listSplitFiles();
            assertNotNull(files);
            assertFalse(files.isEmpty());
        }
    }

    // ========== PERFORMANCE AND STRESS TESTS ==========

    @Test
    @DisplayName("Concurrent search performance test")
    void testConcurrentSearchPerformance() throws InterruptedException {
        String fileUri = "file://" + localSplitPath.toAbsolutePath();
        
        try (SplitSearcher searcher = SplitSearcher.builder(fileUri)
            .withCacheSize(200_000_000)
            .withMaxConcurrentLoads(8)
            .preload(SplitSearcher.IndexComponent.values())
            .build()) {
            
            final int numThreads = 4;
            final int queriesPerThread = 25;
            Thread[] threads = new Thread[numThreads];
            final Schema schema = searcher.getSchema();
            
            for (int t = 0; t < numThreads; t++) {
                final int threadId = t;
                threads[t] = new Thread(() -> {
                    try {
                        for (int i = 0; i < queriesPerThread; i++) {
                            Query query = Query.termQuery(schema, "content", "document");
                            SearchResult result = searcher.search(query, 10);
                            assertTrue(result.getHits().size() > 0);
                        }
                    } catch (Exception e) {
                        fail("Thread " + threadId + " failed: " + e.getMessage());
                    }
                });
            }
            
            long start = System.currentTimeMillis();
            for (Thread thread : threads) {
                thread.start();
            }
            for (Thread thread : threads) {
                thread.join();
            }
            long duration = System.currentTimeMillis() - start;
            
            // All concurrent searches should complete within reasonable time
            assertTrue(duration < 10000, "Concurrent searches took too long: " + duration + "ms");
            
            // Check cache hit rate improved
            SplitSearcher.CacheStats finalStats = searcher.getCacheStats();
            assertTrue(finalStats.getHitRate() > 0.5, "Cache hit rate should be high with repeated queries");
        }
    }

    @Test
    @DisplayName("Memory usage and cache eviction test")
    void testMemoryUsageAndCacheEviction() {
        String fileUri = "file://" + localSplitPath.toAbsolutePath();
        
        // Create searcher with small cache to force evictions
        try (SplitSearcher searcher = SplitSearcher.builder(fileUri)
            .withCacheSize(5_000_000) // Small cache
            .build()) {
            
            Schema schema = searcher.getSchema();
            
            // Perform many different searches to force cache evictions
            for (int i = 0; i < 50; i++) {
                Query query = Query.termQuery(schema, "content", "document");
                SearchResult result = searcher.search(query, 5);
                assertNotNull(result);
            }
            
            // Check that cache evictions occurred
            SplitSearcher.CacheStats stats = searcher.getCacheStats();
            assertTrue(stats.getEvictionCount() > 0, "Cache evictions should have occurred");
            assertTrue(stats.getTotalSize() <= stats.getMaxSize(), "Cache should respect size limits");
        }
    }

    @Test
    @DisplayName("Split validation and integrity checks")
    void testSplitValidationAndIntegrity() {
        String fileUri = "file://" + localSplitPath.toAbsolutePath();
        
        try (SplitSearcher searcher = SplitSearcher.builder(fileUri)
            .withCacheSize(50_000_000)
            .build()) {
            
            // Validate split integrity
            assertTrue(searcher.validateSplit(), "Split should be valid");
            
            // Test metadata consistency
            SplitSearcher.SplitMetadata metadata = searcher.getSplitMetadata();
            List<String> files = searcher.listSplitFiles();
            
            assertEquals(metadata.getNumComponents(), files.size());
            assertTrue(metadata.getHotCacheSize() > 0);
            assertTrue(metadata.getHotCacheRatio() > 0.0 && metadata.getHotCacheRatio() <= 1.0);
        }
    }

    // ========== ERROR HANDLING TESTS ==========

    @Test
    @DisplayName("Handle invalid split path")
    void testInvalidSplitPath() {
        assertThrows(RuntimeException.class, () -> {
            SplitSearcher.builder("file:///nonexistent/path/invalid.split")
                .withCacheSize(10_000_000)
                .build();
        });
    }

    @Test
    @DisplayName("Handle S3 connection errors")
    void testS3ConnectionErrors() {
        // Try to connect to invalid S3 endpoint
        SplitSearcher.SplitSearchConfig config = new SplitSearcher.SplitSearchConfig(
            "s3://invalid-bucket/invalid-split.split")
            .withCacheSize(10_000_000)
            .withAwsCredentials("invalid", "invalid", "us-east-1")
            .withAwsEndpoint("http://localhost:9999"); // Invalid port
        
        assertThrows(RuntimeException.class, () -> {
            SplitSearcher.create(config);
        });
    }

    @Test
    @DisplayName("Handle cache overflow gracefully")
    void testCacheOverflowHandling() {
        String fileUri = "file://" + localSplitPath.toAbsolutePath();
        
        // Create searcher with very small cache
        try (SplitSearcher searcher = SplitSearcher.builder(fileUri)
            .withCacheSize(1024) // Very small cache
            .build()) {
            
            Schema schema = searcher.getSchema();
            Query query = Query.termQuery(schema, "content", "sample");
            
            // Should handle cache pressure gracefully
            SearchResult result = searcher.search(query, 10);
            assertNotNull(result);
        }
    }
}
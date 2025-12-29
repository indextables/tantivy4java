/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.indextables.tantivy4java;

import io.indextables.tantivy4java.core.*;
import io.indextables.tantivy4java.result.*;
import io.indextables.tantivy4java.split.*;
import io.indextables.tantivy4java.split.merge.QuickwitSplit;
import io.indextables.tantivy4java.aggregation.AggregationResult;
import io.indextables.tantivy4java.aggregation.StatsAggregation;
import io.indextables.tantivy4java.aggregation.StatsResult;

import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Comprehensive test for validating prewarm cache behavior.
 *
 * This test verifies that:
 * 1. First prewarm downloads data from storage
 * 2. Second prewarm uses cached data (zero downloads)
 * 3. Queries use cached data (zero downloads)
 *
 * Tests local storage, and optionally S3 and Azure if credentials are available.
 *
 * Run with: TANTIVY4JAVA_DEBUG=1 for detailed cache behavior
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class PrewarmCacheValidationTest {

    @TempDir
    Path tempDir;

    private static final int NUM_DOCUMENTS = 100;

    // AWS credentials
    private static String awsAccessKey;
    private static String awsSecretKey;
    private static final String AWS_BUCKET = System.getProperty("test.s3.bucket", "tantivy4java-testing");
    private static final String AWS_REGION = System.getProperty("test.s3.region", "us-east-2");

    // Azure credentials
    private static String azureStorageAccount;
    private static String azureAccountKey;
    private static final String AZURE_CONTAINER = System.getProperty("test.azure.container", "tantivy4java-testing");

    @BeforeAll
    static void loadCredentials() {
        loadAwsCredentials();
        loadAzureCredentials();
    }

    // ========================================
    // LOCAL STORAGE TEST
    // ========================================

    @Test
    @Order(1)
    @DisplayName("Local: Prewarm->Prewarm->Query with zero downloads validation")
    void testLocalPrewarmCacheValidation() throws Exception {
        System.out.println("\n" + "=".repeat(80));
        System.out.println("LOCAL STORAGE PREWARM CACHE VALIDATION TEST");
        System.out.println("=".repeat(80));

        // Create test split
        Path indexPath = tempDir.resolve("local-prewarm-test-index");
        Path splitPath = tempDir.resolve("local-prewarm-test.split");
        Path diskCachePath = tempDir.resolve("local-disk-cache");

        QuickwitSplit.SplitMetadata splitMetadata = createTestSplit(indexPath, splitPath);
        String splitUri = "file://" + splitPath.toAbsolutePath();

        // Create cache manager with disk cache
        SplitCacheManager.TieredCacheConfig tieredConfig = new SplitCacheManager.TieredCacheConfig()
            .withDiskCachePath(diskCachePath.toString())
            .withMaxDiskSize(100_000_000);

        SplitCacheManager.CacheConfig cacheConfig = new SplitCacheManager.CacheConfig("local-prewarm-validation-" + System.currentTimeMillis())
            .withMaxCacheSize(50_000_000)
            .withTieredCache(tieredConfig);

        try (SplitCacheManager cacheManager = SplitCacheManager.getInstance(cacheConfig)) {
            runPrewarmValidation(cacheManager, splitUri, splitMetadata, "LOCAL", diskCachePath);
        }
    }

    // ========================================
    // S3 STORAGE TEST
    // ========================================

    @Test
    @Order(2)
    @DisplayName("S3: Prewarm->Prewarm->Query with zero downloads validation")
    void testS3PrewarmCacheValidation() throws Exception {
        String accessKey = getAwsAccessKey();
        String secretKey = getAwsSecretKey();

        if (accessKey == null || secretKey == null) {
            System.out.println("Skipping S3 test - no AWS credentials available");
            Assumptions.abort("AWS credentials not available");
        }

        System.out.println("\n" + "=".repeat(80));
        System.out.println("S3 STORAGE PREWARM CACHE VALIDATION TEST");
        System.out.println("=".repeat(80));

        // Create local split first
        Path indexPath = tempDir.resolve("s3-prewarm-test-index");
        Path localSplitPath = tempDir.resolve("s3-prewarm-test.split");
        Path diskCachePath = tempDir.resolve("s3-disk-cache");

        QuickwitSplit.SplitMetadata splitMetadata = createTestSplit(indexPath, localSplitPath);

        // Upload to S3 using AWS SDK
        String splitKey = "prewarm-validation-test/test-" + System.currentTimeMillis() + ".split";
        String splitUri = "s3://" + AWS_BUCKET + "/" + splitKey;

        uploadToS3(localSplitPath, splitKey, accessKey, secretKey);

        try {
            // Create cache manager with disk cache
            SplitCacheManager.TieredCacheConfig tieredConfig = new SplitCacheManager.TieredCacheConfig()
                .withDiskCachePath(diskCachePath.toString())
                .withMaxDiskSize(100_000_000);

            SplitCacheManager.CacheConfig cacheConfig = new SplitCacheManager.CacheConfig("s3-prewarm-validation-" + System.currentTimeMillis())
                .withMaxCacheSize(50_000_000)
                .withAwsCredentials(accessKey, secretKey)
                .withAwsRegion(AWS_REGION)
                .withTieredCache(tieredConfig);

            try (SplitCacheManager cacheManager = SplitCacheManager.getInstance(cacheConfig)) {
                runPrewarmValidation(cacheManager, splitUri, splitMetadata, "S3", diskCachePath);
            }
        } finally {
            // Cleanup S3
            deleteFromS3(splitKey, accessKey, secretKey);
        }
    }

    // ========================================
    // AZURE STORAGE TEST
    // ========================================

    @Test
    @Order(3)
    @DisplayName("Azure: Prewarm->Prewarm->Query with zero downloads validation")
    void testAzurePrewarmCacheValidation() throws Exception {
        String storageAccount = getAzureStorageAccount();
        String accountKey = getAzureAccountKey();

        if (storageAccount == null || accountKey == null) {
            System.out.println("Skipping Azure test - no Azure credentials available");
            Assumptions.abort("Azure credentials not available");
        }

        System.out.println("\n" + "=".repeat(80));
        System.out.println("AZURE STORAGE PREWARM CACHE VALIDATION TEST");
        System.out.println("=".repeat(80));

        // Create local split first
        Path indexPath = tempDir.resolve("azure-prewarm-test-index");
        Path localSplitPath = tempDir.resolve("azure-prewarm-test.split");
        Path diskCachePath = tempDir.resolve("azure-disk-cache");

        QuickwitSplit.SplitMetadata splitMetadata = createTestSplit(indexPath, localSplitPath);

        // Upload to Azure
        String blobName = "prewarm-validation-test/test-" + System.currentTimeMillis() + ".split";
        String splitUri = "azure://" + AZURE_CONTAINER + "/" + blobName;

        uploadToAzure(localSplitPath, blobName, storageAccount, accountKey);

        try {
            // Create cache manager with disk cache
            SplitCacheManager.TieredCacheConfig tieredConfig = new SplitCacheManager.TieredCacheConfig()
                .withDiskCachePath(diskCachePath.toString())
                .withMaxDiskSize(100_000_000);

            SplitCacheManager.CacheConfig cacheConfig = new SplitCacheManager.CacheConfig("azure-prewarm-validation-" + System.currentTimeMillis())
                .withMaxCacheSize(50_000_000)
                .withAzureCredentials(storageAccount, accountKey)
                .withTieredCache(tieredConfig);

            try (SplitCacheManager cacheManager = SplitCacheManager.getInstance(cacheConfig)) {
                runPrewarmValidation(cacheManager, splitUri, splitMetadata, "AZURE", diskCachePath);
            }
        } finally {
            // Cleanup Azure
            deleteFromAzure(blobName, storageAccount, accountKey);
        }
    }

    // ========================================
    // CORE VALIDATION LOGIC
    // ========================================

    private void runPrewarmValidation(SplitCacheManager cacheManager, String splitUri,
                                       QuickwitSplit.SplitMetadata splitMetadata, String storageType,
                                       Path diskCachePath) throws Exception {
        System.out.println("\n--- Testing: " + storageType + " ---");
        System.out.println("Split URI: " + splitUri);

        // First, reset metrics to clear any downloads from previous tests
        SplitCacheManager.resetStorageDownloadMetrics();

        try (SplitSearcher searcher = cacheManager.createSplitSearcher(splitUri, splitMetadata)) {
            // Track downloads from SplitSearcher creation
            var afterSearcherCreate = SplitCacheManager.getStorageDownloadMetrics();
            System.out.println("   Downloads after SplitSearcher creation: " + afterSearcherCreate);

            // STEP 1: Reset metrics for prewarm test
            System.out.println("\n[STEP 1] Resetting download metrics...");
            SplitCacheManager.resetStorageDownloadMetrics();
            var beforePrewarm = SplitCacheManager.getStorageDownloadMetrics();
            assertEquals(0, beforePrewarm.getTotalDownloads(), "Metrics should start at zero");
            System.out.println("   Initial metrics: " + beforePrewarm);

            // STEP 2: First prewarm - should download data
            System.out.println("\n[STEP 2] First prewarm (should download data)...");
            long startTime = System.nanoTime();
            // Preload all components (no ALL constant - use individual components)
            searcher.preloadComponents(
                SplitSearcher.IndexComponent.TERM,
                SplitSearcher.IndexComponent.POSTINGS,
                SplitSearcher.IndexComponent.POSITIONS,
                SplitSearcher.IndexComponent.FASTFIELD,
                SplitSearcher.IndexComponent.FIELDNORM,
                SplitSearcher.IndexComponent.STORE
            ).join();
            long prewarmMs = (System.nanoTime() - startTime) / 1_000_000;

            var afterFirstPrewarm = SplitCacheManager.getStorageDownloadMetrics();
            System.out.println("   Prewarm completed in " + prewarmMs + " ms");
            System.out.println("   Metrics after first prewarm: " + afterFirstPrewarm);

            // Prewarm uses the same storage layer as queries, so both counters may be incremented.
            // What matters is that TOTAL downloads occurred during first prewarm.
            assertTrue(afterFirstPrewarm.getTotalDownloads() > 0,
                "First prewarm should download data (got " + afterFirstPrewarm.getTotalDownloads() + " total downloads)");

            long firstPrewarmDownloads = afterFirstPrewarm.getTotalDownloads();
            long firstPrewarmBytes = afterFirstPrewarm.getTotalBytes();
            System.out.println("   ✓ First prewarm downloaded " + firstPrewarmDownloads + " chunks (" + firstPrewarmBytes + " bytes)");

            // STEP 2.5: Validate disk cache files exist
            System.out.println("\n[STEP 2.5] Validating disk cache files exist on disk...");
            validateDiskCacheFiles(diskCachePath, splitUri);

            // STEP 3: Second prewarm - should use cached data (zero new downloads)
            System.out.println("\n[STEP 3] Second prewarm (should use cache - zero downloads)...");
            System.out.println("   Calling resetStorageDownloadMetrics()...");
            SplitCacheManager.resetStorageDownloadMetrics();
            var afterReset = SplitCacheManager.getStorageDownloadMetrics();
            System.out.println("   After reset: " + afterReset);

            // Verify reset worked by reading multiple times
            System.out.println("   Verify reset (read 1): " + SplitCacheManager.getStorageDownloadMetrics());
            System.out.println("   Verify reset (read 2): " + SplitCacheManager.getStorageDownloadMetrics());

            startTime = System.nanoTime();
            System.out.println("   About to call preloadComponents...");
            searcher.preloadComponents(
                SplitSearcher.IndexComponent.TERM,
                SplitSearcher.IndexComponent.POSTINGS,
                SplitSearcher.IndexComponent.POSITIONS,
                SplitSearcher.IndexComponent.FASTFIELD,
                SplitSearcher.IndexComponent.FIELDNORM,
                SplitSearcher.IndexComponent.STORE
            ).join();
            System.out.println("   preloadComponents.join() returned");
            prewarmMs = (System.nanoTime() - startTime) / 1_000_000;

            // Read metrics immediately after prewarm
            var afterSecondPrewarm = SplitCacheManager.getStorageDownloadMetrics();
            System.out.println("   Prewarm completed in " + prewarmMs + " ms");
            System.out.println("   Metrics after second prewarm: " + afterSecondPrewarm);

            // Read multiple times to verify
            System.out.println("   Verify metrics (read 1): " + SplitCacheManager.getStorageDownloadMetrics());
            System.out.println("   Verify metrics (read 2): " + SplitCacheManager.getStorageDownloadMetrics());

            assertEquals(0, afterSecondPrewarm.getTotalDownloads(),
                "Second prewarm should have zero downloads (cache hit) - got " + afterSecondPrewarm.getTotalDownloads());
            System.out.println("   ✓ Second prewarm used cached data (zero downloads)");

            // STEP 4: Run queries - should use cached data (zero downloads)
            System.out.println("\n[STEP 4] Running queries (should use cache - zero downloads)...");
            SplitCacheManager.resetStorageDownloadMetrics();

            // Run several different queries
            String[] testQueries = {"content:test", "content:document", "title:test", "*"};
            int totalHits = 0;

            for (String queryStr : testQueries) {
                SplitQuery query = searcher.parseQuery(queryStr);
                SearchResult result = searcher.search(query, 10);
                totalHits += result.getHits().size();
                System.out.println("   Query '" + queryStr + "' returned " + result.getHits().size() + " hits");
            }

            var afterQueries = SplitCacheManager.getStorageDownloadMetrics();
            System.out.println("   Total hits: " + totalHits);
            System.out.println("   Metrics after queries: " + afterQueries);

            assertEquals(0, afterQueries.getTotalDownloads(),
                "Queries should have zero downloads (cache hit) - got " + afterQueries.getTotalDownloads());
            System.out.println("   ✓ Queries used cached data (zero downloads)");

            // STEP 5: Run aggregation queries - should use cached data (zero downloads)
            System.out.println("\n[STEP 5] Running aggregation queries (should use cache - zero downloads)...");
            SplitCacheManager.resetStorageDownloadMetrics();

            // Stats aggregation on the doc_id field
            StatsAggregation statsAgg = new StatsAggregation("doc_id_stats", "doc_id");
            SplitQuery aggQuery = searcher.parseQuery("*");
            SearchResult aggResult = searcher.search(aggQuery, 10, "doc_id_stats", statsAgg);

            System.out.println("   Aggregation query returned " + aggResult.getHits().size() + " hits");
            Map<String, AggregationResult> aggregations = aggResult.getAggregations();
            if (aggregations != null && aggregations.containsKey("doc_id_stats")) {
                AggregationResult aggValue = aggregations.get("doc_id_stats");
                if (aggValue instanceof StatsResult) {
                    StatsResult stats = (StatsResult) aggValue;
                    System.out.println("   Stats aggregation: count=" + stats.getCount() +
                        ", min=" + stats.getMin() + ", max=" + stats.getMax() +
                        ", avg=" + stats.getAverage() + ", sum=" + stats.getSum());
                } else {
                    System.out.println("   Aggregation result type: " + aggValue.getClass().getSimpleName());
                }
            } else {
                System.out.println("   No aggregation result returned (aggregations=" + aggregations + ")");
            }

            var afterAggregations = SplitCacheManager.getStorageDownloadMetrics();
            System.out.println("   Metrics after aggregations: " + afterAggregations);

            assertEquals(0, afterAggregations.getTotalDownloads(),
                "Aggregation queries should have zero downloads (cache hit) - got " + afterAggregations.getTotalDownloads());
            System.out.println("   ✓ Aggregation queries used cached data (zero downloads)");

            // SUMMARY
            System.out.println("\n" + "=".repeat(60));
            System.out.println("✅ " + storageType + " PREWARM CACHE VALIDATION PASSED!");
            System.out.println("   - First prewarm: " + firstPrewarmDownloads + " downloads (" + firstPrewarmBytes + " bytes)");
            System.out.println("   - Second prewarm: 0 downloads (cache hit)");
            System.out.println("   - Queries: 0 downloads (cache hit)");
            System.out.println("   - Aggregations: 0 downloads (cache hit)");
            System.out.println("=".repeat(60));
        }
    }

    // ========================================
    // TEST UTILITIES
    // ========================================

    private QuickwitSplit.SplitMetadata createTestSplit(Path indexPath, Path splitPath) throws IOException {
        System.out.println("Creating test split at: " + splitPath);

        try (SchemaBuilder schemaBuilder = new SchemaBuilder()) {
            schemaBuilder.addTextField("content", true, false, "default", "position");
            schemaBuilder.addTextField("title", true, false, "default", "position");
            schemaBuilder.addIntegerField("doc_id", true, true, true);

            try (Schema schema = schemaBuilder.build()) {
                try (Index index = new Index(schema, indexPath.toString(), false)) {
                    try (IndexWriter writer = index.writer(Index.Memory.DEFAULT_HEAP_SIZE, 1)) {
                        for (int i = 0; i < NUM_DOCUMENTS; i++) {
                            try (Document doc = new Document()) {
                                doc.addText("content",
                                    "This is test document " + i + " with some content for testing prewarm functionality");
                                doc.addText("title", "Test Document " + i);
                                doc.addInteger("doc_id", i);
                                writer.addDocument(doc);
                            }
                        }
                        writer.commit();
                    }

                    index.reload();

                    // Convert to split
                    QuickwitSplit.SplitConfig config = new QuickwitSplit.SplitConfig(
                        "prewarm-test", "prewarm-source", "test-node");
                    QuickwitSplit.SplitMetadata metadata = QuickwitSplit.convertIndexFromPath(
                        indexPath.toString(), splitPath.toString(), config);

                    System.out.println("   Split created: " + Files.size(splitPath) + " bytes");
                    return metadata;
                }
            }
        }
    }

    /**
     * Validate that disk cache files exist on disk after prewarm.
     * The L2DiskCache stores files in a specific directory structure:
     * {diskCachePath}/tantivy4java_slicecache/{storage_loc_hash}/{split_id}/...
     */
    private void validateDiskCacheFiles(Path diskCachePath, String splitUri) throws IOException {
        // The disk cache uses a subdirectory "tantivy4java_slicecache"
        Path cacheDir = diskCachePath.resolve("tantivy4java_slicecache");

        assertTrue(Files.exists(cacheDir),
            "Disk cache directory should exist at: " + cacheDir);
        assertTrue(Files.isDirectory(cacheDir),
            "Disk cache path should be a directory: " + cacheDir);

        // List all files in the cache directory recursively
        List<Path> cachedFiles = new ArrayList<>();
        Files.walk(cacheDir)
            .filter(Files::isRegularFile)
            .forEach(cachedFiles::add);

        System.out.println("   Disk cache directory: " + cacheDir);
        System.out.println("   Found " + cachedFiles.size() + " cached files:");

        long totalCacheSize = 0;
        for (Path file : cachedFiles) {
            long fileSize = Files.size(file);
            totalCacheSize += fileSize;
            System.out.println("      - " + cacheDir.relativize(file) + " (" + fileSize + " bytes)");
        }

        System.out.println("   Total disk cache size: " + totalCacheSize + " bytes");

        // Validate we have cached files
        assertTrue(cachedFiles.size() > 0,
            "Disk cache should contain at least one file after prewarm");

        // Validate total size is reasonable (should be roughly the size of the prewarm data)
        assertTrue(totalCacheSize > 0,
            "Disk cache total size should be greater than zero");

        // Check for manifest file (the L2DiskCache persists a manifest)
        List<Path> manifestFiles = cachedFiles.stream()
            .filter(f -> f.getFileName().toString().contains("manifest"))
            .collect(java.util.stream.Collectors.toList());
        System.out.println("   Found " + manifestFiles.size() + " manifest file(s)");

        System.out.println("   ✓ Disk cache files validated successfully");
    }

    // ========================================
    // S3 UTILITIES
    // ========================================

    private void uploadToS3(Path localPath, String key, String accessKey, String secretKey) {
        System.out.println("Uploading to S3: s3://" + AWS_BUCKET + "/" + key);
        try {
            software.amazon.awssdk.services.s3.S3Client s3Client = software.amazon.awssdk.services.s3.S3Client.builder()
                .region(software.amazon.awssdk.regions.Region.of(AWS_REGION))
                .credentialsProvider(software.amazon.awssdk.auth.credentials.StaticCredentialsProvider.create(
                    software.amazon.awssdk.auth.credentials.AwsBasicCredentials.create(accessKey, secretKey)))
                .build();

            s3Client.putObject(
                software.amazon.awssdk.services.s3.model.PutObjectRequest.builder()
                    .bucket(AWS_BUCKET)
                    .key(key)
                    .build(),
                localPath);

            s3Client.close();
            System.out.println("   Upload complete");
        } catch (Exception e) {
            System.out.println("   Upload failed: " + e.getMessage());
            throw new RuntimeException("Failed to upload to S3", e);
        }
    }

    private void deleteFromS3(String key, String accessKey, String secretKey) {
        try {
            software.amazon.awssdk.services.s3.S3Client s3Client = software.amazon.awssdk.services.s3.S3Client.builder()
                .region(software.amazon.awssdk.regions.Region.of(AWS_REGION))
                .credentialsProvider(software.amazon.awssdk.auth.credentials.StaticCredentialsProvider.create(
                    software.amazon.awssdk.auth.credentials.AwsBasicCredentials.create(accessKey, secretKey)))
                .build();

            s3Client.deleteObject(software.amazon.awssdk.services.s3.model.DeleteObjectRequest.builder()
                .bucket(AWS_BUCKET)
                .key(key)
                .build());

            s3Client.close();
            System.out.println("Cleaned up S3: " + key);
        } catch (Exception e) {
            System.out.println("Warning: Failed to cleanup S3: " + e.getMessage());
        }
    }

    // ========================================
    // AZURE UTILITIES
    // ========================================

    private void uploadToAzure(Path localPath, String blobName, String storageAccount, String accountKey) {
        System.out.println("Uploading to Azure: azure://" + AZURE_CONTAINER + "/" + blobName);
        try {
            com.azure.storage.blob.BlobServiceClient blobServiceClient = new com.azure.storage.blob.BlobServiceClientBuilder()
                .endpoint("https://" + storageAccount + ".blob.core.windows.net")
                .credential(new com.azure.storage.common.StorageSharedKeyCredential(storageAccount, accountKey))
                .buildClient();

            com.azure.storage.blob.BlobContainerClient containerClient = blobServiceClient.getBlobContainerClient(AZURE_CONTAINER);
            if (!containerClient.exists()) {
                containerClient.create();
            }

            com.azure.storage.blob.BlobClient blobClient = containerClient.getBlobClient(blobName);
            blobClient.uploadFromFile(localPath.toString(), true);

            System.out.println("   Upload complete");
        } catch (Exception e) {
            System.out.println("   Upload failed: " + e.getMessage());
            throw new RuntimeException("Failed to upload to Azure", e);
        }
    }

    private void deleteFromAzure(String blobName, String storageAccount, String accountKey) {
        try {
            com.azure.storage.blob.BlobServiceClient blobServiceClient = new com.azure.storage.blob.BlobServiceClientBuilder()
                .endpoint("https://" + storageAccount + ".blob.core.windows.net")
                .credential(new com.azure.storage.common.StorageSharedKeyCredential(storageAccount, accountKey))
                .buildClient();

            com.azure.storage.blob.BlobContainerClient containerClient = blobServiceClient.getBlobContainerClient(AZURE_CONTAINER);
            com.azure.storage.blob.BlobClient blobClient = containerClient.getBlobClient(blobName);
            blobClient.delete();

            System.out.println("Cleaned up Azure: " + blobName);
        } catch (Exception e) {
            System.out.println("Warning: Failed to cleanup Azure: " + e.getMessage());
        }
    }

    // ========================================
    // CREDENTIAL LOADING
    // ========================================

    private static String getAwsAccessKey() {
        String fromEnv = System.getenv("AWS_ACCESS_KEY_ID");
        if (fromEnv != null) return fromEnv;
        String fromProp = System.getProperty("test.s3.accessKey");
        if (fromProp != null) return fromProp;
        return awsAccessKey;
    }

    private static String getAwsSecretKey() {
        String fromEnv = System.getenv("AWS_SECRET_ACCESS_KEY");
        if (fromEnv != null) return fromEnv;
        String fromProp = System.getProperty("test.s3.secretKey");
        if (fromProp != null) return fromProp;
        return awsSecretKey;
    }

    private static String getAzureStorageAccount() {
        String fromEnv = System.getenv("AZURE_STORAGE_ACCOUNT");
        if (fromEnv != null) return fromEnv;
        String fromProp = System.getProperty("test.azure.storageAccount");
        if (fromProp != null) return fromProp;
        return azureStorageAccount;
    }

    private static String getAzureAccountKey() {
        String fromEnv = System.getenv("AZURE_STORAGE_KEY");
        if (fromEnv != null) return fromEnv;
        String fromProp = System.getProperty("test.azure.accountKey");
        if (fromProp != null) return fromProp;
        return azureAccountKey;
    }

    private static void loadAwsCredentials() {
        try {
            Path credentialsPath = Paths.get(System.getProperty("user.home"), ".aws", "credentials");
            if (!Files.exists(credentialsPath)) return;

            List<String> lines = Files.readAllLines(credentialsPath);
            boolean inDefaultSection = false;

            for (String line : lines) {
                line = line.trim();
                if (line.equals("[default]")) {
                    inDefaultSection = true;
                } else if (line.startsWith("[") && line.endsWith("]")) {
                    inDefaultSection = false;
                } else if (inDefaultSection && line.contains("=")) {
                    String[] parts = line.split("=", 2);
                    if (parts.length == 2) {
                        String key = parts[0].trim();
                        String value = parts[1].trim();
                        if ("aws_access_key_id".equals(key)) awsAccessKey = value;
                        else if ("aws_secret_access_key".equals(key)) awsSecretKey = value;
                    }
                }
            }
        } catch (Exception e) {
            System.out.println("Warning: Failed to load AWS credentials: " + e.getMessage());
        }
    }

    private static void loadAzureCredentials() {
        try {
            Path credentialsPath = Paths.get(System.getProperty("user.home"), ".azure", "credentials");
            if (!Files.exists(credentialsPath)) return;

            List<String> lines = Files.readAllLines(credentialsPath);
            boolean inDefaultSection = false;

            for (String line : lines) {
                line = line.trim();
                if (line.equals("[default]")) {
                    inDefaultSection = true;
                } else if (line.startsWith("[") && line.endsWith("]")) {
                    inDefaultSection = false;
                } else if (inDefaultSection && line.contains("=")) {
                    String[] parts = line.split("=", 2);
                    if (parts.length == 2) {
                        String key = parts[0].trim();
                        String value = parts[1].trim();
                        if ("storage_account".equals(key)) azureStorageAccount = value;
                        else if ("account_key".equals(key)) azureAccountKey = value;
                    }
                }
            }
        } catch (Exception e) {
            System.out.println("Warning: Failed to load Azure credentials: " + e.getMessage());
        }
    }
}

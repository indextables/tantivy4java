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
import io.indextables.tantivy4java.split.*;
import io.indextables.tantivy4java.split.merge.*;
import io.indextables.tantivy4java.result.*;

import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.BlobServiceClientBuilder;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobClient;
import com.azure.storage.common.StorageSharedKeyCredential;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * Tests for L2 tiered disk cache with real Azure Blob Storage backend.
 *
 * This test validates that the disk cache works correctly with Azure storage:
 * 1. Creates a split and uploads to Azure Blob Storage
 * 2. Searches with disk cache enabled - populates L2 cache
 * 3. Closes and re-opens with fresh memory cache
 * 4. Verifies data is served from disk cache (not re-downloaded from Azure)
 *
 * Prerequisites:
 * - Azure credentials configured in ~/.azure/credentials
 * - Or system properties test.azure.storageAccount and test.azure.accountKey
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class RealAzureDiskCacheTest {

    private static final String TEST_CONTAINER = System.getProperty("test.azure.container", "tantivy4java-test");
    private static final String STORAGE_ACCOUNT = System.getProperty("test.azure.storageAccount");
    private static final String ACCOUNT_KEY = System.getProperty("test.azure.accountKey");

    private static String azureStorageAccount;
    private static String azureAccountKey;
    private static BlobServiceClient blobServiceClient;
    private static String splitAzureUrl;
    private static Path diskCachePath;
    private static QuickwitSplit.SplitMetadata splitMetadata;

    @TempDir
    static Path tempDir;

    @BeforeAll
    static void setup() throws Exception {
        loadAzureCredentials();

        String storageAccount = getStorageAccount();
        String accountKey = getAccountKey();

        assumeTrue(storageAccount != null && accountKey != null,
            "Skipping Azure disk cache test - no Azure credentials found");

        blobServiceClient = new BlobServiceClientBuilder()
            .endpoint("https://" + storageAccount + ".blob.core.windows.net")
            .credential(new StorageSharedKeyCredential(storageAccount, accountKey))
            .buildClient();

        // Ensure container exists
        BlobContainerClient containerClient = blobServiceClient.getBlobContainerClient(TEST_CONTAINER);
        if (!containerClient.exists()) {
            containerClient.create();
        }

        // Create disk cache directory
        diskCachePath = tempDir.resolve("azure_disk_cache");
        Files.createDirectories(diskCachePath);

        // Create and upload a test split
        createAndUploadTestSplit();

        System.out.println("✅ Azure disk cache test setup complete");
        System.out.println("   Container: " + TEST_CONTAINER);
        System.out.println("   Split URL: " + splitAzureUrl);
        System.out.println("   Disk cache: " + diskCachePath);
    }

    @AfterAll
    static void cleanup() {
        if (blobServiceClient != null && splitAzureUrl != null) {
            try {
                // Extract blob name from URL
                String blobName = splitAzureUrl.replace("azure://" + TEST_CONTAINER + "/", "");
                BlobContainerClient containerClient = blobServiceClient.getBlobContainerClient(TEST_CONTAINER);
                BlobClient blobClient = containerClient.getBlobClient(blobName);
                if (blobClient.exists()) {
                    blobClient.delete();
                    System.out.println("✅ Cleaned up test split from Azure");
                }
            } catch (Exception e) {
                System.out.println("⚠️ Failed to clean up Azure split: " + e.getMessage());
            }
        }
    }

    private static void createAndUploadTestSplit() throws Exception {
        Path indexPath = tempDir.resolve("test_index");
        Path splitPath = tempDir.resolve("test.split");

        // Create index with test data
        try (SchemaBuilder builder = new SchemaBuilder()) {
            builder.addTextField("id", true, false, "raw", "position");
            builder.addTextField("title", true, false, "default", "position");
            builder.addTextField("content", true, false, "default", "position");
            builder.addIntegerField("count", true, true, true);

            try (Schema schema = builder.build();
                 Index index = new Index(schema, indexPath.toString(), false)) {

                try (IndexWriter writer = index.writer(Index.Memory.DEFAULT_HEAP_SIZE, 1)) {
                    // Add 100 documents
                    for (int i = 0; i < 100; i++) {
                        try (Document doc = new Document()) {
                            doc.addText("id", "doc-" + i);
                            doc.addText("title", "Azure Test Document " + i);
                            doc.addText("content", "This is the azure content for document number " + i +
                                " with some searchable text");
                            doc.addInteger("count", i);
                            writer.addDocument(doc);
                        }
                    }
                    writer.commit();
                }

                // Convert to split
                QuickwitSplit.SplitConfig config = new QuickwitSplit.SplitConfig(
                    "azure-disk-cache-test", "test-source", "test-node");
                splitMetadata = QuickwitSplit.convertIndexFromPath(indexPath.toString(), splitPath.toString(), config);
            }
        }

        // Upload to Azure
        String blobName = "disk-cache-tests/" + System.currentTimeMillis() + "/test.split";
        BlobContainerClient containerClient = blobServiceClient.getBlobContainerClient(TEST_CONTAINER);
        BlobClient blobClient = containerClient.getBlobClient(blobName);
        blobClient.uploadFromFile(splitPath.toString(), true);

        splitAzureUrl = "azure://" + TEST_CONTAINER + "/" + blobName;
    }

    @Test
    @Order(0)
    void testCountersNonZeroWithoutDiskCache() throws Exception {
        System.out.println("\n=== Test 0: Verify Azure Request Counters (NO Disk Cache) ===");

        // 🎯 This test verifies the OBJECT_STORAGE_REQUEST_COUNT counters work correctly
        // WITHOUT disk cache - should show non-zero requests to Azure

        // Create cache manager WITHOUT disk cache
        SplitCacheManager.CacheConfig cacheConfig =
            new SplitCacheManager.CacheConfig("azure-no-disk-cache-" + System.currentTimeMillis())
                .withMaxCacheSize(50_000_000) // 50MB L1 only, NO disk cache
                .withAzureCredentials(getStorageAccount(), getAccountKey());
                // NOTE: No .withTieredCache() - no disk cache!

        // Reset counters before test
        SplitCacheManager.resetObjectStorageRequestStats();
        long requestsBefore = SplitCacheManager.getObjectStorageRequestCount();
        long bytesBefore = SplitCacheManager.getObjectStorageBytesFetched();
        System.out.println("   Azure counters reset: requests=" + requestsBefore + ", bytes=" + bytesBefore);

        try (SplitCacheManager cacheManager = SplitCacheManager.getInstance(cacheConfig)) {
            try (SplitSearcher searcher = cacheManager.createSplitSearcher(splitAzureUrl, splitMetadata)) {
                // Search to trigger Azure requests
                SplitQuery query = searcher.parseQuery("content:searchable");
                SearchResult result = searcher.search(query, 10);

                assertTrue(result.getHits().size() > 0, "Should find documents");
                System.out.println("   Found " + result.getHits().size() + " documents");

                // Retrieve a document
                if (!result.getHits().isEmpty()) {
                    try (Document doc = searcher.doc(result.getHits().get(0).getDocAddress())) {
                        assertNotNull(doc.getFirst("title"));
                        System.out.println("   Retrieved document: " + doc.getFirst("title"));
                    }
                }
            }
        }

        // 🎯 CRITICAL: Verify counters show NON-ZERO - proves they work correctly
        long requestsAfter = SplitCacheManager.getObjectStorageRequestCount();
        long bytesAfter = SplitCacheManager.getObjectStorageBytesFetched();
        System.out.println("   Azure counters after query: requests=" + requestsAfter + ", bytes=" + bytesAfter);

        assertTrue(requestsAfter > requestsBefore,
            "🚨 COUNTER FAILURE: Expected non-zero Azure requests but got " + (requestsAfter - requestsBefore) +
            ". Counters may not be working correctly!");
        assertTrue(bytesAfter > bytesBefore,
            "🚨 COUNTER FAILURE: Expected non-zero bytes from Azure but got " + (bytesAfter - bytesBefore) +
            ". Counters may not be working correctly!");

        System.out.println("✅ Azure request counters verified: " + (requestsAfter - requestsBefore) +
            " requests, " + (bytesAfter - bytesBefore) + " bytes fetched from Azure");
    }

    @Test
    @Order(1)
    void testDiskCachePopulation() throws Exception {
        System.out.println("\n=== Test 1: Azure Disk Cache Population ===");

        // Configure with disk cache
        SplitCacheManager.TieredCacheConfig tieredConfig = new SplitCacheManager.TieredCacheConfig()
            .withDiskCachePath(diskCachePath.toString())
            .withMaxDiskSize(100_000_000L) // 100MB
            .withCompression(SplitCacheManager.CompressionAlgorithm.LZ4);

        SplitCacheManager.CacheConfig cacheConfig =
            new SplitCacheManager.CacheConfig("azure-disk-cache-test-" + System.currentTimeMillis())
                .withMaxCacheSize(50_000_000) // 50MB L1
                .withAzureCredentials(getStorageAccount(), getAccountKey())
                .withTieredCache(tieredConfig);

        try (SplitCacheManager cacheManager = SplitCacheManager.getInstance(cacheConfig)) {
            try (SplitSearcher searcher = cacheManager.createSplitSearcher(splitAzureUrl, splitMetadata)) {
                // Search to trigger cache population
                SplitQuery query = searcher.parseQuery("content:searchable");
                SearchResult result = searcher.search(query, 10);

                assertTrue(result.getHits().size() > 0, "Should find documents");
                System.out.println("   Found " + result.getHits().size() + " documents");

                // Also retrieve a document to cache more data
                if (!result.getHits().isEmpty()) {
                    try (Document doc = searcher.doc(result.getHits().get(0).getDocAddress())) {
                        assertNotNull(doc.getFirst("title"));
                        System.out.println("   Retrieved document: " + doc.getFirst("title"));
                    }
                }
            }

            // Get disk cache stats
            SplitCacheManager.DiskCacheStats stats = cacheManager.getDiskCacheStats();
            if (stats != null) {
                System.out.println("   Disk cache stats: " + stats);
            }
        }

        // Wait for async disk writes
        Thread.sleep(500);

        // Verify files were created in disk cache
        Path cacheSubdir = diskCachePath.resolve("tantivy4java_slicecache");
        assertTrue(Files.exists(cacheSubdir), "Cache subdirectory should exist");

        long fileCount = Files.walk(cacheSubdir)
            .filter(Files::isRegularFile)
            .count();
        System.out.println("   Disk cache files: " + fileCount);
        assertTrue(fileCount > 0, "Should have cached files on disk");

        System.out.println("✅ Azure disk cache population test passed");
    }

    @Test
    @Order(2)
    void testDiskCachePersistence() throws Exception {
        System.out.println("\n=== Test 2: Azure Disk Cache Persistence ===");

        // Create a FRESH cache manager (new memory cache, but same disk cache)
        SplitCacheManager.TieredCacheConfig tieredConfig = new SplitCacheManager.TieredCacheConfig()
            .withDiskCachePath(diskCachePath.toString())
            .withMaxDiskSize(100_000_000L);

        SplitCacheManager.CacheConfig cacheConfig =
            new SplitCacheManager.CacheConfig("azure-disk-cache-test-fresh-" + System.currentTimeMillis())
                .withMaxCacheSize(50_000_000)
                .withAzureCredentials(getStorageAccount(), getAccountKey())
                .withTieredCache(tieredConfig);

        try (SplitCacheManager cacheManager = SplitCacheManager.getInstance(cacheConfig)) {
            // 🎯 CRITICAL: Reset Azure request counters BEFORE query to verify disk cache is used
            SplitCacheManager.resetObjectStorageRequestStats();
            long requestsBefore = SplitCacheManager.getObjectStorageRequestCount();
            long bytesBefore = SplitCacheManager.getObjectStorageBytesFetched();
            System.out.println("   Azure request counters reset: requests=" + requestsBefore + ", bytes=" + bytesBefore);

            try (SplitSearcher searcher = cacheManager.createSplitSearcher(splitAzureUrl, splitMetadata)) {
                // Search again - should hit disk cache
                SplitQuery query = searcher.parseQuery("content:searchable");
                SearchResult result = searcher.search(query, 10);

                assertTrue(result.getHits().size() > 0, "Should find documents from disk cache");
                System.out.println("   Found " + result.getHits().size() + " documents (from disk cache)");

                // Retrieve document
                if (!result.getHits().isEmpty()) {
                    try (Document doc = searcher.doc(result.getHits().get(0).getDocAddress())) {
                        assertNotNull(doc.getFirst("title"));
                        System.out.println("   Retrieved: " + doc.getFirst("title"));
                    }
                }
            }

            // 🎯 CRITICAL: Verify NO Azure requests were made - all data served from disk cache
            long requestsAfter = SplitCacheManager.getObjectStorageRequestCount();
            long bytesAfter = SplitCacheManager.getObjectStorageBytesFetched();
            System.out.println("   Azure requests after query: requests=" + requestsAfter + ", bytes=" + bytesAfter);

            assertEquals(0, requestsAfter - requestsBefore,
                "🚨 DISK CACHE FAILURE: Expected 0 Azure requests but got " + (requestsAfter - requestsBefore) +
                ". Data should be served from L2 disk cache, not from Azure!");
            assertEquals(0, bytesAfter - bytesBefore,
                "🚨 DISK CACHE FAILURE: Expected 0 bytes from Azure but got " + (bytesAfter - bytesBefore) +
                ". Data should be served from L2 disk cache!");

            // Verify disk cache stats show data
            SplitCacheManager.DiskCacheStats stats = cacheManager.getDiskCacheStats();
            if (stats != null) {
                System.out.println("   Disk cache stats: " + stats);
            }
        }

        System.out.println("✅ Azure disk cache persistence test passed - ZERO Azure requests!");
    }

    @Test
    @Order(3)
    void testDiskCacheWithDifferentQueries() throws Exception {
        System.out.println("\n=== Test 3: Azure Multiple Queries with Disk Cache ===");

        SplitCacheManager.TieredCacheConfig tieredConfig = new SplitCacheManager.TieredCacheConfig()
            .withDiskCachePath(diskCachePath.toString())
            .withMaxDiskSize(100_000_000L);

        SplitCacheManager.CacheConfig cacheConfig =
            new SplitCacheManager.CacheConfig("azure-disk-cache-multi-" + System.currentTimeMillis())
                .withMaxCacheSize(50_000_000)
                .withAzureCredentials(getStorageAccount(), getAccountKey())
                .withTieredCache(tieredConfig);

        try (SplitCacheManager cacheManager = SplitCacheManager.getInstance(cacheConfig)) {
            try (SplitSearcher searcher = cacheManager.createSplitSearcher(splitAzureUrl, splitMetadata)) {
                // Multiple different queries
                String[] queries = {
                    "title:Azure",
                    "content:number",
                    "id:doc-50",
                    "*"
                };

                for (String queryStr : queries) {
                    SplitQuery query = searcher.parseQuery(queryStr);
                    SearchResult result = searcher.search(query, 5);
                    System.out.println("   Query '" + queryStr + "': " + result.getHits().size() + " hits");
                    assertTrue(result.getHits().size() >= 0);
                }

                // Range query
                SplitQuery rangeQuery = new SplitRangeQuery(
                    "count",
                    SplitRangeQuery.RangeBound.inclusive("10"),
                    SplitRangeQuery.RangeBound.inclusive("20"),
                    "i64");
                SearchResult rangeResult = searcher.search(rangeQuery, 20);
                System.out.println("   Range query [10-20]: " + rangeResult.getHits().size() + " hits");
                assertTrue(rangeResult.getHits().size() > 0);
            }
        }

        System.out.println("✅ Azure multiple queries test passed");
    }

    private static String getStorageAccount() {
        if (STORAGE_ACCOUNT != null) return STORAGE_ACCOUNT;
        if (azureStorageAccount != null) return azureStorageAccount;
        return System.getenv("AZURE_STORAGE_ACCOUNT");
    }

    private static String getAccountKey() {
        if (ACCOUNT_KEY != null) return ACCOUNT_KEY;
        if (azureAccountKey != null) return azureAccountKey;
        return System.getenv("AZURE_STORAGE_KEY");
    }

    private static void loadAzureCredentials() {
        try {
            Path credentialsPath = Paths.get(System.getProperty("user.home"), ".azure", "credentials");
            if (!Files.exists(credentialsPath)) {
                System.out.println("Azure credentials file not found at: " + credentialsPath);
                return;
            }

            List<String> lines = Files.readAllLines(credentialsPath);
            boolean inDefaultSection = false;

            for (String line : lines) {
                line = line.trim();
                if (line.equals("[default]")) {
                    inDefaultSection = true;
                    continue;
                } else if (line.startsWith("[") && line.endsWith("]")) {
                    inDefaultSection = false;
                    continue;
                }

                if (inDefaultSection && line.contains("=")) {
                    String[] parts = line.split("=", 2);
                    if (parts.length == 2) {
                        String key = parts[0].trim();
                        String value = parts[1].trim();

                        if ("storage_account".equals(key)) {
                            azureStorageAccount = value;
                        } else if ("account_key".equals(key)) {
                            azureAccountKey = value;
                        }
                    }
                }
            }

            if (azureStorageAccount != null && azureAccountKey != null) {
                System.out.println("✅ Loaded Azure credentials from ~/.azure/credentials");
            }
        } catch (Exception e) {
            System.out.println("Failed to read Azure credentials: " + e.getMessage());
        }
    }
}

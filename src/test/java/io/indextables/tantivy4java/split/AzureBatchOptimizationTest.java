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

package io.indextables.tantivy4java.split;

import io.indextables.tantivy4java.core.*;
import io.indextables.tantivy4java.result.*;
import io.indextables.tantivy4java.split.merge.*;

import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.BlobServiceClientBuilder;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobClient;
import com.azure.storage.common.StorageSharedKeyCredential;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Azure Blob Storage batch optimization test.
 *
 * This test validates that batch optimization works correctly for Azure Blob Storage,
 * similar to the S3 batch optimization tests. Key validations:
 *
 * 1. Batch optimization metrics are recorded on FIRST query (not just cached queries)
 * 2. Consolidation ratio is calculated correctly
 * 3. Cost savings percentages are computed
 * 4. Metrics accumulate across multiple batch operations
 *
 * Prerequisites:
 * - Azure credentials configured in ~/.azure/credentials:
 *   [default]
 *   storage_account=yourstorageaccount
 *   account_key=your-account-key
 *
 * Or via system properties:
 * -Dtest.azure.storageAccount=yourstorageaccount
 * -Dtest.azure.accountKey=your-account-key
 *
 * Or via environment variables:
 * AZURE_STORAGE_ACCOUNT, AZURE_STORAGE_KEY
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class AzureBatchOptimizationTest {

    // Test configuration from system properties
    private static final String TEST_CONTAINER = System.getProperty("test.azure.container", "tantivy4java-batch-test");
    private static final String STORAGE_ACCOUNT = System.getProperty("test.azure.storageAccount");
    private static final String ACCOUNT_KEY = System.getProperty("test.azure.accountKey");

    // Azure credentials loaded from ~/.azure/credentials
    private static String azureStorageAccount;
    private static String azureAccountKey;

    @TempDir
    static Path tempDir;

    // Test data tracking
    private static QuickwitSplit.SplitMetadata splitMetadata;
    private static String splitAzureUrl;
    private static BlobServiceClient blobServiceClient;
    private static int totalDocs = 0;

    @BeforeAll
    static void setupAzureClient() {
        System.out.println("\n=== AZURE BATCH OPTIMIZATION TEST ===\n");

        // Load Azure credentials
        loadAzureCredentials();

        String storageAccount = getStorageAccount();
        String accountKey = getAccountKey();

        if (storageAccount == null || accountKey == null) {
            System.out.println("No Azure credentials found. Skipping Azure batch optimization tests.");
            System.out.println("   Set -Dtest.azure.storageAccount and -Dtest.azure.accountKey");
            System.out.println("   Or configure ~/.azure/credentials file");
            System.out.println("   Or configure AZURE_STORAGE_ACCOUNT/AZURE_STORAGE_KEY environment variables");
            Assumptions.abort("Azure credentials not available");
        }

        // Create Azure blob service client
        blobServiceClient = new BlobServiceClientBuilder()
            .endpoint("https://" + storageAccount + ".blob.core.windows.net")
            .credential(new StorageSharedKeyCredential(storageAccount, accountKey))
            .buildClient();
        System.out.println("Azure client initialized for storage account: " + storageAccount);

        // Ensure the test container exists
        BlobContainerClient containerClient = blobServiceClient.getBlobContainerClient(TEST_CONTAINER);
        if (!containerClient.exists()) {
            containerClient.create();
            System.out.println("Created Azure container: " + TEST_CONTAINER);
        } else {
            System.out.println("Azure container exists: " + TEST_CONTAINER);
        }
    }

    @AfterAll
    static void cleanupAzure() {
        // Cleanup uploaded test split
        if (blobServiceClient != null && splitAzureUrl != null) {
            try {
                String blobName = "batch-test/test-batch.split";
                BlobClient blobClient = blobServiceClient
                    .getBlobContainerClient(TEST_CONTAINER)
                    .getBlobClient(blobName);
                if (blobClient.exists()) {
                    blobClient.delete();
                    System.out.println("Cleaned up test blob: " + blobName);
                }
            } catch (Exception e) {
                System.err.println("Warning: Failed to cleanup test blob: " + e.getMessage());
            }
        }
        System.out.println("Azure batch optimization test cleanup complete\n");
    }

    private static String getStorageAccount() {
        if (STORAGE_ACCOUNT != null) return STORAGE_ACCOUNT;
        if (azureStorageAccount != null) return azureStorageAccount;
        String env = System.getenv("AZURE_STORAGE_ACCOUNT");
        if (env != null) return env;
        return null;
    }

    private static String getAccountKey() {
        if (ACCOUNT_KEY != null) return ACCOUNT_KEY;
        if (azureAccountKey != null) return azureAccountKey;
        String env = System.getenv("AZURE_STORAGE_KEY");
        if (env != null) return env;
        return null;
    }

    /**
     * Reads Azure credentials from ~/.azure/credentials file
     */
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
                System.out.println("Loaded Azure credentials from ~/.azure/credentials");
            }
        } catch (IOException e) {
            System.err.println("Warning: Failed to read Azure credentials file: " + e.getMessage());
        }
    }

    @Test
    @Order(1)
    @DisplayName("Step 1: Create test index with sufficient documents for batch optimization")
    public void step1_createTestIndex() throws Exception {
        System.out.println("Step 1: Creating test index with documents for batch testing...");

        // Create index with enough documents to trigger batch optimization (threshold is 50)
        totalDocs = 200;  // Enough for multiple batch tests

        try (SchemaBuilder builder = new SchemaBuilder()) {
            builder.addTextField("title", true, false, "default", "position");
            builder.addTextField("content", true, false, "default", "position");
            builder.addIntegerField("id", true, true, false);
            builder.addIntegerField("category", true, true, false);

            try (Schema schema = builder.build()) {
                Path indexPath = tempDir.resolve("batch-test-index");
                try (Index index = new Index(schema, indexPath.toString(), false)) {
                    try (IndexWriter writer = index.writer(Index.Memory.DEFAULT_HEAP_SIZE, 1)) {
                        for (int i = 0; i < totalDocs; i++) {
                            try (Document doc = new Document()) {
                                doc.addText("title", "Document " + i + " for batch testing");
                                doc.addText("content", "This is content for document " + i + " with searchable text batch azure");
                                doc.addInteger("id", i);
                                doc.addInteger("category", i % 10);
                                writer.addDocument(doc);
                            }
                        }
                        writer.commit();
                    }

                    // Reload and merge segments for optimal performance
                    index.reload();
                    try (Searcher searcher = index.searcher()) {
                        List<String> segmentIds = searcher.getSegmentIds();
                        if (segmentIds.size() > 1) {
                            try (IndexWriter writer = index.writer(Index.Memory.LARGE_HEAP_SIZE, 1)) {
                                writer.merge(segmentIds);
                                writer.commit();
                            }
                            index.reload();
                        }
                    }

                    // Convert to Quickwit split
                    Path splitPath = tempDir.resolve("batch-test.split");
                    QuickwitSplit.SplitConfig splitConfig = new QuickwitSplit.SplitConfig(
                        "batch-test-index", "batch-source", "batch-node");
                    splitMetadata = QuickwitSplit.convertIndexFromPath(indexPath.toString(), splitPath.toString(), splitConfig);

                    System.out.println("   Created split with " + splitMetadata.getNumDocs() + " documents");
                    assertEquals(totalDocs, splitMetadata.getNumDocs(), "Split should contain all documents");
                }
            }
        }

        System.out.println("Step 1 complete: Test index created\n");
    }

    @Test
    @Order(2)
    @DisplayName("Step 2: Upload split to Azure Blob Storage")
    public void step2_uploadSplitToAzure() throws Exception {
        System.out.println("Step 2: Uploading split to Azure Blob Storage...");

        assertNotNull(splitMetadata, "Split metadata should be available from Step 1");

        Path splitPath = tempDir.resolve("batch-test.split");
        assertTrue(Files.exists(splitPath), "Split file should exist");

        String blobName = "batch-test/test-batch.split";

        // Upload to Azure
        BlobClient blobClient = blobServiceClient
            .getBlobContainerClient(TEST_CONTAINER)
            .getBlobClient(blobName);

        blobClient.uploadFromFile(splitPath.toString(), true);

        // Construct Azure URL
        splitAzureUrl = "azure://" + TEST_CONTAINER + "/" + blobName;
        System.out.println("   Uploaded to: " + splitAzureUrl);

        // Verify upload
        assertTrue(blobClient.exists(), "Blob should exist after upload");
        long blobSize = blobClient.getProperties().getBlobSize();
        System.out.println("   Blob size: " + blobSize + " bytes");

        System.out.println("Step 2 complete: Split uploaded to Azure\n");
    }

    @Test
    @Order(3)
    @DisplayName("Step 3: Test batch optimization on FIRST query (cold cache)")
    public void step3_testBatchOptimizationFirstQuery() throws Exception {
        System.out.println("Step 3: Testing batch optimization on FIRST query (cold cache)...");

        assertNotNull(splitAzureUrl, "Azure URL should be available from Step 2");
        assertNotNull(splitMetadata, "Split metadata should be available from Step 1");

        // Reset metrics before test
        SplitCacheManager.resetBatchMetrics();

        // Create a fresh cache manager (simulates cold cache)
        String uniqueCacheName = "azure-batch-cold-" + System.currentTimeMillis();
        SplitCacheManager.CacheConfig cacheConfig = new SplitCacheManager.CacheConfig(uniqueCacheName)
            .withMaxCacheSize(100_000_000)
            .withAzureCredentials(getStorageAccount(), getAccountKey());

        try (SplitCacheManager cacheManager = SplitCacheManager.getInstance(cacheConfig)) {
            try (SplitSearcher searcher = cacheManager.createSplitSearcher(splitAzureUrl, splitMetadata)) {

                // Search for documents to get doc addresses
                SplitQuery query = new SplitTermQuery("content", "batch");
                SearchResult results = searcher.search(query, 100);

                assertTrue(results.getHits().size() >= 50,
                    "Should find at least 50 documents to trigger batch optimization");

                // Collect doc addresses for batch retrieval (minimum 50 to trigger optimization)
                List<DocAddress> batchAddresses = results.getHits()
                    .stream()
                    .limit(75)  // Use 75 docs to ensure we're above threshold
                    .map(hit -> hit.getDocAddress())
                    .collect(Collectors.toList());

                System.out.println("   Batch size: " + batchAddresses.size() + " documents");

                // Reset metrics right before batch operation
                SplitCacheManager.resetBatchMetrics();

                // Perform batch retrieval - THIS IS THE FIRST QUERY (cold cache)
                long startTime = System.nanoTime();
                List<Document> batchDocs = searcher.docBatch(batchAddresses);
                long endTime = System.nanoTime();
                long durationMs = (endTime - startTime) / 1_000_000;

                System.out.println("   Batch retrieval completed in " + durationMs + " ms");
                assertEquals(batchAddresses.size(), batchDocs.size(), "Should retrieve all requested documents");

                // Validate batch optimization metrics were recorded
                BatchOptimizationMetrics metrics = SplitCacheManager.getBatchMetrics();

                System.out.println("\n   Batch Optimization Metrics (FIRST QUERY):");
                System.out.println("      Total Operations: " + metrics.getTotalBatchOperations());
                System.out.println("      Documents Requested: " + metrics.getTotalDocumentsRequested());
                System.out.println("      Total Requests: " + metrics.getTotalRequests());
                System.out.println("      Consolidated Requests: " + metrics.getConsolidatedRequests());
                System.out.println("      Consolidation Ratio: " + String.format("%.1fx", metrics.getConsolidationRatio()));
                System.out.println("      Cost Savings: " + String.format("%.1f%%", metrics.getCostSavingsPercent()));
                System.out.println("      Bytes Transferred: " + metrics.getBytesTransferred());

                // KEY ASSERTION: Metrics should be recorded on FIRST query (not just cached queries)
                assertTrue(metrics.getTotalBatchOperations() > 0,
                    "CRITICAL: Batch operations should be recorded on FIRST query (cold cache). " +
                    "If this fails, the bug described in TANTIVY4JAVA_BATCH_OPTIMIZATION_BUG_REPORT.md is present.");

                assertEquals(batchAddresses.size(), metrics.getTotalDocumentsRequested(),
                    "Documents requested should match batch size");

                assertTrue(metrics.getConsolidationRatio() >= 1.0,
                    "Consolidation ratio should be at least 1.0 (no worse than individual requests)");

                System.out.println("\n   FIRST QUERY BATCH OPTIMIZATION: WORKING");

                // Cleanup documents
                for (Document doc : batchDocs) {
                    doc.close();
                }
            }
        }

        System.out.println("Step 3 complete: First query batch optimization validated\n");
    }

    @Test
    @Order(4)
    @DisplayName("Step 4: Test batch optimization on SECOND query (warm cache)")
    public void step4_testBatchOptimizationSecondQuery() throws Exception {
        System.out.println("Step 4: Testing batch optimization on SECOND query (warm cache)...");

        assertNotNull(splitAzureUrl, "Azure URL should be available from Step 2");
        assertNotNull(splitMetadata, "Split metadata should be available from Step 1");

        // Use a consistent cache name to get warm cache
        SplitCacheManager.CacheConfig cacheConfig = new SplitCacheManager.CacheConfig("azure-batch-warm")
            .withMaxCacheSize(100_000_000)
            .withAzureCredentials(getStorageAccount(), getAccountKey());

        try (SplitCacheManager cacheManager = SplitCacheManager.getInstance(cacheConfig)) {
            // First, create a searcher and do a query to warm the cache
            try (SplitSearcher warmupSearcher = cacheManager.createSplitSearcher(splitAzureUrl, splitMetadata)) {
                SplitQuery warmupQuery = new SplitTermQuery("content", "azure");
                SearchResult warmupResults = warmupSearcher.search(warmupQuery, 10);
                System.out.println("   Cache warmup query returned " + warmupResults.getHits().size() + " results");
            }

            // Reset metrics before the actual test
            SplitCacheManager.resetBatchMetrics();

            // Now do a second query with the warm cache
            try (SplitSearcher searcher = cacheManager.createSplitSearcher(splitAzureUrl, splitMetadata)) {
                SplitQuery query = new SplitTermQuery("content", "batch");
                SearchResult results = searcher.search(query, 100);

                List<DocAddress> batchAddresses = results.getHits()
                    .stream()
                    .limit(60)
                    .map(hit -> hit.getDocAddress())
                    .collect(Collectors.toList());

                long startTime = System.nanoTime();
                List<Document> batchDocs = searcher.docBatch(batchAddresses);
                long endTime = System.nanoTime();
                long durationMs = (endTime - startTime) / 1_000_000;

                System.out.println("   Warm cache batch retrieval: " + durationMs + " ms");

                BatchOptimizationMetrics metrics = SplitCacheManager.getBatchMetrics();

                System.out.println("\n   Batch Optimization Metrics (SECOND QUERY - warm cache):");
                System.out.println("      Total Operations: " + metrics.getTotalBatchOperations());
                System.out.println("      Documents Requested: " + metrics.getTotalDocumentsRequested());
                System.out.println("      Consolidation Ratio: " + String.format("%.1fx", metrics.getConsolidationRatio()));
                System.out.println("      Cost Savings: " + String.format("%.1f%%", metrics.getCostSavingsPercent()));

                // Metrics should be recorded for warm cache queries too
                assertTrue(metrics.getTotalBatchOperations() > 0,
                    "Batch operations should be recorded for warm cache queries");

                System.out.println("\n   SECOND QUERY BATCH OPTIMIZATION: WORKING");

                for (Document doc : batchDocs) {
                    doc.close();
                }
            }
        }

        System.out.println("Step 4 complete: Second query batch optimization validated\n");
    }

    @Test
    @Order(5)
    @DisplayName("Step 5: Test metrics accumulation across multiple batches")
    public void step5_testMetricsAccumulation() throws Exception {
        System.out.println("Step 5: Testing metrics accumulation across multiple batch operations...");

        assertNotNull(splitAzureUrl, "Azure URL should be available from Step 2");
        assertNotNull(splitMetadata, "Split metadata should be available from Step 1");

        // Reset metrics
        SplitCacheManager.resetBatchMetrics();

        SplitCacheManager.CacheConfig cacheConfig = new SplitCacheManager.CacheConfig("azure-batch-accumulation")
            .withMaxCacheSize(100_000_000)
            .withAzureCredentials(getStorageAccount(), getAccountKey());

        try (SplitCacheManager cacheManager = SplitCacheManager.getInstance(cacheConfig)) {
            try (SplitSearcher searcher = cacheManager.createSplitSearcher(splitAzureUrl, splitMetadata)) {

                // Perform 3 separate batch operations
                for (int batch = 1; batch <= 3; batch++) {
                    SplitQuery query = new SplitTermQuery("content", "document");
                    SearchResult results = searcher.search(query, 200);

                    int startIdx = (batch - 1) * 50;
                    int endIdx = Math.min(startIdx + 50, results.getHits().size());

                    if (endIdx - startIdx < 50) {
                        System.out.println("   Skipping batch " + batch + " - not enough documents");
                        continue;
                    }

                    List<DocAddress> batchAddresses = results.getHits()
                        .subList(startIdx, endIdx)
                        .stream()
                        .map(hit -> hit.getDocAddress())
                        .collect(Collectors.toList());

                    List<Document> batchDocs = searcher.docBatch(batchAddresses);
                    System.out.println("   Batch " + batch + ": Retrieved " + batchDocs.size() + " documents");

                    for (Document doc : batchDocs) {
                        doc.close();
                    }

                    // Check accumulated metrics
                    BatchOptimizationMetrics metrics = SplitCacheManager.getBatchMetrics();
                    System.out.println("      Accumulated operations: " + metrics.getTotalBatchOperations());
                    System.out.println("      Accumulated documents: " + metrics.getTotalDocumentsRequested());
                }

                // Final metrics check
                BatchOptimizationMetrics finalMetrics = SplitCacheManager.getBatchMetrics();

                System.out.println("\n   Final Accumulated Metrics:");
                System.out.println("      Total Batch Operations: " + finalMetrics.getTotalBatchOperations());
                System.out.println("      Total Documents Requested: " + finalMetrics.getTotalDocumentsRequested());
                System.out.println("      Average Batch Size: " + String.format("%.1f", finalMetrics.getAverageBatchSize()));
                System.out.println("      Overall Consolidation Ratio: " + String.format("%.1fx", finalMetrics.getConsolidationRatio()));

                // Should have accumulated metrics from multiple operations
                assertTrue(finalMetrics.getTotalBatchOperations() >= 3,
                    "Should have at least 3 batch operations recorded");
                assertTrue(finalMetrics.getTotalDocumentsRequested() >= 150,
                    "Should have requested at least 150 documents total (3 batches of 50)");

                System.out.println("\n   METRICS ACCUMULATION: WORKING");
            }
        }

        System.out.println("Step 5 complete: Metrics accumulation validated\n");
    }

    @Test
    @Order(6)
    @DisplayName("Step 6: Verify metrics reset functionality")
    public void step6_testMetricsReset() {
        System.out.println("Step 6: Verifying metrics reset functionality...");

        // Get current metrics (should have data from previous tests)
        BatchOptimizationMetrics before = SplitCacheManager.getBatchMetrics();
        System.out.println("   Before reset - Operations: " + before.getTotalBatchOperations());

        // Reset
        SplitCacheManager.resetBatchMetrics();

        // Verify all metrics are zero
        BatchOptimizationMetrics after = SplitCacheManager.getBatchMetrics();

        assertEquals(0, after.getTotalBatchOperations(), "Operations should be 0 after reset");
        assertEquals(0, after.getTotalDocumentsRequested(), "Documents should be 0 after reset");
        assertEquals(0, after.getTotalRequests(), "Total requests should be 0 after reset");
        assertEquals(0, after.getConsolidatedRequests(), "Consolidated requests should be 0 after reset");

        System.out.println("   After reset - All metrics verified as 0");
        System.out.println("Step 6 complete: Metrics reset validated\n");
    }

    @Test
    @Order(7)
    @DisplayName("Step 7: Summary and consolidated metrics report")
    public void step7_summaryReport() throws Exception {
        System.out.println("Step 7: Generating summary report...");

        assertNotNull(splitAzureUrl, "Azure URL should be available");
        assertNotNull(splitMetadata, "Split metadata should be available");

        // Reset and do one final comprehensive batch operation
        SplitCacheManager.resetBatchMetrics();

        SplitCacheManager.CacheConfig cacheConfig = new SplitCacheManager.CacheConfig("azure-batch-summary")
            .withMaxCacheSize(100_000_000)
            .withAzureCredentials(getStorageAccount(), getAccountKey());

        try (SplitCacheManager cacheManager = SplitCacheManager.getInstance(cacheConfig)) {
            try (SplitSearcher searcher = cacheManager.createSplitSearcher(splitAzureUrl, splitMetadata)) {

                // Do a larger batch for meaningful metrics
                SplitQuery query = new SplitTermQuery("content", "batch");
                SearchResult results = searcher.search(query, 150);

                List<DocAddress> batchAddresses = results.getHits()
                    .stream()
                    .map(hit -> hit.getDocAddress())
                    .collect(Collectors.toList());

                if (batchAddresses.size() >= 50) {
                    List<Document> batchDocs = searcher.docBatch(batchAddresses);

                    BatchOptimizationMetrics metrics = SplitCacheManager.getBatchMetrics();

                    System.out.println("\n" + "=".repeat(60));
                    System.out.println("       AZURE BATCH OPTIMIZATION TEST SUMMARY");
                    System.out.println("=".repeat(60));
                    System.out.println(metrics.getSummary());
                    System.out.println("=".repeat(60));

                    // Validate key metrics
                    assertTrue(metrics.getTotalBatchOperations() > 0, "Should have recorded batch operations");
                    assertTrue(metrics.getConsolidationRatio() >= 1.0, "Should have consolidation ratio >= 1.0");

                    double estimatedSavings = metrics.getEstimatedCostSavingsUSD();
                    System.out.println("\nEstimated Cost Savings: $" + String.format("%.6f", estimatedSavings));
                    System.out.println("(Based on $0.0004 per 10,000 S3/Azure requests)\n");

                    for (Document doc : batchDocs) {
                        doc.close();
                    }
                }
            }
        }

        System.out.println("=== AZURE BATCH OPTIMIZATION TEST COMPLETE ===\n");
    }
}

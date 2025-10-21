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
import io.indextables.tantivy4java.query.*;
import io.indextables.tantivy4java.result.*;
import io.indextables.tantivy4java.split.*;
import io.indextables.tantivy4java.split.merge.*;
import io.indextables.tantivy4java.aggregation.*;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;

import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.BlobServiceClientBuilder;
import com.azure.storage.common.StorageSharedKeyCredential;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.Files;
import java.util.Map;
import java.util.HashMap;
import java.util.List;

/**
 * Comprehensive test suite for validating aggregations (sum, avg, count, min, max)
 * work correctly with Azure Blob Storage backend.
 *
 * Tests the complete workflow:
 * 1. Create index with numeric data
 * 2. Convert to Quickwit split
 * 3. Upload to Azure Blob Storage
 * 4. Search split with various aggregations
 * 5. Validate aggregation results
 *
 * <p><b>NOTE:</b> These tests require real Azure Blob Storage credentials.
 * Azurite (local emulator) cannot be used because custom endpoint configuration
 * was removed in January 2025. Configure Azure credentials via system properties:
 * <pre>
 * -Dtest.azure.container=your-container
 * -Dtest.azure.storageAccount=youraccount
 * -Dtest.azure.accountKey=yourkey
 * </pre>
 * Or via ~/.azure/credentials file. Tests will be skipped if credentials are not available.
 */
public class AzureAggregationTest {

    // Test configuration from system properties
    private static final String AZURE_CONTAINER = System.getProperty("test.azure.container", "tantivy4java-test");
    private static final String STORAGE_ACCOUNT_PROP = System.getProperty("test.azure.storageAccount");
    private static final String ACCOUNT_KEY_PROP = System.getProperty("test.azure.accountKey");

    // Azure credentials loaded from ~/.azure/credentials
    private static String azureStorageAccount;
    private static String azureAccountKey;

    private static BlobServiceClient blobServiceClient;
    private static BlobContainerClient containerClient;

    private SplitCacheManager cacheManager;
    private SplitSearcher searcher;
    private String azureSplitUrl;
    private QuickwitSplit.SplitMetadata metadata;

    /**
     * Gets the effective storage account from various sources
     */
    private static String getStorageAccount() {
        if (STORAGE_ACCOUNT_PROP != null) return STORAGE_ACCOUNT_PROP;
        if (azureStorageAccount != null) return azureStorageAccount;
        return null;
    }

    /**
     * Gets the effective account key from various sources
     */
    private static String getAccountKey() {
        if (ACCOUNT_KEY_PROP != null) return ACCOUNT_KEY_PROP;
        if (azureAccountKey != null) return azureAccountKey;
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
                System.out.println("✅ Loaded Azure credentials from ~/.azure/credentials");
            }
        } catch (Exception e) {
            System.out.println("Failed to read Azure credentials from ~/.azure/credentials: " + e.getMessage());
        }
    }

    @BeforeAll
    public static void checkAzureCredentials() {
        System.out.println("=== AZURE AGGREGATION TEST ===");
        System.out.println("Test container: " + AZURE_CONTAINER);

        // Try to load credentials in order of preference:
        // 1. System properties (test.azure.storageAccount, test.azure.accountKey)
        // 2. ~/.azure/credentials file
        // 3. Environment variables (AZURE_STORAGE_ACCOUNT, AZURE_STORAGE_KEY)

        boolean hasExplicitCreds = STORAGE_ACCOUNT_PROP != null && ACCOUNT_KEY_PROP != null;
        if (!hasExplicitCreds) {
            loadAzureCredentials();
        }

        boolean hasFileCredentials = azureStorageAccount != null && azureAccountKey != null;
        boolean hasEnvCreds = System.getenv("AZURE_STORAGE_ACCOUNT") != null;

        if (!hasExplicitCreds && !hasFileCredentials && !hasEnvCreds) {
            System.out.println("⚠️  No Azure credentials found. Skipping Azure aggregation tests.");
            System.out.println("   Set -Dtest.azure.storageAccount and -Dtest.azure.accountKey");
            System.out.println("   Or configure ~/.azure/credentials file");
            System.out.println("   Or configure AZURE_STORAGE_ACCOUNT/AZURE_STORAGE_KEY environment variables");
            Assumptions.assumeTrue(false, "Azure credentials not available");
            return;
        }

        if (hasExplicitCreds) {
            System.out.println("✅ Using Azure credentials from system properties");
        } else if (hasFileCredentials) {
            System.out.println("✅ Using Azure credentials from ~/.azure/credentials");
        } else {
            System.out.println("✅ Using environment Azure credentials");
        }

        // Create Azure client
        String storageAccount = getStorageAccount();
        String accountKey = getAccountKey();

        if (storageAccount != null && accountKey != null) {
            try {
                blobServiceClient = new BlobServiceClientBuilder()
                    .endpoint("https://" + storageAccount + ".blob.core.windows.net")
                    .credential(new StorageSharedKeyCredential(storageAccount, accountKey))
                    .buildClient();

                containerClient = blobServiceClient.getBlobContainerClient(AZURE_CONTAINER);

                // Create container if it doesn't exist
                if (!containerClient.exists()) {
                    containerClient.create();
                    System.out.println("✅ Created Azure container: " + AZURE_CONTAINER);
                } else {
                    System.out.println("✅ Using existing Azure container: " + AZURE_CONTAINER);
                }

                System.out.println("✅ Azure client initialized successfully");
            } catch (Exception e) {
                System.err.println("Failed to connect to Azure: " + e.getMessage());
                Assumptions.assumeTrue(false, "Failed to connect to Azure: " + e.getMessage());
            }
        } else {
            Assumptions.assumeTrue(false, "Azure credentials not found");
        }
    }

    @AfterEach
    public void tearDown() {
        if (searcher != null) {
            try {
                searcher.close();
            } catch (Exception e) {
                // Ignore cleanup errors
            }
        }
        if (cacheManager != null) {
            try {
                cacheManager.close();
            } catch (Exception e) {
                // Ignore cleanup errors
            }
        }
    }

    @BeforeEach
    public void setUp(@TempDir Path tempDir) throws Exception {

        // Create test index with numeric data for aggregations
        String indexPath = tempDir.resolve("azure_agg_index").toString();
        String localSplitPath = tempDir.resolve("azure_agg.split").toString();

        try (SchemaBuilder builder = new SchemaBuilder()) {
            builder
                .addTextField("product_name", true, false, "default", "position")
                .addTextField("category", true, true, "default", "position")  // Fast field for terms aggregation
                .addIntegerField("price", true, true, true)          // For sum, avg, min, max
                .addIntegerField("quantity", true, true, true)       // For count and other stats
                .addIntegerField("rating", true, true, true)         // For additional aggregations
                .addIntegerField("id", true, true, true);

            try (Schema schema = builder.build()) {
                try (Index index = new Index(schema, indexPath, false)) {
                    try (IndexWriter writer = index.writer(Index.Memory.DEFAULT_HEAP_SIZE, 1)) {

                        // Add test products with varying numeric values
                        addProduct(writer, 1, "Laptop", "electronics", 1200, 5, 5);
                        addProduct(writer, 2, "Mouse", "electronics", 25, 50, 4);
                        addProduct(writer, 3, "Keyboard", "electronics", 75, 20, 4);
                        addProduct(writer, 4, "Monitor", "electronics", 300, 10, 5);
                        addProduct(writer, 5, "Desk", "furniture", 450, 8, 3);
                        addProduct(writer, 6, "Chair", "furniture", 200, 15, 4);
                        addProduct(writer, 7, "Lamp", "furniture", 50, 25, 4);
                        addProduct(writer, 8, "Book", "books", 15, 100, 5);
                        addProduct(writer, 9, "Pen", "books", 2, 500, 3);
                        addProduct(writer, 10, "Notebook", "books", 5, 200, 4);

                        writer.commit();
                    }

                    // Convert to QuickwitSplit
                    QuickwitSplit.SplitConfig config = new QuickwitSplit.SplitConfig(
                        "azure-agg-test", "test-source", "test-node");

                    metadata = QuickwitSplit.convertIndexFromPath(indexPath, localSplitPath, config);
                    System.out.println("✅ Created split with " + metadata.getNumDocs() + " documents");
                }
            }
        }

        // Upload split to Azure Blob Storage
        String blobName = "splits/azure_agg_" + System.currentTimeMillis() + ".split";
        BlobClient blobClient = containerClient.getBlobClient(blobName);
        blobClient.uploadFromFile(localSplitPath, true);
        System.out.println("✅ Uploaded split to Azure: " + blobName);

        // Get Azure URL for the split
        azureSplitUrl = String.format("azure://%s/%s", AZURE_CONTAINER, blobName);
        System.out.println("✅ Azure split URL: " + azureSplitUrl);

        // Create cache manager with Azure credentials
        String uniqueCacheName = "azure-agg-cache-" + System.nanoTime();
        String storageAccount = getStorageAccount();
        String accountKey = getAccountKey();

        SplitCacheManager.CacheConfig cacheConfig =
            new SplitCacheManager.CacheConfig(uniqueCacheName)
                .withMaxCacheSize(100_000_000)
                .withAzureCredentials(storageAccount, accountKey);

        cacheManager = SplitCacheManager.getInstance(cacheConfig);

        // Create searcher for Azure split
        searcher = cacheManager.createSplitSearcher(azureSplitUrl, metadata);
        System.out.println("✅ SplitSearcher created for Azure split");
    }

    private void addProduct(IndexWriter writer, int id, String name, String category,
                          int price, int quantity, int rating) throws Exception {
        try (Document doc = new Document()) {
            doc.addInteger("id", id);
            doc.addText("product_name", name);
            doc.addText("category", category);
            doc.addInteger("price", price);
            doc.addInteger("quantity", quantity);
            doc.addInteger("rating", rating);
            writer.addDocument(doc);
        }
    }

    @Test
    @DisplayName("Test stats aggregation (count, sum, avg, min, max) with Azure")
    public void testStatsAggregationWithAzure() {
        System.out.println("\n🧮 Testing stats aggregation with Azure Blob Storage...");

        // Create stats aggregation for price field
        StatsAggregation priceStats = new StatsAggregation("price_stats", "price");
        SplitQuery matchAllQuery = new SplitMatchAllQuery();

        // Perform aggregation on Azure-backed split
        SearchResult result = searcher.search(matchAllQuery, 10, "stats", priceStats);

        // Verify aggregation result
        assertTrue(result.hasAggregations(), "Result should contain aggregations");
        StatsResult stats = (StatsResult) result.getAggregation("stats");
        assertNotNull(stats, "Stats aggregation result should not be null");

        // Verify stats values
        // Price data: 1200, 25, 75, 300, 450, 200, 50, 15, 2, 5
        assertEquals(10, stats.getCount(), "Count should be 10");
        assertEquals(2322.0, stats.getSum(), 0.01, "Sum should be 2322");
        assertEquals(232.2, stats.getAverage(), 0.01, "Average should be 232.2");
        assertEquals(2.0, stats.getMin(), 0.01, "Min should be 2");
        assertEquals(1200.0, stats.getMax(), 0.01, "Max should be 1200");

        System.out.println("✅ Stats aggregation from Azure: " + stats);
        System.out.println("   Count: " + stats.getCount());
        System.out.println("   Sum: " + stats.getSum());
        System.out.println("   Average: " + stats.getAverage());
        System.out.println("   Min: " + stats.getMin());
        System.out.println("   Max: " + stats.getMax());
    }

    @Test
    @DisplayName("Test individual aggregations (sum, avg, count, min, max) with Azure")
    public void testIndividualAggregationsWithAzure() {
        System.out.println("\n🔢 Testing individual aggregations with Azure...");

        SplitQuery matchAllQuery = new SplitMatchAllQuery();

        // Test Count Aggregation
        CountAggregation countAgg = new CountAggregation("product_count", "id");
        SearchResult countResult = searcher.search(matchAllQuery, 10, "count", countAgg);
        CountResult count = (CountResult) countResult.getAggregation("count");
        assertNotNull(count, "Count result should not be null");
        assertEquals(10, count.getCount(), "Product count should be 10");
        System.out.println("✅ Count from Azure: " + count.getCount());

        // Test Sum Aggregation
        SumAggregation sumAgg = new SumAggregation("quantity_sum", "quantity");
        SearchResult sumResult = searcher.search(matchAllQuery, 10, "sum", sumAgg);
        SumResult sum = (SumResult) sumResult.getAggregation("sum");
        assertNotNull(sum, "Sum result should not be null");
        // Quantity: 5, 50, 20, 10, 8, 15, 25, 100, 500, 200 = 933
        assertEquals(933.0, sum.getSum(), 0.01, "Sum should be 933");
        System.out.println("✅ Sum from Azure: " + sum.getSum());

        // Test Average Aggregation
        AverageAggregation avgAgg = new AverageAggregation("rating_avg", "rating");
        SearchResult avgResult = searcher.search(matchAllQuery, 10, "avg", avgAgg);
        AverageResult avg = (AverageResult) avgResult.getAggregation("avg");
        assertNotNull(avg, "Average result should not be null");
        // Rating: 5, 4, 4, 5, 3, 4, 4, 5, 3, 4 = 41/10 = 4.1
        assertEquals(4.1, avg.getAverage(), 0.01, "Average should be 4.1");
        System.out.println("✅ Average from Azure: " + avg.getAverage());

        // Test Min Aggregation
        MinAggregation minAgg = new MinAggregation("price_min", "price");
        SearchResult minResult = searcher.search(matchAllQuery, 10, "min", minAgg);
        MinResult min = (MinResult) minResult.getAggregation("min");
        assertNotNull(min, "Min result should not be null");
        assertEquals(2.0, min.getMin(), 0.01, "Min price should be 2");
        System.out.println("✅ Min from Azure: " + min.getMin());

        // Test Max Aggregation
        MaxAggregation maxAgg = new MaxAggregation("price_max", "price");
        SearchResult maxResult = searcher.search(matchAllQuery, 10, "max", maxAgg);
        MaxResult max = (MaxResult) maxResult.getAggregation("max");
        assertNotNull(max, "Max result should not be null");
        assertEquals(1200.0, max.getMax(), 0.01, "Max price should be 1200");
        System.out.println("✅ Max from Azure: " + max.getMax());
    }

    @Test
    @DisplayName("Test multiple aggregations in single search with Azure")
    public void testMultipleAggregationsWithAzure() {
        System.out.println("\n📊 Testing multiple aggregations with Azure...");

        SplitQuery matchAllQuery = new SplitMatchAllQuery();

        // Create multiple aggregations
        Map<String, SplitAggregation> aggregations = new HashMap<>();
        aggregations.put("price_stats", new StatsAggregation("price"));
        aggregations.put("quantity_sum", new SumAggregation("quantity"));
        aggregations.put("rating_avg", new AverageAggregation("rating"));
        aggregations.put("product_count", new CountAggregation("id"));

        // Perform search with multiple aggregations on Azure-backed split
        SearchResult result = searcher.search(matchAllQuery, 10, aggregations);

        // Verify all aggregations are present
        assertTrue(result.hasAggregations(), "Result should contain aggregations");
        assertEquals(4, result.getAggregations().size(), "Should have 4 aggregations");

        // Verify price stats
        StatsResult priceStats = (StatsResult) result.getAggregation("price_stats");
        assertNotNull(priceStats, "Price stats should not be null");
        assertEquals(10, priceStats.getCount(), "Price count should be 10");
        assertEquals(232.2, priceStats.getAverage(), 0.01, "Price average should be 232.2");

        // Verify quantity sum
        SumResult quantitySum = (SumResult) result.getAggregation("quantity_sum");
        assertNotNull(quantitySum, "Quantity sum should not be null");
        assertEquals(933.0, quantitySum.getSum(), 0.01, "Quantity sum should be 933");

        // Verify rating average
        AverageResult ratingAvg = (AverageResult) result.getAggregation("rating_avg");
        assertNotNull(ratingAvg, "Rating average should not be null");
        assertEquals(4.1, ratingAvg.getAverage(), 0.01, "Rating average should be 4.1");

        // Verify product count
        CountResult productCount = (CountResult) result.getAggregation("product_count");
        assertNotNull(productCount, "Product count should not be null");
        assertEquals(10, productCount.getCount(), "Product count should be 10");

        System.out.println("✅ Multiple aggregations from Azure completed successfully");
        System.out.println("   Price stats: count=" + priceStats.getCount() + ", avg=" + priceStats.getAverage());
        System.out.println("   Quantity sum: " + quantitySum.getSum());
        System.out.println("   Rating avg: " + ratingAvg.getAverage());
        System.out.println("   Product count: " + productCount.getCount());
    }

    @Test
    @DisplayName("Test terms aggregation with Azure")
    public void testTermsAggregationWithAzure() {
        System.out.println("\n📊 Testing terms aggregation with Azure...");

        SplitQuery matchAllQuery = new SplitMatchAllQuery();
        TermsAggregation categoryTerms = new TermsAggregation("categories", "category", 10, 0);

        SearchResult result = searcher.search(matchAllQuery, 10, "terms", categoryTerms);

        assertTrue(result.hasAggregations(), "Result should contain aggregations");
        TermsResult terms = (TermsResult) result.getAggregation("terms");
        assertNotNull(terms, "Terms aggregation result should not be null");

        // Verify buckets: electronics (4), furniture (3), books (3)
        assertEquals(3, terms.getBuckets().size(), "Should have 3 category buckets");

        // Find specific buckets and verify counts
        Map<String, Long> bucketCounts = new HashMap<>();
        for (TermsResult.TermsBucket bucket : terms.getBuckets()) {
            bucketCounts.put(bucket.getKeyAsString(), bucket.getDocCount());
        }

        assertEquals(4L, bucketCounts.get("electronics"), "Electronics should have 4 products");
        assertEquals(3L, bucketCounts.get("furniture"), "Furniture should have 3 products");
        assertEquals(3L, bucketCounts.get("books"), "Books should have 3 products");

        System.out.println("✅ Terms aggregation from Azure: " + terms);
        for (TermsResult.TermsBucket bucket : terms.getBuckets()) {
            System.out.println("   " + bucket.getKeyAsString() + ": " + bucket.getDocCount() + " docs");
        }
    }

    @Test
    @DisplayName("Test aggregations with filtered query on Azure")
    public void testAggregationsWithFilterOnAzure() {
        System.out.println("\n🔍 Testing aggregations with filtered query on Azure...");

        // Create query for electronics category only
        SplitQuery electronicsQuery = searcher.parseQuery("electronics", "category");
        StatsAggregation priceStats = new StatsAggregation("price");

        // Perform filtered aggregation
        SearchResult result = searcher.search(electronicsQuery, 10, "stats", priceStats);

        // Verify aggregation on filtered data
        StatsResult stats = (StatsResult) result.getAggregation("stats");
        assertNotNull(stats, "Stats should not be null");

        // Electronics: Laptop (1200), Mouse (25), Keyboard (75), Monitor (300)
        assertEquals(4, stats.getCount(), "Should have 4 electronics products");
        assertEquals(1600.0, stats.getSum(), 0.01, "Electronics sum should be 1600");
        assertEquals(400.0, stats.getAverage(), 0.01, "Electronics average should be 400");
        assertEquals(25.0, stats.getMin(), 0.01, "Electronics min should be 25");
        assertEquals(1200.0, stats.getMax(), 0.01, "Electronics max should be 1200");

        System.out.println("✅ Filtered aggregation from Azure (electronics): " + stats);
        System.out.println("   Count: " + stats.getCount());
        System.out.println("   Sum: " + stats.getSum());
        System.out.println("   Average: " + stats.getAverage());
        System.out.println("   Min: " + stats.getMin());
        System.out.println("   Max: " + stats.getMax());
    }

    @Test
    @DisplayName("Test aggregation-only search with Azure (no document hits)")
    public void testAggregationOnlySearchWithAzure() {
        System.out.println("\n🎯 Testing aggregation-only search with Azure...");

        SplitQuery matchAllQuery = new SplitMatchAllQuery();
        StatsAggregation priceStats = new StatsAggregation("price");

        // Perform aggregation-only search
        SearchResult result = searcher.aggregate(matchAllQuery, "stats", priceStats);

        // Verify no hits but aggregations present
        assertEquals(0, result.getHits().size(), "Should have no document hits");
        assertTrue(result.hasAggregations(), "Should have aggregations");

        StatsResult stats = (StatsResult) result.getAggregation("stats");
        assertNotNull(stats, "Stats should not be null");
        assertEquals(10, stats.getCount(), "Count should still be 10");
        assertEquals(232.2, stats.getAverage(), 0.01, "Average should still be 232.2");

        System.out.println("✅ Aggregation-only search from Azure: " + stats);
        System.out.println("   Document hits: " + result.getHits().size());
        System.out.println("   Aggregation count: " + stats.getCount());
    }

    @Test
    @DisplayName("Test aggregations with different numeric fields on Azure")
    public void testAggregationsOnDifferentFieldsWithAzure() {
        System.out.println("\n🔢 Testing aggregations on different numeric fields with Azure...");

        SplitQuery matchAllQuery = new SplitMatchAllQuery();

        // Test stats on price field
        StatsAggregation priceStats = new StatsAggregation("price_stats", "price");
        SearchResult priceResult = searcher.search(matchAllQuery, 0, "price_stats", priceStats);
        StatsResult priceStatsResult = (StatsResult) priceResult.getAggregation("price_stats");
        assertEquals(2322.0, priceStatsResult.getSum(), 0.01, "Price sum should be 2322");

        // Test stats on quantity field
        StatsAggregation quantityStats = new StatsAggregation("quantity_stats", "quantity");
        SearchResult quantityResult = searcher.search(matchAllQuery, 0, "quantity_stats", quantityStats);
        StatsResult quantityStatsResult = (StatsResult) quantityResult.getAggregation("quantity_stats");
        assertEquals(933.0, quantityStatsResult.getSum(), 0.01, "Quantity sum should be 933");

        // Test stats on rating field
        StatsAggregation ratingStats = new StatsAggregation("rating_stats", "rating");
        SearchResult ratingResult = searcher.search(matchAllQuery, 0, "rating_stats", ratingStats);
        StatsResult ratingStatsResult = (StatsResult) ratingResult.getAggregation("rating_stats");
        assertEquals(4.1, ratingStatsResult.getAverage(), 0.01, "Rating average should be 4.1");

        System.out.println("✅ Aggregations on different fields from Azure:");
        System.out.println("   Price sum: " + priceStatsResult.getSum());
        System.out.println("   Quantity sum: " + quantityStatsResult.getSum());
        System.out.println("   Rating avg: " + ratingStatsResult.getAverage());
    }
}

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

import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;

import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.BlobServiceClientBuilder;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobClient;
import com.azure.storage.common.StorageSharedKeyCredential;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests the TERM prewarm feature with real Azure Blob Storage.
 *
 * This test verifies that preloading the TERM component (FST/term dictionaries)
 * eliminates cache misses when querying different terms in the same field.
 *
 * Prerequisites:
 * - Azure credentials configured in ~/.azure/credentials
 *   [default]
 *   storage_account=yourstorageaccount
 *   account_key=your-account-key
 *
 * Run with: TANTIVY4JAVA_DEBUG=1 to see detailed cache behavior
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class RealAzurePrewarmTest {

    // Test configuration
    private static final String TEST_CONTAINER = System.getProperty("test.azure.container", "tantivy4java-test");
    private static final String STORAGE_ACCOUNT = System.getProperty("test.azure.storageAccount");
    private static final String ACCOUNT_KEY = System.getProperty("test.azure.accountKey");

    // Number of unique terms to generate
    private static final int NUM_UNIQUE_TERMS = 10_000;
    private static final int NUM_DOCUMENTS = 5_000;

    // Azure credentials
    private static String azureStorageAccount;
    private static String azureAccountKey;

    @TempDir
    static Path tempDir;

    private static BlobServiceClient blobServiceClient;
    private static String splitAzureUrl;
    private static QuickwitSplit.SplitMetadata splitMetadata;
    private static List<String> generatedTerms = new ArrayList<>();
    private static Random random = new Random(42);

    @BeforeAll
    static void setup() {
        loadAzureCredentials();

        String storageAccount = getStorageAccount();
        String accountKey = getAccountKey();

        if (storageAccount == null || accountKey == null) {
            System.out.println("No Azure credentials found. Skipping Azure prewarm tests.");
            Assumptions.abort("Azure credentials not available");
        }

        blobServiceClient = new BlobServiceClientBuilder()
            .endpoint("https://" + storageAccount + ".blob.core.windows.net")
            .credential(new StorageSharedKeyCredential(storageAccount, accountKey))
            .buildClient();

        System.out.println("Azure client initialized for prewarm test");
        System.out.println("   Storage Account: " + storageAccount);
        System.out.println("   Container: " + TEST_CONTAINER);

        // Ensure container exists
        BlobContainerClient containerClient = blobServiceClient.getBlobContainerClient(TEST_CONTAINER);
        if (!containerClient.exists()) {
            containerClient.create();
            System.out.println("Created Azure container: " + TEST_CONTAINER);
        }
    }

    @AfterAll
    static void cleanup() {
        // Azure client doesn't need explicit close
        System.out.println("Azure client cleanup complete");
    }

    @Test
    @Order(1)
    @DisplayName("Step 1: Create index with diverse terms for prewarm testing")
    void step1_createIndex() throws IOException {
        System.out.println("Creating index with " + NUM_UNIQUE_TERMS + " unique terms...");

        // Generate unique random terms
        Set<String> termSet = new HashSet<>();
        while (termSet.size() < NUM_UNIQUE_TERMS) {
            termSet.add(generateRandomTerm());
        }
        generatedTerms = new ArrayList<>(termSet);
        Collections.sort(generatedTerms);

        System.out.println("   Generated " + generatedTerms.size() + " unique terms");

        Path indexPath = tempDir.resolve("azure-prewarm-test-index");

        try (SchemaBuilder schemaBuilder = new SchemaBuilder()) {
            // Multiple text fields to test per-field FST behavior
            schemaBuilder.addTextField("content", true, false, "default", "position");
            schemaBuilder.addTextField("title", true, false, "default", "position");
            schemaBuilder.addTextField("tags", true, false, "default", "position");
            schemaBuilder.addIntegerField("doc_id", true, true, true);

            try (Schema schema = schemaBuilder.build()) {
                try (Index index = new Index(schema, indexPath.toString(), false)) {
                    try (IndexWriter writer = index.writer(Index.Memory.LARGE_HEAP_SIZE, 1)) {

                        for (int i = 0; i < NUM_DOCUMENTS; i++) {
                            try (Document doc = new Document()) {
                                // Content with multiple random terms
                                StringBuilder content = new StringBuilder();
                                int numTerms = 5 + random.nextInt(15);
                                for (int j = 0; j < numTerms; j++) {
                                    content.append(generatedTerms.get(random.nextInt(generatedTerms.size()))).append(" ");
                                }
                                doc.addText("content", content.toString().trim());

                                // Title with 2-4 random terms
                                StringBuilder title = new StringBuilder();
                                int numTitleTerms = 2 + random.nextInt(3);
                                for (int j = 0; j < numTitleTerms; j++) {
                                    title.append(generatedTerms.get(random.nextInt(generatedTerms.size()))).append(" ");
                                }
                                doc.addText("title", title.toString().trim());

                                // Tags with 1-3 random terms
                                StringBuilder tags = new StringBuilder();
                                int numTags = 1 + random.nextInt(3);
                                for (int j = 0; j < numTags; j++) {
                                    tags.append(generatedTerms.get(random.nextInt(generatedTerms.size()))).append(" ");
                                }
                                doc.addText("tags", tags.toString().trim());

                                doc.addInteger("doc_id", i);
                                writer.addDocument(doc);
                            }
                        }

                        writer.commit();
                    }

                    index.reload();
                    System.out.println("Index created with " + NUM_DOCUMENTS + " documents");
                }
            }
        }

        // Convert to split
        Path splitPath = tempDir.resolve("azure-prewarm-test.split");
        QuickwitSplit.SplitConfig config = new QuickwitSplit.SplitConfig(
            "azure-prewarm-test-index", "test-source", "test-node"
        );

        splitMetadata = QuickwitSplit.convertIndexFromPath(
            indexPath.toString(), splitPath.toString(), config
        );

        System.out.println("Split created: " + splitMetadata.getSplitId());
        System.out.println("   Documents: " + splitMetadata.getNumDocs());
        System.out.println("   Size: " + Files.size(splitPath) + " bytes");
    }

    @Test
    @Order(2)
    @DisplayName("Step 2: Upload split to Azure Blob Storage")
    void step2_uploadToAzure() throws IOException {
        Path localSplitPath = tempDir.resolve("azure-prewarm-test.split");
        assertTrue(Files.exists(localSplitPath), "Split file should exist from Step 1");

        String blobName = "prewarm-test/azure-prewarm-" + System.currentTimeMillis() + ".split";
        splitAzureUrl = String.format("azure://%s/%s", TEST_CONTAINER, blobName);

        System.out.println("Uploading split to Azure: " + splitAzureUrl);

        BlobContainerClient containerClient = blobServiceClient.getBlobContainerClient(TEST_CONTAINER);
        BlobClient blobClient = containerClient.getBlobClient(blobName);
        blobClient.uploadFromFile(localSplitPath.toString(), true);

        System.out.println("Split uploaded: " + blobClient.getProperties().getBlobSize() + " bytes");
    }

    @Test
    @Order(3)
    @DisplayName("Step 3: Test TERM prewarm eliminates cache misses on Azure")
    void step3_testTermPrewarm() throws Exception {
        System.out.println("\n" + "=".repeat(70));
        System.out.println("TESTING TERM PREWARM WITH AZURE");
        System.out.println("=".repeat(70));

        assertNotNull(splitAzureUrl, "Split Azure URL should be available");
        assertNotNull(splitMetadata, "Split metadata should be available");
        assertTrue(generatedTerms.size() > 0, "Generated terms should be available");

        // Create cache manager with fresh cache
        String cacheName = "azure-prewarm-test-" + System.currentTimeMillis();
        SplitCacheManager.CacheConfig cacheConfig = new SplitCacheManager.CacheConfig(cacheName)
            .withMaxCacheSize(500_000_000);

        String storageAccount = getStorageAccount();
        String accountKey = getAccountKey();
        if (storageAccount != null && accountKey != null) {
            cacheConfig = cacheConfig.withAzureCredentials(storageAccount, accountKey);
        }

        SplitCacheManager cacheManager = SplitCacheManager.getInstance(cacheConfig);

        // Select test terms from different parts of the FST
        List<String> testTerms = Arrays.asList(
            generatedTerms.get(0),
            generatedTerms.get(generatedTerms.size() / 4),
            generatedTerms.get(generatedTerms.size() / 2),
            generatedTerms.get(3 * generatedTerms.size() / 4),
            generatedTerms.get(generatedTerms.size() - 1)
        );

        try (SplitSearcher searcher = cacheManager.createSplitSearcher(splitAzureUrl, splitMetadata)) {

            // Get initial stats
            var initialStats = searcher.getCacheStats();
            System.out.println("Initial cache: hits=" + initialStats.getHitCount() +
                             ", misses=" + initialStats.getMissCount());

            // PREWARM the TERM component
            System.out.println("\nPrewarming TERM component...");
            long prewarmStart = System.nanoTime();
            searcher.preloadComponents(SplitSearcher.IndexComponent.TERM).join();
            long prewarmTimeMs = (System.nanoTime() - prewarmStart) / 1_000_000;

            var afterPrewarmStats = searcher.getCacheStats();
            System.out.println("Prewarm completed in " + prewarmTimeMs + " ms");
            System.out.println("Cache after prewarm: size=" + afterPrewarmStats.getTotalSize() + " bytes");

            // Query different terms - should have ZERO new cache misses
            System.out.println("\nQuerying different terms (should have zero cache misses):");
            long missesBeforeQueries = afterPrewarmStats.getMissCount();
            int passCount = 0;
            int failCount = 0;

            for (String term : testTerms) {
                var statsBefore = searcher.getCacheStats();
                long missesBefore = statsBefore.getMissCount();

                SplitQuery query = new SplitTermQuery("content", term);
                SearchResult result = searcher.search(query, 10);

                var statsAfter = searcher.getCacheStats();
                long newMisses = statsAfter.getMissCount() - missesBefore;

                String status = newMisses == 0 ? "PASS" : "FAIL";
                if (newMisses == 0) passCount++; else failCount++;

                System.out.println("   Term \"" + term.substring(0, Math.min(15, term.length())) + "...\" " +
                                 "-> hits=" + result.getHits().size() + ", new_misses=" + newMisses + " [" + status + "]");
            }

            // Test different field (title)
            var statsBeforeTitle = searcher.getCacheStats();
            SplitQuery titleQuery = new SplitTermQuery("title", testTerms.get(0));
            searcher.search(titleQuery, 10);
            var statsAfterTitle = searcher.getCacheStats();
            long titleMisses = statsAfterTitle.getMissCount() - statsBeforeTitle.getMissCount();

            String titleStatus = titleMisses == 0 ? "PASS" : "FAIL";
            if (titleMisses == 0) passCount++; else failCount++;
            System.out.println("   Title field query -> new_misses=" + titleMisses + " [" + titleStatus + "]");

            // Summary
            var finalStats = searcher.getCacheStats();
            long totalNewMisses = finalStats.getMissCount() - missesBeforeQueries;

            System.out.println("\n" + "=".repeat(70));
            System.out.println("PREWARM TEST SUMMARY");
            System.out.println("=".repeat(70));
            System.out.println("   Prewarm time: " + prewarmTimeMs + " ms");
            System.out.println("   Total queries: " + (testTerms.size() + 1));
            System.out.println("   Passed: " + passCount);
            System.out.println("   Failed: " + failCount);
            System.out.println("   New cache misses after prewarm: " + totalNewMisses);

            if (totalNewMisses == 0) {
                System.out.println("\n   SUCCESS: TERM prewarm eliminated all FST cache misses!");
            }

            // Assert success
            assertEquals(0, totalNewMisses,
                "After TERM prewarm, queries should have zero new cache misses for FST lookups");
        }
    }

    // Helper methods

    private static String generateRandomTerm() {
        int length = 8 + random.nextInt(8);
        StringBuilder sb = new StringBuilder(length);
        for (int i = 0; i < length; i++) {
            sb.append((char) ('a' + random.nextInt(26)));
        }
        return sb.toString();
    }

    private static String getStorageAccount() {
        if (STORAGE_ACCOUNT != null) return STORAGE_ACCOUNT;
        if (azureStorageAccount != null) return azureStorageAccount;
        return null;
    }

    private static String getAccountKey() {
        if (ACCOUNT_KEY != null) return ACCOUNT_KEY;
        if (azureAccountKey != null) return azureAccountKey;
        return null;
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
                System.out.println("Loaded Azure credentials from ~/.azure/credentials");
            }
        } catch (Exception e) {
            System.out.println("Failed to read Azure credentials: " + e.getMessage());
        }
    }
}

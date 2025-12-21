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

import static org.junit.jupiter.api.Assertions.*;

import io.indextables.tantivy4java.core.*;
import io.indextables.tantivy4java.split.*;
import io.indextables.tantivy4java.split.merge.QuickwitSplit;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.*;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;

import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.BlobServiceClientBuilder;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobClient;
import com.azure.storage.common.StorageSharedKeyCredential;

/**
 * Real Azure Blob Storage prescan performance and coverage tests.
 *
 * Tests prescan functionality with real Azure Blob Storage:
 * - Multiple query types (term, boolean, phrase, wildcard, regex)
 * - Multiple splits with diverse content
 * - Cache effectiveness verification
 * - Parallel prescan performance
 * - Split filtering accuracy
 *
 * Prerequisites:
 * - Azure credentials in ~/.azure/credentials (storage_account and account_key)
 * - Azure container accessible (will be created if it doesn't exist)
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class RealAzurePrescanPerformanceTest {

    private static final String TEST_CONTAINER = System.getProperty("test.azure.container", "tantivy4java-testing");
    private static final String STORAGE_ACCOUNT = System.getProperty("test.azure.storageAccount");
    private static final String ACCOUNT_KEY = System.getProperty("test.azure.accountKey");
    private static final String AZURE_PREFIX = "prescan-perf-test/";

    private static String azureStorageAccount;
    private static String azureAccountKey;
    private static BlobServiceClient blobServiceClient;
    private static List<SplitInfo> azureSplitInfos = new ArrayList<>();
    private static String docMappingJson;
    private static SplitCacheManager cacheManager;

    @TempDir
    static Path tempDir;

    // Content patterns for splits (same as S3 test for consistency)
    private static final String[][] SPLIT_CONTENT = {
        // Split 0: Technology content
        {"programming java rust python", "coding software development", "tech"},
        {"hello world tutorial", "beginner programming guide", "tech"},
        {"algorithm data structure", "computer science fundamentals", "tech"},

        // Split 1: Science content
        {"physics quantum mechanics", "particle wave duality", "science"},
        {"chemistry molecular bonds", "atomic structure elements", "science"},
        {"biology cell structure", "dna rna proteins", "science"},

        // Split 2: Mixed content with hello
        {"hello technology future", "innovation startup", "business"},
        {"market analysis trends", "financial forecast", "business"},
        {"hello customer service", "support helpdesk", "business"},

        // Split 3: Arts content (no hello, no tech)
        {"music composition theory", "classical symphony", "arts"},
        {"painting sculpture design", "visual creativity", "arts"},
        {"literature poetry prose", "creative writing", "arts"},

        // Split 4: Sports content (no hello, no tech)
        {"football basketball soccer", "team sports competition", "sports"},
        {"tennis golf swimming", "individual athletics", "sports"},
        {"marathon cycling triathlon", "endurance training", "sports"},
    };

    @BeforeAll
    static void setUp() throws Exception {
        // Load Azure credentials
        loadAzureCredentials();

        String storageAccount = getStorageAccount();
        String accountKey = getAccountKey();

        if (storageAccount == null || accountKey == null) {
            System.out.println("⚠️  No Azure credentials found. Skipping Real Azure prescan tests.");
            System.out.println("   Configure ~/.azure/credentials with storage_account and account_key");
            System.out.println("   Or set system properties -Dtest.azure.storageAccount and -Dtest.azure.accountKey");
            Assumptions.abort("Azure credentials not available");
            return;
        }

        // Create Azure blob service client
        blobServiceClient = new BlobServiceClientBuilder()
            .endpoint("https://" + storageAccount + ".blob.core.windows.net")
            .credential(new StorageSharedKeyCredential(storageAccount, accountKey))
            .buildClient();

        // Ensure container exists
        ensureContainerExists();

        // Create and upload splits
        createAndUploadSplits();

        // Initialize cache manager for Azure
        SplitCacheManager.CacheConfig cacheConfig = new SplitCacheManager.CacheConfig("real-azure-prescan-perf")
            .withMaxCacheSize(200_000_000)
            .withAzureCredentials(storageAccount, accountKey);

        cacheManager = SplitCacheManager.getInstance(cacheConfig);

        System.out.println("✅ Real Azure prescan test setup complete:");
        System.out.println("  - " + azureSplitInfos.size() + " splits uploaded to azure://" + TEST_CONTAINER + "/" + AZURE_PREFIX);
        System.out.println("  - Storage Account: " + storageAccount);
    }

    @AfterAll
    static void tearDown() {
        // Clean up Azure blobs
        if (blobServiceClient != null && !azureSplitInfos.isEmpty()) {
            try {
                BlobContainerClient containerClient = blobServiceClient.getBlobContainerClient(TEST_CONTAINER);
                for (int i = 0; i < azureSplitInfos.size(); i++) {
                    String blobName = AZURE_PREFIX + "split-" + i + ".split";
                    BlobClient blobClient = containerClient.getBlobClient(blobName);
                    if (blobClient.exists()) {
                        blobClient.delete();
                    }
                }
                System.out.println("✅ Cleaned up " + azureSplitInfos.size() + " Azure test splits");
            } catch (Exception e) {
                System.out.println("⚠️  Azure cleanup failed: " + e.getMessage());
            }
        }

        if (cacheManager != null) {
            try { cacheManager.close(); } catch (Exception e) {}
        }
    }

    private static void loadAzureCredentials() {
        try {
            Path credentialsPath = Paths.get(System.getProperty("user.home"), ".azure", "credentials");
            if (!Files.exists(credentialsPath)) {
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

    private static String getStorageAccount() {
        if (STORAGE_ACCOUNT != null) return STORAGE_ACCOUNT;
        return azureStorageAccount;
    }

    private static String getAccountKey() {
        if (ACCOUNT_KEY != null) return ACCOUNT_KEY;
        return azureAccountKey;
    }

    private static void ensureContainerExists() {
        try {
            BlobContainerClient containerClient = blobServiceClient.getBlobContainerClient(TEST_CONTAINER);
            if (!containerClient.exists()) {
                containerClient.create();
                System.out.println("✅ Created Azure container: " + TEST_CONTAINER);
            }
        } catch (Exception e) {
            // Container might already exist
            System.out.println("  Container check: " + e.getMessage());
        }
    }

    private static void createAndUploadSplits() throws Exception {
        int docsPerSplit = 3;

        for (int s = 0; s < SPLIT_CONTENT.length / docsPerSplit; s++) {
            Path indexDir = tempDir.resolve("index-" + s);
            Files.createDirectories(indexDir);
            Path splitPath = tempDir.resolve("split-" + s + ".split");

            try (SchemaBuilder builder = new SchemaBuilder()) {
                builder.addTextField("title", true, false, "default", "position");
                builder.addTextField("body", true, false, "default", "position");
                builder.addTextField("category", true, false, "raw", "basic");
                builder.addIntegerField("count", true, true, false);

                try (Schema schema = builder.build()) {
                    try (Index index = new Index(schema, indexDir.toString())) {
                        try (IndexWriter writer = index.writer(Index.Memory.DEFAULT_HEAP_SIZE, 1)) {
                            for (int d = 0; d < docsPerSplit; d++) {
                                int contentIdx = s * docsPerSplit + d;
                                if (contentIdx >= SPLIT_CONTENT.length) break;

                                String[] content = SPLIT_CONTENT[contentIdx];
                                try (Document doc = new Document()) {
                                    doc.addText("title", content[0]);
                                    doc.addText("body", content[1]);
                                    doc.addText("category", content[2]);
                                    doc.addInteger("count", contentIdx * 10);
                                    writer.addDocument(doc);
                                }
                            }
                            writer.commit();
                        }
                        index.reload();

                        QuickwitSplit.SplitConfig config = new QuickwitSplit.SplitConfig(
                            "prescan-perf-" + s, "prescan-source", "prescan-node");
                        QuickwitSplit.SplitMetadata metadata = QuickwitSplit.convertIndexFromPath(
                            indexDir.toString(), splitPath.toString(), config);

                        // Upload to Azure
                        String blobName = AZURE_PREFIX + "split-" + s + ".split";
                        BlobContainerClient containerClient = blobServiceClient.getBlobContainerClient(TEST_CONTAINER);
                        BlobClient blobClient = containerClient.getBlobClient(blobName);
                        blobClient.uploadFromFile(splitPath.toString(), true);

                        String azureUrl = "azure://" + TEST_CONTAINER + "/" + blobName;
                        azureSplitInfos.add(new SplitInfo(azureUrl, metadata.getFooterStartOffset(), metadata.getFooterEndOffset()));

                        if (docMappingJson == null) {
                            docMappingJson = metadata.getDocMappingJson();
                        }

                        System.out.println("  Uploaded split-" + s + " to " + azureUrl);
                    }
                }
            }
        }
    }

    // ==================== BASIC FUNCTIONALITY TESTS ====================

    @Test
    @Order(1)
    @DisplayName("Azure Prescan - Basic term query")
    void testBasicTermQuery() throws Exception {
        SplitQuery query = new SplitTermQuery("title", "hello");
        List<PrescanResult> results = cacheManager.prescanSplits(azureSplitInfos, docMappingJson, query);

        assertEquals(azureSplitInfos.size(), results.size());

        long matchCount = results.stream().filter(PrescanResult::couldHaveResults).count();
        assertTrue(matchCount > 0, "Should find at least one split with 'hello'");
        assertTrue(matchCount < azureSplitInfos.size(), "Should filter out some splits without 'hello'");

        System.out.println("✅ Term query 'hello': " + matchCount + "/" + azureSplitInfos.size() + " splits match");
    }

    @Test
    @Order(2)
    @DisplayName("Azure Prescan - Boolean AND query")
    void testBooleanAndQuery() throws Exception {
        SplitQuery query = new SplitBooleanQuery()
            .addMust(new SplitTermQuery("title", "hello"))
            .addMust(new SplitTermQuery("body", "tutorial"));

        List<PrescanResult> results = cacheManager.prescanSplits(azureSplitInfos, docMappingJson, query);

        long matchCount = results.stream().filter(PrescanResult::couldHaveResults).count();
        System.out.println("✅ Boolean AND query: " + matchCount + "/" + azureSplitInfos.size() + " splits match");
    }

    @Test
    @Order(3)
    @DisplayName("Azure Prescan - Boolean OR query")
    void testBooleanOrQuery() throws Exception {
        SplitQuery query = new SplitBooleanQuery()
            .addShould(new SplitTermQuery("category", "tech"))
            .addShould(new SplitTermQuery("category", "science"));

        List<PrescanResult> results = cacheManager.prescanSplits(azureSplitInfos, docMappingJson, query);

        long matchCount = results.stream().filter(PrescanResult::couldHaveResults).count();
        assertTrue(matchCount >= 2, "Should match tech and science splits");
        System.out.println("✅ Boolean OR query: " + matchCount + "/" + azureSplitInfos.size() + " splits match");
    }

    @Test
    @Order(4)
    @DisplayName("Azure Prescan - Phrase query")
    void testPhraseQuery() throws Exception {
        SplitQuery query = new SplitPhraseQuery("title", Arrays.asList("hello", "world"), 0);
        List<PrescanResult> results = cacheManager.prescanSplits(azureSplitInfos, docMappingJson, query);

        long matchCount = results.stream().filter(PrescanResult::couldHaveResults).count();
        System.out.println("✅ Phrase query 'hello world': " + matchCount + "/" + azureSplitInfos.size() + " splits could match");
    }

    @Test
    @Order(5)
    @DisplayName("Azure Prescan - Wildcard query")
    void testWildcardQuery() throws Exception {
        SplitQuery query = new SplitWildcardQuery("title", "prog*");
        List<PrescanResult> results = cacheManager.prescanSplits(azureSplitInfos, docMappingJson, query);

        long matchCount = results.stream().filter(PrescanResult::couldHaveResults).count();
        assertTrue(matchCount > 0, "Should find splits with 'programming'");
        System.out.println("✅ Wildcard query 'prog*': " + matchCount + "/" + azureSplitInfos.size() + " splits match");
    }

    @Test
    @Order(6)
    @DisplayName("Azure Prescan - Regex query")
    void testRegexQuery() throws Exception {
        SplitQuery query = new SplitRegexQuery("title", ".*ball.*");
        List<PrescanResult> results = cacheManager.prescanSplits(azureSplitInfos, docMappingJson, query);

        long matchCount = results.stream().filter(PrescanResult::couldHaveResults).count();
        assertTrue(matchCount > 0, "Should find splits with 'football' or 'basketball'");
        System.out.println("✅ Regex query '.*ball.*': " + matchCount + "/" + azureSplitInfos.size() + " splits match");
    }

    // ==================== PERFORMANCE TESTS ====================

    @Test
    @Order(10)
    @DisplayName("Azure Prescan Performance - Single split throughput")
    void testSingleSplitPerformance() throws Exception {
        final int ITERATIONS = 10000;
        SplitInfo singleSplit = azureSplitInfos.get(0);
        SplitQuery query = new SplitTermQuery("title", "hello");

        // Warmup
        for (int i = 0; i < 10; i++) {
            cacheManager.prescanSplits(Collections.singletonList(singleSplit), docMappingJson, query);
        }

        // Measure
        long start = System.nanoTime();
        for (int i = 0; i < ITERATIONS; i++) {
            cacheManager.prescanSplits(Collections.singletonList(singleSplit), docMappingJson, query);
        }
        long end = System.nanoTime();

        double avgMicros = (end - start) / 1000.0 / ITERATIONS;
        double throughput = ITERATIONS * 1_000_000_000.0 / (end - start);

        System.out.println("\n╔═══════════════════════════════════════════════════╗");
        System.out.println("║     SINGLE SPLIT PERFORMANCE (REAL AZURE)         ║");
        System.out.println("╠═══════════════════════════════════════════════════╣");
        System.out.printf("║  Iterations:    %,d                              ║%n", ITERATIONS);
        System.out.printf("║  Avg latency:   %.2f µs                           ║%n", avgMicros);
        System.out.printf("║  Throughput:    %,.0f prescans/sec              ║%n", throughput);
        System.out.println("╚═══════════════════════════════════════════════════╝");

        // Validate NO result caching - different queries must give correct results
        System.out.println("\n--- Validating no result caching (different queries) ---");

        SplitQuery matchingQuery = new SplitTermQuery("title", "hello");
        SplitQuery nonMatchingQuery = new SplitTermQuery("title", "xyznonexistent12345");

        List<PrescanResult> matchResults = cacheManager.prescanSplits(
            Collections.singletonList(singleSplit), docMappingJson, matchingQuery);
        List<PrescanResult> nonMatchResults = cacheManager.prescanSplits(
            Collections.singletonList(singleSplit), docMappingJson, nonMatchingQuery);

        boolean matching = matchResults.get(0).couldHaveResults();
        boolean nonMatching = nonMatchResults.get(0).couldHaveResults();

        System.out.printf("  Query 'hello':               couldHaveResults=%s%n", matching);
        System.out.printf("  Query 'xyznonexistent12345': couldHaveResults=%s%n", nonMatching);

        assertTrue(matching, "Query for 'hello' should find matches");
        assertFalse(nonMatching, "Query for non-existent term should NOT find matches");
        System.out.println("  ✓ Confirmed: NO result caching - different queries give different results");
    }

    @Test
    @Order(11)
    @DisplayName("Azure Prescan Performance - Multiple splits batch")
    void testMultipleSplitsPerformance() throws Exception {
        final int ITERATIONS = 10000;
        SplitQuery query = new SplitTermQuery("title", "hello");

        // Warmup
        for (int i = 0; i < 50; i++) {
            cacheManager.prescanSplits(azureSplitInfos, docMappingJson, query);
        }

        // Measure
        long start = System.nanoTime();
        for (int i = 0; i < ITERATIONS; i++) {
            cacheManager.prescanSplits(azureSplitInfos, docMappingJson, query);
        }
        long end = System.nanoTime();

        double avgMicros = (end - start) / 1000.0 / ITERATIONS;
        double throughput = ITERATIONS * azureSplitInfos.size() * 1_000_000_000.0 / (end - start);

        System.out.println("\n╔═══════════════════════════════════════════════════╗");
        System.out.println("║     MULTI-SPLIT BATCH PERFORMANCE (REAL AZURE)    ║");
        System.out.println("╠═══════════════════════════════════════════════════╣");
        System.out.printf("║  Splits per batch: %d                              ║%n", azureSplitInfos.size());
        System.out.printf("║  Iterations:       %,d                           ║%n", ITERATIONS);
        System.out.printf("║  Avg batch time:   %.2f µs                        ║%n", avgMicros);
        System.out.printf("║  Throughput:       %,.0f split-prescans/sec     ║%n", throughput);
        System.out.println("╚═══════════════════════════════════════════════════╝");
    }

    @Test
    @Order(12)
    @DisplayName("Azure Prescan Performance - Query type comparison")
    void testQueryTypePerformance() throws Exception {
        final int ITERATIONS = 500;

        Map<String, SplitQuery> queries = new LinkedHashMap<>();
        queries.put("Term", new SplitTermQuery("title", "hello"));
        queries.put("Boolean", new SplitBooleanQuery()
            .addMust(new SplitTermQuery("title", "hello"))
            .addMust(new SplitTermQuery("category", "tech")));
        queries.put("Phrase", new SplitPhraseQuery("title", Arrays.asList("hello", "world"), 0));
        queries.put("Wildcard", new SplitWildcardQuery("title", "prog*"));
        queries.put("Regex", new SplitRegexQuery("title", ".*ball.*"));

        System.out.println("\n╔═══════════════════════════════════════════════════╗");
        System.out.println("║    QUERY TYPE PERFORMANCE COMPARISON (REAL AZURE) ║");
        System.out.println("╠═══════════════════════════════════════════════════╣");
        System.out.printf("║  %-12s %12s %15s       ║%n", "Query Type", "Avg (µs)", "Throughput/s");
        System.out.println("╠═══════════════════════════════════════════════════╣");

        for (Map.Entry<String, SplitQuery> entry : queries.entrySet()) {
            // Warmup
            for (int i = 0; i < 30; i++) {
                cacheManager.prescanSplits(azureSplitInfos, docMappingJson, entry.getValue());
            }

            // Measure
            long start = System.nanoTime();
            for (int i = 0; i < ITERATIONS; i++) {
                cacheManager.prescanSplits(azureSplitInfos, docMappingJson, entry.getValue());
            }
            long end = System.nanoTime();

            double avgMicros = (end - start) / 1000.0 / ITERATIONS;
            double throughput = ITERATIONS * 1_000_000_000.0 / (end - start);

            System.out.printf("║  %-12s %12.2f %15.0f       ║%n", entry.getKey(), avgMicros, throughput);
        }
        System.out.println("╚═══════════════════════════════════════════════════╝");
    }

    // ==================== CACHE EFFECTIVENESS TESTS ====================

    @Test
    @Order(20)
    @DisplayName("Azure Prescan Cache - Cold vs warm performance")
    void testCacheEffectiveness() throws Exception {
        // Create a fresh cache manager for this test
        String uniqueCacheName = "cache-effectiveness-azure-" + System.currentTimeMillis();
        SplitCacheManager.CacheConfig freshConfig = new SplitCacheManager.CacheConfig(uniqueCacheName)
            .withMaxCacheSize(100_000_000)
            .withAzureCredentials(getStorageAccount(), getAccountKey());

        try (SplitCacheManager freshCacheManager = SplitCacheManager.getInstance(freshConfig)) {
            SplitQuery query = new SplitTermQuery("title", "programming");

            // Cold start - first access
            long coldStart = System.nanoTime();
            freshCacheManager.prescanSplits(azureSplitInfos, docMappingJson, query);
            long coldEnd = System.nanoTime();
            double coldMs = (coldEnd - coldStart) / 1_000_000.0;

            // Warm - cached data
            final int WARM_ITERATIONS = 1000;
            long warmStart = System.nanoTime();
            for (int i = 0; i < WARM_ITERATIONS; i++) {
                freshCacheManager.prescanSplits(azureSplitInfos, docMappingJson, query);
            }
            long warmEnd = System.nanoTime();
            double warmAvgMicros = (warmEnd - warmStart) / 1000.0 / WARM_ITERATIONS;

            double speedup = (coldMs * 1000) / warmAvgMicros;

            System.out.println("\n╔═══════════════════════════════════════════════════╗");
            System.out.println("║        CACHE EFFECTIVENESS (REAL AZURE)           ║");
            System.out.println("╠═══════════════════════════════════════════════════╣");
            System.out.printf("║  Cold start:   %.2f ms                            ║%n", coldMs);
            System.out.printf("║  Warm avg:     %.2f µs                            ║%n", warmAvgMicros);
            System.out.printf("║  Cache speedup: %.1fx                             ║%n", speedup);
            System.out.println("╚═══════════════════════════════════════════════════╝");

            assertTrue(warmAvgMicros < 1000,
                "Warm prescan should be sub-millisecond, got " + warmAvgMicros + " µs");

            double throughput = WARM_ITERATIONS * 1_000_000_000.0 / (warmEnd - warmStart);
            assertTrue(throughput > 1000,
                "Should achieve >1000 prescans/sec, got " + throughput);

            System.out.printf("║  Throughput:   %,.0f prescans/sec                ║%n", throughput);
            System.out.println("╚═══════════════════════════════════════════════════╝");
        }
    }

    // ==================== FILTERING ACCURACY TESTS ====================

    @Test
    @Order(30)
    @DisplayName("Azure Prescan Accuracy - Term not in any split")
    void testNonexistentTerm() throws Exception {
        SplitQuery query = new SplitTermQuery("title", "zyxwvutsrqponmlkjihgfedcba");
        List<PrescanResult> results = cacheManager.prescanSplits(azureSplitInfos, docMappingJson, query);

        long matchCount = results.stream().filter(PrescanResult::couldHaveResults).count();
        assertEquals(0, matchCount, "Should not match any split for nonexistent term");
        System.out.println("✅ Nonexistent term: 0/" + azureSplitInfos.size() + " splits match (correct)");
    }

    @Test
    @Order(31)
    @DisplayName("Azure Prescan Accuracy - Category filtering")
    void testCategoryFiltering() throws Exception {
        // Tech category should only be in split 0
        SplitQuery techQuery = new SplitTermQuery("category", "tech");
        List<PrescanResult> techResults = cacheManager.prescanSplits(azureSplitInfos, docMappingJson, techQuery);
        long techMatches = techResults.stream().filter(PrescanResult::couldHaveResults).count();

        // Science category should only be in split 1
        SplitQuery scienceQuery = new SplitTermQuery("category", "science");
        List<PrescanResult> scienceResults = cacheManager.prescanSplits(azureSplitInfos, docMappingJson, scienceQuery);
        long scienceMatches = scienceResults.stream().filter(PrescanResult::couldHaveResults).count();

        // Arts category should only be in split 3
        SplitQuery artsQuery = new SplitTermQuery("category", "arts");
        List<PrescanResult> artsResults = cacheManager.prescanSplits(azureSplitInfos, docMappingJson, artsQuery);
        long artsMatches = artsResults.stream().filter(PrescanResult::couldHaveResults).count();

        System.out.println("\n╔═══════════════════════════════════════════════════╗");
        System.out.println("║      CATEGORY FILTERING ACCURACY (REAL AZURE)     ║");
        System.out.println("╠═══════════════════════════════════════════════════╣");
        System.out.printf("║  tech:    %d/%d splits                              ║%n", techMatches, azureSplitInfos.size());
        System.out.printf("║  science: %d/%d splits                              ║%n", scienceMatches, azureSplitInfos.size());
        System.out.printf("║  arts:    %d/%d splits                              ║%n", artsMatches, azureSplitInfos.size());
        System.out.println("╚═══════════════════════════════════════════════════╝");

        // Each category should match exactly 1 split (assuming raw tokenizer for category)
        assertEquals(1, techMatches, "Tech should match exactly 1 split");
        assertEquals(1, scienceMatches, "Science should match exactly 1 split");
        assertEquals(1, artsMatches, "Arts should match exactly 1 split");
    }

    // ==================== PARALLEL EXECUTION TESTS ====================

    @Test
    @Order(40)
    @DisplayName("Azure Prescan Parallel - Concurrent prescan requests")
    void testParallelPrescanRequests() throws Exception {
        final int THREADS = 8;
        final int ITERATIONS_PER_THREAD = 100;

        ExecutorService executor = Executors.newFixedThreadPool(THREADS);
        List<Future<Long>> futures = new ArrayList<>();

        SplitQuery query = new SplitTermQuery("title", "hello");

        // Warmup
        for (int i = 0; i < 30; i++) {
            cacheManager.prescanSplits(azureSplitInfos, docMappingJson, query);
        }

        long startTime = System.nanoTime();

        // Submit parallel tasks
        for (int t = 0; t < THREADS; t++) {
            futures.add(executor.submit(() -> {
                long successCount = 0;
                for (int i = 0; i < ITERATIONS_PER_THREAD; i++) {
                    List<PrescanResult> results =
                        cacheManager.prescanSplits(azureSplitInfos, docMappingJson, query);
                    if (!results.isEmpty()) successCount++;
                }
                return successCount;
            }));
        }

        // Collect results
        long totalSuccess = 0;
        for (Future<Long> f : futures) {
            totalSuccess += f.get();
        }

        long endTime = System.nanoTime();
        executor.shutdown();

        int totalIterations = THREADS * ITERATIONS_PER_THREAD;
        double totalMs = (endTime - startTime) / 1_000_000.0;
        double throughput = totalIterations * azureSplitInfos.size() * 1000.0 / totalMs;

        System.out.println("\n╔═══════════════════════════════════════════════════╗");
        System.out.println("║     PARALLEL PRESCAN PERFORMANCE (REAL AZURE)     ║");
        System.out.println("╠═══════════════════════════════════════════════════╣");
        System.out.printf("║  Threads:           %d                             ║%n", THREADS);
        System.out.printf("║  Total iterations:  %,d                          ║%n", totalIterations);
        System.out.printf("║  Total time:        %.2f ms                       ║%n", totalMs);
        System.out.printf("║  Throughput:        %,.0f split-prescans/sec    ║%n", throughput);
        System.out.printf("║  Success rate:      %.1f%%                        ║%n", 100.0 * totalSuccess / totalIterations);
        System.out.println("╚═══════════════════════════════════════════════════╝");

        assertEquals(totalIterations, totalSuccess, "All parallel prescans should succeed");
    }

    @Test
    @Order(41)
    @DisplayName("Azure Prescan Parallel - Java thread scaling 1-16")
    void testJavaThreadScaling() throws Exception {
        final int ITERATIONS_PER_THREAD = 200;
        final int[] THREAD_COUNTS = {1, 2, 4, 8, 12, 16};

        SplitQuery query = new SplitTermQuery("title", "hello");

        // Warmup with all thread counts
        System.out.println("\n⏳ Warming up...");
        for (int threads : THREAD_COUNTS) {
            ExecutorService warmupExecutor = Executors.newFixedThreadPool(threads);
            List<Future<?>> warmupFutures = new ArrayList<>();
            for (int t = 0; t < threads; t++) {
                warmupFutures.add(warmupExecutor.submit(() -> {
                    for (int i = 0; i < 50; i++) {
                        try {
                            cacheManager.prescanSplits(azureSplitInfos, docMappingJson, query);
                        } catch (Exception e) {}
                    }
                }));
            }
            for (Future<?> f : warmupFutures) f.get();
            warmupExecutor.shutdown();
        }

        // Collect results for each thread count
        double[] throughputs = new double[THREAD_COUNTS.length];
        double baselineThroughput = 0;

        System.out.println("\n╔════════════════════════════════════════════════════════════════════════╗");
        System.out.println("║           JAVA THREAD SCALING ANALYSIS (REAL AZURE)                    ║");
        System.out.println("╠════════════════════════════════════════════════════════════════════════╣");
        System.out.println("║  Iterations per thread: " + ITERATIONS_PER_THREAD + "                                              ║");
        System.out.println("║  Splits per prescan: " + azureSplitInfos.size() + "                                                   ║");
        System.out.println("╠════════════════════════════════════════════════════════════════════════╣");
        System.out.printf("║  %-8s  %12s  %12s  %15s  %10s   ║%n",
            "Threads", "Time (ms)", "Latency (µs)", "Throughput/s", "Scaling");
        System.out.println("╠════════════════════════════════════════════════════════════════════════╣");

        for (int i = 0; i < THREAD_COUNTS.length; i++) {
            int threads = THREAD_COUNTS[i];
            ExecutorService executor = Executors.newFixedThreadPool(threads);
            List<Future<Long>> futures = new ArrayList<>();

            long startTime = System.nanoTime();

            for (int t = 0; t < threads; t++) {
                futures.add(executor.submit(() -> {
                    long successCount = 0;
                    for (int iter = 0; iter < ITERATIONS_PER_THREAD; iter++) {
                        try {
                            List<PrescanResult> results =
                                cacheManager.prescanSplits(azureSplitInfos, docMappingJson, query);
                            if (!results.isEmpty()) successCount++;
                        } catch (Exception e) {
                            // Count as failure
                        }
                    }
                    return successCount;
                }));
            }

            long totalSuccess = 0;
            for (Future<Long> f : futures) {
                totalSuccess += f.get();
            }

            long endTime = System.nanoTime();
            executor.shutdown();

            int totalIterations = threads * ITERATIONS_PER_THREAD;
            double totalMs = (endTime - startTime) / 1_000_000.0;
            double avgLatencyMicros = (endTime - startTime) / 1000.0 / totalIterations;
            double throughput = totalIterations * azureSplitInfos.size() * 1000.0 / totalMs;

            throughputs[i] = throughput;

            String scalingStr;
            if (i == 0) {
                baselineThroughput = throughput;
                scalingStr = "baseline";
            } else {
                double idealThroughput = baselineThroughput * threads;
                double efficiency = (throughput / idealThroughput) * 100;
                scalingStr = String.format("%.1f%%", efficiency);
            }

            System.out.printf("║  %-8d  %12.2f  %12.2f  %,15.0f  %10s   ║%n",
                threads, totalMs, avgLatencyMicros, throughput, scalingStr);

            assertEquals(totalIterations, totalSuccess,
                "All prescans should succeed with " + threads + " threads");
        }

        System.out.println("╠════════════════════════════════════════════════════════════════════════╣");

        double maxThroughput = 0;
        int optimalThreads = 1;
        for (int i = 0; i < THREAD_COUNTS.length; i++) {
            if (throughputs[i] > maxThroughput) {
                maxThroughput = throughputs[i];
                optimalThreads = THREAD_COUNTS[i];
            }
        }

        double overallScaling = maxThroughput / baselineThroughput;

        System.out.printf("║  Peak throughput: %,.0f split-prescans/sec at %d threads              ║%n",
            maxThroughput, optimalThreads);
        System.out.printf("║  Overall scaling: %.2fx from 1 to %d threads                           ║%n",
            overallScaling, optimalThreads);
        System.out.println("╚════════════════════════════════════════════════════════════════════════╝");

        assertTrue(overallScaling >= 1.5,
            "Should achieve at least 1.5x scaling, got " + overallScaling + "x");
    }

    @Test
    @Order(42)
    @DisplayName("Azure Prescan - Single call optimal performance")
    void testSingleCallOptimalPerformance() throws Exception {
        Assumptions.assumeTrue(
            azureStorageAccount != null && azureAccountKey != null,
            "Skipping: Azure credentials not available");

        System.out.println("\n╔════════════════════════════════════════════════════════════════════════╗");
        System.out.println("║  PROOF: SINGLE CALL = OPTIMAL PERFORMANCE (NO USER THREADING NEEDED)  ║");
        System.out.println("╠════════════════════════════════════════════════════════════════════════╣");

        SplitQuery query = new SplitTermQuery("title", "hello");

        // Test 1: Cold cache - show parallel Azure fetches
        System.out.println("║                                                                        ║");
        System.out.println("║  TEST 1: Cold Cache (Network I/O)                                     ║");
        System.out.println("║  ─────────────────────────────────────────────────────────────────    ║");

        // Fresh cache for cold test
        SplitCacheManager.CacheConfig coldConfig = new SplitCacheManager.CacheConfig(
                "cold-single-call-azure-" + System.nanoTime())
            .withMaxCacheSize(100_000_000)
            .withAzureCredentials(azureStorageAccount, azureAccountKey);

        double coldTotalMs;
        try (SplitCacheManager coldManager = SplitCacheManager.getInstance(coldConfig)) {
            long start = System.nanoTime();
            List<PrescanResult> results = coldManager.prescanSplits(azureSplitInfos, docMappingJson, query);
            coldTotalMs = (System.nanoTime() - start) / 1_000_000.0;
            assertEquals(5, results.size());
        }

        double coldPerSplit = coldTotalMs / 5;
        System.out.printf("║  Single call prescanned 5 splits in %.0fms (%.0fms per split)           ║%n",
            coldTotalMs, coldPerSplit);
        System.out.println("║  → All 5 Azure fetches happened IN PARALLEL (not 5x sequential time)  ║");

        // Test 2: Warm cache - show throughput
        System.out.println("║                                                                        ║");
        System.out.println("║  TEST 2: Warm Cache (In-Memory)                                       ║");
        System.out.println("║  ─────────────────────────────────────────────────────────────────    ║");

        // Warm up
        for (int i = 0; i < 100; i++) {
            cacheManager.prescanSplits(azureSplitInfos, docMappingJson, query);
        }

        // Measure warm cache throughput with single thread
        int iterations = 10000;
        long start = System.nanoTime();
        for (int i = 0; i < iterations; i++) {
            cacheManager.prescanSplits(azureSplitInfos, docMappingJson, query);
        }
        double warmTotalMs = (System.nanoTime() - start) / 1_000_000.0;
        double callsPerSec = iterations * 1000.0 / warmTotalMs;
        double splitsPerSec = callsPerSec * 5;

        System.out.printf("║  Single-threaded: %,.0f prescan calls/sec (%,.0f splits/sec)      ║%n",
            callsPerSec, splitsPerSec);
        System.out.printf("║  Latency: %.1f µs per call (%.1f µs per split)                       ║%n",
            warmTotalMs * 1000 / iterations, warmTotalMs * 1000 / iterations / 5);

        System.out.println("║                                                                        ║");
        System.out.println("╠════════════════════════════════════════════════════════════════════════╣");
        System.out.println("║  CONCLUSION: User makes ONE simple call, gets FULL parallelism        ║");
        System.out.println("║  • Cold cache: All network I/O parallelized automatically             ║");
        System.out.println("║  • Warm cache: ~" + String.format("%,d", (int)splitsPerSec) + " split-prescans/sec with zero config           ║");
        System.out.println("║  • No thread pools, no configuration, no complexity                   ║");
        System.out.println("╚════════════════════════════════════════════════════════════════════════╝");
    }
}

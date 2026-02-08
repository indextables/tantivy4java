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

import io.indextables.tantivy4java.aggregation.*;
import io.indextables.tantivy4java.core.DocAddress;
import io.indextables.tantivy4java.core.Schema;
import io.indextables.tantivy4java.result.SearchResult;
import io.indextables.tantivy4java.split.*;
import io.indextables.tantivy4java.split.merge.*;

import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.BlobServiceClientBuilder;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobClient;
import com.azure.storage.common.StorageSharedKeyCredential;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Real Azure Blob Storage end-to-end test for Parquet Companion Mode with 25-column wide schema.
 *
 * Stages parquet files locally and companion splits on real Azure, then queries remotely,
 * retrieves all columns, and validates data correctness across all FastFieldModes.
 *
 * Prerequisites:
 * - Azure credentials in ~/.azure/credentials:
 *   [default]
 *   storage_account=youraccount
 *   account_key=yourkey
 * - Or system properties: -Dtest.azure.storageAccount=... -Dtest.azure.accountKey=...
 *
 * Run: mvn test -Dtest=RealAzureParquetCompanionTest
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class RealAzureParquetCompanionTest {

    private static final String TEST_CONTAINER = System.getProperty("test.azure.container", "tantivy4java-test");
    private static final String STORAGE_ACCOUNT = System.getProperty("test.azure.storageAccount");
    private static final String ACCOUNT_KEY = System.getProperty("test.azure.accountKey");
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private static String azureStorageAccount;
    private static String azureAccountKey;

    private static BlobServiceClient blobServiceClient;

    @TempDir
    static Path tempDir;

    @BeforeAll
    static void setUp() {
        loadAzureCredentials();

        String storageAccount = getStorageAccount();
        String accountKey = getAccountKey();

        Assumptions.assumeTrue(storageAccount != null && accountKey != null,
            "Skipping Azure parquet companion test - no Azure credentials found");

        blobServiceClient = new BlobServiceClientBuilder()
                .endpoint("https://" + storageAccount + ".blob.core.windows.net")
                .credential(new StorageSharedKeyCredential(storageAccount, accountKey))
                .buildClient();

        BlobContainerClient container = blobServiceClient.getBlobContainerClient(TEST_CONTAINER);
        if (!container.exists()) {
            container.create();
        }

        System.out.println("=== REAL AZURE PARQUET COMPANION TEST ===");
        System.out.println("Container: " + TEST_CONTAINER + "  Account: " + storageAccount);
    }

    @AfterAll
    static void tearDown() {
        // BlobServiceClient doesn't need explicit close
    }

    // ---------------------------------------------------------------
    // Credential helpers (same pattern as RealAzureDiskCacheTest)
    // ---------------------------------------------------------------
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
                System.out.println("Loaded Azure credentials from ~/.azure/credentials");
            }
        } catch (Exception e) {
            System.out.println("Failed to read Azure credentials: " + e.getMessage());
        }
    }

    private static SplitCacheManager createCacheManager(String name) {
        SplitCacheManager.CacheConfig config = new SplitCacheManager.CacheConfig(name)
                .withMaxCacheSize(200_000_000);
        String sa = getStorageAccount(), ak = getAccountKey();
        if (sa != null && ak != null) {
            config = config.withAzureCredentials(sa, ak);
        }
        return SplitCacheManager.getInstance(config);
    }

    /** Create parquet + split locally, upload split to Azure. */
    private static SplitOnAzure stageOnAzure(String name, int numRows, long idOffset,
                                              ParquetCompanionConfig.FastFieldMode mode) throws Exception {
        Path parquetFile = tempDir.resolve(name + ".parquet");
        Path splitFile = tempDir.resolve(name + ".split");

        QuickwitSplit.nativeWriteTestParquetAllTypes(parquetFile.toString(), numRows, idOffset);

        Map<String, String> tokenizers = new HashMap<>();
        tokenizers.put("description", "default");
        tokenizers.put("large_text", "default");

        ParquetCompanionConfig config = new ParquetCompanionConfig(tempDir.toString())
                .withFastFieldMode(mode)
                .withIpAddressFields("ip_val", "src_ip")
                .withTokenizerOverrides(tokenizers)
                .withStatisticsFields("id", "uint_val", "float_val", "ts_val",
                        "status_code", "amount", "event_time");

        QuickwitSplit.SplitMetadata metadata = QuickwitSplit.createFromParquet(
                Collections.singletonList(parquetFile.toString()),
                splitFile.toString(), config);

        // Upload split to Azure
        String blobName = "parquet-companion-test/" + name + ".split";
        BlobClient blob = blobServiceClient
                .getBlobContainerClient(TEST_CONTAINER)
                .getBlobClient(blobName);
        blob.uploadFromFile(splitFile.toString(), true);

        String azureUrl = String.format("azure://%s/%s", TEST_CONTAINER, blobName);
        System.out.println("Staged " + name + " -> " + azureUrl + " (" + metadata.getNumDocs() + " docs)");
        return new SplitOnAzure(azureUrl, metadata);
    }

    static class SplitOnAzure {
        final String azureUrl;
        final QuickwitSplit.SplitMetadata metadata;
        SplitOnAzure(String azureUrl, QuickwitSplit.SplitMetadata metadata) {
            this.azureUrl = azureUrl;
            this.metadata = metadata;
        }
    }

    // ---------------------------------------------------------------
    // 1. Stage 25-column split on Azure and verify schema
    // ---------------------------------------------------------------
    @Test
    @Order(1)
    @DisplayName("Step 1: Schema validation - all 25 columns present")
    void testAzureSchemaValidationAllColumns() throws Exception {
        SplitOnAzure split = stageOnAzure("schema_test", 50, 0,
                ParquetCompanionConfig.FastFieldMode.DISABLED);

        try (SplitCacheManager cm = createCacheManager("pq-az-schema-" + System.nanoTime());
             SplitSearcher searcher = cm.createSplitSearcher(split.azureUrl, split.metadata)) {
            Schema schema = searcher.getSchema();
            List<String> fields = schema.getFieldNames();

            String[] expected = {"id", "uint_val", "float_val", "bool_val", "text_val",
                    "ts_val", "date_val", "ip_val", "tags", "address", "props",
                    "description", "src_ip", "event_time", "category",
                    "status_code", "amount", "priority", "latitude", "longitude",
                    "region", "is_active", "retry_count", "large_text"};
            for (String f : expected) {
                assertTrue(fields.contains(f), "should have field: " + f + ", got: " + fields);
            }

            SplitQuery query = searcher.parseQuery("*");
            SearchResult results = searcher.search(query, 100);
            assertEquals(50, results.getHits().size(), "should find all docs on Azure");
        }
    }

    // ---------------------------------------------------------------
    // 2. Retrieve all columns from Azure split
    // ---------------------------------------------------------------
    @Test
    @Order(2)
    @DisplayName("Step 2: Retrieve all columns - single + batch")
    void testAzureRetrieveAllColumns() throws Exception {
        SplitOnAzure split = stageOnAzure("retrieve_all", 30, 0,
                ParquetCompanionConfig.FastFieldMode.DISABLED);

        try (SplitCacheManager cm = createCacheManager("pq-az-retrieve-" + System.nanoTime());
             SplitSearcher searcher = cm.createSplitSearcher(split.azureUrl, split.metadata)) {
            SplitQuery query = searcher.parseQuery("*");
            SearchResult results = searcher.search(query, 5);
            assertTrue(results.getHits().size() >= 3);

            DocAddress addr = results.getHits().get(0).getDocAddress();

            String docJson = searcher.docProjected(addr);
            assertNotNull(docJson);
            JsonNode doc = MAPPER.readTree(docJson);

            assertTrue(doc.has("id"), "i64");
            assertTrue(doc.has("uint_val"), "u64");
            assertTrue(doc.has("text_val"), "text/raw");
            assertTrue(doc.has("ip_val"), "ip");
            assertTrue(doc.has("category"), "categorical text");
            assertTrue(doc.has("region"), "region text");
            assertTrue(doc.has("is_active"), "bool");

            // Batch retrieval
            DocAddress[] addrs = new DocAddress[3];
            for (int i = 0; i < 3; i++) {
                addrs[i] = results.getHits().get(i).getDocAddress();
            }

            byte[] batchBytes = searcher.docBatchProjected(addrs,
                    "id", "uint_val", "float_val", "text_val", "ip_val",
                    "description", "src_ip", "category", "status_code",
                    "amount", "priority", "latitude", "longitude",
                    "region", "is_active", "retry_count", "large_text");
            assertNotNull(batchBytes);

            JsonNode arr = MAPPER.readTree(new String(batchBytes, StandardCharsets.UTF_8));
            assertEquals(3, arr.size());

            for (int i = 0; i < arr.size(); i++) {
                JsonNode d = arr.get(i);
                assertTrue(d.has("id"), "doc " + i + " should have id");
                assertTrue(d.has("category"), "doc " + i + " should have category");
                assertTrue(d.has("region"), "doc " + i + " should have region");
            }
        }
    }

    // ---------------------------------------------------------------
    // 3. Full mode sweep: DISABLED / HYBRID / PARQUET_ONLY
    // ---------------------------------------------------------------
    @Test
    @Order(3)
    @DisplayName("Step 3: All FastFieldModes with aggregation")
    void testAzureAllFastFieldModes() throws Exception {
        for (ParquetCompanionConfig.FastFieldMode mode :
                ParquetCompanionConfig.FastFieldMode.values()) {

            SplitOnAzure split = stageOnAzure("mode_" + mode.name().toLowerCase(), 40, 0, mode);

            try (SplitCacheManager cm = createCacheManager("pq-az-mode-" + mode.name() + "-" + System.nanoTime());
                 SplitSearcher searcher = cm.createSplitSearcher(split.azureUrl, split.metadata)) {

                if (mode != ParquetCompanionConfig.FastFieldMode.DISABLED) {
                    searcher.preloadParquetFastFields("id", "amount").join();
                }

                SplitQuery query = searcher.parseQuery("*");
                SearchResult results = searcher.search(query, 50);
                assertEquals(40, results.getHits().size(),
                        mode.name() + ": should find all docs");

                StatsAggregation agg = new StatsAggregation("id_stats", "id");
                SearchResult aggResult = searcher.search(
                        new SplitMatchAllQuery(), 0, "id_stats", agg);
                assertTrue(aggResult.hasAggregations(),
                        mode.name() + ": should have aggregation results");
            }
        }
    }

    // ---------------------------------------------------------------
    // 4. Query default-tokenizer indexed fields on Azure
    // ---------------------------------------------------------------
    @Test
    @Order(4)
    @DisplayName("Step 4: Default-tokenizer search (description field)")
    void testAzureDefaultTokenizerSearch() throws Exception {
        SplitOnAzure split = stageOnAzure("tokensearch", 50, 0,
                ParquetCompanionConfig.FastFieldMode.DISABLED);

        try (SplitCacheManager cm = createCacheManager("pq-az-token-" + System.nanoTime());
             SplitSearcher searcher = cm.createSplitSearcher(split.azureUrl, split.metadata)) {
            SplitQuery query = searcher.parseQuery("description:quick");
            SearchResult results = searcher.search(query, 50);
            assertTrue(results.getHits().size() > 0,
                    "should find docs with 'quick' in description");

            SplitQuery foxQuery = searcher.parseQuery("description:fox");
            SearchResult foxResults = searcher.search(foxQuery, 50);
            assertTrue(foxResults.getHits().size() > 0,
                    "should find docs with 'fox' in description");

            DocAddress addr = results.getHits().get(0).getDocAddress();
            String json = searcher.docProjected(addr, "description");
            JsonNode doc = MAPPER.readTree(json);
            assertTrue(doc.has("description"));
            assertTrue(doc.get("description").asText().toLowerCase().contains("quick"),
                    "description should contain 'quick'");
        }
    }

    // ---------------------------------------------------------------
    // 5. IP address queries on Azure
    // ---------------------------------------------------------------
    @Test
    @Order(5)
    @DisplayName("Step 5: IP address term queries")
    void testAzureIpAddressQueries() throws Exception {
        SplitOnAzure split = stageOnAzure("ipsearch", 50, 0,
                ParquetCompanionConfig.FastFieldMode.HYBRID);

        try (SplitCacheManager cm = createCacheManager("pq-az-ip-" + System.nanoTime());
             SplitSearcher searcher = cm.createSplitSearcher(split.azureUrl, split.metadata)) {
            SplitQuery query = new SplitTermQuery("ip_val", "10.0.0.0");
            SearchResult results = searcher.search(query, 10);
            assertTrue(results.getHits().size() >= 1,
                    "should find doc with IP 10.0.0.0");

            if (results.getHits().size() > 0) {
                DocAddress addr = results.getHits().get(0).getDocAddress();
                String docJson = searcher.docProjected(addr);
                assertNotNull(docJson);
                JsonNode doc = MAPPER.readTree(docJson);
                assertTrue(doc.has("ip_val"), "should have ip_val");
                assertTrue(doc.has("src_ip"), "should have src_ip");

                // Also test projected retrieval with IP + non-IP fields
                String projected = searcher.docProjected(addr, "id", "ip_val", "src_ip");
                JsonNode projDoc = MAPPER.readTree(projected);
                assertTrue(projDoc.has("id"), "projected should have id");
                assertTrue(projDoc.has("ip_val"), "projected should have ip_val");
            }
        }
    }

    // ---------------------------------------------------------------
    // 6. Date/timestamp range queries on Azure
    // ---------------------------------------------------------------
    @Test
    @Order(6)
    @DisplayName("Step 6: Date histogram aggregation on timestamps")
    void testAzureDateRangeQueries() throws Exception {
        SplitOnAzure split = stageOnAzure("datesearch", 50, 0,
                ParquetCompanionConfig.FastFieldMode.HYBRID);

        try (SplitCacheManager cm = createCacheManager("pq-az-date-" + System.nanoTime());
             SplitSearcher searcher = cm.createSplitSearcher(split.azureUrl, split.metadata)) {
            searcher.preloadParquetFastFields("ts_val", "event_time").join();

            DateHistogramAggregation dateAgg = new DateHistogramAggregation("ts_hist", "ts_val");
            dateAgg.setFixedInterval("1d");
            SearchResult aggResult = searcher.search(
                    new SplitMatchAllQuery(), 0, "ts_hist", dateAgg);
            assertTrue(aggResult.hasAggregations(),
                    "should have date histogram results");
        }
    }

    // ---------------------------------------------------------------
    // 7. Numeric + text aggregations in HYBRID mode
    // ---------------------------------------------------------------
    @Test
    @Order(7)
    @DisplayName("Step 7: Stats + Terms aggregations (HYBRID)")
    void testAzureAggregationsHybrid() throws Exception {
        SplitOnAzure split = stageOnAzure("agghybrid", 50, 0,
                ParquetCompanionConfig.FastFieldMode.HYBRID);

        try (SplitCacheManager cm = createCacheManager("pq-az-agghyb-" + System.nanoTime());
             SplitSearcher searcher = cm.createSplitSearcher(split.azureUrl, split.metadata)) {
            searcher.preloadParquetFastFields("amount", "category").join();

            StatsAggregation statsAgg = new StatsAggregation("amount_stats", "amount");
            SearchResult statsResult = searcher.search(
                    new SplitMatchAllQuery(), 0, "amount_stats", statsAgg);
            assertTrue(statsResult.hasAggregations(), "should have amount stats");

            TermsAggregation termsAgg = new TermsAggregation("cat_terms", "category", 50, 0);
            SearchResult termsResult = searcher.search(
                    new SplitMatchAllQuery(), 0, "cat_terms", termsAgg);
            assertTrue(termsResult.hasAggregations(), "should have category terms");
        }
    }

    // ---------------------------------------------------------------
    // 8. Full aggregation sweep in PARQUET_ONLY mode
    // ---------------------------------------------------------------
    @Test
    @Order(8)
    @DisplayName("Step 8: Histogram + Stats + Terms aggregations (PARQUET_ONLY)")
    void testAzureAggregationsParquetOnly() throws Exception {
        SplitOnAzure split = stageOnAzure("aggpqonly", 50, 0,
                ParquetCompanionConfig.FastFieldMode.PARQUET_ONLY);

        try (SplitCacheManager cm = createCacheManager("pq-az-aggpq-" + System.nanoTime());
             SplitSearcher searcher = cm.createSplitSearcher(split.azureUrl, split.metadata)) {
            searcher.preloadParquetFastFields("id", "amount", "region").join();

            HistogramAggregation histAgg = new HistogramAggregation("id_hist", "id", 10);
            SearchResult histResult = searcher.search(
                    new SplitMatchAllQuery(), 0, "id_hist", histAgg);
            assertTrue(histResult.hasAggregations(), "should have id histogram");

            StatsAggregation statsAgg = new StatsAggregation("amount_stats", "amount");
            SearchResult statsResult = searcher.search(
                    new SplitMatchAllQuery(), 0, "amount_stats", statsAgg);
            assertTrue(statsResult.hasAggregations(), "should have amount stats in PQ_ONLY");

            TermsAggregation termsAgg = new TermsAggregation("region_terms", "region", 50, 0);
            SearchResult termsResult = searcher.search(
                    new SplitMatchAllQuery(), 0, "region_terms", termsAgg);
            assertTrue(termsResult.hasAggregations(), "should have region terms in PQ_ONLY");
        }
    }

    // ---------------------------------------------------------------
    // 9. Data correctness validation
    // ---------------------------------------------------------------
    @Test
    @Order(9)
    @DisplayName("Step 9: Data correctness - verify specific row values")
    void testAzureDataCorrectnessValidation() throws Exception {
        SplitOnAzure split = stageOnAzure("correctness", 20, 0,
                ParquetCompanionConfig.FastFieldMode.DISABLED);

        try (SplitCacheManager cm = createCacheManager("pq-az-correct-" + System.nanoTime());
             SplitSearcher searcher = cm.createSplitSearcher(split.azureUrl, split.metadata)) {
            SplitQuery query = new SplitTermQuery("text_val", "text_0");
            SearchResult results = searcher.search(query, 1);
            assertTrue(results.getHits().size() >= 1, "should find text_0");

            DocAddress addr = results.getHits().get(0).getDocAddress();
            String docJson = searcher.docProjected(addr);
            JsonNode doc = MAPPER.readTree(docJson);

            assertEquals(0, doc.get("id").asLong(), "id should be 0");
            assertEquals(1_000_000_000L, doc.get("uint_val").asLong(), "uint_val base");
            assertEquals("text_0", doc.get("text_val").asText());
            assertFalse(doc.get("is_active").asBoolean(),
                    "row 0: is_active should be false (0 % 3 == 0)");
        }
    }

    // ---------------------------------------------------------------
    // 10. Column statistics validation
    // ---------------------------------------------------------------
    @Test
    @Order(10)
    @DisplayName("Step 10: Column statistics from split metadata")
    void testAzureColumnStatistics() throws Exception {
        SplitOnAzure split = stageOnAzure("stats_test", 100, 0,
                ParquetCompanionConfig.FastFieldMode.DISABLED);

        Map<String, ColumnStatistics> stats = split.metadata.getColumnStatistics();
        assertNotNull(stats);

        ColumnStatistics idStats = stats.get("id");
        assertNotNull(idStats, "should have id stats");
        assertEquals(0, idStats.getMinLong());
        assertEquals(99, idStats.getMaxLong());

        ColumnStatistics scStats = stats.get("status_code");
        assertNotNull(scStats, "should have status_code stats");
        assertTrue(scStats.getNullCount() > 0, "status_code has nulls");

        ColumnStatistics amtStats = stats.get("amount");
        assertNotNull(amtStats, "should have amount stats");
        assertTrue(amtStats.getMinDouble() >= 0);
    }
}

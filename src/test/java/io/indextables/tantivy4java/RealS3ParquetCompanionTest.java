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

import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Real S3 end-to-end test for Parquet Companion Mode with 25-column wide schema.
 *
 * Stages parquet files locally and companion splits on real S3, then queries remotely,
 * retrieves all columns, and validates data correctness across all FastFieldModes.
 *
 * 25 columns: id (i64), uint_val (u64), float_val (f64), bool_val (bool),
 *   text_val (utf8/raw), binary_val (binary), ts_val (timestamp), date_val (date32),
 *   ip_val (ip), tags (list->json), address (struct->json), props (map->json),
 *   description (utf8/default), src_ip (ip), event_time (timestamp),
 *   category (utf8/raw), status_code (i32), amount (f64), priority (i16),
 *   latitude (f32), longitude (f32), region (utf8/raw), is_active (bool),
 *   retry_count (u32), large_text (large_utf8)
 *
 * Prerequisites:
 * - AWS credentials configured (AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY)
 * - Or ~/.aws/credentials file
 * - S3 bucket accessible for testing (default: tantivy4java-testing)
 *
 * Run: mvn test -Dtest=RealS3ParquetCompanionTest
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class RealS3ParquetCompanionTest {

    private static final String TEST_BUCKET = System.getProperty("test.s3.bucket", "tantivy4java-testing");
    private static final String TEST_REGION = System.getProperty("test.s3.region", "us-east-2");
    private static final String ACCESS_KEY = System.getProperty("test.s3.accessKey");
    private static final String SECRET_KEY = System.getProperty("test.s3.secretKey");
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private static String awsAccessKey;
    private static String awsSecretKey;

    private static S3Client s3Client;

    @TempDir
    static Path tempDir;

    // Shared state across ordered tests
    private static final Map<String, SplitOnS3> uploadedSplits = new LinkedHashMap<>();

    @BeforeAll
    static void setUp() {
        loadAwsCredentials();
        String ak = getAccessKey();
        String sk = getSecretKey();

        boolean hasExplicit = ACCESS_KEY != null && SECRET_KEY != null;
        boolean hasFile = awsAccessKey != null && awsSecretKey != null;
        boolean hasEnv = System.getenv("AWS_ACCESS_KEY_ID") != null;

        if (!hasExplicit && !hasFile && !hasEnv) {
            System.out.println("No AWS credentials found. Skipping S3 parquet companion tests.");
            Assumptions.abort("AWS credentials not available");
        }

        if (ak != null && sk != null) {
            s3Client = S3Client.builder()
                    .region(Region.of(TEST_REGION))
                    .credentialsProvider(StaticCredentialsProvider.create(
                            AwsBasicCredentials.create(ak, sk)))
                    .build();
        } else {
            s3Client = S3Client.builder()
                    .region(Region.of(TEST_REGION))
                    .build();
        }

        // Ensure bucket exists
        try {
            s3Client.headBucket(HeadBucketRequest.builder().bucket(TEST_BUCKET).build());
        } catch (NoSuchBucketException e) {
            try {
                if ("us-east-1".equals(TEST_REGION)) {
                    s3Client.createBucket(CreateBucketRequest.builder().bucket(TEST_BUCKET).build());
                } else {
                    s3Client.createBucket(CreateBucketRequest.builder()
                            .bucket(TEST_BUCKET)
                            .createBucketConfiguration(CreateBucketConfiguration.builder()
                                    .locationConstraint(BucketLocationConstraint.fromValue(TEST_REGION))
                                    .build())
                            .build());
                }
            } catch (BucketAlreadyExistsException | BucketAlreadyOwnedByYouException ex) {
                // ok
            }
        } catch (Exception e) {
            // ignore other errors, will fail later when uploading
        }

        System.out.println("=== REAL S3 PARQUET COMPANION TEST ===");
        System.out.println("Bucket: " + TEST_BUCKET + "  Region: " + TEST_REGION);
    }

    @AfterAll
    static void tearDown() {
        if (s3Client != null) s3Client.close();
    }

    // ---------------------------------------------------------------
    // Credential helpers (same pattern as RealS3EndToEndTest)
    // ---------------------------------------------------------------
    private static String getAccessKey() {
        if (ACCESS_KEY != null) return ACCESS_KEY;
        if (awsAccessKey != null) return awsAccessKey;
        return null;
    }

    private static String getSecretKey() {
        if (SECRET_KEY != null) return SECRET_KEY;
        if (awsSecretKey != null) return awsSecretKey;
        return null;
    }

    private static void loadAwsCredentials() {
        try {
            Path credPath = Paths.get(System.getProperty("user.home"), ".aws", "credentials");
            if (!Files.exists(credPath)) return;
            List<String> lines = Files.readAllLines(credPath);
            boolean inDefault = false;
            for (String line : lines) {
                line = line.trim();
                if (line.equals("[default]")) { inDefault = true; continue; }
                if (line.startsWith("[") && line.endsWith("]")) { inDefault = false; continue; }
                if (inDefault && line.contains("=")) {
                    String[] parts = line.split("=", 2);
                    if (parts.length == 2) {
                        String k = parts[0].trim(), v = parts[1].trim();
                        if ("aws_access_key_id".equals(k)) awsAccessKey = v;
                        else if ("aws_secret_access_key".equals(k)) awsSecretKey = v;
                    }
                }
            }
            if (awsAccessKey != null && awsSecretKey != null) {
                System.out.println("Loaded AWS credentials from ~/.aws/credentials");
            }
        } catch (Exception e) {
            System.out.println("Could not read ~/.aws/credentials: " + e.getMessage());
        }
    }

    private static SplitCacheManager createCacheManager(String name) {
        SplitCacheManager.CacheConfig config = new SplitCacheManager.CacheConfig(name)
                .withMaxCacheSize(200_000_000)
                .withParquetTableRoot(tempDir.toString());
        String ak = getAccessKey(), sk = getSecretKey();
        if (ak != null && sk != null) {
            config = config.withAwsCredentials(ak, sk);
        }
        config = config.withAwsRegion(TEST_REGION);
        return SplitCacheManager.getInstance(config);
    }

    /** Create parquet + split locally, upload split to S3. Parquet stays local for table_root. */
    private static SplitOnS3 stageOnS3(String name, int numRows, long idOffset,
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

        // Upload split to S3
        String s3Key = "parquet-companion-test/" + name + ".split";
        s3Client.putObject(
                PutObjectRequest.builder().bucket(TEST_BUCKET).key(s3Key).build(),
                splitFile);

        String s3Url = String.format("s3://%s/%s", TEST_BUCKET, s3Key);
        System.out.println("Staged " + name + " -> " + s3Url + " (" + metadata.getNumDocs() + " docs)");
        return new SplitOnS3(s3Url, metadata);
    }

    static class SplitOnS3 {
        final String s3Url;
        final QuickwitSplit.SplitMetadata metadata;
        SplitOnS3(String s3Url, QuickwitSplit.SplitMetadata metadata) {
            this.s3Url = s3Url;
            this.metadata = metadata;
        }
    }

    // ---------------------------------------------------------------
    // 1. Stage 25-column split on S3 and verify schema
    // ---------------------------------------------------------------
    @Test
    @Order(1)
    @DisplayName("Step 1: Schema validation - all 25 columns present")
    void testS3SchemaValidationAllColumns() throws Exception {
        SplitOnS3 split = stageOnS3("schema_test", 50, 0,
                ParquetCompanionConfig.FastFieldMode.DISABLED);
        uploadedSplits.put("schema_test", split);

        try (SplitCacheManager cm = createCacheManager("pq-s3-schema-" + System.nanoTime());
             SplitSearcher searcher = cm.createSplitSearcher(split.s3Url, split.metadata)) {
            Schema schema = searcher.getSchema();
            List<String> fields = schema.getFieldNames();

            // All 25 columns present (minus binary_val which isn't indexed)
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
            assertEquals(50, results.getHits().size(), "should find all docs on S3");
        }
    }

    // ---------------------------------------------------------------
    // 2. Retrieve all columns from S3 split
    // ---------------------------------------------------------------
    @Test
    @Order(2)
    @DisplayName("Step 2: Retrieve all columns - single + batch")
    void testS3RetrieveAllColumns() throws Exception {
        SplitOnS3 split = stageOnS3("retrieve_all", 30, 0,
                ParquetCompanionConfig.FastFieldMode.DISABLED);

        try (SplitCacheManager cm = createCacheManager("pq-s3-retrieve-" + System.nanoTime());
             SplitSearcher searcher = cm.createSplitSearcher(split.s3Url, split.metadata)) {
            SplitQuery query = searcher.parseQuery("*");
            SearchResult results = searcher.search(query, 5);
            assertTrue(results.getHits().size() >= 3);

            DocAddress addr = results.getHits().get(0).getDocAddress();

            // Single doc retrieval
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
    void testS3AllFastFieldModes() throws Exception {
        for (ParquetCompanionConfig.FastFieldMode mode :
                ParquetCompanionConfig.FastFieldMode.values()) {

            SplitOnS3 split = stageOnS3("mode_" + mode.name().toLowerCase(), 40, 0, mode);

            try (SplitCacheManager cm = createCacheManager("pq-s3-mode-" + mode.name() + "-" + System.nanoTime());
                 SplitSearcher searcher = cm.createSplitSearcher(split.s3Url, split.metadata)) {

                if (mode != ParquetCompanionConfig.FastFieldMode.DISABLED) {
                    searcher.preloadParquetFastFields("id", "amount").join();
                }

                SplitQuery query = searcher.parseQuery("*");
                SearchResult results = searcher.search(query, 50);
                assertEquals(40, results.getHits().size(),
                        mode.name() + ": should find all docs");

                // Stats aggregation
                StatsAggregation agg = new StatsAggregation("id_stats", "id");
                SearchResult aggResult = searcher.search(
                        new SplitMatchAllQuery(), 0, "id_stats", agg);
                assertTrue(aggResult.hasAggregations(),
                        mode.name() + ": should have aggregation results");
            }
        }
    }

    // ---------------------------------------------------------------
    // 4. Query default-tokenizer indexed fields on S3
    // ---------------------------------------------------------------
    @Test
    @Order(4)
    @DisplayName("Step 4: Default-tokenizer search (description field)")
    void testS3DefaultTokenizerSearch() throws Exception {
        SplitOnS3 split = stageOnS3("tokensearch_v2", 50, 0,
                ParquetCompanionConfig.FastFieldMode.DISABLED);

        try (SplitCacheManager cm = createCacheManager("pq-s3-token-" + System.nanoTime());
             SplitSearcher searcher = cm.createSplitSearcher(split.s3Url, split.metadata)) {
            // "description" uses default tokenizer -> word-level search
            SplitQuery query = searcher.parseQuery("description:quick");
            SearchResult results = searcher.search(query, 50);
            assertTrue(results.getHits().size() > 0,
                    "should find docs with 'quick' in description");

            SplitQuery foxQuery = searcher.parseQuery("description:fox");
            SearchResult foxResults = searcher.search(foxQuery, 50);
            assertTrue(foxResults.getHits().size() > 0,
                    "should find docs with 'fox' in description");

            // Verify retrieved doc has the description field
            DocAddress addr = results.getHits().get(0).getDocAddress();
            System.out.println("Description query hit: seg=" + addr.getSegmentOrd() + " doc=" + addr.getDoc());
            String json = searcher.docProjected(addr, "description");
            JsonNode doc = MAPPER.readTree(json);
            assertTrue(doc.has("description"));
            assertTrue(doc.get("description").asText().toLowerCase().contains("quick"),
                    "description should contain 'quick'");
        }
    }

    // ---------------------------------------------------------------
    // 5. IP address queries on S3
    // ---------------------------------------------------------------
    @Test
    @Order(5)
    @DisplayName("Step 5: IP address term queries")
    void testS3IpAddressQueries() throws Exception {
        SplitOnS3 split = stageOnS3("ipsearch_v2", 50, 0,
                ParquetCompanionConfig.FastFieldMode.DISABLED);

        try (SplitCacheManager cm = createCacheManager("pq-s3-ip-" + System.nanoTime());
             SplitSearcher searcher = cm.createSplitSearcher(split.s3Url, split.metadata)) {
            // Term query for specific IP in ip_val (row 0 = 10.0.0.0)
            SplitQuery query = new SplitTermQuery("ip_val", "10.0.0.0");
            SearchResult results = searcher.search(query, 10);
            assertTrue(results.getHits().size() >= 1,
                    "should find doc with IP 10.0.0.0");

            // Verify IP fields present in retrieved doc (full retrieval)
            if (results.getHits().size() > 0) {
                DocAddress addr = results.getHits().get(0).getDocAddress();
                System.out.println("IP query hit: seg=" + addr.getSegmentOrd() + " doc=" + addr.getDoc());
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
    // 6. Date/timestamp range queries on S3
    // ---------------------------------------------------------------
    @Test
    @Order(6)
    @DisplayName("Step 6: Date histogram aggregation on timestamps")
    void testS3DateRangeQueries() throws Exception {
        SplitOnS3 split = stageOnS3("datesearch", 50, 0,
                ParquetCompanionConfig.FastFieldMode.HYBRID);

        try (SplitCacheManager cm = createCacheManager("pq-s3-date-" + System.nanoTime());
             SplitSearcher searcher = cm.createSplitSearcher(split.s3Url, split.metadata)) {
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
    void testS3AggregationsHybrid() throws Exception {
        SplitOnS3 split = stageOnS3("agghybrid", 50, 0,
                ParquetCompanionConfig.FastFieldMode.HYBRID);

        try (SplitCacheManager cm = createCacheManager("pq-s3-agghyb-" + System.nanoTime());
             SplitSearcher searcher = cm.createSplitSearcher(split.s3Url, split.metadata)) {
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
    void testS3AggregationsParquetOnly() throws Exception {
        SplitOnS3 split = stageOnS3("aggpqonly", 50, 0,
                ParquetCompanionConfig.FastFieldMode.PARQUET_ONLY);

        try (SplitCacheManager cm = createCacheManager("pq-s3-aggpq-" + System.nanoTime());
             SplitSearcher searcher = cm.createSplitSearcher(split.s3Url, split.metadata)) {
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
    void testS3DataCorrectnessValidation() throws Exception {
        SplitOnS3 split = stageOnS3("correctness", 20, 0,
                ParquetCompanionConfig.FastFieldMode.DISABLED);

        try (SplitCacheManager cm = createCacheManager("pq-s3-correct-" + System.nanoTime());
             SplitSearcher searcher = cm.createSplitSearcher(split.s3Url, split.metadata)) {
            SplitQuery query = new SplitTermQuery("text_val", "text_0");
            SearchResult results = searcher.search(query, 1);
            assertTrue(results.getHits().size() >= 1, "should find text_0");

            DocAddress addr = results.getHits().get(0).getDocAddress();
            String docJson = searcher.docProjected(addr);
            JsonNode doc = MAPPER.readTree(docJson);

            // Row 0 known values from nativeWriteTestParquetAllTypes
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
    void testS3ColumnStatistics() throws Exception {
        SplitOnS3 split = stageOnS3("stats_test", 100, 0,
                ParquetCompanionConfig.FastFieldMode.DISABLED);

        Map<String, ColumnStatistics> stats = split.metadata.getColumnStatistics();
        assertNotNull(stats);

        // id: 0..99
        ColumnStatistics idStats = stats.get("id");
        assertNotNull(idStats, "should have id stats");
        assertEquals(0, idStats.getMinLong());
        assertEquals(99, idStats.getMaxLong());

        // status_code: nullable, values are 200,201,404,500,302
        ColumnStatistics scStats = stats.get("status_code");
        assertNotNull(scStats, "should have status_code stats");
        assertTrue(scStats.getNullCount() > 0, "status_code has nulls");

        // amount: nullable floats
        ColumnStatistics amtStats = stats.get("amount");
        assertNotNull(amtStats, "should have amount stats");
        assertTrue(amtStats.getMinDouble() >= 0);
    }
}

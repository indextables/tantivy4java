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
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;

/**
 * Real AWS S3 prescan performance and coverage tests.
 *
 * Tests prescan functionality with real S3:
 * - Multiple query types (term, boolean, phrase, wildcard, regex)
 * - Multiple splits with diverse content
 * - Cache effectiveness verification
 * - Parallel prescan performance
 * - Split filtering accuracy
 *
 * Prerequisites:
 * - AWS credentials in ~/.aws/credentials
 * - S3 bucket accessible (will be created if it doesn't exist)
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class RealS3PrescanPerformanceTest {

    private static final String TEST_BUCKET = System.getProperty("test.s3.bucket", "tantivy4java-testing");
    private static final String TEST_REGION = System.getProperty("test.s3.region", "us-east-2");
    private static final String ACCESS_KEY = System.getProperty("test.s3.accessKey");
    private static final String SECRET_KEY = System.getProperty("test.s3.secretKey");
    private static final String S3_PREFIX = "prescan-perf-test/";

    private static String awsAccessKey;
    private static String awsSecretKey;
    private static S3Client s3Client;
    private static List<SplitInfo> s3SplitInfos = new ArrayList<>();
    private static String docMappingJson;
    private static SplitCacheManager cacheManager;

    @TempDir
    static Path tempDir;

    // Content patterns for splits
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
        // Load AWS credentials
        loadAwsCredentials();

        String accessKey = getAccessKey();
        String secretKey = getSecretKey();

        if (accessKey == null || secretKey == null) {
            System.out.println("⚠️  No AWS credentials found. Skipping Real S3 prescan tests.");
            System.out.println("   Configure ~/.aws/credentials or set system properties");
            Assumptions.abort("AWS credentials not available");
            return;
        }

        // Create S3 client
        s3Client = S3Client.builder()
            .region(Region.of(TEST_REGION))
            .credentialsProvider(StaticCredentialsProvider.create(
                AwsBasicCredentials.create(accessKey, secretKey)))
            .build();

        // Ensure bucket exists
        ensureBucketExists();

        // Create and upload splits
        createAndUploadSplits();

        // Initialize cache manager for S3
        SplitCacheManager.CacheConfig cacheConfig = new SplitCacheManager.CacheConfig("real-s3-prescan-perf")
            .withMaxCacheSize(200_000_000)
            .withAwsCredentials(accessKey, secretKey)
            .withAwsRegion(TEST_REGION);

        cacheManager = SplitCacheManager.getInstance(cacheConfig);

        System.out.println("✅ Real S3 prescan test setup complete:");
        System.out.println("  - " + s3SplitInfos.size() + " splits uploaded to s3://" + TEST_BUCKET + "/" + S3_PREFIX);
        System.out.println("  - Region: " + TEST_REGION);
    }

    @AfterAll
    static void tearDown() {
        // Clean up S3 objects
        if (s3Client != null && !s3SplitInfos.isEmpty()) {
            try {
                for (int i = 0; i < s3SplitInfos.size(); i++) {
                    String key = S3_PREFIX + "split-" + i + ".split";
                    s3Client.deleteObject(DeleteObjectRequest.builder()
                        .bucket(TEST_BUCKET).key(key).build());
                }
                System.out.println("✅ Cleaned up " + s3SplitInfos.size() + " S3 test splits");
            } catch (Exception e) {
                System.out.println("⚠️  S3 cleanup failed: " + e.getMessage());
            }
        }

        if (cacheManager != null) {
            try { cacheManager.close(); } catch (Exception e) {}
        }
        if (s3Client != null) {
            try { s3Client.close(); } catch (Exception e) {}
        }
    }

    private static void loadAwsCredentials() {
        try {
            Path credentialsPath = Paths.get(System.getProperty("user.home"), ".aws", "credentials");
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

                        if ("aws_access_key_id".equals(key)) {
                            awsAccessKey = value;
                        } else if ("aws_secret_access_key".equals(key)) {
                            awsSecretKey = value;
                        }
                    }
                }
            }

            if (awsAccessKey != null && awsSecretKey != null) {
                System.out.println("✅ Loaded AWS credentials from ~/.aws/credentials");
            }
        } catch (Exception e) {
            System.out.println("Failed to read AWS credentials: " + e.getMessage());
        }
    }

    private static String getAccessKey() {
        if (ACCESS_KEY != null) return ACCESS_KEY;
        return awsAccessKey;
    }

    private static String getSecretKey() {
        if (SECRET_KEY != null) return SECRET_KEY;
        return awsSecretKey;
    }

    private static void ensureBucketExists() {
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
                // OK
            }
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

                        // Upload to S3
                        String s3Key = S3_PREFIX + "split-" + s + ".split";
                        s3Client.putObject(
                            PutObjectRequest.builder()
                                .bucket(TEST_BUCKET)
                                .key(s3Key)
                                .build(),
                            splitPath
                        );

                        String s3Url = "s3://" + TEST_BUCKET + "/" + s3Key;
                        s3SplitInfos.add(new SplitInfo(s3Url, metadata.getFooterStartOffset(), metadata.getFooterEndOffset()));

                        if (docMappingJson == null) {
                            docMappingJson = metadata.getDocMappingJson();
                        }

                        System.out.println("  Uploaded split-" + s + " to " + s3Url);
                    }
                }
            }
        }
    }

    // ==================== BASIC FUNCTIONALITY TESTS ====================

    @Test
    @Order(1)
    @DisplayName("S3 Prescan - Basic term query")
    void testBasicTermQuery() throws Exception {
        SplitQuery query = new SplitTermQuery("title", "hello");
        List<PrescanResult> results = cacheManager.prescanSplits(s3SplitInfos, docMappingJson, query);

        assertEquals(s3SplitInfos.size(), results.size());

        long matchCount = results.stream().filter(PrescanResult::couldHaveResults).count();
        assertTrue(matchCount > 0, "Should find at least one split with 'hello'");
        assertTrue(matchCount < s3SplitInfos.size(), "Should filter out some splits without 'hello'");

        System.out.println("✅ Term query 'hello': " + matchCount + "/" + s3SplitInfos.size() + " splits match");
    }

    @Test
    @Order(2)
    @DisplayName("S3 Prescan - Boolean AND query")
    void testBooleanAndQuery() throws Exception {
        SplitQuery query = new SplitBooleanQuery()
            .addMust(new SplitTermQuery("title", "hello"))
            .addMust(new SplitTermQuery("body", "tutorial"));

        List<PrescanResult> results = cacheManager.prescanSplits(s3SplitInfos, docMappingJson, query);

        long matchCount = results.stream().filter(PrescanResult::couldHaveResults).count();
        System.out.println("✅ Boolean AND query: " + matchCount + "/" + s3SplitInfos.size() + " splits match");
    }

    @Test
    @Order(3)
    @DisplayName("S3 Prescan - Boolean OR query")
    void testBooleanOrQuery() throws Exception {
        SplitQuery query = new SplitBooleanQuery()
            .addShould(new SplitTermQuery("category", "tech"))
            .addShould(new SplitTermQuery("category", "science"));

        List<PrescanResult> results = cacheManager.prescanSplits(s3SplitInfos, docMappingJson, query);

        long matchCount = results.stream().filter(PrescanResult::couldHaveResults).count();
        assertTrue(matchCount >= 2, "Should match tech and science splits");
        System.out.println("✅ Boolean OR query: " + matchCount + "/" + s3SplitInfos.size() + " splits match");
    }

    @Test
    @Order(4)
    @DisplayName("S3 Prescan - Phrase query")
    void testPhraseQuery() throws Exception {
        SplitQuery query = new SplitPhraseQuery("title", Arrays.asList("hello", "world"), 0);
        List<PrescanResult> results = cacheManager.prescanSplits(s3SplitInfos, docMappingJson, query);

        long matchCount = results.stream().filter(PrescanResult::couldHaveResults).count();
        System.out.println("✅ Phrase query 'hello world': " + matchCount + "/" + s3SplitInfos.size() + " splits could match");
    }

    @Test
    @Order(5)
    @DisplayName("S3 Prescan - Wildcard query")
    void testWildcardQuery() throws Exception {
        SplitQuery query = new SplitWildcardQuery("title", "prog*");
        List<PrescanResult> results = cacheManager.prescanSplits(s3SplitInfos, docMappingJson, query);

        long matchCount = results.stream().filter(PrescanResult::couldHaveResults).count();
        assertTrue(matchCount > 0, "Should find splits with 'programming'");
        System.out.println("✅ Wildcard query 'prog*': " + matchCount + "/" + s3SplitInfos.size() + " splits match");
    }

    @Test
    @Order(6)
    @DisplayName("S3 Prescan - Regex query")
    void testRegexQuery() throws Exception {
        SplitQuery query = new SplitRegexQuery("title", ".*ball.*");
        List<PrescanResult> results = cacheManager.prescanSplits(s3SplitInfos, docMappingJson, query);

        long matchCount = results.stream().filter(PrescanResult::couldHaveResults).count();
        assertTrue(matchCount > 0, "Should find splits with 'football' or 'basketball'");
        System.out.println("✅ Regex query '.*ball.*': " + matchCount + "/" + s3SplitInfos.size() + " splits match");
    }

    // ==================== PERFORMANCE TESTS ====================

    @Test
    @Order(10)
    @DisplayName("S3 Prescan Performance - Single split throughput")
    void testSingleSplitPerformance() throws Exception {
        final int ITERATIONS = 10000;  // Increased for better statistics with fast caching
        SplitInfo singleSplit = s3SplitInfos.get(0);
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
        System.out.println("║     SINGLE SPLIT PERFORMANCE (REAL S3)            ║");
        System.out.println("╠═══════════════════════════════════════════════════╣");
        System.out.printf("║  Iterations:    %,d                              ║%n", ITERATIONS);
        System.out.printf("║  Avg latency:   %.2f µs                           ║%n", avgMicros);
        System.out.printf("║  Throughput:    %,.0f prescans/sec              ║%n", throughput);
        System.out.println("╚═══════════════════════════════════════════════════╝");

        // Validate NO result caching - different queries must give correct results
        // If results were cached, these would incorrectly all match or all not match
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

        // Validation: matching query should return true, non-matching should return false
        assertTrue(matching, "Query for 'hello' should find matches");
        assertFalse(nonMatching, "Query for non-existent term should NOT find matches");
        System.out.println("  ✓ Confirmed: NO result caching - different queries give different results");
    }

    @Test
    @Order(11)
    @DisplayName("S3 Prescan Performance - Multiple splits batch")
    void testMultipleSplitsPerformance() throws Exception {
        final int ITERATIONS = 10000;  // Increased for better statistics with fast caching
        SplitQuery query = new SplitTermQuery("title", "hello");

        // Warmup
        for (int i = 0; i < 50; i++) {
            cacheManager.prescanSplits(s3SplitInfos, docMappingJson, query);
        }

        // Measure
        long start = System.nanoTime();
        for (int i = 0; i < ITERATIONS; i++) {
            cacheManager.prescanSplits(s3SplitInfos, docMappingJson, query);
        }
        long end = System.nanoTime();

        double avgMicros = (end - start) / 1000.0 / ITERATIONS;
        double throughput = ITERATIONS * s3SplitInfos.size() * 1_000_000_000.0 / (end - start);

        System.out.println("\n╔═══════════════════════════════════════════════════╗");
        System.out.println("║     MULTI-SPLIT BATCH PERFORMANCE (REAL S3)       ║");
        System.out.println("╠═══════════════════════════════════════════════════╣");
        System.out.printf("║  Splits per batch: %d                              ║%n", s3SplitInfos.size());
        System.out.printf("║  Iterations:       %,d                           ║%n", ITERATIONS);
        System.out.printf("║  Avg batch time:   %.2f µs                        ║%n", avgMicros);
        System.out.printf("║  Throughput:       %,.0f split-prescans/sec     ║%n", throughput);
        System.out.println("╚═══════════════════════════════════════════════════╝");
    }

    @Test
    @Order(12)
    @DisplayName("S3 Prescan Performance - Query type comparison")
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
        System.out.println("║    QUERY TYPE PERFORMANCE COMPARISON (REAL S3)    ║");
        System.out.println("╠═══════════════════════════════════════════════════╣");
        System.out.printf("║  %-12s %12s %15s       ║%n", "Query Type", "Avg (µs)", "Throughput/s");
        System.out.println("╠═══════════════════════════════════════════════════╣");

        for (Map.Entry<String, SplitQuery> entry : queries.entrySet()) {
            // Warmup
            for (int i = 0; i < 30; i++) {
                cacheManager.prescanSplits(s3SplitInfos, docMappingJson, entry.getValue());
            }

            // Measure
            long start = System.nanoTime();
            for (int i = 0; i < ITERATIONS; i++) {
                cacheManager.prescanSplits(s3SplitInfos, docMappingJson, entry.getValue());
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
    @DisplayName("S3 Prescan Cache - Cold vs warm performance")
    void testCacheEffectiveness() throws Exception {
        // Create a fresh cache manager for this test
        String uniqueCacheName = "cache-effectiveness-" + System.currentTimeMillis();
        SplitCacheManager.CacheConfig freshConfig = new SplitCacheManager.CacheConfig(uniqueCacheName)
            .withMaxCacheSize(100_000_000)
            .withAwsCredentials(getAccessKey(), getSecretKey())
            .withAwsRegion(TEST_REGION);

        try (SplitCacheManager freshCacheManager = SplitCacheManager.getInstance(freshConfig)) {
            SplitQuery query = new SplitTermQuery("title", "programming");

            // Cold start - first access (may still benefit from global ByteRangeCache)
            long coldStart = System.nanoTime();
            freshCacheManager.prescanSplits(s3SplitInfos, docMappingJson, query);
            long coldEnd = System.nanoTime();
            double coldMs = (coldEnd - coldStart) / 1_000_000.0;

            // Warm - cached data (searcher + byte ranges cached)
            final int WARM_ITERATIONS = 1000;
            long warmStart = System.nanoTime();
            for (int i = 0; i < WARM_ITERATIONS; i++) {
                freshCacheManager.prescanSplits(s3SplitInfos, docMappingJson, query);
            }
            long warmEnd = System.nanoTime();
            double warmAvgMicros = (warmEnd - warmStart) / 1000.0 / WARM_ITERATIONS;

            double speedup = (coldMs * 1000) / warmAvgMicros;

            System.out.println("\n╔═══════════════════════════════════════════════════╗");
            System.out.println("║        CACHE EFFECTIVENESS (REAL S3)              ║");
            System.out.println("╠═══════════════════════════════════════════════════╣");
            System.out.printf("║  Cold start:   %.2f ms                            ║%n", coldMs);
            System.out.printf("║  Warm avg:     %.2f µs                            ║%n", warmAvgMicros);
            System.out.printf("║  Cache speedup: %.1fx                             ║%n", speedup);
            System.out.println("╚═══════════════════════════════════════════════════╝");

            // With ByteRangeCache optimization, even cold starts are fast due to global caching
            // Validate that warm access is sub-millisecond (the key performance requirement)
            assertTrue(warmAvgMicros < 1000,
                "Warm prescan should be sub-millisecond, got " + warmAvgMicros + " µs");

            // Also validate we're getting reasonable throughput
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
    @DisplayName("S3 Prescan Accuracy - Term not in any split")
    void testNonexistentTerm() throws Exception {
        SplitQuery query = new SplitTermQuery("title", "zyxwvutsrqponmlkjihgfedcba");
        List<PrescanResult> results = cacheManager.prescanSplits(s3SplitInfos, docMappingJson, query);

        long matchCount = results.stream().filter(PrescanResult::couldHaveResults).count();
        assertEquals(0, matchCount, "Should not match any split for nonexistent term");
        System.out.println("✅ Nonexistent term: 0/" + s3SplitInfos.size() + " splits match (correct)");
    }

    @Test
    @Order(31)
    @DisplayName("S3 Prescan Accuracy - Category filtering")
    void testCategoryFiltering() throws Exception {
        // Tech category should only be in split 0
        SplitQuery techQuery = new SplitTermQuery("category", "tech");
        List<PrescanResult> techResults = cacheManager.prescanSplits(s3SplitInfos, docMappingJson, techQuery);
        long techMatches = techResults.stream().filter(PrescanResult::couldHaveResults).count();

        // Science category should only be in split 1
        SplitQuery scienceQuery = new SplitTermQuery("category", "science");
        List<PrescanResult> scienceResults = cacheManager.prescanSplits(s3SplitInfos, docMappingJson, scienceQuery);
        long scienceMatches = scienceResults.stream().filter(PrescanResult::couldHaveResults).count();

        // Arts category should only be in split 3
        SplitQuery artsQuery = new SplitTermQuery("category", "arts");
        List<PrescanResult> artsResults = cacheManager.prescanSplits(s3SplitInfos, docMappingJson, artsQuery);
        long artsMatches = artsResults.stream().filter(PrescanResult::couldHaveResults).count();

        System.out.println("\n╔═══════════════════════════════════════════════════╗");
        System.out.println("║      CATEGORY FILTERING ACCURACY (REAL S3)        ║");
        System.out.println("╠═══════════════════════════════════════════════════╣");
        System.out.printf("║  tech:    %d/%d splits                              ║%n", techMatches, s3SplitInfos.size());
        System.out.printf("║  science: %d/%d splits                              ║%n", scienceMatches, s3SplitInfos.size());
        System.out.printf("║  arts:    %d/%d splits                              ║%n", artsMatches, s3SplitInfos.size());
        System.out.println("╚═══════════════════════════════════════════════════╝");

        // Each category should match exactly 1 split (assuming raw tokenizer for category)
        assertEquals(1, techMatches, "Tech should match exactly 1 split");
        assertEquals(1, scienceMatches, "Science should match exactly 1 split");
        assertEquals(1, artsMatches, "Arts should match exactly 1 split");
    }

    // ==================== PARALLEL EXECUTION TESTS ====================

    @Test
    @Order(40)
    @DisplayName("S3 Prescan Parallel - Concurrent prescan requests")
    void testParallelPrescanRequests() throws Exception {
        final int THREADS = 8;
        final int ITERATIONS_PER_THREAD = 100;

        ExecutorService executor = Executors.newFixedThreadPool(THREADS);
        List<Future<Long>> futures = new ArrayList<>();

        SplitQuery query = new SplitTermQuery("title", "hello");

        // Warmup
        for (int i = 0; i < 30; i++) {
            cacheManager.prescanSplits(s3SplitInfos, docMappingJson, query);
        }

        long startTime = System.nanoTime();

        // Submit parallel tasks
        for (int t = 0; t < THREADS; t++) {
            futures.add(executor.submit(() -> {
                long successCount = 0;
                for (int i = 0; i < ITERATIONS_PER_THREAD; i++) {
                    List<PrescanResult> results =
                        cacheManager.prescanSplits(s3SplitInfos, docMappingJson, query);
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
        double throughput = totalIterations * s3SplitInfos.size() * 1000.0 / totalMs;

        System.out.println("\n╔═══════════════════════════════════════════════════╗");
        System.out.println("║     PARALLEL PRESCAN PERFORMANCE (REAL S3)        ║");
        System.out.println("╠═══════════════════════════════════════════════════╣");
        System.out.printf("║  Threads:           %d                             ║%n", THREADS);
        System.out.printf("║  Total iterations:  %,d                          ║%n", totalIterations);
        System.out.printf("║  Total time:        %.2f ms                       ║%n", totalMs);
        System.out.printf("║  Throughput:        %,.0f split-prescans/sec    ║%n", throughput);
        System.out.printf("║  Success rate:      %.1f%%                        ║%n", 100.0 * totalSuccess / totalIterations);
        System.out.println("╚═══════════════════════════════════════════════════╝");

        assertEquals(totalIterations, totalSuccess, "All parallel prescans should succeed");
    }
}

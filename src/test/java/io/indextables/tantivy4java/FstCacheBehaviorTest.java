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
import java.util.concurrent.ThreadLocalRandom;

import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;
import software.amazon.awssdk.core.sync.RequestBody;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Test to investigate FST/term dictionary caching behavior.
 *
 * This test reproduces the observed behavior where:
 * 1. Query with term A - downloads segments, populates cache
 * 2. Query with term A again - super fast (cache hit)
 * 3. Query with term B (same field!) - slower, downloads MORE segments
 * 4. Query with term B again - super fast (cache hit)
 *
 * The expectation was that the term dictionary (FST) would be downloaded
 * in its entirety on first access, so different terms shouldn't cause
 * additional downloads.
 *
 * Prerequisites:
 * - AWS credentials configured (AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY)
 * - Or ~/.aws/credentials file with [default] profile
 *
 * Run with: TANTIVY4JAVA_DEBUG=1 to see detailed cache behavior
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class FstCacheBehaviorTest {

    // Test configuration
    private static final String TEST_BUCKET = System.getProperty("test.s3.bucket", "tantivy4java-testing");
    private static final String TEST_REGION = System.getProperty("test.s3.region", "us-east-2");
    private static final String ACCESS_KEY = System.getProperty("test.s3.accessKey");
    private static final String SECRET_KEY = System.getProperty("test.s3.secretKey");

    // Number of unique terms to generate (large enough to have significant FST)
    private static final int NUM_UNIQUE_TERMS = 100_000;  // 100K unique terms
    private static final int NUM_DOCUMENTS = 50_000;       // 50K documents

    // AWS credentials
    private static String awsAccessKey;
    private static String awsSecretKey;

    @TempDir
    static Path tempDir;

    private static S3Client s3Client;
    private static String splitS3Url;
    private static QuickwitSplit.SplitMetadata splitMetadata;
    private static List<String> generatedTerms = new ArrayList<>();
    private static Random random = new Random(42); // Fixed seed for reproducibility

    @BeforeAll
    static void setup() {
        loadAwsCredentials();

        String accessKey = getAccessKey();
        String secretKey = getSecretKey();

        if (accessKey == null || secretKey == null) {
            System.out.println("⚠️  No AWS credentials found. Skipping S3 tests.");
            Assumptions.abort("AWS credentials not available");
        }

        s3Client = S3Client.builder()
            .region(Region.of(TEST_REGION))
            .credentialsProvider(StaticCredentialsProvider.create(
                AwsBasicCredentials.create(accessKey, secretKey)))
            .build();

        System.out.println("✅ S3 client initialized");
        System.out.println("   Bucket: " + TEST_BUCKET);
        System.out.println("   Region: " + TEST_REGION);

        // Ensure bucket exists
        ensureBucketExists();
    }

    @AfterAll
    static void cleanup() {
        if (s3Client != null) {
            s3Client.close();
        }
    }

    @Test
    @Order(1)
    @DisplayName("Step 1: Generate index with many unique random terms")
    void step1_createIndexWithManyTerms() throws IOException {
        System.out.println("🏗️  Creating index with " + NUM_UNIQUE_TERMS + " unique terms...");
        System.out.println("   Documents: " + NUM_DOCUMENTS);

        // Generate unique random terms
        Set<String> termSet = new HashSet<>();
        while (termSet.size() < NUM_UNIQUE_TERMS) {
            termSet.add(generateRandomTerm());
        }
        generatedTerms = new ArrayList<>(termSet);
        Collections.sort(generatedTerms); // Sort for easier debugging

        System.out.println("   Generated " + generatedTerms.size() + " unique terms");
        System.out.println("   Sample terms: " + generatedTerms.subList(0, Math.min(10, generatedTerms.size())));

        // Create schema with multiple text fields to test per-field behavior
        Path indexPath = tempDir.resolve("fst-test-index");

        try (SchemaBuilder schemaBuilder = new SchemaBuilder()) {
            // Multiple text fields to test per-field FST behavior
            schemaBuilder.addTextField("content", true, false, "default", "position");
            schemaBuilder.addTextField("title", true, false, "default", "position");
            schemaBuilder.addTextField("tags", true, false, "default", "position");
            schemaBuilder.addTextField("category", true, false, "default", "position");
            schemaBuilder.addIntegerField("doc_id", true, true, true);

            try (Schema schema = schemaBuilder.build()) {
                try (Index index = new Index(schema, indexPath.toString(), false)) {
                    try (IndexWriter writer = index.writer(Index.Memory.LARGE_HEAP_SIZE, 1)) {

                        // Create documents with random terms
                        for (int i = 0; i < NUM_DOCUMENTS; i++) {
                            try (Document doc = new Document()) {
                                // Each document gets multiple random terms in content
                                StringBuilder content = new StringBuilder();
                                int numTermsInDoc = 5 + random.nextInt(20); // 5-25 terms per doc
                                for (int j = 0; j < numTermsInDoc; j++) {
                                    content.append(generatedTerms.get(random.nextInt(generatedTerms.size()))).append(" ");
                                }
                                doc.addText("content", content.toString().trim());

                                // Title gets 2-5 random terms
                                StringBuilder title = new StringBuilder();
                                int numTitleTerms = 2 + random.nextInt(4);
                                for (int j = 0; j < numTitleTerms; j++) {
                                    title.append(generatedTerms.get(random.nextInt(generatedTerms.size()))).append(" ");
                                }
                                doc.addText("title", title.toString().trim());

                                // Tags get 1-3 random terms
                                StringBuilder tags = new StringBuilder();
                                int numTags = 1 + random.nextInt(3);
                                for (int j = 0; j < numTags; j++) {
                                    tags.append(generatedTerms.get(random.nextInt(generatedTerms.size()))).append(" ");
                                }
                                doc.addText("tags", tags.toString().trim());

                                // Category gets a single random term
                                doc.addText("category", generatedTerms.get(random.nextInt(generatedTerms.size())));

                                // Doc ID
                                doc.addInteger("doc_id", i);

                                writer.addDocument(doc);
                            }

                            if ((i + 1) % 10000 == 0) {
                                System.out.println("   Progress: " + (i + 1) + "/" + NUM_DOCUMENTS + " documents");
                            }
                        }

                        writer.commit();
                    }

                    index.reload();
                    System.out.println("✅ Index created with " + NUM_DOCUMENTS + " documents");
                }
            }
        }

        // Convert to split
        Path splitPath = tempDir.resolve("fst-test.split");
        QuickwitSplit.SplitConfig config = new QuickwitSplit.SplitConfig(
            "fst-cache-test-index",
            "test-source",
            "test-node"
        );

        splitMetadata = QuickwitSplit.convertIndexFromPath(
            indexPath.toString(),
            splitPath.toString(),
            config
        );

        System.out.println("✅ Split created:");
        System.out.println("   Split ID: " + splitMetadata.getSplitId());
        System.out.println("   Documents: " + splitMetadata.getNumDocs());
        System.out.println("   Size: " + splitMetadata.getUncompressedSizeBytes() + " bytes");
        System.out.println("   Split file: " + splitPath);
        System.out.println("   Split file size: " + Files.size(splitPath) + " bytes");
    }

    @Test
    @Order(2)
    @DisplayName("Step 2: Upload split to S3")
    void step2_uploadSplitToS3() throws IOException {
        Path localSplitPath = tempDir.resolve("fst-test.split");
        assertTrue(Files.exists(localSplitPath), "Split file should exist from Step 1");

        String s3Key = "fst-cache-test/fst-test-" + System.currentTimeMillis() + ".split";
        splitS3Url = String.format("s3://%s/%s", TEST_BUCKET, s3Key);

        System.out.println("☁️  Uploading split to S3...");
        System.out.println("   Local path: " + localSplitPath);
        System.out.println("   S3 URL: " + splitS3Url);
        System.out.println("   File size: " + Files.size(localSplitPath) + " bytes");

        PutObjectRequest putRequest = PutObjectRequest.builder()
            .bucket(TEST_BUCKET)
            .key(s3Key)
            .build();

        s3Client.putObject(putRequest, localSplitPath);

        // Verify upload
        HeadObjectResponse headResponse = s3Client.headObject(
            HeadObjectRequest.builder().bucket(TEST_BUCKET).key(s3Key).build());

        System.out.println("✅ Split uploaded to S3:");
        System.out.println("   Size: " + headResponse.contentLength() + " bytes");
        System.out.println("   ETag: " + headResponse.eTag());
    }

    @Test
    @Order(3)
    @DisplayName("Step 3: Test FST cache behavior with different terms")
    void step3_testFstCacheBehavior() throws Exception {
        System.out.println("\n" + "=".repeat(80));
        System.out.println("🔬 TESTING FST CACHE BEHAVIOR");
        System.out.println("=".repeat(80));
        System.out.println("\nExpected behavior: After first query, FST should be fully cached.");
        System.out.println("Observed issue: Different terms cause additional downloads.\n");

        assertNotNull(splitS3Url, "Split S3 URL should be available from Step 2");
        assertNotNull(splitMetadata, "Split metadata should be available from Step 1");
        assertTrue(generatedTerms.size() > 0, "Generated terms should be available from Step 1");

        // Create cache manager with fresh cache
        String cacheName = "fst-cache-test-" + System.currentTimeMillis();
        SplitCacheManager.CacheConfig cacheConfig = new SplitCacheManager.CacheConfig(cacheName)
            .withMaxCacheSize(500_000_000); // 500MB cache - plenty of room

        String accessKey = getAccessKey();
        String secretKey = getSecretKey();
        if (accessKey != null && secretKey != null) {
            cacheConfig = cacheConfig.withAwsCredentials(accessKey, secretKey);
        }
        cacheConfig = cacheConfig.withAwsRegion(TEST_REGION);

        SplitCacheManager cacheManager = SplitCacheManager.getInstance(cacheConfig);

        // Pick several test terms from different parts of the term list
        // (different FST paths should require different byte ranges if FST is lazy-loaded)
        List<String> testTerms = Arrays.asList(
            generatedTerms.get(0),                                    // First term (alphabetically)
            generatedTerms.get(generatedTerms.size() / 4),           // 25th percentile
            generatedTerms.get(generatedTerms.size() / 2),           // Median term
            generatedTerms.get(3 * generatedTerms.size() / 4),       // 75th percentile
            generatedTerms.get(generatedTerms.size() - 1)            // Last term (alphabetically)
        );

        System.out.println("Test terms selected from across the FST:");
        for (int i = 0; i < testTerms.size(); i++) {
            System.out.println("   Term " + (i+1) + ": \"" + testTerms.get(i) + "\"");
        }
        System.out.println();

        try (SplitSearcher searcher = cacheManager.createSplitSearcher(splitS3Url, splitMetadata)) {

            System.out.println("SplitSearcher created. Starting cache behavior tests...\n");

            // Get initial cache stats
            var initialStats = searcher.getCacheStats();
            System.out.println("📊 Initial cache stats:");
            System.out.println("   Hits: " + initialStats.getHitCount());
            System.out.println("   Misses: " + initialStats.getMissCount());
            System.out.println("   Size: " + initialStats.getTotalSize() + " bytes\n");

            // Test each term and track cache behavior
            List<CacheTestResult> results = new ArrayList<>();

            for (int round = 0; round < 2; round++) {
                System.out.println("━".repeat(60));
                System.out.println("ROUND " + (round + 1) + (round == 0 ? " (Cold cache)" : " (Warm cache)"));
                System.out.println("━".repeat(60));

                for (int i = 0; i < testTerms.size(); i++) {
                    String term = testTerms.get(i);

                    // Get stats before query
                    var statsBefore = searcher.getCacheStats();
                    long missesBefore = statsBefore.getMissCount();
                    long hitsBefore = statsBefore.getHitCount();
                    long sizeBefore = statsBefore.getTotalSize();

                    // Execute query
                    long startTime = System.nanoTime();
                    SplitQuery query = new SplitTermQuery("content", term);
                    SearchResult result = searcher.search(query, 10);
                    long endTime = System.nanoTime();
                    long queryTimeMs = (endTime - startTime) / 1_000_000;

                    // Get stats after query
                    var statsAfter = searcher.getCacheStats();
                    long missesAfter = statsAfter.getMissCount();
                    long hitsAfter = statsAfter.getHitCount();
                    long sizeAfter = statsAfter.getTotalSize();

                    long newMisses = missesAfter - missesBefore;
                    long newHits = hitsAfter - hitsBefore;
                    long newBytes = sizeAfter - sizeBefore;

                    CacheTestResult testResult = new CacheTestResult(
                        round, i, term, result.getHits().size(),
                        queryTimeMs, newMisses, newHits, newBytes
                    );
                    results.add(testResult);

                    // Determine if this is expected or unexpected behavior
                    String indicator;
                    if (round == 0) {
                        // First round - expect misses for first query, then should be cached
                        if (i == 0) {
                            indicator = newMisses > 0 ? "✅ Expected (initial load)" : "⚠️ Unexpected (no misses on first query)";
                        } else {
                            indicator = newMisses > 0 ? "❌ UNEXPECTED: New cache misses for same field!" : "✅ Expected (cached)";
                        }
                    } else {
                        // Second round - everything should be cached
                        indicator = newMisses > 0 ? "❌ UNEXPECTED: Cache miss on warm cache!" : "✅ Expected (cached)";
                    }

                    System.out.printf("   Term %d: \"%s\"%n", i + 1, term.length() > 20 ? term.substring(0, 20) + "..." : term);
                    System.out.printf("      Hits: %d | Time: %d ms | New Misses: %d | New Bytes: %d%n",
                                     result.getHits().size(), queryTimeMs, newMisses, newBytes);
                    System.out.printf("      %s%n%n", indicator);
                }
            }

            // Print summary analysis
            System.out.println("\n" + "=".repeat(80));
            System.out.println("📈 CACHE BEHAVIOR ANALYSIS");
            System.out.println("=".repeat(80));

            // Analyze round 1 (cold cache) - first term vs subsequent terms
            long round1Term1Misses = results.stream()
                .filter(r -> r.round == 0 && r.termIndex == 0)
                .mapToLong(r -> r.newMisses)
                .sum();
            long round1OtherTermsMisses = results.stream()
                .filter(r -> r.round == 0 && r.termIndex > 0)
                .mapToLong(r -> r.newMisses)
                .sum();

            System.out.println("\nRound 1 (Cold Cache):");
            System.out.println("   First term cache misses: " + round1Term1Misses);
            System.out.println("   Subsequent terms cache misses: " + round1OtherTermsMisses);

            if (round1OtherTermsMisses > 0) {
                System.out.println("\n   ❌ ISSUE CONFIRMED: Different terms in the SAME FIELD caused");
                System.out.println("      additional cache misses after the first term was queried.");
                System.out.println("      This suggests the FST is NOT being fully cached on first access.");
            } else {
                System.out.println("\n   ✅ FST appears to be fully cached after first query.");
            }

            // Analyze round 2 (warm cache)
            long round2Misses = results.stream()
                .filter(r -> r.round == 1)
                .mapToLong(r -> r.newMisses)
                .sum();

            System.out.println("\nRound 2 (Warm Cache):");
            System.out.println("   Total cache misses: " + round2Misses);

            if (round2Misses > 0) {
                System.out.println("   ❌ Cache misses on warm cache - cache may be evicting entries");
            } else {
                System.out.println("   ✅ All queries served from cache");
            }

            // Final cache stats
            var finalStats = searcher.getCacheStats();
            System.out.println("\n📊 Final cache stats:");
            System.out.println("   Total hits: " + finalStats.getHitCount());
            System.out.println("   Total misses: " + finalStats.getMissCount());
            System.out.println("   Total size: " + finalStats.getTotalSize() + " bytes");
            System.out.println("   Hit rate: " + String.format("%.1f%%",
                finalStats.getHitCount() * 100.0 / (finalStats.getHitCount() + finalStats.getMissCount())));

            // Test different field to see per-field FST loading
            System.out.println("\n" + "━".repeat(60));
            System.out.println("TESTING DIFFERENT FIELD (title vs content)");
            System.out.println("━".repeat(60));

            var statsBeforeNewField = searcher.getCacheStats();
            long missesBeforeNewField = statsBeforeNewField.getMissCount();

            // Query the same term but in a different field
            String testTerm = testTerms.get(2);
            SplitQuery titleQuery = new SplitTermQuery("title", testTerm);
            SearchResult titleResult = searcher.search(titleQuery, 10);

            var statsAfterNewField = searcher.getCacheStats();
            long newFieldMisses = statsAfterNewField.getMissCount() - missesBeforeNewField;

            System.out.println("   Query: term \"" + testTerm + "\" in field \"title\"");
            System.out.println("   Hits: " + titleResult.getHits().size());
            System.out.println("   New cache misses: " + newFieldMisses);

            if (newFieldMisses > 0) {
                System.out.println("   ℹ️  New misses expected - each field has separate FST");
            } else {
                System.out.println("   ✅ No new misses - field FST may have been preloaded");
            }
        }

        System.out.println("\n" + "=".repeat(80));
        System.out.println("TEST COMPLETE");
        System.out.println("=".repeat(80));
    }

    @Test
    @Order(4)
    @DisplayName("Step 4: Test TERM prewarm feature - FST preloading eliminates cache misses")
    void step4_testTermPrewarmFeature() throws Exception {
        System.out.println("\n" + "=".repeat(80));
        System.out.println("🔥 TESTING TERM PREWARM FEATURE");
        System.out.println("=".repeat(80));
        System.out.println("\nThis test verifies that preloading the TERM component eliminates");
        System.out.println("cache misses for different terms in the same field.\n");

        assertNotNull(splitS3Url, "Split S3 URL should be available from Step 2");
        assertNotNull(splitMetadata, "Split metadata should be available from Step 1");
        assertTrue(generatedTerms.size() > 0, "Generated terms should be available from Step 1");

        // Create a NEW cache manager with fresh cache to get clean baseline
        String cacheName = "fst-prewarm-test-" + System.currentTimeMillis();
        SplitCacheManager.CacheConfig cacheConfig = new SplitCacheManager.CacheConfig(cacheName)
            .withMaxCacheSize(500_000_000); // 500MB cache

        String accessKey = getAccessKey();
        String secretKey = getSecretKey();
        if (accessKey != null && secretKey != null) {
            cacheConfig = cacheConfig.withAwsCredentials(accessKey, secretKey);
        }
        cacheConfig = cacheConfig.withAwsRegion(TEST_REGION);

        SplitCacheManager cacheManager = SplitCacheManager.getInstance(cacheConfig);

        // Test terms from across the FST
        List<String> testTerms = Arrays.asList(
            generatedTerms.get(0),
            generatedTerms.get(generatedTerms.size() / 4),
            generatedTerms.get(generatedTerms.size() / 2),
            generatedTerms.get(3 * generatedTerms.size() / 4),
            generatedTerms.get(generatedTerms.size() - 1)
        );

        try (SplitSearcher searcher = cacheManager.createSplitSearcher(splitS3Url, splitMetadata)) {

            System.out.println("Step 4a: Initial cache stats (before prewarm)");
            var initialStats = searcher.getCacheStats();
            System.out.println("   Hits: " + initialStats.getHitCount());
            System.out.println("   Misses: " + initialStats.getMissCount() + "\n");

            // PREWARM the TERM component
            System.out.println("Step 4b: Prewarming TERM component (loading all FSTs into cache)...");
            long prewarmStart = System.nanoTime();
            searcher.preloadComponents(SplitSearcher.IndexComponent.TERM).join();
            long prewarmTimeMs = (System.nanoTime() - prewarmStart) / 1_000_000;

            var afterPrewarmStats = searcher.getCacheStats();
            System.out.println("   Prewarm completed in " + prewarmTimeMs + " ms");
            System.out.println("   Cache size after prewarm: " + afterPrewarmStats.getTotalSize() + " bytes");
            System.out.println("   Misses during prewarm: " + (afterPrewarmStats.getMissCount() - initialStats.getMissCount()) + "\n");

            // Now query multiple different terms - should have ZERO new cache misses
            System.out.println("Step 4c: Querying different terms (should have ZERO cache misses)");
            System.out.println("━".repeat(60));

            long missesBeforeQueries = afterPrewarmStats.getMissCount();
            long hitsBeforeQueries = afterPrewarmStats.getHitCount();

            for (int i = 0; i < testTerms.size(); i++) {
                String term = testTerms.get(i);
                var statsBefore = searcher.getCacheStats();
                long missesBefore = statsBefore.getMissCount();

                long startTime = System.nanoTime();
                SplitQuery query = new SplitTermQuery("content", term);
                SearchResult result = searcher.search(query, 10);
                long queryTimeMs = (System.nanoTime() - startTime) / 1_000_000;

                var statsAfter = searcher.getCacheStats();
                long newMisses = statsAfter.getMissCount() - missesBefore;

                String indicator = newMisses == 0 ? "✅ PASS" : "❌ FAIL (unexpected misses!)";

                System.out.printf("   Term %d: \"%s\"%n", i + 1, term.length() > 20 ? term.substring(0, 20) + "..." : term);
                System.out.printf("      Hits: %d | Time: %d ms | New Misses: %d | %s%n%n",
                                 result.getHits().size(), queryTimeMs, newMisses, indicator);
            }

            // Also test a different field (title) - should still be prewarmed
            System.out.println("Step 4d: Testing different field (title) - should also be prewarmed");
            System.out.println("━".repeat(60));

            var statsBeforeTitle = searcher.getCacheStats();
            long missesBeforeTitle = statsBeforeTitle.getMissCount();

            String testTerm = testTerms.get(2);
            SplitQuery titleQuery = new SplitTermQuery("title", testTerm);
            SearchResult titleResult = searcher.search(titleQuery, 10);

            var statsAfterTitle = searcher.getCacheStats();
            long titleNewMisses = statsAfterTitle.getMissCount() - missesBeforeTitle;

            String titleIndicator = titleNewMisses == 0 ? "✅ PASS" : "❌ FAIL (unexpected misses!)";
            System.out.println("   Query: term \"" + testTerm + "\" in field \"title\"");
            System.out.println("   Hits: " + titleResult.getHits().size());
            System.out.println("   New cache misses: " + titleNewMisses + " | " + titleIndicator + "\n");

            // Summary
            var finalStats = searcher.getCacheStats();
            long totalNewMisses = finalStats.getMissCount() - missesBeforeQueries;
            long totalNewHits = finalStats.getHitCount() - hitsBeforeQueries;

            System.out.println("=".repeat(80));
            System.out.println("📊 PREWARM FEATURE SUMMARY");
            System.out.println("=".repeat(80));
            System.out.println("   Prewarm time: " + prewarmTimeMs + " ms");
            System.out.println("   Cache size after prewarm: " + afterPrewarmStats.getTotalSize() + " bytes");
            System.out.println("   Queries executed: " + (testTerms.size() + 1) + " (across content + title fields)");
            System.out.println("   New cache misses after prewarm: " + totalNewMisses);
            System.out.println("   Cache hits after prewarm: " + totalNewHits);

            if (totalNewMisses == 0) {
                System.out.println("\n   🎉 SUCCESS: TERM prewarm eliminated all cache misses!");
                System.out.println("      The FST is now fully cached, and different terms are served");
                System.out.println("      from cache via sub-range coalescing.");
            } else {
                System.out.println("\n   ⚠️  Some cache misses occurred. This may indicate:");
                System.out.println("      - Postings/positions data being fetched (separate from FST)");
                System.out.println("      - Other components not included in TERM prewarm");
            }

            System.out.println("\n" + "=".repeat(80));
            System.out.println("PREWARM TEST COMPLETE");
            System.out.println("=".repeat(80));
        }
    }

    // Helper class to track test results
    private static class CacheTestResult {
        final int round;
        final int termIndex;
        final String term;
        final int hitCount;
        final long queryTimeMs;
        final long newMisses;
        final long newHits;
        final long newBytes;

        CacheTestResult(int round, int termIndex, String term, int hitCount,
                       long queryTimeMs, long newMisses, long newHits, long newBytes) {
            this.round = round;
            this.termIndex = termIndex;
            this.term = term;
            this.hitCount = hitCount;
            this.queryTimeMs = queryTimeMs;
            this.newMisses = newMisses;
            this.newHits = newHits;
            this.newBytes = newBytes;
        }
    }

    // Generate a random term (8-15 lowercase letters)
    private static String generateRandomTerm() {
        int length = 8 + random.nextInt(8); // 8-15 characters
        StringBuilder sb = new StringBuilder(length);
        for (int i = 0; i < length; i++) {
            sb.append((char) ('a' + random.nextInt(26)));
        }
        return sb.toString();
    }

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

    @Test
    @Order(5)
    @DisplayName("Step 5: Test preloading ALL components (TERM, POSTINGS, FIELDNORM, FASTFIELD)")
    void step5_testPreloadAllComponents() throws Exception {
        System.out.println("\n" + "=".repeat(80));
        System.out.println("🔥 TESTING MULTI-COMPONENT PREWARM (ALL COMPONENTS)");
        System.out.println("=".repeat(80));
        System.out.println("\nThis test verifies that all index components can be prewarmed together:\n");
        System.out.println("  - TERM: Term dictionaries (FST) for text search");
        System.out.println("  - POSTINGS: Posting lists for term lookups");
        System.out.println("  - FIELDNORM: Field norms for scoring");
        System.out.println("  - FASTFIELD: Fast fields for sorting/filtering\n");

        assertNotNull(splitS3Url, "Split S3 URL should be available from Step 2");
        assertNotNull(splitMetadata, "Split metadata should be available from Step 1");
        assertTrue(generatedTerms.size() > 0, "Generated terms should be available from Step 1");

        // Create a NEW cache manager with fresh cache
        String cacheName = "multi-component-prewarm-test-" + System.currentTimeMillis();
        SplitCacheManager.CacheConfig cacheConfig = new SplitCacheManager.CacheConfig(cacheName)
            .withMaxCacheSize(500_000_000); // 500MB cache

        String accessKey = getAccessKey();
        String secretKey = getSecretKey();
        if (accessKey != null && secretKey != null) {
            cacheConfig = cacheConfig.withAwsCredentials(accessKey, secretKey);
        }
        cacheConfig = cacheConfig.withAwsRegion(TEST_REGION);

        SplitCacheManager cacheManager = SplitCacheManager.getInstance(cacheConfig);

        // Test terms for validation
        List<String> testTerms = Arrays.asList(
            generatedTerms.get(0),
            generatedTerms.get(generatedTerms.size() / 2),
            generatedTerms.get(generatedTerms.size() - 1)
        );

        try (SplitSearcher searcher = cacheManager.createSplitSearcher(splitS3Url, splitMetadata)) {

            System.out.println("Step 5a: Initial cache stats (before prewarm)");
            var initialStats = searcher.getCacheStats();
            System.out.println("   Hits: " + initialStats.getHitCount());
            System.out.println("   Misses: " + initialStats.getMissCount());
            System.out.println("   Size: " + initialStats.getTotalSize() + " bytes\n");

            // PREWARM ALL components
            System.out.println("Step 5b: Prewarming ALL components...");
            long prewarmStart = System.nanoTime();

            // Prewarm all available components
            searcher.preloadComponents(
                SplitSearcher.IndexComponent.TERM,
                SplitSearcher.IndexComponent.POSTINGS,
                SplitSearcher.IndexComponent.FIELDNORM,
                SplitSearcher.IndexComponent.FASTFIELD
            ).join();

            long prewarmTimeMs = (System.nanoTime() - prewarmStart) / 1_000_000;

            var afterPrewarmStats = searcher.getCacheStats();
            System.out.println("   Prewarm completed in " + prewarmTimeMs + " ms");
            System.out.println("   Cache size after prewarm: " + afterPrewarmStats.getTotalSize() + " bytes");
            System.out.println("   Total cache operations: " + afterPrewarmStats.getMissCount() + " misses, " +
                             afterPrewarmStats.getHitCount() + " hits\n");

            // Verify no exceptions occurred and prewarm completed
            assertTrue(prewarmTimeMs > 0, "Prewarm should have taken some time");
            assertTrue(afterPrewarmStats.getTotalSize() >= initialStats.getTotalSize(),
                "Cache size should increase after prewarm");

            // Test queries after prewarm
            System.out.println("Step 5c: Testing queries after multi-component prewarm");
            System.out.println("━".repeat(60));

            long missesBeforeQueries = afterPrewarmStats.getMissCount();

            for (int i = 0; i < testTerms.size(); i++) {
                String term = testTerms.get(i);
                var statsBefore = searcher.getCacheStats();
                long missesBefore = statsBefore.getMissCount();

                long startTime = System.nanoTime();
                SplitQuery query = new SplitTermQuery("content", term);
                SearchResult result = searcher.search(query, 10);
                long queryTimeMs = (System.nanoTime() - startTime) / 1_000_000;

                var statsAfter = searcher.getCacheStats();
                long newMisses = statsAfter.getMissCount() - missesBefore;

                System.out.printf("   Term %d: \"%s\" -> %d hits in %d ms (new misses: %d)%n",
                                 i + 1, term.length() > 15 ? term.substring(0, 15) + "..." : term,
                                 result.getHits().size(), queryTimeMs, newMisses);
            }

            // Summary
            var finalStats = searcher.getCacheStats();
            long totalNewMisses = finalStats.getMissCount() - missesBeforeQueries;

            System.out.println("\n" + "=".repeat(80));
            System.out.println("📊 MULTI-COMPONENT PREWARM SUMMARY");
            System.out.println("=".repeat(80));
            System.out.println("   Prewarm time: " + prewarmTimeMs + " ms");
            System.out.println("   Cache size after prewarm: " + afterPrewarmStats.getTotalSize() + " bytes");
            System.out.println("   Queries executed: " + testTerms.size());
            System.out.println("   New cache misses after prewarm: " + totalNewMisses);
            System.out.println("   Final hit rate: " + String.format("%.1f%%",
                finalStats.getHitCount() * 100.0 / Math.max(1, finalStats.getHitCount() + finalStats.getMissCount())));

            System.out.println("\n   🎉 SUCCESS: Multi-component prewarm completed without errors!");
            System.out.println("      All components (TERM, POSTINGS, FIELDNORM, FASTFIELD) were prewarmed.");

            System.out.println("\n" + "=".repeat(80));
            System.out.println("MULTI-COMPONENT PREWARM TEST COMPLETE");
            System.out.println("=".repeat(80));
        }
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
                System.out.println("✅ Created S3 bucket: " + TEST_BUCKET);
            } catch (BucketAlreadyExistsException | BucketAlreadyOwnedByYouException ex) {
                // OK
            }
        } catch (Exception e) {
            // Ignore other errors, test will fail later if bucket is truly inaccessible
        }
    }
}

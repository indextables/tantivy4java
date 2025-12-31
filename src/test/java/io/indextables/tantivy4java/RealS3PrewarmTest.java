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

import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;
import software.amazon.awssdk.core.sync.RequestBody;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests the TERM prewarm feature with real S3 storage.
 *
 * This test verifies that preloading the TERM component (FST/term dictionaries)
 * eliminates cache misses when querying different terms in the same field.
 *
 * Prerequisites:
 * - AWS credentials configured (AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY)
 * - Or ~/.aws/credentials file with [default] profile
 *
 * Run with: TANTIVY4JAVA_DEBUG=1 to see detailed cache behavior
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class RealS3PrewarmTest {

    // Test configuration
    private static final String TEST_BUCKET = System.getProperty("test.s3.bucket", "tantivy4java-testing");
    private static final String TEST_REGION = System.getProperty("test.s3.region", "us-east-2");
    private static final String ACCESS_KEY = System.getProperty("test.s3.accessKey");
    private static final String SECRET_KEY = System.getProperty("test.s3.secretKey");

    // Number of unique terms to generate
    private static final int NUM_UNIQUE_TERMS = 10_000;
    private static final int NUM_DOCUMENTS = 5_000;

    // AWS credentials
    private static String awsAccessKey;
    private static String awsSecretKey;

    @TempDir
    static Path tempDir;

    private static S3Client s3Client;
    private static String splitS3Url;
    private static QuickwitSplit.SplitMetadata splitMetadata;
    private static List<String> generatedTerms = new ArrayList<>();
    private static Random random = new Random(42);

    @BeforeAll
    static void setup() {
        loadAwsCredentials();

        String accessKey = getAccessKey();
        String secretKey = getSecretKey();

        if (accessKey == null || secretKey == null) {
            System.out.println("No AWS credentials found. Skipping S3 prewarm tests.");
            Assumptions.abort("AWS credentials not available");
        }

        s3Client = S3Client.builder()
            .region(Region.of(TEST_REGION))
            .credentialsProvider(StaticCredentialsProvider.create(
                AwsBasicCredentials.create(accessKey, secretKey)))
            .build();

        System.out.println("S3 client initialized for prewarm test");
        System.out.println("   Bucket: " + TEST_BUCKET);
        System.out.println("   Region: " + TEST_REGION);

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

        Path indexPath = tempDir.resolve("s3-prewarm-test-index");

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
        Path splitPath = tempDir.resolve("s3-prewarm-test.split");
        QuickwitSplit.SplitConfig config = new QuickwitSplit.SplitConfig(
            "s3-prewarm-test-index", "test-source", "test-node"
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
    @DisplayName("Step 2: Upload split to S3")
    void step2_uploadToS3() throws IOException {
        Path localSplitPath = tempDir.resolve("s3-prewarm-test.split");
        assertTrue(Files.exists(localSplitPath), "Split file should exist from Step 1");

        String s3Key = "prewarm-test/s3-prewarm-" + System.currentTimeMillis() + ".split";
        splitS3Url = String.format("s3://%s/%s", TEST_BUCKET, s3Key);

        System.out.println("Uploading split to S3: " + splitS3Url);

        PutObjectRequest putRequest = PutObjectRequest.builder()
            .bucket(TEST_BUCKET)
            .key(s3Key)
            .build();

        s3Client.putObject(putRequest, localSplitPath);

        HeadObjectResponse headResponse = s3Client.headObject(
            HeadObjectRequest.builder().bucket(TEST_BUCKET).key(s3Key).build());

        System.out.println("Split uploaded: " + headResponse.contentLength() + " bytes");
    }

    @Test
    @Order(3)
    @DisplayName("Step 3: Test TERM prewarm eliminates cache misses on S3")
    void step3_testTermPrewarm() throws Exception {
        System.out.println("\n" + "=".repeat(70));
        System.out.println("TESTING TERM PREWARM WITH S3");
        System.out.println("=".repeat(70));

        assertNotNull(splitS3Url, "Split S3 URL should be available");
        assertNotNull(splitMetadata, "Split metadata should be available");
        assertTrue(generatedTerms.size() > 0, "Generated terms should be available");

        // Create cache manager with fresh cache
        String cacheName = "s3-prewarm-test-" + System.currentTimeMillis();
        SplitCacheManager.CacheConfig cacheConfig = new SplitCacheManager.CacheConfig(cacheName)
            .withMaxCacheSize(500_000_000);

        String accessKey = getAccessKey();
        String secretKey = getSecretKey();
        if (accessKey != null && secretKey != null) {
            cacheConfig = cacheConfig.withAwsCredentials(accessKey, secretKey);
        }
        cacheConfig = cacheConfig.withAwsRegion(TEST_REGION);

        SplitCacheManager cacheManager = SplitCacheManager.getInstance(cacheConfig);

        // Select test terms from different parts of the FST
        List<String> testTerms = Arrays.asList(
            generatedTerms.get(0),
            generatedTerms.get(generatedTerms.size() / 4),
            generatedTerms.get(generatedTerms.size() / 2),
            generatedTerms.get(3 * generatedTerms.size() / 4),
            generatedTerms.get(generatedTerms.size() - 1)
        );

        try (SplitSearcher searcher = cacheManager.createSplitSearcher(splitS3Url, splitMetadata)) {

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

    @Test
    @Order(4)
    @DisplayName("Step 4: Test STORE prewarm works correctly on S3")
    void step4_testStorePrewarm() throws Exception {
        System.out.println("\n" + "=".repeat(70));
        System.out.println("TESTING STORE PREWARM WITH S3");
        System.out.println("=".repeat(70));

        assertNotNull(splitS3Url, "Split S3 URL should be available");
        assertNotNull(splitMetadata, "Split metadata should be available");

        // Create cache manager with fresh cache
        String cacheName = "s3-store-prewarm-test-" + System.currentTimeMillis();
        SplitCacheManager.CacheConfig cacheConfig = new SplitCacheManager.CacheConfig(cacheName)
            .withMaxCacheSize(500_000_000);

        String accessKey = getAccessKey();
        String secretKey = getSecretKey();
        if (accessKey != null && secretKey != null) {
            cacheConfig = cacheConfig.withAwsCredentials(accessKey, secretKey);
        }
        cacheConfig = cacheConfig.withAwsRegion(TEST_REGION);

        SplitCacheManager cacheManager = SplitCacheManager.getInstance(cacheConfig);

        try (SplitSearcher searcher = cacheManager.createSplitSearcher(splitS3Url, splitMetadata)) {

            // Get initial stats
            var initialStats = searcher.getCacheStats();
            System.out.println("Initial cache: hits=" + initialStats.getHitCount() +
                             ", misses=" + initialStats.getMissCount());

            // PREWARM the STORE component - this is the critical test
            System.out.println("\nPrewarming STORE component...");
            long prewarmStart = System.nanoTime();

            try {
                searcher.preloadComponents(SplitSearcher.IndexComponent.STORE).join();
                long prewarmTimeMs = (System.nanoTime() - prewarmStart) / 1_000_000;
                System.out.println("STORE prewarm completed in " + prewarmTimeMs + " ms");

                var afterPrewarmStats = searcher.getCacheStats();
                System.out.println("Cache after prewarm: size=" + afterPrewarmStats.getTotalSize() + " bytes");

                // Now retrieve some documents - should have minimal cache misses
                System.out.println("\nRetrieving documents after STORE prewarm...");
                long missesBeforeRetrieval = afterPrewarmStats.getMissCount();

                // Query to get some documents
                SplitQuery query = new SplitTermQuery("content", generatedTerms.get(0));
                SearchResult result = searcher.search(query, 5);

                int docCount = 0;
                for (var hit : result.getHits()) {
                    try (Document doc = searcher.doc(hit.getDocAddress())) {
                        // Access the stored content field to trigger store read
                        Object content = doc.getFirst("content");
                        if (content != null) {
                            docCount++;
                        }
                    }
                }

                var finalStats = searcher.getCacheStats();
                long newStoreMisses = finalStats.getMissCount() - missesBeforeRetrieval;

                System.out.println("   Retrieved " + docCount + " documents");
                System.out.println("   New cache misses during retrieval: " + newStoreMisses);

                System.out.println("\n" + "=".repeat(70));
                System.out.println("STORE PREWARM TEST SUMMARY");
                System.out.println("=".repeat(70));
                System.out.println("   STORE prewarm: SUCCESS (no errors)");
                System.out.println("   Documents retrieved: " + docCount);
                System.out.println("   New cache misses: " + newStoreMisses);
                System.out.println("=".repeat(70));

            } catch (Exception e) {
                long prewarmTimeMs = (System.nanoTime() - prewarmStart) / 1_000_000;
                System.out.println("STORE prewarm FAILED after " + prewarmTimeMs + " ms");
                System.out.println("Error: " + e.getMessage());
                e.printStackTrace();
                fail("STORE prewarm should not fail: " + e.getMessage());
            }
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
                System.out.println("Loaded AWS credentials from ~/.aws/credentials");
            }
        } catch (Exception e) {
            System.out.println("Failed to read AWS credentials: " + e.getMessage());
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
                System.out.println("Created S3 bucket: " + TEST_BUCKET);
            } catch (BucketAlreadyExistsException | BucketAlreadyOwnedByYouException ex) {
                // OK
            }
        } catch (Exception e) {
            // Ignore other errors
        }
    }
}

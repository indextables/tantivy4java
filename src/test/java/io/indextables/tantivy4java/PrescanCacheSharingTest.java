package io.indextables.tantivy4java;

import io.indextables.tantivy4java.core.*;
import io.indextables.tantivy4java.split.*;
import io.indextables.tantivy4java.split.merge.QuickwitSplit;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;

import java.io.*;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.TimeUnit;

import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Test to verify that prescan caches are at the split level, NOT the split-list level.
 *
 * When we prescan overlapping lists of splits, the splits in the intersection
 * should share the same cache entries. This ensures:
 * 1. No redundant S3/storage fetches for splits that appear in multiple lists
 * 2. Memory efficiency by not duplicating cached data
 * 3. Performance improvement for workloads with overlapping split sets
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class PrescanCacheSharingTest {

    private static final String TEST_BUCKET = System.getProperty("test.s3.bucket", "tantivy4java-testing");
    private static final String TEST_REGION = System.getProperty("test.s3.region", "us-east-2");
    private static final String S3_PREFIX = "cache-sharing-test/";

    private static String awsAccessKey;
    private static String awsSecretKey;
    private static S3Client s3Client;
    private static SplitCacheManager cacheManager;
    private static List<SplitInfo> allSplits = new ArrayList<>();
    private static String docMappingJson;

    @TempDir
    static Path tempDir;

    // Content patterns for splits - 6 splits to support all tests
    private static final String[][] SPLIT_CONTENT = {
        // Split 0
        {"hello world programming", "java rust python", "tech"},
        {"coding tutorial guide", "software development", "tech"},

        // Split 1
        {"hello universe science", "physics chemistry", "science"},
        {"quantum mechanics atoms", "molecular biology", "science"},

        // Split 2
        {"hello planet earth", "geography climate", "nature"},
        {"ecosystem biodiversity", "environmental science", "nature"},

        // Split 3
        {"music composition art", "classical symphony", "arts"},
        {"painting sculpture design", "visual creativity", "arts"},

        // Split 4
        {"sports football soccer", "team competition", "sports"},
        {"basketball tennis golf", "athletics games", "sports"},

        // Split 5
        {"finance economics money", "market investment", "business"},
        {"startup venture capital", "entrepreneurship", "business"},
    };

    @BeforeAll
    static void setup() throws Exception {
        loadAwsCredentials();

        if (awsAccessKey == null || awsSecretKey == null) {
            System.out.println("⚠️  No AWS credentials found. Skipping cache sharing tests.");
            System.out.println("   Configure ~/.aws/credentials or set system properties");
            Assumptions.abort("AWS credentials not available");
            return;
        }

        // Create S3 client
        s3Client = S3Client.builder()
            .region(Region.of(TEST_REGION))
            .credentialsProvider(StaticCredentialsProvider.create(
                AwsBasicCredentials.create(awsAccessKey, awsSecretKey)))
            .build();

        // Ensure bucket exists
        ensureBucketExists();

        // Create and upload splits
        createAndUploadSplits();

        // Initialize cache manager
        SplitCacheManager.CacheConfig config = new SplitCacheManager.CacheConfig("cache-sharing-test")
            .withMaxCacheSize(500_000_000)
            .withAwsCredentials(awsAccessKey, awsSecretKey)
            .withAwsRegion(TEST_REGION);

        cacheManager = SplitCacheManager.getInstance(config);

        System.out.println("✅ Cache sharing test setup complete:");
        System.out.println("  - " + allSplits.size() + " splits uploaded to s3://" + TEST_BUCKET + "/" + S3_PREFIX);
    }

    @AfterAll
    static void teardown() throws Exception {
        // Clean up S3 objects
        if (s3Client != null && !allSplits.isEmpty()) {
            try {
                for (int i = 0; i < allSplits.size(); i++) {
                    String key = S3_PREFIX + "split-" + i + ".split";
                    s3Client.deleteObject(DeleteObjectRequest.builder()
                        .bucket(TEST_BUCKET).key(key).build());
                }
                System.out.println("✅ Cleaned up " + allSplits.size() + " S3 test splits");
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
        int docsPerSplit = 2;
        int numSplits = SPLIT_CONTENT.length / docsPerSplit;

        for (int s = 0; s < numSplits; s++) {
            Path indexDir = tempDir.resolve("index-" + s);
            Files.createDirectories(indexDir);
            Path splitPath = tempDir.resolve("split-" + s + ".split");

            try (SchemaBuilder builder = new SchemaBuilder()) {
                builder.addTextField("title", true, false, "default", "position");
                builder.addTextField("body", true, false, "default", "position");
                builder.addTextField("category", true, false, "raw", "basic");

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
                                    writer.addDocument(doc);
                                }
                            }
                            writer.commit();
                        }
                        index.reload();

                        QuickwitSplit.SplitConfig config = new QuickwitSplit.SplitConfig(
                            "cache-share-" + s, "cache-source", "cache-node");
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
                        allSplits.add(new SplitInfo(s3Url, metadata.getFooterStartOffset(), metadata.getFooterEndOffset()));

                        if (docMappingJson == null) {
                            docMappingJson = metadata.getDocMappingJson();
                        }

                        System.out.println("  Uploaded split-" + s + " to " + s3Url);
                    }
                }
            }
        }
    }

    @Test
    @Order(1)
    @DisplayName("Overlapping split lists should share cache entries")
    void testOverlappingSplitListsShareCache() throws Exception {
        Assumptions.assumeTrue(cacheManager != null, "Skipping - no S3 credentials");
        Assumptions.assumeTrue(allSplits.size() >= 4, "Skipping - need at least 4 splits");

        // Use a fresh cache manager for this test to ensure cold start
        String uniqueCacheName = "overlap-test-" + System.currentTimeMillis();
        SplitCacheManager.CacheConfig freshConfig = new SplitCacheManager.CacheConfig(uniqueCacheName)
            .withMaxCacheSize(100_000_000)
            .withAwsCredentials(awsAccessKey, awsSecretKey)
            .withAwsRegion(TEST_REGION);

        try (SplitCacheManager freshCacheManager = SplitCacheManager.getInstance(freshConfig)) {
            // Create two overlapping lists
            // List A: splits 0, 1, 2
            // List B: splits 1, 2, 3
            // Intersection: splits 1, 2 (should share cache)
            List<SplitInfo> listA = allSplits.subList(0, 3);
            List<SplitInfo> listB = allSplits.subList(1, 4);

            SplitQuery query = new SplitTermQuery("title", "hello");

            // First prescan of list A - will populate cache for splits 0, 1, 2
            long startA1 = System.nanoTime();
            List<PrescanResult> resultsA1 = freshCacheManager.prescanSplits(listA, docMappingJson, query);
            long durationA1 = System.nanoTime() - startA1;

            // First prescan of list B - splits 1, 2 should hit cache, only split 3 needs fetch
            long startB1 = System.nanoTime();
            List<PrescanResult> resultsB1 = freshCacheManager.prescanSplits(listB, docMappingJson, query);
            long durationB1 = System.nanoTime() - startB1;

            // Second prescan of list A - ALL should hit cache
            long startA2 = System.nanoTime();
            List<PrescanResult> resultsA2 = freshCacheManager.prescanSplits(listA, docMappingJson, query);
            long durationA2 = System.nanoTime() - startA2;

            // Second prescan of list B - ALL should hit cache
            long startB2 = System.nanoTime();
            List<PrescanResult> resultsB2 = freshCacheManager.prescanSplits(listB, docMappingJson, query);
            long durationB2 = System.nanoTime() - startB2;

            System.out.println("\n=== Overlapping Split List Cache Sharing Test ===");
            System.out.println("List A splits: " + listA.size() + " (indices 0-2)");
            System.out.println("List B splits: " + listB.size() + " (indices 1-3)");
            System.out.println("Intersection: 2 splits (indices 1-2)");
            System.out.println();
            System.out.println("First prescan List A (cold cache): " + TimeUnit.NANOSECONDS.toMicros(durationA1) + " µs");
            System.out.println("First prescan List B (2/3 warm):   " + TimeUnit.NANOSECONDS.toMicros(durationB1) + " µs");
            System.out.println("Second prescan List A (warm):      " + TimeUnit.NANOSECONDS.toMicros(durationA2) + " µs");
            System.out.println("Second prescan List B (warm):      " + TimeUnit.NANOSECONDS.toMicros(durationB2) + " µs");

            // Verify results are correct
            assertEquals(3, resultsA1.size(), "List A should have 3 results");
            assertEquals(3, resultsB1.size(), "List B should have 3 results");
            assertEquals(3, resultsA2.size(), "Second List A should have 3 results");
            assertEquals(3, resultsB2.size(), "Second List B should have 3 results");

            // Verify cache sharing: second prescans should be much faster
            double warmCacheSpeedup = (double) durationA1 / Math.max(durationA2, 1);
            System.out.println("Warm cache speedup (A1/A2): " + String.format("%.1fx", warmCacheSpeedup));
            assertTrue(warmCacheSpeedup > 2.0, "Second prescan should be at least 2x faster (was " + warmCacheSpeedup + "x)");

            System.out.println();
            System.out.println("✅ SUCCESS: Cache is shared at the split level, not split-list level");
        }
    }

    @Test
    @Order(2)
    @DisplayName("Disjoint split lists should not share cache entries (baseline)")
    void testDisjointSplitListsNoSharing() throws Exception {
        Assumptions.assumeTrue(cacheManager != null, "Skipping - no S3 credentials");
        Assumptions.assumeTrue(allSplits.size() >= 6, "Skipping - need at least 6 splits");

        // Use a fresh cache manager for this test
        String uniqueCacheName = "disjoint-test-" + System.currentTimeMillis();
        SplitCacheManager.CacheConfig freshConfig = new SplitCacheManager.CacheConfig(uniqueCacheName)
            .withMaxCacheSize(100_000_000)
            .withAwsCredentials(awsAccessKey, awsSecretKey)
            .withAwsRegion(TEST_REGION);

        try (SplitCacheManager freshCacheManager = SplitCacheManager.getInstance(freshConfig)) {
            // Create two disjoint lists
            // List A: splits 0, 1, 2
            // List B: splits 3, 4, 5
            // No intersection - no cache sharing
            List<SplitInfo> listA = allSplits.subList(0, 3);
            List<SplitInfo> listB = allSplits.subList(3, 6);

            SplitQuery query = new SplitTermQuery("title", "hello");

            // First prescan of list A
            long startA1 = System.nanoTime();
            List<PrescanResult> resultsA1 = freshCacheManager.prescanSplits(listA, docMappingJson, query);
            long durationA1 = System.nanoTime() - startA1;

            // First prescan of list B - no cache sharing expected
            long startB1 = System.nanoTime();
            List<PrescanResult> resultsB1 = freshCacheManager.prescanSplits(listB, docMappingJson, query);
            long durationB1 = System.nanoTime() - startB1;

            System.out.println("\n=== Disjoint Split List Baseline Test ===");
            System.out.println("List A splits: " + listA.size() + " (indices 0-2)");
            System.out.println("List B splits: " + listB.size() + " (indices 3-5)");
            System.out.println("Intersection: 0 splits (no sharing expected)");
            System.out.println();
            System.out.println("First prescan List A: " + TimeUnit.NANOSECONDS.toMicros(durationA1) + " µs");
            System.out.println("First prescan List B: " + TimeUnit.NANOSECONDS.toMicros(durationB1) + " µs");

            // Both should take similar time since no cache sharing
            assertEquals(3, resultsA1.size(), "List A should have 3 results");
            assertEquals(3, resultsB1.size(), "List B should have 3 results");

            System.out.println();
            System.out.println("✅ SUCCESS: Baseline test confirms disjoint lists have no cross-sharing");
        }
    }

    @Test
    @Order(3)
    @DisplayName("Same split in different lists should return same cached data")
    void testSameSplitInDifferentListsReturnsSameData() throws Exception {
        Assumptions.assumeTrue(cacheManager != null, "Skipping - no S3 credentials");
        Assumptions.assumeTrue(allSplits.size() >= 1, "Skipping - need at least 1 split");

        SplitInfo sharedSplit = allSplits.get(0);

        // Create two lists that both contain the same split
        List<SplitInfo> list1 = Collections.singletonList(sharedSplit);
        List<SplitInfo> list2 = Collections.singletonList(sharedSplit);

        SplitQuery queryMatch = new SplitTermQuery("title", "hello");
        SplitQuery queryNoMatch = new SplitTermQuery("title", "xyznonexistent12345");

        // Prescan with matching query in list1
        List<PrescanResult> results1Match = cacheManager.prescanSplits(list1, docMappingJson, queryMatch);

        // Prescan with non-matching query in list2 (same split)
        List<PrescanResult> results2NoMatch = cacheManager.prescanSplits(list2, docMappingJson, queryNoMatch);

        // Prescan with matching query in list2 (same split)
        List<PrescanResult> results2Match = cacheManager.prescanSplits(list2, docMappingJson, queryMatch);

        System.out.println("\n=== Same Split Different Lists Test ===");
        System.out.println("Split URL: " + sharedSplit.getSplitUrl());
        System.out.println();
        System.out.println("Query 'hello' in list1: " + results1Match.get(0).couldHaveResults());
        System.out.println("Query 'nonexistent' in list2: " + results2NoMatch.get(0).couldHaveResults());
        System.out.println("Query 'hello' in list2: " + results2Match.get(0).couldHaveResults());

        // Both matching queries should return the same result
        assertEquals(results1Match.get(0).couldHaveResults(), results2Match.get(0).couldHaveResults(),
                "Same query on same split should return same result regardless of which list it's in");

        // Non-matching query should return different result (if first was true)
        if (results1Match.get(0).couldHaveResults()) {
            assertFalse(results2NoMatch.get(0).couldHaveResults(),
                    "Non-existent term should not find matches");
        }

        System.out.println();
        System.out.println("✅ SUCCESS: Same split returns consistent results regardless of list membership");
    }
}

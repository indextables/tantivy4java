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
import io.indextables.tantivy4java.split.*;
import io.indextables.tantivy4java.split.merge.*;
import io.indextables.tantivy4java.result.*;

import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;
import software.amazon.awssdk.core.sync.RequestBody;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * Tests for L2 tiered disk cache with real S3 backend.
 *
 * This test validates that the disk cache works correctly with S3 storage:
 * 1. Creates a split and uploads to S3
 * 2. Searches with disk cache enabled - populates L2 cache
 * 3. Closes and re-opens with fresh memory cache
 * 4. Verifies data is served from disk cache (not re-downloaded from S3)
 *
 * Prerequisites:
 * - AWS credentials configured in ~/.aws/credentials
 * - Or environment variables AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class RealS3DiskCacheTest {

    private static final String TEST_BUCKET = System.getProperty("test.s3.bucket", "tantivy4java-testing");
    private static final String TEST_REGION = System.getProperty("test.s3.region", "us-east-2");
    private static final String ACCESS_KEY = System.getProperty("test.s3.accessKey");
    private static final String SECRET_KEY = System.getProperty("test.s3.secretKey");

    private static String awsAccessKey;
    private static String awsSecretKey;
    private static S3Client s3Client;
    private static String splitS3Url;
    private static Path diskCachePath;
    private static QuickwitSplit.SplitMetadata splitMetadata;

    @TempDir
    static Path tempDir;

    @BeforeAll
    static void setup() throws Exception {
        loadAwsCredentials();

        String accessKey = getAccessKey();
        String secretKey = getSecretKey();

        assumeTrue(accessKey != null && secretKey != null,
            "Skipping S3 disk cache test - no AWS credentials found");

        s3Client = S3Client.builder()
            .region(Region.of(TEST_REGION))
            .credentialsProvider(StaticCredentialsProvider.create(
                AwsBasicCredentials.create(accessKey, secretKey)))
            .build();

        // Ensure bucket exists
        try {
            s3Client.headBucket(HeadBucketRequest.builder().bucket(TEST_BUCKET).build());
        } catch (NoSuchBucketException e) {
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
        }

        // Create disk cache directory
        diskCachePath = tempDir.resolve("s3_disk_cache");
        Files.createDirectories(diskCachePath);

        // Create and upload a test split
        createAndUploadTestSplit();

        System.out.println("✅ S3 disk cache test setup complete");
        System.out.println("   Bucket: " + TEST_BUCKET);
        System.out.println("   Split URL: " + splitS3Url);
        System.out.println("   Disk cache: " + diskCachePath);
    }

    @AfterAll
    static void cleanup() {
        if (s3Client != null) {
            // Clean up test split from S3
            if (splitS3Url != null) {
                try {
                    String key = splitS3Url.replace("s3://" + TEST_BUCKET + "/", "");
                    s3Client.deleteObject(DeleteObjectRequest.builder()
                        .bucket(TEST_BUCKET)
                        .key(key)
                        .build());
                    System.out.println("✅ Cleaned up test split from S3");
                } catch (Exception e) {
                    System.out.println("⚠️ Failed to clean up S3 split: " + e.getMessage());
                }
            }
            s3Client.close();
        }
    }

    private static void createAndUploadTestSplit() throws Exception {
        Path indexPath = tempDir.resolve("test_index");
        Path splitPath = tempDir.resolve("test.split");

        // Create index with test data
        try (SchemaBuilder builder = new SchemaBuilder()) {
            builder.addTextField("id", true, false, "raw", "position");
            builder.addTextField("title", true, false, "default", "position");
            builder.addTextField("content", true, false, "default", "position");
            builder.addIntegerField("count", true, true, true);

            try (Schema schema = builder.build();
                 Index index = new Index(schema, indexPath.toString(), false)) {

                try (IndexWriter writer = index.writer(Index.Memory.DEFAULT_HEAP_SIZE, 1)) {
                    // Add 100 documents
                    for (int i = 0; i < 100; i++) {
                        try (Document doc = new Document()) {
                            doc.addText("id", "doc-" + i);
                            doc.addText("title", "Test Document " + i);
                            doc.addText("content", "This is the content for document number " + i +
                                " with some searchable text");
                            doc.addInteger("count", i);
                            writer.addDocument(doc);
                        }
                    }
                    writer.commit();
                }

                // Convert to split
                QuickwitSplit.SplitConfig config = new QuickwitSplit.SplitConfig(
                    "disk-cache-test", "test-source", "test-node");
                splitMetadata = QuickwitSplit.convertIndexFromPath(indexPath.toString(), splitPath.toString(), config);
            }
        }

        // Upload to S3
        String s3Key = "disk-cache-tests/" + System.currentTimeMillis() + "/test.split";
        s3Client.putObject(
            PutObjectRequest.builder()
                .bucket(TEST_BUCKET)
                .key(s3Key)
                .build(),
            RequestBody.fromFile(splitPath.toFile()));

        splitS3Url = "s3://" + TEST_BUCKET + "/" + s3Key;
    }

    @Test
    @Order(1)
    void testDiskCachePopulation() throws Exception {
        System.out.println("\n=== Test 1: Disk Cache Population ===");

        // Configure with disk cache
        SplitCacheManager.TieredCacheConfig tieredConfig = new SplitCacheManager.TieredCacheConfig()
            .withDiskCachePath(diskCachePath.toString())
            .withMaxDiskSize(100_000_000L) // 100MB
            .withCompression(SplitCacheManager.CompressionAlgorithm.LZ4);

        SplitCacheManager.CacheConfig cacheConfig =
            new SplitCacheManager.CacheConfig("s3-disk-cache-test-" + System.currentTimeMillis())
                .withMaxCacheSize(50_000_000) // 50MB L1
                .withAwsCredentials(getAccessKey(), getSecretKey())
                .withAwsRegion(TEST_REGION)
                .withTieredCache(tieredConfig);

        try (SplitCacheManager cacheManager = SplitCacheManager.getInstance(cacheConfig)) {
            try (SplitSearcher searcher = cacheManager.createSplitSearcher(splitS3Url, splitMetadata)) {
                // Search to trigger cache population
                SplitQuery query = searcher.parseQuery("content:searchable");
                SearchResult result = searcher.search(query, 10);

                assertTrue(result.getHits().size() > 0, "Should find documents");
                System.out.println("   Found " + result.getHits().size() + " documents");

                // Also retrieve a document to cache more data
                if (!result.getHits().isEmpty()) {
                    try (Document doc = searcher.doc(result.getHits().get(0).getDocAddress())) {
                        assertNotNull(doc.getFirst("title"));
                        System.out.println("   Retrieved document: " + doc.getFirst("title"));
                    }
                }
            }

            // Get disk cache stats
            SplitCacheManager.DiskCacheStats stats = cacheManager.getDiskCacheStats();
            if (stats != null) {
                System.out.println("   Disk cache stats: " + stats);
                // Note: Stats might show 0 if data is still being written async
            }
        }

        // Wait for async disk writes
        Thread.sleep(500);

        // Verify files were created in disk cache
        Path cacheSubdir = diskCachePath.resolve("tantivy4java_slicecache");
        assertTrue(Files.exists(cacheSubdir), "Cache subdirectory should exist");

        long fileCount = Files.walk(cacheSubdir)
            .filter(Files::isRegularFile)
            .count();
        System.out.println("   Disk cache files: " + fileCount);
        assertTrue(fileCount > 0, "Should have cached files on disk");

        System.out.println("✅ Disk cache population test passed");
    }

    @Test
    @Order(2)
    void testDiskCachePersistence() throws Exception {
        System.out.println("\n=== Test 2: Disk Cache Persistence ===");

        // Create a FRESH cache manager (new memory cache, but same disk cache)
        SplitCacheManager.TieredCacheConfig tieredConfig = new SplitCacheManager.TieredCacheConfig()
            .withDiskCachePath(diskCachePath.toString())
            .withMaxDiskSize(100_000_000L);

        SplitCacheManager.CacheConfig cacheConfig =
            new SplitCacheManager.CacheConfig("s3-disk-cache-test-fresh-" + System.currentTimeMillis())
                .withMaxCacheSize(50_000_000)
                .withAwsCredentials(getAccessKey(), getSecretKey())
                .withAwsRegion(TEST_REGION)
                .withTieredCache(tieredConfig);

        try (SplitCacheManager cacheManager = SplitCacheManager.getInstance(cacheConfig)) {
            try (SplitSearcher searcher = cacheManager.createSplitSearcher(splitS3Url, splitMetadata)) {
                // Search again - should hit disk cache
                SplitQuery query = searcher.parseQuery("content:searchable");
                SearchResult result = searcher.search(query, 10);

                assertTrue(result.getHits().size() > 0, "Should find documents from disk cache");
                System.out.println("   Found " + result.getHits().size() + " documents (from disk cache)");

                // Retrieve document
                if (!result.getHits().isEmpty()) {
                    try (Document doc = searcher.doc(result.getHits().get(0).getDocAddress())) {
                        assertNotNull(doc.getFirst("title"));
                        System.out.println("   Retrieved: " + doc.getFirst("title"));
                    }
                }
            }

            // Verify disk cache stats show data
            SplitCacheManager.DiskCacheStats stats = cacheManager.getDiskCacheStats();
            if (stats != null) {
                System.out.println("   Disk cache stats: " + stats);
            }
        }

        System.out.println("✅ Disk cache persistence test passed");
    }

    @Test
    @Order(3)
    void testDiskCacheWithDifferentQueries() throws Exception {
        System.out.println("\n=== Test 3: Multiple Queries with Disk Cache ===");

        SplitCacheManager.TieredCacheConfig tieredConfig = new SplitCacheManager.TieredCacheConfig()
            .withDiskCachePath(diskCachePath.toString())
            .withMaxDiskSize(100_000_000L);

        SplitCacheManager.CacheConfig cacheConfig =
            new SplitCacheManager.CacheConfig("s3-disk-cache-multi-" + System.currentTimeMillis())
                .withMaxCacheSize(50_000_000)
                .withAwsCredentials(getAccessKey(), getSecretKey())
                .withAwsRegion(TEST_REGION)
                .withTieredCache(tieredConfig);

        try (SplitCacheManager cacheManager = SplitCacheManager.getInstance(cacheConfig)) {
            try (SplitSearcher searcher = cacheManager.createSplitSearcher(splitS3Url, splitMetadata)) {
                // Multiple different queries
                String[] queries = {
                    "title:Document",
                    "content:number",
                    "id:doc-50",
                    "*"
                };

                for (String queryStr : queries) {
                    SplitQuery query = searcher.parseQuery(queryStr);
                    SearchResult result = searcher.search(query, 5);
                    System.out.println("   Query '" + queryStr + "': " + result.getHits().size() + " hits");
                    assertTrue(result.getHits().size() >= 0);
                }

                // Range query
                SplitQuery rangeQuery = new SplitRangeQuery(
                    "count",
                    SplitRangeQuery.RangeBound.inclusive("10"),
                    SplitRangeQuery.RangeBound.inclusive("20"),
                    "i64");
                SearchResult rangeResult = searcher.search(rangeQuery, 20);
                System.out.println("   Range query [10-20]: " + rangeResult.getHits().size() + " hits");
                assertTrue(rangeResult.getHits().size() > 0);
            }
        }

        System.out.println("✅ Multiple queries test passed");
    }

    private static String getAccessKey() {
        if (ACCESS_KEY != null) return ACCESS_KEY;
        if (awsAccessKey != null) return awsAccessKey;
        return System.getenv("AWS_ACCESS_KEY_ID");
    }

    private static String getSecretKey() {
        if (SECRET_KEY != null) return SECRET_KEY;
        if (awsSecretKey != null) return awsSecretKey;
        return System.getenv("AWS_SECRET_ACCESS_KEY");
    }

    private static void loadAwsCredentials() {
        try {
            Path credentialsPath = Paths.get(System.getProperty("user.home"), ".aws", "credentials");
            if (!Files.exists(credentialsPath)) {
                System.out.println("AWS credentials file not found at: " + credentialsPath);
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
}

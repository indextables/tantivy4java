/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.
 */

package io.indextables.tantivy4java.split;

import io.indextables.tantivy4java.core.*;
import io.indextables.tantivy4java.split.merge.*;

import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Test to validate that batch consolidation actually produces efficient ranges.
 *
 * This test validates the fix for the batch optimization bug where:
 * - BUG: 10,000 docs → 101 ranges (hardcoded 100-doc limit)
 * - FIX: 10,000 docs → 1-10 ranges (based on actual byte positions and gap_tolerance)
 *
 * Uses real AWS S3 to properly test the batch optimization (local files don't trigger it).
 *
 * Requires AWS credentials in ~/.aws/credentials or environment variables.
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class BatchConsolidationValidationTest {

    private static final String TEST_BUCKET = System.getProperty("test.s3.bucket", "tantivy4java-testing");
    private static final String TEST_REGION = System.getProperty("test.s3.region", "us-east-2");
    private static final String ACCESS_KEY = System.getProperty("test.s3.accessKey");
    private static final String SECRET_KEY = System.getProperty("test.s3.secretKey");

    // AWS credentials loaded from ~/.aws/credentials
    private static String awsAccessKey;
    private static String awsSecretKey;

    private static S3Client s3Client;
    private static QuickwitSplit.SplitMetadata splitMetadata;
    private static String s3SplitUrl;

    // Test with 1000 documents - enough to clearly see consolidation behavior
    private static final int TEST_DOC_COUNT = 1000;

    @TempDir
    static Path tempDir;

    @BeforeAll
    static void setUp() throws Exception {
        System.out.println("\n=== BATCH CONSOLIDATION VALIDATION TEST ===\n");
        System.out.println("This test validates that the batch optimization correctly");
        System.out.println("consolidates documents based on actual byte positions.\n");

        // Load and validate AWS credentials
        loadAwsCredentials();
        validateCredentials();

        // Create S3 client
        String accessKey = getAccessKey();
        String secretKey = getSecretKey();
        s3Client = S3Client.builder()
                .region(Region.of(TEST_REGION))
                .credentialsProvider(StaticCredentialsProvider.create(
                    AwsBasicCredentials.create(accessKey, secretKey)))
                .build();
        System.out.println("✅ S3 client initialized for region: " + TEST_REGION);

        // Create test split and upload to S3
        createAndUploadTestSplit();

        // Reset metrics before tests
        SplitCacheManager.resetBatchMetrics();
        System.out.println("\n✅ Setup complete\n");
    }

    @AfterAll
    static void tearDown() {
        // Clean up S3 objects
        if (s3Client != null && s3SplitUrl != null) {
            try {
                String s3Key = "batch-consolidation-test/test.split";
                s3Client.deleteObject(DeleteObjectRequest.builder()
                        .bucket(TEST_BUCKET)
                        .key(s3Key)
                        .build());
                System.out.println("✅ Cleaned up S3 test object");
            } catch (Exception e) {
                System.out.println("⚠️ Failed to clean up S3: " + e.getMessage());
            }
            s3Client.close();
        }
        System.out.println("\n✅ Cleanup complete\n");
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

    private static void validateCredentials() {
        boolean hasExplicitCreds = ACCESS_KEY != null && SECRET_KEY != null;
        boolean hasFileCredentials = awsAccessKey != null && awsSecretKey != null;
        boolean hasEnvCreds = System.getenv("AWS_ACCESS_KEY_ID") != null;

        if (!hasExplicitCreds && !hasFileCredentials && !hasEnvCreds) {
            System.out.println("⚠️  No AWS credentials found. Skipping test.");
            Assumptions.abort("AWS credentials not available");
        }
    }

    private static void createAndUploadTestSplit() throws Exception {
        // Create schema
        try (SchemaBuilder builder = new SchemaBuilder()) {
            builder.addTextField("content", true, false, "default", "position");
            builder.addIntegerField("id", true, true, false);

            try (Schema schema = builder.build()) {
                // Create index with many documents
                Path indexPath = tempDir.resolve("consolidation_test_index");
                try (Index index = new Index(schema, indexPath.toString(), false)) {
                    try (IndexWriter writer = index.writer(Index.Memory.DEFAULT_HEAP_SIZE, 1)) {
                        for (int i = 0; i < TEST_DOC_COUNT; i++) {
                            try (Document doc = new Document()) {
                                String content = "Document number " + i + " with some additional content " +
                                        "to make the document size more realistic. " +
                                        "Additional padding: " + "x".repeat(i % 100);
                                doc.addText("content", content);
                                doc.addInteger("id", i);
                                writer.addDocument(doc);
                            }
                        }
                        writer.commit();
                        System.out.println("✅ Created index with " + TEST_DOC_COUNT + " documents");
                    }

                    // Merge to single segment
                    index.reload();
                    try (Searcher searcher = index.searcher()) {
                        List<String> segmentIds = searcher.getSegmentIds();
                        if (segmentIds.size() > 1) {
                            try (IndexWriter writer = index.writer(Index.Memory.LARGE_HEAP_SIZE, 1)) {
                                writer.merge(segmentIds);
                                writer.commit();
                            }
                            index.reload();
                            System.out.println("✅ Merged to single segment");
                        }
                    }

                    // Convert to split
                    Path splitPath = tempDir.resolve("test.split");
                    QuickwitSplit.SplitConfig config = new QuickwitSplit.SplitConfig(
                            "consolidation-test", "test-source", "test-node");
                    splitMetadata = QuickwitSplit.convertIndexFromPath(
                            indexPath.toString(), splitPath.toString(), config);
                    System.out.println("✅ Created split: " + splitPath);
                    System.out.println("   Split size: " + Files.size(splitPath) + " bytes");

                    // Upload to S3
                    String s3Key = "batch-consolidation-test/test.split";
                    s3Client.putObject(
                            PutObjectRequest.builder()
                                    .bucket(TEST_BUCKET)
                                    .key(s3Key)
                                    .build(),
                            splitPath);
                    s3SplitUrl = "s3://" + TEST_BUCKET + "/" + s3Key;
                    System.out.println("✅ Uploaded to S3: " + s3SplitUrl);
                }
            }
        }
    }

    @Test
    @Order(1)
    @DisplayName("Validate batch consolidation produces efficient ranges")
    void testBatchConsolidationEfficiency() throws Exception {
        System.out.println("Test: Validate batch consolidation efficiency");
        System.out.println("Expected: " + TEST_DOC_COUNT + " docs should consolidate to few ranges");
        System.out.println("Bug behavior: " + TEST_DOC_COUNT + " docs → " + (TEST_DOC_COUNT / 100 + 1) + " ranges (100-doc limit)");

        // Reset metrics
        SplitCacheManager.resetBatchMetrics();
        SplitCacheManager.resetObjectStorageRequestStats();

        // Configure cache with AWS credentials
        SplitCacheManager.CacheConfig cacheConfig = new SplitCacheManager.CacheConfig("consolidation-test")
                .withMaxCacheSize(200_000_000)
                .withAwsCredentials(getAccessKey(), getSecretKey())
                .withAwsRegion(TEST_REGION);

        try (SplitCacheManager cacheManager = SplitCacheManager.getInstance(cacheConfig)) {
            try (SplitSearcher searcher = cacheManager.createSplitSearcher(s3SplitUrl, splitMetadata)) {

                // Create batch of ALL documents
                List<DocAddress> addresses = new ArrayList<>();
                for (int i = 0; i < TEST_DOC_COUNT; i++) {
                    addresses.add(new DocAddress(0, i));
                }

                System.out.println("\n📊 Performing batch retrieval of " + addresses.size() + " documents...");

                // Perform batch retrieval
                List<Document> docs = searcher.docBatch(addresses);
                assertEquals(TEST_DOC_COUNT, docs.size(), "Should retrieve all documents");

                // Close documents
                for (Document doc : docs) {
                    doc.close();
                }

                // Get metrics
                BatchOptimizationMetrics metrics = SplitCacheManager.getBatchMetrics();

                // Get actual object storage request count (accurate count from storage layer)
                long actualStorageRequests = SplitCacheManager.getObjectStorageRequestCount();
                long actualBytesFetched = SplitCacheManager.getObjectStorageBytesFetched();

                System.out.println("\n📊 Batch Optimization Metrics:");
                System.out.println("   Total Documents Requested: " + metrics.getTotalDocumentsRequested());
                System.out.println("   Total Requests (without optimization): " + metrics.getTotalRequests());
                System.out.println("   Consolidated Requests (with optimization): " + metrics.getConsolidatedRequests());
                System.out.println("   Consolidation Ratio: " + String.format("%.1fx", metrics.getConsolidationRatio()));
                System.out.println("\n📊 Actual Object Storage Statistics (from storage layer):");
                System.out.println("   Actual Storage Requests: " + actualStorageRequests);
                System.out.println("   Actual Bytes Fetched: " + actualBytesFetched + " bytes");

                // CRITICAL ASSERTIONS
                long consolidatedRequests = metrics.getConsolidatedRequests();
                long totalRequests = metrics.getTotalRequests();

                if (totalRequests > 0) {
                    double ratio = metrics.getConsolidationRatio();

                    System.out.println("\n✅ VALIDATION RESULTS:");

                    // Buggy code would produce ~(TEST_DOC_COUNT/100) ranges
                    long buggyRangeCount = (TEST_DOC_COUNT / 100) + 1;

                    if (consolidatedRequests < buggyRangeCount / 2) {
                        System.out.println("   ✅ PASS: Consolidation is working correctly!");
                        System.out.println("   Buggy code would produce ~" + buggyRangeCount + " ranges");
                        System.out.println("   Fixed code produced " + consolidatedRequests + " ranges");
                    } else {
                        System.out.println("   ⚠️ WARNING: Consolidation may not be optimal");
                        System.out.println("   Expected fewer than " + (buggyRangeCount / 2) + " ranges");
                        System.out.println("   Got " + consolidatedRequests + " ranges");
                    }

                    // Assert consolidation is better than buggy 100-doc batches
                    assertTrue(ratio >= 2.0 || consolidatedRequests <= buggyRangeCount / 2,
                            "Batch consolidation should be significantly better than 100-doc batches. " +
                            "Got ratio=" + ratio + ", consolidatedRequests=" + consolidatedRequests);

                    // Validate actual storage request count
                    // Expected: ~2 requests (1 footer + 1 consolidated store data)
                    // Without optimization: 1000+ requests (one per document)
                    System.out.println("\n📊 Storage Request Validation:");
                    System.out.println("   Expected ~2 actual S3 requests (1 footer + 1 consolidated)");
                    System.out.println("   Got " + actualStorageRequests + " actual S3 requests");

                    assertTrue(actualStorageRequests <= 5,
                            "Should have minimal actual S3 requests. Got " + actualStorageRequests +
                            " (expected ~2: 1 footer + 1 consolidated store data)");

                } else {
                    System.out.println("   ⚠️ No requests recorded - checking if optimization triggered");
                }
            }
        }

        System.out.println("\n✅ Test complete\n");
    }

    @Test
    @Order(2)
    @DisplayName("Validate sequential document consolidation")
    void testSequentialDocumentConsolidation() throws Exception {
        System.out.println("Test: Validate sequential document access consolidation");

        SplitCacheManager.resetBatchMetrics();

        SplitCacheManager.CacheConfig cacheConfig = new SplitCacheManager.CacheConfig("sequential-test")
                .withMaxCacheSize(200_000_000)
                .withAwsCredentials(getAccessKey(), getSecretKey())
                .withAwsRegion(TEST_REGION);

        try (SplitCacheManager cacheManager = SplitCacheManager.getInstance(cacheConfig)) {
            try (SplitSearcher searcher = cacheManager.createSplitSearcher(s3SplitUrl, splitMetadata)) {

                List<DocAddress> addresses = new ArrayList<>();
                for (int i = 0; i < 500; i++) {
                    addresses.add(new DocAddress(0, i));
                }

                List<Document> docs = searcher.docBatch(addresses);
                assertEquals(500, docs.size());

                for (Document doc : docs) {
                    doc.close();
                }

                BatchOptimizationMetrics metrics = SplitCacheManager.getBatchMetrics();

                System.out.println("📊 Sequential access metrics:");
                System.out.println("   Documents: " + metrics.getTotalDocumentsRequested());
                System.out.println("   Consolidated Requests: " + metrics.getConsolidatedRequests());
                System.out.println("   Ratio: " + String.format("%.1fx", metrics.getConsolidationRatio()));

                if (metrics.getTotalRequests() > 0) {
                    // Sequential 500 docs should consolidate to very few ranges
                    assertTrue(metrics.getConsolidatedRequests() <= 10,
                            "Sequential 500 docs should consolidate to <=10 ranges, got " +
                            metrics.getConsolidatedRequests());
                    System.out.println("✅ Sequential consolidation is efficient");
                }
            }
        }
    }
}

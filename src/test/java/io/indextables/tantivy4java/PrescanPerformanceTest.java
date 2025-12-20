package io.indextables.tantivy4java;

import static org.junit.jupiter.api.Assertions.*;

import io.indextables.tantivy4java.core.*;
import io.indextables.tantivy4java.split.*;
import io.indextables.tantivy4java.split.merge.QuickwitSplit;
import org.junit.jupiter.api.*;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;

import java.nio.file.*;
import java.util.*;

/**
 * Performance test for prescan feature - both local and real S3.
 *
 * Requires AWS credentials in ~/.aws/credentials:
 * [default]
 * aws_access_key_id = YOUR_ACCESS_KEY
 * aws_secret_access_key = YOUR_SECRET_KEY
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class PrescanPerformanceTest {

    private static final String TEST_BUCKET = "tantivy4java-testing";
    private static final String TEST_REGION = "us-east-2";
    private static final String S3_KEY = "prescan-perf/test.split";

    private static Path tempDir;
    private static String localSplitPath;
    private static String s3SplitPath;
    private static long footerOffset;
    private static long fileSize;
    private static String docMappingJson;

    // AWS credentials loaded from ~/.aws/credentials
    private static String awsAccessKey;
    private static String awsSecretKey;
    private static S3Client s3Client;
    private static boolean s3Available = false;

    @BeforeAll
    static void setUp() throws Exception {
        tempDir = Files.createTempDirectory("prescan-perf");

        // Create a split with test data
        Path indexDir = tempDir.resolve("index");
        Files.createDirectories(indexDir);
        Path splitFile = tempDir.resolve("test.split");

        try (SchemaBuilder builder = new SchemaBuilder()) {
            builder.addTextField("title");
            builder.addTextField("body");
            builder.addTextField("category");
            builder.addTextField("tags");

            try (Schema schema = builder.build()) {
                try (Index index = new Index(schema, indexDir.toString(), false)) {
                    try (IndexWriter writer = index.writer(Index.Memory.DEFAULT_HEAP_SIZE, 1)) {
                        // Add documents with various terms
                        String[][] docs = {
                            {"hello world", "programming java rust", "tech", "code"},
                            {"goodbye world", "python javascript", "tech", "web"},
                            {"hello again", "rust systems", "tech", "systems"},
                            {"test document", "alpha beta gamma", "science", "math"},
                            {"another test", "delta epsilon", "science", "physics"},
                            {"final doc", "hello programming", "tech", "code"},
                            {"special chars", "test wildcard", "misc", "special"},
                            {"complex query", "alpha beta delta", "science", "complex"},
                        };

                        for (String[] doc : docs) {
                            try (Document d = new Document()) {
                                d.addText("title", doc[0]);
                                d.addText("body", doc[1]);
                                d.addText("category", doc[2]);
                                d.addText("tags", doc[3]);
                                writer.addDocument(d);
                            }
                        }
                        writer.commit();
                    }

                    // Convert to split while index is open
                    QuickwitSplit.SplitConfig config =
                        new QuickwitSplit.SplitConfig("perf-index", "perf-source", "perf-node");
                    QuickwitSplit.SplitMetadata metadata =
                        QuickwitSplit.convertIndexFromPath(indexDir.toString(), splitFile.toString(), config);

                    localSplitPath = "file://" + splitFile.toString();
                    footerOffset = metadata.getFooterStartOffset();
                    fileSize = metadata.getFooterEndOffset();
                    docMappingJson = metadata.getDocMappingJson();
                }
            }
        }

        System.out.println("Local split: " + localSplitPath);
        System.out.println("Footer offset: " + footerOffset + ", File size: " + fileSize);

        // Load AWS credentials and upload to real S3
        loadAwsCredentials();

        if (awsAccessKey != null && awsSecretKey != null) {
            try {
                s3Client = S3Client.builder()
                    .region(Region.of(TEST_REGION))
                    .credentialsProvider(StaticCredentialsProvider.create(
                        AwsBasicCredentials.create(awsAccessKey, awsSecretKey)))
                    .build();

                // Upload the split to S3
                byte[] splitBytes = Files.readAllBytes(splitFile);
                s3Client.putObject(
                    PutObjectRequest.builder().bucket(TEST_BUCKET).key(S3_KEY).build(),
                    RequestBody.fromBytes(splitBytes)
                );

                s3SplitPath = "s3://" + TEST_BUCKET + "/" + S3_KEY;
                s3Available = true;
                System.out.println("S3 split uploaded: " + s3SplitPath);
            } catch (Exception e) {
                System.out.println("S3 setup failed: " + e.getMessage());
                s3Available = false;
            }
        } else {
            System.out.println("AWS credentials not available - S3 test will be skipped");
        }
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
                System.out.println("Loaded AWS credentials from ~/.aws/credentials");
            }
        } catch (Exception e) {
            System.out.println("Failed to read AWS credentials: " + e.getMessage());
        }
    }

    @AfterAll
    static void tearDown() throws Exception {
        // Clean up S3 object
        if (s3Client != null && s3Available) {
            try {
                s3Client.deleteObject(DeleteObjectRequest.builder()
                    .bucket(TEST_BUCKET).key(S3_KEY).build());
                System.out.println("S3 split deleted");
            } catch (Exception e) {
                System.out.println("S3 cleanup failed: " + e.getMessage());
            }
            s3Client.close();
        }

        if (tempDir != null) {
            Files.walk(tempDir)
                .sorted(Comparator.reverseOrder())
                .forEach(p -> {
                    try { Files.deleteIfExists(p); } catch (Exception e) {}
                });
        }
    }

    @Test
    @Order(1)
    void testLocalFilePerformance() throws Exception {
        final int ITERATIONS = 20000;

        SplitCacheManager.CacheConfig cacheConfig =
            new SplitCacheManager.CacheConfig("prescan-perf-local")
                .withMaxCacheSize(100_000_000);

        try (SplitCacheManager cacheManager = SplitCacheManager.getInstance(cacheConfig)) {
            List<SplitInfo> splits = List.of(new SplitInfo(localSplitPath, footerOffset, fileSize));

            // Simple query
            SplitQuery simpleQuery = new SplitTermQuery("title", "hello");

            // Complex query: (A AND B) OR (C AND D)
            SplitQuery andAB = new SplitBooleanQuery()
                .addMust(new SplitTermQuery("title", "hello"))
                .addMust(new SplitTermQuery("body", "programming"));

            SplitQuery andCD = new SplitBooleanQuery()
                .addMust(new SplitTermQuery("title", "test"))
                .addMust(new SplitTermQuery("body", "alpha"));

            SplitQuery complexQuery = new SplitBooleanQuery()
                .addShould(andAB)
                .addShould(andCD);

            // Warmup
            System.out.println("LOCAL: Warming up...");
            for (int i = 0; i < 100; i++) {
                cacheManager.prescanSplits(splits, docMappingJson, simpleQuery);
                cacheManager.prescanSplits(splits, docMappingJson, complexQuery);
            }

            System.out.println("LOCAL: Running " + ITERATIONS + " iterations...");

            // Time simple query
            long simpleStart = System.nanoTime();
            for (int i = 0; i < ITERATIONS; i++) {
                cacheManager.prescanSplits(splits, docMappingJson, simpleQuery);
            }
            long simpleEnd = System.nanoTime();

            // Time complex query
            long complexStart = System.nanoTime();
            for (int i = 0; i < ITERATIONS; i++) {
                cacheManager.prescanSplits(splits, docMappingJson, complexQuery);
            }
            long complexEnd = System.nanoTime();

            printResults("LOCAL FILE SPLIT", ITERATIONS, simpleStart, simpleEnd, complexStart, complexEnd);
        }
    }

    @Test
    @Order(2)
    void testRealS3Performance() throws Exception {
        Assumptions.assumeTrue(s3Available, "S3 not available - skipping");

        final int ITERATIONS = 20000;

        SplitCacheManager.CacheConfig cacheConfig =
            new SplitCacheManager.CacheConfig("prescan-perf-s3")
                .withMaxCacheSize(100_000_000)
                .withAwsCredentials(awsAccessKey, awsSecretKey)
                .withAwsRegion(TEST_REGION);

        try (SplitCacheManager cacheManager = SplitCacheManager.getInstance(cacheConfig)) {
            List<SplitInfo> splits = List.of(new SplitInfo(s3SplitPath, footerOffset, fileSize));

            // Simple query
            SplitQuery simpleQuery = new SplitTermQuery("title", "hello");

            // Complex query: (A AND B) OR (C AND D)
            SplitQuery andAB = new SplitBooleanQuery()
                .addMust(new SplitTermQuery("title", "hello"))
                .addMust(new SplitTermQuery("body", "programming"));

            SplitQuery andCD = new SplitBooleanQuery()
                .addMust(new SplitTermQuery("title", "test"))
                .addMust(new SplitTermQuery("body", "alpha"));

            SplitQuery complexQuery = new SplitBooleanQuery()
                .addShould(andAB)
                .addShould(andCD);

            // Warmup (more for S3 to warm up network/caches)
            System.out.println("S3: Warming up...");
            for (int i = 0; i < 100; i++) {
                cacheManager.prescanSplits(splits, docMappingJson, simpleQuery);
                cacheManager.prescanSplits(splits, docMappingJson, complexQuery);
            }

            System.out.println("S3: Running " + ITERATIONS + " iterations...");

            // Time simple query
            long simpleStart = System.nanoTime();
            for (int i = 0; i < ITERATIONS; i++) {
                cacheManager.prescanSplits(splits, docMappingJson, simpleQuery);
            }
            long simpleEnd = System.nanoTime();

            // Time complex query
            long complexStart = System.nanoTime();
            for (int i = 0; i < ITERATIONS; i++) {
                cacheManager.prescanSplits(splits, docMappingJson, complexQuery);
            }
            long complexEnd = System.nanoTime();

            printResults("REAL S3 SPLIT (" + TEST_REGION + ")", ITERATIONS, simpleStart, simpleEnd, complexStart, complexEnd);
        }
    }

    private void printResults(String label, int iterations,
                              long simpleStart, long simpleEnd,
                              long complexStart, long complexEnd) {
        double simpleAvgMicros = (simpleEnd - simpleStart) / 1000.0 / iterations;
        double complexAvgMicros = (complexEnd - complexStart) / 1000.0 / iterations;
        long simpleTotalMs = (simpleEnd - simpleStart) / 1_000_000;
        long complexTotalMs = (complexEnd - complexStart) / 1_000_000;

        System.out.println("\n╔════════════════════════════════════════════════════════════════╗");
        System.out.println("║           PRESCAN PERFORMANCE COMPARISON SUMMARY               ║");
        System.out.printf("║  %-60s  ║%n", label);
        System.out.println("╠════════════════════════════════════════════════════════════════╣");
        System.out.println("║ Iterations: " + iterations + "                                            ║");
        System.out.println("╠════════════════════════════════════════════════════════════════╣");
        System.out.println("║ SIMPLE (title:hello)                                           ║");
        System.out.printf("║   Total time:    %6d ms                                      ║%n", simpleTotalMs);
        System.out.printf("║   Avg per call:  %6.2f µs                                      ║%n", simpleAvgMicros);
        System.out.printf("║   Throughput:    %6.0f prescans/sec                            ║%n", iterations * 1000.0 / simpleTotalMs);
        System.out.println("╠════════════════════════════════════════════════════════════════╣");
        System.out.println("║ COMPLEX ((A AND B) OR (C AND D))                               ║");
        System.out.printf("║   Total time:    %6d ms                                      ║%n", complexTotalMs);
        System.out.printf("║   Avg per call:  %6.2f µs                                      ║%n", complexAvgMicros);
        System.out.printf("║   Throughput:    %6.0f prescans/sec                            ║%n", iterations * 1000.0 / complexTotalMs);
        System.out.println("╠════════════════════════════════════════════════════════════════╣");
        System.out.printf("║ Complex/Simple ratio: %.2fx                                     ║%n", complexAvgMicros / simpleAvgMicros);
        System.out.println("╚════════════════════════════════════════════════════════════════╝\n");
    }
}

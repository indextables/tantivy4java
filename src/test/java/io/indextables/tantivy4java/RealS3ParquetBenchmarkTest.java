/*
 * Performance benchmark: standalone split vs. parquet companion mode with S3 backend.
 *
 * Compares latency, split sizes, and S3 request counts between the two approaches.
 *
 * Prerequisites:
 *   - AWS credentials configured (see RealS3EndToEndTest for details)
 *   - S3 bucket accessible for testing
 *
 * Run:
 *   mvn test -pl . -Dtest=RealS3ParquetBenchmarkTest \
 *       -Dtest.s3.bucket=your-bucket -Dtest.s3.region=us-east-2
 */
package io.indextables.tantivy4java;

import io.indextables.tantivy4java.core.*;
import io.indextables.tantivy4java.query.*;
import io.indextables.tantivy4java.result.*;
import io.indextables.tantivy4java.split.*;
import io.indextables.tantivy4java.split.merge.*;

import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;

import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;
import software.amazon.awssdk.core.sync.RequestBody;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Compares standalone splits vs. parquet companion splits on real S3:
 *   - Split file sizes
 *   - Search + doc retrieval latency
 *   - Number of S3 requests (storage I/O operations)
 *   - Bytes fetched from S3
 *
 * Uses 500K+ rows to produce 20MB+ standalone splits.
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class RealS3ParquetBenchmarkTest {

    private static final String TEST_BUCKET = System.getProperty("test.s3.bucket", "tantivy4java-testing");
    private static final String TEST_REGION = System.getProperty("test.s3.region", "us-east-2");
    private static final String ACCESS_KEY = System.getProperty("test.s3.accessKey");
    private static final String SECRET_KEY = System.getProperty("test.s3.secretKey");
    private static final String S3_PREFIX = "parquet-benchmark/";

    private static String awsAccessKey;
    private static String awsSecretKey;
    private static S3Client s3Client;

    @TempDir
    static Path tempDir;

    // ───────────────────────────────────────────────────────────
    // Credential loading (same pattern as RealS3EndToEndTest)
    // ───────────────────────────────────────────────────────────

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
            if (!Files.exists(credentialsPath)) return;

            List<String> lines = Files.readAllLines(credentialsPath);
            boolean inDefault = false;

            for (String line : lines) {
                line = line.trim();
                if (line.equals("[default]")) { inDefault = true; continue; }
                if (line.startsWith("[") && line.endsWith("]")) { inDefault = false; continue; }

                if (inDefault && line.contains("=")) {
                    String[] parts = line.split("=", 2);
                    if (parts.length == 2) {
                        String key = parts[0].trim(), value = parts[1].trim();
                        if ("aws_access_key_id".equals(key)) awsAccessKey = value;
                        else if ("aws_secret_access_key".equals(key)) awsSecretKey = value;
                    }
                }
            }
        } catch (Exception e) {
            System.out.println("Failed to read AWS credentials: " + e.getMessage());
        }
    }

    @BeforeAll
    static void setup() {
        System.out.println("=== REAL S3 PARQUET COMPANION BENCHMARK ===");
        System.out.println("Bucket: " + TEST_BUCKET + "  Region: " + TEST_REGION);

        boolean hasExplicit = ACCESS_KEY != null && SECRET_KEY != null;
        if (!hasExplicit) loadAwsCredentials();

        boolean hasFile = awsAccessKey != null && awsSecretKey != null;
        boolean hasEnv = System.getenv("AWS_ACCESS_KEY_ID") != null;

        if (!hasExplicit && !hasFile && !hasEnv) {
            Assumptions.abort("AWS credentials not available -- skipping S3 benchmark");
        }

        if (getAccessKey() != null && getSecretKey() != null) {
            s3Client = S3Client.builder()
                    .region(Region.of(TEST_REGION))
                    .credentialsProvider(StaticCredentialsProvider.create(
                            AwsBasicCredentials.create(getAccessKey(), getSecretKey())))
                    .build();
        }
    }

    @AfterAll
    static void teardown() {
        if (s3Client != null) {
            try {
                ListObjectsV2Response listing = s3Client.listObjectsV2(
                        ListObjectsV2Request.builder()
                                .bucket(TEST_BUCKET).prefix(S3_PREFIX).build());
                for (S3Object obj : listing.contents()) {
                    s3Client.deleteObject(DeleteObjectRequest.builder()
                            .bucket(TEST_BUCKET).key(obj.key()).build());
                }
            } catch (Exception e) {
                System.out.println("Cleanup warning: " + e.getMessage());
            }
            s3Client.close();
        }
    }

    // ───────────────────────────────────────────────────────────
    // Test 1: Performance + Size comparison (large splits)
    // ───────────────────────────────────────────────────────────

    @Test
    @org.junit.jupiter.api.Order(1)
    @DisplayName("Benchmark: standalone vs. parquet companion — 1M rows (20MB+ splits)")
    void benchmarkLargeSplits() throws Exception {
        int numRows = 1_000_000;
        int numCols = 4;
        String tag = "large_" + numRows + "r";

        Path localDir = tempDir.resolve(tag);
        Files.createDirectories(localDir);

        System.out.println("\n=== LARGE SPLIT BENCHMARK: " + numRows + " rows, " + numCols + " cols ===");

        // 1. Create parquet file
        Path parquetFile = localDir.resolve("data.parquet");
        System.out.println("Creating parquet file with " + numRows + " rows...");
        QuickwitSplit.nativeWriteTestParquet(parquetFile.toString(), numRows, 0);
        long parquetSize = Files.size(parquetFile);
        System.out.printf("  Parquet file: %.2f MB%n", parquetSize / (1024.0 * 1024));

        // 2. Create standalone split
        Path standaloneSplit = localDir.resolve("standalone.split");
        System.out.println("Creating standalone split...");
        QuickwitSplit.SplitMetadata standaloneMeta = createStandaloneSplit(
                standaloneSplit, numRows);
        long standaloneSize = Files.size(standaloneSplit);
        System.out.printf("  Standalone split: %.2f MB%n", standaloneSize / (1024.0 * 1024));

        // 3. Create parquet companion split
        Path companionSplit = localDir.resolve("companion.split");
        System.out.println("Creating parquet companion split...");
        ParquetCompanionConfig pqConfig = new ParquetCompanionConfig(localDir.toString());
        QuickwitSplit.SplitMetadata companionMeta = QuickwitSplit.createFromParquet(
                Collections.singletonList(parquetFile.toString()),
                companionSplit.toString(), pqConfig);
        long companionSize = Files.size(companionSplit);
        System.out.printf("  Companion split: %.2f MB%n", companionSize / (1024.0 * 1024));

        double reductionPct = 100.0 * (1.0 - (double) companionSize / standaloneSize);
        System.out.printf("  Size reduction: %.1f%%%n", reductionPct);

        // 4. Upload to S3
        System.out.println("Uploading to S3...");
        String standaloneS3Key = S3_PREFIX + tag + "/standalone.split";
        String companionS3Key = S3_PREFIX + tag + "/companion.split";
        String parquetS3Key = S3_PREFIX + tag + "/data.parquet";

        uploadToS3(standaloneSplit, standaloneS3Key);
        uploadToS3(companionSplit, companionS3Key);
        uploadToS3(parquetFile, parquetS3Key);

        String standaloneUri = "s3://" + TEST_BUCKET + "/" + standaloneS3Key;
        String companionUri = "s3://" + TEST_BUCKET + "/" + companionS3Key;
        String s3TableRoot = "s3://" + TEST_BUCKET + "/" + S3_PREFIX + tag;

        // 5. Benchmark standalone
        System.out.println("\nBenchmarking standalone split...");
        BenchmarkResult standaloneResult = benchmarkSplit(
                standaloneUri, standaloneMeta, null, "bench-standalone-" + tag, 10, 20);

        // 6. Benchmark parquet companion
        System.out.println("Benchmarking parquet companion split...");
        BenchmarkResult companionResult = benchmarkSplit(
                companionUri, companionMeta, s3TableRoot, "bench-companion-" + tag, 10, 20);

        // 7. Report
        System.out.println("\n╔══════════════════════════════════════════════════════════════════════╗");
        System.out.println("║              LARGE SPLIT BENCHMARK RESULTS (1M rows)              ║");
        System.out.println("╠═════════════════════╦═══════════════════╦══════════════════════════╣");
        System.out.println("║                     ║    Standalone     ║   Parquet Companion      ║");
        System.out.println("╠═════════════════════╬═══════════════════╬══════════════════════════╣");
        System.out.printf( "║ Split size          ║ %13.2f MB ║ %20.2f MB ║%n",
                standaloneSize / (1024.0 * 1024), companionSize / (1024.0 * 1024));
        System.out.printf( "║ Size reduction      ║           ---     ║ %19.1f%%  ║%n", reductionPct);
        System.out.printf( "║ Search latency (ms) ║ %,13d ms ║ %,20d ms ║%n",
                standaloneResult.searchMs, companionResult.searchMs);
        System.out.printf( "║ Retrieval lat. (ms) ║ %,13d ms ║ %,20d ms ║%n",
                standaloneResult.retrievalMs, companionResult.retrievalMs);
        System.out.printf( "║ S3 requests (pre)   ║ %,17d ║ %,24d ║%n",
                standaloneResult.prewarmRequests, companionResult.prewarmRequests);
        System.out.printf( "║ S3 bytes (prewarm)  ║ %13.2f MB ║ %20.2f MB ║%n",
                standaloneResult.prewarmBytes / (1024.0 * 1024),
                companionResult.prewarmBytes / (1024.0 * 1024));
        System.out.printf( "║ S3 requests (query) ║ %,17d ║ %,24d ║%n",
                standaloneResult.queryRequests, companionResult.queryRequests);
        System.out.printf( "║ S3 bytes (query)    ║ %13.2f MB ║ %20.2f MB ║%n",
                standaloneResult.queryBytes / (1024.0 * 1024),
                companionResult.queryBytes / (1024.0 * 1024));
        System.out.printf( "║ Total S3 requests   ║ %,17d ║ %,24d ║%n",
                standaloneResult.totalRequests, companionResult.totalRequests);
        System.out.printf( "║ Total S3 bytes      ║ %13.2f MB ║ %20.2f MB ║%n",
                standaloneResult.totalBytes / (1024.0 * 1024),
                companionResult.totalBytes / (1024.0 * 1024));
        System.out.println("╚═════════════════════╩═══════════════════╩══════════════════════════╝");

        // Verify standalone split is at least 20MB
        assertTrue(standaloneSize > 20 * 1024 * 1024,
                "Standalone split should be > 20MB, was " + standaloneSize / (1024 * 1024) + " MB");

        // Verify companion split is significantly smaller
        assertTrue(companionSize < standaloneSize,
                "Companion split should be smaller than standalone");
    }

    // ───────────────────────────────────────────────────────────
    // Test 2: S3 Request Count comparison
    // ───────────────────────────────────────────────────────────

    @Test
    @org.junit.jupiter.api.Order(2)
    @DisplayName("S3 Request Count: standalone vs. parquet companion — detailed breakdown")
    void s3RequestCountComparison() throws Exception {
        int numRows = 1_000_000;
        String tag = "s3req_" + numRows + "r";

        Path localDir = tempDir.resolve(tag);
        Files.createDirectories(localDir);

        System.out.println("\n=== S3 REQUEST COUNT COMPARISON: " + numRows + " rows ===");

        // 1. Create data
        Path parquetFile = localDir.resolve("data.parquet");
        QuickwitSplit.nativeWriteTestParquet(parquetFile.toString(), numRows, 0);

        Path standaloneSplit = localDir.resolve("standalone.split");
        QuickwitSplit.SplitMetadata standaloneMeta = createStandaloneSplit(standaloneSplit, numRows);

        Path companionSplit = localDir.resolve("companion.split");
        ParquetCompanionConfig pqConfig = new ParquetCompanionConfig(localDir.toString());
        QuickwitSplit.SplitMetadata companionMeta = QuickwitSplit.createFromParquet(
                Collections.singletonList(parquetFile.toString()),
                companionSplit.toString(), pqConfig);

        // 2. Upload to S3
        String standaloneS3Key = S3_PREFIX + tag + "/standalone.split";
        String companionS3Key = S3_PREFIX + tag + "/companion.split";
        String parquetS3Key = S3_PREFIX + tag + "/data.parquet";

        uploadToS3(standaloneSplit, standaloneS3Key);
        uploadToS3(companionSplit, companionS3Key);
        uploadToS3(parquetFile, parquetS3Key);

        String standaloneUri = "s3://" + TEST_BUCKET + "/" + standaloneS3Key;
        String companionUri = "s3://" + TEST_BUCKET + "/" + companionS3Key;
        String s3TableRoot = "s3://" + TEST_BUCKET + "/" + S3_PREFIX + tag;

        // 3. Measure: Standalone — prewarm only (no doc retrieval)
        System.out.println("\n--- Phase 1: Prewarm-only S3 requests ---");

        SplitCacheManager.resetObjectStorageRequestStats();
        SplitCacheManager.resetStorageDownloadMetrics();

        SplitCacheManager.CacheConfig standaloneCfg = new SplitCacheManager.CacheConfig("s3req-sa-" + tag)
                .withMaxCacheSize(500_000_000)
                .withAwsCredentials(getAccessKey(), getSecretKey())
                .withAwsRegion(TEST_REGION);

        try (SplitCacheManager cm = SplitCacheManager.getInstance(standaloneCfg)) {
            try (SplitSearcher searcher = cm.createSplitSearcher(standaloneUri, standaloneMeta)) {
                searcher.preloadComponents(
                        SplitSearcher.IndexComponent.TERM,
                        SplitSearcher.IndexComponent.POSTINGS,
                        SplitSearcher.IndexComponent.STORE,
                        SplitSearcher.IndexComponent.FASTFIELD).join();
            }
        }

        long saPrewarmReqs = SplitCacheManager.getObjectStorageRequestCount();
        long saPrewarmBytes = SplitCacheManager.getObjectStorageBytesFetched();
        SplitCacheManager.StorageDownloadMetrics saPrewarmMetrics =
                SplitCacheManager.getStorageDownloadMetrics();

        System.out.printf("  Standalone prewarm:  %,d requests, %.2f MB%n",
                saPrewarmReqs, saPrewarmBytes / (1024.0 * 1024));

        // 4. Measure: Companion — prewarm only
        SplitCacheManager.resetObjectStorageRequestStats();
        SplitCacheManager.resetStorageDownloadMetrics();

        SplitCacheManager.CacheConfig companionCfg = new SplitCacheManager.CacheConfig("s3req-pq-" + tag)
                .withMaxCacheSize(500_000_000)
                .withAwsCredentials(getAccessKey(), getSecretKey())
                .withAwsRegion(TEST_REGION);

        try (SplitCacheManager cm = SplitCacheManager.getInstance(companionCfg)) {
            try (SplitSearcher searcher = cm.createSplitSearcher(companionUri, companionMeta)) {
                searcher.preloadComponents(
                        SplitSearcher.IndexComponent.TERM,
                        SplitSearcher.IndexComponent.POSTINGS).join();
            }
        }

        long pqPrewarmReqs = SplitCacheManager.getObjectStorageRequestCount();
        long pqPrewarmBytes = SplitCacheManager.getObjectStorageBytesFetched();
        SplitCacheManager.StorageDownloadMetrics pqPrewarmMetrics =
                SplitCacheManager.getStorageDownloadMetrics();

        System.out.printf("  Companion prewarm:   %,d requests, %.2f MB%n",
                pqPrewarmReqs, pqPrewarmBytes / (1024.0 * 1024));

        // 5. Measure: Standalone — search + doc retrieval (fresh cache)
        System.out.println("\n--- Phase 2: Search + 100 doc retrievals (cold cache) ---");

        SplitCacheManager.resetObjectStorageRequestStats();
        SplitCacheManager.resetStorageDownloadMetrics();

        SplitCacheManager.CacheConfig saQueryCfg = new SplitCacheManager.CacheConfig("s3req-saQ-" + tag)
                .withMaxCacheSize(500_000_000)
                .withAwsCredentials(getAccessKey(), getSecretKey())
                .withAwsRegion(TEST_REGION);

        try (SplitCacheManager cm = SplitCacheManager.getInstance(saQueryCfg)) {
            try (SplitSearcher searcher = cm.createSplitSearcher(standaloneUri, standaloneMeta)) {
                // Prewarm first
                searcher.preloadComponents(
                        SplitSearcher.IndexComponent.TERM,
                        SplitSearcher.IndexComponent.POSTINGS,
                        SplitSearcher.IndexComponent.STORE).join();

                // Reset after prewarm to measure query-only S3 traffic
                SplitCacheManager.resetObjectStorageRequestStats();
                SplitCacheManager.resetStorageDownloadMetrics();

                // Search + retrieve 100 docs
                SplitQuery q = searcher.parseQuery("*");
                SearchResult res = searcher.search(q, 100);
                for (SearchResult.Hit hit : res.getHits()) {
                    try (Document doc = searcher.doc(hit.getDocAddress())) {
                        doc.getFirst("name");
                        doc.getFirst("id");
                        doc.getFirst("score");
                    }
                }
            }
        }

        long saQueryReqs = SplitCacheManager.getObjectStorageRequestCount();
        long saQueryBytes = SplitCacheManager.getObjectStorageBytesFetched();

        System.out.printf("  Standalone query:    %,d requests, %.2f MB%n",
                saQueryReqs, saQueryBytes / (1024.0 * 1024));

        // 6. Measure: Companion — search + doc retrieval (fresh cache)
        SplitCacheManager.resetObjectStorageRequestStats();
        SplitCacheManager.resetStorageDownloadMetrics();

        SplitCacheManager.CacheConfig pqQueryCfg = new SplitCacheManager.CacheConfig("s3req-pqQ-" + tag)
                .withMaxCacheSize(500_000_000)
                .withAwsCredentials(getAccessKey(), getSecretKey())
                .withAwsRegion(TEST_REGION);

        try (SplitCacheManager cm = SplitCacheManager.getInstance(pqQueryCfg)) {
            try (SplitSearcher searcher = cm.createSplitSearcher(companionUri, companionMeta)) {
                searcher.preloadComponents(
                        SplitSearcher.IndexComponent.TERM,
                        SplitSearcher.IndexComponent.POSTINGS).join();

                // Reset after prewarm to measure query-only S3 traffic
                SplitCacheManager.resetObjectStorageRequestStats();
                SplitCacheManager.resetStorageDownloadMetrics();

                // Search + retrieve 100 docs (from parquet on S3)
                SplitQuery q = searcher.parseQuery("*");
                SearchResult res = searcher.search(q, 100);
                for (SearchResult.Hit hit : res.getHits()) {
                    String json = searcher.docProjected(hit.getDocAddress(),
                            "name", "id", "score");
                    assertNotNull(json);
                }
            }
        }

        long pqQueryReqs = SplitCacheManager.getObjectStorageRequestCount();
        long pqQueryBytes = SplitCacheManager.getObjectStorageBytesFetched();

        System.out.printf("  Companion query:     %,d requests, %.2f MB%n",
                pqQueryReqs, pqQueryBytes / (1024.0 * 1024));

        // 7. Summary
        System.out.println("\n╔══════════════════════════════════════════════════════════════════════╗");
        System.out.println("║              S3 REQUEST COUNT COMPARISON (1M rows)                ║");
        System.out.println("╠═════════════════════════════╦═════════════════╦═════════════════════╣");
        System.out.println("║ Phase                       ║    Standalone   ║ Parquet Companion   ║");
        System.out.println("╠═════════════════════════════╬═════════════════╬═════════════════════╣");
        System.out.printf( "║ Prewarm: requests           ║ %,15d ║ %,19d ║%n",
                saPrewarmReqs, pqPrewarmReqs);
        System.out.printf( "║ Prewarm: bytes              ║ %11.2f MB ║ %15.2f MB ║%n",
                saPrewarmBytes / (1024.0 * 1024), pqPrewarmBytes / (1024.0 * 1024));
        System.out.printf( "║ Query+Retrieve: requests    ║ %,15d ║ %,19d ║%n",
                saQueryReqs, pqQueryReqs);
        System.out.printf( "║ Query+Retrieve: bytes       ║ %11.2f MB ║ %15.2f MB ║%n",
                saQueryBytes / (1024.0 * 1024), pqQueryBytes / (1024.0 * 1024));
        System.out.printf( "║ TOTAL: requests             ║ %,15d ║ %,19d ║%n",
                saPrewarmReqs + saQueryReqs, pqPrewarmReqs + pqQueryReqs);
        System.out.printf( "║ TOTAL: bytes                ║ %11.2f MB ║ %15.2f MB ║%n",
                (saPrewarmBytes + saQueryBytes) / (1024.0 * 1024),
                (pqPrewarmBytes + pqQueryBytes) / (1024.0 * 1024));
        System.out.println("╚═════════════════════════════╩═════════════════╩═════════════════════╝");
    }

    // ───────────────────────────────────────────────────────────
    // Test 3: Cold cache (no prewarm) — true S3 request picture
    // ───────────────────────────────────────────────────────────

    @Test
    @org.junit.jupiter.api.Order(3)
    @DisplayName("Cold Cache (no prewarm): standalone vs. parquet companion — S3 requests + latency")
    void coldCacheBenchmark() throws Exception {
        int numRows = 1_000_000;
        String tag = "cold_" + numRows + "r";

        Path localDir = tempDir.resolve(tag);
        Files.createDirectories(localDir);

        System.out.println("\n=== COLD CACHE BENCHMARK (NO PREWARM): " + numRows + " rows ===");

        // 1. Create data
        Path parquetFile = localDir.resolve("data.parquet");
        System.out.println("Creating parquet file...");
        QuickwitSplit.nativeWriteTestParquet(parquetFile.toString(), numRows, 0);

        Path standaloneSplit = localDir.resolve("standalone.split");
        System.out.println("Creating standalone split...");
        QuickwitSplit.SplitMetadata standaloneMeta = createStandaloneSplit(standaloneSplit, numRows);
        long standaloneSize = Files.size(standaloneSplit);

        Path companionSplit = localDir.resolve("companion.split");
        System.out.println("Creating parquet companion split...");
        ParquetCompanionConfig pqConfig = new ParquetCompanionConfig(localDir.toString());
        QuickwitSplit.SplitMetadata companionMeta = QuickwitSplit.createFromParquet(
                Collections.singletonList(parquetFile.toString()),
                companionSplit.toString(), pqConfig);
        long companionSize = Files.size(companionSplit);

        // 2. Upload to S3
        System.out.println("Uploading to S3...");
        String standaloneS3Key = S3_PREFIX + tag + "/standalone.split";
        String companionS3Key = S3_PREFIX + tag + "/companion.split";
        String parquetS3Key = S3_PREFIX + tag + "/data.parquet";

        uploadToS3(standaloneSplit, standaloneS3Key);
        uploadToS3(companionSplit, companionS3Key);
        uploadToS3(parquetFile, parquetS3Key);

        String standaloneUri = "s3://" + TEST_BUCKET + "/" + standaloneS3Key;
        String companionUri = "s3://" + TEST_BUCKET + "/" + companionS3Key;
        String s3TableRoot = "s3://" + TEST_BUCKET + "/" + S3_PREFIX + tag;

        // 3. Standalone — cold cache: search + retrieve 100 docs, NO prewarm
        System.out.println("\n--- Standalone: cold cache search + 100 doc retrievals ---");

        SplitCacheManager.resetObjectStorageRequestStats();
        SplitCacheManager.resetStorageDownloadMetrics();

        long saMs;
        SplitCacheManager.CacheConfig saCfg = new SplitCacheManager.CacheConfig("cold-sa-" + tag)
                .withMaxCacheSize(500_000_000)
                .withAwsCredentials(getAccessKey(), getSecretKey())
                .withAwsRegion(TEST_REGION);

        try (SplitCacheManager cm = SplitCacheManager.getInstance(saCfg)) {
            try (SplitSearcher searcher = cm.createSplitSearcher(standaloneUri, standaloneMeta)) {
                long start = System.nanoTime();

                SplitQuery q = searcher.parseQuery("*");
                SearchResult res = searcher.search(q, 100);
                for (SearchResult.Hit hit : res.getHits()) {
                    try (Document doc = searcher.doc(hit.getDocAddress())) {
                        doc.getFirst("name");
                        doc.getFirst("id");
                        doc.getFirst("score");
                    }
                }

                saMs = (System.nanoTime() - start) / 1_000_000;
            }
        }

        long saReqs = SplitCacheManager.getObjectStorageRequestCount();
        long saBytes = SplitCacheManager.getObjectStorageBytesFetched();

        System.out.printf("  Standalone:  %,d requests, %.2f MB, %d ms%n",
                saReqs, saBytes / (1024.0 * 1024), saMs);

        // 4. Companion — cold cache: search + retrieve 100 docs, NO prewarm
        System.out.println("--- Companion: cold cache search + 100 doc retrievals ---");

        SplitCacheManager.resetObjectStorageRequestStats();
        SplitCacheManager.resetStorageDownloadMetrics();

        long pqMs;
        SplitCacheManager.CacheConfig pqCfg = new SplitCacheManager.CacheConfig("cold-pq-" + tag)
                .withMaxCacheSize(500_000_000)
                .withAwsCredentials(getAccessKey(), getSecretKey())
                .withAwsRegion(TEST_REGION);

        try (SplitCacheManager cm = SplitCacheManager.getInstance(pqCfg)) {
            try (SplitSearcher searcher = cm.createSplitSearcher(companionUri, companionMeta)) {
                long start = System.nanoTime();

                SplitQuery q = searcher.parseQuery("*");
                SearchResult res = searcher.search(q, 100);
                for (SearchResult.Hit hit : res.getHits()) {
                    String json = searcher.docProjected(hit.getDocAddress(),
                            "name", "id", "score");
                    assertNotNull(json);
                }

                pqMs = (System.nanoTime() - start) / 1_000_000;
            }
        }

        long pqReqs = SplitCacheManager.getObjectStorageRequestCount();
        long pqBytes = SplitCacheManager.getObjectStorageBytesFetched();

        System.out.printf("  Companion:   %,d requests, %.2f MB, %d ms%n",
                pqReqs, pqBytes / (1024.0 * 1024), pqMs);

        // 5. Summary
        System.out.println("\n╔══════════════════════════════════════════════════════════════════════╗");
        System.out.println("║          COLD CACHE BENCHMARK — NO PREWARM (1M rows)               ║");
        System.out.println("╠═════════════════════════════╦═════════════════╦═════════════════════╣");
        System.out.println("║ Metric                      ║    Standalone   ║ Parquet Companion   ║");
        System.out.println("╠═════════════════════════════╬═════════════════╬═════════════════════╣");
        System.out.printf( "║ Split size                  ║ %11.2f MB ║ %15.2f MB ║%n",
                standaloneSize / (1024.0 * 1024), companionSize / (1024.0 * 1024));
        System.out.printf( "║ Total latency               ║ %,11d ms ║ %,15d ms ║%n", saMs, pqMs);
        System.out.printf( "║ S3 requests                 ║ %,15d ║ %,19d ║%n", saReqs, pqReqs);
        System.out.printf( "║ S3 bytes fetched            ║ %11.2f MB ║ %15.2f MB ║%n",
                saBytes / (1024.0 * 1024), pqBytes / (1024.0 * 1024));
        double reqDelta = saReqs > 0 ? 100.0 * (pqReqs - saReqs) / saReqs : 0;
        double bytesDelta = saBytes > 0 ? 100.0 * (pqBytes - saBytes) / saBytes : 0;
        System.out.printf( "║ Request delta               ║           ---   ║ %+18.1f%% ║%n", reqDelta);
        System.out.printf( "║ Bytes delta                 ║           ---   ║ %+18.1f%% ║%n", bytesDelta);
        System.out.println("╚═════════════════════════════╩═════════════════╩═════════════════════╝");
    }

    // ───────────────────────────────────────────────────────────
    // Benchmark runner
    // ───────────────────────────────────────────────────────────

    private static class BenchmarkResult {
        long searchMs;
        long retrievalMs;
        long prewarmRequests;
        long prewarmBytes;
        long queryRequests;
        long queryBytes;
        long totalRequests;
        long totalBytes;
    }

    /**
     * Benchmark a split: prewarm, then run iterations of search + doc retrieval.
     * Tracks S3 request counts for each phase.
     *
     * @param s3TableRoot if non-null, uses parquet companion mode with this table root
     */
    private BenchmarkResult benchmarkSplit(String splitUri, QuickwitSplit.SplitMetadata metadata,
                                           String s3TableRoot, String cacheId,
                                           int searchIters, int docsPerSearch) throws Exception {

        BenchmarkResult result = new BenchmarkResult();
        boolean isCompanion = s3TableRoot != null;

        SplitCacheManager.CacheConfig cacheConfig = new SplitCacheManager.CacheConfig(cacheId)
                .withMaxCacheSize(500_000_000)
                .withAwsCredentials(getAccessKey(), getSecretKey())
                .withAwsRegion(TEST_REGION);

        // Reset counters
        SplitCacheManager.resetObjectStorageRequestStats();
        SplitCacheManager.resetStorageDownloadMetrics();

        try (SplitCacheManager cm = SplitCacheManager.getInstance(cacheConfig)) {
            try (SplitSearcher searcher = cm.createSplitSearcher(splitUri, metadata)) {

                // Phase A: Prewarm
                if (isCompanion) {
                    searcher.preloadComponents(
                            SplitSearcher.IndexComponent.TERM,
                            SplitSearcher.IndexComponent.POSTINGS).join();
                } else {
                    searcher.preloadComponents(
                            SplitSearcher.IndexComponent.TERM,
                            SplitSearcher.IndexComponent.POSTINGS,
                            SplitSearcher.IndexComponent.STORE).join();
                }

                result.prewarmRequests = SplitCacheManager.getObjectStorageRequestCount();
                result.prewarmBytes = SplitCacheManager.getObjectStorageBytesFetched();

                // Reset for query phase
                SplitCacheManager.resetObjectStorageRequestStats();

                // Phase B: Search
                long searchStart = System.nanoTime();
                List<SearchResult.Hit> allHits = new ArrayList<>();
                for (int i = 0; i < searchIters; i++) {
                    SplitQuery q = searcher.parseQuery("*");
                    SearchResult res = searcher.search(q, docsPerSearch);
                    if (i == 0) allHits.addAll(res.getHits());
                }
                result.searchMs = (System.nanoTime() - searchStart) / 1_000_000;

                // Phase C: Doc retrieval
                long retrievalStart = System.nanoTime();
                for (int i = 0; i < searchIters; i++) {
                    for (SearchResult.Hit hit : allHits) {
                        if (isCompanion) {
                            String json = searcher.docProjected(hit.getDocAddress(), "name");
                            assertNotNull(json);
                        } else {
                            try (Document doc = searcher.doc(hit.getDocAddress())) {
                                doc.getFirst("name");
                            }
                        }
                    }
                }
                result.retrievalMs = (System.nanoTime() - retrievalStart) / 1_000_000;

                result.queryRequests = SplitCacheManager.getObjectStorageRequestCount();
                result.queryBytes = SplitCacheManager.getObjectStorageBytesFetched();
                result.totalRequests = result.prewarmRequests + result.queryRequests;
                result.totalBytes = result.prewarmBytes + result.queryBytes;
            }
        }

        System.out.printf("  %s: search=%dms retrieval=%dms prewarm=%d/%,.0fKB query=%d/%,.0fKB%n",
                isCompanion ? "Companion " : "Standalone",
                result.searchMs, result.retrievalMs,
                result.prewarmRequests, result.prewarmBytes / 1024.0,
                result.queryRequests, result.queryBytes / 1024.0);

        return result;
    }

    // ───────────────────────────────────────────────────────────
    // Helpers
    // ───────────────────────────────────────────────────────────

    private static int standaloneCounter = 0;

    private QuickwitSplit.SplitMetadata createStandaloneSplit(Path outputSplit, int numRows) throws Exception {
        Path indexDir = tempDir.resolve("idx_standalone_" + numRows + "_" + (standaloneCounter++));
        Files.createDirectories(indexDir);

        try (SchemaBuilder builder = new SchemaBuilder()) {
            builder.addIntegerField("id", true, true, true);
            builder.addTextField("name", true, true, "raw", "position");
            builder.addFloatField("score", true, true, true);
            builder.addIntegerField("active", true, true, false);

            try (Schema schema = builder.build();
                 Index index = new Index(schema, indexDir.toString(), false);
                 IndexWriter writer = index.writer(Index.Memory.LARGE_HEAP_SIZE, 1)) {

                for (int i = 0; i < numRows; i++) {
                    try (Document doc = new Document()) {
                        doc.addInteger("id", i);
                        doc.addText("name", "item_" + i);
                        doc.addFloat("score", i * 1.5);
                        doc.addInteger("active", (i % 2 == 0) ? 1 : 0);
                        writer.addDocument(doc);
                    }
                }
                writer.commit();
            }

            QuickwitSplit.SplitConfig splitConfig = new QuickwitSplit.SplitConfig(
                    "bench-index", "bench-source", "bench-node");
            return QuickwitSplit.convertIndexFromPath(indexDir.toString(),
                    outputSplit.toString(), splitConfig);
        }
    }

    private void uploadToS3(Path localFile, String s3Key) {
        s3Client.putObject(
                PutObjectRequest.builder().bucket(TEST_BUCKET).key(s3Key).build(),
                RequestBody.fromFile(localFile));
    }
}

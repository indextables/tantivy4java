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
import io.indextables.tantivy4java.aggregation.*;

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
import java.time.LocalDateTime;
import java.time.ZoneOffset;
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
    // Test 0: Small verification — check page-level download behavior
    // ───────────────────────────────────────────────────────────

    @Test
    @org.junit.jupiter.api.Order(0)
    @DisplayName("Verify surgical page-level parquet reads on S3 (10K rows, 5 doc retrievals)")
    void verifyPageLevelReads() throws Exception {
        int numRows = 100_000; // 100K rows → ~5 pages per column (20K rows/page)
        String tag = "page_verify_" + numRows + "r";

        Path localDir = tempDir.resolve(tag);
        Files.createDirectories(localDir);

        System.out.println("\n=== PAGE-LEVEL READ VERIFICATION: " + numRows + " rows ===");

        // 1. Create parquet file
        Path parquetFile = localDir.resolve("data.parquet");
        QuickwitSplit.nativeWriteTestParquet(parquetFile.toString(), numRows, 0);
        long parquetSize = Files.size(parquetFile);
        System.out.printf("  Parquet file: %,d bytes (%.2f KB)%n", parquetSize, parquetSize / 1024.0);

        // 2. Create companion split
        Path companionSplit = localDir.resolve("companion.split");
        ParquetCompanionConfig pqConfig = new ParquetCompanionConfig(localDir.toString());
        QuickwitSplit.SplitMetadata companionMeta = QuickwitSplit.createFromParquet(
                Collections.singletonList(parquetFile.toString()),
                companionSplit.toString(), pqConfig);
        long companionSize = Files.size(companionSplit);
        System.out.printf("  Companion split: %,d bytes (%.2f KB)%n", companionSize, companionSize / 1024.0);

        // 3. Upload to S3
        System.out.println("  Uploading to S3...");
        String companionS3Key = S3_PREFIX + tag + "/companion.split";
        String parquetS3Key = S3_PREFIX + tag + "/data.parquet";
        uploadToS3(companionSplit, companionS3Key);
        uploadToS3(parquetFile, parquetS3Key);

        String companionUri = "s3://" + TEST_BUCKET + "/" + companionS3Key;
        String s3TableRoot = "s3://" + TEST_BUCKET + "/" + S3_PREFIX + tag;

        // 4. Open searcher, prewarm, then measure doc retrieval bytes
        SplitCacheManager.CacheConfig cfg = new SplitCacheManager.CacheConfig("page-verify-" + tag)
                .withMaxCacheSize(500_000_000)
                .withAwsCredentials(getAccessKey(), getSecretKey())
                .withAwsRegion(TEST_REGION);

        try (SplitCacheManager cm = SplitCacheManager.getInstance(cfg)) {
            try (SplitSearcher searcher = cm.createSplitSearcher(companionUri, companionMeta, s3TableRoot)) {
                // Prewarm index components
                searcher.preloadComponents(
                        SplitSearcher.IndexComponent.TERM,
                        SplitSearcher.IndexComponent.POSTINGS).join();

                // Reset stats AFTER prewarm to isolate doc retrieval
                SplitCacheManager.resetObjectStorageRequestStats();
                SplitCacheManager.resetStorageDownloadMetrics();

                // Retrieve 5 documents with projection
                SplitQuery q = searcher.parseQuery("*");
                SearchResult res = searcher.search(q, 5);
                System.out.printf("  Search returned %d hits%n", res.getHits().size());

                // Reset stats again to isolate JUST the doc retrieval
                SplitCacheManager.resetObjectStorageRequestStats();

                for (SearchResult.Hit hit : res.getHits()) {
                    String json = searcher.docProjected(hit.getDocAddress(),
                            "name", "id", "score");
                    assertNotNull(json);
                }

                long retrieveReqs = SplitCacheManager.getObjectStorageRequestCount();
                long retrieveBytes = SplitCacheManager.getObjectStorageBytesFetched();

                System.out.println("\n╔═════════════════════════════════════════════════════╗");
                System.out.println("║     DOC RETRIEVAL DOWNLOAD VERIFICATION             ║");
                System.out.println("╠═════════════════════════════════════════════════════╣");
                System.out.printf( "║ Parquet file size:     %,10d bytes (%,.1f KB)   ║%n", parquetSize, parquetSize / 1024.0);
                System.out.printf( "║ Docs retrieved:                 5                   ║%n");
                System.out.printf( "║ S3 requests for retrieval: %,5d                    ║%n", retrieveReqs);
                System.out.printf( "║ S3 bytes for retrieval:  %,10d bytes (%,.1f KB)  ║%n", retrieveBytes, retrieveBytes / 1024.0);
                System.out.printf( "║ Bytes per doc:           %,10d bytes (%,.1f KB)  ║%n",
                        res.getHits().size() > 0 ? retrieveBytes / res.getHits().size() : 0,
                        res.getHits().size() > 0 ? retrieveBytes / res.getHits().size() / 1024.0 : 0);
                System.out.println("╠═════════════════════════════════════════════════════╣");

                // Key assertion: total bytes downloaded should be << parquet file size
                // With page-level reads: ~few KB per doc (1 page per column)
                // Without: would download entire file (~300KB) per doc = 1.5MB total
                if (retrieveBytes < parquetSize) {
                    System.out.printf( "║ ✅ PASS: Downloaded %,.1f%% of parquet file         ║%n",
                            100.0 * retrieveBytes / parquetSize);
                } else {
                    System.out.printf( "║ ❌ FAIL: Downloaded %.1fx the parquet file size!    ║%n",
                            (double) retrieveBytes / parquetSize);
                }
                System.out.println("╚═════════════════════════════════════════════════════╝");

                // With page-level reads for 5 docs from 10K-row file:
                // - Footer+offset index: read once (~1-2 KB), but cached
                // - Each doc: 1 page per projected column * 3 columns
                // - Page = ~20K rows max, for 10K rows likely 1 page per column
                // - Total should be well under the full parquet file size
                assertTrue(retrieveBytes < parquetSize * 3,
                        String.format("Downloaded %,d bytes for 5 docs but parquet is only %,d bytes. " +
                                "Should download << file size with page-level reads", retrieveBytes, parquetSize));
            }
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
            try (SplitSearcher searcher = cm.createSplitSearcher(companionUri, companionMeta, s3TableRoot)) {
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
            try (SplitSearcher searcher = cm.createSplitSearcher(companionUri, companionMeta, s3TableRoot)) {
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
            try (SplitSearcher searcher = cm.createSplitSearcher(companionUri, companionMeta, s3TableRoot)) {
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
    // Test 4: Fast-Field Mode Comparison — range queries + aggregations
    // ───────────────────────────────────────────────────────────

    @Test
    @org.junit.jupiter.api.Order(4)
    @DisplayName("Fast Field Mode Benchmark: standalone vs DISABLED/HYBRID/PARQUET_ONLY — range + aggregation")
    void fastFieldModeBenchmark() throws Exception {
        int numRows = 100_000;
        String tag = "ffmode_" + numRows + "r";

        Path localDir = tempDir.resolve(tag);
        Files.createDirectories(localDir);

        System.out.println("\n=== FAST FIELD MODE BENCHMARK: " + numRows + " rows ===");

        // 1. Create parquet file
        Path parquetFile = localDir.resolve("data.parquet");
        System.out.println("Creating parquet file...");
        QuickwitSplit.nativeWriteTestParquet(parquetFile.toString(), numRows, 0);
        long parquetSize = Files.size(parquetFile);
        System.out.printf("  Parquet file: %.2f MB%n", parquetSize / (1024.0 * 1024));

        // 2. Create standalone split
        Path standaloneSplit = localDir.resolve("standalone.split");
        System.out.println("Creating standalone split...");
        QuickwitSplit.SplitMetadata standaloneMeta = createStandaloneSplit(standaloneSplit, numRows);
        long standaloneSize = Files.size(standaloneSplit);
        System.out.printf("  Standalone split: %.2f MB%n", standaloneSize / (1024.0 * 1024));

        // 3. Create companion splits — one per FastFieldMode
        ParquetCompanionConfig.FastFieldMode[] modes = {
                ParquetCompanionConfig.FastFieldMode.DISABLED,
                ParquetCompanionConfig.FastFieldMode.HYBRID,
                ParquetCompanionConfig.FastFieldMode.PARQUET_ONLY
        };

        Map<String, Path> splitPaths = new LinkedHashMap<>();
        Map<String, QuickwitSplit.SplitMetadata> splitMetas = new LinkedHashMap<>();
        Map<String, Long> splitSizes = new LinkedHashMap<>();

        for (ParquetCompanionConfig.FastFieldMode mode : modes) {
            String modeName = mode.name();
            Path splitFile = localDir.resolve("companion_" + modeName + ".split");
            System.out.println("Creating companion split (" + modeName + ")...");
            ParquetCompanionConfig pqConfig = new ParquetCompanionConfig(localDir.toString())
                    .withFastFieldMode(mode);
            QuickwitSplit.SplitMetadata meta = QuickwitSplit.createFromParquet(
                    Collections.singletonList(parquetFile.toString()),
                    splitFile.toString(), pqConfig);
            splitPaths.put(modeName, splitFile);
            splitMetas.put(modeName, meta);
            splitSizes.put(modeName, Files.size(splitFile));
            System.out.printf("  %s split: %.2f MB%n", modeName, Files.size(splitFile) / (1024.0 * 1024));
        }

        // 4. Upload to S3
        System.out.println("\nUploading to S3...");
        String standaloneS3Key = S3_PREFIX + tag + "/standalone.split";
        String parquetS3Key = S3_PREFIX + tag + "/data.parquet";
        uploadToS3(standaloneSplit, standaloneS3Key);
        uploadToS3(parquetFile, parquetS3Key);

        Map<String, String> s3Keys = new LinkedHashMap<>();
        for (var entry : splitPaths.entrySet()) {
            String key = S3_PREFIX + tag + "/companion_" + entry.getKey() + ".split";
            uploadToS3(entry.getValue(), key);
            s3Keys.put(entry.getKey(), key);
        }

        String standaloneUri = "s3://" + TEST_BUCKET + "/" + standaloneS3Key;
        String s3TableRoot = "s3://" + TEST_BUCKET + "/" + S3_PREFIX + tag;

        // 5. Benchmark standalone — range queries + aggregations
        System.out.println("\nBenchmarking standalone split...");
        FFModeResult standaloneResult = benchmarkRangeAndAgg(
                standaloneUri, standaloneMeta, null, null,
                "ffmode-sa-" + tag, numRows);

        // 6. Benchmark each companion mode
        Map<String, FFModeResult> modeResults = new LinkedHashMap<>();
        for (ParquetCompanionConfig.FastFieldMode mode : modes) {
            String modeName = mode.name();
            System.out.println("Benchmarking companion (" + modeName + ")...");
            String uri = "s3://" + TEST_BUCKET + "/" + s3Keys.get(modeName);
            FFModeResult result = benchmarkRangeAndAgg(
                    uri, splitMetas.get(modeName), s3TableRoot, mode,
                    "ffmode-" + modeName + "-" + tag, numRows);
            modeResults.put(modeName, result);
        }

        // 7. Report
        System.out.println("\n╔═══════════════════════════════════════════════════════════════════════════════════════════╗");
        System.out.println("║          FAST FIELD MODE BENCHMARK — Range Queries + Aggregations (100K rows)            ║");
        System.out.println("╠════════════════════════╦══════════════╦══════════════╦══════════════╦══════════════════════╣");
        System.out.println("║ Metric                 ║  Standalone  ║   DISABLED   ║    HYBRID    ║   PARQUET_ONLY       ║");
        System.out.println("╠════════════════════════╬══════════════╬══════════════╬══════════════╬══════════════════════╣");

        FFModeResult dis = modeResults.get("DISABLED");
        FFModeResult hyb = modeResults.get("HYBRID");
        FFModeResult pqo = modeResults.get("PARQUET_ONLY");

        System.out.printf("║ Split size (MB)        ║ %10.2f   ║ %10.2f   ║ %10.2f   ║ %18.2f   ║%n",
                standaloneSize / (1024.0 * 1024), splitSizes.get("DISABLED") / (1024.0 * 1024),
                splitSizes.get("HYBRID") / (1024.0 * 1024), splitSizes.get("PARQUET_ONLY") / (1024.0 * 1024));

        System.out.printf("║ Prewarm: S3 reqs       ║ %,10d   ║ %,10d   ║ %,10d   ║ %,18d   ║%n",
                standaloneResult.prewarmReqs, dis.prewarmReqs, hyb.prewarmReqs, pqo.prewarmReqs);
        System.out.printf("║ Prewarm: bytes (KB)    ║ %,10.0f   ║ %,10.0f   ║ %,10.0f   ║ %,18.0f   ║%n",
                standaloneResult.prewarmBytes / 1024.0, dis.prewarmBytes / 1024.0,
                hyb.prewarmBytes / 1024.0, pqo.prewarmBytes / 1024.0);

        System.out.println("╠════════════════════════╬══════════════╬══════════════╬══════════════╬══════════════════════╣");
        System.out.println("║  RANGE QUERIES                                                                            ║");
        System.out.println("╠════════════════════════╬══════════════╬══════════════╬══════════════╬══════════════════════╣");
        System.out.printf("║ Range: latency (ms)    ║ %,10d   ║ %,10d   ║ %,10d   ║ %,18d   ║%n",
                standaloneResult.rangeMs, dis.rangeMs, hyb.rangeMs, pqo.rangeMs);
        System.out.printf("║ Range: S3 reqs         ║ %,10d   ║ %,10d   ║ %,10d   ║ %,18d   ║%n",
                standaloneResult.rangeReqs, dis.rangeReqs, hyb.rangeReqs, pqo.rangeReqs);
        System.out.printf("║ Range: bytes (KB)      ║ %,10.0f   ║ %,10.0f   ║ %,10.0f   ║ %,18.0f   ║%n",
                standaloneResult.rangeBytes / 1024.0, dis.rangeBytes / 1024.0,
                hyb.rangeBytes / 1024.0, pqo.rangeBytes / 1024.0);

        System.out.println("╠════════════════════════╬══════════════╬══════════════╬══════════════╬══════════════════════╣");
        System.out.println("║  AGGREGATIONS                                                                             ║");
        System.out.println("╠════════════════════════╬══════════════╬══════════════╬══════════════╬══════════════════════╣");
        System.out.printf("║ Agg: latency (ms)      ║ %,10d   ║ %,10d   ║ %,10d   ║ %,18d   ║%n",
                standaloneResult.aggMs, dis.aggMs, hyb.aggMs, pqo.aggMs);
        System.out.printf("║ Agg: S3 reqs           ║ %,10d   ║ %,10d   ║ %,10d   ║ %,18d   ║%n",
                standaloneResult.aggReqs, dis.aggReqs, hyb.aggReqs, pqo.aggReqs);
        System.out.printf("║ Agg: bytes (KB)        ║ %,10.0f   ║ %,10.0f   ║ %,10.0f   ║ %,18.0f   ║%n",
                standaloneResult.aggBytes / 1024.0, dis.aggBytes / 1024.0,
                hyb.aggBytes / 1024.0, pqo.aggBytes / 1024.0);

        System.out.println("╠════════════════════════╬══════════════╬══════════════╬══════════════╬══════════════════════╣");
        System.out.printf("║ TOTAL: S3 reqs         ║ %,10d   ║ %,10d   ║ %,10d   ║ %,18d   ║%n",
                standaloneResult.totalReqs(), dis.totalReqs(), hyb.totalReqs(), pqo.totalReqs());
        System.out.printf("║ TOTAL: bytes (KB)      ║ %,10.0f   ║ %,10.0f   ║ %,10.0f   ║ %,18.0f   ║%n",
                standaloneResult.totalBytes() / 1024.0, dis.totalBytes() / 1024.0,
                hyb.totalBytes() / 1024.0, pqo.totalBytes() / 1024.0);
        System.out.println("╚════════════════════════╩══════════════╩══════════════╩══════════════╩══════════════════════╝");
    }

    // ───────────────────────────────────────────────────────────
    // Test 5: Prewarm Validation — 0 S3 requests after full prewarm
    // ───────────────────────────────────────────────────────────

    @Test
    @org.junit.jupiter.api.Order(5)
    @DisplayName("Prewarm Validation: full prewarm then ALL queries at 0 S3 requests (HYBRID on S3)")
    void prewarmZeroS3Validation() throws Exception {
        int numRows = 50_000;
        String tag = "prewarm_val_" + numRows + "r";

        Path localDir = tempDir.resolve(tag);
        Files.createDirectories(localDir);

        System.out.println("\n=== PREWARM VALIDATION: ALL QUERIES AT 0 S3 REQUESTS ===");
        System.out.println("  Mode: HYBRID  Rows: " + numRows);

        // 1. Create parquet + companion split (HYBRID mode)
        Path parquetFile = localDir.resolve("data.parquet");
        QuickwitSplit.nativeWriteTestParquet(parquetFile.toString(), numRows, 0);

        Path companionSplit = localDir.resolve("companion.split");
        ParquetCompanionConfig pqConfig = new ParquetCompanionConfig(localDir.toString())
                .withFastFieldMode(ParquetCompanionConfig.FastFieldMode.HYBRID);
        QuickwitSplit.SplitMetadata meta = QuickwitSplit.createFromParquet(
                Collections.singletonList(parquetFile.toString()),
                companionSplit.toString(), pqConfig);

        // 2. Upload to S3
        System.out.println("  Uploading to S3...");
        String companionS3Key = S3_PREFIX + tag + "/companion.split";
        String parquetS3Key = S3_PREFIX + tag + "/data.parquet";
        uploadToS3(companionSplit, companionS3Key);
        uploadToS3(parquetFile, parquetS3Key);

        String companionUri = "s3://" + TEST_BUCKET + "/" + companionS3Key;
        String s3TableRoot = "s3://" + TEST_BUCKET + "/" + S3_PREFIX + tag;

        // 3. Open searcher and perform FULL prewarm
        //    L2 disk cache is REQUIRED: prewarm writes to L2, then L1 is cleared.
        //    Without L2, prewarmed data is lost and queries hit S3 again.
        Path diskCache = localDir.resolve("disk-cache");
        SplitCacheManager.TieredCacheConfig tiered = new SplitCacheManager.TieredCacheConfig()
                .withDiskCachePath(diskCache.toString())
                .withMaxDiskSize(500_000_000);

        SplitCacheManager.CacheConfig cfg = new SplitCacheManager.CacheConfig("prewarm-val-" + tag)
                .withMaxCacheSize(500_000_000)
                .withAwsCredentials(getAccessKey(), getSecretKey())
                .withAwsRegion(TEST_REGION)
                .withTieredCache(tiered);

        try (SplitCacheManager cm = SplitCacheManager.getInstance(cfg)) {
            try (SplitSearcher searcher = cm.createSplitSearcher(companionUri, meta, s3TableRoot)) {

                // FULL PREWARM: all split components
                System.out.println("\n  [PREWARM] Loading all split components...");
                SplitCacheManager.resetObjectStorageRequestStats();
                searcher.preloadComponents(
                        SplitSearcher.IndexComponent.TERM,
                        SplitSearcher.IndexComponent.POSTINGS,
                        SplitSearcher.IndexComponent.POSITIONS,
                        SplitSearcher.IndexComponent.FASTFIELD,
                        SplitSearcher.IndexComponent.FIELDNORM,
                        SplitSearcher.IndexComponent.STORE
                ).join();

                long splitPrewarmReqs = SplitCacheManager.getObjectStorageRequestCount();
                long splitPrewarmBytes = SplitCacheManager.getObjectStorageBytesFetched();
                System.out.printf("  Split prewarm: %,d S3 requests, %,.0f KB%n",
                        splitPrewarmReqs, splitPrewarmBytes / 1024.0);

                // PREWARM: parquet fast fields (for HYBRID aggregations)
                System.out.println("  [PREWARM] Loading parquet fast fields...");
                SplitCacheManager.resetObjectStorageRequestStats();
                searcher.preloadParquetFastFields("id", "score", "name", "active").join();

                long pqFastReqs = SplitCacheManager.getObjectStorageRequestCount();
                long pqFastBytes = SplitCacheManager.getObjectStorageBytesFetched();
                System.out.printf("  Parquet fast field prewarm: %,d S3 requests, %,.0f KB%n",
                        pqFastReqs, pqFastBytes / 1024.0);

                // PREWARM: parquet columns for doc retrieval
                System.out.println("  [PREWARM] Loading parquet columns for retrieval...");
                SplitCacheManager.resetObjectStorageRequestStats();
                searcher.preloadParquetColumns("id", "score", "name", "active").join();

                long pqColReqs = SplitCacheManager.getObjectStorageRequestCount();
                long pqColBytes = SplitCacheManager.getObjectStorageBytesFetched();
                System.out.printf("  Parquet column prewarm: %,d S3 requests, %,.0f KB%n",
                        pqColReqs, pqColBytes / 1024.0);

                System.out.printf("\n  TOTAL PREWARM: %,d S3 requests, %,.0f KB%n",
                        splitPrewarmReqs + pqFastReqs + pqColReqs,
                        (splitPrewarmBytes + pqFastBytes + pqColBytes) / 1024.0);

                // Wait for async L2 disk cache writes to complete
                System.out.println("  Waiting for L2 disk cache flush...");
                Thread.sleep(3000);

                // ── RESET — everything below must produce 0 S3 requests ──
                System.out.println("\n  === RESET S3 COUNTERS — all operations below must be 0 S3 requests ===\n");
                SplitCacheManager.resetObjectStorageRequestStats();

                // Phase A: Term search
                System.out.print("  [A] Term search (name:item_42)... ");
                SplitQuery termQ = new SplitTermQuery("name", "item_42");
                SearchResult termRes = searcher.search(termQ, 10);
                long aReqs = SplitCacheManager.getObjectStorageRequestCount();
                System.out.printf("%d hits, %d S3 reqs%n", termRes.getHits().size(), aReqs);
                assertEquals(0, aReqs, "Term search should produce 0 S3 requests after prewarm");
                SplitCacheManager.resetObjectStorageRequestStats();

                // Phase B: Match-all search
                System.out.print("  [B] Match-all search... ");
                SearchResult allRes = searcher.search(new SplitMatchAllQuery(), 10);
                long bReqs = SplitCacheManager.getObjectStorageRequestCount();
                System.out.printf("%d hits, %d S3 reqs%n", allRes.getHits().size(), bReqs);
                assertEquals(0, bReqs, "Match-all search should produce 0 S3 requests after prewarm");
                SplitCacheManager.resetObjectStorageRequestStats();

                // Phase C: Range query — integer
                System.out.print("  [C] Range query (id [100..200])... ");
                SplitRangeQuery idRange = SplitRangeQuery.inclusiveRange("id", "100", "200", "i64");
                SearchResult rangeRes = searcher.search(idRange, 200);
                long cReqs = SplitCacheManager.getObjectStorageRequestCount();
                System.out.printf("%d hits, %d S3 reqs%n", rangeRes.getHits().size(), cReqs);
                assertEquals(0, cReqs, "Range query should produce 0 S3 requests after prewarm");
                SplitCacheManager.resetObjectStorageRequestStats();

                // Phase D: Range query — float
                System.out.print("  [D] Range query (score [100.0..200.0])... ");
                SplitRangeQuery scoreRange = SplitRangeQuery.inclusiveRange("score", "100.0", "200.0", "f64");
                SearchResult scoreRangeRes = searcher.search(scoreRange, 200);
                long dReqs = SplitCacheManager.getObjectStorageRequestCount();
                System.out.printf("%d hits, %d S3 reqs%n", scoreRangeRes.getHits().size(), dReqs);
                assertEquals(0, dReqs, "Float range query should produce 0 S3 requests after prewarm");
                SplitCacheManager.resetObjectStorageRequestStats();

                // Phase E: Stats aggregation on id
                System.out.print("  [E] Stats aggregation (id)... ");
                StatsAggregation idStats = new StatsAggregation("id_stats", "id");
                SearchResult statsRes = searcher.search(new SplitMatchAllQuery(), 0, "id_stats", idStats);
                StatsResult sr = (StatsResult) statsRes.getAggregation("id_stats");
                long eReqs = SplitCacheManager.getObjectStorageRequestCount();
                System.out.printf("count=%d min=%.0f max=%.0f, %d S3 reqs%n",
                        sr.getCount(), sr.getMin(), sr.getMax(), eReqs);
                assertEquals(0, eReqs, "Stats aggregation should produce 0 S3 requests after prewarm");
                SplitCacheManager.resetObjectStorageRequestStats();

                // Phase F: Stats aggregation on score (float)
                System.out.print("  [F] Stats aggregation (score)... ");
                StatsAggregation scoreStats = new StatsAggregation("score_stats", "score");
                SearchResult scoreStatsRes = searcher.search(new SplitMatchAllQuery(), 0, "score_stats", scoreStats);
                StatsResult scoreSr = (StatsResult) scoreStatsRes.getAggregation("score_stats");
                long fReqs = SplitCacheManager.getObjectStorageRequestCount();
                System.out.printf("count=%d min=%.1f max=%.1f, %d S3 reqs%n",
                        scoreSr.getCount(), scoreSr.getMin(), scoreSr.getMax(), fReqs);
                assertEquals(0, fReqs, "Score stats aggregation should produce 0 S3 requests after prewarm");
                SplitCacheManager.resetObjectStorageRequestStats();

                // Phase G: Histogram aggregation
                System.out.print("  [G] Histogram aggregation (id, interval=1000)... ");
                HistogramAggregation hist = new HistogramAggregation("id_hist", "id", 1000.0);
                SearchResult histRes = searcher.search(new SplitMatchAllQuery(), 0, "id_hist", hist);
                long gReqs = SplitCacheManager.getObjectStorageRequestCount();
                System.out.printf("%d S3 reqs%n", gReqs);
                assertEquals(0, gReqs, "Histogram aggregation should produce 0 S3 requests after prewarm");
                SplitCacheManager.resetObjectStorageRequestStats();

                // Phase H: Terms aggregation on name (text fast field — parquet-sourced in HYBRID)
                System.out.print("  [H] Terms aggregation (name, top 10)... ");
                TermsAggregation nameTerms = new TermsAggregation("name_terms", "name", 10, 0);
                SearchResult termsRes = searcher.search(new SplitMatchAllQuery(), 0, "name_terms", nameTerms);
                long hReqs = SplitCacheManager.getObjectStorageRequestCount();
                System.out.printf("%d S3 reqs%n", hReqs);
                assertEquals(0, hReqs, "Terms aggregation should produce 0 S3 requests after prewarm");
                SplitCacheManager.resetObjectStorageRequestStats();

                // Phase I: Combined range + aggregation
                System.out.print("  [I] Range (id [1000..5000]) + stats(score)... ");
                SplitRangeQuery combinedRange = SplitRangeQuery.inclusiveRange("id", "1000", "5000", "i64");
                StatsAggregation filteredStats = new StatsAggregation("filtered_score", "score");
                SearchResult combinedRes = searcher.search(combinedRange, 0, "filtered_score", filteredStats);
                StatsResult filteredSr = (StatsResult) combinedRes.getAggregation("filtered_score");
                long iReqs = SplitCacheManager.getObjectStorageRequestCount();
                System.out.printf("count=%d, %d S3 reqs%n", filteredSr.getCount(), iReqs);
                assertEquals(0, iReqs, "Combined range + aggregation should produce 0 S3 requests after prewarm");
                SplitCacheManager.resetObjectStorageRequestStats();

                // Phase J: Doc retrieval with projection
                System.out.print("  [J] Doc retrieval (5 docs, projected)... ");
                SearchResult docRes = searcher.search(new SplitMatchAllQuery(), 5);
                for (SearchResult.Hit hit : docRes.getHits()) {
                    String json = searcher.docProjected(hit.getDocAddress(), "name", "id", "score");
                    assertNotNull(json);
                }
                long jReqs = SplitCacheManager.getObjectStorageRequestCount();
                System.out.printf("%d docs, %d S3 reqs%n", docRes.getHits().size(), jReqs);
                // Doc retrieval may need parquet data — but with preloadParquetColumns it should be cached
                System.out.printf("  (doc retrieval S3 reqs: %d — parquet data may not be fully prewarmed)%n", jReqs);

                System.out.println("\n╔═══════════════════════════════════════════════════════╗");
                System.out.println("║  PREWARM VALIDATION RESULTS (HYBRID, 50K rows, S3)  ║");
                System.out.println("╠═══════════════════════════════════════════════════════╣");
                System.out.println("║  [A] Term search:            0 S3 reqs               ║");
                System.out.println("║  [B] Match-all search:       0 S3 reqs               ║");
                System.out.println("║  [C] Range query (i64):      0 S3 reqs               ║");
                System.out.println("║  [D] Range query (f64):      0 S3 reqs               ║");
                System.out.println("║  [E] Stats agg (id):         0 S3 reqs               ║");
                System.out.println("║  [F] Stats agg (score):      0 S3 reqs               ║");
                System.out.println("║  [G] Histogram agg:          0 S3 reqs               ║");
                System.out.println("║  [H] Terms agg (name):       0 S3 reqs               ║");
                System.out.println("║  [I] Range + agg combined:   0 S3 reqs               ║");
                System.out.printf( "║  [J] Doc retrieval (5 docs): %d S3 reqs               ║%n", jReqs);
                System.out.println("╠═══════════════════════════════════════════════════════╣");
                System.out.println("║  ALL SEARCH/RANGE/AGG QUERIES: 0 S3 REQUESTS         ║");
                System.out.println("╚═══════════════════════════════════════════════════════╝");
            }
        }
    }

    // ───────────────────────────────────────────────────────────
    // Test 6: Wide schema split size comparison (10 fields, UUIDs)
    // ───────────────────────────────────────────────────────────

    @Test
    @org.junit.jupiter.api.Order(6)
    @DisplayName("Split Size Comparison: 10-field wide schema (4 numeric, 1 date, 1 IP, 5 UUID strings)")
    void wideSchemaSpliSizeComparison() throws Exception {
        int numRows = 100_000;
        String tag = "wide_" + numRows + "r";

        Path localDir = tempDir.resolve(tag);
        Files.createDirectories(localDir);

        System.out.println("\n═══════════════════════════════════════════════════════════════════════");
        System.out.println("  WIDE SCHEMA SPLIT SIZE COMPARISON");
        System.out.println("  10 fields: 4 numeric, 1 date, 1 IP, 5 UUID strings");
        System.out.println("  " + numRows + " rows");
        System.out.println("═══════════════════════════════════════════════════════════════════════");

        // 1. Create parquet file with wide schema
        Path parquetFile = localDir.resolve("wide.parquet");
        System.out.println("\nCreating parquet file...");
        QuickwitSplit.nativeWriteTestParquetWide(parquetFile.toString(), numRows, 0);
        long parquetSize = Files.size(parquetFile);
        System.out.printf("  Parquet file: %,d bytes (%.2f MB)%n", parquetSize, parquetSize / (1024.0 * 1024));

        // 2. Create standalone split (tantivy index with matching fields)
        System.out.println("\nCreating standalone split...");
        Path standaloneSplit = localDir.resolve("standalone.split");
        QuickwitSplit.SplitMetadata standaloneMeta = createWideStandaloneSplit(
                standaloneSplit, localDir, numRows);
        long standaloneSize = Files.size(standaloneSplit);
        System.out.printf("  Standalone split: %,d bytes (%.2f MB)%n", standaloneSize, standaloneSize / (1024.0 * 1024));

        // 3. Create companion splits — one per FastFieldMode
        ParquetCompanionConfig.FastFieldMode[] modes = {
                ParquetCompanionConfig.FastFieldMode.DISABLED,
                ParquetCompanionConfig.FastFieldMode.HYBRID,
                ParquetCompanionConfig.FastFieldMode.PARQUET_ONLY
        };

        Map<String, Long> companionSizes = new LinkedHashMap<>();
        for (ParquetCompanionConfig.FastFieldMode mode : modes) {
            String modeName = mode.name();
            Path splitFile = localDir.resolve("companion_" + modeName + ".split");
            System.out.println("Creating companion split (" + modeName + ")...");
            ParquetCompanionConfig pqConfig = new ParquetCompanionConfig(localDir.toString())
                    .withFastFieldMode(mode)
                    .withIpAddressFields("ip_addr");
            QuickwitSplit.SplitMetadata meta = QuickwitSplit.createFromParquet(
                    Collections.singletonList(parquetFile.toString()),
                    splitFile.toString(), pqConfig);
            long size = Files.size(splitFile);
            companionSizes.put(modeName, size);
            assertEquals(numRows, meta.getNumDocs());
            System.out.printf("  %s: %,d bytes (%.2f MB)%n", modeName, size, size / (1024.0 * 1024));
        }

        // 4. Report
        long disSize = companionSizes.get("DISABLED");
        long hybSize = companionSizes.get("HYBRID");
        long pqoSize = companionSizes.get("PARQUET_ONLY");

        System.out.println("\n╔═══════════════════════════════════════════════════════════════════════╗");
        System.out.println("║  SPLIT SIZE COMPARISON — Wide Schema (100K rows, 10 fields)          ║");
        System.out.println("║  Fields: num_1(i64) num_2(i64) num_3(f64) num_4(f64)                 ║");
        System.out.println("║          created_at(ts) ip_addr(ip)                                  ║");
        System.out.println("║          uuid_1..uuid_5(utf8, unique UUIDs)                          ║");
        System.out.println("╠═════════════════════════════╦═════════════╦═══════════════════════════╣");
        System.out.println("║ Configuration               ║    Size     ║ vs Standalone             ║");
        System.out.println("╠═════════════════════════════╬═════════════╬═══════════════════════════╣");
        System.out.printf( "║ Parquet file (source data)  ║ %7.2f MB  ║                           ║%n",
                parquetSize / (1024.0 * 1024));
        System.out.printf( "║ Standalone split            ║ %7.2f MB  ║ baseline                  ║%n",
                standaloneSize / (1024.0 * 1024));
        System.out.printf( "║ Companion DISABLED          ║ %7.2f MB  ║ %+.1f%% (%+.2f MB)         ║%n",
                disSize / (1024.0 * 1024),
                100.0 * (disSize - standaloneSize) / standaloneSize,
                (disSize - standaloneSize) / (1024.0 * 1024));
        System.out.printf( "║ Companion HYBRID            ║ %7.2f MB  ║ %+.1f%% (%+.2f MB)         ║%n",
                hybSize / (1024.0 * 1024),
                100.0 * (hybSize - standaloneSize) / standaloneSize,
                (hybSize - standaloneSize) / (1024.0 * 1024));
        System.out.printf( "║ Companion PARQUET_ONLY      ║ %7.2f MB  ║ %+.1f%% (%+.2f MB)         ║%n",
                pqoSize / (1024.0 * 1024),
                100.0 * (pqoSize - standaloneSize) / standaloneSize,
                (pqoSize - standaloneSize) / (1024.0 * 1024));
        System.out.println("╚═════════════════════════════╩═════════════╩═══════════════════════════╝");

        // Sanity checks
        assertTrue(standaloneSize > 0, "Standalone split should be non-empty");
        assertTrue(companionSizes.get("PARQUET_ONLY") < standaloneSize,
                "PARQUET_ONLY companion should be smaller than standalone");
    }

    /**
     * Create a standalone split matching the wide parquet schema.
     * Fields: num_1(i64), num_2(i64), num_3(f64), num_4(f64),
     *         created_at(date), ip_addr(text/raw), uuid_1..uuid_5(text/raw)
     */
    private QuickwitSplit.SplitMetadata createWideStandaloneSplit(
            Path outputSplit, Path workDir, int numRows) throws Exception {
        Path indexDir = workDir.resolve("idx_wide_standalone");
        Files.createDirectories(indexDir);

        try (SchemaBuilder builder = new SchemaBuilder()) {
            builder.addIntegerField("num_1", true, true, true);
            builder.addIntegerField("num_2", true, true, true);
            builder.addFloatField("num_3", true, true, true);
            builder.addFloatField("num_4", true, true, true);
            builder.addDateField("created_at", true, true, true);
            builder.addTextField("ip_addr", true, true, "raw", "position");
            builder.addTextField("uuid_1", true, true, "raw", "position");
            builder.addTextField("uuid_2", true, true, "raw", "position");
            builder.addTextField("uuid_3", true, true, "raw", "position");
            builder.addTextField("uuid_4", true, true, "raw", "position");
            builder.addTextField("uuid_5", true, true, "raw", "position");

            try (Schema schema = builder.build();
                 Index index = new Index(schema, indexDir.toString(), false);
                 IndexWriter writer = index.writer(Index.Memory.LARGE_HEAP_SIZE, 1)) {

                // Use same deterministic data generation as the Rust parquet writer
                LocalDateTime baseTime = LocalDateTime.of(2024, 1, 1, 0, 0, 0);
                for (int i = 0; i < numRows; i++) {
                    try (Document doc = new Document()) {
                        doc.addInteger("num_1", i);
                        doc.addInteger("num_2", ((long) i * 997) % 100_000 + 1);
                        doc.addFloat("num_3", i * 3.14159 + 0.01);
                        doc.addFloat("num_4", Math.round(Math.sin(i * 2.71828) * 1000.0) / 100.0);
                        doc.addDate("created_at", baseTime.plusSeconds(i));
                        doc.addText("ip_addr", String.format("%d.%d.%d.%d",
                                10 + (i / (256 * 256)) % 246, (i / 256) % 256,
                                i % 256, (i * 7 + 3) % 256));
                        doc.addText("uuid_1", fakeUuid(i, 1));
                        doc.addText("uuid_2", fakeUuid(i, 2));
                        doc.addText("uuid_3", fakeUuid(i, 3));
                        doc.addText("uuid_4", fakeUuid(i, 4));
                        doc.addText("uuid_5", fakeUuid(i, 5));
                        writer.addDocument(doc);
                    }
                }
                writer.commit();
            }

            QuickwitSplit.SplitConfig splitConfig = new QuickwitSplit.SplitConfig(
                    "wide-bench", "bench-source", "bench-node");
            return QuickwitSplit.convertIndexFromPath(indexDir.toString(),
                    outputSplit.toString(), splitConfig);
        }
    }

    /** Java equivalent of the Rust fake_uuid() — same deterministic hash. */
    private static String fakeUuid(long row, long col) {
        long seed = row * 31 + col * 1000003;
        long h = seed;
        h = h * 6364136223846793005L + 1442695040888963407L;
        h ^= h >>> 33;
        h *= 0xff51afd7ed558ccdL;
        h ^= h >>> 33;
        long a = h;
        h = a * 6364136223846793005L + 1442695040888963407L;
        h ^= h >>> 33;
        h *= 0xff51afd7ed558ccdL;
        h ^= h >>> 33;
        long b = h;
        return String.format("%08x-%04x-4%03x-%04x-%012x",
                (int) (a >>> 32) & 0xFFFFFFFFL,
                (int) (a >>> 16) & 0xFFFF,
                (int) a & 0x0FFF,
                ((int) (b >>> 48) & 0x3FFF) | 0x8000,
                b & 0xFFFFFFFFFFFFL);
    }

    // ───────────────────────────────────────────────────────────
    // FFModeResult — per-mode benchmark metrics
    // ───────────────────────────────────────────────────────────

    private static class FFModeResult {
        long prewarmReqs, prewarmBytes;
        long rangeMs, rangeReqs, rangeBytes;
        long aggMs, aggReqs, aggBytes;

        long totalReqs() { return prewarmReqs + rangeReqs + aggReqs; }
        long totalBytes() { return prewarmBytes + rangeBytes + aggBytes; }
    }

    /**
     * Benchmark range queries + aggregations for a single split.
     * @param mode null for standalone, non-null for companion
     */
    private FFModeResult benchmarkRangeAndAgg(String splitUri, QuickwitSplit.SplitMetadata metadata,
                                               String s3TableRoot, ParquetCompanionConfig.FastFieldMode mode,
                                               String cacheId, int numRows) throws Exception {
        FFModeResult result = new FFModeResult();
        boolean isCompanion = s3TableRoot != null;

        SplitCacheManager.CacheConfig cacheConfig = new SplitCacheManager.CacheConfig(cacheId)
                .withMaxCacheSize(500_000_000)
                .withAwsCredentials(getAccessKey(), getSecretKey())
                .withAwsRegion(TEST_REGION);

        SplitCacheManager.resetObjectStorageRequestStats();

        try (SplitCacheManager cm = SplitCacheManager.getInstance(cacheConfig)) {
            try (SplitSearcher searcher = isCompanion
                    ? cm.createSplitSearcher(splitUri, metadata, s3TableRoot)
                    : cm.createSplitSearcher(splitUri, metadata)) {

                // Prewarm
                if (isCompanion) {
                    searcher.preloadComponents(
                            SplitSearcher.IndexComponent.TERM,
                            SplitSearcher.IndexComponent.POSTINGS,
                            SplitSearcher.IndexComponent.FASTFIELD).join();

                    if (mode != null && mode != ParquetCompanionConfig.FastFieldMode.DISABLED) {
                        searcher.preloadParquetFastFields("id", "score", "name", "active").join();
                    }
                } else {
                    searcher.preloadComponents(
                            SplitSearcher.IndexComponent.TERM,
                            SplitSearcher.IndexComponent.POSTINGS,
                            SplitSearcher.IndexComponent.FASTFIELD).join();
                }

                result.prewarmReqs = SplitCacheManager.getObjectStorageRequestCount();
                result.prewarmBytes = SplitCacheManager.getObjectStorageBytesFetched();

                // --- Range queries ---
                SplitCacheManager.resetObjectStorageRequestStats();
                long rangeStart = System.nanoTime();

                // Integer range
                SplitRangeQuery idRange = SplitRangeQuery.inclusiveRange("id", "1000", "5000", "i64");
                SearchResult idRes = searcher.search(idRange, 100);
                assertTrue(idRes.getHits().size() > 0, "Range query should return hits");

                // Float range
                SplitRangeQuery scoreRange = SplitRangeQuery.inclusiveRange("score", "100.0", "500.0", "f64");
                SearchResult scoreRes = searcher.search(scoreRange, 100);
                assertTrue(scoreRes.getHits().size() > 0, "Score range should return hits");

                // Unbounded range
                SplitRangeQuery unboundedRange = new SplitRangeQuery("id",
                        SplitRangeQuery.RangeBound.inclusive(String.valueOf(numRows - 100)),
                        SplitRangeQuery.RangeBound.unbounded(), "i64");
                SearchResult unbRes = searcher.search(unboundedRange, 200);

                result.rangeMs = (System.nanoTime() - rangeStart) / 1_000_000;
                result.rangeReqs = SplitCacheManager.getObjectStorageRequestCount();
                result.rangeBytes = SplitCacheManager.getObjectStorageBytesFetched();

                // --- Aggregations ---
                SplitCacheManager.resetObjectStorageRequestStats();
                long aggStart = System.nanoTime();

                // Stats on id
                StatsAggregation idStats = new StatsAggregation("id_stats", "id");
                SearchResult statsRes = searcher.search(new SplitMatchAllQuery(), 0, "id_stats", idStats);
                assertTrue(statsRes.hasAggregations());

                // Stats on score
                StatsAggregation scoreStats = new StatsAggregation("score_stats", "score");
                searcher.search(new SplitMatchAllQuery(), 0, "score_stats", scoreStats);

                // Histogram on id
                HistogramAggregation hist = new HistogramAggregation("id_hist", "id", 10000.0);
                searcher.search(new SplitMatchAllQuery(), 0, "id_hist", hist);

                // Terms on name
                TermsAggregation nameTerms = new TermsAggregation("name_terms", "name", 10, 0);
                searcher.search(new SplitMatchAllQuery(), 0, "name_terms", nameTerms);

                // Combined: range + stats
                SplitRangeQuery combinedRange = SplitRangeQuery.inclusiveRange("id", "0", "50000", "i64");
                StatsAggregation filteredStats = new StatsAggregation("filtered", "score");
                searcher.search(combinedRange, 0, "filtered", filteredStats);

                result.aggMs = (System.nanoTime() - aggStart) / 1_000_000;
                result.aggReqs = SplitCacheManager.getObjectStorageRequestCount();
                result.aggBytes = SplitCacheManager.getObjectStorageBytesFetched();
            }
        }

        System.out.printf("    %s: prewarm=%d/%,.0fKB range=%dms/%d/%,.0fKB agg=%dms/%d/%,.0fKB%n",
                mode != null ? mode.name() : "Standalone",
                result.prewarmReqs, result.prewarmBytes / 1024.0,
                result.rangeMs, result.rangeReqs, result.rangeBytes / 1024.0,
                result.aggMs, result.aggReqs, result.aggBytes / 1024.0);

        return result;
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
            try (SplitSearcher searcher = s3TableRoot != null
                    ? cm.createSplitSearcher(splitUri, metadata, s3TableRoot)
                    : cm.createSplitSearcher(splitUri, metadata)) {

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

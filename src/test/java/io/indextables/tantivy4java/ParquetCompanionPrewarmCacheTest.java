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
import io.indextables.tantivy4java.result.*;
import io.indextables.tantivy4java.split.*;
import io.indextables.tantivy4java.split.merge.*;

import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.*;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Validates that after a full prewarm, all queries (term, range, aggregation)
 * on parquet companion splits produce zero storage downloads — across all three
 * FastFieldMode settings (DISABLED, HYBRID, PARQUET_ONLY).
 *
 * Tests local storage first, then optionally S3 and Azure if credentials are available.
 *
 * Run with: mvn test -pl . -Dtest=ParquetCompanionPrewarmCacheTest
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class ParquetCompanionPrewarmCacheTest {

    private static final int NUM_ROWS = 50;

    // Fields written by nativeWriteTestParquet: id(i64), name(utf8), score(f64), active(bool)
    // Data patterns:
    //   id:     0 .. NUM_ROWS-1
    //   name:   "item_{id}"
    //   score:  id * 1.5 + 10.0
    //   active: id % 2 == 0

    // AWS/Azure credentials (same pattern as PrewarmCacheValidationTest)
    private static String awsAccessKey;
    private static String awsSecretKey;
    private static final String AWS_BUCKET = System.getProperty("test.s3.bucket", "tantivy4java-testing");
    private static final String AWS_REGION = System.getProperty("test.s3.region", "us-east-2");

    private static String azureStorageAccount;
    private static String azureAccountKey;
    private static final String AZURE_CONTAINER = System.getProperty("test.azure.container", "tantivy4java-testing");

    @BeforeAll
    static void loadCredentials() {
        loadAwsCredentials();
        loadAzureCredentials();
    }

    // ════════════════════════════════════════════════════════════════
    //  LOCAL TESTS — one per FastFieldMode
    // ════════════════════════════════════════════════════════════════

    @Test @Order(1)
    @DisplayName("Local DISABLED: prewarm then zero-download queries")
    void localDisabled(@TempDir Path dir) throws Exception {
        runForMode(dir, ParquetCompanionConfig.FastFieldMode.DISABLED, "LOCAL-DISABLED", null);
    }

    @Test @Order(2)
    @DisplayName("Local HYBRID: prewarm then zero-download queries")
    void localHybrid(@TempDir Path dir) throws Exception {
        runForMode(dir, ParquetCompanionConfig.FastFieldMode.HYBRID, "LOCAL-HYBRID", null);
    }

    @Test @Order(3)
    @DisplayName("Local PARQUET_ONLY: prewarm then zero-download queries")
    void localParquetOnly(@TempDir Path dir) throws Exception {
        runForMode(dir, ParquetCompanionConfig.FastFieldMode.PARQUET_ONLY, "LOCAL-PARQUET_ONLY", null);
    }

    // ════════════════════════════════════════════════════════════════
    //  S3 TESTS — split AND parquet files on S3, all 3 modes
    // ════════════════════════════════════════════════════════════════

    @Test @Order(4)
    @DisplayName("S3 DISABLED: split+parquet on S3, prewarm then zero-download queries")
    void s3Disabled(@TempDir Path dir) throws Exception {
        skipIfNoAws();
        runForModeS3(dir, ParquetCompanionConfig.FastFieldMode.DISABLED, "S3-DISABLED");
    }

    @Test @Order(5)
    @DisplayName("S3 HYBRID: split+parquet on S3, prewarm then zero-download queries")
    void s3Hybrid(@TempDir Path dir) throws Exception {
        skipIfNoAws();
        runForModeS3(dir, ParquetCompanionConfig.FastFieldMode.HYBRID, "S3-HYBRID");
    }

    @Test @Order(6)
    @DisplayName("S3 PARQUET_ONLY: split+parquet on S3, prewarm then zero-download queries")
    void s3ParquetOnly(@TempDir Path dir) throws Exception {
        skipIfNoAws();
        runForModeS3(dir, ParquetCompanionConfig.FastFieldMode.PARQUET_ONLY, "S3-PARQUET_ONLY");
    }

    // ════════════════════════════════════════════════════════════════
    //  AZURE TESTS — split AND parquet files on Azure, all 3 modes
    // ════════════════════════════════════════════════════════════════

    @Test @Order(7)
    @DisplayName("Azure DISABLED: split+parquet on Azure, prewarm then zero-download queries")
    void azureDisabled(@TempDir Path dir) throws Exception {
        skipIfNoAzure();
        runForModeAzure(dir, ParquetCompanionConfig.FastFieldMode.DISABLED, "AZURE-DISABLED");
    }

    @Test @Order(8)
    @DisplayName("Azure HYBRID: split+parquet on Azure, prewarm then zero-download queries")
    void azureHybrid(@TempDir Path dir) throws Exception {
        skipIfNoAzure();
        runForModeAzure(dir, ParquetCompanionConfig.FastFieldMode.HYBRID, "AZURE-HYBRID");
    }

    @Test @Order(9)
    @DisplayName("Azure PARQUET_ONLY: split+parquet on Azure, prewarm then zero-download queries")
    void azureParquetOnly(@TempDir Path dir) throws Exception {
        skipIfNoAzure();
        runForModeAzure(dir, ParquetCompanionConfig.FastFieldMode.PARQUET_ONLY, "AZURE-PARQUET_ONLY");
    }

    // ════════════════════════════════════════════════════════════════
    //  S3 / Azure wrappers
    // ════════════════════════════════════════════════════════════════

    private void runForModeS3(Path dir, ParquetCompanionConfig.FastFieldMode mode, String label) throws Exception {
        SplitInfo info = createCompanionSplit(dir, mode, label);
        String ts = String.valueOf(System.currentTimeMillis());
        String s3Prefix = "pq-prewarm-test/" + label + "-" + ts;
        String s3SplitKey = s3Prefix + ".split";
        String splitUri = "s3://" + AWS_BUCKET + "/" + s3SplitKey;

        // Upload split file to S3
        uploadToS3(info.path, s3SplitKey);

        // Upload parquet file(s) to S3 — same relative path structure as local
        String parquetFileName = label + ".parquet";
        String s3ParquetKey = s3Prefix + "/" + parquetFileName;
        uploadToS3(dir.resolve(parquetFileName), s3ParquetKey);

        // The parquet table_root on S3 points to the directory containing parquet files
        String s3TableRoot = "s3://" + AWS_BUCKET + "/" + s3Prefix + "/";

        try {
            Path diskCache = dir.resolve("disk-cache");
            SplitCacheManager.TieredCacheConfig tiered = new SplitCacheManager.TieredCacheConfig()
                    .withDiskCachePath(diskCache.toString())
                    .withMaxDiskSize(100_000_000);

            SplitCacheManager.CacheConfig cfg = new SplitCacheManager.CacheConfig("pq-s3-" + label + "-" + System.nanoTime())
                    .withMaxCacheSize(50_000_000)
                    .withAwsCredentials(getAwsAccessKey(), getAwsSecretKey())
                    .withAwsRegion(AWS_REGION)
                    .withParquetTableRoot(s3TableRoot)
                    .withTieredCache(tiered);

            try (SplitCacheManager cm = SplitCacheManager.getInstance(cfg)) {
                runPrewarmAndValidate(cm, splitUri, info.metadata, mode, label, diskCache);
            }
        } finally {
            deleteFromS3(s3SplitKey);
            deleteFromS3(s3ParquetKey);
        }
    }

    private void runForModeAzure(Path dir, ParquetCompanionConfig.FastFieldMode mode, String label) throws Exception {
        SplitInfo info = createCompanionSplit(dir, mode, label);
        String ts = String.valueOf(System.currentTimeMillis());
        String azPrefix = "pq-prewarm-test/" + label + "-" + ts;
        String azSplitBlob = azPrefix + ".split";
        String splitUri = "azure://" + AZURE_CONTAINER + "/" + azSplitBlob;

        // Upload split file to Azure
        uploadToAzure(info.path, azSplitBlob);

        // Upload parquet file(s) to Azure — same relative path structure as local
        String parquetFileName = label + ".parquet";
        String azParquetBlob = azPrefix + "/" + parquetFileName;
        uploadToAzure(dir.resolve(parquetFileName), azParquetBlob);

        // The parquet table_root on Azure points to the directory containing parquet files
        String azTableRoot = "azure://" + AZURE_CONTAINER + "/" + azPrefix + "/";

        try {
            Path diskCache = dir.resolve("disk-cache");
            SplitCacheManager.TieredCacheConfig tiered = new SplitCacheManager.TieredCacheConfig()
                    .withDiskCachePath(diskCache.toString())
                    .withMaxDiskSize(100_000_000);

            SplitCacheManager.CacheConfig cfg = new SplitCacheManager.CacheConfig("pq-az-" + label + "-" + System.nanoTime())
                    .withMaxCacheSize(50_000_000)
                    .withAzureCredentials(getAzureStorageAccount(), getAzureAccountKey())
                    .withParquetTableRoot(azTableRoot)
                    .withTieredCache(tiered);

            try (SplitCacheManager cm = SplitCacheManager.getInstance(cfg)) {
                runPrewarmAndValidate(cm, splitUri, info.metadata, mode, label, diskCache);
            }
        } finally {
            deleteFromAzure(azSplitBlob);
            deleteFromAzure(azParquetBlob);
        }
    }

    // ════════════════════════════════════════════════════════════════
    //  LOCAL wrapper
    // ════════════════════════════════════════════════════════════════

    private void runForMode(Path dir, ParquetCompanionConfig.FastFieldMode mode,
                            String label, SplitCacheManager.CacheConfig extraCfg) throws Exception {
        SplitInfo info = createCompanionSplit(dir, mode, label);
        String splitUri = "file://" + info.path.toAbsolutePath();

        Path diskCache = dir.resolve("disk-cache");
        SplitCacheManager.TieredCacheConfig tiered = new SplitCacheManager.TieredCacheConfig()
                .withDiskCachePath(diskCache.toString())
                .withMaxDiskSize(100_000_000);

        SplitCacheManager.CacheConfig cfg = new SplitCacheManager.CacheConfig("pq-local-" + label + "-" + System.nanoTime())
                .withMaxCacheSize(50_000_000)
                .withParquetTableRoot(dir.toString())
                .withTieredCache(tiered);

        try (SplitCacheManager cm = SplitCacheManager.getInstance(cfg)) {
            runPrewarmAndValidate(cm, splitUri, info.metadata, mode, label, diskCache);
        }
    }

    // ════════════════════════════════════════════════════════════════
    //  Split creation helper
    // ════════════════════════════════════════════════════════════════

    /** Return value holder for split path + metadata. */
    private static class SplitInfo {
        final Path path;
        final QuickwitSplit.SplitMetadata metadata;
        SplitInfo(Path path, QuickwitSplit.SplitMetadata metadata) {
            this.path = path;
            this.metadata = metadata;
        }
    }

    private SplitInfo createCompanionSplit(Path dir, ParquetCompanionConfig.FastFieldMode mode, String tag) {
        Path parquetFile = dir.resolve(tag + ".parquet");
        Path splitFile = dir.resolve(tag + ".split");

        QuickwitSplit.nativeWriteTestParquet(parquetFile.toString(), NUM_ROWS, 0);

        ParquetCompanionConfig config = new ParquetCompanionConfig(dir.toString())
                .withFastFieldMode(mode)
                .withStatisticsFields("id", "score", "name");

        QuickwitSplit.SplitMetadata metadata = QuickwitSplit.createFromParquet(
                Collections.singletonList(parquetFile.toString()),
                splitFile.toString(), config);

        assertNotNull(metadata);
        assertEquals(NUM_ROWS, metadata.getNumDocs());
        System.out.println("[" + tag + "] Created companion split: " + splitFile.toFile().length() + " bytes, mode=" + mode);
        return new SplitInfo(splitFile, metadata);
    }

    // ════════════════════════════════════════════════════════════════
    //  Core validation: prewarm, then zero-download queries
    // ════════════════════════════════════════════════════════════════

    private void runPrewarmAndValidate(SplitCacheManager cm, String splitUri,
                                       QuickwitSplit.SplitMetadata metadata,
                                       ParquetCompanionConfig.FastFieldMode mode,
                                       String label, Path diskCache) throws Exception {
        System.out.println("\n" + "=".repeat(80));
        System.out.println("  " + label + "  (mode=" + mode + ")");
        System.out.println("=".repeat(80));

        SplitCacheManager.resetStorageDownloadMetrics();

        try (SplitSearcher searcher = cm.createSplitSearcher(splitUri, metadata)) {
            assertTrue(searcher.hasParquetCompanion(), "Should be a parquet companion split");

            // ── PHASE 1: PREWARM ──────────────────────────────────────
            System.out.println("\n[PHASE 1] Prewarming all split components...");
            SplitCacheManager.resetStorageDownloadMetrics();

            searcher.preloadComponents(
                    SplitSearcher.IndexComponent.TERM,
                    SplitSearcher.IndexComponent.POSTINGS,
                    SplitSearcher.IndexComponent.POSITIONS,
                    SplitSearcher.IndexComponent.FASTFIELD,
                    SplitSearcher.IndexComponent.FIELDNORM,
                    SplitSearcher.IndexComponent.STORE
            ).join();

            var afterSplitPrewarm = SplitCacheManager.getStorageDownloadMetrics();
            System.out.println("   Split component prewarm: " + afterSplitPrewarm.getTotalDownloads()
                    + " downloads (" + afterSplitPrewarm.getTotalBytes() + " bytes)");

            // Prewarm parquet fast fields for HYBRID / PARQUET_ONLY modes
            if (mode != ParquetCompanionConfig.FastFieldMode.DISABLED) {
                System.out.println("   Prewarming parquet fast fields (id, score, name, active)...");
                searcher.preloadParquetFastFields("id", "score", "name", "active").join();

                var afterPqPrewarm = SplitCacheManager.getStorageDownloadMetrics();
                System.out.println("   After parquet fast field prewarm: " + afterPqPrewarm.getTotalDownloads()
                        + " total downloads (" + afterPqPrewarm.getTotalBytes() + " bytes)");
            }

            // Also preload parquet columns for doc retrieval
            System.out.println("   Preloading parquet columns for retrieval (id, score, name, active)...");
            searcher.preloadParquetColumns("id", "score", "name", "active").join();

            var afterAllPrewarm = SplitCacheManager.getStorageDownloadMetrics();
            assertTrue(afterAllPrewarm.getTotalDownloads() > 0,
                    "Prewarm should have downloaded data (got " + afterAllPrewarm.getTotalDownloads() + ")");
            System.out.println("   Total prewarm downloads: " + afterAllPrewarm.getTotalDownloads()
                    + " (" + afterAllPrewarm.getTotalBytes() + " bytes)");

            // Wait for async disk cache writes to complete
            for (int attempt = 0; attempt < 20; attempt++) {
                Thread.sleep(200);
                // Check if metrics are stable (no more writes in progress)
                long dl1 = SplitCacheManager.getStorageDownloadMetrics().getTotalBytes();
                Thread.sleep(200);
                long dl2 = SplitCacheManager.getStorageDownloadMetrics().getTotalBytes();
                if (dl1 == dl2) break;
            }

            // ── PHASE 2: RESET & VERIFY ZERO DOWNLOADS ───────────────
            System.out.println("\n[PHASE 2] Resetting metrics — all subsequent operations must produce 0 downloads");
            SplitCacheManager.resetStorageDownloadMetrics();
            assertEquals(0, SplitCacheManager.getStorageDownloadMetrics().getTotalDownloads());

            // ── PHASE 3: TERM QUERIES ─────────────────────────────────
            System.out.println("\n[PHASE 3] Term queries...");
            runTermQueries(searcher);
            assertZeroDownloads("Term queries");

            // ── PHASE 4: RANGE QUERIES ────────────────────────────────
            System.out.println("\n[PHASE 4] Range queries...");
            runRangeQueries(searcher);
            assertZeroDownloads("Range queries");

            // ── PHASE 5: AGGREGATIONS ─────────────────────────────────
            System.out.println("\n[PHASE 5] Aggregations...");
            runAggregations(searcher);
            assertZeroDownloads("Aggregations");

            // ── PHASE 6: COMBINED RANGE + AGGREGATION ─────────────────
            System.out.println("\n[PHASE 6] Combined range query with aggregation...");
            runCombinedRangeAndAggregation(searcher);
            assertZeroDownloads("Combined range + aggregation");

            // ── SUMMARY ───────────────────────────────────────────────
            System.out.println("\n" + "=".repeat(60));
            System.out.println("  PASSED: " + label);
            System.out.println("  All query phases: 0 storage downloads");
            System.out.println("=".repeat(60));
        }
    }

    // ════════════════════════════════════════════════════════════════
    //  Query phases
    // ════════════════════════════════════════════════════════════════

    private void runTermQueries(SplitSearcher searcher) {
        // Match-all
        SearchResult all = searcher.search(new SplitMatchAllQuery(), 100);
        assertEquals(NUM_ROWS, all.getHits().size(), "match-all should return all docs");
        System.out.println("   * returned " + all.getHits().size() + " hits");

        // Term query on name field (raw tokenizer — exact match)
        SplitQuery nameQuery = new SplitTermQuery("name", "item_0");
        SearchResult nameResult = searcher.search(nameQuery, 10);
        assertTrue(nameResult.getHits().size() >= 1, "Should find item_0");
        System.out.println("   name:item_0 => " + nameResult.getHits().size() + " hit(s)");

        // parseQuery on name
        SplitQuery parsedQuery = searcher.parseQuery("name:item_5");
        SearchResult parsedResult = searcher.search(parsedQuery, 10);
        assertTrue(parsedResult.getHits().size() >= 1, "Should find item_5");
        System.out.println("   name:item_5 => " + parsedResult.getHits().size() + " hit(s)");
    }

    private void runRangeQueries(SplitSearcher searcher) {
        // Integer range: id in [5, 10] => 6 docs
        SplitRangeQuery idRange = SplitRangeQuery.inclusiveRange("id", "5", "10", "i64");
        SearchResult idResult = searcher.search(idRange, 100);
        assertEquals(6, idResult.getHits().size(), "id [5..10] should match 6 docs");
        System.out.println("   id [5..10] => " + idResult.getHits().size() + " hits");

        // Float range: score in [10.0, 20.0] inclusive
        // score = id * 1.5 + 10.0, so score=10 at id=0, score=20 at id=6.67 => ids 0..6 => 7 docs
        SplitRangeQuery scoreRange = SplitRangeQuery.inclusiveRange("score", "10.0", "20.0", "f64");
        SearchResult scoreResult = searcher.search(scoreRange, 100);
        assertEquals(7, scoreResult.getHits().size(), "score [10.0..20.0] should match 7 docs");
        System.out.println("   score [10.0..20.0] => " + scoreResult.getHits().size() + " hits");

        // Exclusive upper bound: id in [0, 5) => 5 docs
        SplitRangeQuery exclusiveRange = new SplitRangeQuery("id",
                SplitRangeQuery.RangeBound.inclusive("0"),
                SplitRangeQuery.RangeBound.exclusive("5"), "i64");
        SearchResult exclusiveResult = searcher.search(exclusiveRange, 100);
        assertEquals(5, exclusiveResult.getHits().size(), "id [0..5) should match 5 docs");
        System.out.println("   id [0..5) => " + exclusiveResult.getHits().size() + " hits");

        // Unbounded upper: id >= 45 => 5 docs (45,46,47,48,49)
        SplitRangeQuery unboundedRange = new SplitRangeQuery("id",
                SplitRangeQuery.RangeBound.inclusive("45"),
                SplitRangeQuery.RangeBound.unbounded(), "i64");
        SearchResult unboundedResult = searcher.search(unboundedRange, 100);
        assertEquals(5, unboundedResult.getHits().size(), "id >= 45 should match 5 docs");
        System.out.println("   id >= 45 => " + unboundedResult.getHits().size() + " hits");
    }

    private void runAggregations(SplitSearcher searcher) {
        // Stats on id field
        StatsAggregation idStats = new StatsAggregation("id_stats", "id");
        SearchResult idResult = searcher.search(new SplitMatchAllQuery(), 0, "id_stats", idStats);
        assertTrue(idResult.hasAggregations(), "Should have aggregation results");
        StatsResult idSr = (StatsResult) idResult.getAggregation("id_stats");
        assertNotNull(idSr);
        assertEquals(NUM_ROWS, idSr.getCount());
        assertEquals(0.0, idSr.getMin(), 0.01);
        assertEquals(NUM_ROWS - 1.0, idSr.getMax(), 0.01);
        System.out.println("   stats(id): count=" + idSr.getCount() + " min=" + idSr.getMin()
                + " max=" + idSr.getMax() + " avg=" + idSr.getAverage());

        // Stats on score field (parquet-sourced in HYBRID text, native in DISABLED)
        StatsAggregation scoreStats = new StatsAggregation("score_stats", "score");
        SearchResult scoreResult = searcher.search(new SplitMatchAllQuery(), 0, "score_stats", scoreStats);
        StatsResult scoreSr = (StatsResult) scoreResult.getAggregation("score_stats");
        assertNotNull(scoreSr);
        assertEquals(NUM_ROWS, scoreSr.getCount());
        assertEquals(10.0, scoreSr.getMin(), 0.01);   // id=0 => 0*1.5+10=10
        double maxScore = (NUM_ROWS - 1) * 1.5 + 10.0;
        assertEquals(maxScore, scoreSr.getMax(), 0.01);
        System.out.println("   stats(score): count=" + scoreSr.getCount() + " min=" + scoreSr.getMin()
                + " max=" + scoreSr.getMax() + " avg=" + scoreSr.getAverage());

        // Histogram on id field with interval=10
        HistogramAggregation idHist = new HistogramAggregation("id_hist", "id", 10.0);
        SearchResult histResult = searcher.search(new SplitMatchAllQuery(), 0, "id_hist", idHist);
        assertTrue(histResult.hasAggregations(), "Should have histogram results");
        System.out.println("   histogram(id, interval=10): returned aggregation");

        // Terms on name field (text fast field — raw tokenizer)
        TermsAggregation nameTerms = new TermsAggregation("name_terms", "name", 10, 0);
        SearchResult termsResult = searcher.search(new SplitMatchAllQuery(), 0, "name_terms", nameTerms);
        assertTrue(termsResult.hasAggregations(), "Should have terms results");
        System.out.println("   terms(name, size=10): returned aggregation");

        // Multi-aggregation: stats on id + histogram on score in one call
        Map<String, SplitAggregation> multiAggs = new LinkedHashMap<>();
        multiAggs.put("multi_id_stats", new StatsAggregation("multi_id_stats", "id"));
        multiAggs.put("multi_score_hist", new HistogramAggregation("multi_score_hist", "score", 10.0));
        SearchResult multiResult = searcher.search(new SplitMatchAllQuery(), 0, multiAggs);
        assertTrue(multiResult.hasAggregations(), "Multi-agg should have results");
        assertNotNull(multiResult.getAggregation("multi_id_stats"));
        assertNotNull(multiResult.getAggregation("multi_score_hist"));
        System.out.println("   multi-agg(stats+histogram): both returned");
    }

    private void runCombinedRangeAndAggregation(SplitSearcher searcher) {
        // Range query [10..30] combined with stats aggregation on score
        SplitRangeQuery range = SplitRangeQuery.inclusiveRange("id", "10", "30", "i64");
        StatsAggregation scoreStats = new StatsAggregation("filtered_score", "score");
        SearchResult result = searcher.search(range, 0, "filtered_score", scoreStats);

        assertTrue(result.hasAggregations());
        StatsResult sr = (StatsResult) result.getAggregation("filtered_score");
        assertNotNull(sr);
        // ids 10..30 = 21 docs; score = id*1.5+10 => min=25.0 (id=10), max=55.0 (id=30)
        assertEquals(21, sr.getCount());
        assertEquals(25.0, sr.getMin(), 0.01);
        assertEquals(55.0, sr.getMax(), 0.01);
        System.out.println("   range(id [10..30]) + stats(score): count=" + sr.getCount()
                + " min=" + sr.getMin() + " max=" + sr.getMax());
    }

    // ════════════════════════════════════════════════════════════════
    //  Assertion helper
    // ════════════════════════════════════════════════════════════════

    private void assertZeroDownloads(String phase) {
        var metrics = SplitCacheManager.getStorageDownloadMetrics();
        assertEquals(0, metrics.getTotalDownloads(),
                phase + " should have 0 storage downloads but got " + metrics.getTotalDownloads()
                        + " (" + metrics.getTotalBytes() + " bytes)");
        System.out.println("   => 0 downloads (cache hit)");
    }

    // ════════════════════════════════════════════════════════════════
    //  Credential helpers (same as PrewarmCacheValidationTest)
    // ════════════════════════════════════════════════════════════════

    private void skipIfNoAws() {
        if (getAwsAccessKey() == null || getAwsSecretKey() == null) {
            Assumptions.abort("AWS credentials not available");
        }
    }

    private void skipIfNoAzure() {
        if (getAzureStorageAccount() == null || getAzureAccountKey() == null) {
            Assumptions.abort("Azure credentials not available");
        }
    }

    private static String getAwsAccessKey() {
        String v = System.getenv("AWS_ACCESS_KEY_ID");
        if (v != null) return v;
        v = System.getProperty("test.s3.accessKey");
        if (v != null) return v;
        return awsAccessKey;
    }

    private static String getAwsSecretKey() {
        String v = System.getenv("AWS_SECRET_ACCESS_KEY");
        if (v != null) return v;
        v = System.getProperty("test.s3.secretKey");
        if (v != null) return v;
        return awsSecretKey;
    }

    private static String getAzureStorageAccount() {
        String v = System.getenv("AZURE_STORAGE_ACCOUNT");
        if (v != null) return v;
        v = System.getProperty("test.azure.storageAccount");
        if (v != null) return v;
        return azureStorageAccount;
    }

    private static String getAzureAccountKey() {
        String v = System.getenv("AZURE_STORAGE_KEY");
        if (v != null) return v;
        v = System.getProperty("test.azure.accountKey");
        if (v != null) return v;
        return azureAccountKey;
    }

    private static void loadAwsCredentials() {
        try {
            Path p = Paths.get(System.getProperty("user.home"), ".aws", "credentials");
            if (!Files.exists(p)) return;
            boolean inDefault = false;
            for (String line : Files.readAllLines(p)) {
                line = line.trim();
                if ("[default]".equals(line)) { inDefault = true; continue; }
                if (line.startsWith("[")) { inDefault = false; continue; }
                if (inDefault && line.contains("=")) {
                    String[] kv = line.split("=", 2);
                    if (kv.length == 2) {
                        if ("aws_access_key_id".equals(kv[0].trim())) awsAccessKey = kv[1].trim();
                        else if ("aws_secret_access_key".equals(kv[0].trim())) awsSecretKey = kv[1].trim();
                    }
                }
            }
        } catch (Exception e) {
            System.out.println("Warning: failed to load AWS credentials: " + e.getMessage());
        }
    }

    private static void loadAzureCredentials() {
        try {
            Path p = Paths.get(System.getProperty("user.home"), ".azure", "credentials");
            if (!Files.exists(p)) return;
            boolean inDefault = false;
            for (String line : Files.readAllLines(p)) {
                line = line.trim();
                if ("[default]".equals(line)) { inDefault = true; continue; }
                if (line.startsWith("[")) { inDefault = false; continue; }
                if (inDefault && line.contains("=")) {
                    String[] kv = line.split("=", 2);
                    if (kv.length == 2) {
                        if ("storage_account".equals(kv[0].trim())) azureStorageAccount = kv[1].trim();
                        else if ("account_key".equals(kv[0].trim())) azureAccountKey = kv[1].trim();
                    }
                }
            }
        } catch (Exception e) {
            System.out.println("Warning: failed to load Azure credentials: " + e.getMessage());
        }
    }

    // ════════════════════════════════════════════════════════════════
    //  S3 upload / delete
    // ════════════════════════════════════════════════════════════════

    private void uploadToS3(Path local, String key) {
        System.out.println("Uploading to s3://" + AWS_BUCKET + "/" + key);
        try {
            var s3 = software.amazon.awssdk.services.s3.S3Client.builder()
                    .region(software.amazon.awssdk.regions.Region.of(AWS_REGION))
                    .credentialsProvider(software.amazon.awssdk.auth.credentials.StaticCredentialsProvider.create(
                            software.amazon.awssdk.auth.credentials.AwsBasicCredentials.create(getAwsAccessKey(), getAwsSecretKey())))
                    .build();
            s3.putObject(software.amazon.awssdk.services.s3.model.PutObjectRequest.builder()
                    .bucket(AWS_BUCKET).key(key).build(), local);
            s3.close();
        } catch (Exception e) {
            throw new RuntimeException("S3 upload failed", e);
        }
    }

    private void deleteFromS3(String key) {
        try {
            var s3 = software.amazon.awssdk.services.s3.S3Client.builder()
                    .region(software.amazon.awssdk.regions.Region.of(AWS_REGION))
                    .credentialsProvider(software.amazon.awssdk.auth.credentials.StaticCredentialsProvider.create(
                            software.amazon.awssdk.auth.credentials.AwsBasicCredentials.create(getAwsAccessKey(), getAwsSecretKey())))
                    .build();
            s3.deleteObject(software.amazon.awssdk.services.s3.model.DeleteObjectRequest.builder()
                    .bucket(AWS_BUCKET).key(key).build());
            s3.close();
        } catch (Exception e) {
            System.out.println("Warning: S3 cleanup failed: " + e.getMessage());
        }
    }

    // ════════════════════════════════════════════════════════════════
    //  Azure upload / delete
    // ════════════════════════════════════════════════════════════════

    private void uploadToAzure(Path local, String blobName) {
        System.out.println("Uploading to azure://" + AZURE_CONTAINER + "/" + blobName);
        try {
            var client = new com.azure.storage.blob.BlobServiceClientBuilder()
                    .endpoint("https://" + getAzureStorageAccount() + ".blob.core.windows.net")
                    .credential(new com.azure.storage.common.StorageSharedKeyCredential(getAzureStorageAccount(), getAzureAccountKey()))
                    .buildClient();
            var container = client.getBlobContainerClient(AZURE_CONTAINER);
            if (!container.exists()) container.create();
            container.getBlobClient(blobName).uploadFromFile(local.toString(), true);
        } catch (Exception e) {
            throw new RuntimeException("Azure upload failed", e);
        }
    }

    private void deleteFromAzure(String blobName) {
        try {
            var client = new com.azure.storage.blob.BlobServiceClientBuilder()
                    .endpoint("https://" + getAzureStorageAccount() + ".blob.core.windows.net")
                    .credential(new com.azure.storage.common.StorageSharedKeyCredential(getAzureStorageAccount(), getAzureAccountKey()))
                    .buildClient();
            client.getBlobContainerClient(AZURE_CONTAINER).getBlobClient(blobName).delete();
        } catch (Exception e) {
            System.out.println("Warning: Azure cleanup failed: " + e.getMessage());
        }
    }
}

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

import io.indextables.tantivy4java.core.DocAddress;
import io.indextables.tantivy4java.core.Document;
import io.indextables.tantivy4java.result.*;
import io.indextables.tantivy4java.split.*;
import io.indextables.tantivy4java.split.merge.*;
import io.indextables.tantivy4java.aggregation.*;

import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.*;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Reproduces three suspected gaps in the prewarm primitives for parquet companion splits.
 *
 * Each test follows the same pattern:
 *   1. Create a companion split with a specific configuration
 *   2. Prewarm using the relevant API
 *   3. Reset download metrics
 *   4. Execute an operation that needs the data
 *   5. Assert zero downloads (if prewarm was complete, cache should serve everything)
 *
 * If the assertion fails, we've confirmed the gap — prewarm didn't cache what the
 * query path subsequently needed.
 *
 * Run with: mvn test -pl . -Dtest=PrewarmGapReproTest
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class PrewarmGapReproTest {

    private static final int NUM_ROWS = 30;

    // ════════════════════════════════════════════════════════════════
    //  GAP 1: _pq fields not prewarmed by ANY warming operation
    //
    //  The __pq_file_hash and __pq_row_in_file fast fields are used by
    //  doc retrieval (ensure_pq_segment_loaded). They should be cached
    //  after ANY prewarm operation on a companion split — not just
    //  FASTFIELD. Even a TERM-only prewarm should trigger _pq caching
    //  because the caller will inevitably need doc retrieval next.
    //
    //  Sub-test a: TERM-only prewarm (minimal — not even FASTFIELD)
    //  Sub-test b: Full prewarm (all components)
    // ════════════════════════════════════════════════════════════════

    @Test
    @Order(1)
    @DisplayName("GAP 1a: _pq fields should be cached after TERM-only prewarm")
    void gap1a_pqFieldsNotPrewarmedByTermOnly(@TempDir Path dir) throws Exception {
        gap1_helper(dir, "gap1a",
                new SplitSearcher.IndexComponent[] { SplitSearcher.IndexComponent.TERM },
                "TERM-only");
    }

    @Test
    @Order(2)
    @DisplayName("GAP 1b: _pq fields should be cached after full component prewarm")
    void gap1b_pqFieldsNotPrewarmedByFullPrewarm(@TempDir Path dir) throws Exception {
        gap1_helper(dir, "gap1b",
                new SplitSearcher.IndexComponent[] {
                        SplitSearcher.IndexComponent.TERM,
                        SplitSearcher.IndexComponent.POSTINGS,
                        SplitSearcher.IndexComponent.FASTFIELD,
                        SplitSearcher.IndexComponent.FIELDNORM,
                        SplitSearcher.IndexComponent.STORE
                },
                "full");
    }

    private void gap1_helper(Path dir, String tag, SplitSearcher.IndexComponent[] components,
                             String label) throws Exception {
        Path parquetFile = dir.resolve(tag + ".parquet");
        Path splitFile = dir.resolve(tag + ".split");

        QuickwitSplit.nativeWriteTestParquet(parquetFile.toString(), NUM_ROWS, 0);

        ParquetCompanionConfig config = new ParquetCompanionConfig(dir.toString())
                .withFastFieldMode(ParquetCompanionConfig.FastFieldMode.HYBRID);

        QuickwitSplit.SplitMetadata metadata = QuickwitSplit.createFromParquet(
                Collections.singletonList(parquetFile.toString()),
                splitFile.toString(), config);

        assertEquals(NUM_ROWS, metadata.getNumDocs());

        Path diskCache = dir.resolve("disk-cache");
        SplitCacheManager.TieredCacheConfig tiered = new SplitCacheManager.TieredCacheConfig()
                .withDiskCachePath(diskCache.toString())
                .withMaxDiskSize(100_000_000);

        SplitCacheManager.CacheConfig cacheConfig =
                new SplitCacheManager.CacheConfig(tag + "-" + System.nanoTime())
                        .withMaxCacheSize(50_000_000)
                        .withParquetTableRoot(dir.toString())
                        .withTieredCache(tiered);

        try (SplitCacheManager cm = SplitCacheManager.getInstance(cacheConfig)) {
            String splitUri = "file://" + splitFile.toAbsolutePath();
            try (SplitSearcher searcher = cm.createSplitSearcher(splitUri, metadata)) {
                assertTrue(searcher.hasParquetCompanion());

                // PREWARM: only the specified components
                SplitCacheManager.resetStorageDownloadMetrics();
                searcher.preloadComponents(components).join();
                // Also prewarm parquet columns for doc retrieval
                searcher.preloadParquetColumns("id", "name", "score", "active").join();

                Thread.sleep(3000);

                var afterPrewarm = SplitCacheManager.getStorageDownloadMetrics();
                System.out.println("[GAP 1 " + label + "] Prewarm downloads: "
                        + afterPrewarm.getTotalDownloads()
                        + " (" + afterPrewarm.getTotalBytes() + " bytes)");
                assertTrue(afterPrewarm.getTotalDownloads() > 0, "Prewarm should download something");

                Thread.sleep(500);

                // RESET — everything after this should be zero downloads
                SplitCacheManager.resetStorageDownloadMetrics();

                // DOC RETRIEVAL — triggers ensure_pq_segment_loaded() for _pq fields
                SplitQuery query = searcher.parseQuery("*");
                SearchResult results = searcher.search(query, 5);
                assertTrue(results.getHits().size() >= 1);

                DocAddress addr = results.getHits().get(0).getDocAddress();
                Document doc = searcher.docProjected(addr, "id", "name");
                assertNotNull(doc.getFirst("id"), "Should retrieve id field");

                var afterDocRetrieval = SplitCacheManager.getStorageDownloadMetrics();
                System.out.println("[GAP 1 " + label + "] Post-prewarm doc retrieval downloads: "
                        + afterDocRetrieval.getTotalDownloads()
                        + " (" + afterDocRetrieval.getTotalBytes() + " bytes)");

                assertEquals(0, afterDocRetrieval.getTotalDownloads(),
                        "GAP 1 (" + label + "): _pq field reads caused "
                                + afterDocRetrieval.getTotalDownloads() + " downloads ("
                                + afterDocRetrieval.getTotalBytes() + " bytes) after "
                                + label + " prewarm. ensure_pq_segment_loaded() reads "
                                + "__pq_file_hash/__pq_row_in_file via a path that bypasses "
                                + "the prewarm cache.");
            }
        }
    }

    // ════════════════════════════════════════════════════════════════
    //  GAP 2: exact_only hash fields not prewarmed with parent field
    //
    //  When a string field uses exact_only mode, the field IS a U64
    //  hash field. Prewarming FASTFIELD and TERM for the parent field
    //  name should include this hash field's data, since it's a native
    //  tantivy field. If the hash field is stored under a different
    //  name in the index, field-specific prewarm might miss it.
    // ════════════════════════════════════════════════════════════════

    @Test
    @Order(3)
    @DisplayName("GAP 2a: exact_only hash fields prewarmed with parent field (all components)")
    void gap2a_hashFieldsNotPrewarmed(@TempDir Path dir) throws Exception {
        Path parquetFile = dir.resolve("gap2.parquet");
        Path splitFile = dir.resolve("gap2.split");

        // Use the string indexing test parquet: id, trace_id, message, error_log, category
        QuickwitSplit.nativeWriteTestParquetForStringIndexing(parquetFile.toString(), NUM_ROWS, 0);

        Map<String, String> tokenizers = new HashMap<>();
        tokenizers.put("trace_id", ParquetCompanionConfig.StringIndexingMode.EXACT_ONLY);

        ParquetCompanionConfig config = new ParquetCompanionConfig(dir.toString())
                .withFastFieldMode(ParquetCompanionConfig.FastFieldMode.HYBRID)
                .withTokenizerOverrides(tokenizers);

        QuickwitSplit.SplitMetadata metadata = QuickwitSplit.createFromParquet(
                Collections.singletonList(parquetFile.toString()),
                splitFile.toString(), config);

        assertEquals(NUM_ROWS, metadata.getNumDocs());

        Path diskCache = dir.resolve("disk-cache");
        SplitCacheManager.TieredCacheConfig tiered = new SplitCacheManager.TieredCacheConfig()
                .withDiskCachePath(diskCache.toString())
                .withMaxDiskSize(100_000_000);

        SplitCacheManager.CacheConfig cacheConfig =
                new SplitCacheManager.CacheConfig("gap2-" + System.nanoTime())
                        .withMaxCacheSize(50_000_000)
                        .withParquetTableRoot(dir.toString())
                        .withTieredCache(tiered);

        try (SplitCacheManager cm = SplitCacheManager.getInstance(cacheConfig)) {
            String splitUri = "file://" + splitFile.toAbsolutePath();
            try (SplitSearcher searcher = cm.createSplitSearcher(splitUri, metadata)) {
                assertTrue(searcher.hasParquetCompanion());

                // PREWARM: all components (FASTFIELD includes trace_id which IS the hash field)
                // plus TERM for term dictionary lookups
                SplitCacheManager.resetStorageDownloadMetrics();
                searcher.preloadComponents(
                        SplitSearcher.IndexComponent.TERM,
                        SplitSearcher.IndexComponent.POSTINGS,
                        SplitSearcher.IndexComponent.FASTFIELD,
                        SplitSearcher.IndexComponent.FIELDNORM,
                        SplitSearcher.IndexComponent.STORE
                ).join();

                // Also prewarm parquet fast fields for the string fields that need transcoding
                searcher.preloadParquetFastFields("message", "error_log", "category").join();

                Thread.sleep(3000);

                var afterPrewarm = SplitCacheManager.getStorageDownloadMetrics();
                System.out.println("[GAP 2] Prewarm downloads: " + afterPrewarm.getTotalDownloads()
                        + " (" + afterPrewarm.getTotalBytes() + " bytes)");

                Thread.sleep(500);

                // RESET
                SplitCacheManager.resetStorageDownloadMetrics();

                // QUERY: terms aggregation on trace_id (which is the hash U64 field)
                // This requires the FASTFIELD data for trace_id to be cached
                StatsAggregation traceStats = new StatsAggregation("trace_stats", "trace_id");
                SearchResult aggResult = searcher.search(
                        new SplitMatchAllQuery(), 0, "trace_stats", traceStats);
                assertTrue(aggResult.hasAggregations(), "Should have aggregation results");
                StatsResult sr = (StatsResult) aggResult.getAggregation("trace_stats");
                assertNotNull(sr);
                assertEquals(NUM_ROWS, sr.getCount(),
                        "Stats on exact_only trace_id should count all docs");

                var afterQuery = SplitCacheManager.getStorageDownloadMetrics();
                System.out.println("[GAP 2] Post-prewarm query downloads: "
                        + afterQuery.getTotalDownloads()
                        + " (" + afterQuery.getTotalBytes() + " bytes)");

                assertEquals(0, afterQuery.getTotalDownloads(),
                        "GAP 2 CONFIRMED: exact_only hash field (trace_id) query caused "
                                + afterQuery.getTotalDownloads() + " downloads ("
                                + afterQuery.getTotalBytes() + " bytes) after full prewarm. "
                                + "The _phash_* / exact_only field data was not cached by "
                                + "preloadComponents(FASTFIELD).");
            }
        }
    }

    // ════════════════════════════════════════════════════════════════
    //  GAP 2b: text_uuid_exactonly companion field not prewarmed when
    //          parent field is prewarmed with field-specific prewarm
    //
    //  When "message" uses text_uuid_exactonly, a companion U64 field
    //  "message__uuids" is created for UUID hash lookups. Field-specific
    //  prewarm on "message" should automatically include "message__uuids"
    //  for FASTFIELD, TERM, and POSTINGS — because any query hitting
    //  "message" with a UUID value gets rewritten to target the companion.
    // ════════════════════════════════════════════════════════════════

    @Test
    @Order(4)
    @DisplayName("GAP 2b: companion __uuids field should prewarm with parent field")
    void gap2b_companionFieldNotPrewarmedWithParent(@TempDir Path dir) throws Exception {
        Path parquetFile = dir.resolve("gap2b.parquet");
        Path splitFile = dir.resolve("gap2b.split");

        QuickwitSplit.nativeWriteTestParquetForStringIndexing(parquetFile.toString(), NUM_ROWS, 0);

        Map<String, String> tokenizers = new HashMap<>();
        tokenizers.put("message", ParquetCompanionConfig.StringIndexingMode.TEXT_UUID_EXACTONLY);

        ParquetCompanionConfig config = new ParquetCompanionConfig(dir.toString())
                .withFastFieldMode(ParquetCompanionConfig.FastFieldMode.HYBRID)
                .withTokenizerOverrides(tokenizers);

        QuickwitSplit.SplitMetadata metadata = QuickwitSplit.createFromParquet(
                Collections.singletonList(parquetFile.toString()),
                splitFile.toString(), config);

        assertEquals(NUM_ROWS, metadata.getNumDocs());

        Path diskCache = dir.resolve("disk-cache");
        SplitCacheManager.TieredCacheConfig tiered = new SplitCacheManager.TieredCacheConfig()
                .withDiskCachePath(diskCache.toString())
                .withMaxDiskSize(100_000_000);

        SplitCacheManager.CacheConfig cacheConfig =
                new SplitCacheManager.CacheConfig("gap2b-" + System.nanoTime())
                        .withMaxCacheSize(50_000_000)
                        .withParquetTableRoot(dir.toString())
                        .withTieredCache(tiered);

        try (SplitCacheManager cm = SplitCacheManager.getInstance(cacheConfig)) {
            String splitUri = "file://" + splitFile.toAbsolutePath();
            try (SplitSearcher searcher = cm.createSplitSearcher(splitUri, metadata)) {
                assertTrue(searcher.hasParquetCompanion());

                // PREWARM: field-specific on "message" only — should also pull in
                // "message__uuids" for all three component types
                SplitCacheManager.resetStorageDownloadMetrics();
                searcher.preloadFields(SplitSearcher.IndexComponent.FASTFIELD, "message").join();
                searcher.preloadFields(SplitSearcher.IndexComponent.TERM, "message").join();
                searcher.preloadFields(SplitSearcher.IndexComponent.POSTINGS, "message").join();

                Thread.sleep(3000);

                var afterPrewarm = SplitCacheManager.getStorageDownloadMetrics();
                System.out.println("[GAP 2b] Field-specific prewarm for 'message' downloads: "
                        + afterPrewarm.getTotalDownloads()
                        + " (" + afterPrewarm.getTotalBytes() + " bytes)");

                Thread.sleep(500);

                // RESET
                SplitCacheManager.resetStorageDownloadMetrics();

                // QUERY: stats aggregation on the companion field "message__uuids"
                // Needs FASTFIELD data for message__uuids
                StatsAggregation companionStats = new StatsAggregation("companion_stats", "message__uuids");
                SearchResult aggResult = searcher.search(
                        new SplitMatchAllQuery(), 0, "companion_stats", companionStats);
                assertTrue(aggResult.hasAggregations(), "Should have aggregation results");
                StatsResult sr = (StatsResult) aggResult.getAggregation("companion_stats");
                assertNotNull(sr);
                assertEquals(NUM_ROWS, sr.getCount(),
                        "Stats on message__uuids should count all docs");

                var afterAgg = SplitCacheManager.getStorageDownloadMetrics();
                System.out.println("[GAP 2b] Post-prewarm companion agg downloads: "
                        + afterAgg.getTotalDownloads()
                        + " (" + afterAgg.getTotalBytes() + " bytes)");

                assertEquals(0, afterAgg.getTotalDownloads(),
                        "GAP 2b CONFIRMED (FASTFIELD): preloadFields(FASTFIELD, \"message\") "
                                + "did not cache companion field \"message__uuids\". "
                                + "Aggregation caused " + afterAgg.getTotalDownloads()
                                + " downloads (" + afterAgg.getTotalBytes() + " bytes). "
                                + "Field-specific FASTFIELD prewarm should automatically include "
                                + "companion __uuids fields.");
            }
        }
    }

    // ════════════════════════════════════════════════════════════════
    //  GAP 3: preloadParquetColumns doesn't actually cache column data
    //
    //  preloadParquetColumns() reads column chunks via storage.get_slice()
    //  but the data may not go through the same cache layer that doc
    //  retrieval uses (ByteRangeCache for dictionary pages, etc.).
    //  If the prewarm and retrieval use different cache keys or layers,
    //  the first doc retrieval after prewarm will re-download.
    // ════════════════════════════════════════════════════════════════

    @Test
    @Order(5)
    @DisplayName("GAP 3: preloadParquetColumns should eliminate doc retrieval downloads")
    void gap3_parquetColumnPrewarmIneffective(@TempDir Path dir) throws Exception {
        Path parquetFile = dir.resolve("gap3.parquet");
        Path splitFile = dir.resolve("gap3.split");

        QuickwitSplit.nativeWriteTestParquet(parquetFile.toString(), NUM_ROWS, 0);

        ParquetCompanionConfig config = new ParquetCompanionConfig(dir.toString())
                .withFastFieldMode(ParquetCompanionConfig.FastFieldMode.HYBRID);

        QuickwitSplit.SplitMetadata metadata = QuickwitSplit.createFromParquet(
                Collections.singletonList(parquetFile.toString()),
                splitFile.toString(), config);

        assertEquals(NUM_ROWS, metadata.getNumDocs());

        Path diskCache = dir.resolve("disk-cache");
        SplitCacheManager.TieredCacheConfig tiered = new SplitCacheManager.TieredCacheConfig()
                .withDiskCachePath(diskCache.toString())
                .withMaxDiskSize(100_000_000);

        SplitCacheManager.CacheConfig cacheConfig =
                new SplitCacheManager.CacheConfig("gap3-" + System.nanoTime())
                        .withMaxCacheSize(50_000_000)
                        .withParquetTableRoot(dir.toString())
                        .withTieredCache(tiered);

        try (SplitCacheManager cm = SplitCacheManager.getInstance(cacheConfig)) {
            String splitUri = "file://" + splitFile.toAbsolutePath();
            try (SplitSearcher searcher = cm.createSplitSearcher(splitUri, metadata)) {
                assertTrue(searcher.hasParquetCompanion());

                // PREWARM: all split components first (for search + _pq field resolution)
                SplitCacheManager.resetStorageDownloadMetrics();
                searcher.preloadComponents(
                        SplitSearcher.IndexComponent.TERM,
                        SplitSearcher.IndexComponent.POSTINGS,
                        SplitSearcher.IndexComponent.FASTFIELD,
                        SplitSearcher.IndexComponent.FIELDNORM,
                        SplitSearcher.IndexComponent.STORE
                ).join();

                Thread.sleep(3000);

                var afterSplitPrewarm = SplitCacheManager.getStorageDownloadMetrics();
                System.out.println("[GAP 3] Split prewarm downloads: "
                        + afterSplitPrewarm.getTotalDownloads()
                        + " (" + afterSplitPrewarm.getTotalBytes() + " bytes)");

                // PREWARM: parquet columns specifically for doc retrieval
                SplitCacheManager.resetStorageDownloadMetrics();
                searcher.preloadParquetColumns("id", "name", "score", "active").join();

                Thread.sleep(3000);

                var afterPqPrewarm = SplitCacheManager.getStorageDownloadMetrics();
                System.out.println("[GAP 3] Parquet column prewarm downloads: "
                        + afterPqPrewarm.getTotalDownloads()
                        + " (" + afterPqPrewarm.getTotalBytes() + " bytes)");

                // Check if parquet column prewarm actually downloaded anything
                // If it downloaded 0 bytes, that alone confirms the gap
                if (afterPqPrewarm.getTotalDownloads() == 0) {
                    System.out.println("[GAP 3] WARNING: preloadParquetColumns downloaded NOTHING — "
                            + "the prewarm call is a no-op or the storage path is wrong.");
                }

                Thread.sleep(500);

                // RESET
                SplitCacheManager.resetStorageDownloadMetrics();

                // DOC RETRIEVAL: single doc + batch
                SplitQuery query = searcher.parseQuery("*");
                SearchResult results = searcher.search(query, 5);
                assertTrue(results.getHits().size() >= 3);

                // Single doc retrieval with projection
                Document singleDoc = searcher.docProjected(
                        results.getHits().get(0).getDocAddress(), "id", "name");
                assertNotNull(singleDoc.getFirst("id"));

                // Batch doc retrieval
                DocAddress[] addrs = new DocAddress[3];
                for (int i = 0; i < 3; i++) {
                    addrs[i] = results.getHits().get(i).getDocAddress();
                }
                List<Document> batchDocs = searcher.docBatchProjected(addrs, "id", "name");
                assertEquals(3, batchDocs.size());

                var afterRetrieval = SplitCacheManager.getStorageDownloadMetrics();
                System.out.println("[GAP 3] Post-prewarm doc retrieval downloads: "
                        + afterRetrieval.getTotalDownloads()
                        + " (" + afterRetrieval.getTotalBytes() + " bytes)");

                assertEquals(0, afterRetrieval.getTotalDownloads(),
                        "GAP 3 CONFIRMED: doc retrieval caused " + afterRetrieval.getTotalDownloads()
                                + " downloads (" + afterRetrieval.getTotalBytes()
                                + " bytes) after preloadParquetColumns(). "
                                + "The parquet column prewarm either didn't download data or used "
                                + "a different cache path than doc retrieval.");
            }
        }
    }

    // ════════════════════════════════════════════════════════════════
    //  GAP 4: preloadParquetFastFields should eliminate aggregation downloads
    //
    //  preloadParquetFastFields() transcodes parquet columns into tantivy
    //  columnar format and caches them in the ParquetAugmentedDirectory.
    //  After prewarm, aggregations on those fields should be served
    //  entirely from the transcoded cache — zero storage downloads.
    // ════════════════════════════════════════════════════════════════

    @Test
    @Order(6)
    @DisplayName("GAP 4: preloadParquetFastFields should eliminate aggregation downloads")
    void gap4_parquetFastFieldPrewarmEliminatesAggDownloads(@TempDir Path dir) throws Exception {
        Path parquetFile = dir.resolve("gap4.parquet");
        Path splitFile = dir.resolve("gap4.split");

        QuickwitSplit.nativeWriteTestParquet(parquetFile.toString(), NUM_ROWS, 0);

        // Use PARQUET_ONLY mode so ALL fast field reads go through parquet transcoding
        // (in HYBRID mode, numeric fields are native fast fields and don't need transcoding)
        ParquetCompanionConfig config = new ParquetCompanionConfig(dir.toString())
                .withFastFieldMode(ParquetCompanionConfig.FastFieldMode.PARQUET_ONLY);

        QuickwitSplit.SplitMetadata metadata = QuickwitSplit.createFromParquet(
                Collections.singletonList(parquetFile.toString()),
                splitFile.toString(), config);

        assertEquals(NUM_ROWS, metadata.getNumDocs());

        Path diskCache = dir.resolve("disk-cache");
        SplitCacheManager.TieredCacheConfig tiered = new SplitCacheManager.TieredCacheConfig()
                .withDiskCachePath(diskCache.toString())
                .withMaxDiskSize(100_000_000);

        SplitCacheManager.CacheConfig cacheConfig =
                new SplitCacheManager.CacheConfig("gap4-" + System.nanoTime())
                        .withMaxCacheSize(50_000_000)
                        .withParquetTableRoot(dir.toString())
                        .withTieredCache(tiered);

        try (SplitCacheManager cm = SplitCacheManager.getInstance(cacheConfig)) {
            String splitUri = "file://" + splitFile.toAbsolutePath();
            try (SplitSearcher searcher = cm.createSplitSearcher(splitUri, metadata)) {
                assertTrue(searcher.hasParquetCompanion());

                // PREWARM: split components first (for search infrastructure)
                SplitCacheManager.resetStorageDownloadMetrics();
                searcher.preloadComponents(
                        SplitSearcher.IndexComponent.TERM,
                        SplitSearcher.IndexComponent.POSTINGS,
                        SplitSearcher.IndexComponent.FASTFIELD,
                        SplitSearcher.IndexComponent.FIELDNORM,
                        SplitSearcher.IndexComponent.STORE
                ).join();

                Thread.sleep(3000);

                var afterSplitPrewarm = SplitCacheManager.getStorageDownloadMetrics();
                System.out.println("[GAP 4] Split prewarm downloads: "
                        + afterSplitPrewarm.getTotalDownloads()
                        + " (" + afterSplitPrewarm.getTotalBytes() + " bytes)");

                // PREWARM: parquet fast fields for aggregation columns
                // In PARQUET_ONLY mode, score requires parquet→tantivy transcoding
                SplitCacheManager.resetStorageDownloadMetrics();
                searcher.preloadParquetFastFields("score", "active").join();

                Thread.sleep(3000);

                var afterFfPrewarm = SplitCacheManager.getStorageDownloadMetrics();
                System.out.println("[GAP 4] Parquet fast field prewarm downloads: "
                        + afterFfPrewarm.getTotalDownloads()
                        + " (" + afterFfPrewarm.getTotalBytes() + " bytes)");
                assertTrue(afterFfPrewarm.getTotalDownloads() > 0,
                        "Parquet fast field prewarm should download parquet data for transcoding");

                Thread.sleep(500);

                // RESET — everything after this should be zero downloads
                SplitCacheManager.resetStorageDownloadMetrics();

                // AGGREGATION: stats on "score" — needs transcoded fast field data
                StatsAggregation scoreStats = new StatsAggregation("score_stats", "score");
                SearchResult aggResult = searcher.search(
                        new SplitMatchAllQuery(), 0, "score_stats", scoreStats);
                assertTrue(aggResult.hasAggregations(), "Should have aggregation results");
                StatsResult sr = (StatsResult) aggResult.getAggregation("score_stats");
                assertNotNull(sr);
                assertEquals(NUM_ROWS, sr.getCount(),
                        "Stats on score should count all docs");

                var afterAgg = SplitCacheManager.getStorageDownloadMetrics();
                System.out.println("[GAP 4] Post-prewarm aggregation downloads: "
                        + afterAgg.getTotalDownloads()
                        + " (" + afterAgg.getTotalBytes() + " bytes)");

                assertEquals(0, afterAgg.getTotalDownloads(),
                        "GAP 4: aggregation on prewarmed parquet fast field caused "
                                + afterAgg.getTotalDownloads() + " downloads ("
                                + afterAgg.getTotalBytes() + " bytes) after "
                                + "preloadParquetFastFields(). Transcoded fast field data "
                                + "should be fully cached.");
            }
        }
    }
}

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

import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.*;
import java.util.*;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Validates that a companion split backed by 5 parquet files correctly prewarms
 * and retrieves data from ALL files with zero post-prewarm storage downloads.
 *
 * Setup:
 *   - 5 parquet files, each with 20 rows (100 total), 5 columns each
 *   - idOffset ensures non-overlapping IDs across files (0-19, 20-39, ..., 80-99)
 *   - Prewarm only 1 parquet column ("name") + TERM dictionary
 *   - Query match-all, retrieve all 100 rows, assert data from all 5 files present
 *   - Assert zero storage downloads after prewarm
 *
 * Run with: mvn test -Dtest=MultiParquetPrewarmTest
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class MultiParquetPrewarmTest {

    private static final int NUM_FILES = 5;
    private static final int ROWS_PER_FILE = 20;
    private static final int TOTAL_ROWS = NUM_FILES * ROWS_PER_FILE;

    @Test
    @Order(1)
    @DisplayName("5 parquet files: prewarm 1 column + TERM, retrieve all rows, zero downloads")
    void multiFilePrewarmAndRetrieve(@TempDir Path dir) throws Exception {
        // ── CREATE 5 PARQUET FILES ──────────────────────────────────
        List<String> parquetPaths = new ArrayList<>();
        for (int i = 0; i < NUM_FILES; i++) {
            Path pqFile = dir.resolve("data_" + i + ".parquet");
            long idOffset = (long) i * ROWS_PER_FILE;
            QuickwitSplit.nativeWriteTestParquet(pqFile.toString(), ROWS_PER_FILE, idOffset);
            parquetPaths.add(pqFile.toString());
            System.out.println("Created parquet file " + i + ": " + pqFile.getFileName()
                    + " (ids " + idOffset + ".." + (idOffset + ROWS_PER_FILE - 1) + ")");
        }

        // ── INDEX INTO A SINGLE COMPANION SPLIT ─────────────────────
        Path splitFile = dir.resolve("multi.split");
        ParquetCompanionConfig config = new ParquetCompanionConfig(dir.toString())
                .withFastFieldMode(ParquetCompanionConfig.FastFieldMode.HYBRID);

        QuickwitSplit.SplitMetadata metadata = QuickwitSplit.createFromParquet(
                parquetPaths, splitFile.toString(), config);

        assertNotNull(metadata);
        assertEquals(TOTAL_ROWS, metadata.getNumDocs(),
                "Split should contain all " + TOTAL_ROWS + " rows from " + NUM_FILES + " files");
        System.out.println("Created companion split: " + splitFile.getFileName()
                + " (" + metadata.getNumDocs() + " docs from " + NUM_FILES + " files)");

        // ── OPEN SEARCHER ───────────────────────────────────────────
        Path diskCache = dir.resolve("disk-cache");
        SplitCacheManager.TieredCacheConfig tiered = new SplitCacheManager.TieredCacheConfig()
                .withDiskCachePath(diskCache.toString())
                .withMaxDiskSize(100_000_000);

        SplitCacheManager.CacheConfig cacheConfig =
                new SplitCacheManager.CacheConfig("multi-pq-" + System.nanoTime())
                        .withMaxCacheSize(50_000_000)
                        .withParquetTableRoot(dir.toString())
                        .withTieredCache(tiered);

        try (SplitCacheManager cm = SplitCacheManager.getInstance(cacheConfig)) {
            String splitUri = "file://" + splitFile.toAbsolutePath();
            try (SplitSearcher searcher = cm.createSplitSearcher(splitUri, metadata)) {
                assertTrue(searcher.hasParquetCompanion(), "Should be a parquet companion split");

                // ── PREWARM: TERM dictionary + 1 parquet column ("name") ──
                System.out.println("\n[PREWARM] Loading TERM dictionary...");
                SplitCacheManager.resetStorageDownloadMetrics();

                searcher.preloadComponents(
                        SplitSearcher.IndexComponent.TERM,
                        SplitSearcher.IndexComponent.POSTINGS,
                        SplitSearcher.IndexComponent.FASTFIELD,
                        SplitSearcher.IndexComponent.FIELDNORM
                ).join();

                var afterSplitPrewarm = SplitCacheManager.getStorageDownloadMetrics();
                System.out.println("   Split component prewarm: "
                        + afterSplitPrewarm.getTotalDownloads() + " downloads ("
                        + afterSplitPrewarm.getTotalBytes() + " bytes)");

                System.out.println("[PREWARM] Loading parquet column 'name' across all files...");
                searcher.preloadParquetColumns("name").join();

                var afterPqPrewarm = SplitCacheManager.getStorageDownloadMetrics();
                System.out.println("   After parquet column prewarm: "
                        + afterPqPrewarm.getTotalDownloads() + " total downloads ("
                        + afterPqPrewarm.getTotalBytes() + " bytes)");
                assertTrue(afterPqPrewarm.getTotalDownloads() > 0,
                        "Prewarm should have downloaded data");

                // Wait for async cache writes
                Thread.sleep(3000);

                // ── RESET ───────────────────────────────────────────────
                System.out.println("\n[RESET] Clearing download metrics — all subsequent ops must be zero downloads");
                SplitCacheManager.resetStorageDownloadMetrics();

                // ── QUERY: match-all, retrieve ALL rows ─────────────────
                System.out.println("\n[QUERY] Searching for all " + TOTAL_ROWS + " rows...");
                SplitQuery query = searcher.parseQuery("*");
                SearchResult results = searcher.search(query, TOTAL_ROWS + 10);
                assertEquals(TOTAL_ROWS, results.getHits().size(),
                        "Should return all " + TOTAL_ROWS + " rows");
                System.out.println("   Search returned " + results.getHits().size() + " hits");

                var afterSearch = SplitCacheManager.getStorageDownloadMetrics();
                assertEquals(0, afterSearch.getTotalDownloads(),
                        "Search should have 0 storage downloads but got "
                                + afterSearch.getTotalDownloads() + " ("
                                + afterSearch.getTotalBytes() + " bytes)");
                System.out.println("   => 0 downloads (cache hit)");

                // ── RETRIEVE: all docs with projected "name" field ──────
                System.out.println("\n[RETRIEVE] Fetching 'name' field from all " + TOTAL_ROWS + " docs...");
                SplitCacheManager.resetStorageDownloadMetrics();

                Set<Long> seenIds = new HashSet<>();
                Set<String> seenNames = new HashSet<>();
                Set<Integer> fileIndicesSeen = new HashSet<>();

                for (SearchResult.Hit hit : results.getHits()) {
                    DocAddress addr = hit.getDocAddress();
                    Document doc = searcher.docProjected(addr, "id", "name");

                    Object idVal = doc.getFirst("id");
                    assertNotNull(idVal, "id should not be null");
                    long id = ((Number) idVal).longValue();
                    seenIds.add(id);

                    Object nameVal = doc.getFirst("name");
                    assertNotNull(nameVal, "name should not be null");
                    String name = nameVal.toString();
                    seenNames.add(name);
                    assertEquals("item_" + id, name,
                            "name should match id pattern");

                    // Track which source file this row came from
                    int fileIndex = (int) (id / ROWS_PER_FILE);
                    fileIndicesSeen.add(fileIndex);
                }

                var afterRetrieval = SplitCacheManager.getStorageDownloadMetrics();
                System.out.println("   Retrieved " + seenIds.size() + " unique docs");
                System.out.println("   Files represented: " + fileIndicesSeen.stream()
                        .sorted().map(i -> "data_" + i + ".parquet")
                        .collect(Collectors.joining(", ")));
                System.out.println("   Downloads: " + afterRetrieval.getTotalDownloads()
                        + " (" + afterRetrieval.getTotalBytes() + " bytes)");

                // ── ASSERTIONS ──────────────────────────────────────────

                // All 100 unique IDs present
                assertEquals(TOTAL_ROWS, seenIds.size(),
                        "Should see all " + TOTAL_ROWS + " unique IDs");

                // All 5 files contributed data
                assertEquals(NUM_FILES, fileIndicesSeen.size(),
                        "Data should come from all " + NUM_FILES + " parquet files, "
                                + "but only got files: " + fileIndicesSeen);

                // Verify complete ID range: 0..99
                for (long expectedId = 0; expectedId < TOTAL_ROWS; expectedId++) {
                    assertTrue(seenIds.contains(expectedId),
                            "Missing id=" + expectedId + " (from file data_"
                                    + (expectedId / ROWS_PER_FILE) + ".parquet)");
                }

                // Zero downloads after prewarm
                assertEquals(0, afterRetrieval.getTotalDownloads(),
                        "Doc retrieval should have 0 storage downloads but got "
                                + afterRetrieval.getTotalDownloads() + " ("
                                + afterRetrieval.getTotalBytes() + " bytes). "
                                + "Parquet column prewarm should cover all " + NUM_FILES + " files.");

                System.out.println("\n" + "=".repeat(60));
                System.out.println("  PASSED: " + TOTAL_ROWS + " docs from " + NUM_FILES
                        + " parquet files, 0 downloads after prewarm");
                System.out.println("=".repeat(60));
            }
        }
    }
}

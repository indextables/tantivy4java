package io.indextables.tantivy4java;

import io.indextables.tantivy4java.core.Document;
import io.indextables.tantivy4java.core.Schema;
import io.indextables.tantivy4java.result.SearchResult;
import io.indextables.tantivy4java.split.*;
import io.indextables.tantivy4java.split.merge.*;

import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.nio.file.Path;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Integration tests for Parquet Page Index — both retrieval paths.
 *
 * Path 1 (legacy files): Parquet files WITHOUT native offset index in their footer.
 *   At indexing time, page locations are computed via Thrift page header scanning
 *   and stored in the split manifest. At read time, they are injected into
 *   ParquetMetaData so the reader uses page-level byte range requests.
 *
 * Path 2 (modern files): Parquet files WITH native offset index in their footer.
 *   At indexing time, NO page locations are computed or stored — the manifest
 *   page_locations are empty. At read time, CachedParquetReader loads the
 *   offset index directly from the parquet footer via PageIndexPolicy::Optional.
 *
 * Both paths must produce correct search results and document retrieval.
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class ParquetCompanionPageIndexTest {

    private static SplitCacheManager cacheManager;
    private static final ObjectMapper MAPPER = new ObjectMapper();

    @BeforeAll
    static void setUp() {
        SplitCacheManager.CacheConfig config =
                new SplitCacheManager.CacheConfig("parquet-page-index-test")
                        .withMaxCacheSize(200_000_000);
        cacheManager = SplitCacheManager.getInstance(config);
    }

    @AfterAll
    static void tearDown() {
        if (cacheManager != null) {
            try { cacheManager.close(); } catch (Exception e) { /* ignore */ }
        }
    }

    /** Write parquet WITH native offset index (default Arrow behavior). */
    private static void writeModernParquet(String path, int numRows, long idOffset) {
        QuickwitSplit.nativeWriteTestParquet(path, numRows, idOffset);
    }

    /** Write parquet WITHOUT native offset index (legacy format). */
    private static void writeLegacyParquet(String path, int numRows, long idOffset) {
        QuickwitSplit.nativeWriteTestParquetNoPageIndex(path, numRows, idOffset);
    }

    /**
     * Assert that ALL files in the split have the expected pageIndexSource.
     * Validates that:
     * - Legacy files have "manifest" (computed at indexing time, stored in manifest)
     * - Modern files have "native" (offset index in parquet footer, used at read time)
     */
    private void assertPageIndexSource(SplitSearcher searcher, String expectedSource,
                                        String testLabel) throws Exception {
        String statsJson = searcher.getParquetRetrievalStats();
        assertNotNull(statsJson, testLabel + ": stats should not be null");

        JsonNode stats = MAPPER.readTree(statsJson);
        JsonNode fileSizes = stats.get("fileSizes");
        assertNotNull(fileSizes, testLabel + ": fileSizes should be present");
        assertTrue(fileSizes.isArray() && fileSizes.size() > 0,
                testLabel + ": fileSizes should be non-empty array");

        for (int i = 0; i < fileSizes.size(); i++) {
            JsonNode file = fileSizes.get(i);
            String source = file.get("pageIndexSource").asText();
            assertEquals(expectedSource, source,
                    testLabel + ": file " + i + " (" + file.get("path").asText()
                    + ") should have pageIndexSource=" + expectedSource);
        }
    }

    /**
     * Assert pageIndexSource per file for mixed splits (some legacy, some modern).
     */
    private void assertMixedPageIndexSources(SplitSearcher searcher,
                                              String[] expectedSources,
                                              String testLabel) throws Exception {
        String statsJson = searcher.getParquetRetrievalStats();
        assertNotNull(statsJson, testLabel + ": stats should not be null");

        JsonNode stats = MAPPER.readTree(statsJson);
        JsonNode fileSizes = stats.get("fileSizes");
        assertEquals(expectedSources.length, fileSizes.size(),
                testLabel + ": file count mismatch");

        for (int i = 0; i < expectedSources.length; i++) {
            JsonNode file = fileSizes.get(i);
            String source = file.get("pageIndexSource").asText();
            assertEquals(expectedSources[i], source,
                    testLabel + ": file " + i + " (" + file.get("path").asText()
                    + ") should have pageIndexSource=" + expectedSources[i]);
        }
    }

    /**
     * Shared helper: create companion split, search, retrieve docs, verify correctness.
     * Used by both legacy and modern tests to ensure identical behavior.
     *
     * @param expectedPageIndexSource expected pageIndexSource for all files ("manifest" or "native")
     */
    private void verifySearchAndRetrieval(
            Path dir, Path splitFile, QuickwitSplit.SplitMetadata metadata,
            int totalRows, String expectedPageIndexSource, String testLabel) throws Exception {

        String splitUrl = "file://" + splitFile.toAbsolutePath();
        try (SplitSearcher searcher = cacheManager.createSplitSearcher(
                splitUrl, metadata, dir.toString())) {

            assertTrue(searcher.hasParquetCompanion(),
                    testLabel + ": should be parquet companion");

            // Assert page index source for all files
            assertPageIndexSource(searcher, expectedPageIndexSource, testLabel);

            // Match-all to confirm doc count
            SplitQuery allQuery = searcher.parseQuery("*");
            SearchResult allResults = searcher.search(allQuery, totalRows + 1);
            assertEquals(totalRows, allResults.getHits().size(),
                    testLabel + ": match-all should return all docs");

            // Term query for first item
            SplitQuery firstQuery = new SplitTermQuery("name", "item_0");
            SearchResult firstResults = searcher.search(firstQuery, 5);
            assertTrue(firstResults.getHits().size() >= 1,
                    testLabel + ": should find item_0");
            Document firstDoc = searcher.docProjected(
                    firstResults.getHits().get(0).getDocAddress(),
                    "id", "name", "score", "active", "category");
            assertEquals(0L, ((Number) firstDoc.getFirst("id")).longValue(),
                    testLabel + ": item_0 should have id=0");
            assertEquals("item_0", firstDoc.getFirst("name"));
            assertEquals(10.0, ((Number) firstDoc.getFirst("score")).doubleValue(), 0.01);
            assertEquals(true, firstDoc.getFirst("active"));
            assertEquals("cat_0", firstDoc.getFirst("category"));

            // Term query for middle item
            int midId = totalRows / 2;
            SplitQuery midQuery = new SplitTermQuery("name", "item_" + midId);
            SearchResult midResults = searcher.search(midQuery, 5);
            assertTrue(midResults.getHits().size() >= 1,
                    testLabel + ": should find item_" + midId);
            Document midDoc = searcher.docProjected(
                    midResults.getHits().get(0).getDocAddress(),
                    "id", "name", "score");
            assertEquals((long) midId, ((Number) midDoc.getFirst("id")).longValue());
            assertEquals("item_" + midId, midDoc.getFirst("name"));
            // score formula: i * 1.5 + 10.0 (i is relative to file offset)
            assertNotNull(midDoc.getFirst("score"));

            // Term query for last item
            int lastId = totalRows - 1;
            SplitQuery lastQuery = new SplitTermQuery("name", "item_" + lastId);
            SearchResult lastResults = searcher.search(lastQuery, 5);
            assertTrue(lastResults.getHits().size() >= 1,
                    testLabel + ": should find item_" + lastId);
            Document lastDoc = searcher.docProjected(
                    lastResults.getHits().get(0).getDocAddress(),
                    "id", "name");
            assertEquals((long) lastId, ((Number) lastDoc.getFirst("id")).longValue());
        }
    }

    // ===================================================================
    // PATH 1: LEGACY FILES (no native offset index)
    //   Page locations computed at indexing time, stored in manifest,
    //   injected at read time.
    // ===================================================================

    @Test
    @Order(1)
    void testLegacySingleLargeFile(@TempDir Path dir) throws Exception {
        Path parquetFile = dir.resolve("legacy_single.parquet");
        Path splitFile = dir.resolve("legacy_single.split");

        writeLegacyParquet(parquetFile.toString(), 20_000, 0);

        ParquetCompanionConfig config = new ParquetCompanionConfig(dir.toString())
                .withStatisticsFields("id", "score", "name");

        QuickwitSplit.SplitMetadata metadata = QuickwitSplit.createFromParquet(
                Collections.singletonList(parquetFile.toString()),
                splitFile.toString(), config);

        assertEquals(20_000, metadata.getNumDocs());
        verifySearchAndRetrieval(dir, splitFile, metadata, 20_000, "manifest", "legacy-single");
    }

    @Test
    @Order(2)
    void testLegacyMultiFile(@TempDir Path dir) throws Exception {
        int filesCount = 5;
        int rowsPerFile = 10_000;
        int totalRows = filesCount * rowsPerFile;

        List<String> parquetPaths = new ArrayList<>();
        for (int i = 0; i < filesCount; i++) {
            Path pq = dir.resolve("legacy_part_" + i + ".parquet");
            writeLegacyParquet(pq.toString(), rowsPerFile, (long) i * rowsPerFile);
            parquetPaths.add(pq.toString());
        }

        Path splitFile = dir.resolve("legacy_multi.split");
        ParquetCompanionConfig config = new ParquetCompanionConfig(dir.toString())
                .withStatisticsFields("id", "score");

        QuickwitSplit.SplitMetadata metadata = QuickwitSplit.createFromParquet(
                parquetPaths, splitFile.toString(), config);

        assertEquals(totalRows, metadata.getNumDocs());
        verifySearchAndRetrieval(dir, splitFile, metadata, totalRows, "manifest", "legacy-multi");
    }

    @Test
    @Order(3)
    void testLegacyBatchRetrieval(@TempDir Path dir) throws Exception {
        Path pq1 = dir.resolve("legacy_batch1.parquet");
        Path pq2 = dir.resolve("legacy_batch2.parquet");
        Path pq3 = dir.resolve("legacy_batch3.parquet");
        Path splitFile = dir.resolve("legacy_batch.split");

        writeLegacyParquet(pq1.toString(), 15_000, 0);
        writeLegacyParquet(pq2.toString(), 15_000, 15_000);
        writeLegacyParquet(pq3.toString(), 15_000, 30_000);

        ParquetCompanionConfig config = new ParquetCompanionConfig(dir.toString())
                .withStatisticsFields("id");

        QuickwitSplit.SplitMetadata metadata = QuickwitSplit.createFromParquet(
                Arrays.asList(pq1.toString(), pq2.toString(), pq3.toString()),
                splitFile.toString(), config);

        assertEquals(45_000, metadata.getNumDocs());

        String splitUrl = "file://" + splitFile.toAbsolutePath();
        try (SplitSearcher searcher = cacheManager.createSplitSearcher(
                splitUrl, metadata, dir.toString())) {

            // Assert all files use manifest-based page index
            assertPageIndexSource(searcher, "manifest", "legacy-batch");

            // Search for items from each file (different page ranges)
            String[] targets = {"item_5000", "item_20000", "item_40000"};
            long[] expectedIds = {5000L, 20000L, 40000L};
            io.indextables.tantivy4java.core.DocAddress[] addrs =
                    new io.indextables.tantivy4java.core.DocAddress[targets.length];

            for (int i = 0; i < targets.length; i++) {
                SplitQuery query = new SplitTermQuery("name", targets[i]);
                SearchResult results = searcher.search(query, 1);
                assertTrue(results.getHits().size() >= 1,
                        "legacy-batch: should find " + targets[i]);
                addrs[i] = results.getHits().get(0).getDocAddress();
            }

            List<Document> batchDocs = searcher.docBatchProjected(addrs,
                    "id", "name", "score");
            assertEquals(targets.length, batchDocs.size());

            for (int i = 0; i < targets.length; i++) {
                assertEquals(expectedIds[i],
                        ((Number) batchDocs.get(i).getFirst("id")).longValue());
                assertEquals(targets[i], batchDocs.get(i).getFirst("name"));
            }
        }
    }

    // ===================================================================
    // PATH 2: MODERN FILES (native offset index in footer)
    //   No page locations stored in manifest. CachedParquetReader loads
    //   the offset index from the parquet footer at read time.
    // ===================================================================

    @Test
    @Order(4)
    void testModernSingleLargeFile(@TempDir Path dir) throws Exception {
        Path parquetFile = dir.resolve("modern_single.parquet");
        Path splitFile = dir.resolve("modern_single.split");

        writeModernParquet(parquetFile.toString(), 20_000, 0);

        ParquetCompanionConfig config = new ParquetCompanionConfig(dir.toString())
                .withStatisticsFields("id", "score", "name");

        QuickwitSplit.SplitMetadata metadata = QuickwitSplit.createFromParquet(
                Collections.singletonList(parquetFile.toString()),
                splitFile.toString(), config);

        assertEquals(20_000, metadata.getNumDocs());
        verifySearchAndRetrieval(dir, splitFile, metadata, 20_000, "native", "modern-single");
    }

    @Test
    @Order(5)
    void testModernMultiFile(@TempDir Path dir) throws Exception {
        int filesCount = 5;
        int rowsPerFile = 10_000;
        int totalRows = filesCount * rowsPerFile;

        List<String> parquetPaths = new ArrayList<>();
        for (int i = 0; i < filesCount; i++) {
            Path pq = dir.resolve("modern_part_" + i + ".parquet");
            writeModernParquet(pq.toString(), rowsPerFile, (long) i * rowsPerFile);
            parquetPaths.add(pq.toString());
        }

        Path splitFile = dir.resolve("modern_multi.split");
        ParquetCompanionConfig config = new ParquetCompanionConfig(dir.toString())
                .withStatisticsFields("id", "score");

        QuickwitSplit.SplitMetadata metadata = QuickwitSplit.createFromParquet(
                parquetPaths, splitFile.toString(), config);

        assertEquals(totalRows, metadata.getNumDocs());
        verifySearchAndRetrieval(dir, splitFile, metadata, totalRows, "native", "modern-multi");
    }

    @Test
    @Order(6)
    void testModernBatchRetrieval(@TempDir Path dir) throws Exception {
        Path pq1 = dir.resolve("modern_batch1.parquet");
        Path pq2 = dir.resolve("modern_batch2.parquet");
        Path pq3 = dir.resolve("modern_batch3.parquet");
        Path splitFile = dir.resolve("modern_batch.split");

        writeModernParquet(pq1.toString(), 15_000, 0);
        writeModernParquet(pq2.toString(), 15_000, 15_000);
        writeModernParquet(pq3.toString(), 15_000, 30_000);

        ParquetCompanionConfig config = new ParquetCompanionConfig(dir.toString())
                .withStatisticsFields("id");

        QuickwitSplit.SplitMetadata metadata = QuickwitSplit.createFromParquet(
                Arrays.asList(pq1.toString(), pq2.toString(), pq3.toString()),
                splitFile.toString(), config);

        assertEquals(45_000, metadata.getNumDocs());

        String splitUrl = "file://" + splitFile.toAbsolutePath();
        try (SplitSearcher searcher = cacheManager.createSplitSearcher(
                splitUrl, metadata, dir.toString())) {

            // Assert all files use native page index from footer
            assertPageIndexSource(searcher, "native", "modern-batch");

            String[] targets = {"item_5000", "item_20000", "item_40000"};
            long[] expectedIds = {5000L, 20000L, 40000L};
            io.indextables.tantivy4java.core.DocAddress[] addrs =
                    new io.indextables.tantivy4java.core.DocAddress[targets.length];

            for (int i = 0; i < targets.length; i++) {
                SplitQuery query = new SplitTermQuery("name", targets[i]);
                SearchResult results = searcher.search(query, 1);
                assertTrue(results.getHits().size() >= 1,
                        "modern-batch: should find " + targets[i]);
                addrs[i] = results.getHits().get(0).getDocAddress();
            }

            List<Document> batchDocs = searcher.docBatchProjected(addrs,
                    "id", "name", "score");
            assertEquals(targets.length, batchDocs.size());

            for (int i = 0; i < targets.length; i++) {
                assertEquals(expectedIds[i],
                        ((Number) batchDocs.get(i).getFirst("id")).longValue());
                assertEquals(targets[i], batchDocs.get(i).getFirst("name"));
            }
        }
    }

    // ===================================================================
    // MIXED: Legacy + modern files in the same companion split
    // ===================================================================

    @Test
    @Order(7)
    void testMixedLegacyAndModernFiles(@TempDir Path dir) throws Exception {
        // 2 legacy files (page locations computed, stored in manifest)
        Path legacy1 = dir.resolve("mixed_legacy1.parquet");
        Path legacy2 = dir.resolve("mixed_legacy2.parquet");
        writeLegacyParquet(legacy1.toString(), 10_000, 0);
        writeLegacyParquet(legacy2.toString(), 10_000, 10_000);

        // 2 modern files (page locations from footer, nothing in manifest)
        Path modern1 = dir.resolve("mixed_modern1.parquet");
        Path modern2 = dir.resolve("mixed_modern2.parquet");
        writeModernParquet(modern1.toString(), 10_000, 20_000);
        writeModernParquet(modern2.toString(), 10_000, 30_000);

        Path splitFile = dir.resolve("mixed.split");
        ParquetCompanionConfig config = new ParquetCompanionConfig(dir.toString())
                .withStatisticsFields("id");

        QuickwitSplit.SplitMetadata metadata = QuickwitSplit.createFromParquet(
                Arrays.asList(
                        legacy1.toString(), legacy2.toString(),
                        modern1.toString(), modern2.toString()),
                splitFile.toString(), config);

        assertEquals(40_000, metadata.getNumDocs());

        String splitUrl = "file://" + splitFile.toAbsolutePath();
        try (SplitSearcher searcher = cacheManager.createSplitSearcher(
                splitUrl, metadata, dir.toString())) {

            // Assert per-file page index sources: 2 legacy (manifest) + 2 modern (native)
            assertMixedPageIndexSources(searcher,
                    new String[]{"manifest", "manifest", "native", "native"},
                    "mixed");

            // Verify retrieval from each file:
            // legacy1: items 0-9999
            // legacy2: items 10000-19999
            // modern1: items 20000-29999
            // modern2: items 30000-39999
            int[][] fileRanges = {{0, 5000}, {10000, 15000}, {20000, 25000}, {30000, 35000}};
            String[] labels = {"legacy1", "legacy2", "modern1", "modern2"};

            for (int f = 0; f < fileRanges.length; f++) {
                int targetId = fileRanges[f][1];
                String targetName = "item_" + targetId;

                SplitQuery query = new SplitTermQuery("name", targetName);
                SearchResult results = searcher.search(query, 1);
                assertTrue(results.getHits().size() >= 1,
                        "mixed: should find " + targetName + " from " + labels[f]);

                Document doc = searcher.docProjected(
                        results.getHits().get(0).getDocAddress(),
                        "id", "name", "score", "active");
                assertEquals((long) targetId,
                        ((Number) doc.getFirst("id")).longValue(),
                        "mixed: " + labels[f] + " should have correct id");
                assertEquals(targetName, doc.getFirst("name"));
                assertNotNull(doc.getFirst("score"));
                assertNotNull(doc.getFirst("active"));
            }

            // Batch retrieval spanning all 4 files
            io.indextables.tantivy4java.core.DocAddress[] addrs =
                    new io.indextables.tantivy4java.core.DocAddress[4];
            long[] ids = {5000L, 15000L, 25000L, 35000L};
            for (int i = 0; i < ids.length; i++) {
                SplitQuery query = new SplitTermQuery("name", "item_" + ids[i]);
                SearchResult results = searcher.search(query, 1);
                addrs[i] = results.getHits().get(0).getDocAddress();
            }

            List<Document> batchDocs = searcher.docBatchProjected(addrs,
                    "id", "name");
            assertEquals(4, batchDocs.size());

            for (int i = 0; i < ids.length; i++) {
                assertEquals(ids[i],
                        ((Number) batchDocs.get(i).getFirst("id")).longValue(),
                        "batch doc " + i + " should have correct id");
            }
        }
    }

    // ===================================================================
    // HYBRID mode tests with both paths
    // ===================================================================

    @Test
    @Order(8)
    void testHybridModeLegacy(@TempDir Path dir) throws Exception {
        Path pq1 = dir.resolve("hybrid_legacy1.parquet");
        Path pq2 = dir.resolve("hybrid_legacy2.parquet");
        Path splitFile = dir.resolve("hybrid_legacy.split");

        writeLegacyParquet(pq1.toString(), 10_000, 0);
        writeLegacyParquet(pq2.toString(), 10_000, 10_000);

        ParquetCompanionConfig config = new ParquetCompanionConfig(dir.toString())
                .withFastFieldMode(ParquetCompanionConfig.FastFieldMode.HYBRID)
                .withStatisticsFields("id", "score");

        QuickwitSplit.SplitMetadata metadata = QuickwitSplit.createFromParquet(
                Arrays.asList(pq1.toString(), pq2.toString()),
                splitFile.toString(), config);

        assertEquals(20_000, metadata.getNumDocs());
        verifySearchAndRetrieval(dir, splitFile, metadata, 20_000, "manifest", "hybrid-legacy");
    }

    @Test
    @Order(9)
    void testHybridModeModern(@TempDir Path dir) throws Exception {
        Path pq1 = dir.resolve("hybrid_modern1.parquet");
        Path pq2 = dir.resolve("hybrid_modern2.parquet");
        Path splitFile = dir.resolve("hybrid_modern.split");

        writeModernParquet(pq1.toString(), 10_000, 0);
        writeModernParquet(pq2.toString(), 10_000, 10_000);

        ParquetCompanionConfig config = new ParquetCompanionConfig(dir.toString())
                .withFastFieldMode(ParquetCompanionConfig.FastFieldMode.HYBRID)
                .withStatisticsFields("id", "score");

        QuickwitSplit.SplitMetadata metadata = QuickwitSplit.createFromParquet(
                Arrays.asList(pq1.toString(), pq2.toString()),
                splitFile.toString(), config);

        assertEquals(20_000, metadata.getNumDocs());
        verifySearchAndRetrieval(dir, splitFile, metadata, 20_000, "native", "hybrid-modern");
    }

    // ===================================================================
    // Retrieval stats validation
    // ===================================================================

    @Test
    @Order(10)
    void testRetrievalStatsLegacy(@TempDir Path dir) throws Exception {
        Path pq1 = dir.resolve("stats_legacy1.parquet");
        Path pq2 = dir.resolve("stats_legacy2.parquet");
        Path pq3 = dir.resolve("stats_legacy3.parquet");
        Path splitFile = dir.resolve("stats_legacy.split");

        writeLegacyParquet(pq1.toString(), 10_000, 0);
        writeLegacyParquet(pq2.toString(), 10_000, 10_000);
        writeLegacyParquet(pq3.toString(), 10_000, 20_000);

        ParquetCompanionConfig config = new ParquetCompanionConfig(dir.toString())
                .withStatisticsFields("id");

        QuickwitSplit.SplitMetadata metadata = QuickwitSplit.createFromParquet(
                Arrays.asList(pq1.toString(), pq2.toString(), pq3.toString()),
                splitFile.toString(), config);

        String splitUrl = "file://" + splitFile.toAbsolutePath();
        try (SplitSearcher searcher = cacheManager.createSplitSearcher(
                splitUrl, metadata, dir.toString())) {

            // Assert all files use manifest-based page index
            assertPageIndexSource(searcher, "manifest", "stats-legacy");

            String statsJson = searcher.getParquetRetrievalStats();
            assertNotNull(statsJson);
            JsonNode stats = MAPPER.readTree(statsJson);
            assertEquals(3, stats.get("totalFiles").asInt());
            assertEquals(30_000, stats.get("totalRows").asInt());

            // Retrieval from middle file
            SplitQuery query = new SplitTermQuery("name", "item_15000");
            SearchResult results = searcher.search(query, 1);
            assertTrue(results.getHits().size() >= 1);
            searcher.docProjected(results.getHits().get(0).getDocAddress(),
                    "id", "name");
        }
    }

    @Test
    @Order(11)
    void testRetrievalStatsModern(@TempDir Path dir) throws Exception {
        Path pq1 = dir.resolve("stats_modern1.parquet");
        Path pq2 = dir.resolve("stats_modern2.parquet");
        Path pq3 = dir.resolve("stats_modern3.parquet");
        Path splitFile = dir.resolve("stats_modern.split");

        writeModernParquet(pq1.toString(), 10_000, 0);
        writeModernParquet(pq2.toString(), 10_000, 10_000);
        writeModernParquet(pq3.toString(), 10_000, 20_000);

        ParquetCompanionConfig config = new ParquetCompanionConfig(dir.toString())
                .withStatisticsFields("id");

        QuickwitSplit.SplitMetadata metadata = QuickwitSplit.createFromParquet(
                Arrays.asList(pq1.toString(), pq2.toString(), pq3.toString()),
                splitFile.toString(), config);

        String splitUrl = "file://" + splitFile.toAbsolutePath();
        try (SplitSearcher searcher = cacheManager.createSplitSearcher(
                splitUrl, metadata, dir.toString())) {

            // Assert all files use native page index from footer
            assertPageIndexSource(searcher, "native", "stats-modern");

            String statsJson = searcher.getParquetRetrievalStats();
            assertNotNull(statsJson);
            JsonNode stats = MAPPER.readTree(statsJson);
            assertEquals(3, stats.get("totalFiles").asInt());
            assertEquals(30_000, stats.get("totalRows").asInt());

            // Retrieval from middle file
            SplitQuery query = new SplitTermQuery("name", "item_15000");
            SearchResult results = searcher.search(query, 1);
            assertTrue(results.getHits().size() >= 1);
            searcher.docProjected(results.getHits().get(0).getDocAddress(),
                    "id", "name");
        }
    }

    // ===================================================================
    // MERGE TESTS: Verify page metadata survives split merges
    // ===================================================================

    @Test
    @Order(12)
    void testLegacyPageMetadataSurvivedMerge(@TempDir Path dir) throws Exception {
        // Create 2 companion splits from legacy parquet files
        Path pq1 = dir.resolve("merge_leg1.parquet");
        Path pq2 = dir.resolve("merge_leg2.parquet");
        Path split1 = dir.resolve("merge_leg1.split");
        Path split2 = dir.resolve("merge_leg2.split");
        Path mergedSplit = dir.resolve("merge_leg_merged.split");

        writeLegacyParquet(pq1.toString(), 10_000, 0);
        writeLegacyParquet(pq2.toString(), 10_000, 10_000);

        ParquetCompanionConfig config1 = new ParquetCompanionConfig(dir.toString())
                .withStatisticsFields("id");
        ParquetCompanionConfig config2 = new ParquetCompanionConfig(dir.toString())
                .withStatisticsFields("id");

        QuickwitSplit.SplitMetadata meta1 = QuickwitSplit.createFromParquet(
                Collections.singletonList(pq1.toString()), split1.toString(), config1);
        QuickwitSplit.SplitMetadata meta2 = QuickwitSplit.createFromParquet(
                Collections.singletonList(pq2.toString()), split2.toString(), config2);

        assertEquals(10_000, meta1.getNumDocs());
        assertEquals(10_000, meta2.getNumDocs());

        // Verify pre-merge: both splits have manifest-based page index
        String splitUrl1 = "file://" + split1.toAbsolutePath();
        try (SplitSearcher searcher = cacheManager.createSplitSearcher(
                splitUrl1, meta1, dir.toString())) {
            assertPageIndexSource(searcher, "manifest", "pre-merge-split1");
        }
        String splitUrl2 = "file://" + split2.toAbsolutePath();
        try (SplitSearcher searcher = cacheManager.createSplitSearcher(
                splitUrl2, meta2, dir.toString())) {
            assertPageIndexSource(searcher, "manifest", "pre-merge-split2");
        }

        // Merge the two splits
        QuickwitSplit.MergeConfig mergeConfig = new QuickwitSplit.MergeConfig(
                "test-merge-idx", "test-source", "test-node");
        QuickwitSplit.SplitMetadata mergedMeta = QuickwitSplit.mergeSplits(
                Arrays.asList(split1.toString(), split2.toString()),
                mergedSplit.toString(), mergeConfig);

        assertEquals(20_000, mergedMeta.getNumDocs());

        // Verify post-merge: merged split still has manifest-based page index
        String mergedUrl = "file://" + mergedSplit.toAbsolutePath();
        try (SplitSearcher searcher = cacheManager.createSplitSearcher(
                mergedUrl, mergedMeta, dir.toString())) {

            assertTrue(searcher.hasParquetCompanion(), "merged split should be companion");

            // Page index source should still be "manifest" after merge
            assertPageIndexSource(searcher, "manifest", "post-merge");

            // Verify retrieval works for docs from both original splits
            SplitQuery q1 = new SplitTermQuery("name", "item_0");
            SearchResult r1 = searcher.search(q1, 5);
            assertTrue(r1.getHits().size() >= 1, "should find item_0 from split1");
            Document d1 = searcher.docProjected(
                    r1.getHits().get(0).getDocAddress(), "id", "name");
            assertEquals(0L, ((Number) d1.getFirst("id")).longValue());

            SplitQuery q2 = new SplitTermQuery("name", "item_15000");
            SearchResult r2 = searcher.search(q2, 5);
            assertTrue(r2.getHits().size() >= 1, "should find item_15000 from split2");
            Document d2 = searcher.docProjected(
                    r2.getHits().get(0).getDocAddress(), "id", "name");
            assertEquals(15_000L, ((Number) d2.getFirst("id")).longValue());

            // Match-all to confirm total doc count
            SplitQuery allQuery = searcher.parseQuery("*");
            SearchResult allResults = searcher.search(allQuery, 20_001);
            assertEquals(20_000, allResults.getHits().size(),
                    "merged split should have 20000 docs");
        }
    }

    @Test
    @Order(13)
    void testModernPageMetadataSurvivedMerge(@TempDir Path dir) throws Exception {
        // Create 2 companion splits from modern parquet files
        Path pq1 = dir.resolve("merge_mod1.parquet");
        Path pq2 = dir.resolve("merge_mod2.parquet");
        Path split1 = dir.resolve("merge_mod1.split");
        Path split2 = dir.resolve("merge_mod2.split");
        Path mergedSplit = dir.resolve("merge_mod_merged.split");

        writeModernParquet(pq1.toString(), 10_000, 0);
        writeModernParquet(pq2.toString(), 10_000, 10_000);

        ParquetCompanionConfig config1 = new ParquetCompanionConfig(dir.toString())
                .withStatisticsFields("id");
        ParquetCompanionConfig config2 = new ParquetCompanionConfig(dir.toString())
                .withStatisticsFields("id");

        QuickwitSplit.SplitMetadata meta1 = QuickwitSplit.createFromParquet(
                Collections.singletonList(pq1.toString()), split1.toString(), config1);
        QuickwitSplit.SplitMetadata meta2 = QuickwitSplit.createFromParquet(
                Collections.singletonList(pq2.toString()), split2.toString(), config2);

        // Merge the two splits
        QuickwitSplit.MergeConfig mergeConfig = new QuickwitSplit.MergeConfig(
                "test-merge-idx", "test-source", "test-node");
        QuickwitSplit.SplitMetadata mergedMeta = QuickwitSplit.mergeSplits(
                Arrays.asList(split1.toString(), split2.toString()),
                mergedSplit.toString(), mergeConfig);

        assertEquals(20_000, mergedMeta.getNumDocs());

        // Verify post-merge: modern files should still use native page index
        String mergedUrl = "file://" + mergedSplit.toAbsolutePath();
        try (SplitSearcher searcher = cacheManager.createSplitSearcher(
                mergedUrl, mergedMeta, dir.toString())) {

            assertTrue(searcher.hasParquetCompanion(), "merged split should be companion");

            // Page index source should be "native" — modern files have offset index in footer
            assertPageIndexSource(searcher, "native", "post-merge-modern");

            // Verify retrieval works for docs from both original splits
            SplitQuery q1 = new SplitTermQuery("name", "item_5000");
            SearchResult r1 = searcher.search(q1, 5);
            assertTrue(r1.getHits().size() >= 1, "should find item_5000 from split1");

            SplitQuery q2 = new SplitTermQuery("name", "item_15000");
            SearchResult r2 = searcher.search(q2, 5);
            assertTrue(r2.getHits().size() >= 1, "should find item_15000 from split2");
            Document d2 = searcher.docProjected(
                    r2.getHits().get(0).getDocAddress(), "id", "name");
            assertEquals(15_000L, ((Number) d2.getFirst("id")).longValue());
        }
    }
}

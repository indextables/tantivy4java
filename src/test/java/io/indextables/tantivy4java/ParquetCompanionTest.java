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

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Integration tests for Parquet Companion Mode.
 *
 * Each test gets its own @TempDir to avoid path reuse issues with the native cache.
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class ParquetCompanionTest {

    private static SplitCacheManager cacheManager;

    @BeforeAll
    static void setUp() {
        SplitCacheManager.CacheConfig config = new SplitCacheManager.CacheConfig("parquet-companion-test")
                .withMaxCacheSize(100_000_000);
        cacheManager = SplitCacheManager.getInstance(config);
    }

    @AfterAll
    static void tearDown() {
        if (cacheManager != null) {
            try { cacheManager.close(); } catch (Exception e) { /* ignore */ }
        }
    }

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private static void writeTestParquet(String path, int numRows, long idOffset) {
        QuickwitSplit.nativeWriteTestParquet(path, numRows, idOffset);
    }

    private static void writeTestParquetComplex(String path, int numRows, long idOffset) {
        QuickwitSplit.nativeWriteTestParquetComplex(path, numRows, idOffset);
    }

    // ---------------------------------------------------------------
    // 1. Basic single-file createFromParquet + search
    // ---------------------------------------------------------------
    @Test
    @Order(1)
    void testCreateFromSingleParquetAndSearch(@TempDir Path dir) throws Exception {
        Path parquetFile = dir.resolve("data.parquet");
        Path splitFile = dir.resolve("basic.split");

        writeTestParquet(parquetFile.toString(), 50, 0);
        assertTrue(parquetFile.toFile().exists());

        ParquetCompanionConfig config = new ParquetCompanionConfig(dir.toString())
                .withStatisticsFields("id", "score", "name");

        QuickwitSplit.SplitMetadata metadata = QuickwitSplit.createFromParquet(
                Collections.singletonList(parquetFile.toString()),
                splitFile.toString(),
                config);

        assertNotNull(metadata);
        assertEquals(50, metadata.getNumDocs());
        assertTrue(splitFile.toFile().exists());

        String splitUrl = "file://" + splitFile.toAbsolutePath();
        try (SplitSearcher searcher = cacheManager.createSplitSearcher(splitUrl, metadata, dir.toString())) {
            Schema schema = searcher.getSchema();
            assertNotNull(schema);
            List<String> fieldNames = schema.getFieldNames();
            assertTrue(fieldNames.contains("id"));
            assertTrue(fieldNames.contains("name"));
            assertTrue(fieldNames.contains("score"));
            assertTrue(fieldNames.contains("active"));

            SplitQuery allQuery = searcher.parseQuery("*");
            SearchResult allResults = searcher.search(allQuery, 100);
            assertEquals(50, allResults.getHits().size());

            SplitQuery nameQuery = new SplitTermQuery("name", "item_0");
            SearchResult nameResults = searcher.search(nameQuery, 10);
            assertTrue(nameResults.getHits().size() >= 1);

            assertTrue(searcher.hasParquetCompanion());
        }
    }

    // ---------------------------------------------------------------
    // 2. Column statistics validation
    // ---------------------------------------------------------------
    @Test
    @Order(2)
    void testColumnStatistics(@TempDir Path dir) throws Exception {
        Path parquetFile = dir.resolve("data.parquet");
        Path splitFile = dir.resolve("colstats.split");

        writeTestParquet(parquetFile.toString(), 30, 100);

        ParquetCompanionConfig config = new ParquetCompanionConfig(dir.toString())
                .withStatisticsFields("id", "score", "name");

        QuickwitSplit.SplitMetadata metadata = QuickwitSplit.createFromParquet(
                Collections.singletonList(parquetFile.toString()),
                splitFile.toString(),
                config);

        Map<String, ColumnStatistics> stats = metadata.getColumnStatistics();
        assertNotNull(stats);
        assertFalse(stats.isEmpty());

        // id stats: values 100..129
        ColumnStatistics idStats = stats.get("id");
        assertNotNull(idStats);
        assertEquals("I64", idStats.getFieldType());
        assertEquals(100L, idStats.getMinLong());
        assertEquals(129L, idStats.getMaxLong());
        assertEquals(0, idStats.getNullCount());

        // score stats: f64 values 10.0, 11.5, ... 53.5
        ColumnStatistics scoreStats = stats.get("score");
        assertNotNull(scoreStats);
        assertEquals("F64", scoreStats.getFieldType());
        assertEquals(10.0, scoreStats.getMinDouble(), 0.01);
        assertEquals(53.5, scoreStats.getMaxDouble(), 0.01);

        // name stats: string
        ColumnStatistics nameStats = stats.get("name");
        assertNotNull(nameStats);
        assertEquals("Str", nameStats.getFieldType());
        assertNotNull(nameStats.getMinString());
        assertNotNull(nameStats.getMaxString());

        // overlap methods
        assertTrue(idStats.overlapsRange(110, 120));
        assertFalse(idStats.overlapsRange(200, 300));
        assertTrue(scoreStats.overlapsDoubleRange(20.0, 40.0));
        assertFalse(scoreStats.overlapsDoubleRange(100.0, 200.0));
    }

    // ---------------------------------------------------------------
    // 3. Multi-file indexing
    // ---------------------------------------------------------------
    @Test
    @Order(3)
    void testMultiFileIndexing(@TempDir Path dir) throws Exception {
        Path pq1 = dir.resolve("part1.parquet");
        Path pq2 = dir.resolve("part2.parquet");
        Path splitFile = dir.resolve("multifile.split");

        writeTestParquet(pq1.toString(), 30, 0);
        writeTestParquet(pq2.toString(), 20, 30);

        ParquetCompanionConfig config = new ParquetCompanionConfig(dir.toString())
                .withStatisticsFields("id");

        QuickwitSplit.SplitMetadata metadata = QuickwitSplit.createFromParquet(
                Arrays.asList(pq1.toString(), pq2.toString()), splitFile.toString(), config);

        assertEquals(50, metadata.getNumDocs());

        ColumnStatistics idStats = metadata.getColumnStatistics().get("id");
        assertNotNull(idStats);
        assertEquals(0L, idStats.getMinLong());
        assertEquals(49L, idStats.getMaxLong());

        String splitUrl = "file://" + splitFile.toAbsolutePath();
        try (SplitSearcher searcher = cacheManager.createSplitSearcher(splitUrl, metadata, dir.toString())) {
            SplitQuery allQuery = searcher.parseQuery("*");
            SearchResult results = searcher.search(allQuery, 100);
            assertEquals(50, results.getHits().size());
        }
    }

    // ---------------------------------------------------------------
    // 4. Skip fields
    // ---------------------------------------------------------------
    @Test
    @Order(4)
    void testSkipFields(@TempDir Path dir) throws Exception {
        Path parquetFile = dir.resolve("data.parquet");
        Path splitFile = dir.resolve("skipfields.split");

        writeTestParquet(parquetFile.toString(), 10, 0);

        ParquetCompanionConfig config = new ParquetCompanionConfig(dir.toString())
                .withSkipFields("score", "active");

        QuickwitSplit.SplitMetadata metadata = QuickwitSplit.createFromParquet(
                Collections.singletonList(parquetFile.toString()),
                splitFile.toString(),
                config);

        assertEquals(10, metadata.getNumDocs());

        String splitUrl = "file://" + splitFile.toAbsolutePath();
        try (SplitSearcher searcher = cacheManager.createSplitSearcher(splitUrl, metadata, dir.toString())) {
            Schema schema = searcher.getSchema();
            List<String> fieldNames = schema.getFieldNames();
            assertTrue(fieldNames.contains("id"));
            assertTrue(fieldNames.contains("name"));
            assertFalse(fieldNames.contains("score"));
            assertFalse(fieldNames.contains("active"));
        }
    }

    // ---------------------------------------------------------------
    // 5. Fast field mode: hybrid
    // ---------------------------------------------------------------
    @Test
    @Order(5)
    void testFastFieldModeHybrid(@TempDir Path dir) throws Exception {
        Path parquetFile = dir.resolve("data.parquet");
        Path splitFile = dir.resolve("hybrid.split");

        writeTestParquet(parquetFile.toString(), 20, 0);

        ParquetCompanionConfig config = new ParquetCompanionConfig(dir.toString())
                .withFastFieldMode(ParquetCompanionConfig.FastFieldMode.HYBRID);

        QuickwitSplit.SplitMetadata metadata = QuickwitSplit.createFromParquet(
                Collections.singletonList(parquetFile.toString()),
                splitFile.toString(),
                config);

        assertEquals(20, metadata.getNumDocs());

        String splitUrl = "file://" + splitFile.toAbsolutePath();
        try (SplitSearcher searcher = cacheManager.createSplitSearcher(splitUrl, metadata, dir.toString())) {
            SplitQuery allQuery = searcher.parseQuery("*");
            SearchResult results = searcher.search(allQuery, 50);
            assertEquals(20, results.getHits().size());
            assertTrue(searcher.hasParquetCompanion());
        }
    }

    // ---------------------------------------------------------------
    // 6. Fast field mode: parquet only
    // ---------------------------------------------------------------
    @Test
    @Order(6)
    void testFastFieldModeParquetOnly(@TempDir Path dir) throws Exception {
        Path parquetFile = dir.resolve("data.parquet");
        Path splitFile = dir.resolve("pqonly.split");

        writeTestParquet(parquetFile.toString(), 15, 0);

        ParquetCompanionConfig config = new ParquetCompanionConfig(dir.toString())
                .withFastFieldMode(ParquetCompanionConfig.FastFieldMode.PARQUET_ONLY);

        QuickwitSplit.SplitMetadata metadata = QuickwitSplit.createFromParquet(
                Collections.singletonList(parquetFile.toString()),
                splitFile.toString(),
                config);

        assertEquals(15, metadata.getNumDocs());

        String splitUrl = "file://" + splitFile.toAbsolutePath();
        try (SplitSearcher searcher = cacheManager.createSplitSearcher(splitUrl, metadata, dir.toString())) {
            SplitQuery allQuery = searcher.parseQuery("*");
            SearchResult results = searcher.search(allQuery, 50);
            assertEquals(15, results.getHits().size());
            assertTrue(searcher.hasParquetCompanion());
        }
    }

    // ---------------------------------------------------------------
    // 7. Validation: empty file list
    // ---------------------------------------------------------------
    @Test
    @Order(7)
    void testEmptyFileListThrows(@TempDir Path dir) {
        ParquetCompanionConfig config = new ParquetCompanionConfig(dir.toString());

        assertThrows(IllegalArgumentException.class, () ->
            QuickwitSplit.createFromParquet(Collections.emptyList(),
                    dir.resolve("empty.split").toString(), config));
    }

    // ---------------------------------------------------------------
    // 8. Validation: bad output path
    // ---------------------------------------------------------------
    @Test
    @Order(8)
    void testBadOutputPathThrows(@TempDir Path dir) {
        Path parquetFile = dir.resolve("data.parquet");
        writeTestParquet(parquetFile.toString(), 5, 0);

        ParquetCompanionConfig config = new ParquetCompanionConfig(dir.toString());

        assertThrows(IllegalArgumentException.class, () ->
            QuickwitSplit.createFromParquet(
                    Collections.singletonList(parquetFile.toString()),
                    dir.resolve("output.txt").toString(),
                    config));
    }

    // ---------------------------------------------------------------
    // 9. Nonexistent parquet file
    // ---------------------------------------------------------------
    @Test
    @Order(9)
    void testNonexistentParquetFileThrows(@TempDir Path dir) {
        ParquetCompanionConfig config = new ParquetCompanionConfig(dir.toString());

        assertThrows(RuntimeException.class, () ->
            QuickwitSplit.createFromParquet(
                    Collections.singletonList("/nonexistent/file.parquet"),
                    dir.resolve("nonexist.split").toString(),
                    config));
    }

    // ---------------------------------------------------------------
    // 10. Merge two parquet companion splits
    // ---------------------------------------------------------------
    @Test
    @Order(10)
    void testMergeParquetCompanionSplits(@TempDir Path dir) throws Exception {
        // Create split A
        Path pqA = dir.resolve("a.parquet");
        Path splitA = dir.resolve("a.split");
        writeTestParquet(pqA.toString(), 25, 0);
        ParquetCompanionConfig configA = new ParquetCompanionConfig(dir.toString())
                .withStatisticsFields("id");
        QuickwitSplit.SplitMetadata metaA = QuickwitSplit.createFromParquet(
                Collections.singletonList(pqA.toString()), splitA.toString(), configA);
        assertEquals(25, metaA.getNumDocs());

        // Create split B
        Path pqB = dir.resolve("b.parquet");
        Path splitB = dir.resolve("b.split");
        writeTestParquet(pqB.toString(), 25, 25);
        ParquetCompanionConfig configB = new ParquetCompanionConfig(dir.toString())
                .withStatisticsFields("id");
        QuickwitSplit.SplitMetadata metaB = QuickwitSplit.createFromParquet(
                Collections.singletonList(pqB.toString()), splitB.toString(), configB);
        assertEquals(25, metaB.getNumDocs());

        // Merge A+B
        Path mergedSplit = dir.resolve("merged.split");
        QuickwitSplit.MergeConfig mergeConfig = new QuickwitSplit.MergeConfig(
                "merge-test-index", "merge-test-source", "merge-test-node");

        QuickwitSplit.SplitMetadata mergedMeta = QuickwitSplit.mergeSplits(
                Arrays.asList(splitA.toString(), splitB.toString()),
                mergedSplit.toString(),
                mergeConfig);

        assertEquals(50, mergedMeta.getNumDocs());
        assertTrue(mergedSplit.toFile().exists());

        // Search the merged split
        String splitUrl = "file://" + mergedSplit.toAbsolutePath();
        try (SplitSearcher searcher = cacheManager.createSplitSearcher(splitUrl, mergedMeta, dir.toString())) {
            SplitQuery allQuery = searcher.parseQuery("*");
            SearchResult results = searcher.search(allQuery, 100);
            assertEquals(50, results.getHits().size());

            assertTrue(searcher.hasParquetCompanion(),
                    "Merged split should retain parquet companion manifest");

            // Verify doc values survive merge — check a doc from each original split
            SplitQuery queryA = new SplitTermQuery("name", "item_0");
            SearchResult hitsA = searcher.search(queryA, 1);
            assertTrue(hitsA.getHits().size() >= 1, "Should find item_0 from split A");
            Document docA = searcher.docProjected(hitsA.getHits().get(0).getDocAddress(),
                    "id", "name", "score");
            assertEquals(0L, ((Number) docA.getFirst("id")).longValue());
            assertEquals("item_0", docA.getFirst("name"));

            SplitQuery queryB = new SplitTermQuery("name", "item_30");
            SearchResult hitsB = searcher.search(queryB, 1);
            assertTrue(hitsB.getHits().size() >= 1, "Should find item_30 from split B");
            Document docB = searcher.docProjected(hitsB.getHits().get(0).getDocAddress(),
                    "id", "name", "score");
            assertEquals(30L, ((Number) docB.getFirst("id")).longValue());
            assertEquals("item_30", docB.getFirst("name"));
        }
    }

    // ---------------------------------------------------------------
    // 11. ParquetCompanionConfig JSON serialization
    // ---------------------------------------------------------------
    @Test
    @Order(11)
    void testConfigJsonSerialization() {
        Map<String, String> tokenizers = new HashMap<>();
        tokenizers.put("description", "en_stem");

        Map<String, String> fieldMapping = new HashMap<>();
        fieldMapping.put("col_1", "user_id");

        ParquetCompanionConfig config = new ParquetCompanionConfig("/data/table")
                .withFastFieldMode(ParquetCompanionConfig.FastFieldMode.HYBRID)
                .withStatisticsFields("id", "score")
                .withStatisticsTruncateLength(128)
                .withSkipFields("internal")
                .withTokenizerOverrides(tokenizers)
                .withFieldIdMapping(fieldMapping)
                .withAutoDetectNameMapping(true)
                .withIndexUid("test-uid")
                .withSourceId("test-source")
                .withNodeId("test-node");

        String json = config.toIndexingConfigJson();
        assertNotNull(json);
        assertTrue(json.contains("\"table_root\":\"/data/table\""));
        assertTrue(json.contains("\"fast_field_mode\":\"HYBRID\""));
        assertTrue(json.contains("\"statistics_truncate_length\":128"));
        assertTrue(json.contains("\"auto_detect_name_mapping\":true"));
        assertTrue(json.contains("\"index_uid\":\"test-uid\""));
        assertTrue(json.contains("\"statistics_fields\":[\"id\",\"score\"]"));
        assertTrue(json.contains("\"skip_fields\":[\"internal\"]"));
        assertTrue(json.contains("\"tokenizer_overrides\":{"));
        assertTrue(json.contains("\"field_id_mapping\":{"));
    }

    // ---------------------------------------------------------------
    // 12. ColumnStatistics overlap methods
    // ---------------------------------------------------------------
    @Test
    @Order(12)
    void testColumnStatisticsOverlapMethods() {
        ColumnStatistics stats = new ColumnStatistics("test_field", "I64");

        // Without min/max set, should conservatively return true
        assertTrue(stats.overlapsRange(0, 100));
        assertTrue(stats.overlapsDoubleRange(0.0, 100.0));
        assertTrue(stats.overlapsTimestampRange(0, 1000));

        assertEquals(0, stats.getNullCount());
    }

    // ---------------------------------------------------------------
    // 13. Complex data types: timestamp, list, struct, nullable
    // ---------------------------------------------------------------
    @Test
    @Order(13)
    void testComplexDataTypes(@TempDir Path dir) throws Exception {
        Path parquetFile = dir.resolve("complex.parquet");
        Path splitFile = dir.resolve("complex.split");

        writeTestParquetComplex(parquetFile.toString(), 20, 0);

        ParquetCompanionConfig config = new ParquetCompanionConfig(dir.toString())
                .withStatisticsFields("id", "score", "created_at", "name");

        QuickwitSplit.SplitMetadata metadata = QuickwitSplit.createFromParquet(
                Collections.singletonList(parquetFile.toString()),
                splitFile.toString(),
                config);

        assertEquals(20, metadata.getNumDocs());

        // Verify schema includes all types
        String splitUrl = "file://" + splitFile.toAbsolutePath();
        try (SplitSearcher searcher = cacheManager.createSplitSearcher(splitUrl, metadata, dir.toString())) {
            Schema schema = searcher.getSchema();
            List<String> fieldNames = schema.getFieldNames();
            assertTrue(fieldNames.contains("id"), "should have i64 field");
            assertTrue(fieldNames.contains("name"), "should have utf8 field");
            assertTrue(fieldNames.contains("score"), "should have f64 field");
            assertTrue(fieldNames.contains("active"), "should have bool field");
            assertTrue(fieldNames.contains("created_at"), "should have timestamp field");
            // tags (list) and address (struct) → mapped to JSON fields
            assertTrue(fieldNames.contains("tags"), "should have list→json field");
            assertTrue(fieldNames.contains("address"), "should have struct→json field");
            assertTrue(fieldNames.contains("notes"), "should have nullable utf8 field");

            // Search works
            SplitQuery allQuery = searcher.parseQuery("*");
            SearchResult results = searcher.search(allQuery, 50);
            assertEquals(20, results.getHits().size());

            assertTrue(searcher.hasParquetCompanion());
        }

        // Verify timestamp statistics
        Map<String, ColumnStatistics> stats = metadata.getColumnStatistics();
        ColumnStatistics tsStats = stats.get("created_at");
        assertNotNull(tsStats, "should have timestamp statistics");
        assertEquals("Date", tsStats.getFieldType());
    }

    // ---------------------------------------------------------------
    // 14. docProjected: single document retrieval with field projection
    // ---------------------------------------------------------------
    @Test
    @Order(14)
    void testDocProjected(@TempDir Path dir) throws Exception {
        Path parquetFile = dir.resolve("data.parquet");
        Path splitFile = dir.resolve("docproj.split");

        writeTestParquet(parquetFile.toString(), 10, 100);

        ParquetCompanionConfig config = new ParquetCompanionConfig(dir.toString());
        QuickwitSplit.SplitMetadata metadata = QuickwitSplit.createFromParquet(
                Collections.singletonList(parquetFile.toString()),
                splitFile.toString(), config);

        String splitUrl = "file://" + splitFile.toAbsolutePath();
        try (SplitSearcher searcher = cacheManager.createSplitSearcher(splitUrl, metadata, dir.toString())) {
            // Find a specific doc
            SplitQuery query = new SplitTermQuery("name", "item_100");
            SearchResult results = searcher.search(query, 1);
            assertTrue(results.getHits().size() >= 1, "should find item_100");

            // Retrieve with field projection
            Document doc = searcher.docProjected(results.getHits().get(0).getDocAddress(),
                    "id", "name");
            assertNotNull(doc, "docProjected should return Document");

            assertEquals(100L, ((Number) doc.getFirst("id")).longValue());
            assertEquals("item_100", doc.getFirst("name"));
            // score should not be present (not projected)
            assertNull(doc.getFirst("score"), "non-projected field should be absent");

            // Retrieve all fields (no projection)
            Document allDoc = searcher.docProjected(results.getHits().get(0).getDocAddress());
            assertNotNull(allDoc);
            assertNotNull(allDoc.getFirst("id"));
            assertNotNull(allDoc.getFirst("name"));
            assertNotNull(allDoc.getFirst("score"));
            assertNotNull(allDoc.getFirst("active"));
        }
    }

    // ---------------------------------------------------------------
    // 15. docProjected with complex types
    // ---------------------------------------------------------------
    @Test
    @Order(15)
    void testDocProjectedComplexTypes(@TempDir Path dir) throws Exception {
        Path parquetFile = dir.resolve("complex.parquet");
        Path splitFile = dir.resolve("docprojcx.split");

        writeTestParquetComplex(parquetFile.toString(), 10, 0);

        ParquetCompanionConfig config = new ParquetCompanionConfig(dir.toString());
        QuickwitSplit.SplitMetadata metadata = QuickwitSplit.createFromParquet(
                Collections.singletonList(parquetFile.toString()),
                splitFile.toString(), config);

        String splitUrl = "file://" + splitFile.toAbsolutePath();
        try (SplitSearcher searcher = cacheManager.createSplitSearcher(splitUrl, metadata, dir.toString())) {
            SplitQuery query = searcher.parseQuery("*");
            SearchResult results = searcher.search(query, 1);
            assertTrue(results.getHits().size() >= 1);

            // Retrieve all fields to validate complex type serialization
            Document doc = searcher.docProjected(results.getHits().get(0).getDocAddress());
            assertNotNull(doc);

            // Basic types
            assertNotNull(doc.getFirst("id"), "should have i64");
            assertNotNull(doc.getFirst("name"), "should have utf8");
            assertNotNull(doc.getFirst("score"), "should have f64");
            assertNotNull(doc.getFirst("active"), "should have bool");
            // Timestamp should be present
            assertNotNull(doc.getFirst("created_at"), "should have timestamp");
        }
    }

    // ---------------------------------------------------------------
    // 16. docBatchProjected: batch document retrieval
    // ---------------------------------------------------------------
    @Test
    @Order(16)
    void testDocBatchProjected(@TempDir Path dir) throws Exception {
        Path parquetFile = dir.resolve("data.parquet");
        Path splitFile = dir.resolve("batchproj.split");

        writeTestParquet(parquetFile.toString(), 20, 0);

        ParquetCompanionConfig config = new ParquetCompanionConfig(dir.toString());
        QuickwitSplit.SplitMetadata metadata = QuickwitSplit.createFromParquet(
                Collections.singletonList(parquetFile.toString()),
                splitFile.toString(), config);

        String splitUrl = "file://" + splitFile.toAbsolutePath();
        try (SplitSearcher searcher = cacheManager.createSplitSearcher(splitUrl, metadata, dir.toString())) {
            SplitQuery query = searcher.parseQuery("*");
            SearchResult results = searcher.search(query, 5);
            assertTrue(results.getHits().size() >= 3);

            // Collect doc addresses
            io.indextables.tantivy4java.core.DocAddress[] addrs =
                    new io.indextables.tantivy4java.core.DocAddress[3];
            for (int i = 0; i < 3; i++) {
                addrs[i] = results.getHits().get(i).getDocAddress();
            }

            // Batch retrieval with projection
            List<Document> batchDocs = searcher.docBatchProjected(addrs, "id", "name");
            assertEquals(3, batchDocs.size(), "should have 3 documents");

            for (Document batchDoc : batchDocs) {
                assertNotNull(batchDoc.getFirst("id"), "each doc should have id");
                assertNotNull(batchDoc.getFirst("name"), "each doc should have name");
                assertNull(batchDoc.getFirst("score"), "non-projected field should be absent");
            }
        }
    }

    // ---------------------------------------------------------------
    // 17. getParquetRetrievalStats
    // ---------------------------------------------------------------
    @Test
    @Order(17)
    void testGetParquetRetrievalStats(@TempDir Path dir) throws Exception {
        Path parquetFile = dir.resolve("data.parquet");
        Path splitFile = dir.resolve("stats.split");

        writeTestParquet(parquetFile.toString(), 30, 0);

        ParquetCompanionConfig config = new ParquetCompanionConfig(dir.toString());
        QuickwitSplit.SplitMetadata metadata = QuickwitSplit.createFromParquet(
                Collections.singletonList(parquetFile.toString()),
                splitFile.toString(), config);

        String splitUrl = "file://" + splitFile.toAbsolutePath();
        try (SplitSearcher searcher = cacheManager.createSplitSearcher(splitUrl, metadata, dir.toString())) {
            String statsJson = searcher.getParquetRetrievalStats();
            assertNotNull(statsJson, "stats should not be null for parquet companion split");

            JsonNode stats = MAPPER.readTree(statsJson);
            assertEquals(1, stats.get("totalFiles").asInt());
            assertEquals(30, stats.get("totalRows").asInt());
            assertTrue(stats.has("fastFieldMode"));
            assertTrue(stats.has("totalColumns"));
            assertTrue(stats.has("fileSizes"));
            assertTrue(stats.get("fileSizes").isArray());
            assertEquals(1, stats.get("fileSizes").size());
        }
    }

    // ---------------------------------------------------------------
    // 18. preloadParquetColumns
    // ---------------------------------------------------------------
    @Test
    @Order(18)
    void testPreloadParquetColumns(@TempDir Path dir) throws Exception {
        Path parquetFile = dir.resolve("data.parquet");
        Path splitFile = dir.resolve("preload.split");

        writeTestParquet(parquetFile.toString(), 20, 0);

        ParquetCompanionConfig config = new ParquetCompanionConfig(dir.toString());
        QuickwitSplit.SplitMetadata metadata = QuickwitSplit.createFromParquet(
                Collections.singletonList(parquetFile.toString()),
                splitFile.toString(), config);

        String splitUrl = "file://" + splitFile.toAbsolutePath();
        try (SplitSearcher searcher = cacheManager.createSplitSearcher(splitUrl, metadata, dir.toString())) {
            // Should not throw — pre-warms column pages into L2 cache
            searcher.preloadParquetColumns("id", "name").join();

            // Verify searcher still works after prewarm
            SplitQuery query = searcher.parseQuery("*");
            SearchResult results = searcher.search(query, 50);
            assertEquals(20, results.getHits().size());
        }
    }

    // ---------------------------------------------------------------
    // 19. preloadParquetFastFields
    // ---------------------------------------------------------------
    @Test
    @Order(19)
    void testPreloadParquetFastFields(@TempDir Path dir) throws Exception {
        Path parquetFile = dir.resolve("data.parquet");
        Path splitFile = dir.resolve("prewarmff.split");

        writeTestParquet(parquetFile.toString(), 20, 0);

        ParquetCompanionConfig config = new ParquetCompanionConfig(dir.toString())
                .withFastFieldMode(ParquetCompanionConfig.FastFieldMode.HYBRID);
        QuickwitSplit.SplitMetadata metadata = QuickwitSplit.createFromParquet(
                Collections.singletonList(parquetFile.toString()),
                splitFile.toString(), config);

        String splitUrl = "file://" + splitFile.toAbsolutePath();
        try (SplitSearcher searcher = cacheManager.createSplitSearcher(splitUrl, metadata, dir.toString())) {
            // Should not throw — transcodes parquet to fast fields
            searcher.preloadParquetFastFields("id", "score").join();

            // Verify searcher still works after fast field prewarm
            SplitQuery query = searcher.parseQuery("*");
            SearchResult results = searcher.search(query, 50);
            assertEquals(20, results.getHits().size());
        }
    }

    // ---------------------------------------------------------------
    // 20. Complex type statistics: timestamp
    // ---------------------------------------------------------------
    @Test
    @Order(20)
    void testTimestampStatistics(@TempDir Path dir) throws Exception {
        Path parquetFile = dir.resolve("complex.parquet");
        Path splitFile = dir.resolve("tsstats.split");

        writeTestParquetComplex(parquetFile.toString(), 10, 0);

        ParquetCompanionConfig config = new ParquetCompanionConfig(dir.toString())
                .withStatisticsFields("id", "created_at", "active");

        QuickwitSplit.SplitMetadata metadata = QuickwitSplit.createFromParquet(
                Collections.singletonList(parquetFile.toString()),
                splitFile.toString(), config);

        Map<String, ColumnStatistics> stats = metadata.getColumnStatistics();

        // id stats
        ColumnStatistics idStats = stats.get("id");
        assertNotNull(idStats);
        assertEquals(0L, idStats.getMinLong());
        assertEquals(9L, idStats.getMaxLong());

        // Timestamp stats: base = 2024-01-01T00:00:00Z = 1704067200000000 micros
        // Row 0: base, Row 9: base + 9*3600000000
        ColumnStatistics tsStats = stats.get("created_at");
        assertNotNull(tsStats, "should have timestamp statistics");
        assertEquals("Date", tsStats.getFieldType());
        long baseMicros = 1704067200_000_000L;
        assertEquals(baseMicros, tsStats.getMinTimestampMicros());
        assertEquals(baseMicros + 9 * 3_600_000_000L, tsStats.getMaxTimestampMicros());
        assertTrue(tsStats.overlapsTimestampRange(baseMicros, baseMicros + 5 * 3_600_000_000L));
        assertFalse(tsStats.overlapsTimestampRange(0, 1000));

        // Boolean stats
        ColumnStatistics boolStats = stats.get("active");
        assertNotNull(boolStats, "should have boolean statistics");
        assertEquals("Bool", boolStats.getFieldType());
    }

    // ---------------------------------------------------------------
    // 21. IP address field support — create, search, verify schema
    // ---------------------------------------------------------------
    @Test
    @Order(21)
    void testIpAddressFields(@TempDir Path dir) throws Exception {
        Path parquetFile = dir.resolve("ips.parquet");
        QuickwitSplit.nativeWriteTestParquetWithIps(parquetFile.toString(), 50, 0);

        ParquetCompanionConfig config = new ParquetCompanionConfig(dir.toString())
                .withFastFieldMode(ParquetCompanionConfig.FastFieldMode.HYBRID)
                .withIpAddressFields("src_ip", "dst_ip");

        QuickwitSplit.SplitMetadata metadata = QuickwitSplit.createFromParquet(
                Collections.singletonList(parquetFile.toString()),
                dir.resolve("ips.split").toString(),
                config);

        assertNotNull(metadata);
        assertEquals(50, metadata.getNumDocs());

        // Search the split with IP fields
        try (SplitSearcher searcher = cacheManager.createSplitSearcher(
                "file://" + dir.resolve("ips.split"), metadata, dir.toString())) {

            Schema schema = searcher.getSchema();
            assertNotNull(schema);

            // Verify all fields are present
            assertTrue(schema.hasField("id"));
            assertTrue(schema.hasField("src_ip"));
            assertTrue(schema.hasField("dst_ip"));
            assertTrue(schema.hasField("port"));
            assertTrue(schema.hasField("label"));

            // Search for all docs
            SearchResult allResult = searcher.search(
                    searcher.parseQuery("*"), 10);
            assertNotNull(allResult);
            assertTrue(allResult.getHits().size() > 0,
                    "Should find docs with match-all query");

            System.out.println("[IP test] Found " + allResult.getHits().size() +
                    " hits with match-all query");
        }
    }

    // ---------------------------------------------------------------
    // 22. IP address field with DISABLED mode (all native fast fields)
    // ---------------------------------------------------------------
    @Test
    @Order(22)
    void testIpAddressFieldsDisabledMode(@TempDir Path dir) throws Exception {
        Path parquetFile = dir.resolve("ips_disabled.parquet");
        QuickwitSplit.nativeWriteTestParquetWithIps(parquetFile.toString(), 30, 0);

        ParquetCompanionConfig config = new ParquetCompanionConfig(dir.toString())
                .withFastFieldMode(ParquetCompanionConfig.FastFieldMode.DISABLED)
                .withIpAddressFields("src_ip", "dst_ip");

        QuickwitSplit.SplitMetadata metadata = QuickwitSplit.createFromParquet(
                Collections.singletonList(parquetFile.toString()),
                dir.resolve("ips_disabled.split").toString(),
                config);

        assertEquals(30, metadata.getNumDocs());

        try (SplitSearcher searcher = cacheManager.createSplitSearcher(
                "file://" + dir.resolve("ips_disabled.split"), metadata, dir.toString())) {
            SearchResult result = searcher.search(searcher.parseQuery("*"), 5);
            assertTrue(result.getHits().size() > 0);
            System.out.println("[IP DISABLED test] Found " + result.getHits().size() + " hits");
        }
    }

    // ---------------------------------------------------------------
    // 23. Coalesce max gap config: single + batch doc retrieval
    // ---------------------------------------------------------------
    @Test
    @Order(23)
    void testCoalesceMaxGapConfig(@TempDir Path dir) throws Exception {
        Path parquetFile = dir.resolve("data.parquet");
        Path splitFile = dir.resolve("coalesce.split");

        writeTestParquet(parquetFile.toString(), 20, 0);

        ParquetCompanionConfig config = new ParquetCompanionConfig(dir.toString());
        QuickwitSplit.SplitMetadata metadata = QuickwitSplit.createFromParquet(
                Collections.singletonList(parquetFile.toString()),
                splitFile.toString(), config);

        // Create a cache manager with a small coalesce max gap (1KB)
        SplitCacheManager.CacheConfig coalesceConfig =
                new SplitCacheManager.CacheConfig("parquet-coalesce-test")
                        .withMaxCacheSize(100_000_000)
                        .withCoalesceMaxGap(1024);
        try (SplitCacheManager coalesceMgr = SplitCacheManager.getInstance(coalesceConfig)) {
            String splitUrl = "file://" + splitFile.toAbsolutePath();
            try (SplitSearcher searcher = coalesceMgr.createSplitSearcher(splitUrl, metadata, dir.toString())) {
                // Single doc retrieval
                SplitQuery query = searcher.parseQuery("*");
                SearchResult results = searcher.search(query, 5);
                assertTrue(results.getHits().size() >= 3);

                Document singleDoc = searcher.docProjected(
                        results.getHits().get(0).getDocAddress(), "id", "name");
                assertNotNull(singleDoc.getFirst("id"), "single doc should have id");
                assertNotNull(singleDoc.getFirst("name"), "single doc should have name");

                // Batch doc retrieval
                io.indextables.tantivy4java.core.DocAddress[] addrs =
                        new io.indextables.tantivy4java.core.DocAddress[3];
                for (int i = 0; i < 3; i++) {
                    addrs[i] = results.getHits().get(i).getDocAddress();
                }
                List<Document> batchDocs = searcher.docBatchProjected(addrs, "id", "name");
                assertEquals(3, batchDocs.size(), "batch should return 3 documents");
                for (Document doc : batchDocs) {
                    assertNotNull(doc.getFirst("id"), "batch doc should have id");
                    assertNotNull(doc.getFirst("name"), "batch doc should have name");
                    assertNull(doc.getFirst("score"), "non-projected field should be absent");
                }
            }
        }
    }
}

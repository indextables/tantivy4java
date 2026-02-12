package io.indextables.tantivy4java;

import io.indextables.tantivy4java.aggregation.HistogramAggregation;
import io.indextables.tantivy4java.aggregation.StatsAggregation;
import io.indextables.tantivy4java.aggregation.TermsAggregation;
import io.indextables.tantivy4java.core.DocAddress;
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
 * Comprehensive all-data-types test for Parquet Companion Mode.
 *
 * Uses nativeWriteTestParquetAllTypes which generates:
 *   id (i64), uint_val (u64), float_val (f64), bool_val (bool),
 *   text_val (utf8), binary_val (binary), ts_val (timestamp_us),
 *   date_val (date32), ip_val (utf8→ip), tags (list→json),
 *   address (struct→json), props (map→json)
 *
 * Tests all FastFieldModes, all data types for retrieval, aggregation, statistics,
 * and byte-buffer batch retrieval.
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class ParquetCompanionAllTypesTest {

    private static SplitCacheManager cacheManager;
    private static final ObjectMapper MAPPER = new ObjectMapper();

    @BeforeAll
    static void setUp() {
        SplitCacheManager.CacheConfig config = new SplitCacheManager.CacheConfig(
                "pq-all-types-test-" + System.nanoTime())
                .withMaxCacheSize(100_000_000);
        cacheManager = SplitCacheManager.getInstance(config);
    }

    @AfterAll
    static void tearDown() {
        if (cacheManager != null) {
            try { cacheManager.close(); } catch (Exception e) { /* ignore */ }
        }
    }

    private static void writeAllTypes(String path, int numRows, long idOffset) {
        QuickwitSplit.nativeWriteTestParquetAllTypes(path, numRows, idOffset);
    }

    // Helper: create split from all-types parquet with given mode and IP config
    private QuickwitSplit.SplitMetadata createAllTypesSplit(
            Path dir, String splitName, int numRows, long idOffset,
            ParquetCompanionConfig.FastFieldMode mode) throws Exception {
        Path parquetFile = dir.resolve("all_types.parquet");
        Path splitFile = dir.resolve(splitName);
        writeAllTypes(parquetFile.toString(), numRows, idOffset);

        ParquetCompanionConfig config = new ParquetCompanionConfig(dir.toString())
                .withFastFieldMode(mode)
                .withIpAddressFields("ip_val", "src_ip")
                .withStatisticsFields("id", "uint_val", "float_val", "bool_val",
                        "text_val", "ts_val", "status_code", "amount", "event_time");

        return QuickwitSplit.createFromParquet(
                Collections.singletonList(parquetFile.toString()),
                splitFile.toString(), config);
    }

    // ---------------------------------------------------------------
    // 1. All-types split creation and schema validation (DISABLED)
    // ---------------------------------------------------------------
    @Test
    @Order(1)
    void testAllTypesCreationDisabled(@TempDir Path dir) throws Exception {
        QuickwitSplit.SplitMetadata metadata = createAllTypesSplit(
                dir, "disabled.split", 50, 0,
                ParquetCompanionConfig.FastFieldMode.DISABLED);

        assertNotNull(metadata);
        assertEquals(50, metadata.getNumDocs());

        String splitUrl = "file://" + dir.resolve("disabled.split").toAbsolutePath();
        try (SplitSearcher searcher = cacheManager.createSplitSearcher(splitUrl, metadata, dir.toString())) {
            Schema schema = searcher.getSchema();
            List<String> fields = schema.getFieldNames();

            // All types should be present
            assertTrue(fields.contains("id"), "i64 field");
            assertTrue(fields.contains("uint_val"), "u64 field");
            assertTrue(fields.contains("float_val"), "f64 field");
            assertTrue(fields.contains("bool_val"), "bool field");
            assertTrue(fields.contains("text_val"), "text field");
            assertTrue(fields.contains("binary_val"), "bytes field");
            assertTrue(fields.contains("ts_val"), "timestamp field");
            assertTrue(fields.contains("date_val"), "date32 field");
            assertTrue(fields.contains("ip_val"), "ip address field");
            assertTrue(fields.contains("tags"), "list→json field");
            assertTrue(fields.contains("address"), "struct→json field");
            assertTrue(fields.contains("props"), "map→json field");

            // Basic search
            SplitQuery allQuery = searcher.parseQuery("*");
            SearchResult allResults = searcher.search(allQuery, 100);
            assertEquals(50, allResults.getHits().size());
        }
    }

    // ---------------------------------------------------------------
    // 2. All-types split creation (HYBRID)
    // ---------------------------------------------------------------
    @Test
    @Order(2)
    void testAllTypesCreationHybrid(@TempDir Path dir) throws Exception {
        QuickwitSplit.SplitMetadata metadata = createAllTypesSplit(
                dir, "hybrid.split", 50, 0,
                ParquetCompanionConfig.FastFieldMode.HYBRID);

        assertNotNull(metadata);
        assertEquals(50, metadata.getNumDocs());

        String splitUrl = "file://" + dir.resolve("hybrid.split").toAbsolutePath();
        try (SplitSearcher searcher = cacheManager.createSplitSearcher(splitUrl, metadata, dir.toString())) {
            SplitQuery allQuery = searcher.parseQuery("*");
            SearchResult allResults = searcher.search(allQuery, 100);
            assertEquals(50, allResults.getHits().size());
        }
    }

    // ---------------------------------------------------------------
    // 3. All-types split creation (PARQUET_ONLY)
    // ---------------------------------------------------------------
    @Test
    @Order(3)
    void testAllTypesCreationParquetOnly(@TempDir Path dir) throws Exception {
        QuickwitSplit.SplitMetadata metadata = createAllTypesSplit(
                dir, "pqonly.split", 50, 0,
                ParquetCompanionConfig.FastFieldMode.PARQUET_ONLY);

        assertNotNull(metadata);
        assertEquals(50, metadata.getNumDocs());

        String splitUrl = "file://" + dir.resolve("pqonly.split").toAbsolutePath();
        try (SplitSearcher searcher = cacheManager.createSplitSearcher(splitUrl, metadata, dir.toString())) {
            SplitQuery allQuery = searcher.parseQuery("*");
            SearchResult allResults = searcher.search(allQuery, 100);
            assertEquals(50, allResults.getHits().size());
        }
    }

    // ---------------------------------------------------------------
    // 4. Document retrieval with ALL data types (single doc)
    // ---------------------------------------------------------------
    @Test
    @Order(4)
    void testAllTypesDocRetrieval(@TempDir Path dir) throws Exception {
        QuickwitSplit.SplitMetadata metadata = createAllTypesSplit(
                dir, "retrieve.split", 20, 0,
                ParquetCompanionConfig.FastFieldMode.DISABLED);

        String splitUrl = "file://" + dir.resolve("retrieve.split").toAbsolutePath();
        try (SplitSearcher searcher = cacheManager.createSplitSearcher(splitUrl, metadata, dir.toString())) {
            // Search for first doc
            SplitQuery query = new SplitTermQuery("text_val", "text_0");
            SearchResult results = searcher.search(query, 1);
            assertTrue(results.getHits().size() >= 1);

            DocAddress addr = results.getHits().get(0).getDocAddress();

            // Retrieve doc with all fields (no projection = all fields)
            Document doc = searcher.docProjected(addr);
            assertNotNull(doc);

            // Validate i64
            assertNotNull(doc.getFirst("id"), "should have id (i64)");
            assertEquals(0L, ((Number) doc.getFirst("id")).longValue());

            // Validate u64
            assertNotNull(doc.getFirst("uint_val"), "should have uint_val (u64)");
            assertEquals(1_000_000_000L, ((Number) doc.getFirst("uint_val")).longValue());

            // Validate text
            assertNotNull(doc.getFirst("text_val"), "should have text_val");
            assertEquals("text_0", (String) doc.getFirst("text_val"));

            // Validate bool (row 0 → true)
            assertNotNull(doc.getFirst("bool_val"), "should have bool_val");

            // Validate ip
            assertNotNull(doc.getFirst("ip_val"), "should have ip_val");
        }
    }

    // ---------------------------------------------------------------
    // 5. Byte-buffer batch retrieval with ALL data types
    // ---------------------------------------------------------------
    @Test
    @Order(5)
    void testAllTypesBatchRetrieval(@TempDir Path dir) throws Exception {
        QuickwitSplit.SplitMetadata metadata = createAllTypesSplit(
                dir, "batch.split", 30, 0,
                ParquetCompanionConfig.FastFieldMode.DISABLED);

        String splitUrl = "file://" + dir.resolve("batch.split").toAbsolutePath();
        try (SplitSearcher searcher = cacheManager.createSplitSearcher(splitUrl, metadata, dir.toString())) {
            SplitQuery query = searcher.parseQuery("*");
            SearchResult results = searcher.search(query, 10);
            assertTrue(results.getHits().size() >= 5);

            // Collect 5 doc addresses
            DocAddress[] addrs = new DocAddress[5];
            for (int i = 0; i < 5; i++) {
                addrs[i] = results.getHits().get(i).getDocAddress();
            }

            // Batch retrieval with ALL fields projected
            List<Document> docs = searcher.docBatchProjected(addrs,
                    "id", "uint_val", "float_val", "bool_val", "text_val",
                    "binary_val", "ts_val", "date_val", "ip_val",
                    "tags", "address", "props");
            assertEquals(5, docs.size(), "should have 5 documents");

            // Verify each doc has all projected fields (that have non-null values)
            for (int i = 0; i < docs.size(); i++) {
                Document doc = docs.get(i);
                assertNotNull(doc.getFirst("id"), "doc " + i + " should have id (i64)");
                assertNotNull(doc.getFirst("uint_val"), "doc " + i + " should have uint_val (u64)");
                assertNotNull(doc.getFirst("text_val"), "doc " + i + " should have text_val");
                assertNotNull(doc.getFirst("ip_val"), "doc " + i + " should have ip_val");
                // Note: nullable fields may be absent if null in the source
            }
        }
    }

    // ---------------------------------------------------------------
    // 6. Batch retrieval with selective projection
    // ---------------------------------------------------------------
    @Test
    @Order(6)
    void testBatchRetrievalSelectiveProjection(@TempDir Path dir) throws Exception {
        QuickwitSplit.SplitMetadata metadata = createAllTypesSplit(
                dir, "batch_proj.split", 20, 0,
                ParquetCompanionConfig.FastFieldMode.DISABLED);

        String splitUrl = "file://" + dir.resolve("batch_proj.split").toAbsolutePath();
        try (SplitSearcher searcher = cacheManager.createSplitSearcher(splitUrl, metadata, dir.toString())) {
            SplitQuery query = searcher.parseQuery("*");
            SearchResult results = searcher.search(query, 5);
            assertTrue(results.getHits().size() >= 3);

            DocAddress[] addrs = new DocAddress[3];
            for (int i = 0; i < 3; i++) {
                addrs[i] = results.getHits().get(i).getDocAddress();
            }

            // Only project id and ip_val
            List<Document> docs = searcher.docBatchProjected(addrs, "id", "ip_val");
            assertEquals(3, docs.size());

            for (int i = 0; i < docs.size(); i++) {
                Document doc = docs.get(i);
                assertNotNull(doc.getFirst("id"), "should have projected field id");
                assertNotNull(doc.getFirst("ip_val"), "should have projected field ip_val");
                // Non-projected fields should be absent
                assertNull(doc.getFirst("text_val"), "non-projected field should be absent");
                assertNull(doc.getFirst("uint_val"), "non-projected field should be absent");
            }
        }
    }

    // ---------------------------------------------------------------
    // 7. Column statistics for all types
    // ---------------------------------------------------------------
    @Test
    @Order(7)
    void testAllTypesStatistics(@TempDir Path dir) throws Exception {
        Path parquetFile = dir.resolve("stats.parquet");
        Path splitFile = dir.resolve("stats.split");
        writeAllTypes(parquetFile.toString(), 100, 0);

        ParquetCompanionConfig config = new ParquetCompanionConfig(dir.toString())
                .withIpAddressFields("ip_val")
                .withStatisticsFields("id", "uint_val", "float_val", "bool_val",
                        "text_val", "ts_val")
                .withStatisticsTruncateLength(10);

        QuickwitSplit.SplitMetadata metadata = QuickwitSplit.createFromParquet(
                Collections.singletonList(parquetFile.toString()),
                splitFile.toString(), config);

        Map<String, ColumnStatistics> stats = metadata.getColumnStatistics();
        assertNotNull(stats);

        // I64 stats
        ColumnStatistics idStats = stats.get("id");
        assertNotNull(idStats, "should have id stats");
        assertEquals(0, idStats.getMinLong());
        assertEquals(99, idStats.getMaxLong());
        assertEquals(0, idStats.getNullCount());

        // F64 stats (NaN on every 10th, null on every 7th)
        ColumnStatistics floatStats = stats.get("float_val");
        assertNotNull(floatStats, "should have float_val stats");
        assertTrue(floatStats.getNullCount() > 0, "should have some nulls");
        assertTrue(floatStats.getMinDouble() >= 0.0);

        // Bool stats
        ColumnStatistics boolStats = stats.get("bool_val");
        assertNotNull(boolStats, "should have bool_val stats");

        // String stats with truncation
        ColumnStatistics textStats = stats.get("text_val");
        assertNotNull(textStats, "should have text_val stats");
        assertNotNull(textStats.getMinString());
        assertTrue(textStats.getMinString().length() <= 10,
                "min string should be truncated to 10 chars");

        // Timestamp stats
        ColumnStatistics tsStats = stats.get("ts_val");
        assertNotNull(tsStats, "should have ts_val stats");
        assertTrue(tsStats.getMinTimestampMicros() > 0);
        assertTrue(tsStats.getMaxTimestampMicros() > tsStats.getMinTimestampMicros());
        assertTrue(tsStats.getNullCount() > 0, "ts has nulls every 8th row");
    }

    // ---------------------------------------------------------------
    // 8. Stats aggregation on i64 across all modes
    // ---------------------------------------------------------------
    @Test
    @Order(8)
    void testStatsAggregationI64AllModes(@TempDir Path dir) throws Exception {
        for (ParquetCompanionConfig.FastFieldMode mode :
                ParquetCompanionConfig.FastFieldMode.values()) {
            Path parquetFile = dir.resolve("agg_" + mode.name() + ".parquet");
            Path splitFile = dir.resolve("agg_" + mode.name() + ".split");
            writeAllTypes(parquetFile.toString(), 50, 0);

            ParquetCompanionConfig config = new ParquetCompanionConfig(dir.toString())
                    .withFastFieldMode(mode)
                    .withIpAddressFields("ip_val");

            QuickwitSplit.SplitMetadata metadata = QuickwitSplit.createFromParquet(
                    Collections.singletonList(parquetFile.toString()),
                    splitFile.toString(), config);

            String splitUrl = "file://" + splitFile.toAbsolutePath();
            try (SplitSearcher searcher = cacheManager.createSplitSearcher(splitUrl, metadata, dir.toString())) {
                if (mode != ParquetCompanionConfig.FastFieldMode.DISABLED) {
                    searcher.preloadParquetFastFields("id").join();
                }

                StatsAggregation agg = new StatsAggregation("id_stats", "id");
                SearchResult result = searcher.search(new SplitMatchAllQuery(), 0, "id_stats", agg);
                assertTrue(result.hasAggregations(),
                        mode.name() + " mode: stats aggregation should return results");
            }
        }
    }

    // ---------------------------------------------------------------
    // 9. IP address fields in PARQUET_ONLY mode
    // ---------------------------------------------------------------
    @Test
    @Order(9)
    void testIpAddressFieldsParquetOnly(@TempDir Path dir) throws Exception {
        QuickwitSplit.SplitMetadata metadata = createAllTypesSplit(
                dir, "ip_pqonly.split", 30, 0,
                ParquetCompanionConfig.FastFieldMode.PARQUET_ONLY);

        String splitUrl = "file://" + dir.resolve("ip_pqonly.split").toAbsolutePath();
        try (SplitSearcher searcher = cacheManager.createSplitSearcher(splitUrl, metadata, dir.toString())) {
            Schema schema = searcher.getSchema();
            assertTrue(schema.getFieldNames().contains("ip_val"));

            // Search should work
            SplitQuery query = searcher.parseQuery("*");
            SearchResult results = searcher.search(query, 10);
            assertTrue(results.getHits().size() > 0);
        }
    }

    // ---------------------------------------------------------------
    // 10. Merge 3 all-types splits
    // ---------------------------------------------------------------
    @Test
    @Order(10)
    void testMergeThreeAllTypesSplits(@TempDir Path dir) throws Exception {
        List<String> splitPaths = new ArrayList<>();
        for (int i = 0; i < 3; i++) {
            Path pq = dir.resolve("merge_" + i + ".parquet");
            Path sp = dir.resolve("merge_" + i + ".split");
            writeAllTypes(pq.toString(), 20, i * 20);

            ParquetCompanionConfig config = new ParquetCompanionConfig(dir.toString())
                    .withIpAddressFields("ip_val");
            QuickwitSplit.createFromParquet(
                    Collections.singletonList(pq.toString()),
                    sp.toString(), config);
            splitPaths.add(sp.toString());
        }

        Path mergedFile = dir.resolve("merged.split");
        QuickwitSplit.MergeConfig mergeConfig = new QuickwitSplit.MergeConfig(
                "test-idx", "test-src", "test-node");
        QuickwitSplit.SplitMetadata merged = QuickwitSplit.mergeSplits(
                splitPaths, mergedFile.toString(), mergeConfig);

        assertNotNull(merged);
        assertEquals(60, merged.getNumDocs(), "3 splits × 20 rows = 60 total");

        // Search the merged split
        String splitUrl = "file://" + mergedFile.toAbsolutePath();
        try (SplitSearcher searcher = cacheManager.createSplitSearcher(splitUrl, merged, dir.toString())) {
            SplitQuery allQuery = searcher.parseQuery("*");
            SearchResult results = searcher.search(allQuery, 100);
            assertEquals(60, results.getHits().size());
        }
    }

    // ---------------------------------------------------------------
    // 11. Prewarm fast fields in PARQUET_ONLY mode
    // ---------------------------------------------------------------
    @Test
    @Order(11)
    void testPrewarmParquetOnly(@TempDir Path dir) throws Exception {
        QuickwitSplit.SplitMetadata metadata = createAllTypesSplit(
                dir, "prewarm_pq.split", 30, 0,
                ParquetCompanionConfig.FastFieldMode.PARQUET_ONLY);

        String splitUrl = "file://" + dir.resolve("prewarm_pq.split").toAbsolutePath();
        try (SplitSearcher searcher = cacheManager.createSplitSearcher(splitUrl, metadata, dir.toString())) {
            // Prewarm in PARQUET_ONLY — all fast fields come from parquet
            searcher.preloadParquetFastFields("id", "text_val").join();

            StatsAggregation agg = new StatsAggregation("id_stats", "id");
            SearchResult result = searcher.search(new SplitMatchAllQuery(), 0, "id_stats", agg);
            assertTrue(result.hasAggregations(), "prewarm PARQUET_ONLY should produce aggregation");
        }
    }

    // ---------------------------------------------------------------
    // 12. Batch retrieval includes complex types (JSON: tags, address, props)
    // ---------------------------------------------------------------
    @Test
    @Order(12)
    void testBatchRetrievalComplexTypes(@TempDir Path dir) throws Exception {
        QuickwitSplit.SplitMetadata metadata = createAllTypesSplit(
                dir, "batch_complex.split", 20, 0,
                ParquetCompanionConfig.FastFieldMode.DISABLED);

        String splitUrl = "file://" + dir.resolve("batch_complex.split").toAbsolutePath();
        try (SplitSearcher searcher = cacheManager.createSplitSearcher(splitUrl, metadata, dir.toString())) {
            SplitQuery query = searcher.parseQuery("*");
            SearchResult results = searcher.search(query, 5);

            DocAddress[] addrs = new DocAddress[Math.min(3, results.getHits().size())];
            for (int i = 0; i < addrs.length; i++) {
                addrs[i] = results.getHits().get(i).getDocAddress();
            }

            // Batch retrieval including complex/JSON fields
            List<Document> docs = searcher.docBatchProjected(addrs,
                    "id", "tags", "address", "props");
            assertEquals(addrs.length, docs.size());

            // At least some docs should have JSON complex type data
            boolean foundTags = false;
            boolean foundAddress = false;
            boolean foundProps = false;
            for (int i = 0; i < docs.size(); i++) {
                Document doc = docs.get(i);
                if (doc.getFirst("tags") != null) foundTags = true;
                if (doc.getFirst("address") != null) foundAddress = true;
                if (doc.getFirst("props") != null) foundProps = true;
            }
            // tags, address, props are JSON fields — they're stored in tantivy for JSON types
            // At least tags and address should be present for most rows
            assertTrue(foundTags || foundAddress || foundProps,
                    "at least one complex type should be retrievable");
        }
    }

    // ---------------------------------------------------------------
    // 13. Batch retrieval with binary data
    // ---------------------------------------------------------------
    @Test
    @Order(13)
    void testBatchRetrievalBinaryField(@TempDir Path dir) throws Exception {
        QuickwitSplit.SplitMetadata metadata = createAllTypesSplit(
                dir, "batch_bin.split", 20, 0,
                ParquetCompanionConfig.FastFieldMode.DISABLED);

        String splitUrl = "file://" + dir.resolve("batch_bin.split").toAbsolutePath();
        try (SplitSearcher searcher = cacheManager.createSplitSearcher(splitUrl, metadata, dir.toString())) {
            SplitQuery query = searcher.parseQuery("*");
            SearchResult results = searcher.search(query, 5);

            DocAddress[] addrs = new DocAddress[Math.min(3, results.getHits().size())];
            for (int i = 0; i < addrs.length; i++) {
                addrs[i] = results.getHits().get(i).getDocAddress();
            }

            // Batch retrieval with binary field
            List<Document> docs = searcher.docBatchProjected(addrs, "id", "binary_val");
            assertEquals(addrs.length, docs.size());

            for (int i = 0; i < docs.size(); i++) {
                Document doc = docs.get(i);
                assertNotNull(doc.getFirst("id"));
                // binary_val may be null for some rows (null every 4th)
            }
        }
    }

    // ---------------------------------------------------------------
    // 14. Batch retrieval with timestamp and date fields
    // ---------------------------------------------------------------
    @Test
    @Order(14)
    void testBatchRetrievalDateTimeFields(@TempDir Path dir) throws Exception {
        QuickwitSplit.SplitMetadata metadata = createAllTypesSplit(
                dir, "batch_ts.split", 20, 0,
                ParquetCompanionConfig.FastFieldMode.DISABLED);

        String splitUrl = "file://" + dir.resolve("batch_ts.split").toAbsolutePath();
        try (SplitSearcher searcher = cacheManager.createSplitSearcher(splitUrl, metadata, dir.toString())) {
            SplitQuery query = searcher.parseQuery("*");
            SearchResult results = searcher.search(query, 5);

            DocAddress[] addrs = new DocAddress[Math.min(3, results.getHits().size())];
            for (int i = 0; i < addrs.length; i++) {
                addrs[i] = results.getHits().get(i).getDocAddress();
            }

            List<Document> docs = searcher.docBatchProjected(addrs, "id", "ts_val", "date_val");
            assertEquals(addrs.length, docs.size());

            for (int i = 0; i < docs.size(); i++) {
                Document doc = docs.get(i);
                assertNotNull(doc.getFirst("id"));
                // ts_val and date_val are nullable — verify they can be present
            }
        }
    }

    // ---------------------------------------------------------------
    // 15. Multi-file statistics validation
    // ---------------------------------------------------------------
    @Test
    @Order(15)
    void testMultiFileStatisticsAcrossBoundaries(@TempDir Path dir) throws Exception {
        Path pq1 = dir.resolve("file1.parquet");
        Path pq2 = dir.resolve("file2.parquet");
        Path splitFile = dir.resolve("multistat.split");

        // File 1: ids 0-19, File 2: ids 100-119 (distinct ranges)
        writeAllTypes(pq1.toString(), 20, 0);
        writeAllTypes(pq2.toString(), 20, 100);

        ParquetCompanionConfig config = new ParquetCompanionConfig(dir.toString())
                .withIpAddressFields("ip_val")
                .withStatisticsFields("id");

        QuickwitSplit.SplitMetadata metadata = QuickwitSplit.createFromParquet(
                Arrays.asList(pq1.toString(), pq2.toString()),
                splitFile.toString(), config);

        assertEquals(40, metadata.getNumDocs());

        // Stats should span both files
        Map<String, ColumnStatistics> stats = metadata.getColumnStatistics();
        ColumnStatistics idStats = stats.get("id");
        assertNotNull(idStats);
        assertEquals(0, idStats.getMinLong(), "min should be from file 1");
        assertEquals(119, idStats.getMaxLong(), "max should be from file 2");
    }

    // ---------------------------------------------------------------
    // 16. Field ID mapping end-to-end
    // ---------------------------------------------------------------
    @Test
    @Order(16)
    void testFieldIdMapping(@TempDir Path dir) throws Exception {
        Path parquetFile = dir.resolve("mapping.parquet");
        Path splitFile = dir.resolve("mapping.split");

        // Write simple parquet
        QuickwitSplit.nativeWriteTestParquet(parquetFile.toString(), 20, 0);

        // Map parquet column names to custom field names
        Map<String, String> mapping = new HashMap<>();
        mapping.put("name", "product_name");  // rename "name" → "product_name"

        ParquetCompanionConfig config = new ParquetCompanionConfig(dir.toString())
                .withFieldIdMapping(mapping);

        QuickwitSplit.SplitMetadata metadata = QuickwitSplit.createFromParquet(
                Collections.singletonList(parquetFile.toString()),
                splitFile.toString(), config);

        String splitUrl = "file://" + splitFile.toAbsolutePath();
        try (SplitSearcher searcher = cacheManager.createSplitSearcher(splitUrl, metadata, dir.toString())) {
            Schema schema = searcher.getSchema();
            List<String> fields = schema.getFieldNames();

            // The renamed field should exist
            assertTrue(fields.contains("product_name"),
                    "mapped field 'product_name' should exist, got: " + fields);
            // Original name should NOT exist (it was remapped)
            assertFalse(fields.contains("name"),
                    "original field 'name' should be renamed");
        }
    }

    // ---------------------------------------------------------------
    // 17. Histogram aggregation on u64 field (HYBRID mode)
    // ---------------------------------------------------------------
    @Test
    @Order(17)
    void testHistogramAggregationOnU64(@TempDir Path dir) throws Exception {
        QuickwitSplit.SplitMetadata metadata = createAllTypesSplit(
                dir, "hist_u64.split", 50, 0,
                ParquetCompanionConfig.FastFieldMode.HYBRID);

        String splitUrl = "file://" + dir.resolve("hist_u64.split").toAbsolutePath();
        try (SplitSearcher searcher = cacheManager.createSplitSearcher(splitUrl, metadata, dir.toString())) {
            searcher.preloadParquetFastFields("uint_val").join();

            HistogramAggregation agg = new HistogramAggregation("uint_hist", "uint_val", 100);
            SearchResult result = searcher.search(new SplitMatchAllQuery(), 0, "uint_hist", agg);
            assertTrue(result.hasAggregations(),
                    "u64 histogram should return results");
        }
    }

    // ---------------------------------------------------------------
    // 18. Terms aggregation on text_val (all modes)
    // ---------------------------------------------------------------
    @Test
    @Order(18)
    void testTermsAggregationTextAllModes(@TempDir Path dir) throws Exception {
        for (ParquetCompanionConfig.FastFieldMode mode :
                ParquetCompanionConfig.FastFieldMode.values()) {
            Path pq = dir.resolve("terms_" + mode.name() + ".parquet");
            Path sp = dir.resolve("terms_" + mode.name() + ".split");
            writeAllTypes(pq.toString(), 30, 0);

            ParquetCompanionConfig config = new ParquetCompanionConfig(dir.toString())
                    .withFastFieldMode(mode)
                    .withIpAddressFields("ip_val");

            QuickwitSplit.SplitMetadata meta = QuickwitSplit.createFromParquet(
                    Collections.singletonList(pq.toString()), sp.toString(), config);

            String splitUrl = "file://" + sp.toAbsolutePath();
            try (SplitSearcher searcher = cacheManager.createSplitSearcher(splitUrl, meta, dir.toString())) {
                if (mode != ParquetCompanionConfig.FastFieldMode.DISABLED) {
                    searcher.preloadParquetFastFields("text_val").join();
                }

                TermsAggregation agg = new TermsAggregation("text_terms", "text_val", 100, 0);
                SearchResult result = searcher.search(new SplitMatchAllQuery(), 0, "text_terms", agg);
                assertTrue(result.hasAggregations(),
                        mode.name() + " mode should have terms agg result");
            }
        }
    }

    // ---------------------------------------------------------------
    // 19. Config JSON serialization with all options
    // ---------------------------------------------------------------
    @Test
    @Order(19)
    void testConfigJsonWithAllOptions(@TempDir Path dir) throws Exception {
        Map<String, String> mapping = new HashMap<>();
        mapping.put("col_a", "field_a");

        Map<String, String> tokenizers = new HashMap<>();
        tokenizers.put("desc", "raw");

        ParquetCompanionConfig config = new ParquetCompanionConfig("/data/table")
                .withFastFieldMode(ParquetCompanionConfig.FastFieldMode.HYBRID)
                .withIpAddressFields("src_ip", "dst_ip")
                .withSkipFields("internal_id")
                .withStatisticsFields("price", "ts")
                .withStatisticsTruncateLength(16)
                .withFieldIdMapping(mapping)
                .withTokenizerOverrides(tokenizers)
                .withIndexUid("my-index")
                .withSourceId("my-source")
                .withNodeId("node-1");

        String json = config.toIndexingConfigJson();
        assertNotNull(json);

        JsonNode parsed = MAPPER.readTree(json);
        assertEquals("HYBRID", parsed.get("fast_field_mode").asText());
        assertTrue(parsed.has("ip_address_fields"));
        assertTrue(parsed.has("skip_fields"));
        assertTrue(parsed.has("statistics_fields"));
        assertEquals(16, parsed.get("statistics_truncate_length").asInt());
        assertTrue(parsed.has("field_id_mapping"));
        assertTrue(parsed.has("tokenizer_overrides"));
    }

    // ---------------------------------------------------------------
    // 20. Batch retrieval returns u64 values correctly
    // ---------------------------------------------------------------
    @Test
    @Order(20)
    void testBatchRetrievalU64Values(@TempDir Path dir) throws Exception {
        QuickwitSplit.SplitMetadata metadata = createAllTypesSplit(
                dir, "batch_u64.split", 20, 0,
                ParquetCompanionConfig.FastFieldMode.DISABLED);

        String splitUrl = "file://" + dir.resolve("batch_u64.split").toAbsolutePath();
        try (SplitSearcher searcher = cacheManager.createSplitSearcher(splitUrl, metadata, dir.toString())) {
            SplitQuery query = searcher.parseQuery("*");
            SearchResult results = searcher.search(query, 5);

            DocAddress[] addrs = new DocAddress[Math.min(3, results.getHits().size())];
            for (int i = 0; i < addrs.length; i++) {
                addrs[i] = results.getHits().get(i).getDocAddress();
            }

            List<Document> docs = searcher.docBatchProjected(addrs, "id", "uint_val");
            assertEquals(addrs.length, docs.size());

            for (int i = 0; i < docs.size(); i++) {
                Document doc = docs.get(i);
                assertNotNull(doc.getFirst("uint_val"), "should have u64 field");
                // u64 values start at 1_000_000_000 (TANT binary format)
                long val = ((Number) doc.getFirst("uint_val")).longValue();
                assertTrue(val >= 1_000_000_000L, "u64 value should be >= 1B, got " + val);
            }
        }
    }
}

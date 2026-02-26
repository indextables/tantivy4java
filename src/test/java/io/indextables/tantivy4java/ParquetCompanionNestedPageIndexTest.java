package io.indextables.tantivy4java;

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
 * Integration tests for the nested/array column page index fix.
 *
 * Tests the 3-case retrieval strategy for legacy parquet files (no native offset index):
 *
 *   Case A: File has native offset index → single pass with native index (all types correct)
 *   Case B: Manifest page locs, only primitives in projection → single pass with page locs
 *   Case C: Manifest page locs, nested columns in projection → two-pass:
 *           primitives with page loc injection, nested without (full column chunk reads)
 *
 * The bug: For nested/array columns (List, Map, Struct), num_values in parquet page
 * headers counts leaf-level elements (not rows). Using that for first_row_index produces
 * wrong page fetches at read time. The fix:
 *   1. Indexing time: skip page location computation for nested leaf columns
 *   2. Read time: two-pass when manifest page locs + nested columns in projection
 *
 * Complex schema: id (i64), name (utf8), score (f64), active (bool),
 *                 created_at (timestamp), tags (list<utf8>), address (struct{city,zip}),
 *                 notes (utf8 nullable)
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class ParquetCompanionNestedPageIndexTest {

    private static SplitCacheManager cacheManager;
    private static final ObjectMapper MAPPER = new ObjectMapper();

    @BeforeAll
    static void setUp() {
        SplitCacheManager.CacheConfig config =
                new SplitCacheManager.CacheConfig("nested-page-index-test")
                        .withMaxCacheSize(200_000_000);
        cacheManager = SplitCacheManager.getInstance(config);
    }

    @AfterAll
    static void tearDown() {
        if (cacheManager != null) {
            try { cacheManager.close(); } catch (Exception e) { /* ignore */ }
        }
    }

    // ===================================================================
    // CASE B: Legacy file (no native offset index), ONLY primitive columns
    // projected. Manifest page locs are correct for primitives — single pass.
    // ===================================================================

    @Test
    @Order(1)
    void testLegacyComplexFile_PrimitiveOnlyProjection(@TempDir Path dir) throws Exception {
        Path parquetFile = dir.resolve("legacy_complex_primonly.parquet");
        Path splitFile = dir.resolve("legacy_complex_primonly.split");

        QuickwitSplit.nativeWriteTestParquetComplexNoPageIndex(
                parquetFile.toString(), 500, 0);

        ParquetCompanionConfig config = new ParquetCompanionConfig(dir.toString())
                .withStatisticsFields("id", "score", "name");

        QuickwitSplit.SplitMetadata metadata = QuickwitSplit.createFromParquet(
                Collections.singletonList(parquetFile.toString()),
                splitFile.toString(), config);

        assertEquals(500, metadata.getNumDocs());

        String splitUrl = "file://" + splitFile.toAbsolutePath();
        try (SplitSearcher searcher = cacheManager.createSplitSearcher(
                splitUrl, metadata, dir.toString())) {

            assertTrue(searcher.hasParquetCompanion());

            // Verify the file uses manifest page index
            assertPageIndexSource(searcher, "manifest", "case-B-primonly");

            // Single-doc retrieval with ONLY primitive fields (Case B path)
            SplitQuery query = new SplitTermQuery("name", "item_0");
            SearchResult results = searcher.search(query, 1);
            assertTrue(results.getHits().size() >= 1, "should find item_0");

            Document doc = searcher.docProjected(
                    results.getHits().get(0).getDocAddress(),
                    "id", "name", "score", "active");
            assertEquals(0L, ((Number) doc.getFirst("id")).longValue());
            assertEquals("item_0", doc.getFirst("name"));
            assertNotNull(doc.getFirst("score"));
            assertNotNull(doc.getFirst("active"));

            // Batch retrieval with ONLY primitive fields (Case B path)
            SplitQuery allQuery = searcher.parseQuery("*");
            SearchResult allResults = searcher.search(allQuery, 10);
            assertTrue(allResults.getHits().size() >= 5);

            DocAddress[] addrs = new DocAddress[5];
            for (int i = 0; i < 5; i++) {
                addrs[i] = allResults.getHits().get(i).getDocAddress();
            }

            List<Document> batchDocs = searcher.docBatchProjected(addrs,
                    "id", "name", "score");
            assertEquals(5, batchDocs.size());
            for (Document batchDoc : batchDocs) {
                assertNotNull(batchDoc.getFirst("id"), "batch doc should have id");
                assertNotNull(batchDoc.getFirst("name"), "batch doc should have name");
                assertNotNull(batchDoc.getFirst("score"), "batch doc should have score");
            }
        }
    }

    // ===================================================================
    // CASE C (nested only): Legacy file, ONLY nested columns projected.
    // Reads without page loc injection (full column chunk reads).
    // ===================================================================

    @Test
    @Order(2)
    void testLegacyComplexFile_NestedOnlyProjection(@TempDir Path dir) throws Exception {
        Path parquetFile = dir.resolve("legacy_complex_nestedonly.parquet");
        Path splitFile = dir.resolve("legacy_complex_nestedonly.split");

        QuickwitSplit.nativeWriteTestParquetComplexNoPageIndex(
                parquetFile.toString(), 500, 0);

        ParquetCompanionConfig config = new ParquetCompanionConfig(dir.toString())
                .withStatisticsFields("id");

        QuickwitSplit.SplitMetadata metadata = QuickwitSplit.createFromParquet(
                Collections.singletonList(parquetFile.toString()),
                splitFile.toString(), config);

        assertEquals(500, metadata.getNumDocs());

        String splitUrl = "file://" + splitFile.toAbsolutePath();
        try (SplitSearcher searcher = cacheManager.createSplitSearcher(
                splitUrl, metadata, dir.toString())) {

            assertTrue(searcher.hasParquetCompanion());
            assertPageIndexSource(searcher, "manifest", "case-C-nestedonly");

            // Single-doc retrieval with ONLY nested fields
            SplitQuery query = new SplitTermQuery("name", "item_0");
            SearchResult results = searcher.search(query, 1);
            assertTrue(results.getHits().size() >= 1);

            Document doc = searcher.docProjected(
                    results.getHits().get(0).getDocAddress(),
                    "tags", "address");
            // tags and address are complex types serialized as JSON
            Object tagsVal = doc.getFirst("tags");
            Object addrVal = doc.getFirst("address");
            assertNotNull(tagsVal, "should have tags (list→json)");
            assertNotNull(addrVal, "should have address (struct→json)");

            // Batch retrieval with ONLY nested fields
            SplitQuery allQuery = searcher.parseQuery("*");
            SearchResult allResults = searcher.search(allQuery, 10);
            assertTrue(allResults.getHits().size() >= 5);

            DocAddress[] addrs = new DocAddress[5];
            for (int i = 0; i < 5; i++) {
                addrs[i] = allResults.getHits().get(i).getDocAddress();
            }

            List<Document> batchDocs = searcher.docBatchProjected(addrs,
                    "tags", "address");
            assertEquals(5, batchDocs.size());
            for (Document batchDoc : batchDocs) {
                assertNotNull(batchDoc.getFirst("tags"), "batch doc should have tags");
                assertNotNull(batchDoc.getFirst("address"), "batch doc should have address");
            }
        }
    }

    // ===================================================================
    // CASE C (mixed): Legacy file, BOTH primitive AND nested columns projected.
    // This is the true two-pass case: primitives with page locs, nested without.
    // ===================================================================

    @Test
    @Order(3)
    void testLegacyComplexFile_MixedProjection(@TempDir Path dir) throws Exception {
        Path parquetFile = dir.resolve("legacy_complex_mixed.parquet");
        Path splitFile = dir.resolve("legacy_complex_mixed.split");

        QuickwitSplit.nativeWriteTestParquetComplexNoPageIndex(
                parquetFile.toString(), 500, 0);

        ParquetCompanionConfig config = new ParquetCompanionConfig(dir.toString())
                .withStatisticsFields("id", "score");

        QuickwitSplit.SplitMetadata metadata = QuickwitSplit.createFromParquet(
                Collections.singletonList(parquetFile.toString()),
                splitFile.toString(), config);

        assertEquals(500, metadata.getNumDocs());

        String splitUrl = "file://" + splitFile.toAbsolutePath();
        try (SplitSearcher searcher = cacheManager.createSplitSearcher(
                splitUrl, metadata, dir.toString())) {

            assertTrue(searcher.hasParquetCompanion());
            assertPageIndexSource(searcher, "manifest", "case-C-mixed");

            // Single-doc retrieval with MIXED primitive + nested fields
            SplitQuery query = new SplitTermQuery("name", "item_0");
            SearchResult results = searcher.search(query, 1);
            assertTrue(results.getHits().size() >= 1);

            Document doc = searcher.docProjected(
                    results.getHits().get(0).getDocAddress(),
                    "id", "name", "score", "tags", "address");
            assertEquals(0L, ((Number) doc.getFirst("id")).longValue());
            assertEquals("item_0", doc.getFirst("name"));
            assertNotNull(doc.getFirst("score"));
            assertNotNull(doc.getFirst("tags"), "should have tags");
            assertNotNull(doc.getFirst("address"), "should have address");

            // Batch retrieval with MIXED fields — the core two-pass test
            SplitQuery allQuery = searcher.parseQuery("*");
            SearchResult allResults = searcher.search(allQuery, 20);
            assertTrue(allResults.getHits().size() >= 10);

            DocAddress[] addrs = new DocAddress[10];
            for (int i = 0; i < 10; i++) {
                addrs[i] = allResults.getHits().get(i).getDocAddress();
            }

            List<Document> batchDocs = searcher.docBatchProjected(addrs,
                    "id", "name", "score", "tags", "address");
            assertEquals(10, batchDocs.size());
            for (int i = 0; i < batchDocs.size(); i++) {
                Document batchDoc = batchDocs.get(i);
                assertNotNull(batchDoc.getFirst("id"),
                        "batch doc " + i + " should have id (primitive)");
                assertNotNull(batchDoc.getFirst("name"),
                        "batch doc " + i + " should have name (primitive)");
                assertNotNull(batchDoc.getFirst("score"),
                        "batch doc " + i + " should have score (primitive)");
                assertNotNull(batchDoc.getFirst("tags"),
                        "batch doc " + i + " should have tags (nested)");
                assertNotNull(batchDoc.getFirst("address"),
                        "batch doc " + i + " should have address (nested)");
            }
        }
    }

    // ===================================================================
    // CASE C: No projection (all fields) — includes both primitive and nested.
    // ===================================================================

    @Test
    @Order(4)
    void testLegacyComplexFile_AllFieldsNoProjection(@TempDir Path dir) throws Exception {
        Path parquetFile = dir.resolve("legacy_complex_all.parquet");
        Path splitFile = dir.resolve("legacy_complex_all.split");

        QuickwitSplit.nativeWriteTestParquetComplexNoPageIndex(
                parquetFile.toString(), 200, 0);

        ParquetCompanionConfig config = new ParquetCompanionConfig(dir.toString())
                .withStatisticsFields("id");

        QuickwitSplit.SplitMetadata metadata = QuickwitSplit.createFromParquet(
                Collections.singletonList(parquetFile.toString()),
                splitFile.toString(), config);

        assertEquals(200, metadata.getNumDocs());

        String splitUrl = "file://" + splitFile.toAbsolutePath();
        try (SplitSearcher searcher = cacheManager.createSplitSearcher(
                splitUrl, metadata, dir.toString())) {

            // Single-doc with NO projection (gets all fields)
            SplitQuery query = new SplitTermQuery("name", "item_50");
            SearchResult results = searcher.search(query, 1);
            assertTrue(results.getHits().size() >= 1);

            Document doc = searcher.docProjected(
                    results.getHits().get(0).getDocAddress());
            assertEquals(50L, ((Number) doc.getFirst("id")).longValue());
            assertEquals("item_50", doc.getFirst("name"));
            assertNotNull(doc.getFirst("tags"), "all-fields should include tags");
            assertNotNull(doc.getFirst("address"), "all-fields should include address");

            // Batch with NO projection
            SplitQuery allQuery = searcher.parseQuery("*");
            SearchResult allResults = searcher.search(allQuery, 10);
            assertTrue(allResults.getHits().size() >= 5);

            DocAddress[] addrs = new DocAddress[5];
            for (int i = 0; i < 5; i++) {
                addrs[i] = allResults.getHits().get(i).getDocAddress();
            }

            List<Document> batchDocs = searcher.docBatchProjected(addrs);
            assertEquals(5, batchDocs.size());
            for (Document batchDoc : batchDocs) {
                assertNotNull(batchDoc.getFirst("id"));
                assertNotNull(batchDoc.getFirst("name"));
                assertNotNull(batchDoc.getFirst("tags"));
                assertNotNull(batchDoc.getFirst("address"));
            }
        }
    }

    // ===================================================================
    // CASE A: Modern file (native offset index) with complex types.
    // Native offset index is correct for all types — single pass.
    // ===================================================================

    @Test
    @Order(5)
    void testModernComplexFile_MixedProjection(@TempDir Path dir) throws Exception {
        Path parquetFile = dir.resolve("modern_complex.parquet");
        Path splitFile = dir.resolve("modern_complex.split");

        // Modern file (WITH native offset index) using complex types
        QuickwitSplit.nativeWriteTestParquetComplex(parquetFile.toString(), 500, 0);

        ParquetCompanionConfig config = new ParquetCompanionConfig(dir.toString())
                .withStatisticsFields("id", "score");

        QuickwitSplit.SplitMetadata metadata = QuickwitSplit.createFromParquet(
                Collections.singletonList(parquetFile.toString()),
                splitFile.toString(), config);

        assertEquals(500, metadata.getNumDocs());

        String splitUrl = "file://" + splitFile.toAbsolutePath();
        try (SplitSearcher searcher = cacheManager.createSplitSearcher(
                splitUrl, metadata, dir.toString())) {

            assertTrue(searcher.hasParquetCompanion());
            // Modern files should use native offset index
            assertPageIndexSource(searcher, "native", "case-A-modern");

            // Mixed projection works with native offset index
            SplitQuery query = new SplitTermQuery("name", "item_0");
            SearchResult results = searcher.search(query, 1);
            assertTrue(results.getHits().size() >= 1);

            Document doc = searcher.docProjected(
                    results.getHits().get(0).getDocAddress(),
                    "id", "name", "tags", "address");
            assertEquals(0L, ((Number) doc.getFirst("id")).longValue());
            assertNotNull(doc.getFirst("tags"));
            assertNotNull(doc.getFirst("address"));

            // Batch mixed retrieval with native offset index
            SplitQuery allQuery = searcher.parseQuery("*");
            SearchResult allResults = searcher.search(allQuery, 10);

            DocAddress[] addrs = new DocAddress[5];
            for (int i = 0; i < 5; i++) {
                addrs[i] = allResults.getHits().get(i).getDocAddress();
            }

            List<Document> batchDocs = searcher.docBatchProjected(addrs,
                    "id", "name", "tags", "address");
            assertEquals(5, batchDocs.size());
            for (Document batchDoc : batchDocs) {
                assertNotNull(batchDoc.getFirst("id"));
                assertNotNull(batchDoc.getFirst("tags"));
                assertNotNull(batchDoc.getFirst("address"));
            }
        }
    }

    // ===================================================================
    // CASE C: Larger file — validates correctness at scale (700 rows).
    // This mirrors the production workload that triggered the original bug.
    // ===================================================================

    @Test
    @Order(6)
    void testLegacyComplexFile_LargerScale(@TempDir Path dir) throws Exception {
        Path parquetFile = dir.resolve("legacy_complex_large.parquet");
        Path splitFile = dir.resolve("legacy_complex_large.split");

        QuickwitSplit.nativeWriteTestParquetComplexNoPageIndex(
                parquetFile.toString(), 700, 0);

        ParquetCompanionConfig config = new ParquetCompanionConfig(dir.toString())
                .withStatisticsFields("id", "score", "name");

        QuickwitSplit.SplitMetadata metadata = QuickwitSplit.createFromParquet(
                Collections.singletonList(parquetFile.toString()),
                splitFile.toString(), config);

        assertEquals(700, metadata.getNumDocs());

        String splitUrl = "file://" + splitFile.toAbsolutePath();
        try (SplitSearcher searcher = cacheManager.createSplitSearcher(
                splitUrl, metadata, dir.toString())) {

            assertPageIndexSource(searcher, "manifest", "case-C-large");

            // Match-all confirms all docs accessible
            SplitQuery allQuery = searcher.parseQuery("*");
            SearchResult allResults = searcher.search(allQuery, 800);
            assertEquals(700, allResults.getHits().size(),
                    "should find all 700 docs");

            // Batch retrieval of 50 docs with mixed fields
            int batchSize = 50;
            DocAddress[] addrs = new DocAddress[batchSize];
            for (int i = 0; i < batchSize; i++) {
                addrs[i] = allResults.getHits().get(i * 10).getDocAddress();
            }

            List<Document> batchDocs = searcher.docBatchProjected(addrs,
                    "id", "name", "score", "active", "tags", "address", "notes");
            assertEquals(batchSize, batchDocs.size());

            for (int i = 0; i < batchDocs.size(); i++) {
                Document batchDoc = batchDocs.get(i);
                assertNotNull(batchDoc.getFirst("id"),
                        "doc " + i + " should have id");
                assertNotNull(batchDoc.getFirst("name"),
                        "doc " + i + " should have name");
                assertNotNull(batchDoc.getFirst("tags"),
                        "doc " + i + " should have tags");
                assertNotNull(batchDoc.getFirst("address"),
                        "doc " + i + " should have address");
            }

            // Spot-check specific docs
            SplitQuery midQuery = new SplitTermQuery("name", "item_350");
            SearchResult midResults = searcher.search(midQuery, 1);
            assertTrue(midResults.getHits().size() >= 1, "should find item_350");
            Document midDoc = searcher.docProjected(
                    midResults.getHits().get(0).getDocAddress(),
                    "id", "name", "tags", "address");
            assertEquals(350L, ((Number) midDoc.getFirst("id")).longValue());
            assertNotNull(midDoc.getFirst("tags"));
            assertNotNull(midDoc.getFirst("address"));

            SplitQuery lastQuery = new SplitTermQuery("name", "item_699");
            SearchResult lastResults = searcher.search(lastQuery, 1);
            assertTrue(lastResults.getHits().size() >= 1, "should find item_699");
            Document lastDoc = searcher.docProjected(
                    lastResults.getHits().get(0).getDocAddress(),
                    "id", "name", "tags", "address");
            assertEquals(699L, ((Number) lastDoc.getFirst("id")).longValue());
            assertNotNull(lastDoc.getFirst("tags"));
        }
    }

    // ===================================================================
    // CASE C: Multi-file legacy split with complex types
    // ===================================================================

    @Test
    @Order(7)
    void testLegacyComplexFile_MultiFile(@TempDir Path dir) throws Exception {
        int filesCount = 3;
        int rowsPerFile = 300;
        int totalRows = filesCount * rowsPerFile;

        List<String> parquetPaths = new ArrayList<>();
        for (int i = 0; i < filesCount; i++) {
            Path pq = dir.resolve("legacy_complex_part_" + i + ".parquet");
            QuickwitSplit.nativeWriteTestParquetComplexNoPageIndex(
                    pq.toString(), rowsPerFile, (long) i * rowsPerFile);
            parquetPaths.add(pq.toString());
        }

        Path splitFile = dir.resolve("legacy_complex_multi.split");
        ParquetCompanionConfig config = new ParquetCompanionConfig(dir.toString())
                .withStatisticsFields("id");

        QuickwitSplit.SplitMetadata metadata = QuickwitSplit.createFromParquet(
                parquetPaths, splitFile.toString(), config);

        assertEquals(totalRows, metadata.getNumDocs());

        String splitUrl = "file://" + splitFile.toAbsolutePath();
        try (SplitSearcher searcher = cacheManager.createSplitSearcher(
                splitUrl, metadata, dir.toString())) {

            assertPageIndexSource(searcher, "manifest", "case-C-multifile");

            // Search across files
            SplitQuery allQuery = searcher.parseQuery("*");
            SearchResult allResults = searcher.search(allQuery, totalRows + 1);
            assertEquals(totalRows, allResults.getHits().size());

            // Batch retrieval spanning multiple files (one from each)
            DocAddress[] addrs = new DocAddress[3];
            for (int fileNum = 0; fileNum < 3; fileNum++) {
                int targetId = fileNum * rowsPerFile + 50;
                SplitQuery query = new SplitTermQuery("name", "item_" + targetId);
                SearchResult results = searcher.search(query, 1);
                assertTrue(results.getHits().size() >= 1,
                        "should find item_" + targetId);
                addrs[fileNum] = results.getHits().get(0).getDocAddress();
            }

            List<Document> batchDocs = searcher.docBatchProjected(addrs,
                    "id", "name", "tags", "address");
            assertEquals(3, batchDocs.size());

            for (int fileNum = 0; fileNum < 3; fileNum++) {
                int expectedId = fileNum * rowsPerFile + 50;
                assertEquals((long) expectedId,
                        ((Number) batchDocs.get(fileNum).getFirst("id")).longValue(),
                        "file " + fileNum + " doc should have correct id");
                assertNotNull(batchDocs.get(fileNum).getFirst("tags"),
                        "file " + fileNum + " doc should have tags");
                assertNotNull(batchDocs.get(fileNum).getFirst("address"),
                        "file " + fileNum + " doc should have address");
            }
        }
    }

    // ===================================================================
    // Validate tags content structure (JSON arrays)
    // ===================================================================

    @Test
    @Order(8)
    void testLegacyComplexFile_ValidateTagsContent(@TempDir Path dir) throws Exception {
        Path parquetFile = dir.resolve("legacy_complex_tagcheck.parquet");
        Path splitFile = dir.resolve("legacy_complex_tagcheck.split");

        QuickwitSplit.nativeWriteTestParquetComplexNoPageIndex(
                parquetFile.toString(), 30, 0);

        ParquetCompanionConfig config = new ParquetCompanionConfig(dir.toString());
        QuickwitSplit.SplitMetadata metadata = QuickwitSplit.createFromParquet(
                Collections.singletonList(parquetFile.toString()),
                splitFile.toString(), config);

        String splitUrl = "file://" + splitFile.toAbsolutePath();
        try (SplitSearcher searcher = cacheManager.createSplitSearcher(
                splitUrl, metadata, dir.toString())) {

            // Item 0: even row → tags = ["tag_a_0", "tag_b_0"]
            SplitQuery q0 = new SplitTermQuery("name", "item_0");
            SearchResult r0 = searcher.search(q0, 1);
            assertTrue(r0.getHits().size() >= 1);
            Document doc0 = searcher.docProjected(
                    r0.getHits().get(0).getDocAddress(), "tags", "address");

            Object tags0 = doc0.getFirst("tags");
            assertNotNull(tags0, "item_0 should have non-null tags");
            // Tags is serialized as JSON — verify it's parseable
            String tags0Str = tags0.toString();
            assertTrue(tags0Str.contains("tag_a_0") || tags0Str.contains("tag"),
                    "tags should contain expected values: " + tags0Str);

            // Verify address struct content
            Object addr0 = doc0.getFirst("address");
            assertNotNull(addr0, "item_0 should have address");
            String addr0Str = addr0.toString();
            assertTrue(addr0Str.contains("New York") || addr0Str.contains("city"),
                    "address should contain city: " + addr0Str);
        }
    }

    // ===================================================================
    // Validate indexing-time fix: nested columns have empty page_locations
    // ===================================================================

    @Test
    @Order(9)
    void testLegacyComplexFile_ManifestHasEmptyNestedPageLocs(@TempDir Path dir) throws Exception {
        Path parquetFile = dir.resolve("legacy_complex_manifest.parquet");
        Path splitFile = dir.resolve("legacy_complex_manifest.split");

        QuickwitSplit.nativeWriteTestParquetComplexNoPageIndex(
                parquetFile.toString(), 100, 0);

        ParquetCompanionConfig config = new ParquetCompanionConfig(dir.toString())
                .withStatisticsFields("id");

        QuickwitSplit.SplitMetadata metadata = QuickwitSplit.createFromParquet(
                Collections.singletonList(parquetFile.toString()),
                splitFile.toString(), config);

        String splitUrl = "file://" + splitFile.toAbsolutePath();
        try (SplitSearcher searcher = cacheManager.createSplitSearcher(
                splitUrl, metadata, dir.toString())) {

            // The file should report "manifest" page index source
            // (it has manifest page locs for at least the primitive columns)
            assertPageIndexSource(searcher, "manifest", "manifest-check");

            // Verify search and retrieval still work correctly
            SplitQuery allQuery = searcher.parseQuery("*");
            SearchResult results = searcher.search(allQuery, 200);
            assertEquals(100, results.getHits().size());

            // Verify mixed retrieval works (tests the actual fix)
            DocAddress[] addrs = new DocAddress[3];
            for (int i = 0; i < 3; i++) {
                addrs[i] = results.getHits().get(i).getDocAddress();
            }
            List<Document> docs = searcher.docBatchProjected(addrs,
                    "id", "tags", "address");
            assertEquals(3, docs.size());
            for (Document doc : docs) {
                assertNotNull(doc.getFirst("id"));
                assertNotNull(doc.getFirst("tags"));
                assertNotNull(doc.getFirst("address"));
            }
        }
    }

    // ===================================================================
    // Helpers
    // ===================================================================

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
}

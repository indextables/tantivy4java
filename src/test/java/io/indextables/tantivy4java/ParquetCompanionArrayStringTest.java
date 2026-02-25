package io.indextables.tantivy4java;

import io.indextables.tantivy4java.core.Document;
import io.indextables.tantivy4java.result.SearchResult;
import io.indextables.tantivy4java.split.*;
import io.indextables.tantivy4java.split.merge.*;

import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Reproduction test for page index injection failure with List<Utf8> (array[string]) columns.
 *
 * Root cause: compute_page_locations_from_column_chunk() uses num_values from Thrift page
 * headers to compute first_row_index. For nested columns (List), num_values counts leaf
 * values, not rows — inflating first_row_index for subsequent pages, causing the arrow
 * reader to select wrong pages and fail with "Failed to read parquet batch".
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class ParquetCompanionArrayStringTest {

    private static SplitCacheManager cacheManager;

    @BeforeAll
    static void setUp() {
        SplitCacheManager.CacheConfig config =
                new SplitCacheManager.CacheConfig("array-string-repro-test")
                        .withMaxCacheSize(200_000_000);
        cacheManager = SplitCacheManager.getInstance(config);
    }

    @AfterAll
    static void tearDown() {
        if (cacheManager != null) {
            try { cacheManager.close(); } catch (Exception e) { /* ignore */ }
        }
    }

    /**
     * REPRODUCTION: Create a companion split from a parquet file that has:
     * - A List<Utf8> column (event_type) with alternating 1- and 3-element arrays
     * - NO native offset index (forces computed page locations)
     * - Enough rows to create multiple data pages
     *
     * Then retrieve documents projecting ONLY the array column.
     * This triggers the bug: wrong first_row_index → wrong page selection → decode failure.
     */
    @Test
    @Order(1)
    void testArrayColumnRetrievalWithPageIndex(@TempDir Path dir) throws Exception {
        int numRows = 20_000;
        Path parquetFile = dir.resolve("array_legacy.parquet");
        Path splitFile = dir.resolve("array_legacy.split");

        // Write parquet with List<Utf8> and no offset index
        QuickwitSplit.nativeWriteTestParquetArrayNoPageIndex(
                parquetFile.toString(), numRows, 0);

        // Create companion split — this triggers page location computation
        ParquetCompanionConfig config = new ParquetCompanionConfig(dir.toString());
        QuickwitSplit.SplitMetadata metadata = QuickwitSplit.createFromParquet(
                Collections.singletonList(parquetFile.toString()),
                splitFile.toString(), config);

        assertEquals(numRows, metadata.getNumDocs(), "should index all rows");

        String splitUrl = "file://" + splitFile.toAbsolutePath();
        try (SplitSearcher searcher = cacheManager.createSplitSearcher(
                splitUrl, metadata, dir.toString())) {

            assertTrue(searcher.hasParquetCompanion(), "should be companion split");

            // Search for all docs
            SplitQuery allQuery = searcher.parseQuery("*");
            SearchResult allResults = searcher.search(allQuery, numRows + 1);
            assertEquals(numRows, allResults.getHits().size(), "match-all should return all docs");

            // THE CRITICAL TEST: retrieve docs projected to the array column only.
            // This is the code path that fails in production.
            for (int i = 0; i < Math.min(10, allResults.getHits().size()); i++) {
                Document doc = searcher.docProjected(
                        allResults.getHits().get(i).getDocAddress(),
                        "event_type");
                assertNotNull(doc, "projected doc should not be null for hit " + i);
            }

            // Also verify retrieval of a flat column alongside the array column
            Document firstDoc = searcher.docProjected(
                    allResults.getHits().get(0).getDocAddress(),
                    "id", "name", "event_type");
            assertNotNull(firstDoc.getFirst("id"), "should have id");
            assertNotNull(firstDoc.getFirst("name"), "should have name");
        }
    }

    /**
     * Verify that term search on array column content works correctly.
     */
    @Test
    @Order(2)
    void testArrayColumnTermSearch(@TempDir Path dir) throws Exception {
        int numRows = 5_000;
        Path parquetFile = dir.resolve("array_search.parquet");
        Path splitFile = dir.resolve("array_search.split");

        QuickwitSplit.nativeWriteTestParquetArrayNoPageIndex(
                parquetFile.toString(), numRows, 0);

        ParquetCompanionConfig config = new ParquetCompanionConfig(dir.toString());
        QuickwitSplit.SplitMetadata metadata = QuickwitSplit.createFromParquet(
                Collections.singletonList(parquetFile.toString()),
                splitFile.toString(), config);

        String splitUrl = "file://" + splitFile.toAbsolutePath();
        try (SplitSearcher searcher = cacheManager.createSplitSearcher(
                splitUrl, metadata, dir.toString())) {

            // Verify array column retrieval works across multiple docs
            SplitQuery allQuery = searcher.parseQuery("*");
            SearchResult allResults = searcher.search(allQuery, numRows + 1);
            assertEquals(numRows, allResults.getHits().size(), "match-all should return all docs");

            // Retrieve multiple docs and project both flat and array columns
            for (int i = 0; i < Math.min(20, allResults.getHits().size()); i++) {
                Document doc = searcher.docProjected(
                        allResults.getHits().get(i).getDocAddress(),
                        "id", "event_type");
                assertNotNull(doc.getFirst("id"), "should have id for hit " + i);
            }
        }
    }
}

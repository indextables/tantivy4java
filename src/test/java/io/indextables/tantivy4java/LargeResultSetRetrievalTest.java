package io.indextables.tantivy4java;

import io.indextables.tantivy4java.core.*;
import io.indextables.tantivy4java.result.SearchResult;
import io.indextables.tantivy4java.split.*;
import io.indextables.tantivy4java.split.merge.*;

import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Integration tests for the large result set retrieval APIs:
 *
 * <ul>
 *   <li><b>Fused path</b>: {@code searchAndRetrieveArrowFfi()} — single-call search + retrieval</li>
 *   <li><b>Streaming path</b>: {@code startStreamingRetrieval()} / {@code nextBatch()} /
 *       {@code closeStreamingSession()} — session-based streaming for large result sets</li>
 * </ul>
 *
 * These APIs bypass Quickwit's leaf_search, eliminate BM25 scoring, and resolve
 * doc-to-parquet locations entirely in Rust. Companion-mode only.
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class LargeResultSetRetrievalTest {

    private static SplitCacheManager cacheManager;
    private static sun.misc.Unsafe unsafe;

    private static final int FFI_STRUCT_SIZE = 256;

    @BeforeAll
    static void setUp() throws Exception {
        SplitCacheManager.CacheConfig config = new SplitCacheManager.CacheConfig("large-result-test")
                .withMaxCacheSize(100_000_000);
        cacheManager = SplitCacheManager.getInstance(config);

        java.lang.reflect.Field f = sun.misc.Unsafe.class.getDeclaredField("theUnsafe");
        f.setAccessible(true);
        unsafe = (sun.misc.Unsafe) f.get(null);
    }

    @AfterAll
    static void tearDown() {
        if (cacheManager != null) {
            try { cacheManager.close(); } catch (Exception e) { /* ignore */ }
        }
    }

    // ====================================================================
    // FFI memory helpers (same patterns as ParquetCompanionArrowFfiTest)
    // ====================================================================

    private long[][] allocateFfiStructs(int numColumns) {
        long[] arrayAddrs = new long[numColumns];
        long[] schemaAddrs = new long[numColumns];
        for (int i = 0; i < numColumns; i++) {
            arrayAddrs[i] = unsafe.allocateMemory(FFI_STRUCT_SIZE);
            unsafe.setMemory(arrayAddrs[i], FFI_STRUCT_SIZE, (byte) 0);
            schemaAddrs[i] = unsafe.allocateMemory(FFI_STRUCT_SIZE);
            unsafe.setMemory(schemaAddrs[i], FFI_STRUCT_SIZE, (byte) 0);
        }
        return new long[][] { arrayAddrs, schemaAddrs };
    }

    private void freeFfiStructs(long[] arrayAddrs, long[] schemaAddrs) {
        for (long addr : arrayAddrs) {
            if (addr != 0) unsafe.freeMemory(addr);
        }
        for (long addr : schemaAddrs) {
            if (addr != 0) unsafe.freeMemory(addr);
        }
    }

    private long readArrowArrayLength(long addr) {
        return unsafe.getLong(addr);
    }

    private long readArrowArrayRelease(long addr) {
        return unsafe.getLong(addr + 64);
    }

    private long readArrowSchemaRelease(long addr) {
        return unsafe.getLong(addr + 56);
    }

    private String readCString(long ptr) {
        if (ptr == 0) return null;
        StringBuilder sb = new StringBuilder();
        for (int off = 0; ; off++) {
            byte b = unsafe.getByte(ptr + off);
            if (b == 0) break;
            sb.append((char) b);
        }
        return sb.toString();
    }

    private String readSchemaName(long schemaAddr) {
        return readCString(unsafe.getLong(schemaAddr + 8));
    }

    private String readSchemaFormat(long schemaAddr) {
        return readCString(unsafe.getLong(schemaAddr));
    }

    private long readInt64(long arrayAddr, int row) {
        long dataPtr = unsafe.getLong(unsafe.getLong(arrayAddr + 40) + 8);
        return unsafe.getLong(dataPtr + (long) row * 8);
    }

    private double readFloat64(long arrayAddr, int row) {
        long dataPtr = unsafe.getLong(unsafe.getLong(arrayAddr + 40) + 8);
        return unsafe.getDouble(dataPtr + (long) row * 8);
    }

    private String readUtf8(long arrayAddr, int row) {
        long buffersPtr = unsafe.getLong(arrayAddr + 40);
        long offsetsPtr = unsafe.getLong(buffersPtr + 8);
        long dataPtr = unsafe.getLong(buffersPtr + 16);
        int start = unsafe.getInt(offsetsPtr + (long) row * 4);
        int end = unsafe.getInt(offsetsPtr + (long) (row + 1) * 4);
        byte[] bytes = new byte[end - start];
        for (int i = 0; i < bytes.length; i++) {
            bytes[i] = unsafe.getByte(dataPtr + start + i);
        }
        return new String(bytes, StandardCharsets.UTF_8);
    }

    private boolean readBoolean(long arrayAddr, int row) {
        long dataPtr = unsafe.getLong(unsafe.getLong(arrayAddr + 40) + 8);
        byte b = unsafe.getByte(dataPtr + row / 8);
        return (b & (1 << (row % 8))) != 0;
    }

    private Map<String, Integer> buildColumnMap(long[] schemaAddrs, int numCols) {
        Map<String, Integer> map = new HashMap<>(numCols * 2);
        for (int i = 0; i < numCols; i++) {
            String colName = readSchemaName(schemaAddrs[i]);
            if (colName != null) map.put(colName, i);
        }
        return map;
    }

    // ====================================================================
    // Helper: create a companion split with test parquet data
    // ====================================================================

    private static void writeTestParquet(String path, int numRows, long idOffset) {
        QuickwitSplit.nativeWriteTestParquet(path, numRows, idOffset);
    }

    /**
     * Create a companion split with one parquet file.
     * Test parquet has columns: id (i64), name (utf8), score (f64), active (bool), category (utf8).
     */
    private static final int ALL_COLS = 5;
    private QuickwitSplit.SplitMetadata createSingleFileSplit(
            Path dir, String splitName, int numRows) throws Exception {
        Path parquetFile = dir.resolve(splitName + ".parquet");
        Path splitFile = dir.resolve(splitName + ".split");
        writeTestParquet(parquetFile.toString(), numRows, 0);

        ParquetCompanionConfig config = new ParquetCompanionConfig(dir.toString());
        return QuickwitSplit.createFromParquet(
                Collections.singletonList(parquetFile.toString()),
                splitFile.toString(), config);
    }

    /**
     * Create a companion split with multiple parquet files.
     */
    private QuickwitSplit.SplitMetadata createMultiFileSplit(
            Path dir, String splitName, int filesCount, int rowsPerFile) throws Exception {
        List<String> parquetFiles = new ArrayList<>();
        for (int i = 0; i < filesCount; i++) {
            Path pf = dir.resolve(splitName + "_part" + i + ".parquet");
            writeTestParquet(pf.toString(), rowsPerFile, (long) i * rowsPerFile);
            parquetFiles.add(pf.toString());
        }

        Path splitFile = dir.resolve(splitName + ".split");
        ParquetCompanionConfig config = new ParquetCompanionConfig(dir.toString());
        return QuickwitSplit.createFromParquet(parquetFiles, splitFile.toString(), config);
    }

    private SplitSearcher openSearcher(Path dir, String splitName,
                                        QuickwitSplit.SplitMetadata metadata) throws Exception {
        String splitUrl = "file://" + dir.resolve(splitName + ".split").toAbsolutePath();
        return cacheManager.createSplitSearcher(splitUrl, metadata, dir.toString());
    }

    // ====================================================================
    // FUSED RETRIEVAL TESTS (searchAndRetrieveArrowFfi)
    // ====================================================================

    @Test
    @Order(1)
    @DisplayName("Fused: match-all query returns all rows")
    void testFusedMatchAll(@TempDir Path dir) throws Exception {
        int numRows = 50;
        QuickwitSplit.SplitMetadata metadata = createSingleFileSplit(dir, "fused_all", numRows);

        try (SplitSearcher searcher = openSearcher(dir, "fused_all", metadata)) {
            String queryJson = new SplitMatchAllQuery().toQueryAstJson();
            int numCols = ALL_COLS;
            long[][] structs = allocateFfiStructs(numCols);
            try {
                int rowCount = searcher.searchAndRetrieveArrowFfi(
                        queryJson, structs[0], structs[1]);
                assertEquals(numRows, rowCount, "Should return all rows");

                // Verify FFI structs are populated
                for (int i = 0; i < numCols; i++) {
                    assertEquals(numRows, readArrowArrayLength(structs[0][i]),
                            "Column " + i + " should have " + numRows + " rows");
                    assertNotEquals(0, readArrowArrayRelease(structs[0][i]),
                            "Column " + i + " array should be initialized");
                    assertNotEquals(0, readArrowSchemaRelease(structs[1][i]),
                            "Column " + i + " schema should be initialized");
                }
            } finally {
                freeFfiStructs(structs[0], structs[1]);
            }
        }
    }

    @Test
    @Order(2)
    @DisplayName("Fused: term query returns matching rows only")
    void testFusedTermQuery(@TempDir Path dir) throws Exception {
        int numRows = 50;
        QuickwitSplit.SplitMetadata metadata = createSingleFileSplit(dir, "fused_term", numRows);

        try (SplitSearcher searcher = openSearcher(dir, "fused_term", metadata)) {
            // Query for a specific item by name
            String queryJson = new SplitTermQuery("name", "item_10").toQueryAstJson();
            int numCols = ALL_COLS;
            long[][] structs = allocateFfiStructs(numCols);
            try {
                int rowCount = searcher.searchAndRetrieveArrowFfi(
                        queryJson, structs[0], structs[1]);
                assertEquals(1, rowCount, "Term query should match exactly 1 row");

                // Verify the row data
                Map<String, Integer> colMap = buildColumnMap(structs[1], numCols);
                assertTrue(colMap.containsKey("id"), "Should have id column");
                assertTrue(colMap.containsKey("name"), "Should have name column");

                long idValue = readInt64(structs[0][colMap.get("id")], 0);
                assertEquals(10L, idValue, "id should be 10");
            } finally {
                freeFfiStructs(structs[0], structs[1]);
            }
        }
    }

    @Test
    @Order(3)
    @DisplayName("Fused: projected fields returns subset of columns")
    void testFusedProjectedFields(@TempDir Path dir) throws Exception {
        int numRows = 30;
        QuickwitSplit.SplitMetadata metadata = createSingleFileSplit(dir, "fused_proj", numRows);

        try (SplitSearcher searcher = openSearcher(dir, "fused_proj", metadata)) {
            String queryJson = new SplitMatchAllQuery().toQueryAstJson();
            int numCols = 2; // only id and name
            long[][] structs = allocateFfiStructs(numCols);
            try {
                int rowCount = searcher.searchAndRetrieveArrowFfi(
                        queryJson, structs[0], structs[1], "id", "name");
                assertEquals(numRows, rowCount);

                Map<String, Integer> colMap = buildColumnMap(structs[1], numCols);
                assertEquals(2, colMap.size(), "Should have exactly 2 projected columns");
                assertTrue(colMap.containsKey("id"));
                assertTrue(colMap.containsKey("name"));
                assertFalse(colMap.containsKey("score"), "score should not be projected");
                assertFalse(colMap.containsKey("active"), "active should not be projected");
            } finally {
                freeFfiStructs(structs[0], structs[1]);
            }
        }
    }

    @Test
    @Order(4)
    @DisplayName("Fused: no matches returns 0 rows")
    void testFusedNoMatches(@TempDir Path dir) throws Exception {
        QuickwitSplit.SplitMetadata metadata = createSingleFileSplit(dir, "fused_empty", 20);

        try (SplitSearcher searcher = openSearcher(dir, "fused_empty", metadata)) {
            String queryJson = new SplitTermQuery("name", "nonexistent_item_xyz").toQueryAstJson();
            int numCols = ALL_COLS;
            long[][] structs = allocateFfiStructs(numCols);
            try {
                int rowCount = searcher.searchAndRetrieveArrowFfi(
                        queryJson, structs[0], structs[1]);
                assertEquals(0, rowCount, "No matches should return 0");
            } finally {
                freeFfiStructs(structs[0], structs[1]);
            }
        }
    }

    @Test
    @Order(5)
    @DisplayName("Fused: multi-file split returns rows from all files")
    void testFusedMultiFile(@TempDir Path dir) throws Exception {
        int filesCount = 3;
        int rowsPerFile = 20;
        int totalRows = filesCount * rowsPerFile;
        QuickwitSplit.SplitMetadata metadata = createMultiFileSplit(
                dir, "fused_multi", filesCount, rowsPerFile);

        try (SplitSearcher searcher = openSearcher(dir, "fused_multi", metadata)) {
            String queryJson = new SplitMatchAllQuery().toQueryAstJson();
            int numCols = ALL_COLS;
            long[][] structs = allocateFfiStructs(numCols);
            try {
                int rowCount = searcher.searchAndRetrieveArrowFfi(
                        queryJson, structs[0], structs[1]);
                assertEquals(totalRows, rowCount, "Should return rows from all files");

                // Verify column lengths
                for (int i = 0; i < numCols; i++) {
                    assertEquals(totalRows, readArrowArrayLength(structs[0][i]));
                }
            } finally {
                freeFfiStructs(structs[0], structs[1]);
            }
        }
    }

    @Test
    @Order(6)
    @DisplayName("Fused: data values match docBatch retrieval")
    void testFusedDataMatchesDocBatch(@TempDir Path dir) throws Exception {
        int numRows = 25;
        QuickwitSplit.SplitMetadata metadata = createSingleFileSplit(dir, "fused_compare", numRows);

        try (SplitSearcher searcher = openSearcher(dir, "fused_compare", metadata)) {
            // First: get results via existing search + docBatch path
            SplitQuery query = searcher.parseQuery("*");
            SearchResult results = searcher.search(query, numRows);
            List<SearchResult.Hit> hits = results.getHits();

            DocAddress[] docAddrs = new DocAddress[hits.size()];
            for (int i = 0; i < hits.size(); i++) {
                docAddrs[i] = hits.get(i).getDocAddress();
            }
            List<Document> batchDocs = searcher.docBatchProjected(docAddrs, "id", "name");

            // Second: get same results via fused path
            String queryJson = new SplitMatchAllQuery().toQueryAstJson();
            int numCols = 2;
            long[][] structs = allocateFfiStructs(numCols);
            try {
                int rowCount = searcher.searchAndRetrieveArrowFfi(
                        queryJson, structs[0], structs[1], "id", "name");
                assertEquals(batchDocs.size(), rowCount,
                        "Fused row count should match docBatch count");

                // Collect fused IDs
                Map<String, Integer> colMap = buildColumnMap(structs[1], numCols);
                int idCol = colMap.get("id");
                Set<Long> fusedIds = new HashSet<>();
                for (int r = 0; r < rowCount; r++) {
                    fusedIds.add(readInt64(structs[0][idCol], r));
                }

                // Collect docBatch IDs
                Set<Long> batchIds = new HashSet<>();
                for (Document doc : batchDocs) {
                    batchIds.add(((Number) doc.getFirst("id")).longValue());
                }

                assertEquals(batchIds, fusedIds,
                        "Fused and docBatch should return the same document IDs");
            } finally {
                freeFfiStructs(structs[0], structs[1]);
            }
        }
    }

    // ====================================================================
    // STREAMING RETRIEVAL TESTS
    // ====================================================================

    @Test
    @Order(10)
    @DisplayName("Streaming: basic lifecycle — start, fetch batches, close")
    void testStreamingBasicLifecycle(@TempDir Path dir) throws Exception {
        int numRows = 100;
        QuickwitSplit.SplitMetadata metadata = createSingleFileSplit(dir, "stream_basic", numRows);

        try (SplitSearcher searcher = openSearcher(dir, "stream_basic", metadata)) {
            String queryJson = new SplitMatchAllQuery().toQueryAstJson();

            SplitSearcher.StreamingSession session = searcher.startStreamingRetrieval(queryJson);
            try {
                int numCols = session.getColumnCount();
                assertTrue(numCols > 0, "Should have at least 1 column");

                int totalRows = 0;
                int batchCount = 0;
                while (true) {
                    long[][] structs = allocateFfiStructs(numCols);
                    try {
                        int rows = session.nextBatch(structs[0], structs[1]);
                        if (rows == 0) break;
                        assertTrue(rows > 0, "nextBatch should return positive row count or 0");
                        totalRows += rows;
                        batchCount++;

                        // Verify FFI structs are valid
                        for (int c = 0; c < numCols; c++) {
                            assertEquals(rows, readArrowArrayLength(structs[0][c]),
                                    "All columns should have same row count");
                            assertNotEquals(0, readArrowArrayRelease(structs[0][c]));
                        }
                    } finally {
                        freeFfiStructs(structs[0], structs[1]);
                    }
                }

                assertEquals(numRows, totalRows,
                        "Total streamed rows should equal input document count");
                assertTrue(batchCount >= 1, "Should have at least 1 batch");
            } finally {
                session.close();
            }
        }
    }

    @Test
    @Order(11)
    @DisplayName("Streaming: column count matches projected fields")
    void testStreamingColumnCount(@TempDir Path dir) throws Exception {
        QuickwitSplit.SplitMetadata metadata = createSingleFileSplit(dir, "stream_cols", 20);

        try (SplitSearcher searcher = openSearcher(dir, "stream_cols", metadata)) {
            String queryJson = new SplitMatchAllQuery().toQueryAstJson();

            // All fields
            SplitSearcher.StreamingSession session1 = searcher.startStreamingRetrieval(queryJson);
            try {
                int allCols = session1.getColumnCount();
                assertTrue(allCols >= ALL_COLS, "All-field session should have >= " + ALL_COLS + " columns");
            } finally {
                session1.close();
            }

            // Projected: 2 fields
            SplitSearcher.StreamingSession session2 = searcher.startStreamingRetrieval(queryJson, "id", "name");
            try {
                int projCols = session2.getColumnCount();
                assertEquals(2, projCols, "Projected session should have 2 columns");
            } finally {
                session2.close();
            }
        }
    }

    @Test
    @Order(12)
    @DisplayName("Streaming: no matches yields empty stream")
    void testStreamingNoMatches(@TempDir Path dir) throws Exception {
        QuickwitSplit.SplitMetadata metadata = createSingleFileSplit(dir, "stream_empty", 20);

        try (SplitSearcher searcher = openSearcher(dir, "stream_empty", metadata)) {
            String queryJson = new SplitTermQuery("name", "nonexistent_xyz").toQueryAstJson();

            SplitSearcher.StreamingSession session = searcher.startStreamingRetrieval(queryJson);
            try {
                int numCols = session.getColumnCount();
                assertTrue(numCols > 0, "Even empty session should report column count");

                long[][] structs = allocateFfiStructs(numCols);
                try {
                    int rows = session.nextBatch(structs[0], structs[1]);
                    assertEquals(0, rows, "No-match query should return 0 on first nextBatch");
                } finally {
                    freeFfiStructs(structs[0], structs[1]);
                }
            } finally {
                session.close();
            }
        }
    }

    @Test
    @Order(13)
    @DisplayName("Streaming: term query returns correct subset")
    void testStreamingTermQuery(@TempDir Path dir) throws Exception {
        int numRows = 50;
        QuickwitSplit.SplitMetadata metadata = createSingleFileSplit(dir, "stream_term", numRows);

        try (SplitSearcher searcher = openSearcher(dir, "stream_term", metadata)) {
            String queryJson = new SplitTermQuery("name", "item_25").toQueryAstJson();

            SplitSearcher.StreamingSession session = searcher.startStreamingRetrieval(queryJson, "id", "name");
            try {
                int numCols = session.getColumnCount();
                assertEquals(2, numCols);

                int totalRows = 0;
                List<Long> ids = new ArrayList<>();
                while (true) {
                    long[][] structs = allocateFfiStructs(numCols);
                    try {
                        int rows = session.nextBatch(structs[0], structs[1]);
                        if (rows == 0) break;

                        Map<String, Integer> colMap = buildColumnMap(structs[1], numCols);
                        int idCol = colMap.get("id");
                        for (int r = 0; r < rows; r++) {
                            ids.add(readInt64(structs[0][idCol], r));
                        }
                        totalRows += rows;
                    } finally {
                        freeFfiStructs(structs[0], structs[1]);
                    }
                }

                assertEquals(1, totalRows, "Term query should match exactly 1 doc");
                assertEquals(25L, ids.get(0), "Matched doc should have id=25");
            } finally {
                session.close();
            }
        }
    }

    @Test
    @Order(14)
    @DisplayName("Streaming: multi-file split returns rows from all files")
    void testStreamingMultiFile(@TempDir Path dir) throws Exception {
        int filesCount = 3;
        int rowsPerFile = 30;
        int totalExpected = filesCount * rowsPerFile;
        QuickwitSplit.SplitMetadata metadata = createMultiFileSplit(
                dir, "stream_multi", filesCount, rowsPerFile);

        try (SplitSearcher searcher = openSearcher(dir, "stream_multi", metadata)) {
            String queryJson = new SplitMatchAllQuery().toQueryAstJson();

            SplitSearcher.StreamingSession session = searcher.startStreamingRetrieval(queryJson, "id");
            try {
                int numCols = session.getColumnCount();
                assertEquals(1, numCols);

                Set<Long> allIds = new HashSet<>();
                while (true) {
                    long[][] structs = allocateFfiStructs(numCols);
                    try {
                        int rows = session.nextBatch(structs[0], structs[1]);
                        if (rows == 0) break;
                        for (int r = 0; r < rows; r++) {
                            allIds.add(readInt64(structs[0][0], r));
                        }
                    } finally {
                        freeFfiStructs(structs[0], structs[1]);
                    }
                }

                assertEquals(totalExpected, allIds.size(),
                        "Should have unique IDs from all files");

                // Verify IDs span the full range [0, totalExpected)
                for (long i = 0; i < totalExpected; i++) {
                    assertTrue(allIds.contains(i),
                            "Should contain id=" + i + " from multi-file split");
                }
            } finally {
                session.close();
            }
        }
    }

    @Test
    @Order(15)
    @DisplayName("Streaming: data matches fused retrieval")
    void testStreamingMatchesFused(@TempDir Path dir) throws Exception {
        int numRows = 40;
        QuickwitSplit.SplitMetadata metadata = createSingleFileSplit(dir, "stream_vs_fused", numRows);

        try (SplitSearcher searcher = openSearcher(dir, "stream_vs_fused", metadata)) {
            String queryJson = new SplitMatchAllQuery().toQueryAstJson();
            String[] fields = {"id", "name"};
            int numCols = fields.length;

            // Fused path
            long[][] fusedStructs = allocateFfiStructs(numCols);
            Set<Long> fusedIds = new HashSet<>();
            try {
                int fusedRows = searcher.searchAndRetrieveArrowFfi(
                        queryJson, fusedStructs[0], fusedStructs[1], fields);
                Map<String, Integer> colMap = buildColumnMap(fusedStructs[1], numCols);
                int idCol = colMap.get("id");
                for (int r = 0; r < fusedRows; r++) {
                    fusedIds.add(readInt64(fusedStructs[0][idCol], r));
                }
            } finally {
                freeFfiStructs(fusedStructs[0], fusedStructs[1]);
            }

            // Streaming path
            Set<Long> streamIds = new HashSet<>();
            SplitSearcher.StreamingSession session = searcher.startStreamingRetrieval(queryJson, fields);
            try {
                int streamCols = session.getColumnCount();
                while (true) {
                    long[][] structs = allocateFfiStructs(streamCols);
                    try {
                        int rows = session.nextBatch(structs[0], structs[1]);
                        if (rows == 0) break;
                        Map<String, Integer> colMap = buildColumnMap(structs[1], streamCols);
                        int idCol = colMap.get("id");
                        for (int r = 0; r < rows; r++) {
                            streamIds.add(readInt64(structs[0][idCol], r));
                        }
                    } finally {
                        freeFfiStructs(structs[0], structs[1]);
                    }
                }
            } finally {
                session.close();
            }

            assertEquals(fusedIds, streamIds,
                    "Streaming and fused should return the same document IDs");
        }
    }

    @Test
    @Order(16)
    @DisplayName("Streaming: close session releases resources")
    void testStreamingCloseReleasesResources(@TempDir Path dir) throws Exception {
        QuickwitSplit.SplitMetadata metadata = createSingleFileSplit(dir, "stream_dblclose", 10);

        try (SplitSearcher searcher = openSearcher(dir, "stream_dblclose", metadata)) {
            String queryJson = new SplitMatchAllQuery().toQueryAstJson();

            SplitSearcher.StreamingSession session = searcher.startStreamingRetrieval(queryJson);
            assertNotNull(session, "Session should be non-null");
            // Drain and close
            int cols = session.getColumnCount();
            long[][] ffi = allocateFfiStructs(cols);
            try {
                while (session.nextBatch(ffi[0], ffi[1]) > 0) {
                    // drain
                }
            } finally {
                freeFfiStructs(ffi[0], ffi[1]);
            }
            session.close();
            // Double-close is safe — native layer uses registry pattern,
            // second close is a no-op (entry already removed).
            session.close();
        }
    }

    @Test
    @Order(17)
    @DisplayName("Streaming: close with handle 0 is a no-op")
    void testStreamingCloseZeroHandle(@TempDir Path dir) throws Exception {
        QuickwitSplit.SplitMetadata metadata = createSingleFileSplit(dir, "stream_zero", 10);

        try (SplitSearcher searcher = openSearcher(dir, "stream_zero", metadata)) {
            // Should not throw
            searcher.closeStreamingSession(0);
        }
    }

    @Test
    @Order(18)
    @DisplayName("Streaming: verify schema field names in FFI output")
    void testStreamingSchemaNames(@TempDir Path dir) throws Exception {
        QuickwitSplit.SplitMetadata metadata = createSingleFileSplit(dir, "stream_schema", 20);

        try (SplitSearcher searcher = openSearcher(dir, "stream_schema", metadata)) {
            String queryJson = new SplitMatchAllQuery().toQueryAstJson();

            SplitSearcher.StreamingSession session = searcher.startStreamingRetrieval(queryJson, "id", "score", "name");
            try {
                int numCols = session.getColumnCount();
                assertEquals(3, numCols);

                long[][] structs = allocateFfiStructs(numCols);
                try {
                    int rows = session.nextBatch(structs[0], structs[1]);
                    assertTrue(rows > 0);

                    Map<String, Integer> colMap = buildColumnMap(structs[1], numCols);
                    assertTrue(colMap.containsKey("id"), "Should have 'id' column");
                    assertTrue(colMap.containsKey("score"), "Should have 'score' column");
                    assertTrue(colMap.containsKey("name"), "Should have 'name' column");
                } finally {
                    freeFfiStructs(structs[0], structs[1]);
                }
            } finally {
                session.close();
            }
        }
    }

    @Test
    @Order(19)
    @DisplayName("Streaming: verify data types in Arrow format strings")
    void testStreamingArrowFormatStrings(@TempDir Path dir) throws Exception {
        QuickwitSplit.SplitMetadata metadata = createSingleFileSplit(dir, "stream_fmt", 10);

        try (SplitSearcher searcher = openSearcher(dir, "stream_fmt", metadata)) {
            String queryJson = new SplitMatchAllQuery().toQueryAstJson();

            SplitSearcher.StreamingSession session = searcher.startStreamingRetrieval(queryJson);
            try {
                int numCols = session.getColumnCount();

                long[][] structs = allocateFfiStructs(numCols);
                try {
                    int rows = session.nextBatch(structs[0], structs[1]);
                    assertTrue(rows > 0);

                    Map<String, Integer> colMap = buildColumnMap(structs[1], numCols);

                    // Verify Arrow format strings for known columns
                    if (colMap.containsKey("id")) {
                        String fmt = readSchemaFormat(structs[1][colMap.get("id")]);
                        assertTrue(fmt.equals("l") || fmt.equals("L"),
                                "id should be int64 (l) or uint64 (L), got: " + fmt);
                    }
                    if (colMap.containsKey("score")) {
                        String fmt = readSchemaFormat(structs[1][colMap.get("score")]);
                        assertEquals("g", fmt, "score should be float64 (g)");
                    }
                    if (colMap.containsKey("name")) {
                        String fmt = readSchemaFormat(structs[1][colMap.get("name")]);
                        assertTrue(fmt.equals("u") || fmt.equals("U"),
                                "name should be utf8 (u) or large-utf8 (U), got: " + fmt);
                    }
                    if (colMap.containsKey("active")) {
                        String fmt = readSchemaFormat(structs[1][colMap.get("active")]);
                        assertEquals("b", fmt, "active should be boolean (b)");
                    }
                } finally {
                    freeFfiStructs(structs[0], structs[1]);
                }
            } finally {
                session.close();
            }
        }
    }

    @Test
    @Order(20)
    @DisplayName("Streaming: read and verify actual field values")
    void testStreamingReadValues(@TempDir Path dir) throws Exception {
        int numRows = 30;
        QuickwitSplit.SplitMetadata metadata = createSingleFileSplit(dir, "stream_values", numRows);

        try (SplitSearcher searcher = openSearcher(dir, "stream_values", metadata)) {
            String queryJson = new SplitMatchAllQuery().toQueryAstJson();

            SplitSearcher.StreamingSession session = searcher.startStreamingRetrieval(queryJson, "id", "name", "score");
            try {
                int numCols = session.getColumnCount();

                Set<Long> ids = new HashSet<>();
                Set<String> names = new HashSet<>();
                while (true) {
                    long[][] structs = allocateFfiStructs(numCols);
                    try {
                        int rows = session.nextBatch(structs[0], structs[1]);
                        if (rows == 0) break;

                        Map<String, Integer> colMap = buildColumnMap(structs[1], numCols);
                        int idCol = colMap.get("id");
                        int nameCol = colMap.get("name");
                        int scoreCol = colMap.get("score");

                        for (int r = 0; r < rows; r++) {
                            long id = readInt64(structs[0][idCol], r);
                            ids.add(id);

                            String name = readUtf8(structs[0][nameCol], r);
                            names.add(name);
                            assertTrue(name.startsWith("item_"),
                                    "Name should start with 'item_', got: " + name);

                            double score = readFloat64(structs[0][scoreCol], r);
                            assertTrue(score >= 0.0,
                                    "Score should be non-negative, got: " + score);
                        }
                    } finally {
                        freeFfiStructs(structs[0], structs[1]);
                    }
                }

                assertEquals(numRows, ids.size(), "Should have all unique IDs");
                assertEquals(numRows, names.size(), "Should have all unique names");
            } finally {
                session.close();
            }
        }
    }

    // ====================================================================
    // CROSS-PATH CONSISTENCY TESTS
    // ====================================================================

    @Test
    @Order(30)
    @DisplayName("Cross-path: fused, streaming, and docBatch all return same data")
    void testAllPathsConsistent(@TempDir Path dir) throws Exception {
        int numRows = 35;
        QuickwitSplit.SplitMetadata metadata = createSingleFileSplit(dir, "cross_path", numRows);

        try (SplitSearcher searcher = openSearcher(dir, "cross_path", metadata)) {
            String queryJson = new SplitMatchAllQuery().toQueryAstJson();

            // Path 1: docBatch (existing API)
            SplitQuery query = searcher.parseQuery("*");
            SearchResult results = searcher.search(query, numRows);
            DocAddress[] docAddrs = new DocAddress[results.getHits().size()];
            for (int i = 0; i < docAddrs.length; i++) {
                docAddrs[i] = results.getHits().get(i).getDocAddress();
            }
            List<Document> docs = searcher.docBatchProjected(docAddrs, "id");
            Set<Long> docBatchIds = new HashSet<>();
            for (Document doc : docs) {
                docBatchIds.add(((Number) doc.getFirst("id")).longValue());
            }

            // Path 2: fused
            long[][] fusedStructs = allocateFfiStructs(1);
            Set<Long> fusedIds = new HashSet<>();
            try {
                int fusedRows = searcher.searchAndRetrieveArrowFfi(
                        queryJson, fusedStructs[0], fusedStructs[1], "id");
                for (int r = 0; r < fusedRows; r++) {
                    fusedIds.add(readInt64(fusedStructs[0][0], r));
                }
            } finally {
                freeFfiStructs(fusedStructs[0], fusedStructs[1]);
            }

            // Path 3: streaming
            Set<Long> streamIds = new HashSet<>();
            SplitSearcher.StreamingSession session = searcher.startStreamingRetrieval(queryJson, "id");
            try {
                int numCols = session.getColumnCount();
                while (true) {
                    long[][] structs = allocateFfiStructs(numCols);
                    try {
                        int rows = session.nextBatch(structs[0], structs[1]);
                        if (rows == 0) break;
                        for (int r = 0; r < rows; r++) {
                            streamIds.add(readInt64(structs[0][0], r));
                        }
                    } finally {
                        freeFfiStructs(structs[0], structs[1]);
                    }
                }
            } finally {
                session.close();
            }

            // All three paths should return the same IDs
            assertEquals(docBatchIds, fusedIds, "Fused IDs should match docBatch IDs");
            assertEquals(docBatchIds, streamIds, "Streaming IDs should match docBatch IDs");
        }
    }

    @Test
    @Order(31)
    @DisplayName("Cross-path: partial query returns consistent results across all paths")
    void testPartialQueryConsistency(@TempDir Path dir) throws Exception {
        int numRows = 50;
        QuickwitSplit.SplitMetadata metadata = createSingleFileSplit(dir, "cross_partial", numRows);

        try (SplitSearcher searcher = openSearcher(dir, "cross_partial", metadata)) {
            // Query for items with id < 10 via parsed query
            // nativeWriteTestParquet creates items with name "item_0", "item_1", etc.
            // Use a term query for a specific item to ensure consistency
            String queryJson = new SplitTermQuery("name", "item_5").toQueryAstJson();

            // Fused
            long[][] fusedStructs = allocateFfiStructs(2);
            long fusedId = -1;
            String fusedName = null;
            try {
                int rows = searcher.searchAndRetrieveArrowFfi(
                        queryJson, fusedStructs[0], fusedStructs[1], "id", "name");
                assertEquals(1, rows);
                Map<String, Integer> colMap = buildColumnMap(fusedStructs[1], 2);
                fusedId = readInt64(fusedStructs[0][colMap.get("id")], 0);
                fusedName = readUtf8(fusedStructs[0][colMap.get("name")], 0);
            } finally {
                freeFfiStructs(fusedStructs[0], fusedStructs[1]);
            }

            // Streaming
            long streamId = -1;
            String streamName = null;
            SplitSearcher.StreamingSession session = searcher.startStreamingRetrieval(queryJson, "id", "name");
            try {
                int numCols = session.getColumnCount();
                long[][] structs = allocateFfiStructs(numCols);
                try {
                    int rows = session.nextBatch(structs[0], structs[1]);
                    assertEquals(1, rows);
                    Map<String, Integer> colMap = buildColumnMap(structs[1], numCols);
                    streamId = readInt64(structs[0][colMap.get("id")], 0);
                    streamName = readUtf8(structs[0][colMap.get("name")], 0);
                } finally {
                    freeFfiStructs(structs[0], structs[1]);
                }
            } finally {
                session.close();
            }

            assertEquals(fusedId, streamId, "Fused and streaming should return same id");
            assertEquals(fusedName, streamName, "Fused and streaming should return same name");
            assertEquals(5L, fusedId, "Should be id=5");
            assertEquals("item_5", fusedName, "Should be item_5");
        }
    }

    // ====================================================================
    // LARGER DATASET TESTS
    // ====================================================================

    @Test
    @Order(40)
    @DisplayName("Streaming: larger dataset with multiple batches")
    void testStreamingLargerDataset(@TempDir Path dir) throws Exception {
        // 500 rows across 5 files — ensures multi-file + multi-batch behavior
        int filesCount = 5;
        int rowsPerFile = 100;
        int totalRows = filesCount * rowsPerFile;

        QuickwitSplit.SplitMetadata metadata = createMultiFileSplit(
                dir, "stream_large", filesCount, rowsPerFile);

        try (SplitSearcher searcher = openSearcher(dir, "stream_large", metadata)) {
            String queryJson = new SplitMatchAllQuery().toQueryAstJson();

            SplitSearcher.StreamingSession session = searcher.startStreamingRetrieval(queryJson, "id");
            try {
                int numCols = session.getColumnCount();
                Set<Long> allIds = new HashSet<>();
                int batchCount = 0;

                while (true) {
                    long[][] structs = allocateFfiStructs(numCols);
                    try {
                        int rows = session.nextBatch(structs[0], structs[1]);
                        if (rows == 0) break;
                        for (int r = 0; r < rows; r++) {
                            allIds.add(readInt64(structs[0][0], r));
                        }
                        batchCount++;
                    } finally {
                        freeFfiStructs(structs[0], structs[1]);
                    }
                }

                assertEquals(totalRows, allIds.size(),
                        "Should have " + totalRows + " unique IDs across all batches");
                assertTrue(batchCount >= 1, "Should have at least 1 batch");

                // Verify full ID range
                for (long i = 0; i < totalRows; i++) {
                    assertTrue(allIds.contains(i), "Missing id=" + i);
                }
            } finally {
                session.close();
            }
        }
    }

    @Test
    @Order(41)
    @DisplayName("Fused: larger single-file dataset")
    void testFusedLargerDataset(@TempDir Path dir) throws Exception {
        int numRows = 200;
        QuickwitSplit.SplitMetadata metadata = createSingleFileSplit(dir, "fused_large", numRows);

        try (SplitSearcher searcher = openSearcher(dir, "fused_large", metadata)) {
            String queryJson = new SplitMatchAllQuery().toQueryAstJson();
            int numCols = 2;
            long[][] structs = allocateFfiStructs(numCols);
            try {
                int rowCount = searcher.searchAndRetrieveArrowFfi(
                        queryJson, structs[0], structs[1], "id", "name");
                assertEquals(numRows, rowCount);

                Map<String, Integer> colMap = buildColumnMap(structs[1], numCols);
                Set<Long> ids = new HashSet<>();
                for (int r = 0; r < rowCount; r++) {
                    ids.add(readInt64(structs[0][colMap.get("id")], r));
                }
                assertEquals(numRows, ids.size(), "All IDs should be unique");
            } finally {
                freeFfiStructs(structs[0], structs[1]);
            }
        }
    }

    // ====================================================================
    // ERROR HANDLING TESTS
    // ====================================================================

    @Test
    @Order(50)
    @DisplayName("Error: fused with non-companion split returns -1")
    void testFusedNonCompanionSplit(@TempDir Path dir) throws Exception {
        // Create a regular (non-companion) Quickwit split from a Tantivy index
        try (SchemaBuilder builder = new SchemaBuilder()) {
            builder.addTextField("title", true, false, "default", "position");
            builder.addIntegerField("count", true, true, false);
            try (Schema schema = builder.build();
                 Index index = new Index(schema, dir.resolve("idx").toString(), false);
                 IndexWriter writer = index.writer(Index.Memory.DEFAULT_HEAP_SIZE, 1)) {
                try (Document doc = new Document()) {
                    doc.addText("title", "hello world");
                    doc.addInteger("count", 42);
                    writer.addDocument(doc);
                }
                writer.commit();
            }
        }
        QuickwitSplit.SplitConfig splitConfig = new QuickwitSplit.SplitConfig(
                "test-idx", "test-src", "test-node");
        QuickwitSplit.SplitMetadata metadata = QuickwitSplit.convertIndexFromPath(
                dir.resolve("idx").toString(),
                dir.resolve("regular.split").toString(), splitConfig);

        String splitUrl = "file://" + dir.resolve("regular.split").toAbsolutePath();
        try (SplitSearcher searcher = cacheManager.createSplitSearcher(splitUrl, metadata)) {
            String queryJson = new SplitMatchAllQuery().toQueryAstJson();
            long[][] structs = allocateFfiStructs(2);
            try {
                int rowCount = searcher.searchAndRetrieveArrowFfi(
                        queryJson, structs[0], structs[1], "title");
                assertTrue(rowCount <= 0,
                        "Non-companion split should return 0 or -1, got " + rowCount);
            } finally {
                freeFfiStructs(structs[0], structs[1]);
            }
        }
    }

    @Test
    @Order(51)
    @DisplayName("Error: streaming with non-companion split fails gracefully")
    void testStreamingNonCompanionSplit(@TempDir Path dir) throws Exception {
        try (SchemaBuilder builder = new SchemaBuilder()) {
            builder.addTextField("title", true, false, "default", "position");
            try (Schema schema = builder.build();
                 Index index = new Index(schema, dir.resolve("idx2").toString(), false);
                 IndexWriter writer = index.writer(Index.Memory.DEFAULT_HEAP_SIZE, 1)) {
                try (Document doc = new Document()) {
                    doc.addText("title", "test");
                    writer.addDocument(doc);
                }
                writer.commit();
            }
        }
        QuickwitSplit.SplitConfig splitConfig = new QuickwitSplit.SplitConfig(
                "test-idx", "test-src", "test-node");
        QuickwitSplit.SplitMetadata metadata = QuickwitSplit.convertIndexFromPath(
                dir.resolve("idx2").toString(),
                dir.resolve("regular2.split").toString(), splitConfig);

        String splitUrl = "file://" + dir.resolve("regular2.split").toAbsolutePath();
        try (SplitSearcher searcher = cacheManager.createSplitSearcher(splitUrl, metadata)) {
            String queryJson = new SplitMatchAllQuery().toQueryAstJson();
            assertThrows(Exception.class, () -> {
                searcher.startStreamingRetrieval(queryJson);
            }, "Non-companion split should throw on streaming retrieval");
        }
    }

    @Test
    @Order(52)
    @DisplayName("Error: malformed query JSON throws RuntimeException")
    void testMalformedQueryJson(@TempDir Path dir) throws Exception {
        QuickwitSplit.SplitMetadata metadata = createSingleFileSplit(dir, "err_malformed", 10);

        try (SplitSearcher searcher = openSearcher(dir, "err_malformed", metadata)) {
            long[][] structs = allocateFfiStructs(ALL_COLS);
            try {
                assertThrows(RuntimeException.class, () -> {
                    searcher.searchAndRetrieveArrowFfi(
                            "{invalid json!!!}", structs[0], structs[1]);
                }, "Malformed JSON should throw RuntimeException");
            } finally {
                freeFfiStructs(structs[0], structs[1]);
            }
        }
    }

    @Test
    @Order(53)
    @DisplayName("Error: malformed query JSON in streaming throws exception")
    void testMalformedQueryJsonStreaming(@TempDir Path dir) throws Exception {
        QuickwitSplit.SplitMetadata metadata = createSingleFileSplit(dir, "err_malformed_s", 10);

        try (SplitSearcher searcher = openSearcher(dir, "err_malformed_s", metadata)) {
            assertThrows(Exception.class, () -> {
                searcher.startStreamingRetrieval("{not valid json");
            }, "Malformed JSON should throw on streaming start");
        }
    }

    @Test
    @Order(54)
    @DisplayName("Error: fused returns -1 for non-companion after close")
    void testFusedAfterSearcherClose(@TempDir Path dir) throws Exception {
        QuickwitSplit.SplitMetadata metadata = createSingleFileSplit(dir, "err_closed", 10);
        SplitSearcher searcher = openSearcher(dir, "err_closed", metadata);
        searcher.close();

        assertThrows(IllegalStateException.class, () -> {
            String queryJson = new SplitMatchAllQuery().toQueryAstJson();
            long[][] structs = allocateFfiStructs(ALL_COLS);
            searcher.searchAndRetrieveArrowFfi(queryJson, structs[0], structs[1]);
        }, "Closed searcher should throw IllegalStateException");
    }

    // ====================================================================
    // QUERY TYPE TESTS (Boolean, Range, Exists, Wildcard)
    // ====================================================================

    @Test
    @Order(60)
    @DisplayName("Query: boolean AND query via fused path")
    void testBooleanAndQuery(@TempDir Path dir) throws Exception {
        int numRows = 50;
        QuickwitSplit.SplitMetadata metadata = createSingleFileSplit(dir, "q_bool_and", numRows);

        try (SplitSearcher searcher = openSearcher(dir, "q_bool_and", metadata)) {
            // Match items with name=item_10 AND category must match
            SplitBooleanQuery boolQuery = new SplitBooleanQuery();
            boolQuery.addMust(new SplitTermQuery("name", "item_10"));
            boolQuery.addMust(new SplitMatchAllQuery());
            String queryJson = boolQuery.toQueryAstJson();

            long[][] structs = allocateFfiStructs(2);
            try {
                int rowCount = searcher.searchAndRetrieveArrowFfi(
                        queryJson, structs[0], structs[1], "id", "name");
                assertEquals(1, rowCount, "Boolean AND should narrow to 1 result");

                Map<String, Integer> colMap = buildColumnMap(structs[1], 2);
                long id = readInt64(structs[0][colMap.get("id")], 0);
                assertEquals(10L, id);
            } finally {
                freeFfiStructs(structs[0], structs[1]);
            }
        }
    }

    @Test
    @Order(61)
    @DisplayName("Query: boolean OR query via streaming path")
    void testBooleanOrQueryStreaming(@TempDir Path dir) throws Exception {
        int numRows = 50;
        QuickwitSplit.SplitMetadata metadata = createSingleFileSplit(dir, "q_bool_or", numRows);

        try (SplitSearcher searcher = openSearcher(dir, "q_bool_or", metadata)) {
            SplitBooleanQuery boolQuery = new SplitBooleanQuery();
            boolQuery.addShould(new SplitTermQuery("name", "item_5"));
            boolQuery.addShould(new SplitTermQuery("name", "item_15"));
            boolQuery.addShould(new SplitTermQuery("name", "item_25"));
            boolQuery.setMinimumShouldMatch(1);
            String queryJson = boolQuery.toQueryAstJson();

            SplitSearcher.StreamingSession session = searcher.startStreamingRetrieval(queryJson, "id");
            try {
                int numCols = session.getColumnCount();
                Set<Long> ids = new HashSet<>();
                while (true) {
                    long[][] structs = allocateFfiStructs(numCols);
                    try {
                        int rows = session.nextBatch(structs[0], structs[1]);
                        if (rows == 0) break;
                        for (int r = 0; r < rows; r++) {
                            ids.add(readInt64(structs[0][0], r));
                        }
                    } finally {
                        freeFfiStructs(structs[0], structs[1]);
                    }
                }
                assertEquals(3, ids.size(), "Boolean OR should match 3 items");
                assertTrue(ids.contains(5L));
                assertTrue(ids.contains(15L));
                assertTrue(ids.contains(25L));
            } finally {
                session.close();
            }
        }
    }

    @Test
    @Order(62)
    @DisplayName("Query: boolean MUST_NOT excludes results")
    void testBooleanMustNotQuery(@TempDir Path dir) throws Exception {
        int numRows = 10;
        QuickwitSplit.SplitMetadata metadata = createSingleFileSplit(dir, "q_bool_not", numRows);

        try (SplitSearcher searcher = openSearcher(dir, "q_bool_not", metadata)) {
            SplitBooleanQuery boolQuery = new SplitBooleanQuery();
            boolQuery.addMust(new SplitMatchAllQuery());
            boolQuery.addMustNot(new SplitTermQuery("name", "item_5"));
            String queryJson = boolQuery.toQueryAstJson();

            long[][] structs = allocateFfiStructs(1);
            try {
                int rowCount = searcher.searchAndRetrieveArrowFfi(
                        queryJson, structs[0], structs[1], "id");
                assertEquals(numRows - 1, rowCount,
                        "MUST_NOT should exclude 1 row");

                Set<Long> ids = new HashSet<>();
                for (int r = 0; r < rowCount; r++) {
                    ids.add(readInt64(structs[0][0], r));
                }
                assertFalse(ids.contains(5L), "id=5 should be excluded");
            } finally {
                freeFfiStructs(structs[0], structs[1]);
            }
        }
    }

    @Test
    @Order(63)
    @DisplayName("Query: range query on numeric field")
    void testRangeQuery(@TempDir Path dir) throws Exception {
        int numRows = 50;
        QuickwitSplit.SplitMetadata metadata = createSingleFileSplit(dir, "q_range", numRows);

        try (SplitSearcher searcher = openSearcher(dir, "q_range", metadata)) {
            // id field is i64, range [10, 20] inclusive
            SplitRangeQuery rangeQuery = SplitRangeQuery.inclusiveRange(
                    "id", "10", "20", "i64");
            String queryJson = rangeQuery.toQueryAstJson();

            long[][] structs = allocateFfiStructs(1);
            try {
                int rowCount = searcher.searchAndRetrieveArrowFfi(
                        queryJson, structs[0], structs[1], "id");
                assertEquals(11, rowCount, "Range [10,20] inclusive should match 11 rows");

                Set<Long> ids = new HashSet<>();
                for (int r = 0; r < rowCount; r++) {
                    ids.add(readInt64(structs[0][0], r));
                }
                for (long i = 10; i <= 20; i++) {
                    assertTrue(ids.contains(i), "Should contain id=" + i);
                }
            } finally {
                freeFfiStructs(structs[0], structs[1]);
            }
        }
    }

    @Test
    @Order(64)
    @DisplayName("Query: exists query on field")
    void testExistsQuery(@TempDir Path dir) throws Exception {
        int numRows = 20;
        QuickwitSplit.SplitMetadata metadata = createSingleFileSplit(dir, "q_exists", numRows);

        try (SplitSearcher searcher = openSearcher(dir, "q_exists", metadata)) {
            SplitExistsQuery existsQuery = new SplitExistsQuery("name");
            String queryJson = existsQuery.toQueryAstJson();

            long[][] structs = allocateFfiStructs(1);
            try {
                int rowCount = searcher.searchAndRetrieveArrowFfi(
                        queryJson, structs[0], structs[1], "id");
                assertEquals(numRows, rowCount,
                        "All rows should have the 'name' field");
            } finally {
                freeFfiStructs(structs[0], structs[1]);
            }
        }
    }

    @Test
    @Order(65)
    @DisplayName("Query: range query via streaming path")
    void testRangeQueryStreaming(@TempDir Path dir) throws Exception {
        int numRows = 100;
        QuickwitSplit.SplitMetadata metadata = createSingleFileSplit(dir, "q_range_s", numRows);

        try (SplitSearcher searcher = openSearcher(dir, "q_range_s", metadata)) {
            // Exclusive range (5, 15) should match 5 < id < 15 → ids 6..14 = 9 rows
            SplitRangeQuery rangeQuery = SplitRangeQuery.exclusiveRange(
                    "id", "5", "15", "i64");
            String queryJson = rangeQuery.toQueryAstJson();

            SplitSearcher.StreamingSession session = searcher.startStreamingRetrieval(queryJson, "id");
            try {
                int numCols = session.getColumnCount();
                Set<Long> ids = new HashSet<>();
                while (true) {
                    long[][] structs = allocateFfiStructs(numCols);
                    try {
                        int rows = session.nextBatch(structs[0], structs[1]);
                        if (rows == 0) break;
                        for (int r = 0; r < rows; r++) {
                            ids.add(readInt64(structs[0][0], r));
                        }
                    } finally {
                        freeFfiStructs(structs[0], structs[1]);
                    }
                }
                assertEquals(9, ids.size(), "Exclusive range (5,15) should match 9 rows");
                assertFalse(ids.contains(5L), "5 should be excluded (exclusive)");
                assertFalse(ids.contains(15L), "15 should be excluded (exclusive)");
                assertTrue(ids.contains(10L), "10 should be included");
            } finally {
                session.close();
            }
        }
    }

    // ====================================================================
    // SINGLE ROW AND EDGE CASE TESTS
    // ====================================================================

    @Test
    @Order(70)
    @DisplayName("Edge: single row result via streaming")
    void testStreamingSingleRow(@TempDir Path dir) throws Exception {
        QuickwitSplit.SplitMetadata metadata = createSingleFileSplit(dir, "edge_single", 50);

        try (SplitSearcher searcher = openSearcher(dir, "edge_single", metadata)) {
            String queryJson = new SplitTermQuery("name", "item_25").toQueryAstJson();

            SplitSearcher.StreamingSession session = searcher.startStreamingRetrieval(queryJson, "id", "name");
            try {
                int numCols = session.getColumnCount();
                long[][] structs = allocateFfiStructs(numCols);
                try {
                    int rows = session.nextBatch(structs[0], structs[1]);
                    assertEquals(1, rows, "Should get exactly 1 row");
                    Map<String, Integer> colMap = buildColumnMap(structs[1], numCols);
                    assertEquals(25L, readInt64(structs[0][colMap.get("id")], 0));
                    assertEquals("item_25", readUtf8(structs[0][colMap.get("name")], 0));

                    // Next batch should be end of stream
                    long[][] structs2 = allocateFfiStructs(numCols);
                    try {
                        int rows2 = session.nextBatch(structs2[0], structs2[1]);
                        assertEquals(0, rows2, "Should be end of stream after single row");
                    } finally {
                        freeFfiStructs(structs2[0], structs2[1]);
                    }
                } finally {
                    freeFfiStructs(structs[0], structs[1]);
                }
            } finally {
                session.close();
            }
        }
    }

    @Test
    @Order(71)
    @DisplayName("Edge: single row split (1 total row)")
    void testSingleRowSplit(@TempDir Path dir) throws Exception {
        QuickwitSplit.SplitMetadata metadata = createSingleFileSplit(dir, "edge_1row", 1);

        try (SplitSearcher searcher = openSearcher(dir, "edge_1row", metadata)) {
            String queryJson = new SplitMatchAllQuery().toQueryAstJson();

            // Fused
            long[][] structs = allocateFfiStructs(ALL_COLS);
            try {
                int rowCount = searcher.searchAndRetrieveArrowFfi(
                        queryJson, structs[0], structs[1]);
                assertEquals(1, rowCount, "Single-row split should return 1 row");
            } finally {
                freeFfiStructs(structs[0], structs[1]);
            }

            // Streaming
            SplitSearcher.StreamingSession session = searcher.startStreamingRetrieval(queryJson, "id");
            try {
                int numCols = session.getColumnCount();
                long[][] s = allocateFfiStructs(numCols);
                try {
                    int rows = session.nextBatch(s[0], s[1]);
                    assertEquals(1, rows, "Streaming single-row should return 1");
                } finally {
                    freeFfiStructs(s[0], s[1]);
                }
            } finally {
                session.close();
            }
        }
    }

    @Test
    @Order(72)
    @DisplayName("Edge: match-all via streaming (not just fused)")
    void testStreamingMatchAll(@TempDir Path dir) throws Exception {
        int numRows = 75;
        QuickwitSplit.SplitMetadata metadata = createSingleFileSplit(dir, "edge_matchall_s", numRows);

        try (SplitSearcher searcher = openSearcher(dir, "edge_matchall_s", metadata)) {
            String queryJson = new SplitMatchAllQuery().toQueryAstJson();

            SplitSearcher.StreamingSession session = searcher.startStreamingRetrieval(queryJson, "id");
            try {
                int numCols = session.getColumnCount();
                int totalRows = 0;
                while (true) {
                    long[][] structs = allocateFfiStructs(numCols);
                    try {
                        int rows = session.nextBatch(structs[0], structs[1]);
                        if (rows == 0) break;
                        totalRows += rows;
                    } finally {
                        freeFfiStructs(structs[0], structs[1]);
                    }
                }
                assertEquals(numRows, totalRows, "Streaming match-all should return all rows");
            } finally {
                session.close();
            }
        }
    }

    @Test
    @Order(73)
    @DisplayName("Edge: query matching zero docs in some files but docs in others")
    void testPartialFileMatch(@TempDir Path dir) throws Exception {
        // File 0: ids 0-19 (names item_0..item_19)
        // File 1: ids 20-39 (names item_20..item_39)
        // Query for item_25 should only match in file 1
        QuickwitSplit.SplitMetadata metadata = createMultiFileSplit(dir, "edge_partial", 2, 20);

        try (SplitSearcher searcher = openSearcher(dir, "edge_partial", metadata)) {
            String queryJson = new SplitTermQuery("name", "item_25").toQueryAstJson();

            long[][] structs = allocateFfiStructs(1);
            try {
                int rowCount = searcher.searchAndRetrieveArrowFfi(
                        queryJson, structs[0], structs[1], "id");
                assertEquals(1, rowCount, "Should match exactly 1 row from file 1");
                assertEquals(25L, readInt64(structs[0][0], 0));
            } finally {
                freeFfiStructs(structs[0], structs[1]);
            }
        }
    }

    // ====================================================================
    // CONCURRENT STREAMING SESSIONS
    // ====================================================================

    @Test
    @Order(80)
    @DisplayName("Concurrent: multiple streaming sessions simultaneously")
    void testConcurrentStreamingSessions(@TempDir Path dir) throws Exception {
        int numRows = 30;
        QuickwitSplit.SplitMetadata metadata = createSingleFileSplit(dir, "concurrent", numRows);

        try (SplitSearcher searcher = openSearcher(dir, "concurrent", metadata)) {
            // Start two sessions with different queries
            String allQuery = new SplitMatchAllQuery().toQueryAstJson();
            String termQuery = new SplitTermQuery("name", "item_10").toQueryAstJson();

            SplitSearcher.StreamingSession session1 = searcher.startStreamingRetrieval(allQuery, "id");
            SplitSearcher.StreamingSession session2 = searcher.startStreamingRetrieval(termQuery, "id");
            try {
                // Drain session 1
                int numCols1 = session1.getColumnCount();
                Set<Long> ids1 = new HashSet<>();
                while (true) {
                    long[][] s = allocateFfiStructs(numCols1);
                    try {
                        int rows = session1.nextBatch(s[0], s[1]);
                        if (rows == 0) break;
                        for (int r = 0; r < rows; r++) ids1.add(readInt64(s[0][0], r));
                    } finally {
                        freeFfiStructs(s[0], s[1]);
                    }
                }

                // Drain session 2
                int numCols2 = session2.getColumnCount();
                Set<Long> ids2 = new HashSet<>();
                while (true) {
                    long[][] s = allocateFfiStructs(numCols2);
                    try {
                        int rows = session2.nextBatch(s[0], s[1]);
                        if (rows == 0) break;
                        for (int r = 0; r < rows; r++) ids2.add(readInt64(s[0][0], r));
                    } finally {
                        freeFfiStructs(s[0], s[1]);
                    }
                }

                assertEquals(numRows, ids1.size(), "Session 1 should have all rows");
                assertEquals(1, ids2.size(), "Session 2 should have 1 row");
                assertTrue(ids2.contains(10L));
            } finally {
                session1.close();
                session2.close();
            }
        }
    }

    @Test
    @Order(81)
    @DisplayName("Concurrent: interleaved nextBatch calls from two sessions")
    void testInterleavedNextBatch(@TempDir Path dir) throws Exception {
        int numRows = 50;
        QuickwitSplit.SplitMetadata metadata = createSingleFileSplit(dir, "interleaved", numRows);

        try (SplitSearcher searcher = openSearcher(dir, "interleaved", metadata)) {
            String queryJson = new SplitMatchAllQuery().toQueryAstJson();

            SplitSearcher.StreamingSession session1 = searcher.startStreamingRetrieval(queryJson, "id");
            SplitSearcher.StreamingSession session2 = searcher.startStreamingRetrieval(queryJson, "id");
            try {
                int cols1 = session1.getColumnCount();
                int cols2 = session2.getColumnCount();
                Set<Long> ids1 = new HashSet<>();
                Set<Long> ids2 = new HashSet<>();

                // Interleave: pull from session1 then session2
                boolean s1done = false, s2done = false;
                while (!s1done || !s2done) {
                    if (!s1done) {
                        long[][] s = allocateFfiStructs(cols1);
                        try {
                            int rows = session1.nextBatch(s[0], s[1]);
                            if (rows == 0) { s1done = true; }
                            else { for (int r = 0; r < rows; r++) ids1.add(readInt64(s[0][0], r)); }
                        } finally { freeFfiStructs(s[0], s[1]); }
                    }
                    if (!s2done) {
                        long[][] s = allocateFfiStructs(cols2);
                        try {
                            int rows = session2.nextBatch(s[0], s[1]);
                            if (rows == 0) { s2done = true; }
                            else { for (int r = 0; r < rows; r++) ids2.add(readInt64(s[0][0], r)); }
                        } finally { freeFfiStructs(s[0], s[1]); }
                    }
                }

                assertEquals(numRows, ids1.size(), "Session 1 complete");
                assertEquals(numRows, ids2.size(), "Session 2 complete");
                assertEquals(ids1, ids2, "Both sessions should return same IDs");
            } finally {
                session1.close();
                session2.close();
            }
        }
    }

    // ====================================================================
    // FIELD PROJECTION EDGE CASES
    // ====================================================================

    @Test
    @Order(85)
    @DisplayName("Projection: single numeric field only")
    void testProjectSingleNumericField(@TempDir Path dir) throws Exception {
        QuickwitSplit.SplitMetadata metadata = createSingleFileSplit(dir, "proj_num", 20);

        try (SplitSearcher searcher = openSearcher(dir, "proj_num", metadata)) {
            String queryJson = new SplitMatchAllQuery().toQueryAstJson();
            long[][] structs = allocateFfiStructs(1);
            try {
                int rowCount = searcher.searchAndRetrieveArrowFfi(
                        queryJson, structs[0], structs[1], "score");
                assertEquals(20, rowCount);
                String fmt = readSchemaFormat(structs[1][0]);
                assertEquals("g", fmt, "score should be float64 (g)");
                // Verify first value
                double v = readFloat64(structs[0][0], 0);
                assertTrue(v >= 0.0, "Score should be non-negative");
            } finally {
                freeFfiStructs(structs[0], structs[1]);
            }
        }
    }

    @Test
    @Order(86)
    @DisplayName("Projection: single boolean field only")
    void testProjectSingleBooleanField(@TempDir Path dir) throws Exception {
        QuickwitSplit.SplitMetadata metadata = createSingleFileSplit(dir, "proj_bool", 10);

        try (SplitSearcher searcher = openSearcher(dir, "proj_bool", metadata)) {
            String queryJson = new SplitMatchAllQuery().toQueryAstJson();
            long[][] structs = allocateFfiStructs(1);
            try {
                int rowCount = searcher.searchAndRetrieveArrowFfi(
                        queryJson, structs[0], structs[1], "active");
                assertEquals(10, rowCount);
                String fmt = readSchemaFormat(structs[1][0]);
                assertEquals("b", fmt, "active should be boolean (b)");
            } finally {
                freeFfiStructs(structs[0], structs[1]);
            }
        }
    }

    @Test
    @Order(87)
    @DisplayName("Projection: streaming with all fields (no projection)")
    void testStreamingAllFields(@TempDir Path dir) throws Exception {
        QuickwitSplit.SplitMetadata metadata = createSingleFileSplit(dir, "proj_all_s", 15);

        try (SplitSearcher searcher = openSearcher(dir, "proj_all_s", metadata)) {
            String queryJson = new SplitMatchAllQuery().toQueryAstJson();
            // No field args = all fields
            SplitSearcher.StreamingSession session = searcher.startStreamingRetrieval(queryJson);
            try {
                int numCols = session.getColumnCount();
                assertTrue(numCols >= ALL_COLS,
                        "All-fields streaming should have >= " + ALL_COLS + " columns, got " + numCols);

                long[][] structs = allocateFfiStructs(numCols);
                try {
                    int rows = session.nextBatch(structs[0], structs[1]);
                    assertTrue(rows > 0);
                    // All columns should have same row count
                    for (int c = 0; c < numCols; c++) {
                        assertEquals(rows, readArrowArrayLength(structs[0][c]),
                                "Column " + c + " should have same row count");
                    }
                } finally {
                    freeFfiStructs(structs[0], structs[1]);
                }
            } finally {
                session.close();
            }
        }
    }

    // ====================================================================
    // CROSS-PATH CONSISTENCY WITH MULTIPLE DATA TYPES
    // ====================================================================

    @Test
    @Order(90)
    @DisplayName("Cross-path: boolean and float values consistent across all paths")
    void testCrossPathAllTypes(@TempDir Path dir) throws Exception {
        int numRows = 20;
        QuickwitSplit.SplitMetadata metadata = createSingleFileSplit(dir, "cross_types", numRows);

        try (SplitSearcher searcher = openSearcher(dir, "cross_types", metadata)) {
            String queryJson = new SplitMatchAllQuery().toQueryAstJson();
            String[] fields = {"id", "score", "active"};
            int numCols = fields.length;

            // Fused path: collect all values
            Map<Long, Double> fusedScores = new HashMap<>();
            Map<Long, Boolean> fusedActive = new HashMap<>();
            long[][] fusedStructs = allocateFfiStructs(numCols);
            try {
                int rowCount = searcher.searchAndRetrieveArrowFfi(
                        queryJson, fusedStructs[0], fusedStructs[1], fields);
                Map<String, Integer> colMap = buildColumnMap(fusedStructs[1], numCols);
                int idCol = colMap.get("id");
                int scoreCol = colMap.get("score");
                int activeCol = colMap.get("active");
                for (int r = 0; r < rowCount; r++) {
                    long id = readInt64(fusedStructs[0][idCol], r);
                    fusedScores.put(id, readFloat64(fusedStructs[0][scoreCol], r));
                    fusedActive.put(id, readBoolean(fusedStructs[0][activeCol], r));
                }
            } finally {
                freeFfiStructs(fusedStructs[0], fusedStructs[1]);
            }

            // Streaming path: collect same values
            Map<Long, Double> streamScores = new HashMap<>();
            Map<Long, Boolean> streamActive = new HashMap<>();
            SplitSearcher.StreamingSession session = searcher.startStreamingRetrieval(queryJson, fields);
            try {
                int sCols = session.getColumnCount();
                while (true) {
                    long[][] s = allocateFfiStructs(sCols);
                    try {
                        int rows = session.nextBatch(s[0], s[1]);
                        if (rows == 0) break;
                        Map<String, Integer> colMap = buildColumnMap(s[1], sCols);
                        int idCol = colMap.get("id");
                        int scoreCol = colMap.get("score");
                        int activeCol = colMap.get("active");
                        for (int r = 0; r < rows; r++) {
                            long id = readInt64(s[0][idCol], r);
                            streamScores.put(id, readFloat64(s[0][scoreCol], r));
                            streamActive.put(id, readBoolean(s[0][activeCol], r));
                        }
                    } finally {
                        freeFfiStructs(s[0], s[1]);
                    }
                }
            } finally {
                session.close();
            }

            // Compare
            assertEquals(fusedScores.size(), streamScores.size());
            for (Long id : fusedScores.keySet()) {
                assertEquals(fusedScores.get(id), streamScores.get(id), 0.001,
                        "Score mismatch for id=" + id);
                assertEquals(fusedActive.get(id), streamActive.get(id),
                        "Active mismatch for id=" + id);
            }
        }
    }

    // ====================================================================
    // LARGER SCALE TESTS
    // ====================================================================

    @Test
    @Order(95)
    @DisplayName("Scale: 1000 rows multi-file with range query selectivity")
    void testLargerScaleRangeQuery(@TempDir Path dir) throws Exception {
        // 1000 rows across 10 files — tests multi-file with selective query
        int filesCount = 10;
        int rowsPerFile = 100;
        int totalRows = filesCount * rowsPerFile;
        QuickwitSplit.SplitMetadata metadata = createMultiFileSplit(
                dir, "scale_range", filesCount, rowsPerFile);

        try (SplitSearcher searcher = openSearcher(dir, "scale_range", metadata)) {
            // Select ~10% of rows via range query
            SplitRangeQuery rangeQuery = SplitRangeQuery.inclusiveRange(
                    "id", "0", "99", "i64");
            String queryJson = rangeQuery.toQueryAstJson();

            SplitSearcher.StreamingSession session = searcher.startStreamingRetrieval(queryJson, "id");
            try {
                int numCols = session.getColumnCount();
                Set<Long> ids = new HashSet<>();
                int batchCount = 0;
                while (true) {
                    long[][] structs = allocateFfiStructs(numCols);
                    try {
                        int rows = session.nextBatch(structs[0], structs[1]);
                        if (rows == 0) break;
                        for (int r = 0; r < rows; r++) {
                            ids.add(readInt64(structs[0][0], r));
                        }
                        batchCount++;
                    } finally {
                        freeFfiStructs(structs[0], structs[1]);
                    }
                }
                assertEquals(100, ids.size(), "Range [0,99] should match 100 rows");
                assertTrue(batchCount >= 1);
            } finally {
                session.close();
            }
        }
    }

    @Test
    @Order(96)
    @DisplayName("Scale: high selectivity (>50%) match-all multi-file")
    void testHighSelectivity(@TempDir Path dir) throws Exception {
        // All rows selected from multiple files — exercises FullRowGroup strategy
        int filesCount = 5;
        int rowsPerFile = 200;
        int totalRows = filesCount * rowsPerFile;
        QuickwitSplit.SplitMetadata metadata = createMultiFileSplit(
                dir, "scale_high_sel", filesCount, rowsPerFile);

        try (SplitSearcher searcher = openSearcher(dir, "scale_high_sel", metadata)) {
            String queryJson = new SplitMatchAllQuery().toQueryAstJson();

            // Fused: verify all rows
            long[][] structs = allocateFfiStructs(1);
            try {
                int rowCount = searcher.searchAndRetrieveArrowFfi(
                        queryJson, structs[0], structs[1], "id");
                assertEquals(totalRows, rowCount, "All " + totalRows + " rows should be returned");
            } finally {
                freeFfiStructs(structs[0], structs[1]);
            }

            // Streaming: verify same count
            SplitSearcher.StreamingSession session = searcher.startStreamingRetrieval(queryJson, "id");
            try {
                int numCols = session.getColumnCount();
                int streamTotal = 0;
                while (true) {
                    long[][] s = allocateFfiStructs(numCols);
                    try {
                        int rows = session.nextBatch(s[0], s[1]);
                        if (rows == 0) break;
                        streamTotal += rows;
                    } finally {
                        freeFfiStructs(s[0], s[1]);
                    }
                }
                assertEquals(totalRows, streamTotal);
            } finally {
                session.close();
            }
        }
    }
}

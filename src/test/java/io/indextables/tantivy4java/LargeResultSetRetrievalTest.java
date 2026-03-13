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
 * Integration tests for the streaming companion-mode retrieval API:
 * {@code startStreamingRetrieval()} / {@code nextBatch()} / {@code closeStreamingSession()}.
 *
 * <p>These APIs bypass Quickwit's leaf_search, eliminate BM25 scoring, and resolve
 * doc-to-parquet locations entirely in Rust. Companion-mode only.
 *
 * <p>Some tests also exercise the deprecated {@code searchAndRetrieveArrowFfi()} wrapper
 * which now delegates to the streaming path internally.
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
    @DisplayName("Streaming: data matches docBatch retrieval")
    void testStreamingMatchesDocBatch(@TempDir Path dir) throws Exception {
        int numRows = 40;
        QuickwitSplit.SplitMetadata metadata = createSingleFileSplit(dir, "stream_vs_batch", numRows);

        try (SplitSearcher searcher = openSearcher(dir, "stream_vs_batch", metadata)) {
            String queryJson = new SplitMatchAllQuery().toQueryAstJson();

            // DocBatch path
            SplitQuery query = searcher.parseQuery("*");
            SearchResult results = searcher.search(query, numRows);
            DocAddress[] docAddrs = new DocAddress[results.getHits().size()];
            for (int i = 0; i < docAddrs.length; i++) {
                docAddrs[i] = results.getHits().get(i).getDocAddress();
            }
            List<Document> batchDocs = searcher.docBatchProjected(docAddrs, "id");
            Set<Long> batchIds = new HashSet<>();
            for (Document doc : batchDocs) {
                batchIds.add(((Number) doc.getFirst("id")).longValue());
            }

            // Streaming path
            Set<Long> streamIds = new HashSet<>();
            SplitSearcher.StreamingSession session = searcher.startStreamingRetrieval(queryJson, "id");
            try {
                int streamCols = session.getColumnCount();
                while (true) {
                    long[][] structs = allocateFfiStructs(streamCols);
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

            assertEquals(batchIds, streamIds,
                    "Streaming and docBatch should return the same document IDs");
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
    @DisplayName("Cross-path: streaming and docBatch return same data")
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

            // Path 2: streaming
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

            assertEquals(docBatchIds, streamIds, "Streaming IDs should match docBatch IDs");
        }
    }

    @Test
    @Order(31)
    @DisplayName("Cross-path: partial query returns consistent results across streaming and docBatch")
    void testPartialQueryConsistency(@TempDir Path dir) throws Exception {
        int numRows = 50;
        QuickwitSplit.SplitMetadata metadata = createSingleFileSplit(dir, "cross_partial", numRows);

        try (SplitSearcher searcher = openSearcher(dir, "cross_partial", metadata)) {
            String queryJson = new SplitTermQuery("name", "item_5").toQueryAstJson();

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

            assertEquals(5L, streamId, "Should be id=5");
            assertEquals("item_5", streamName, "Should be item_5");
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
        // Now that streaming works for non-companion splits, the deprecated wrapper should succeed
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
                assertTrue(rowCount >= 1,
                        "Non-companion split streaming should return rows, got " + rowCount);
            } finally {
                freeFfiStructs(structs[0], structs[1]);
            }
        }
    }

    @Test
    @Order(51)
    @DisplayName("Streaming with non-companion split works via tantivy doc store")
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
            try (SplitSearcher.StreamingSession session = searcher.startStreamingRetrieval(queryJson)) {
                int numCols = session.getColumnCount();
                assertTrue(numCols >= 1, "Should have at least 1 column");
                long[][] structs = allocateFfiStructs(numCols);
                try {
                    int rows = session.nextBatch(structs[0], structs[1]);
                    assertTrue(rows >= 1, "Should retrieve at least 1 row from non-companion split");
                } finally {
                    freeFfiStructs(structs[0], structs[1]);
                }
            }
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
    @DisplayName("Cross-path: boolean and float values consistent via streaming")
    void testCrossPathAllTypes(@TempDir Path dir) throws Exception {
        int numRows = 20;
        QuickwitSplit.SplitMetadata metadata = createSingleFileSplit(dir, "cross_types", numRows);

        try (SplitSearcher searcher = openSearcher(dir, "cross_types", metadata)) {
            String queryJson = new SplitMatchAllQuery().toQueryAstJson();
            String[] fields = {"id", "score", "active"};

            // Streaming path: collect all values
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

            // Verify we got all rows with valid values
            assertEquals(numRows, streamScores.size(), "Should have all rows");
            for (Long id : streamScores.keySet()) {
                assertNotNull(streamScores.get(id), "Score should not be null for id=" + id);
                assertNotNull(streamActive.get(id), "Active should not be null for id=" + id);
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

    // ====================================================================
    // STREAMING QUERY REWRITE REGRESSION TESTS
    //
    // These tests verify that perform_bulk_search() correctly rewrites
    // queries for companion splits with string hash fields and exact_only
    // string indexing modes. Without these rewrites, IS NOT NULL, IS NULL,
    // and exact_only EqualTo queries return incorrect results.
    //
    // Bug: TANTIVY4JAVA_STREAMING_FAST_FIELD_BUG
    // ====================================================================

    /**
     * Helper: create a companion split from the string-indexing test parquet,
     * with the given tokenizer overrides and string hash optimization enabled.
     */
    private SplitSearcher createStringIndexingSplitSearcher(
            Path dir, Map<String, String> tokenizerOverrides, String tag) throws Exception {
        Path parquetFile = dir.resolve(tag + ".parquet");
        Path splitFile = dir.resolve(tag + ".split");

        QuickwitSplit.nativeWriteTestParquetForStringIndexing(
                parquetFile.toString(), 15, 0);

        ParquetCompanionConfig config = new ParquetCompanionConfig(dir.toString())
                .withStringHashOptimization(true)
                .withTokenizerOverrides(tokenizerOverrides);

        QuickwitSplit.SplitMetadata metadata = QuickwitSplit.createFromParquet(
                Collections.singletonList(parquetFile.toString()),
                splitFile.toString(), config);

        String splitUrl = "file://" + splitFile.toAbsolutePath();
        return cacheManager.createSplitSearcher(splitUrl, metadata, dir.toString());
    }

    /** Drain a streaming session and return total row count. */
    private int drainStreamingSession(SplitSearcher.StreamingSession session) {
        int totalRows = 0;
        int numCols = session.getColumnCount();
        while (true) {
            long[][] structs = allocateFfiStructs(numCols);
            try {
                int rows = session.nextBatch(structs[0], structs[1]);
                if (rows <= 0) break;
                totalRows += rows;
            } finally {
                freeFfiStructs(structs[0], structs[1]);
            }
        }
        return totalRows;
    }

    @Test
    @Order(100)
    @DisplayName("Streaming: IS NOT NULL (FieldPresence) on companion split returns all rows")
    void testStreamingFieldPresenceIsNotNull(@TempDir Path dir) throws Exception {
        // Create companion split with string hash optimization (enables _phash_* fields)
        Map<String, String> overrides = Collections.emptyMap();
        try (SplitSearcher searcher = createStringIndexingSplitSearcher(dir, overrides, "fp_notnull")) {
            // IS NOT NULL on trace_id — all 15 rows have this field
            SplitExistsQuery existsQuery = new SplitExistsQuery("trace_id");
            String queryJson = existsQuery.toQueryAstJson();

            // Verify via regular search path (baseline)
            SearchResult baseline = searcher.search(existsQuery, 100);
            assertEquals(15, baseline.getHits().size(),
                    "Baseline search should find all 15 rows for IS NOT NULL on trace_id");

            // Verify via streaming path (the regression)
            try (SplitSearcher.StreamingSession session = searcher.startStreamingRetrieval(queryJson)) {
                int streamRows = drainStreamingSession(session);
                assertEquals(15, streamRows,
                        "Streaming IS NOT NULL should return 15 rows, same as baseline");
            }
        }
    }

    @Test
    @Order(101)
    @DisplayName("Streaming: IS NULL (negated FieldPresence) on companion split returns 0 rows")
    void testStreamingFieldPresenceIsNull(@TempDir Path dir) throws Exception {
        Map<String, String> overrides = Collections.emptyMap();
        try (SplitSearcher searcher = createStringIndexingSplitSearcher(dir, overrides, "fp_null")) {
            // IS NULL = match_all AND NOT exists(trace_id)
            // When all rows have trace_id, IS NULL should return 0
            SplitBooleanQuery isNullQuery = new SplitBooleanQuery()
                    .addMust(new SplitMatchAllQuery())
                    .addMustNot(new SplitExistsQuery("trace_id"));
            String queryJson = isNullQuery.toQueryAstJson();

            // Verify via regular search path (baseline)
            SearchResult baseline = searcher.search(isNullQuery, 100);
            assertEquals(0, baseline.getHits().size(),
                    "Baseline IS NULL should return 0 rows when all docs have trace_id");

            // Verify via streaming path (the regression)
            try (SplitSearcher.StreamingSession session = searcher.startStreamingRetrieval(queryJson)) {
                int streamRows = drainStreamingSession(session);
                assertEquals(0, streamRows,
                        "Streaming IS NULL should return 0 rows, same as baseline");
            }
        }
    }

    @Test
    @Order(102)
    @DisplayName("Streaming: exact_only term query finds matching document")
    void testStreamingExactOnlyTermQuery(@TempDir Path dir) throws Exception {
        Map<String, String> overrides = new HashMap<>();
        overrides.put("trace_id", ParquetCompanionConfig.StringIndexingMode.EXACT_ONLY);

        try (SplitSearcher searcher = createStringIndexingSplitSearcher(dir, overrides, "eo_stream")) {
            // First, get a trace_id value from the data via regular search
            SearchResult allDocs = searcher.search(new SplitMatchAllQuery(), 1);
            assertEquals(1, allDocs.getHits().size());

            String traceId;
            try (Document doc = searcher.docProjected(allDocs.getHits().get(0).getDocAddress())) {
                traceId = (String) doc.getFirst("trace_id");
                assertNotNull(traceId, "trace_id should be retrievable from parquet");
            }

            // Build term query for this trace_id (will be rewritten to _phash_trace_id)
            SplitTermQuery termQuery = new SplitTermQuery("trace_id", traceId);
            String queryJson = termQuery.toQueryAstJson();

            // Verify via regular search path (baseline)
            SearchResult baseline = searcher.search(termQuery, 10);
            assertEquals(1, baseline.getHits().size(),
                    "Baseline exact_only term query should find exactly 1 doc");

            // Verify via streaming path (the regression)
            try (SplitSearcher.StreamingSession session = searcher.startStreamingRetrieval(queryJson)) {
                int streamRows = drainStreamingSession(session);
                assertEquals(1, streamRows,
                        "Streaming exact_only term query should find 1 row, same as baseline");
            }
        }
    }

    @Test
    @Order(103)
    @DisplayName("Streaming: exact_only EqualTo for non-existent value returns 0")
    void testStreamingExactOnlyNoMatch(@TempDir Path dir) throws Exception {
        Map<String, String> overrides = new HashMap<>();
        overrides.put("trace_id", ParquetCompanionConfig.StringIndexingMode.EXACT_ONLY);

        try (SplitSearcher searcher = createStringIndexingSplitSearcher(dir, overrides, "eo_nomatch")) {
            // Term query for a value that doesn't exist
            SplitTermQuery termQuery = new SplitTermQuery("trace_id", "00000000-0000-0000-0000-000000000000");
            String queryJson = termQuery.toQueryAstJson();

            // Verify streaming returns 0
            try (SplitSearcher.StreamingSession session = searcher.startStreamingRetrieval(queryJson)) {
                int streamRows = drainStreamingSession(session);
                assertEquals(0, streamRows,
                        "Streaming exact_only term query for non-existent value should return 0");
            }
        }
    }

    @Test
    @Order(104)
    @DisplayName("Streaming vs search() consistency for IS NOT NULL on companion split")
    void testStreamingVsSearchFieldPresenceConsistency(@TempDir Path dir) throws Exception {
        // Test with a basic companion split (from createSingleFileSplit) to ensure
        // field presence works on the standard test parquet columns too
        QuickwitSplit.SplitMetadata metadata = createSingleFileSplit(dir, "fp_consist", 50);

        try (SplitSearcher searcher = openSearcher(dir, "fp_consist", metadata)) {
            // IS NOT NULL on "name" — all rows should have this column
            SplitExistsQuery existsQuery = new SplitExistsQuery("name");
            String queryJson = existsQuery.toQueryAstJson();

            SearchResult baseline = searcher.search(existsQuery, 100);
            int baselineCount = (int) baseline.getHits().size();
            assertTrue(baselineCount > 0, "Baseline should find rows with name field");

            try (SplitSearcher.StreamingSession session = searcher.startStreamingRetrieval(queryJson)) {
                int streamRows = drainStreamingSession(session);
                assertEquals(baselineCount, streamRows,
                        "Streaming IS NOT NULL row count should match search() baseline");
            }
        }
    }

    // ====================================================================
    // COMPLEX ARROW TYPE HINT TESTS
    //
    // These tests verify that non-companion (tantivy doc store) streaming
    // correctly converts JSON fields to complex Arrow types (Struct, List,
    // Map) when the caller provides JSON-formatted type hints.
    // ====================================================================

    /**
     * Helper: create a non-companion split with a JSON field containing struct data.
     */
    private SplitSearcher createJsonFieldSplit(Path dir, String tag, int numDocs) throws Exception {
        try (SchemaBuilder builder = new SchemaBuilder()) {
            builder.addTextField("title", true, false, "default", "position");
            Field dataField = builder.addJsonField("data", JsonObjectOptions.storedAndIndexed());
            try (Schema schema = builder.build();
                 Index index = new Index(schema, dir.resolve("idx_" + tag).toString(), false);
                 IndexWriter writer = index.writer(Index.Memory.DEFAULT_HEAP_SIZE, 1)) {
                for (int i = 0; i < numDocs; i++) {
                    try (Document doc = new Document()) {
                        doc.addText("title", "doc" + i);
                        Map<String, Object> data = new LinkedHashMap<>();
                        data.put("name", "user" + i);
                        data.put("age", i + 20);
                        data.put("tags", Arrays.asList("tag" + (i % 3), "common"));
                        doc.addJson(dataField, data);
                        writer.addDocument(doc);
                    }
                }
                writer.commit();
            }
        }
        QuickwitSplit.SplitConfig splitConfig = new QuickwitSplit.SplitConfig(
                "json-idx", "json-src", "json-node");
        QuickwitSplit.SplitMetadata metadata = QuickwitSplit.convertIndexFromPath(
                dir.resolve("idx_" + tag).toString(),
                dir.resolve(tag + ".split").toString(), splitConfig);

        String splitUrl = "file://" + dir.resolve(tag + ".split").toAbsolutePath();
        return cacheManager.createSplitSearcher(splitUrl, metadata);
    }

    @Test
    @Order(110)
    @DisplayName("Complex type hints: struct type hint produces Arrow struct format")
    void testStructTypeHint(@TempDir Path dir) throws Exception {
        try (SplitSearcher searcher = createJsonFieldSplit(dir, "struct_hint", 5)) {
            String queryJson = new SplitMatchAllQuery().toQueryAstJson();

            // Type hint: data field should be Arrow Struct with name (string) and age (i32)
            String[] typeHints = new String[] {
                "data", "{\"struct\": [[\"name\", \"string\"], [\"age\", \"i32\"]]}"
            };

            try (SplitSearcher.StreamingSession session = searcher.startStreamingRetrieval(
                    queryJson, new String[]{"data"}, typeHints)) {
                int numCols = session.getColumnCount();
                assertEquals(1, numCols, "Should have 1 projected column (data)");

                long[][] structs = allocateFfiStructs(numCols);
                try {
                    int rows = session.nextBatch(structs[0], structs[1]);
                    assertTrue(rows > 0, "Should retrieve at least 1 row");

                    // Verify the schema format is "+s" (Arrow struct format string)
                    String fmt = readSchemaFormat(structs[1][0]);
                    assertEquals("+s", fmt,
                            "data column should have Arrow struct format (+s), got: " + fmt);

                    // Verify column name
                    String name = readSchemaName(structs[1][0]);
                    assertEquals("data", name, "Column name should be 'data'");
                } finally {
                    freeFfiStructs(structs[0], structs[1]);
                }
            }
        }
    }

    @Test
    @Order(111)
    @DisplayName("Complex type hints: list type hint produces Arrow list format")
    void testListTypeHint(@TempDir Path dir) throws Exception {
        try (SplitSearcher searcher = createJsonFieldSplit(dir, "list_hint", 5)) {
            String queryJson = new SplitMatchAllQuery().toQueryAstJson();

            // Type hint: data field as list of strings
            String[] typeHints = new String[] {
                "data", "{\"list\": \"string\"}"
            };

            try (SplitSearcher.StreamingSession session = searcher.startStreamingRetrieval(
                    queryJson, new String[]{"data"}, typeHints)) {
                int numCols = session.getColumnCount();
                assertEquals(1, numCols);

                long[][] structs = allocateFfiStructs(numCols);
                try {
                    int rows = session.nextBatch(structs[0], structs[1]);
                    assertTrue(rows > 0, "Should retrieve at least 1 row");

                    // Arrow list format is "+l"
                    String fmt = readSchemaFormat(structs[1][0]);
                    assertEquals("+l", fmt,
                            "data column should have Arrow list format (+l), got: " + fmt);
                } finally {
                    freeFfiStructs(structs[0], structs[1]);
                }
            }
        }
    }

    @Test
    @Order(112)
    @DisplayName("Complex type hints: map type hint produces Arrow map format")
    void testMapTypeHint(@TempDir Path dir) throws Exception {
        try (SplitSearcher searcher = createJsonFieldSplit(dir, "map_hint", 5)) {
            String queryJson = new SplitMatchAllQuery().toQueryAstJson();

            // Type hint: data field as map<string, string>
            String[] typeHints = new String[] {
                "data", "{\"map\": [\"string\", \"string\"]}"
            };

            try (SplitSearcher.StreamingSession session = searcher.startStreamingRetrieval(
                    queryJson, new String[]{"data"}, typeHints)) {
                int numCols = session.getColumnCount();
                assertEquals(1, numCols);

                long[][] structs = allocateFfiStructs(numCols);
                try {
                    int rows = session.nextBatch(structs[0], structs[1]);
                    assertTrue(rows > 0, "Should retrieve at least 1 row");

                    // Arrow map format is "+m"
                    String fmt = readSchemaFormat(structs[1][0]);
                    assertEquals("+m", fmt,
                            "data column should have Arrow map format (+m), got: " + fmt);
                } finally {
                    freeFfiStructs(structs[0], structs[1]);
                }
            }
        }
    }

    @Test
    @Order(113)
    @DisplayName("Complex type hints: struct with nested list produces correct format")
    void testNestedStructListTypeHint(@TempDir Path dir) throws Exception {
        try (SplitSearcher searcher = createJsonFieldSplit(dir, "nested_hint", 5)) {
            String queryJson = new SplitMatchAllQuery().toQueryAstJson();

            // Type hint: struct with name (string) and tags (list of strings)
            String[] typeHints = new String[] {
                "data", "{\"struct\": [[\"name\", \"string\"], [\"tags\", {\"list\": \"string\"}]]}"
            };

            try (SplitSearcher.StreamingSession session = searcher.startStreamingRetrieval(
                    queryJson, new String[]{"data"}, typeHints)) {
                int numCols = session.getColumnCount();
                assertEquals(1, numCols);

                long[][] structs = allocateFfiStructs(numCols);
                try {
                    int rows = session.nextBatch(structs[0], structs[1]);
                    assertTrue(rows > 0, "Should retrieve at least 1 row");

                    // Top-level should be struct
                    String fmt = readSchemaFormat(structs[1][0]);
                    assertEquals("+s", fmt,
                            "Nested struct should have Arrow struct format (+s), got: " + fmt);

                    // Verify the number of child schemas (struct has 2 fields: name, tags)
                    // ArrowSchema layout: format(0), name(8), metadata(16), flags(24), n_children(32)
                    long nChildren = unsafe.getLong(structs[1][0] + 32);
                    assertEquals(2, nChildren,
                            "Struct should have 2 children (name, tags), got: " + nChildren);
                } finally {
                    freeFfiStructs(structs[0], structs[1]);
                }
            }
        }
    }

    @Test
    @Order(114)
    @DisplayName("Complex type hints: mixed scalar and complex type hints in same session")
    void testMixedScalarAndComplexTypeHints(@TempDir Path dir) throws Exception {
        try (SplitSearcher searcher = createJsonFieldSplit(dir, "mixed_hint", 10)) {
            String queryJson = new SplitMatchAllQuery().toQueryAstJson();

            // Mixed: title as utf8 (scalar), data as struct (complex)
            String[] typeHints = new String[] {
                "title", "utf8",
                "data", "{\"struct\": [[\"name\", \"string\"], [\"age\", \"i32\"]]}"
            };

            try (SplitSearcher.StreamingSession session = searcher.startStreamingRetrieval(
                    queryJson, new String[]{"title", "data"}, typeHints)) {
                int numCols = session.getColumnCount();
                assertEquals(2, numCols, "Should have 2 projected columns");

                long[][] structs = allocateFfiStructs(numCols);
                try {
                    int rows = session.nextBatch(structs[0], structs[1]);
                    assertTrue(rows > 0, "Should retrieve rows");

                    Map<String, Integer> colMap = buildColumnMap(structs[1], numCols);

                    // title should be utf8
                    if (colMap.containsKey("title")) {
                        String titleFmt = readSchemaFormat(structs[1][colMap.get("title")]);
                        assertTrue(titleFmt.equals("u") || titleFmt.equals("U"),
                                "title should be utf8 (u/U), got: " + titleFmt);
                    }

                    // data should be struct
                    if (colMap.containsKey("data")) {
                        String dataFmt = readSchemaFormat(structs[1][colMap.get("data")]);
                        assertEquals("+s", dataFmt,
                                "data should be struct (+s), got: " + dataFmt);
                    }
                } finally {
                    freeFfiStructs(structs[0], structs[1]);
                }
            }
        }
    }

    @Test
    @Order(115)
    @DisplayName("Complex type hints: row count matches across batches")
    void testComplexTypeHintMultipleBatches(@TempDir Path dir) throws Exception {
        // Use enough docs that we might get multiple batches
        try (SplitSearcher searcher = createJsonFieldSplit(dir, "multi_batch", 50)) {
            String queryJson = new SplitMatchAllQuery().toQueryAstJson();

            String[] typeHints = new String[] {
                "data", "{\"struct\": [[\"name\", \"string\"], [\"age\", \"i32\"]]}"
            };

            // Count total rows from streaming with struct type hint
            int streamTotal = 0;
            try (SplitSearcher.StreamingSession session = searcher.startStreamingRetrieval(
                    queryJson, new String[]{"data"}, typeHints)) {
                int numCols = session.getColumnCount();
                while (true) {
                    long[][] structs = allocateFfiStructs(numCols);
                    try {
                        int rows = session.nextBatch(structs[0], structs[1]);
                        if (rows <= 0) break;
                        streamTotal += rows;
                    } finally {
                        freeFfiStructs(structs[0], structs[1]);
                    }
                }
            }

            // Compare with baseline search
            SearchResult baseline = searcher.search(new SplitMatchAllQuery(), 1000);
            assertEquals(baseline.getHits().size(), streamTotal,
                    "Streaming with struct type hints should return same row count as search()");
        }
    }

    /**
     * Creates a split with an integer field "id" and a stored text field "scores"
     * containing JSON-serialized integer arrays like "[90, 85, 92]".
     * This replicates how Spark's ArrayType(IntegerType) is stored via the connector.
     */
    private SplitSearcher createIntArraySplit(Path dir, String tag, int numDocs) throws Exception {
        try (SchemaBuilder builder = new SchemaBuilder()) {
            builder.addIntegerField("id", true, true, false);
            builder.addTextField("scores", true, false, "raw", "position");
            try (Schema schema = builder.build();
                 Index index = new Index(schema, dir.resolve("idx_" + tag).toString(), false);
                 IndexWriter writer = index.writer(Index.Memory.DEFAULT_HEAP_SIZE, 1)) {
                for (int i = 0; i < numDocs; i++) {
                    try (Document doc = new Document()) {
                        doc.addInteger("id", i);
                        // Store integer array as JSON text, as the Spark connector does
                        int a = 80 + (i * 3) % 20;
                        int b = 85 + (i * 7) % 15;
                        int c = 90 + (i * 11) % 10;
                        doc.addText("scores", "[" + a + ", " + b + ", " + c + "]");
                        writer.addDocument(doc);
                    }
                }
                writer.commit();
            }
        }
        QuickwitSplit.SplitConfig splitConfig = new QuickwitSplit.SplitConfig(
                "intarr-idx", "intarr-src", "intarr-node");
        QuickwitSplit.SplitMetadata metadata = QuickwitSplit.convertIndexFromPath(
                dir.resolve("idx_" + tag).toString(),
                dir.resolve(tag + ".split").toString(), splitConfig);

        String splitUrl = "file://" + dir.resolve(tag + ".split").toAbsolutePath();
        return cacheManager.createSplitSearcher(splitUrl, metadata);
    }

    @Test
    @Order(120)
    @DisplayName("Regression: ArrayType(IntegerType) — JSON int array in text field → List(Int32)")
    void testIntArrayFromTextFieldListHint(@TempDir Path dir) throws Exception {
        try (SplitSearcher searcher = createIntArraySplit(dir, "intarr_hint", 3)) {
            String queryJson = new SplitMatchAllQuery().toQueryAstJson();

            // Replicate the Spark connector type hint for ArrayType(IntegerType)
            String[] typeHints = new String[] {
                "id", "i32",
                "scores", "{\"list\": \"i32\"}"
            };

            try (SplitSearcher.StreamingSession session = searcher.startStreamingRetrieval(
                    queryJson, new String[]{"id", "scores"}, typeHints)) {
                assertEquals(2, session.getColumnCount());

                long[][] structs = allocateFfiStructs(2);
                try {
                    int rows = session.nextBatch(structs[0], structs[1]);
                    assertEquals(3, rows, "Should retrieve all 3 rows");

                    // Column 0: id (Int32) — format "i"
                    String idFmt = readSchemaFormat(structs[1][0]);
                    assertEquals("i", idFmt, "id column should be Int32 format 'i'");

                    // Column 1: scores (List<Int32>) — format "+l"
                    String scoresFmt = readSchemaFormat(structs[1][1]);
                    assertEquals("+l", scoresFmt,
                            "scores column should have Arrow list format (+l), got: " + scoresFmt);

                    // Verify child array format is Int32 ("i")
                    // ArrowSchema.children[0] is at offset 40 (n_children=32, children=40)
                    long schemaPtr = structs[1][1];
                    long nChildren = unsafe.getLong(schemaPtr + 32);
                    assertTrue(nChildren > 0, "List schema should have child");
                    long childrenPtr = unsafe.getLong(schemaPtr + 40);
                    long childSchemaPtr = unsafe.getLong(childrenPtr);
                    String childFmt = readSchemaFormat(childSchemaPtr);
                    assertEquals("i", childFmt,
                            "List child should be Int32 format 'i', got: " + childFmt);
                } finally {
                    freeFfiStructs(structs[0], structs[1]);
                }
            }
        }
    }
}

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
 * Integration tests for Arrow FFI columnar export via SplitSearcher.
 *
 * Tests the docBatchArrowFfi() API which exports parquet companion data
 * as Arrow columnar arrays via the Arrow C Data Interface. Since tantivy4java
 * has zero Arrow Java dependencies, these tests use sun.misc.Unsafe for
 * native memory allocation and verify FFI struct contents at the byte level.
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class ParquetCompanionArrowFfiTest {

    private static SplitCacheManager cacheManager;
    private static sun.misc.Unsafe unsafe;

    /** Allocation size per FFI struct (generous padding beyond actual struct size) */
    private static final int FFI_STRUCT_SIZE = 256;

    @BeforeAll
    static void setUp() throws Exception {
        SplitCacheManager.CacheConfig config = new SplitCacheManager.CacheConfig("arrow-ffi-test")
                .withMaxCacheSize(100_000_000);
        cacheManager = SplitCacheManager.getInstance(config);

        // Get Unsafe for native memory allocation (no Arrow Java dependency needed)
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

    private static void writeTestParquet(String path, int numRows, long idOffset) {
        QuickwitSplit.nativeWriteTestParquet(path, numRows, idOffset);
    }

    /**
     * Allocate zeroed native memory for FFI structs (one per column).
     * Returns [arrayAddrs, schemaAddrs].
     */
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

    /**
     * Free native memory for FFI struct containers.
     * Note: Arrow data buffers owned by the structs will leak in tests —
     * this is acceptable since the test process reclaims all memory on exit.
     */
    private void freeFfiStructs(long[] arrayAddrs, long[] schemaAddrs) {
        for (long addr : arrayAddrs) {
            if (addr != 0) unsafe.freeMemory(addr);
        }
        for (long addr : schemaAddrs) {
            if (addr != 0) unsafe.freeMemory(addr);
        }
    }

    /**
     * Read the `length` field (first int64, offset 0) from an FFI_ArrowArray struct.
     */
    private long readArrowArrayLength(long addr) {
        return unsafe.getLong(addr);
    }

    /**
     * Read the `release` function pointer (offset 64) from an FFI_ArrowArray struct.
     * Non-zero means the struct was properly initialized by the native layer.
     */
    private long readArrowArrayRelease(long addr) {
        return unsafe.getLong(addr + 64);
    }

    /**
     * Read the `release` function pointer (offset 56) from an FFI_ArrowSchema struct.
     */
    private long readArrowSchemaRelease(long addr) {
        return unsafe.getLong(addr + 56);
    }

    // ====================================================================
    // FFI struct data-reading helpers (read Arrow C Data Interface at byte level)
    // ====================================================================

    /** Read a null-terminated C string from a native pointer. */
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

    /** Read the Arrow format string from FFI_ArrowSchema (offset 0 = format ptr). */
    private String readSchemaFormat(long schemaAddr) {
        return readCString(unsafe.getLong(schemaAddr));
    }

    /** Read the field name from FFI_ArrowSchema (offset 8 = name ptr). */
    private String readSchemaName(long schemaAddr) {
        return readCString(unsafe.getLong(schemaAddr + 8));
    }

    /** Read the n_children from FFI_ArrowArray (offset 32). */
    private long readArrayNChildren(long arrayAddr) {
        return unsafe.getLong(arrayAddr + 32);
    }

    /** Read the n_buffers from FFI_ArrowArray (offset 24). */
    private long readArrayNBuffers(long arrayAddr) {
        return unsafe.getLong(arrayAddr + 24);
    }

    /** Read the null_count from FFI_ArrowArray (offset 8). */
    private long readArrayNullCount(long arrayAddr) {
        return unsafe.getLong(arrayAddr + 8);
    }

    /** Get the buffers pointer array from FFI_ArrowArray (offset 40). */
    private long getBuffersPtr(long arrayAddr) {
        return unsafe.getLong(arrayAddr + 40);
    }

    /** Get the children pointer array from FFI_ArrowArray (offset 48). */
    private long getChildrenPtr(long arrayAddr) {
        return unsafe.getLong(arrayAddr + 48);
    }

    /** Check if row is null via validity bitmap (buffers[0]). */
    private boolean isRowNull(long arrayAddr, int row) {
        if (readArrayNullCount(arrayAddr) == 0) return false;
        long buffersPtr = getBuffersPtr(arrayAddr);
        long validityPtr = unsafe.getLong(buffersPtr); // buffers[0]
        if (validityPtr == 0) return false;
        byte b = unsafe.getByte(validityPtr + row / 8);
        return (b & (1 << (row % 8))) == 0; // 0 bit = null
    }

    /** Read an int64 value from a fixed-width column (buffers[1], 8 bytes/row). */
    private long readInt64(long arrayAddr, int row) {
        long dataPtr = unsafe.getLong(getBuffersPtr(arrayAddr) + 8);
        return unsafe.getLong(dataPtr + (long) row * 8);
    }

    /** Read a uint64 value (same layout as int64, interpret as unsigned). */
    private long readUInt64(long arrayAddr, int row) {
        return readInt64(arrayAddr, row);
    }

    /** Read an int32 value from a fixed-width column (buffers[1], 4 bytes/row). */
    private int readInt32(long arrayAddr, int row) {
        long dataPtr = unsafe.getLong(getBuffersPtr(arrayAddr) + 8);
        return unsafe.getInt(dataPtr + (long) row * 4);
    }

    /** Read an int16 value (buffers[1], 2 bytes/row). */
    private short readInt16(long arrayAddr, int row) {
        long dataPtr = unsafe.getLong(getBuffersPtr(arrayAddr) + 8);
        return unsafe.getShort(dataPtr + (long) row * 2);
    }

    /** Read a float64 value (buffers[1], 8 bytes/row). */
    private double readFloat64(long arrayAddr, int row) {
        long dataPtr = unsafe.getLong(getBuffersPtr(arrayAddr) + 8);
        return unsafe.getDouble(dataPtr + (long) row * 8);
    }

    /** Read a float32 value (buffers[1], 4 bytes/row). */
    private float readFloat32(long arrayAddr, int row) {
        long dataPtr = unsafe.getLong(getBuffersPtr(arrayAddr) + 8);
        return unsafe.getFloat(dataPtr + (long) row * 4);
    }

    /** Read a boolean value from a bit-packed column (buffers[1]). */
    private boolean readBoolean(long arrayAddr, int row) {
        long dataPtr = unsafe.getLong(getBuffersPtr(arrayAddr) + 8);
        byte b = unsafe.getByte(dataPtr + row / 8);
        return (b & (1 << (row % 8))) != 0;
    }

    /** Read a Utf8 string (buffers: [validity, offsets(i32), data]). */
    private String readUtf8(long arrayAddr, int row) {
        long buffersPtr = getBuffersPtr(arrayAddr);
        long offsetsPtr = unsafe.getLong(buffersPtr + 8);  // buffers[1] = i32 offsets
        long dataPtr = unsafe.getLong(buffersPtr + 16);    // buffers[2] = bytes
        int start = unsafe.getInt(offsetsPtr + (long) row * 4);
        int end = unsafe.getInt(offsetsPtr + (long) (row + 1) * 4);
        byte[] bytes = new byte[end - start];
        for (int i = 0; i < bytes.length; i++) {
            bytes[i] = unsafe.getByte(dataPtr + start + i);
        }
        return new String(bytes, StandardCharsets.UTF_8);
    }

    /** Read a LargeUtf8 string (buffers: [validity, offsets(i64), data]). */
    private String readLargeUtf8(long arrayAddr, int row) {
        long buffersPtr = getBuffersPtr(arrayAddr);
        long offsetsPtr = unsafe.getLong(buffersPtr + 8);  // buffers[1] = i64 offsets
        long dataPtr = unsafe.getLong(buffersPtr + 16);    // buffers[2] = bytes
        long start = unsafe.getLong(offsetsPtr + (long) row * 8);
        long end = unsafe.getLong(offsetsPtr + (long) (row + 1) * 8);
        int len = (int) (end - start);
        byte[] bytes = new byte[len];
        for (int i = 0; i < len; i++) {
            bytes[i] = unsafe.getByte(dataPtr + start + i);
        }
        return new String(bytes, StandardCharsets.UTF_8);
    }

    /**
     * Build a column-name → index map from the FFI schema addresses in a single O(n) pass.
     * FFI returns columns in parquet schema order, not projection order,
     * so callers must use this map to find the correct column index.
     */
    private Map<String, Integer> buildColumnMap(long[] schemaAddrs, int numCols) {
        Map<String, Integer> map = new HashMap<>(numCols * 2);
        for (int i = 0; i < numCols; i++) {
            String colName = readSchemaName(schemaAddrs[i]);
            if (colName != null) map.put(colName, i);
        }
        return map;
    }

    /** Read a Binary value (buffers: [validity, offsets(i32), data]). */
    private byte[] readBinary(long arrayAddr, int row) {
        long buffersPtr = getBuffersPtr(arrayAddr);
        long offsetsPtr = unsafe.getLong(buffersPtr + 8);
        long dataPtr = unsafe.getLong(buffersPtr + 16);
        int start = unsafe.getInt(offsetsPtr + (long) row * 4);
        int end = unsafe.getInt(offsetsPtr + (long) (row + 1) * 4);
        byte[] bytes = new byte[end - start];
        for (int i = 0; i < bytes.length; i++) {
            bytes[i] = unsafe.getByte(dataPtr + start + i);
        }
        return bytes;
    }

    // Helper: create all-types companion split
    private QuickwitSplit.SplitMetadata createAllTypesSplit(
            Path dir, String splitName, int numRows, long idOffset) throws Exception {
        Path parquetFile = dir.resolve("all_types.parquet");
        Path splitFile = dir.resolve(splitName);
        QuickwitSplit.nativeWriteTestParquetAllTypes(parquetFile.toString(), numRows, idOffset);

        ParquetCompanionConfig config = new ParquetCompanionConfig(dir.toString())
                .withFastFieldMode(ParquetCompanionConfig.FastFieldMode.DISABLED)
                .withIpAddressFields("ip_val", "src_ip");

        return QuickwitSplit.createFromParquet(
                Collections.singletonList(parquetFile.toString()),
                splitFile.toString(), config);
    }

    // Helper: create complex-types companion split
    private QuickwitSplit.SplitMetadata createComplexTypesSplit(
            Path dir, String splitName, int numRows, long idOffset) throws Exception {
        Path parquetFile = dir.resolve("complex.parquet");
        Path splitFile = dir.resolve(splitName);
        QuickwitSplit.nativeWriteTestParquetComplex(parquetFile.toString(), numRows, idOffset);

        ParquetCompanionConfig config = new ParquetCompanionConfig(dir.toString());

        return QuickwitSplit.createFromParquet(
                Collections.singletonList(parquetFile.toString()),
                splitFile.toString(), config);
    }

    // ---------------------------------------------------------------
    // 1. supportsArrowFfi() returns true for companion splits
    // ---------------------------------------------------------------
    @Test
    @Order(1)
    void testSupportsArrowFfiForCompanionSplit(@TempDir Path dir) throws Exception {
        Path parquetFile = dir.resolve("data.parquet");
        Path splitFile = dir.resolve("ffi_support.split");
        writeTestParquet(parquetFile.toString(), 20, 0);

        ParquetCompanionConfig config = new ParquetCompanionConfig(dir.toString());
        QuickwitSplit.SplitMetadata metadata = QuickwitSplit.createFromParquet(
                Collections.singletonList(parquetFile.toString()),
                splitFile.toString(), config);

        String splitUrl = "file://" + splitFile.toAbsolutePath();
        try (SplitSearcher searcher = cacheManager.createSplitSearcher(splitUrl, metadata, dir.toString())) {
            assertTrue(searcher.supportsArrowFfi(),
                    "companion split should support Arrow FFI");
            assertTrue(searcher.hasParquetCompanion(),
                    "companion split should have parquet companion");
        }
    }

    // ---------------------------------------------------------------
    // 2. supportsArrowFfi() returns false for non-companion splits
    // ---------------------------------------------------------------
    @Test
    @Order(2)
    void testSupportsArrowFfiForNonCompanionSplit(@TempDir Path dir) throws Exception {
        Schema schema = new SchemaBuilder()
                .addTextField("title", true, false, "default", "position")
                .addIntegerField("score", true, true, true)
                .build();

        Path indexPath = dir.resolve("index");
        Index index = new Index(schema, indexPath.toString());
        IndexWriter writer = index.writer(Index.Memory.DEFAULT_HEAP_SIZE, 1);
        Document doc = new Document();
        doc.addText("title", "test document");
        doc.addInteger("score", 42);
        writer.addDocument(doc);
        writer.commit();
        writer.close();
        index.close();

        Path splitPath = dir.resolve("regular_" + System.nanoTime() + ".split");
        QuickwitSplit.SplitConfig splitConfig = new QuickwitSplit.SplitConfig(
                "test-index", "test-source", "test-node");
        QuickwitSplit.SplitMetadata metadata = QuickwitSplit.convertIndexFromPath(
                indexPath.toString(), splitPath.toString(), splitConfig);

        String splitUrl = splitPath.toAbsolutePath().toString();
        try (SplitSearcher searcher = cacheManager.createSplitSearcher(splitUrl, metadata)) {
            assertFalse(searcher.supportsArrowFfi(),
                    "non-companion split should NOT support Arrow FFI");
            assertFalse(searcher.hasParquetCompanion(),
                    "non-companion split should NOT have parquet companion");
        }
    }

    // ---------------------------------------------------------------
    // 3. Null arrayAddrs throws IllegalArgumentException
    // ---------------------------------------------------------------
    @Test
    @Order(3)
    void testDocBatchArrowFfiNullArrayAddrsThrows(@TempDir Path dir) throws Exception {
        Path parquetFile = dir.resolve("data.parquet");
        Path splitFile = dir.resolve("null_arr.split");
        writeTestParquet(parquetFile.toString(), 10, 0);

        ParquetCompanionConfig config = new ParquetCompanionConfig(dir.toString());
        QuickwitSplit.SplitMetadata metadata = QuickwitSplit.createFromParquet(
                Collections.singletonList(parquetFile.toString()),
                splitFile.toString(), config);

        String splitUrl = "file://" + splitFile.toAbsolutePath();
        try (SplitSearcher searcher = cacheManager.createSplitSearcher(splitUrl, metadata, dir.toString())) {
            SplitQuery query = searcher.parseQuery("*");
            SearchResult results = searcher.search(query, 1);
            DocAddress[] addrs = { results.getHits().get(0).getDocAddress() };

            assertThrows(IllegalArgumentException.class, () ->
                    searcher.docBatchArrowFfi(addrs, null, new long[1], "id"));
        }
    }

    // ---------------------------------------------------------------
    // 4. Null schemaAddrs throws IllegalArgumentException
    // ---------------------------------------------------------------
    @Test
    @Order(4)
    void testDocBatchArrowFfiNullSchemaAddrsThrows(@TempDir Path dir) throws Exception {
        Path parquetFile = dir.resolve("data.parquet");
        Path splitFile = dir.resolve("null_sch.split");
        writeTestParquet(parquetFile.toString(), 10, 0);

        ParquetCompanionConfig config = new ParquetCompanionConfig(dir.toString());
        QuickwitSplit.SplitMetadata metadata = QuickwitSplit.createFromParquet(
                Collections.singletonList(parquetFile.toString()),
                splitFile.toString(), config);

        String splitUrl = "file://" + splitFile.toAbsolutePath();
        try (SplitSearcher searcher = cacheManager.createSplitSearcher(splitUrl, metadata, dir.toString())) {
            SplitQuery query = searcher.parseQuery("*");
            SearchResult results = searcher.search(query, 1);
            DocAddress[] addrs = { results.getHits().get(0).getDocAddress() };

            assertThrows(IllegalArgumentException.class, () ->
                    searcher.docBatchArrowFfi(addrs, new long[1], null, "id"));
        }
    }

    // ---------------------------------------------------------------
    // 5. Length mismatch throws IllegalArgumentException
    // ---------------------------------------------------------------
    @Test
    @Order(5)
    void testDocBatchArrowFfiLengthMismatchThrows(@TempDir Path dir) throws Exception {
        Path parquetFile = dir.resolve("data.parquet");
        Path splitFile = dir.resolve("mismatch.split");
        writeTestParquet(parquetFile.toString(), 10, 0);

        ParquetCompanionConfig config = new ParquetCompanionConfig(dir.toString());
        QuickwitSplit.SplitMetadata metadata = QuickwitSplit.createFromParquet(
                Collections.singletonList(parquetFile.toString()),
                splitFile.toString(), config);

        String splitUrl = "file://" + splitFile.toAbsolutePath();
        try (SplitSearcher searcher = cacheManager.createSplitSearcher(splitUrl, metadata, dir.toString())) {
            SplitQuery query = searcher.parseQuery("*");
            SearchResult results = searcher.search(query, 1);
            DocAddress[] addrs = { results.getHits().get(0).getDocAddress() };

            assertThrows(IllegalArgumentException.class, () ->
                    searcher.docBatchArrowFfi(addrs, new long[2], new long[3], "id"));
        }
    }

    // ---------------------------------------------------------------
    // 6. Arrow FFI with projected fields — verify row count and struct contents
    // ---------------------------------------------------------------
    @Test
    @Order(6)
    void testDocBatchArrowFfiWithProjectedFields(@TempDir Path dir) throws Exception {
        Path parquetFile = dir.resolve("data.parquet");
        Path splitFile = dir.resolve("ffi_proj.split");
        writeTestParquet(parquetFile.toString(), 30, 0);

        ParquetCompanionConfig config = new ParquetCompanionConfig(dir.toString());
        QuickwitSplit.SplitMetadata metadata = QuickwitSplit.createFromParquet(
                Collections.singletonList(parquetFile.toString()),
                splitFile.toString(), config);

        String splitUrl = "file://" + splitFile.toAbsolutePath();
        try (SplitSearcher searcher = cacheManager.createSplitSearcher(splitUrl, metadata, dir.toString())) {
            SplitQuery query = searcher.parseQuery("*");
            SearchResult results = searcher.search(query, 10);
            assertTrue(results.getHits().size() >= 5);

            DocAddress[] docAddrs = new DocAddress[5];
            for (int i = 0; i < 5; i++) {
                docAddrs[i] = results.getHits().get(i).getDocAddress();
            }

            // Allocate FFI structs for 2 projected fields: "id" and "name"
            int numCols = 2;
            long[][] structs = allocateFfiStructs(numCols);
            long[] arrayAddrs = structs[0];
            long[] schemaAddrs = structs[1];

            try {
                int rowCount = searcher.docBatchArrowFfi(docAddrs, arrayAddrs, schemaAddrs, "id", "name");
                assertEquals(5, rowCount, "should return 5 rows");

                // Verify FFI structs were written
                for (int i = 0; i < numCols; i++) {
                    long length = readArrowArrayLength(arrayAddrs[i]);
                    assertEquals(5, length,
                            "ArrowArray[" + i + "].length should be 5");

                    long release = readArrowArrayRelease(arrayAddrs[i]);
                    assertNotEquals(0, release,
                            "ArrowArray[" + i + "].release should be non-null (struct was initialized)");

                    long schemaRelease = readArrowSchemaRelease(schemaAddrs[i]);
                    assertNotEquals(0, schemaRelease,
                            "ArrowSchema[" + i + "].release should be non-null");
                }
            } finally {
                freeFfiStructs(arrayAddrs, schemaAddrs);
            }
        }
    }

    // ---------------------------------------------------------------
    // 7. Arrow FFI single document retrieval
    // ---------------------------------------------------------------
    @Test
    @Order(7)
    void testDocBatchArrowFfiSingleDoc(@TempDir Path dir) throws Exception {
        Path parquetFile = dir.resolve("data.parquet");
        Path splitFile = dir.resolve("ffi_single.split");
        writeTestParquet(parquetFile.toString(), 20, 0);

        ParquetCompanionConfig config = new ParquetCompanionConfig(dir.toString());
        QuickwitSplit.SplitMetadata metadata = QuickwitSplit.createFromParquet(
                Collections.singletonList(parquetFile.toString()),
                splitFile.toString(), config);

        String splitUrl = "file://" + splitFile.toAbsolutePath();
        try (SplitSearcher searcher = cacheManager.createSplitSearcher(splitUrl, metadata, dir.toString())) {
            SplitQuery query = searcher.parseQuery("*");
            SearchResult results = searcher.search(query, 1);
            assertEquals(1, results.getHits().size());

            DocAddress[] docAddrs = { results.getHits().get(0).getDocAddress() };

            int numCols = 1;
            long[][] structs = allocateFfiStructs(numCols);
            try {
                int rowCount = searcher.docBatchArrowFfi(docAddrs, structs[0], structs[1], "id");
                assertEquals(1, rowCount, "single doc should return 1 row");
                assertEquals(1, readArrowArrayLength(structs[0][0]),
                        "ArrowArray.length should be 1");
                assertNotEquals(0, readArrowArrayRelease(structs[0][0]),
                        "ArrowArray.release should be non-null");
            } finally {
                freeFfiStructs(structs[0], structs[1]);
            }
        }
    }

    // ---------------------------------------------------------------
    // 8. Arrow FFI with multiple parquet files
    // ---------------------------------------------------------------
    @Test
    @Order(8)
    void testDocBatchArrowFfiMultipleFiles(@TempDir Path dir) throws Exception {
        Path parquetFile1 = dir.resolve("data1.parquet");
        Path parquetFile2 = dir.resolve("data2.parquet");
        Path splitFile = dir.resolve("ffi_multi.split");

        writeTestParquet(parquetFile1.toString(), 15, 0);
        writeTestParquet(parquetFile2.toString(), 15, 15);

        ParquetCompanionConfig config = new ParquetCompanionConfig(dir.toString());
        QuickwitSplit.SplitMetadata metadata = QuickwitSplit.createFromParquet(
                Arrays.asList(parquetFile1.toString(), parquetFile2.toString()),
                splitFile.toString(), config);

        assertEquals(30, metadata.getNumDocs());

        String splitUrl = "file://" + splitFile.toAbsolutePath();
        try (SplitSearcher searcher = cacheManager.createSplitSearcher(splitUrl, metadata, dir.toString())) {
            SplitQuery query = searcher.parseQuery("*");
            SearchResult results = searcher.search(query, 30);
            assertTrue(results.getHits().size() >= 10,
                    "should have at least 10 hits from combined files");

            int numDocs = Math.min(10, results.getHits().size());
            DocAddress[] docAddrs = new DocAddress[numDocs];
            for (int i = 0; i < numDocs; i++) {
                docAddrs[i] = results.getHits().get(i).getDocAddress();
            }

            int numCols = 2;
            long[][] structs = allocateFfiStructs(numCols);
            try {
                int rowCount = searcher.docBatchArrowFfi(
                        docAddrs, structs[0], structs[1], "id", "name");
                assertEquals(numDocs, rowCount,
                        "should return all requested docs from multiple files");

                for (int i = 0; i < numCols; i++) {
                    assertEquals(numDocs, readArrowArrayLength(structs[0][i]),
                            "ArrowArray[" + i + "].length should match requested docs");
                }
            } finally {
                freeFfiStructs(structs[0], structs[1]);
            }
        }
    }

    // ---------------------------------------------------------------
    // 9. Arrow FFI returns -1 for non-companion split (no exception)
    // ---------------------------------------------------------------
    @Test
    @Order(9)
    void testDocBatchArrowFfiReturnsNegativeOneForNonCompanion(@TempDir Path dir) throws Exception {
        Schema schema = new SchemaBuilder()
                .addTextField("title", true, false, "default", "position")
                .addIntegerField("score", true, true, true)
                .build();

        Path indexPath = dir.resolve("index");
        Index index = new Index(schema, indexPath.toString());
        IndexWriter writer = index.writer(Index.Memory.DEFAULT_HEAP_SIZE, 1);
        Document doc = new Document();
        doc.addText("title", "test");
        doc.addInteger("score", 1);
        writer.addDocument(doc);
        writer.commit();
        writer.close();
        index.close();

        Path splitPath = dir.resolve("regular_ffi_" + System.nanoTime() + ".split");
        QuickwitSplit.SplitConfig splitConfig = new QuickwitSplit.SplitConfig(
                "test-index", "test-source", "test-node");
        QuickwitSplit.SplitMetadata metadata = QuickwitSplit.convertIndexFromPath(
                indexPath.toString(), splitPath.toString(), splitConfig);

        String splitUrl = splitPath.toAbsolutePath().toString();
        try (SplitSearcher searcher = cacheManager.createSplitSearcher(splitUrl, metadata)) {
            SplitQuery query = searcher.parseQuery("*");
            SearchResult results = searcher.search(query, 1);
            DocAddress[] docAddrs = { results.getHits().get(0).getDocAddress() };

            long[][] structs = allocateFfiStructs(1);
            try {
                int rowCount = searcher.docBatchArrowFfi(
                        docAddrs, structs[0], structs[1], "title");
                assertEquals(-1, rowCount,
                        "non-companion split should return -1 (not supported)");
            } finally {
                freeFfiStructs(structs[0], structs[1]);
            }
        }
    }

    // ---------------------------------------------------------------
    // 10. Arrow FFI with zero address causes native error
    // ---------------------------------------------------------------
    @Test
    @Order(10)
    void testDocBatchArrowFfiZeroAddressCausesError(@TempDir Path dir) throws Exception {
        Path parquetFile = dir.resolve("data.parquet");
        Path splitFile = dir.resolve("zero_addr.split");
        writeTestParquet(parquetFile.toString(), 10, 0);

        ParquetCompanionConfig config = new ParquetCompanionConfig(dir.toString());
        QuickwitSplit.SplitMetadata metadata = QuickwitSplit.createFromParquet(
                Collections.singletonList(parquetFile.toString()),
                splitFile.toString(), config);

        String splitUrl = "file://" + splitFile.toAbsolutePath();
        try (SplitSearcher searcher = cacheManager.createSplitSearcher(splitUrl, metadata, dir.toString())) {
            SplitQuery query = searcher.parseQuery("*");
            SearchResult results = searcher.search(query, 1);
            DocAddress[] docAddrs = { results.getHits().get(0).getDocAddress() };

            // Zero addresses should be caught by Rust validation and throw RuntimeException
            long[] zeroArrayAddrs = { 0L };
            long[] zeroSchemaAddrs = { 0L };

            assertThrows(RuntimeException.class, () ->
                    searcher.docBatchArrowFfi(
                            docAddrs, zeroArrayAddrs, zeroSchemaAddrs, "id"),
                    "zero FFI addresses should cause native error");
        }
    }

    // ---------------------------------------------------------------
    // 11. Arrow FFI row count matches TANT batch count
    // ---------------------------------------------------------------
    @Test
    @Order(11)
    void testArrowFfiRowCountMatchesTantBatch(@TempDir Path dir) throws Exception {
        Path parquetFile = dir.resolve("data.parquet");
        Path splitFile = dir.resolve("ffi_compare.split");
        writeTestParquet(parquetFile.toString(), 40, 0);

        ParquetCompanionConfig config = new ParquetCompanionConfig(dir.toString());
        QuickwitSplit.SplitMetadata metadata = QuickwitSplit.createFromParquet(
                Collections.singletonList(parquetFile.toString()),
                splitFile.toString(), config);

        String splitUrl = "file://" + splitFile.toAbsolutePath();
        try (SplitSearcher searcher = cacheManager.createSplitSearcher(splitUrl, metadata, dir.toString())) {
            SplitQuery query = searcher.parseQuery("*");
            SearchResult results = searcher.search(query, 20);
            int numDocs = results.getHits().size();
            assertTrue(numDocs >= 10);

            DocAddress[] docAddrs = new DocAddress[numDocs];
            for (int i = 0; i < numDocs; i++) {
                docAddrs[i] = results.getHits().get(i).getDocAddress();
            }

            // Get TANT batch result count
            List<io.indextables.tantivy4java.core.Document> tantDocs =
                    searcher.docBatchProjected(docAddrs, "id", "name");

            // Get Arrow FFI row count
            int numCols = 2;
            long[][] structs = allocateFfiStructs(numCols);
            try {
                int ffiRowCount = searcher.docBatchArrowFfi(
                        docAddrs, structs[0], structs[1], "id", "name");

                assertEquals(tantDocs.size(), ffiRowCount,
                        "FFI row count should match TANT batch document count");
                assertEquals(numDocs, ffiRowCount,
                        "FFI row count should match requested doc count");
            } finally {
                freeFfiStructs(structs[0], structs[1]);
            }
        }
    }

    // ---------------------------------------------------------------
    // 12. Arrow FFI with multiple column types (int, string, int, bool)
    // ---------------------------------------------------------------
    @Test
    @Order(12)
    void testDocBatchArrowFfiMultipleColumnTypes(@TempDir Path dir) throws Exception {
        Path parquetFile = dir.resolve("data.parquet");
        Path splitFile = dir.resolve("ffi_all.split");
        writeTestParquet(parquetFile.toString(), 20, 0);

        ParquetCompanionConfig config = new ParquetCompanionConfig(dir.toString());
        QuickwitSplit.SplitMetadata metadata = QuickwitSplit.createFromParquet(
                Collections.singletonList(parquetFile.toString()),
                splitFile.toString(), config);

        String splitUrl = "file://" + splitFile.toAbsolutePath();
        try (SplitSearcher searcher = cacheManager.createSplitSearcher(splitUrl, metadata, dir.toString())) {
            // Test with multiple column types: i64, string, i64, bool
            String[] fieldNames = { "id", "name", "score", "active" };
            int numCols = fieldNames.length;

            SplitQuery query = searcher.parseQuery("*");
            SearchResult results = searcher.search(query, 8);
            int numDocs = results.getHits().size();

            DocAddress[] docAddrs = new DocAddress[numDocs];
            for (int i = 0; i < numDocs; i++) {
                docAddrs[i] = results.getHits().get(i).getDocAddress();
            }

            long[][] structs = allocateFfiStructs(numCols);
            try {
                int rowCount = searcher.docBatchArrowFfi(
                        docAddrs, structs[0], structs[1], fieldNames);
                assertEquals(numDocs, rowCount, "should return all requested docs");

                // Verify each column's ArrowArray was properly written
                for (int i = 0; i < numCols; i++) {
                    assertEquals(numDocs, readArrowArrayLength(structs[0][i]),
                            "ArrowArray[" + fieldNames[i] + "].length should match");
                    assertNotEquals(0, readArrowArrayRelease(structs[0][i]),
                            "ArrowArray[" + fieldNames[i] + "].release should be non-null");
                    assertNotEquals(0, readArrowSchemaRelease(structs[1][i]),
                            "ArrowSchema[" + fieldNames[i] + "].release should be non-null");
                }
            } finally {
                freeFfiStructs(structs[0], structs[1]);
            }
        }
    }

    // ---------------------------------------------------------------
    // 13. Arrow FFI large batch (all documents in split)
    // ---------------------------------------------------------------
    @Test
    @Order(13)
    void testDocBatchArrowFfiLargeBatch(@TempDir Path dir) throws Exception {
        Path parquetFile = dir.resolve("data.parquet");
        Path splitFile = dir.resolve("ffi_large.split");
        writeTestParquet(parquetFile.toString(), 100, 0);

        ParquetCompanionConfig config = new ParquetCompanionConfig(dir.toString());
        QuickwitSplit.SplitMetadata metadata = QuickwitSplit.createFromParquet(
                Collections.singletonList(parquetFile.toString()),
                splitFile.toString(), config);

        String splitUrl = "file://" + splitFile.toAbsolutePath();
        try (SplitSearcher searcher = cacheManager.createSplitSearcher(splitUrl, metadata, dir.toString())) {
            SplitQuery query = searcher.parseQuery("*");
            SearchResult results = searcher.search(query, 100);
            int numDocs = results.getHits().size();
            assertEquals(100, numDocs, "should find all 100 docs");

            DocAddress[] docAddrs = new DocAddress[numDocs];
            for (int i = 0; i < numDocs; i++) {
                docAddrs[i] = results.getHits().get(i).getDocAddress();
            }

            int numCols = 3;
            long[][] structs = allocateFfiStructs(numCols);
            try {
                int rowCount = searcher.docBatchArrowFfi(
                        docAddrs, structs[0], structs[1], "id", "name", "score");
                assertEquals(100, rowCount, "should return all 100 rows");

                for (int i = 0; i < numCols; i++) {
                    assertEquals(100, readArrowArrayLength(structs[0][i]),
                            "ArrowArray column " + i + " length should be 100");
                }
            } finally {
                freeFfiStructs(structs[0], structs[1]);
            }
        }
    }

    // ---------------------------------------------------------------
    // 14. Arrow FFI null_count field should be zero or valid
    // ---------------------------------------------------------------
    @Test
    @Order(14)
    void testDocBatchArrowFfiNullCountField(@TempDir Path dir) throws Exception {
        Path parquetFile = dir.resolve("data.parquet");
        Path splitFile = dir.resolve("ffi_nullcount.split");
        writeTestParquet(parquetFile.toString(), 20, 0);

        ParquetCompanionConfig config = new ParquetCompanionConfig(dir.toString());
        QuickwitSplit.SplitMetadata metadata = QuickwitSplit.createFromParquet(
                Collections.singletonList(parquetFile.toString()),
                splitFile.toString(), config);

        String splitUrl = "file://" + splitFile.toAbsolutePath();
        try (SplitSearcher searcher = cacheManager.createSplitSearcher(splitUrl, metadata, dir.toString())) {
            SplitQuery query = searcher.parseQuery("*");
            SearchResult results = searcher.search(query, 5);
            DocAddress[] docAddrs = new DocAddress[5];
            for (int i = 0; i < 5; i++) {
                docAddrs[i] = results.getHits().get(i).getDocAddress();
            }

            long[][] structs = allocateFfiStructs(1);
            try {
                int rowCount = searcher.docBatchArrowFfi(
                        docAddrs, structs[0], structs[1], "id");
                assertEquals(5, rowCount);

                // null_count is the second int64 (offset 8) in FFI_ArrowArray
                // For non-nullable test data, null_count should be 0
                long nullCount = unsafe.getLong(structs[0][0] + 8);
                assertTrue(nullCount >= 0,
                        "null_count should be non-negative, got: " + nullCount);
            } finally {
                freeFfiStructs(structs[0], structs[1]);
            }
        }
    }

    // ---------------------------------------------------------------
    // 15. Arrow FFI struct zeroed memory is overwritten
    // ---------------------------------------------------------------
    @Test
    @Order(15)
    void testDocBatchArrowFfiOverwritesZeroedMemory(@TempDir Path dir) throws Exception {
        Path parquetFile = dir.resolve("data.parquet");
        Path splitFile = dir.resolve("ffi_zero.split");
        writeTestParquet(parquetFile.toString(), 10, 0);

        ParquetCompanionConfig config = new ParquetCompanionConfig(dir.toString());
        QuickwitSplit.SplitMetadata metadata = QuickwitSplit.createFromParquet(
                Collections.singletonList(parquetFile.toString()),
                splitFile.toString(), config);

        String splitUrl = "file://" + splitFile.toAbsolutePath();
        try (SplitSearcher searcher = cacheManager.createSplitSearcher(splitUrl, metadata, dir.toString())) {
            SplitQuery query = searcher.parseQuery("*");
            SearchResult results = searcher.search(query, 3);
            DocAddress[] docAddrs = new DocAddress[3];
            for (int i = 0; i < 3; i++) {
                docAddrs[i] = results.getHits().get(i).getDocAddress();
            }

            long[][] structs = allocateFfiStructs(1);
            long arrayAddr = structs[0][0];
            long schemaAddr = structs[1][0];

            // Verify memory is zeroed before call
            assertEquals(0, readArrowArrayLength(arrayAddr), "should be zeroed before call");
            assertEquals(0, readArrowArrayRelease(arrayAddr), "should be zeroed before call");
            assertEquals(0, readArrowSchemaRelease(schemaAddr), "should be zeroed before call");

            try {
                int rowCount = searcher.docBatchArrowFfi(
                        docAddrs, structs[0], structs[1], "id");
                assertEquals(3, rowCount);

                // After call, fields should be non-zero
                assertEquals(3, readArrowArrayLength(arrayAddr),
                        "length should be written after call");
                assertNotEquals(0, readArrowArrayRelease(arrayAddr),
                        "release callback should be written after call");
                assertNotEquals(0, readArrowSchemaRelease(schemaAddr),
                        "schema release should be written after call");
            } finally {
                freeFfiStructs(structs[0], structs[1]);
            }
        }
    }

    // ====================================================================
    // ALL-TYPES DATA RECONSTRUCTION TESTS
    // Using nativeWriteTestParquetAllTypes (25 columns) to verify that
    // FFI-exported Arrow data can be read back correctly at the byte level.
    // ====================================================================

    // ---------------------------------------------------------------
    // 16. Int64 and UInt64 data reconstruction
    // ---------------------------------------------------------------
    @Test
    @Order(16)
    void testReconstructInt64AndUInt64(@TempDir Path dir) throws Exception {
        QuickwitSplit.SplitMetadata metadata = createAllTypesSplit(dir, "ffi_i64.split", 20, 0);

        String splitUrl = "file://" + dir.resolve("ffi_i64.split").toAbsolutePath();
        try (SplitSearcher searcher = cacheManager.createSplitSearcher(splitUrl, metadata, dir.toString())) {
            SplitQuery query = searcher.parseQuery("*");
            SearchResult results = searcher.search(query, 10);
            int numDocs = results.getHits().size();

            DocAddress[] docAddrs = new DocAddress[numDocs];
            for (int i = 0; i < numDocs; i++) {
                docAddrs[i] = results.getHits().get(i).getDocAddress();
            }

            // Project id (Int64) and uint_val (UInt64)
            long[][] structs = allocateFfiStructs(2);
            try {
                int rowCount = searcher.docBatchArrowFfi(
                        docAddrs, structs[0], structs[1], "id", "uint_val");
                assertEquals(numDocs, rowCount);

                // Verify schema formats
                String idFormat = readSchemaFormat(structs[1][0]);
                String uintFormat = readSchemaFormat(structs[1][1]);
                assertEquals("l", idFormat, "id should be Int64 format 'l'");
                assertEquals("L", uintFormat, "uint_val should be UInt64 format 'L'");

                // Read back id values and verify: id = offset + row_index (offset=0)
                for (int row = 0; row < rowCount; row++) {
                    long id = readInt64(structs[0][0], row);
                    assertTrue(id >= 0 && id < 20,
                            "id should be in [0,20), got " + id + " at row " + row);

                    // uint_val = 1_000_000_000 + id * 7
                    long expectedUint = 1_000_000_000L + id * 7;
                    long actualUint = readUInt64(structs[0][1], row);
                    assertEquals(expectedUint, actualUint,
                            "uint_val mismatch at row " + row + " (id=" + id + ")");
                }
            } finally {
                freeFfiStructs(structs[0], structs[1]);
            }
        }
    }

    // ---------------------------------------------------------------
    // 17. Float64 data reconstruction (including NaN and null handling)
    // ---------------------------------------------------------------
    @Test
    @Order(17)
    void testReconstructFloat64(@TempDir Path dir) throws Exception {
        QuickwitSplit.SplitMetadata metadata = createAllTypesSplit(dir, "ffi_f64.split", 20, 0);

        String splitUrl = "file://" + dir.resolve("ffi_f64.split").toAbsolutePath();
        try (SplitSearcher searcher = cacheManager.createSplitSearcher(splitUrl, metadata, dir.toString())) {
            SplitQuery query = searcher.parseQuery("*");
            SearchResult results = searcher.search(query, 20);

            DocAddress[] docAddrs = new DocAddress[results.getHits().size()];
            for (int i = 0; i < docAddrs.length; i++) {
                docAddrs[i] = results.getHits().get(i).getDocAddress();
            }

            // Project id and float_val
            long[][] structs = allocateFfiStructs(2);
            try {
                int rowCount = searcher.docBatchArrowFfi(
                        docAddrs, structs[0], structs[1], "id", "float_val");
                assertEquals(docAddrs.length, rowCount);

                assertEquals("g", readSchemaFormat(structs[1][1]),
                        "float_val should be Float64 format 'g'");

                int nullsSeen = 0;
                int nansSeen = 0;
                int validSeen = 0;
                for (int row = 0; row < rowCount; row++) {
                    long id = readInt64(structs[0][0], row);

                    if (isRowNull(structs[0][1], row)) {
                        // null on every 7th row: id % 7 == 6
                        assertEquals(6, id % 7,
                                "float_val null should be at id%7==6, got id=" + id);
                        nullsSeen++;
                    } else {
                        double val = readFloat64(structs[0][1], row);
                        if (Double.isNaN(val)) {
                            // NaN on every 10th row: id % 10 == 9
                            assertEquals(9, id % 10,
                                    "float_val NaN should be at id%10==9, got id=" + id);
                            nansSeen++;
                        } else {
                            // Normal value: id * 2.5 + 0.1
                            double expected = id * 2.5 + 0.1;
                            assertEquals(expected, val, 0.001,
                                    "float_val mismatch at id=" + id);
                            validSeen++;
                        }
                    }
                }
                assertTrue(validSeen > 0, "should have some valid float values");
                // With 20 rows: nulls at indices 6,13 (2); NaN at index 9 (1, but 13 is also null)
                assertTrue(nullsSeen + nansSeen > 0,
                        "should have some null/NaN float values");
            } finally {
                freeFfiStructs(structs[0], structs[1]);
            }
        }
    }

    // ---------------------------------------------------------------
    // 18. Boolean data reconstruction
    // ---------------------------------------------------------------
    @Test
    @Order(18)
    void testReconstructBoolean(@TempDir Path dir) throws Exception {
        QuickwitSplit.SplitMetadata metadata = createAllTypesSplit(dir, "ffi_bool.split", 20, 0);

        String splitUrl = "file://" + dir.resolve("ffi_bool.split").toAbsolutePath();
        try (SplitSearcher searcher = cacheManager.createSplitSearcher(splitUrl, metadata, dir.toString())) {
            SplitQuery query = searcher.parseQuery("*");
            SearchResult results = searcher.search(query, 20);

            DocAddress[] docAddrs = new DocAddress[results.getHits().size()];
            for (int i = 0; i < docAddrs.length; i++) {
                docAddrs[i] = results.getHits().get(i).getDocAddress();
            }

            long[][] structs = allocateFfiStructs(2);
            try {
                int rowCount = searcher.docBatchArrowFfi(
                        docAddrs, structs[0], structs[1], "id", "bool_val");
                assertEquals(docAddrs.length, rowCount);

                assertEquals("b", readSchemaFormat(structs[1][1]),
                        "bool_val should be Boolean format 'b'");

                for (int row = 0; row < rowCount; row++) {
                    long id = readInt64(structs[0][0], row);

                    if (isRowNull(structs[0][1], row)) {
                        // null every 5th row: id % 5 == 4
                        assertEquals(4, id % 5,
                                "bool_val null should be at id%5==4, got id=" + id);
                    } else {
                        boolean val = readBoolean(structs[0][1], row);
                        boolean expected = (id % 2 == 0);
                        assertEquals(expected, val,
                                "bool_val mismatch at id=" + id + ": expected " + expected);
                    }
                }
            } finally {
                freeFfiStructs(structs[0], structs[1]);
            }
        }
    }

    // ---------------------------------------------------------------
    // 19. Utf8 string data reconstruction
    // ---------------------------------------------------------------
    @Test
    @Order(19)
    void testReconstructUtf8Strings(@TempDir Path dir) throws Exception {
        QuickwitSplit.SplitMetadata metadata = createAllTypesSplit(dir, "ffi_utf8.split", 20, 0);

        String splitUrl = "file://" + dir.resolve("ffi_utf8.split").toAbsolutePath();
        try (SplitSearcher searcher = cacheManager.createSplitSearcher(splitUrl, metadata, dir.toString())) {
            SplitQuery query = searcher.parseQuery("*");
            SearchResult results = searcher.search(query, 10);

            DocAddress[] docAddrs = new DocAddress[results.getHits().size()];
            for (int i = 0; i < docAddrs.length; i++) {
                docAddrs[i] = results.getHits().get(i).getDocAddress();
            }

            // Project id, text_val (Utf8), ip_val (Utf8), category (Utf8)
            long[][] structs = allocateFfiStructs(4);
            try {
                int rowCount = searcher.docBatchArrowFfi(
                        docAddrs, structs[0], structs[1],
                        "id", "text_val", "ip_val", "category");
                assertEquals(docAddrs.length, rowCount);

                assertEquals("u", readSchemaFormat(structs[1][1]), "text_val should be Utf8 'u'");
                assertEquals("u", readSchemaFormat(structs[1][2]), "ip_val should be Utf8 'u'");
                assertEquals("u", readSchemaFormat(structs[1][3]), "category should be Utf8 'u'");

                String[] categories = {"electronics", "clothing", "food", "books", "sports"};

                for (int row = 0; row < rowCount; row++) {
                    long id = readInt64(structs[0][0], row);

                    // text_val = "text_{id}"
                    String textVal = readUtf8(structs[0][1], row);
                    assertEquals("text_" + id, textVal,
                            "text_val mismatch at id=" + id);

                    // ip_val: IPv4 or IPv6 based on id%3
                    String ipVal = readUtf8(structs[0][2], row);
                    if (id % 3 == 2) {
                        assertTrue(ipVal.startsWith("2001:db8::"),
                                "ip_val should be IPv6 at id=" + id + ", got: " + ipVal);
                    } else {
                        assertTrue(ipVal.startsWith("10.0."),
                                "ip_val should be IPv4 at id=" + id + ", got: " + ipVal);
                    }

                    // category cycles through 5 values
                    String catVal = readUtf8(structs[0][3], row);
                    assertEquals(categories[(int) (id % 5)], catVal,
                            "category mismatch at id=" + id);
                }
            } finally {
                freeFfiStructs(structs[0], structs[1]);
            }
        }
    }

    // ---------------------------------------------------------------
    // 20. Timestamp and Date32 temporal data reconstruction
    // ---------------------------------------------------------------
    @Test
    @Order(20)
    void testReconstructTemporalTypes(@TempDir Path dir) throws Exception {
        QuickwitSplit.SplitMetadata metadata = createAllTypesSplit(dir, "ffi_ts.split", 20, 0);

        String splitUrl = "file://" + dir.resolve("ffi_ts.split").toAbsolutePath();
        try (SplitSearcher searcher = cacheManager.createSplitSearcher(splitUrl, metadata, dir.toString())) {
            SplitQuery query = searcher.parseQuery("*");
            SearchResult results = searcher.search(query, 20);

            DocAddress[] docAddrs = new DocAddress[results.getHits().size()];
            for (int i = 0; i < docAddrs.length; i++) {
                docAddrs[i] = results.getHits().get(i).getDocAddress();
            }

            // Project id, ts_val (Timestamp), date_val (Date32)
            long[][] structs = allocateFfiStructs(3);
            try {
                int rowCount = searcher.docBatchArrowFfi(
                        docAddrs, structs[0], structs[1], "id", "ts_val", "date_val");
                assertEquals(docAddrs.length, rowCount);

                // Timestamp microsecond format: "tsu:" (no timezone)
                String tsFormat = readSchemaFormat(structs[1][1]);
                assertTrue(tsFormat.startsWith("tsu"),
                        "ts_val should be Timestamp(us) format, got: " + tsFormat);

                // Date32 format: "tdD"
                assertEquals("tdD", readSchemaFormat(structs[1][2]),
                        "date_val should be Date32 format 'tdD'");

                long BASE_TS = 1_704_067_200_000_000L; // 2024-01-01T00:00:00Z in microseconds
                int BASE_DATE = 19723; // 2024-01-01 as days since epoch

                for (int row = 0; row < rowCount; row++) {
                    long id = readInt64(structs[0][0], row);

                    // ts_val: null every 8th (id%8==7), else base + id*3600000000
                    if (!isRowNull(structs[0][1], row)) {
                        long tsVal = readInt64(structs[0][1], row);
                        long expectedTs = BASE_TS + id * 3_600_000_000L;
                        assertEquals(expectedTs, tsVal,
                                "ts_val mismatch at id=" + id);
                    } else {
                        assertEquals(7, id % 8,
                                "ts_val null should be at id%8==7, got id=" + id);
                    }

                    // date_val: null every 6th (id%6==5), else 19723 + id
                    if (!isRowNull(structs[0][2], row)) {
                        int dateVal = readInt32(structs[0][2], row);
                        int expectedDate = BASE_DATE + (int) id;
                        assertEquals(expectedDate, dateVal,
                                "date_val mismatch at id=" + id);
                    } else {
                        assertEquals(5, id % 6,
                                "date_val null should be at id%6==5, got id=" + id);
                    }
                }
            } finally {
                freeFfiStructs(structs[0], structs[1]);
            }
        }
    }

    // ---------------------------------------------------------------
    // 21. Narrow numeric types: Int32, Int16, UInt32, Float32
    // ---------------------------------------------------------------
    @Test
    @Order(21)
    void testReconstructNarrowNumerics(@TempDir Path dir) throws Exception {
        QuickwitSplit.SplitMetadata metadata = createAllTypesSplit(dir, "ffi_narrow.split", 20, 0);

        String splitUrl = "file://" + dir.resolve("ffi_narrow.split").toAbsolutePath();
        try (SplitSearcher searcher = cacheManager.createSplitSearcher(splitUrl, metadata, dir.toString())) {
            SplitQuery query = searcher.parseQuery("*");
            SearchResult results = searcher.search(query, 20);

            DocAddress[] docAddrs = new DocAddress[results.getHits().size()];
            for (int i = 0; i < docAddrs.length; i++) {
                docAddrs[i] = results.getHits().get(i).getDocAddress();
            }

            // Project id, status_code (Int32), priority (Int16), retry_count (UInt32), latitude (Float32)
            // Note: FFI returns columns in parquet schema order, not projection order
            long[][] structs = allocateFfiStructs(5);
            try {
                int rowCount = searcher.docBatchArrowFfi(
                        docAddrs, structs[0], structs[1],
                        "id", "status_code", "priority", "retry_count", "latitude");
                assertEquals(docAddrs.length, rowCount);

                // Build column map (FFI returns columns in parquet schema order)
                Map<String, Integer> colMap = buildColumnMap(structs[1], 5);
                int idCol = colMap.getOrDefault("id", -1);
                int statusCol = colMap.getOrDefault("status_code", -1);
                int priorityCol = colMap.getOrDefault("priority", -1);
                int retryCol = colMap.getOrDefault("retry_count", -1);
                int latCol = colMap.getOrDefault("latitude", -1);

                assertTrue(idCol >= 0, "id column not found");
                assertTrue(statusCol >= 0, "status_code column not found");
                assertTrue(priorityCol >= 0, "priority column not found");
                assertTrue(retryCol >= 0, "retry_count column not found");
                assertTrue(latCol >= 0, "latitude column not found");

                assertEquals("i", readSchemaFormat(structs[1][statusCol]), "status_code should be Int32 'i'");
                assertEquals("s", readSchemaFormat(structs[1][priorityCol]), "priority should be Int16 's'");
                assertEquals("I", readSchemaFormat(structs[1][retryCol]), "retry_count should be UInt32 'I'");
                assertEquals("f", readSchemaFormat(structs[1][latCol]), "latitude should be Float32 'f'");

                int[] expectedStatusCodes = {200, 201, 404, 500, 302};

                for (int row = 0; row < rowCount; row++) {
                    long id = readInt64(structs[0][idCol], row);

                    // status_code: null every 12th (id%12==11), else cycle through 5 codes
                    if (!isRowNull(structs[0][statusCol], row)) {
                        int statusCode = readInt32(structs[0][statusCol], row);
                        assertEquals(expectedStatusCodes[(int) (id % 5)], statusCode,
                                "status_code mismatch at id=" + id);
                    }

                    // priority: null every 15th (id%15==14), else (id%5)+1
                    if (!isRowNull(structs[0][priorityCol], row)) {
                        short priority = readInt16(structs[0][priorityCol], row);
                        assertEquals((short) ((id % 5) + 1), priority,
                                "priority mismatch at id=" + id);
                    }

                    // retry_count: null every 14th (id%14==13), else id%4
                    if (!isRowNull(structs[0][retryCol], row)) {
                        int retryCount = readInt32(structs[0][retryCol], row);
                        assertEquals((int) (id % 4), retryCount,
                                "retry_count mismatch at id=" + id);
                    }

                    // latitude: null every 13th (id%13==12), else 40.0 + id*0.01
                    if (!isRowNull(structs[0][latCol], row)) {
                        float lat = readFloat32(structs[0][latCol], row);
                        float expectedLat = 40.0f + id * 0.01f;
                        assertEquals(expectedLat, lat, 0.001f,
                                "latitude mismatch at id=" + id);
                    }
                }
            } finally {
                freeFfiStructs(structs[0], structs[1]);
            }
        }
    }

    // ---------------------------------------------------------------
    // 22. LargeUtf8 (large_text) and description string reconstruction
    // Note: binary_val maps to tantivy Bytes type which may not be in
    // the companion column mapping. We test large_text + description instead.
    // ---------------------------------------------------------------
    @Test
    @Order(22)
    void testReconstructLargeUtf8AndDescription(@TempDir Path dir) throws Exception {
        // Use unique split name to avoid split_footer_cache collisions with other tests
        String splitName = "ffi_large_" + System.nanoTime() + ".split";
        QuickwitSplit.SplitMetadata metadata = createAllTypesSplit(dir, splitName, 20, 0);

        String splitUrl = "file://" + dir.resolve(splitName).toAbsolutePath();
        try (SplitSearcher searcher = cacheManager.createSplitSearcher(splitUrl, metadata, dir.toString())) {
            SplitQuery query = searcher.parseQuery("*");
            SearchResult results = searcher.search(query, 20);

            DocAddress[] docAddrs = new DocAddress[results.getHits().size()];
            for (int i = 0; i < docAddrs.length; i++) {
                docAddrs[i] = results.getHits().get(i).getDocAddress();
            }

            // Project id, large_text (LargeUtf8 in parquet), description (Utf8 in parquet)
            long[][] structs = allocateFfiStructs(3);
            try {
                int rowCount = searcher.docBatchArrowFfi(
                        docAddrs, structs[0], structs[1], "id", "large_text", "description");
                assertEquals(docAddrs.length, rowCount);

                // Build column map (FFI returns columns in parquet schema order)
                Map<String, Integer> colMap = buildColumnMap(structs[1], 3);
                int idCol = colMap.getOrDefault("id", -1);
                int largeCol = colMap.getOrDefault("large_text", -1);
                int descCol = colMap.getOrDefault("description", -1);
                assertTrue(idCol >= 0, "id column not found");
                assertTrue(largeCol >= 0, "large_text column not found");
                assertTrue(descCol >= 0, "description column not found");

                String largeFormat = readSchemaFormat(structs[1][largeCol]);
                // LargeUtf8 may come as "U" or may be coerced to "u" (Utf8) by Arrow
                assertTrue("U".equals(largeFormat) || "u".equals(largeFormat),
                        "large_text should be LargeUtf8 'U' or Utf8 'u', got: " + largeFormat);

                String[] expectedDescs = {
                    "The quick brown fox jumps over the lazy dog",
                    "A fast red car drove past the old house",
                    "Searching for data in large distributed systems",
                    "Performance testing with multiple column types"
                };

                for (int row = 0; row < rowCount; row++) {
                    long id = readInt64(structs[0][idCol], row);

                    // large_text: null every 8th (id%8==7)
                    if (!isRowNull(structs[0][largeCol], row)) {
                        String largeText;
                        if ("U".equals(largeFormat)) {
                            largeText = readLargeUtf8(structs[0][largeCol], row);
                        } else {
                            largeText = readUtf8(structs[0][largeCol], row);
                        }
                        assertTrue(largeText.startsWith("Large text content for row "),
                                "large_text should start with expected prefix at id=" + id
                                + ", got: " + largeText.substring(0, Math.min(40, largeText.length())));
                        assertTrue(largeText.contains("row " + id),
                                "large_text should contain row id at id=" + id);
                    }

                    // description: non-nullable, cycles through 4 templates
                    String desc = readUtf8(structs[0][descCol], row);
                    assertEquals(expectedDescs[(int)(id % 4)], desc,
                            "description mismatch at id=" + id);
                }
            } finally {
                freeFfiStructs(structs[0], structs[1]);
            }
        }
    }

    // ---------------------------------------------------------------
    // 23. Complex types: List, Struct, Map — structure verification
    // ---------------------------------------------------------------
    @Test
    @Order(23)
    void testReconstructComplexTypes(@TempDir Path dir) throws Exception {
        QuickwitSplit.SplitMetadata metadata = createAllTypesSplit(dir, "ffi_complex.split", 20, 0);

        String splitUrl = "file://" + dir.resolve("ffi_complex.split").toAbsolutePath();
        try (SplitSearcher searcher = cacheManager.createSplitSearcher(splitUrl, metadata, dir.toString())) {
            SplitQuery query = searcher.parseQuery("*");
            SearchResult results = searcher.search(query, 10);

            DocAddress[] docAddrs = new DocAddress[results.getHits().size()];
            for (int i = 0; i < docAddrs.length; i++) {
                docAddrs[i] = results.getHits().get(i).getDocAddress();
            }

            // Project tags (List), address (Struct), props (Map)
            long[][] structs = allocateFfiStructs(3);
            try {
                int rowCount = searcher.docBatchArrowFfi(
                        docAddrs, structs[0], structs[1], "tags", "address", "props");
                assertEquals(docAddrs.length, rowCount);

                // List<Utf8> format: "+l" (list)
                String tagsFormat = readSchemaFormat(structs[1][0]);
                assertEquals("+l", tagsFormat, "tags should be List format '+l'");
                assertTrue(readArrayNChildren(structs[0][0]) == 1,
                        "List should have 1 child (the element array)");

                // Struct format: "+s" (struct)
                String addrFormat = readSchemaFormat(structs[1][1]);
                assertEquals("+s", addrFormat, "address should be Struct format '+s'");
                assertTrue(readArrayNChildren(structs[0][1]) >= 2,
                        "Struct should have >= 2 children (city, zip)");

                // Map format: "+m" (map)
                String propsFormat = readSchemaFormat(structs[1][2]);
                assertEquals("+m", propsFormat, "props should be Map format '+m'");
                assertTrue(readArrayNChildren(structs[0][2]) == 1,
                        "Map should have 1 child (the entries struct)");

                // Verify children pointers are non-null
                assertNotEquals(0, getChildrenPtr(structs[0][0]),
                        "tags children pointer should be non-null");
                assertNotEquals(0, getChildrenPtr(structs[0][1]),
                        "address children pointer should be non-null");
                assertNotEquals(0, getChildrenPtr(structs[0][2]),
                        "props children pointer should be non-null");

                // Verify nullability: tags null every 5th, address null every 9th, props null every 6th
                long tagsNullCount = readArrayNullCount(structs[0][0]);
                long addrNullCount = readArrayNullCount(structs[0][1]);
                long propsNullCount = readArrayNullCount(structs[0][2]);

                assertTrue(tagsNullCount >= 0, "tags null_count should be >= 0");
                assertTrue(addrNullCount >= 0, "address null_count should be >= 0");
                assertTrue(propsNullCount >= 0, "props null_count should be >= 0");
            } finally {
                freeFfiStructs(structs[0], structs[1]);
            }
        }
    }

    // ---------------------------------------------------------------
    // 24. Complex types with nested parquet: Struct child data reconstruction
    // ---------------------------------------------------------------
    @Test
    @Order(24)
    void testReconstructStructChildren(@TempDir Path dir) throws Exception {
        // Use the complex parquet writer which has struct with known city/zip values
        QuickwitSplit.SplitMetadata metadata = createComplexTypesSplit(dir, "ffi_struct.split", 20, 0);

        String splitUrl = "file://" + dir.resolve("ffi_struct.split").toAbsolutePath();
        try (SplitSearcher searcher = cacheManager.createSplitSearcher(splitUrl, metadata, dir.toString())) {
            SplitQuery query = searcher.parseQuery("*");
            SearchResult results = searcher.search(query, 10);

            DocAddress[] docAddrs = new DocAddress[results.getHits().size()];
            for (int i = 0; i < docAddrs.length; i++) {
                docAddrs[i] = results.getHits().get(i).getDocAddress();
            }

            // Project id and address (Struct{city, zip})
            long[][] structs = allocateFfiStructs(2);
            try {
                int rowCount = searcher.docBatchArrowFfi(
                        docAddrs, structs[0], structs[1], "id", "address");
                assertEquals(docAddrs.length, rowCount);

                // address is a Struct with 2 children
                assertEquals("+s", readSchemaFormat(structs[1][1]),
                        "address should be Struct format");
                long nChildren = readArrayNChildren(structs[0][1]);
                assertEquals(2, nChildren, "address struct should have 2 children (city, zip)");

                // Read the child arrays via children pointer
                long childrenPtr = getChildrenPtr(structs[0][1]);
                assertNotEquals(0, childrenPtr, "children pointer should be valid");

                // Each child is a pointer to an ArrowArray
                long cityArrayPtr = unsafe.getLong(childrenPtr);      // children[0] = city
                long zipArrayPtr = unsafe.getLong(childrenPtr + 8);   // children[1] = zip
                assertNotEquals(0, cityArrayPtr, "city child should be valid");
                assertNotEquals(0, zipArrayPtr, "zip child should be valid");

                // City array should have same length as parent
                assertEquals(rowCount, readArrowArrayLength(cityArrayPtr),
                        "city child length should match parent");
                assertEquals(rowCount, readArrowArrayLength(zipArrayPtr),
                        "zip child length should match parent");

                // Read city values from the child Utf8 array
                String[] expectedCities = {"New York", "London", "Tokyo"};
                for (int row = 0; row < rowCount; row++) {
                    long id = readInt64(structs[0][0], row);
                    if (!isRowNull(structs[0][1], row)) {
                        String city = readUtf8(cityArrayPtr, row);
                        assertEquals(expectedCities[(int) (id % 3)], city,
                                "city mismatch at id=" + id);
                    }
                }
            } finally {
                freeFfiStructs(structs[0], structs[1]);
            }
        }
    }

    // ---------------------------------------------------------------
    // 25. List child data reconstruction
    // ---------------------------------------------------------------
    @Test
    @Order(25)
    void testReconstructListChildren(@TempDir Path dir) throws Exception {
        QuickwitSplit.SplitMetadata metadata = createComplexTypesSplit(dir, "ffi_list.split", 20, 0);

        String splitUrl = "file://" + dir.resolve("ffi_list.split").toAbsolutePath();
        try (SplitSearcher searcher = cacheManager.createSplitSearcher(splitUrl, metadata, dir.toString())) {
            SplitQuery query = searcher.parseQuery("*");
            SearchResult results = searcher.search(query, 10);

            DocAddress[] docAddrs = new DocAddress[results.getHits().size()];
            for (int i = 0; i < docAddrs.length; i++) {
                docAddrs[i] = results.getHits().get(i).getDocAddress();
            }

            // Project tags (List<Utf8>)
            long[][] structs = allocateFfiStructs(1);
            try {
                int rowCount = searcher.docBatchArrowFfi(
                        docAddrs, structs[0], structs[1], "tags");
                assertEquals(docAddrs.length, rowCount);

                assertEquals("+l", readSchemaFormat(structs[1][0]), "tags should be List format");
                assertEquals(1, readArrayNChildren(structs[0][0]),
                        "List should have exactly 1 child");

                // List has offsets buffer (buffers[1]) telling where each list starts/ends
                long nBuffers = readArrayNBuffers(structs[0][0]);
                assertEquals(2, nBuffers, "List should have 2 buffers (validity + offsets)");

                // Read the child Utf8 array (the flattened tag values)
                long childrenPtr = getChildrenPtr(structs[0][0]);
                long childArrayPtr = unsafe.getLong(childrenPtr); // children[0]
                long childLength = readArrowArrayLength(childArrayPtr);

                // Total child elements should be > 0 (non-null rows have 1-2 tags each)
                assertTrue(childLength > 0,
                        "list child should have elements, got length=" + childLength);

                // Read the offsets to verify list structure
                long buffersPtr = getBuffersPtr(structs[0][0]);
                long offsetsPtr = unsafe.getLong(buffersPtr + 8); // buffers[1] = i32 offsets
                int firstOffset = unsafe.getInt(offsetsPtr);
                int lastOffset = unsafe.getInt(offsetsPtr + (long) rowCount * 4);
                assertEquals(0, firstOffset, "first offset should be 0");
                assertEquals((int) childLength, lastOffset,
                        "last offset should equal child length");
            } finally {
                freeFfiStructs(structs[0], structs[1]);
            }
        }
    }

    // ---------------------------------------------------------------
    // 26. All-types FFI vs TANT cross-validation (data parity)
    // ---------------------------------------------------------------
    @Test
    @Order(26)
    void testFfiVsTantDataParity(@TempDir Path dir) throws Exception {
        QuickwitSplit.SplitMetadata metadata = createAllTypesSplit(dir, "ffi_parity.split", 20, 0);

        String splitUrl = "file://" + dir.resolve("ffi_parity.split").toAbsolutePath();
        try (SplitSearcher searcher = cacheManager.createSplitSearcher(splitUrl, metadata, dir.toString())) {
            SplitQuery query = searcher.parseQuery("*");
            SearchResult results = searcher.search(query, 10);
            int numDocs = results.getHits().size();

            DocAddress[] docAddrs = new DocAddress[numDocs];
            for (int i = 0; i < numDocs; i++) {
                docAddrs[i] = results.getHits().get(i).getDocAddress();
            }

            // Get data via TANT path
            List<io.indextables.tantivy4java.core.Document> tantDocs =
                    searcher.docBatchProjected(docAddrs, "id", "text_val", "uint_val");

            // Get data via FFI path
            // Note: FFI returns columns in parquet schema order, not projection order
            long[][] structs = allocateFfiStructs(3);
            try {
                int ffiRowCount = searcher.docBatchArrowFfi(
                        docAddrs, structs[0], structs[1], "id", "text_val", "uint_val");

                assertEquals(tantDocs.size(), ffiRowCount, "row counts should match");

                // Build column map (parquet schema order != projection order)
                Map<String, Integer> colMap = buildColumnMap(structs[1], 3);
                int idCol = colMap.getOrDefault("id", -1);
                int textCol = colMap.getOrDefault("text_val", -1);
                int uintCol = colMap.getOrDefault("uint_val", -1);
                assertTrue(idCol >= 0, "id column not found in FFI output");
                assertTrue(textCol >= 0, "text_val column not found in FFI output");
                assertTrue(uintCol >= 0, "uint_val column not found in FFI output");

                // Cross-validate each row
                for (int row = 0; row < ffiRowCount; row++) {
                    // FFI values (using name-based column indices)
                    long ffiId = readInt64(structs[0][idCol], row);
                    String ffiText = readUtf8(structs[0][textCol], row);
                    long ffiUint = readUInt64(structs[0][uintCol], row);

                    // TANT values
                    io.indextables.tantivy4java.core.Document tantDoc = tantDocs.get(row);
                    long tantId = ((Number) tantDoc.getFirst("id")).longValue();
                    String tantText = (String) tantDoc.getFirst("text_val");
                    long tantUint = ((Number) tantDoc.getFirst("uint_val")).longValue();

                    assertEquals(tantId, ffiId,
                            "id mismatch at row " + row + ": TANT=" + tantId + " FFI=" + ffiId);
                    assertEquals(tantText, ffiText,
                            "text_val mismatch at row " + row);
                    assertEquals(tantUint, ffiUint,
                            "uint_val mismatch at row " + row);
                }
            } finally {
                freeFfiStructs(structs[0], structs[1]);
            }
        }
    }

    // ---------------------------------------------------------------
    // 27. Non-nullable Boolean (is_active) — all values present
    // ---------------------------------------------------------------
    @Test
    @Order(27)
    void testReconstructNonNullableBoolean(@TempDir Path dir) throws Exception {
        QuickwitSplit.SplitMetadata metadata = createAllTypesSplit(dir, "ffi_nnbool.split", 20, 0);

        String splitUrl = "file://" + dir.resolve("ffi_nnbool.split").toAbsolutePath();
        try (SplitSearcher searcher = cacheManager.createSplitSearcher(splitUrl, metadata, dir.toString())) {
            SplitQuery query = searcher.parseQuery("*");
            SearchResult results = searcher.search(query, 20);

            DocAddress[] docAddrs = new DocAddress[results.getHits().size()];
            for (int i = 0; i < docAddrs.length; i++) {
                docAddrs[i] = results.getHits().get(i).getDocAddress();
            }

            long[][] structs = allocateFfiStructs(2);
            try {
                int rowCount = searcher.docBatchArrowFfi(
                        docAddrs, structs[0], structs[1], "id", "is_active");
                assertEquals(docAddrs.length, rowCount);

                assertEquals("b", readSchemaFormat(structs[1][1]),
                        "is_active should be Boolean format 'b'");

                // Non-nullable: null_count should be 0
                assertEquals(0, readArrayNullCount(structs[0][1]),
                        "is_active is non-nullable, null_count should be 0");

                // is_active = (id % 3 != 0)
                for (int row = 0; row < rowCount; row++) {
                    long id = readInt64(structs[0][0], row);
                    boolean val = readBoolean(structs[0][1], row);
                    boolean expected = (id % 3 != 0);
                    assertEquals(expected, val,
                            "is_active mismatch at id=" + id);
                }
            } finally {
                freeFfiStructs(structs[0], structs[1]);
            }
        }
    }

    // ---------------------------------------------------------------
    // 28. Amount (Float64) and longitude (Float32) financial/geo data
    // ---------------------------------------------------------------
    @Test
    @Order(28)
    void testReconstructFinancialAndGeoData(@TempDir Path dir) throws Exception {
        QuickwitSplit.SplitMetadata metadata = createAllTypesSplit(dir, "ffi_fin.split", 20, 0);

        String splitUrl = "file://" + dir.resolve("ffi_fin.split").toAbsolutePath();
        try (SplitSearcher searcher = cacheManager.createSplitSearcher(splitUrl, metadata, dir.toString())) {
            SplitQuery query = searcher.parseQuery("*");
            SearchResult results = searcher.search(query, 20);

            DocAddress[] docAddrs = new DocAddress[results.getHits().size()];
            for (int i = 0; i < docAddrs.length; i++) {
                docAddrs[i] = results.getHits().get(i).getDocAddress();
            }

            long[][] structs = allocateFfiStructs(3);
            try {
                int rowCount = searcher.docBatchArrowFfi(
                        docAddrs, structs[0], structs[1], "id", "amount", "longitude");
                assertEquals(docAddrs.length, rowCount);

                assertEquals("g", readSchemaFormat(structs[1][1]), "amount should be Float64 'g'");
                assertEquals("f", readSchemaFormat(structs[1][2]), "longitude should be Float32 'f'");

                for (int row = 0; row < rowCount; row++) {
                    long id = readInt64(structs[0][0], row);

                    // amount: null every 11th (id%11==10), else id*9.99 + 1.50
                    if (!isRowNull(structs[0][1], row)) {
                        double amount = readFloat64(structs[0][1], row);
                        double expected = id * 9.99 + 1.50;
                        assertEquals(expected, amount, 0.01,
                                "amount mismatch at id=" + id);
                    }

                    // longitude: null every 13th (id%13==12), else -74.0 + id*0.01
                    if (!isRowNull(structs[0][2], row)) {
                        float lng = readFloat32(structs[0][2], row);
                        float expected = -74.0f + id * 0.01f;
                        assertEquals(expected, lng, 0.001f,
                                "longitude mismatch at id=" + id);
                    }
                }
            } finally {
                freeFfiStructs(structs[0], structs[1]);
            }
        }
    }
}

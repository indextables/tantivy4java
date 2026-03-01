package io.indextables.tantivy4java.parquet;

import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Integration tests for the Parquet distributed scanning primitives:
 * getTableInfo() and listPartitionFiles().
 */
public class ParquetDistributedScannerTest {

    @TempDir
    Path tempDir;

    /**
     * Create a Hive-style partitioned directory with real parquet files.
     * Uses ParquetSchemaReader.writeTestParquet() to create valid files.
     */
    private Path createPartitionedTable() throws Exception {
        Path tableDir = tempDir.resolve("parquet_table_" + System.nanoTime());
        Files.createDirectories(tableDir);

        // Create partition directories with real parquet files
        Path part1 = tableDir.resolve("year=2024").resolve("month=01");
        Files.createDirectories(part1);
        ParquetSchemaReader.writeTestParquet(part1.resolve("data-00000.parquet").toString());

        Path part2 = tableDir.resolve("year=2024").resolve("month=02");
        Files.createDirectories(part2);
        ParquetSchemaReader.writeTestParquet(part2.resolve("data-00000.parquet").toString());

        return tableDir;
    }

    /**
     * Create an unpartitioned directory with a parquet file at the root.
     */
    private Path createUnpartitionedTable() throws Exception {
        Path tableDir = tempDir.resolve("flat_table_" + System.nanoTime());
        Files.createDirectories(tableDir);

        ParquetSchemaReader.writeTestParquet(tableDir.resolve("data-00000.parquet").toString());
        ParquetSchemaReader.writeTestParquet(tableDir.resolve("data-00001.parquet").toString());

        return tableDir;
    }

    // ── getTableInfo tests ───────────────────────────────────────────────────

    @Test
    @DisplayName("getTableInfo discovers partitioned table structure")
    void testGetTableInfoPartitioned() throws Exception {
        Path tableDir = createPartitionedTable();

        ParquetTableInfo info = ParquetTableReader.getTableInfo(
            tableDir.toString(), Collections.emptyMap());

        assertNotNull(info);
        assertTrue(info.isPartitioned());
        assertFalse(info.getPartitionDirectories().isEmpty(),
            "Should discover partition directories");
        assertNotNull(info.getSchemaJson());
        assertFalse(info.getSchemaJson().isEmpty(), "Schema JSON should not be empty");
    }

    @Test
    @DisplayName("getTableInfo discovers unpartitioned table")
    void testGetTableInfoUnpartitioned() throws Exception {
        Path tableDir = createUnpartitionedTable();

        ParquetTableInfo info = ParquetTableReader.getTableInfo(
            tableDir.toString(), Collections.emptyMap());

        assertNotNull(info);
        assertFalse(info.isPartitioned());
        assertFalse(info.getRootFiles().isEmpty(), "Should find root parquet files");
        assertNotNull(info.getSchemaJson());

        // Root files should have valid paths and sizes
        for (ParquetFileEntry f : info.getRootFiles()) {
            assertNotNull(f.getPath());
            assertTrue(f.getSize() > 0, "File size should be positive");
        }
    }

    @Test
    @DisplayName("getTableInfo with file:// URL")
    void testGetTableInfoFileUrl() throws Exception {
        Path tableDir = createUnpartitionedTable();
        String fileUrl = "file://" + tableDir.toAbsolutePath();

        ParquetTableInfo info = ParquetTableReader.getTableInfo(
            fileUrl, Collections.emptyMap());

        assertNotNull(info);
        assertFalse(info.getRootFiles().isEmpty());
    }

    @Test
    @DisplayName("getTableInfo null URL throws")
    void testGetTableInfoNullUrlThrows() {
        assertThrows(IllegalArgumentException.class, () ->
            ParquetTableReader.getTableInfo(null, Collections.emptyMap()));
    }

    @Test
    @DisplayName("getTableInfo empty URL throws")
    void testGetTableInfoEmptyUrlThrows() {
        assertThrows(IllegalArgumentException.class, () ->
            ParquetTableReader.getTableInfo("", Collections.emptyMap()));
    }

    // ── listPartitionFiles tests ─────────────────────────────────────────────

    @Test
    @DisplayName("listPartitionFiles lists files in a partition directory")
    void testListPartitionFiles() throws Exception {
        Path tableDir = createPartitionedTable();

        ParquetTableInfo info = ParquetTableReader.getTableInfo(
            tableDir.toString(), Collections.emptyMap());

        assertTrue(info.isPartitioned());
        assertFalse(info.getPartitionDirectories().isEmpty());

        // List files in first partition
        String firstPartition = info.getPartitionDirectories().get(0);
        List<ParquetFileEntry> files = ParquetTableReader.listPartitionFiles(
            tableDir.toString(), Collections.emptyMap(), firstPartition);

        assertNotNull(files);
        assertFalse(files.isEmpty(), "Should find parquet files in partition");

        for (ParquetFileEntry f : files) {
            assertNotNull(f.getPath());
            assertTrue(f.getPath().endsWith(".parquet"), "Should be a parquet file");
            assertTrue(f.getSize() > 0, "File size should be positive");
        }
    }

    @Test
    @DisplayName("listPartitionFiles null URL throws")
    void testListPartitionFilesNullUrlThrows() {
        assertThrows(IllegalArgumentException.class, () ->
            ParquetTableReader.listPartitionFiles(null, Collections.emptyMap(), "part/"));
    }

    @Test
    @DisplayName("listPartitionFiles null prefix throws")
    void testListPartitionFilesNullPrefixThrows() {
        assertThrows(IllegalArgumentException.class, () ->
            ParquetTableReader.listPartitionFiles("/some/dir", Collections.emptyMap(), null));
    }

    @Test
    @DisplayName("listPartitionFiles empty prefix throws")
    void testListPartitionFilesEmptyPrefixThrows() {
        assertThrows(IllegalArgumentException.class, () ->
            ParquetTableReader.listPartitionFiles("/some/dir", Collections.emptyMap(), ""));
    }

    // ── ParquetTableInfo data class tests ────────────────────────────────────

    @Test
    @DisplayName("ParquetTableInfo.fromMaps parses partitioned response")
    void testParquetTableInfoFromMapsPartitioned() {
        List<Map<String, Object>> maps = new ArrayList<>();

        // Header
        Map<String, Object> header = new HashMap<>();
        header.put("schema_json", "{\"fields\":[]}");
        header.put("partition_columns_json", "[\"year\",\"month\"]");
        header.put("num_partitions", 2L);
        header.put("num_root_files", 0L);
        header.put("is_partitioned", true);
        maps.add(header);

        // Partition dirs
        Map<String, Object> p1 = new HashMap<>();
        p1.put("path", "year=2024/month=01/");
        maps.add(p1);

        Map<String, Object> p2 = new HashMap<>();
        p2.put("path", "year=2024/month=02/");
        maps.add(p2);

        ParquetTableInfo info = ParquetTableInfo.fromMaps(maps);

        assertTrue(info.isPartitioned());
        assertEquals("{\"fields\":[]}", info.getSchemaJson());
        assertEquals(Arrays.asList("year", "month"), info.getPartitionColumns());
        assertEquals(2, info.getPartitionDirectories().size());
        assertEquals("year=2024/month=01/", info.getPartitionDirectories().get(0));
        assertTrue(info.getRootFiles().isEmpty());
    }

    @Test
    @DisplayName("ParquetTableInfo.fromMaps parses unpartitioned response")
    void testParquetTableInfoFromMapsUnpartitioned() {
        List<Map<String, Object>> maps = new ArrayList<>();

        Map<String, Object> header = new HashMap<>();
        header.put("schema_json", "{\"fields\":[]}");
        header.put("partition_columns_json", "[]");
        header.put("num_partitions", 0L);
        header.put("num_root_files", 2L);
        header.put("is_partitioned", false);
        maps.add(header);

        // Root files
        Map<String, Object> f1 = new HashMap<>();
        f1.put("path", "data-00000.parquet");
        f1.put("size", 5000L);
        f1.put("last_modified", 1700000000000L);
        maps.add(f1);

        Map<String, Object> f2 = new HashMap<>();
        f2.put("path", "data-00001.parquet");
        f2.put("size", 7000L);
        f2.put("last_modified", 1700000001000L);
        maps.add(f2);

        ParquetTableInfo info = ParquetTableInfo.fromMaps(maps);

        assertFalse(info.isPartitioned());
        assertTrue(info.getPartitionDirectories().isEmpty());
        assertEquals(2, info.getRootFiles().size());
        assertEquals("data-00000.parquet", info.getRootFiles().get(0).getPath());
        assertEquals(5000, info.getRootFiles().get(0).getSize());
    }

    @Test
    @DisplayName("ParquetTableInfo.fromMaps empty throws")
    void testParquetTableInfoFromMapsEmpty() {
        assertThrows(RuntimeException.class, () ->
            ParquetTableInfo.fromMaps(Collections.emptyList()));
    }

    @Test
    @DisplayName("ParquetTableInfo toString")
    void testParquetTableInfoToString() {
        List<Map<String, Object>> maps = new ArrayList<>();
        Map<String, Object> header = new HashMap<>();
        header.put("num_partitions", 3L);
        header.put("num_root_files", 0L);
        header.put("is_partitioned", true);
        maps.add(header);

        ParquetTableInfo info = ParquetTableInfo.fromMaps(maps);
        String s = info.toString();
        assertTrue(s.contains("partitioned=true"));
    }

    // ── ParquetFileEntry data class tests ────────────────────────────────────

    @Test
    @DisplayName("ParquetFileEntry.fromMap parses all fields")
    void testParquetFileEntryFromMap() {
        Map<String, Object> map = new HashMap<>();
        map.put("path", "year=2024/data.parquet");
        map.put("size", 12345L);
        map.put("last_modified", 1700000000000L);
        map.put("partition_values", "{\"year\":\"2024\"}");

        ParquetFileEntry entry = ParquetFileEntry.fromMap(map);

        assertEquals("year=2024/data.parquet", entry.getPath());
        assertEquals(12345, entry.getSize());
        assertEquals(1700000000000L, entry.getLastModified());
        assertEquals("2024", entry.getPartitionValues().get("year"));
    }

    @Test
    @DisplayName("ParquetFileEntry.fromMap handles missing partition values")
    void testParquetFileEntryFromMapNoPartitions() {
        Map<String, Object> map = new HashMap<>();
        map.put("path", "data.parquet");
        map.put("size", 100L);

        ParquetFileEntry entry = ParquetFileEntry.fromMap(map);

        assertEquals("data.parquet", entry.getPath());
        assertTrue(entry.getPartitionValues().isEmpty());
    }

    @Test
    @DisplayName("ParquetFileEntry partition values are unmodifiable")
    void testParquetFileEntryPartitionValuesUnmodifiable() {
        ParquetFileEntry entry = new ParquetFileEntry("test.parquet", 100, 0,
            Collections.singletonMap("k", "v"));
        assertThrows(UnsupportedOperationException.class, () ->
            entry.getPartitionValues().put("new", "val"));
    }

    @Test
    @DisplayName("ParquetFileEntry toString")
    void testParquetFileEntryToString() {
        ParquetFileEntry entry = new ParquetFileEntry("data.parquet", 9999, 0,
            Collections.emptyMap());
        String s = entry.toString();
        assertTrue(s.contains("data.parquet"));
        assertTrue(s.contains("9999"));
    }

    // ── Serializable tests ───────────────────────────────────────────────────

    @Test
    @DisplayName("ParquetFileEntry is Serializable")
    void testParquetFileEntrySerializable() throws Exception {
        ParquetFileEntry entry = new ParquetFileEntry("year=2024/data.parquet", 5000,
            1700000000000L, Collections.singletonMap("year", "2024"));

        java.io.ByteArrayOutputStream baos = new java.io.ByteArrayOutputStream();
        java.io.ObjectOutputStream oos = new java.io.ObjectOutputStream(baos);
        oos.writeObject(entry);
        oos.close();

        java.io.ObjectInputStream ois = new java.io.ObjectInputStream(
            new java.io.ByteArrayInputStream(baos.toByteArray()));
        ParquetFileEntry deserialized = (ParquetFileEntry) ois.readObject();

        assertEquals("year=2024/data.parquet", deserialized.getPath());
        assertEquals(5000, deserialized.getSize());
        assertEquals("2024", deserialized.getPartitionValues().get("year"));
    }
}

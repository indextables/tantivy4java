package io.indextables.tantivy4java;

import io.indextables.tantivy4java.delta.DeltaFileEntry;
import io.indextables.tantivy4java.delta.DeltaTableReader;

import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Integration tests for DeltaTableReader.
 *
 * Tests that require a live Delta table are skipped when no table is available.
 * The helper method createMinimalDeltaTable() creates a minimal Delta log
 * with a single Add action for testing the file listing pipeline.
 */
public class DeltaTableReaderTest {

    @TempDir
    Path tempDir;

    /**
     * Create a minimal Delta table on disk for testing.
     * Writes a _delta_log/00000000000000000000.json with one Add action
     * pointing to a dummy parquet file.
     */
    private Path createMinimalDeltaTable() throws IOException {
        Path tableDir = tempDir.resolve("test_delta_table");
        Files.createDirectories(tableDir);

        // Create _delta_log directory
        Path deltaLog = tableDir.resolve("_delta_log");
        Files.createDirectories(deltaLog);

        // Write commit 0 with protocol + metadata + add action
        String commit0 = String.join("\n",
            // Protocol action
            "{\"protocol\":{\"minReaderVersion\":1,\"minWriterVersion\":2}}",
            // Metadata action
            "{\"metaData\":{\"id\":\"test-table-id\"," +
                "\"format\":{\"provider\":\"parquet\",\"options\":{}}," +
                "\"schemaString\":\"{\\\"type\\\":\\\"struct\\\",\\\"fields\\\":[" +
                    "{\\\"name\\\":\\\"id\\\",\\\"type\\\":\\\"long\\\",\\\"nullable\\\":true,\\\"metadata\\\":{}}," +
                    "{\\\"name\\\":\\\"name\\\",\\\"type\\\":\\\"string\\\",\\\"nullable\\\":true,\\\"metadata\\\":{}}" +
                "]}\"," +
                "\"partitionColumns\":[]," +
                "\"configuration\":{}," +
                "\"createdTime\":1700000000000}}",
            // Add action for a parquet file
            "{\"add\":{\"path\":\"part-00000-test.snappy.parquet\"," +
                "\"partitionValues\":{}," +
                "\"size\":12345," +
                "\"modificationTime\":1700000000000," +
                "\"dataChange\":true," +
                "\"stats\":\"{\\\"numRecords\\\":100}\"}}"
        );
        Files.write(deltaLog.resolve("00000000000000000000.json"), commit0.getBytes());

        // Create the referenced parquet file (just needs to exist for listing, not for reading)
        Files.write(tableDir.resolve("part-00000-test.snappy.parquet"), new byte[]{0});

        return tableDir;
    }

    @Test
    @DisplayName("List files from a local minimal Delta table")
    void testListFilesLocal() throws Exception {
        Path tableDir = createMinimalDeltaTable();

        List<DeltaFileEntry> files = DeltaTableReader.listFiles(tableDir.toString());

        assertNotNull(files);
        assertEquals(1, files.size());

        DeltaFileEntry entry = files.get(0);
        assertEquals("part-00000-test.snappy.parquet", entry.getPath());
        assertEquals(12345, entry.getSize());
        assertEquals(1700000000000L, entry.getModificationTime());
        assertTrue(entry.hasNumRecords());
        assertEquals(100, entry.getNumRecords());
        assertFalse(entry.hasDeletionVector());
        assertTrue(entry.getPartitionValues().isEmpty());
        assertEquals(0, entry.getTableVersion());
    }

    @Test
    @DisplayName("List files with explicit file:// URL")
    void testListFilesWithFileUrl() throws Exception {
        Path tableDir = createMinimalDeltaTable();
        String fileUrl = "file://" + tableDir.toAbsolutePath();

        List<DeltaFileEntry> files = DeltaTableReader.listFiles(fileUrl);

        assertNotNull(files);
        assertEquals(1, files.size());
        assertEquals("part-00000-test.snappy.parquet", files.get(0).getPath());
    }

    @Test
    @DisplayName("List files with empty config map")
    void testListFilesWithEmptyConfig() throws Exception {
        Path tableDir = createMinimalDeltaTable();

        List<DeltaFileEntry> files = DeltaTableReader.listFiles(
            tableDir.toString(), Collections.emptyMap());

        assertNotNull(files);
        assertEquals(1, files.size());
    }

    @Test
    @DisplayName("List files at specific version 0")
    void testListFilesAtVersion() throws Exception {
        Path tableDir = createMinimalDeltaTable();

        List<DeltaFileEntry> files = DeltaTableReader.listFiles(
            tableDir.toString(), Collections.emptyMap(), 0);

        assertNotNull(files);
        assertEquals(1, files.size());
        assertEquals(0, files.get(0).getTableVersion());
    }

    @Test
    @DisplayName("DeltaFileEntry toString includes key information")
    void testDeltaFileEntryToString() throws Exception {
        Path tableDir = createMinimalDeltaTable();
        List<DeltaFileEntry> files = DeltaTableReader.listFiles(tableDir.toString());

        String str = files.get(0).toString();
        assertTrue(str.contains("part-00000-test.snappy.parquet"));
        assertTrue(str.contains("12345"));
    }

    @Test
    @DisplayName("Invalid path throws exception")
    void testInvalidPathThrows() {
        assertThrows(RuntimeException.class, () -> {
            DeltaTableReader.listFiles("/nonexistent/path/to/delta/table");
        });
    }

    @Test
    @DisplayName("Null URL throws IllegalArgumentException")
    void testNullUrlThrows() {
        assertThrows(IllegalArgumentException.class, () -> {
            DeltaTableReader.listFiles(null);
        });
    }

    @Test
    @DisplayName("Empty URL throws IllegalArgumentException")
    void testEmptyUrlThrows() {
        assertThrows(IllegalArgumentException.class, () -> {
            DeltaTableReader.listFiles("");
        });
    }

    @Test
    @DisplayName("Multi-file Delta table with two commits")
    void testMultiFileDeltaTable() throws Exception {
        Path tableDir = tempDir.resolve("multi_file_delta");
        Files.createDirectories(tableDir);
        Path deltaLog = tableDir.resolve("_delta_log");
        Files.createDirectories(deltaLog);

        // Commit 0: protocol + metadata + first file
        String commit0 = String.join("\n",
            "{\"protocol\":{\"minReaderVersion\":1,\"minWriterVersion\":2}}",
            "{\"metaData\":{\"id\":\"multi-table\"," +
                "\"format\":{\"provider\":\"parquet\",\"options\":{}}," +
                "\"schemaString\":\"{\\\"type\\\":\\\"struct\\\",\\\"fields\\\":[" +
                    "{\\\"name\\\":\\\"id\\\",\\\"type\\\":\\\"long\\\",\\\"nullable\\\":true,\\\"metadata\\\":{}}" +
                "]}\"," +
                "\"partitionColumns\":[]," +
                "\"configuration\":{}," +
                "\"createdTime\":1700000000000}}",
            "{\"add\":{\"path\":\"part-00000.parquet\"," +
                "\"partitionValues\":{}," +
                "\"size\":5000," +
                "\"modificationTime\":1700000000000," +
                "\"dataChange\":true," +
                "\"stats\":\"{\\\"numRecords\\\":50}\"}}"
        );
        Files.write(deltaLog.resolve("00000000000000000000.json"), commit0.getBytes());

        // Commit 1: add second file
        String commit1 = "{\"add\":{\"path\":\"part-00001.parquet\"," +
                "\"partitionValues\":{}," +
                "\"size\":7000," +
                "\"modificationTime\":1700000001000," +
                "\"dataChange\":true," +
                "\"stats\":\"{\\\"numRecords\\\":75}\"}}";
        Files.write(deltaLog.resolve("00000000000000000001.json"), commit1.getBytes());

        // Create dummy parquet files
        Files.write(tableDir.resolve("part-00000.parquet"), new byte[]{0});
        Files.write(tableDir.resolve("part-00001.parquet"), new byte[]{0});

        // Latest version should have both files
        List<DeltaFileEntry> files = DeltaTableReader.listFiles(tableDir.toString());
        assertEquals(2, files.size());

        // Version 0 should have only one file
        List<DeltaFileEntry> v0Files = DeltaTableReader.listFiles(
            tableDir.toString(), Collections.emptyMap(), 0);
        assertEquals(1, v0Files.size());
        assertEquals("part-00000.parquet", v0Files.get(0).getPath());
    }

    @Test
    @DisplayName("Delta table with remove action")
    void testDeltaTableWithRemove() throws Exception {
        Path tableDir = tempDir.resolve("remove_delta");
        Files.createDirectories(tableDir);
        Path deltaLog = tableDir.resolve("_delta_log");
        Files.createDirectories(deltaLog);

        // Commit 0: protocol + metadata + add file
        String commit0 = String.join("\n",
            "{\"protocol\":{\"minReaderVersion\":1,\"minWriterVersion\":2}}",
            "{\"metaData\":{\"id\":\"remove-table\"," +
                "\"format\":{\"provider\":\"parquet\",\"options\":{}}," +
                "\"schemaString\":\"{\\\"type\\\":\\\"struct\\\",\\\"fields\\\":[" +
                    "{\\\"name\\\":\\\"id\\\",\\\"type\\\":\\\"long\\\",\\\"nullable\\\":true,\\\"metadata\\\":{}}" +
                "]}\"," +
                "\"partitionColumns\":[]," +
                "\"configuration\":{}," +
                "\"createdTime\":1700000000000}}",
            "{\"add\":{\"path\":\"part-00000.parquet\"," +
                "\"partitionValues\":{}," +
                "\"size\":5000," +
                "\"modificationTime\":1700000000000," +
                "\"dataChange\":true," +
                "\"stats\":\"{\\\"numRecords\\\":50}\"}}",
            "{\"add\":{\"path\":\"part-00001.parquet\"," +
                "\"partitionValues\":{}," +
                "\"size\":6000," +
                "\"modificationTime\":1700000000000," +
                "\"dataChange\":true," +
                "\"stats\":\"{\\\"numRecords\\\":60}\"}}"
        );
        Files.write(deltaLog.resolve("00000000000000000000.json"), commit0.getBytes());

        // Commit 1: remove first file
        String commit1 = "{\"remove\":{\"path\":\"part-00000.parquet\"," +
                "\"deletionTimestamp\":1700000002000," +
                "\"dataChange\":true}}";
        Files.write(deltaLog.resolve("00000000000000000001.json"), commit1.getBytes());

        // Latest version should have only the second file
        List<DeltaFileEntry> files = DeltaTableReader.listFiles(tableDir.toString());
        assertEquals(1, files.size());
        assertEquals("part-00001.parquet", files.get(0).getPath());
    }

    @Test
    @DisplayName("Compact mode returns core fields only")
    void testCompactModeLocal() throws Exception {
        Path tableDir = createMinimalDeltaTable();

        List<DeltaFileEntry> files = DeltaTableReader.listFiles(tableDir.toString(), true);

        assertNotNull(files);
        assertEquals(1, files.size());

        DeltaFileEntry entry = files.get(0);
        // Core fields present
        assertEquals("part-00000-test.snappy.parquet", entry.getPath());
        assertEquals(12345, entry.getSize());
        assertEquals(1700000000000L, entry.getModificationTime());
        assertTrue(entry.hasNumRecords());
        assertEquals(100, entry.getNumRecords());
        assertEquals(0, entry.getTableVersion());

        // Skipped fields should be defaults
        assertTrue(entry.getPartitionValues().isEmpty(), "Compact mode should have empty partition values");
        assertFalse(entry.hasDeletionVector(), "Compact mode should have false for deletion vector");
    }

    @Test
    @DisplayName("Compact mode on partitioned table omits partition values")
    void testCompactModePartitioned() throws Exception {
        Path tableDir = tempDir.resolve("compact_partitioned_delta");
        Files.createDirectories(tableDir);
        Path deltaLog = tableDir.resolve("_delta_log");
        Files.createDirectories(deltaLog);

        String commit0 = String.join("\n",
            "{\"protocol\":{\"minReaderVersion\":1,\"minWriterVersion\":2}}",
            "{\"metaData\":{\"id\":\"compact-part-table\"," +
                "\"format\":{\"provider\":\"parquet\",\"options\":{}}," +
                "\"schemaString\":\"{\\\"type\\\":\\\"struct\\\",\\\"fields\\\":[" +
                    "{\\\"name\\\":\\\"id\\\",\\\"type\\\":\\\"long\\\",\\\"nullable\\\":true,\\\"metadata\\\":{}}," +
                    "{\\\"name\\\":\\\"date\\\",\\\"type\\\":\\\"string\\\",\\\"nullable\\\":true,\\\"metadata\\\":{}}" +
                "]}\"," +
                "\"partitionColumns\":[\"date\"]," +
                "\"configuration\":{}," +
                "\"createdTime\":1700000000000}}",
            "{\"add\":{\"path\":\"date=2024-01-01/part-00000.parquet\"," +
                "\"partitionValues\":{\"date\":\"2024-01-01\"}," +
                "\"size\":3000," +
                "\"modificationTime\":1700000000000," +
                "\"dataChange\":true," +
                "\"stats\":\"{\\\"numRecords\\\":25}\"}}"
        );
        Files.write(deltaLog.resolve("00000000000000000000.json"), commit0.getBytes());

        // Full mode: partition values present
        List<DeltaFileEntry> fullFiles = DeltaTableReader.listFiles(tableDir.toString(), false);
        assertEquals(1, fullFiles.size());
        assertFalse(fullFiles.get(0).getPartitionValues().isEmpty(), "Full mode should have partition values");
        assertEquals("2024-01-01", fullFiles.get(0).getPartitionValues().get("date"));

        // Compact mode: partition values absent
        List<DeltaFileEntry> compactFiles = DeltaTableReader.listFiles(tableDir.toString(), true);
        assertEquals(1, compactFiles.size());
        assertTrue(compactFiles.get(0).getPartitionValues().isEmpty(), "Compact mode should not have partition values");

        // Core fields still match
        assertEquals(fullFiles.get(0).getPath(), compactFiles.get(0).getPath());
        assertEquals(fullFiles.get(0).getSize(), compactFiles.get(0).getSize());
        assertEquals(fullFiles.get(0).getNumRecords(), compactFiles.get(0).getNumRecords());
    }

    @Test
    @DisplayName("Partitioned Delta table")
    void testPartitionedDeltaTable() throws Exception {
        Path tableDir = tempDir.resolve("partitioned_delta");
        Files.createDirectories(tableDir);
        Path deltaLog = tableDir.resolve("_delta_log");
        Files.createDirectories(deltaLog);

        String commit0 = String.join("\n",
            "{\"protocol\":{\"minReaderVersion\":1,\"minWriterVersion\":2}}",
            "{\"metaData\":{\"id\":\"partitioned-table\"," +
                "\"format\":{\"provider\":\"parquet\",\"options\":{}}," +
                "\"schemaString\":\"{\\\"type\\\":\\\"struct\\\",\\\"fields\\\":[" +
                    "{\\\"name\\\":\\\"id\\\",\\\"type\\\":\\\"long\\\",\\\"nullable\\\":true,\\\"metadata\\\":{}}," +
                    "{\\\"name\\\":\\\"date\\\",\\\"type\\\":\\\"string\\\",\\\"nullable\\\":true,\\\"metadata\\\":{}}" +
                "]}\"," +
                "\"partitionColumns\":[\"date\"]," +
                "\"configuration\":{}," +
                "\"createdTime\":1700000000000}}",
            "{\"add\":{\"path\":\"date=2024-01-01/part-00000.parquet\"," +
                "\"partitionValues\":{\"date\":\"2024-01-01\"}," +
                "\"size\":3000," +
                "\"modificationTime\":1700000000000," +
                "\"dataChange\":true," +
                "\"stats\":\"{\\\"numRecords\\\":25}\"}}",
            "{\"add\":{\"path\":\"date=2024-01-02/part-00000.parquet\"," +
                "\"partitionValues\":{\"date\":\"2024-01-02\"}," +
                "\"size\":4000," +
                "\"modificationTime\":1700000001000," +
                "\"dataChange\":true," +
                "\"stats\":\"{\\\"numRecords\\\":30}\"}}"
        );
        Files.write(deltaLog.resolve("00000000000000000000.json"), commit0.getBytes());

        List<DeltaFileEntry> files = DeltaTableReader.listFiles(tableDir.toString());
        assertEquals(2, files.size());

        // Check partition values are present
        boolean foundJan1 = false;
        boolean foundJan2 = false;
        for (DeltaFileEntry entry : files) {
            Map<String, String> pv = entry.getPartitionValues();
            if ("2024-01-01".equals(pv.get("date"))) {
                foundJan1 = true;
                assertEquals(25, entry.getNumRecords());
            }
            if ("2024-01-02".equals(pv.get("date"))) {
                foundJan2 = true;
                assertEquals(30, entry.getNumRecords());
            }
        }
        assertTrue(foundJan1, "Should find date=2024-01-01 partition");
        assertTrue(foundJan2, "Should find date=2024-01-02 partition");
    }
}

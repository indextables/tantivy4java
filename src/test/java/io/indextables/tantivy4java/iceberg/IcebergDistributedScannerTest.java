package io.indextables.tantivy4java.iceberg;

import org.junit.jupiter.api.*;

import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Integration tests for the Iceberg distributed scanning primitives:
 * getSnapshotInfo() and readManifestFile().
 *
 * Most tests exercise parameter validation and data class parsing
 * since catalog-based tests require a live catalog.
 */
class IcebergDistributedScannerTest {

    // ── getSnapshotInfo parameter validation ─────────────────────────────────

    @Test
    @DisplayName("getSnapshotInfo null catalogName throws")
    void testGetSnapshotInfoNullCatalogThrows() {
        Map<String, String> config = Collections.singletonMap("catalog_type", "rest");
        assertThrows(IllegalArgumentException.class, () ->
            IcebergTableReader.getSnapshotInfo(null, "ns", "table", config));
    }

    @Test
    @DisplayName("getSnapshotInfo empty catalogName throws")
    void testGetSnapshotInfoEmptyCatalogThrows() {
        Map<String, String> config = Collections.singletonMap("catalog_type", "rest");
        assertThrows(IllegalArgumentException.class, () ->
            IcebergTableReader.getSnapshotInfo("", "ns", "table", config));
    }

    @Test
    @DisplayName("getSnapshotInfo null namespace throws")
    void testGetSnapshotInfoNullNamespaceThrows() {
        Map<String, String> config = Collections.singletonMap("catalog_type", "rest");
        assertThrows(IllegalArgumentException.class, () ->
            IcebergTableReader.getSnapshotInfo("cat", null, "table", config));
    }

    @Test
    @DisplayName("getSnapshotInfo null tableName throws")
    void testGetSnapshotInfoNullTableThrows() {
        Map<String, String> config = Collections.singletonMap("catalog_type", "rest");
        assertThrows(IllegalArgumentException.class, () ->
            IcebergTableReader.getSnapshotInfo("cat", "ns", null, config));
    }

    @Test
    @DisplayName("getSnapshotInfo with snapshotId overload validates params")
    void testGetSnapshotInfoWithSnapshotIdValidation() {
        Map<String, String> config = Collections.singletonMap("catalog_type", "rest");
        assertThrows(IllegalArgumentException.class, () ->
            IcebergTableReader.getSnapshotInfo(null, "ns", "table", config, 123L));
    }

    // ── readManifestFile parameter validation ────────────────────────────────

    @Test
    @DisplayName("readManifestFile null catalogName throws")
    void testReadManifestFileNullCatalogThrows() {
        Map<String, String> config = Collections.singletonMap("catalog_type", "rest");
        assertThrows(IllegalArgumentException.class, () ->
            IcebergTableReader.readManifestFile(null, "ns", "table", config, "/path/manifest.avro"));
    }

    @Test
    @DisplayName("readManifestFile null manifestPath throws")
    void testReadManifestFileNullManifestPathThrows() {
        Map<String, String> config = Collections.singletonMap("catalog_type", "rest");
        assertThrows(IllegalArgumentException.class, () ->
            IcebergTableReader.readManifestFile("cat", "ns", "table", config, null));
    }

    @Test
    @DisplayName("readManifestFile empty manifestPath throws")
    void testReadManifestFileEmptyManifestPathThrows() {
        Map<String, String> config = Collections.singletonMap("catalog_type", "rest");
        assertThrows(IllegalArgumentException.class, () ->
            IcebergTableReader.readManifestFile("cat", "ns", "table", config, ""));
    }

    @Test
    @DisplayName("readManifestFile compact overload validates params")
    void testReadManifestFileCompactValidation() {
        Map<String, String> config = Collections.singletonMap("catalog_type", "rest");
        assertThrows(IllegalArgumentException.class, () ->
            IcebergTableReader.readManifestFile("", "ns", "table", config, "/path", true));
    }

    // ── IcebergSnapshotInfo data class tests ─────────────────────────────────

    @Test
    @DisplayName("IcebergSnapshotInfo.fromMaps parses header and manifests")
    void testIcebergSnapshotInfoFromMaps() {
        List<Map<String, Object>> maps = new ArrayList<>();

        // Header
        Map<String, Object> header = new HashMap<>();
        header.put("snapshot_id", 12345L);
        header.put("schema_json", "{\"schema-id\":0}");
        header.put("partition_spec_json", "{\"spec-id\":0}");
        header.put("manifest_count", 2L);
        maps.add(header);

        // Manifest 1
        Map<String, Object> m1 = new HashMap<>();
        m1.put("manifest_path", "s3://bucket/metadata/manifest-1.avro");
        m1.put("manifest_length", 50000L);
        m1.put("added_snapshot_id", 12345L);
        m1.put("added_files_count", 100L);
        m1.put("existing_files_count", 0L);
        m1.put("deleted_files_count", 0L);
        m1.put("partition_spec_id", 0);
        maps.add(m1);

        // Manifest 2
        Map<String, Object> m2 = new HashMap<>();
        m2.put("manifest_path", "s3://bucket/metadata/manifest-2.avro");
        m2.put("manifest_length", 30000L);
        m2.put("added_snapshot_id", 12340L);
        m2.put("added_files_count", 50L);
        m2.put("existing_files_count", 200L);
        m2.put("deleted_files_count", 10L);
        m2.put("partition_spec_id", 0);
        maps.add(m2);

        IcebergSnapshotInfo info = IcebergSnapshotInfo.fromMaps(maps);

        assertEquals(12345, info.getSnapshotId());
        assertEquals("{\"schema-id\":0}", info.getSchemaJson());
        assertEquals("{\"spec-id\":0}", info.getPartitionSpecJson());
        assertEquals(2, info.getManifestFiles().size());

        // Verify manifest 1
        IcebergSnapshotInfo.ManifestFileInfo mf1 = info.getManifestFiles().get(0);
        assertEquals("s3://bucket/metadata/manifest-1.avro", mf1.getManifestPath());
        assertEquals(50000, mf1.getManifestLength());
        assertEquals(12345, mf1.getAddedSnapshotId());
        assertEquals(100, mf1.getAddedFilesCount());
        assertEquals(0, mf1.getExistingFilesCount());
        assertEquals(0, mf1.getDeletedFilesCount());
        assertEquals(0, mf1.getPartitionSpecId());

        // Verify manifest 2
        IcebergSnapshotInfo.ManifestFileInfo mf2 = info.getManifestFiles().get(1);
        assertEquals("s3://bucket/metadata/manifest-2.avro", mf2.getManifestPath());
        assertEquals(10, mf2.getDeletedFilesCount());
    }

    @Test
    @DisplayName("IcebergSnapshotInfo.getManifestFilePaths convenience method")
    void testGetManifestFilePaths() {
        List<Map<String, Object>> maps = new ArrayList<>();

        Map<String, Object> header = new HashMap<>();
        header.put("snapshot_id", 1L);
        maps.add(header);

        Map<String, Object> m1 = new HashMap<>();
        m1.put("manifest_path", "path/a.avro");
        maps.add(m1);

        Map<String, Object> m2 = new HashMap<>();
        m2.put("manifest_path", "path/b.avro");
        maps.add(m2);

        IcebergSnapshotInfo info = IcebergSnapshotInfo.fromMaps(maps);
        List<String> paths = info.getManifestFilePaths();

        assertEquals(2, paths.size());
        assertEquals("path/a.avro", paths.get(0));
        assertEquals("path/b.avro", paths.get(1));
    }

    @Test
    @DisplayName("IcebergSnapshotInfo.fromMaps empty throws")
    void testIcebergSnapshotInfoFromMapsEmpty() {
        assertThrows(RuntimeException.class, () ->
            IcebergSnapshotInfo.fromMaps(Collections.emptyList()));
    }

    @Test
    @DisplayName("IcebergSnapshotInfo manifestFiles list is unmodifiable")
    void testManifestFilesUnmodifiable() {
        List<Map<String, Object>> maps = new ArrayList<>();
        Map<String, Object> header = new HashMap<>();
        header.put("snapshot_id", 1L);
        maps.add(header);

        IcebergSnapshotInfo info = IcebergSnapshotInfo.fromMaps(maps);
        assertThrows(UnsupportedOperationException.class, () ->
            info.getManifestFiles().add(null));
    }

    @Test
    @DisplayName("IcebergSnapshotInfo toString")
    void testIcebergSnapshotInfoToString() {
        List<Map<String, Object>> maps = new ArrayList<>();
        Map<String, Object> header = new HashMap<>();
        header.put("snapshot_id", 999L);
        maps.add(header);

        IcebergSnapshotInfo info = IcebergSnapshotInfo.fromMaps(maps);
        String s = info.toString();
        assertTrue(s.contains("999"));
        assertTrue(s.contains("0")); // 0 manifests
    }

    @Test
    @DisplayName("ManifestFileInfo toString")
    void testManifestFileInfoToString() {
        Map<String, Object> m = new HashMap<>();
        m.put("manifest_path", "s3://bucket/snap.avro");
        m.put("added_files_count", 10L);
        m.put("existing_files_count", 20L);
        m.put("deleted_files_count", 5L);

        IcebergSnapshotInfo.ManifestFileInfo mf = IcebergSnapshotInfo.ManifestFileInfo.fromMap(m);
        String s = mf.toString();
        assertTrue(s.contains("s3://bucket/snap.avro"));
        assertTrue(s.contains("10")); // added
        assertTrue(s.contains("5"));  // deleted
    }

    // ── IcebergFileEntry Serializable test ───────────────────────────────────

    @Test
    @DisplayName("IcebergFileEntry is Serializable")
    void testIcebergFileEntrySerializable() throws Exception {
        IcebergFileEntry entry = new IcebergFileEntry(
            "s3://bucket/data/part-00000.parquet", "parquet", 1000, 50000,
            Collections.singletonMap("year", "2024"), "data", 42L);

        java.io.ByteArrayOutputStream baos = new java.io.ByteArrayOutputStream();
        java.io.ObjectOutputStream oos = new java.io.ObjectOutputStream(baos);
        oos.writeObject(entry);
        oos.close();

        java.io.ObjectInputStream ois = new java.io.ObjectInputStream(
            new java.io.ByteArrayInputStream(baos.toByteArray()));
        IcebergFileEntry deserialized = (IcebergFileEntry) ois.readObject();

        assertEquals("s3://bucket/data/part-00000.parquet", deserialized.getPath());
        assertEquals("parquet", deserialized.getFileFormat());
        assertEquals(1000, deserialized.getRecordCount());
        assertEquals(50000, deserialized.getFileSizeBytes());
        assertEquals("2024", deserialized.getPartitionValues().get("year"));
        assertEquals("data", deserialized.getContentType());
        assertEquals(42, deserialized.getSnapshotId());
    }
}

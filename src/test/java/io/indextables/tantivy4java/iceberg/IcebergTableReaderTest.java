package io.indextables.tantivy4java.iceberg;

import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tier 2: Java unit tests for Iceberg data classes and parameter validation.
 * No Docker, no native calls — exercises pure Java logic only.
 */
class IcebergTableReaderTest {

    // ── Parameter validation tests ──────────────────────────────────────────

    @Test
    void testNullCatalogNameThrows() {
        Map<String, String> config = Collections.singletonMap("catalog_type", "rest");
        assertThrows(IllegalArgumentException.class,
                () -> IcebergTableReader.listFiles(null, "ns", "table", config));
    }

    @Test
    void testEmptyCatalogNameThrows() {
        Map<String, String> config = Collections.singletonMap("catalog_type", "rest");
        assertThrows(IllegalArgumentException.class,
                () -> IcebergTableReader.listFiles("", "ns", "table", config));
    }

    @Test
    void testNullNamespaceThrows() {
        Map<String, String> config = Collections.singletonMap("catalog_type", "rest");
        assertThrows(IllegalArgumentException.class,
                () -> IcebergTableReader.listFiles("cat", null, "table", config));
    }

    @Test
    void testEmptyNamespaceThrows() {
        Map<String, String> config = Collections.singletonMap("catalog_type", "rest");
        assertThrows(IllegalArgumentException.class,
                () -> IcebergTableReader.listFiles("cat", "", "table", config));
    }

    @Test
    void testNullTableNameThrows() {
        Map<String, String> config = Collections.singletonMap("catalog_type", "rest");
        assertThrows(IllegalArgumentException.class,
                () -> IcebergTableReader.listFiles("cat", "ns", null, config));
    }

    @Test
    void testEmptyTableNameThrows() {
        Map<String, String> config = Collections.singletonMap("catalog_type", "rest");
        assertThrows(IllegalArgumentException.class,
                () -> IcebergTableReader.listFiles("cat", "ns", "", config));
    }

    @Test
    void testValidationAppliesForReadSchema() {
        Map<String, String> config = Collections.singletonMap("catalog_type", "rest");
        assertThrows(IllegalArgumentException.class,
                () -> IcebergTableReader.readSchema(null, "ns", "table", config));
        assertThrows(IllegalArgumentException.class,
                () -> IcebergTableReader.readSchema("cat", "", "table", config));
    }

    @Test
    void testValidationAppliesForListSnapshots() {
        Map<String, String> config = Collections.singletonMap("catalog_type", "rest");
        assertThrows(IllegalArgumentException.class,
                () -> IcebergTableReader.listSnapshots(null, "ns", "table", config));
        assertThrows(IllegalArgumentException.class,
                () -> IcebergTableReader.listSnapshots("cat", "ns", null, config));
    }

    // ── IcebergFileEntry.fromMap tests ──────────────────────────────────────

    @Test
    void testIcebergFileEntryFromMap() {
        Map<String, Object> map = new HashMap<>();
        map.put("path", "s3://bucket/data/part-00000.parquet");
        map.put("file_format", "parquet");
        map.put("record_count", 1000L);
        map.put("file_size_bytes", 50000L);
        map.put("partition_values", "{\"year\":\"2024\",\"month\":\"01\"}");
        map.put("content_type", "data");
        map.put("snapshot_id", 12345L);

        IcebergFileEntry entry = IcebergFileEntry.fromMap(map);

        assertEquals("s3://bucket/data/part-00000.parquet", entry.getPath());
        assertEquals("parquet", entry.getFileFormat());
        assertEquals(1000, entry.getRecordCount());
        assertEquals(50000, entry.getFileSizeBytes());
        assertEquals("2024", entry.getPartitionValues().get("year"));
        assertEquals("01", entry.getPartitionValues().get("month"));
        assertEquals("data", entry.getContentType());
        assertEquals(12345, entry.getSnapshotId());
    }

    @Test
    void testIcebergFileEntryFromMapCompact() {
        // Compact mode: partition_values and content_type are absent
        Map<String, Object> map = new HashMap<>();
        map.put("path", "s3://bucket/data/file.parquet");
        map.put("file_format", "orc");
        map.put("record_count", 500);
        map.put("file_size_bytes", 25000);
        map.put("snapshot_id", 99L);

        IcebergFileEntry entry = IcebergFileEntry.fromMap(map);

        assertEquals("s3://bucket/data/file.parquet", entry.getPath());
        assertEquals("orc", entry.getFileFormat());
        assertEquals(500, entry.getRecordCount());
        assertEquals(25000, entry.getFileSizeBytes());
        assertTrue(entry.getPartitionValues().isEmpty());
        assertEquals("data", entry.getContentType()); // default
        assertEquals(99, entry.getSnapshotId());
    }

    @Test
    void testIcebergFileEntryPartitionValuesUnmodifiable() {
        Map<String, Object> map = new HashMap<>();
        map.put("path", "s3://bucket/file.parquet");
        map.put("partition_values", "{\"year\":\"2024\"}");
        map.put("snapshot_id", 1L);

        IcebergFileEntry entry = IcebergFileEntry.fromMap(map);
        assertThrows(UnsupportedOperationException.class,
                () -> entry.getPartitionValues().put("new_key", "value"));
    }

    // ── IcebergSchemaField.fromMap tests ────────────────────────────────────

    @Test
    void testIcebergSchemaFieldFromMap() {
        Map<String, Object> map = new HashMap<>();
        map.put("name", "user_id");
        map.put("data_type", "long");
        map.put("field_id", 1);
        map.put("nullable", false);
        map.put("doc", "The user identifier");

        IcebergSchemaField field = IcebergSchemaField.fromMap(map);

        assertEquals("user_id", field.getName());
        assertEquals("long", field.getDataType());
        assertEquals(1, field.getFieldId());
        assertFalse(field.isNullable());
        assertEquals("The user identifier", field.getDoc());
    }

    @Test
    void testIcebergSchemaFieldIsPrimitive() {
        Map<String, Object> map = new HashMap<>();
        map.put("name", "id");
        map.put("data_type", "long");
        map.put("field_id", 1);
        map.put("nullable", false);
        IcebergSchemaField primitiveField = IcebergSchemaField.fromMap(map);
        assertTrue(primitiveField.isPrimitive());

        Map<String, Object> complexMap = new HashMap<>();
        complexMap.put("name", "tags");
        complexMap.put("data_type", "{\"type\":\"list\",\"element\":\"string\"}");
        complexMap.put("field_id", 2);
        complexMap.put("nullable", true);
        IcebergSchemaField complexField = IcebergSchemaField.fromMap(complexMap);
        assertFalse(complexField.isPrimitive());
    }

    @Test
    void testIcebergSchemaFieldEmptyDocTreatedAsNull() {
        Map<String, Object> map = new HashMap<>();
        map.put("name", "col");
        map.put("data_type", "string");
        map.put("field_id", 1);
        map.put("nullable", true);
        map.put("doc", "");  // empty string

        IcebergSchemaField field = IcebergSchemaField.fromMap(map);
        assertNull(field.getDoc(), "Empty doc string should be treated as null");
    }

    @Test
    void testIcebergSchemaFieldMissingDoc() {
        Map<String, Object> map = new HashMap<>();
        map.put("name", "col");
        map.put("data_type", "double");
        map.put("field_id", 3);
        map.put("nullable", true);
        // no "doc" key

        IcebergSchemaField field = IcebergSchemaField.fromMap(map);
        assertNull(field.getDoc());
    }

    // ── IcebergSnapshot.fromMap tests ───────────────────────────────────────

    @Test
    void testIcebergSnapshotFromMap() {
        Map<String, Object> map = new HashMap<>();
        map.put("snapshot_id", 100L);
        map.put("parent_snapshot_id", 99L);
        map.put("sequence_number", 10L);
        map.put("timestamp_ms", 1700000000000L);
        map.put("manifest_list", "s3://bucket/metadata/snap-100.avro");
        map.put("operation", "append");
        map.put("summary", "{\"operation\":\"append\",\"added-files-count\":\"5\"}");

        IcebergSnapshot snapshot = IcebergSnapshot.fromMap(map);

        assertEquals(100, snapshot.getSnapshotId());
        assertEquals(99, snapshot.getParentSnapshotId());
        assertEquals(10, snapshot.getSequenceNumber());
        assertEquals(1700000000000L, snapshot.getTimestampMs());
        assertEquals("s3://bucket/metadata/snap-100.avro", snapshot.getManifestList());
        assertEquals("append", snapshot.getOperation());
        assertTrue(snapshot.hasParent());
        assertEquals("5", snapshot.getSummary().get("added-files-count"));
    }

    @Test
    void testIcebergSnapshotNoParent() {
        Map<String, Object> map = new HashMap<>();
        map.put("snapshot_id", 1L);
        map.put("parent_snapshot_id", -1L);
        map.put("sequence_number", 1L);
        map.put("timestamp_ms", 1700000000000L);

        IcebergSnapshot snapshot = IcebergSnapshot.fromMap(map);

        assertFalse(snapshot.hasParent());
        assertEquals(-1, snapshot.getParentSnapshotId());
    }

    @Test
    void testIcebergSnapshotSummaryParsing() {
        Map<String, Object> map = new HashMap<>();
        map.put("snapshot_id", 42L);
        map.put("parent_snapshot_id", -1L);
        map.put("sequence_number", 1L);
        map.put("timestamp_ms", 1700000000000L);
        map.put("summary", "{\"operation\":\"overwrite\",\"total-records\":\"1000\",\"added-data-files\":\"3\"}");

        IcebergSnapshot snapshot = IcebergSnapshot.fromMap(map);

        Map<String, String> summary = snapshot.getSummary();
        assertEquals("overwrite", summary.get("operation"));
        assertEquals("1000", summary.get("total-records"));
        assertEquals("3", summary.get("added-data-files"));
    }

    @Test
    void testIcebergSnapshotEmptySummary() {
        Map<String, Object> map = new HashMap<>();
        map.put("snapshot_id", 1L);
        map.put("parent_snapshot_id", -1L);
        map.put("sequence_number", 1L);
        map.put("timestamp_ms", 1700000000000L);
        // no summary key

        IcebergSnapshot snapshot = IcebergSnapshot.fromMap(map);
        assertTrue(snapshot.getSummary().isEmpty());
    }

    @Test
    void testIcebergSnapshotSummaryUnmodifiable() {
        Map<String, Object> map = new HashMap<>();
        map.put("snapshot_id", 1L);
        map.put("parent_snapshot_id", -1L);
        map.put("sequence_number", 1L);
        map.put("timestamp_ms", 1700000000000L);
        map.put("summary", "{\"k\":\"v\"}");

        IcebergSnapshot snapshot = IcebergSnapshot.fromMap(map);
        assertThrows(UnsupportedOperationException.class,
                () -> snapshot.getSummary().put("new_key", "value"));
    }

    // ── IcebergTableSchema tests ────────────────────────────────────────────

    @Test
    void testIcebergTableSchemaConstruction() {
        Map<String, Object> fieldMap = new HashMap<>();
        fieldMap.put("name", "id");
        fieldMap.put("data_type", "long");
        fieldMap.put("field_id", 1);
        fieldMap.put("nullable", false);

        IcebergSchemaField field = IcebergSchemaField.fromMap(fieldMap);
        List<IcebergSchemaField> fields = Collections.singletonList(field);

        IcebergTableSchema schema = new IcebergTableSchema(fields, "{\"schema-id\":0}", 42);

        assertEquals(1, schema.getFieldCount());
        assertEquals("id", schema.getFields().get(0).getName());
        assertEquals("{\"schema-id\":0}", schema.getSchemaJson());
        assertEquals(42, schema.getSnapshotId());
    }

    @Test
    void testIcebergTableSchemaFieldsUnmodifiable() {
        IcebergTableSchema schema = new IcebergTableSchema(
                Collections.emptyList(), "{}", -1);
        assertThrows(UnsupportedOperationException.class,
                () -> schema.getFields().add(null));
    }

    // ── toString tests ──────────────────────────────────────────────────────

    @Test
    void testIcebergFileEntryToString() {
        Map<String, Object> map = new HashMap<>();
        map.put("path", "s3://bucket/file.parquet");
        map.put("file_format", "parquet");
        map.put("record_count", 100L);
        map.put("file_size_bytes", 5000L);
        map.put("snapshot_id", 7L);

        IcebergFileEntry entry = IcebergFileEntry.fromMap(map);
        String s = entry.toString();
        assertTrue(s.contains("s3://bucket/file.parquet"), "toString should contain path");
        assertTrue(s.contains("parquet"), "toString should contain format");
        assertTrue(s.contains("100"), "toString should contain record count");
    }

    @Test
    void testIcebergSchemaFieldToString() {
        Map<String, Object> map = new HashMap<>();
        map.put("name", "score");
        map.put("data_type", "double");
        map.put("field_id", 3);
        map.put("nullable", true);

        IcebergSchemaField field = IcebergSchemaField.fromMap(map);
        String s = field.toString();
        assertTrue(s.contains("score"), "toString should contain name");
        assertTrue(s.contains("double"), "toString should contain type");
    }

    @Test
    void testIcebergSnapshotToString() {
        Map<String, Object> map = new HashMap<>();
        map.put("snapshot_id", 123L);
        map.put("parent_snapshot_id", -1L);
        map.put("sequence_number", 1L);
        map.put("timestamp_ms", 1700000000000L);
        map.put("operation", "append");

        IcebergSnapshot snapshot = IcebergSnapshot.fromMap(map);
        String s = snapshot.toString();
        assertTrue(s.contains("123"), "toString should contain id");
        assertTrue(s.contains("append"), "toString should contain operation");
    }

    @Test
    void testIcebergTableSchemaToString() {
        IcebergTableSchema schema = new IcebergTableSchema(
                Collections.emptyList(), "{}", 42);
        String s = schema.toString();
        assertTrue(s.contains("0"), "toString should contain field count");
        assertTrue(s.contains("42"), "toString should contain snapshot id");
    }
}

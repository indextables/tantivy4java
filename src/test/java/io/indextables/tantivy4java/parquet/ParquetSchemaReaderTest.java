package io.indextables.tantivy4java.parquet;

import io.indextables.tantivy4java.delta.DeltaSchemaField;
import io.indextables.tantivy4java.delta.DeltaTableSchema;

import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for {@link ParquetSchemaReader}.
 */
public class ParquetSchemaReaderTest {

    @TempDir
    Path tempDir;

    // --- parameter validation (no native lib needed) ---

    @Test
    void testNullFileUrlThrows() {
        assertThrows(IllegalArgumentException.class, () ->
            ParquetSchemaReader.readSchema(null));
    }

    @Test
    void testEmptyFileUrlThrows() {
        assertThrows(IllegalArgumentException.class, () ->
            ParquetSchemaReader.readSchema(""));
    }

    @Test
    void testNullConfigUsesDefaults() {
        assertThrows(IllegalArgumentException.class, () ->
            ParquetSchemaReader.readSchema(null, null));
    }

    // --- integration: write a parquet file via Rust, read its schema back ---

    @Test
    void testReadSchemaFromLocalParquetFile() {
        Path parquetFile = tempDir.resolve("test.parquet");
        ParquetSchemaReader.writeTestParquet(parquetFile.toString());

        DeltaTableSchema schema = ParquetSchemaReader.readSchema(parquetFile.toString());

        assertNotNull(schema);
        assertEquals(-1, schema.getTableVersion(), "parquet files have no table version");
        assertEquals(5, schema.getFieldCount());

        List<DeltaSchemaField> fields = schema.getFields();

        // id — Int64, not null
        assertEquals("id", fields.get(0).getName());
        assertEquals("int64", fields.get(0).getDataType());
        assertFalse(fields.get(0).isNullable());

        // name — Utf8, nullable
        assertEquals("name", fields.get(1).getName());
        assertEquals("string", fields.get(1).getDataType());
        assertTrue(fields.get(1).isNullable());

        // score — Float64, nullable
        assertEquals("score", fields.get(2).getName());
        assertEquals("double", fields.get(2).getDataType());
        assertTrue(fields.get(2).isNullable());

        // active — Boolean, not null
        assertEquals("active", fields.get(3).getName());
        assertEquals("boolean", fields.get(3).getDataType());
        assertFalse(fields.get(3).isNullable());

        // created — Timestamp, nullable
        assertEquals("created", fields.get(4).getName());
        assertEquals("timestamp", fields.get(4).getDataType());
        assertTrue(fields.get(4).isNullable());
    }

    @Test
    void testReadSchemaJsonContainsFieldNames() {
        Path parquetFile = tempDir.resolve("test_json.parquet");
        ParquetSchemaReader.writeTestParquet(parquetFile.toString());

        DeltaTableSchema schema = ParquetSchemaReader.readSchema(parquetFile.toString());
        String json = schema.getSchemaJson();

        assertNotNull(json);
        assertTrue(json.contains("id"), "schema JSON should contain 'id'");
        assertTrue(json.contains("name"), "schema JSON should contain 'name'");
        assertTrue(json.contains("score"), "schema JSON should contain 'score'");
        assertTrue(json.contains("active"), "schema JSON should contain 'active'");
        assertTrue(json.contains("created"), "schema JSON should contain 'created'");
    }

    @Test
    void testReadSchemaViaFileUrl() {
        Path parquetFile = tempDir.resolve("test_fileurl.parquet");
        ParquetSchemaReader.writeTestParquet(parquetFile.toString());

        // Use file:// URL instead of bare path
        String fileUrl = "file://" + parquetFile.toAbsolutePath();
        DeltaTableSchema schema = ParquetSchemaReader.readSchema(fileUrl);

        assertEquals(5, schema.getFieldCount());
        assertEquals("id", schema.getFields().get(0).getName());
    }

    @Test
    void testReadSchemaNonexistentFileThrows() {
        Path bogus = tempDir.resolve("does_not_exist.parquet");
        assertThrows(RuntimeException.class, () ->
            ParquetSchemaReader.readSchema(bogus.toString()));
    }

    // --- column name mapping tests ---

    @Test
    void testReadColumnMappingNullUrlThrows() {
        Map<Integer, String> fieldIdToName = new HashMap<>();
        fieldIdToName.put(1, "id");
        assertThrows(IllegalArgumentException.class, () ->
            ParquetSchemaReader.readColumnMapping(null, fieldIdToName));
    }

    @Test
    void testReadColumnMappingNullFieldMapThrows() {
        assertThrows(IllegalArgumentException.class, () ->
            ParquetSchemaReader.readColumnMapping("/some/file.parquet", null));
    }

    @Test
    void testReadColumnMappingResolvesPhysicalToLogicalNames() {
        // Write a parquet file with physical names (col_1, col_2, col_3) and field IDs
        Path parquetFile = tempDir.resolve("databricks_style.parquet");
        ParquetSchemaReader.writeTestParquetWithFieldIds(parquetFile.toString());

        // Simulate Iceberg schema field_id → logical_name mapping
        Map<Integer, String> fieldIdToName = new HashMap<>();
        fieldIdToName.put(1, "id");
        fieldIdToName.put(2, "name");
        fieldIdToName.put(3, "price");

        Map<String, String> mapping = ParquetSchemaReader.readColumnMapping(
                parquetFile.toString(), fieldIdToName);

        assertNotNull(mapping);
        assertEquals(3, mapping.size());
        assertEquals("id", mapping.get("col_1"), "col_1 should map to 'id'");
        assertEquals("name", mapping.get("col_2"), "col_2 should map to 'name'");
        assertEquals("price", mapping.get("col_3"), "col_3 should map to 'price'");
    }

    @Test
    void testReadColumnMappingIdentityWhenNoFieldIds() {
        // Write a standard parquet file (no field IDs)
        Path parquetFile = tempDir.resolve("standard.parquet");
        ParquetSchemaReader.writeTestParquet(parquetFile.toString());

        // Even with a field_id map, should fall back to identity since no field IDs in file
        Map<Integer, String> fieldIdToName = new HashMap<>();
        fieldIdToName.put(1, "identifier");
        fieldIdToName.put(2, "full_name");

        Map<String, String> mapping = ParquetSchemaReader.readColumnMapping(
                parquetFile.toString(), fieldIdToName);

        assertNotNull(mapping);
        assertEquals(5, mapping.size());
        // All should be identity mappings
        assertEquals("id", mapping.get("id"));
        assertEquals("name", mapping.get("name"));
        assertEquals("score", mapping.get("score"));
        assertEquals("active", mapping.get("active"));
        assertEquals("created", mapping.get("created"));
    }

    @Test
    void testReadColumnMappingWithEmptyFieldMap() {
        // Write a parquet file with field IDs
        Path parquetFile = tempDir.resolve("field_ids_empty_map.parquet");
        ParquetSchemaReader.writeTestParquetWithFieldIds(parquetFile.toString());

        // Empty field_id map — all should be identity
        Map<Integer, String> fieldIdToName = new HashMap<>();

        Map<String, String> mapping = ParquetSchemaReader.readColumnMapping(
                parquetFile.toString(), fieldIdToName);

        assertNotNull(mapping);
        assertEquals(3, mapping.size());
        assertEquals("col_1", mapping.get("col_1"));
        assertEquals("col_2", mapping.get("col_2"));
        assertEquals("col_3", mapping.get("col_3"));
    }
}

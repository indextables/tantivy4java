package io.indextables.tantivy4java.parquet;

import io.indextables.tantivy4java.delta.DeltaSchemaField;
import io.indextables.tantivy4java.delta.DeltaTableSchema;

import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.List;

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
}

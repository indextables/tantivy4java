package io.indextables.tantivy4java.parquet;

import io.indextables.tantivy4java.batch.BatchDocumentReader;
import io.indextables.tantivy4java.delta.DeltaSchemaField;
import io.indextables.tantivy4java.delta.DeltaTableSchema;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Static utility for reading the schema from a standalone parquet file.
 *
 * <p>Reads the Arrow schema from the parquet file footer and returns it
 * using the same {@link DeltaTableSchema} / {@link DeltaSchemaField} classes
 * as {@link io.indextables.tantivy4java.delta.DeltaTableReader}.
 *
 * <p>Supports local paths, {@code file://}, {@code s3://}, and {@code azure://} URLs.
 * Credentials are passed via a config map with the same keys as DeltaTableReader:
 * <ul>
 *   <li>{@code aws_access_key_id}, {@code aws_secret_access_key}, {@code aws_session_token},
 *       {@code aws_region}, {@code aws_endpoint}, {@code aws_force_path_style}</li>
 *   <li>{@code azure_account_name}, {@code azure_access_key}, {@code azure_bearer_token}</li>
 * </ul>
 *
 * <h3>Usage Examples</h3>
 * <pre>{@code
 * // Local parquet file
 * DeltaTableSchema schema = ParquetSchemaReader.readSchema("/data/file.parquet");
 *
 * // S3 parquet file with credentials
 * Map<String, String> config = new HashMap<>();
 * config.put("aws_access_key_id", "AKIA...");
 * config.put("aws_secret_access_key", "...");
 * config.put("aws_region", "us-east-1");
 * DeltaTableSchema schema = ParquetSchemaReader.readSchema("s3://bucket/file.parquet", config);
 *
 * // Inspect fields
 * for (DeltaSchemaField field : schema.getFields()) {
 *     System.out.println(field.getName() + " : " + field.getDataType() + " (nullable=" + field.isNullable() + ")");
 * }
 * }</pre>
 */
public class ParquetSchemaReader {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    static {
        io.indextables.tantivy4java.core.Tantivy.initialize();
    }

    private ParquetSchemaReader() {
        // static utility
    }

    /**
     * Read column name mapping from a parquet file using Iceberg field IDs.
     *
     * <p>Databricks Unity Catalog (and other engines using Iceberg column mapping)
     * may store data in parquet files using physical column names (e.g. "col_1",
     * "col_2") that differ from the logical Iceberg column names (e.g. "id", "name").
     * This method reads the parquet file's metadata, extracts field IDs, and maps
     * physical parquet column names to logical Iceberg column names.
     *
     * <p>Resolution order:
     * <ol>
     *   <li>Arrow field metadata ({@code PARQUET:field_id})</li>
     *   <li>Parquet schema descriptor field IDs</li>
     *   <li>{@code iceberg.schema} KV metadata embedded in the parquet footer</li>
     *   <li>Identity mapping (physical = logical) as fallback</li>
     * </ol>
     *
     * <h3>Usage Example</h3>
     * <pre>{@code
     * // 1. Read Iceberg schema (has field IDs and logical names)
     * IcebergTableSchema schema = IcebergTableReader.readSchema(
     *     "catalog", "namespace", "table", catalogConfig);
     *
     * // 2. Pick a parquet file from the file listing
     * List<IcebergFileEntry> files = IcebergTableReader.listFiles(
     *     "catalog", "namespace", "table", catalogConfig);
     * String parquetUrl = files.get(0).getPath();
     *
     * // 3. Get column name mapping
     * Map<String, String> mapping = ParquetSchemaReader.readColumnMapping(
     *     parquetUrl, schema.getFieldIdToNameMap(), storageConfig);
     * // Returns: {col_1=id, col_2=name, col_3=price}
     *
     * // 4. Use mapping when processing parquet data
     * for (Map.Entry<String, String> e : mapping.entrySet()) {
     *     System.out.println(e.getKey() + " → " + e.getValue());
     * }
     * }</pre>
     *
     * @param parquetFileUrl path to a parquet file (local path, file://, s3://, or azure://)
     * @param fieldIdToName  Iceberg field ID to logical column name mapping
     *                       (use {@link io.indextables.tantivy4java.iceberg.IcebergTableSchema#getFieldIdToNameMap()})
     * @param config         storage credentials (same keys as {@link #readSchema(String, Map)})
     * @return map of physical parquet column name to logical Iceberg column name
     * @throws IllegalArgumentException if parquetFileUrl is null or empty
     * @throws RuntimeException if the file cannot be read
     */
    public static Map<String, String> readColumnMapping(
            String parquetFileUrl,
            Map<Integer, String> fieldIdToName,
            Map<String, String> config) {
        if (parquetFileUrl == null || parquetFileUrl.isEmpty()) {
            throw new IllegalArgumentException("parquetFileUrl must not be null or empty");
        }
        if (fieldIdToName == null) {
            throw new IllegalArgumentException("fieldIdToName must not be null");
        }

        // Convert Map<Integer, String> to JSON: {"1":"id", "2":"name"}
        Map<String, String> stringKeyMap = new HashMap<>(fieldIdToName.size());
        for (Map.Entry<Integer, String> entry : fieldIdToName.entrySet()) {
            stringKeyMap.put(String.valueOf(entry.getKey()), entry.getValue());
        }
        String fieldIdJson;
        try {
            fieldIdJson = MAPPER.writeValueAsString(stringKeyMap);
        } catch (Exception e) {
            throw new RuntimeException("Failed to serialize fieldIdToName to JSON", e);
        }

        byte[] resultBytes = nativeReadColumnMapping(
                parquetFileUrl,
                fieldIdJson,
                config != null ? config : Collections.emptyMap());

        // Parse JSON result bytes → Map<String, String>
        String resultJson = new String(resultBytes, StandardCharsets.UTF_8);
        try {
            return MAPPER.readValue(resultJson, new TypeReference<Map<String, String>>() {});
        } catch (Exception e) {
            throw new RuntimeException("Failed to parse column mapping result: " + resultJson, e);
        }
    }

    /**
     * Read column name mapping from a parquet file without credentials.
     *
     * @see #readColumnMapping(String, Map, Map)
     */
    public static Map<String, String> readColumnMapping(
            String parquetFileUrl,
            Map<Integer, String> fieldIdToName) {
        return readColumnMapping(parquetFileUrl, fieldIdToName, Collections.emptyMap());
    }

    /**
     * Read the schema from a parquet file.
     *
     * @param fileUrl path to a parquet file (local path, file://, s3://, or azure://)
     * @return the schema as a {@link DeltaTableSchema} (tableVersion will be -1)
     * @throws IllegalArgumentException if fileUrl is null or empty
     * @throws RuntimeException if the file cannot be read
     */
    public static DeltaTableSchema readSchema(String fileUrl) {
        return readSchema(fileUrl, Collections.emptyMap());
    }

    /**
     * Read the schema from a parquet file with storage credentials.
     *
     * @param fileUrl path to a parquet file (local path, file://, s3://, or azure://)
     * @param config  credential and storage configuration (see class javadoc for keys)
     * @return the schema as a {@link DeltaTableSchema} (tableVersion will be -1)
     * @throws IllegalArgumentException if fileUrl is null or empty
     * @throws RuntimeException if the file cannot be read
     */
    public static DeltaTableSchema readSchema(String fileUrl, Map<String, String> config) {
        if (fileUrl == null || fileUrl.isEmpty()) {
            throw new IllegalArgumentException("fileUrl must not be null or empty");
        }

        byte[] bytes = nativeReadParquetSchema(fileUrl, config != null ? config : Collections.emptyMap());

        // Parse TANT byte buffer
        ByteBuffer buffer = ByteBuffer.wrap(bytes);
        buffer.order(ByteOrder.nativeOrder());

        BatchDocumentReader reader = new BatchDocumentReader();
        List<Map<String, Object>> maps = reader.parseToMaps(buffer);

        if (maps.isEmpty()) {
            throw new RuntimeException("Empty schema response from native layer");
        }

        // First document is the header: schema_json, table_version, field_count
        Map<String, Object> header = maps.get(0);
        String schemaJson = (String) header.get("schema_json");
        long tableVersion = toLong(header.get("table_version"));

        // For parquet files, normalize the version to -1 (not applicable)
        if (tableVersion < 0 || tableVersion == Long.MAX_VALUE) {
            tableVersion = -1;
        }

        // Remaining documents are field entries
        List<DeltaSchemaField> fields = new ArrayList<>(maps.size() - 1);
        for (int i = 1; i < maps.size(); i++) {
            fields.add(DeltaSchemaField.fromMap(maps.get(i)));
        }

        return new DeltaTableSchema(fields, schemaJson, tableVersion);
    }

    private static long toLong(Object value) {
        if (value instanceof Number) {
            return ((Number) value).longValue();
        }
        return -1;
    }

    /**
     * Write a small test parquet file with a known schema.
     *
     * <p>Schema: id (int64), name (string, nullable), score (double, nullable),
     * active (boolean), created (timestamp, nullable).
     * Contains 3 rows. Used by tests to create fixtures without a Java parquet dependency.
     *
     * @param path local file path to write
     */
    public static void writeTestParquet(String path) {
        nativeWriteTestParquet(path);
    }

    /**
     * Write a test parquet file with physical column names and Iceberg field IDs.
     *
     * <p>Schema: col_1 (int64, field_id=1), col_2 (string, field_id=2),
     * col_3 (double, field_id=3). Contains 3 rows.
     *
     * <p>Simulates a Databricks-style parquet file where physical column names
     * (col_1, col_2, col_3) differ from logical Iceberg names (id, name, price).
     *
     * @param path local file path to write
     */
    public static void writeTestParquetWithFieldIds(String path) {
        nativeWriteTestParquetWithFieldIds(path);
    }

    private static native byte[] nativeReadParquetSchema(String fileUrl, Map<String, String> config);
    private static native void nativeWriteTestParquet(String path);
    private static native void nativeWriteTestParquetWithFieldIds(String path);
    private static native byte[] nativeReadColumnMapping(String parquetFileUrl, String fieldIdToNameJson, Map<String, String> config);
}

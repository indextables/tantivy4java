package io.indextables.tantivy4java.parquet;

import io.indextables.tantivy4java.batch.BatchDocumentReader;
import io.indextables.tantivy4java.delta.DeltaSchemaField;
import io.indextables.tantivy4java.delta.DeltaTableSchema;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.Collections;
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

    static {
        io.indextables.tantivy4java.core.Tantivy.initialize();
    }

    private ParquetSchemaReader() {
        // static utility
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

    private static native byte[] nativeReadParquetSchema(String fileUrl, Map<String, String> config);
    private static native void nativeWriteTestParquet(String path);
}

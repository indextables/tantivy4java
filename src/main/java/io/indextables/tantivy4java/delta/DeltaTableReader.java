package io.indextables.tantivy4java.delta;

import io.indextables.tantivy4java.batch.BatchDocumentReader;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Static utility for listing active parquet files in a Delta Lake table.
 *
 * <p>Uses delta-kernel-rs to read the Delta transaction log and returns
 * metadata about every active parquet file at the specified (or latest) version.
 *
 * <p>Supports local, S3, and Azure table locations. Credentials are passed
 * via a config map with the following keys:
 * <ul>
 *   <li>{@code aws_access_key_id}, {@code aws_secret_access_key}, {@code aws_session_token},
 *       {@code aws_region}, {@code aws_endpoint}, {@code aws_force_path_style}</li>
 *   <li>{@code azure_account_name}, {@code azure_access_key}, {@code azure_bearer_token}</li>
 * </ul>
 *
 * <h3>Usage Examples</h3>
 * <pre>{@code
 * // Local Delta table (latest version)
 * List<DeltaFileEntry> files = DeltaTableReader.listFiles("/data/my_delta_table");
 *
 * // S3 Delta table with credentials
 * Map<String, String> config = new HashMap<>();
 * config.put("aws_access_key_id", "AKIA...");
 * config.put("aws_secret_access_key", "...");
 * config.put("aws_region", "us-east-1");
 * List<DeltaFileEntry> files = DeltaTableReader.listFiles("s3://bucket/delta_table", config);
 *
 * // Specific version
 * List<DeltaFileEntry> files = DeltaTableReader.listFiles("s3://bucket/delta_table", config, 42);
 * }</pre>
 */
public class DeltaTableReader {

    static {
        io.indextables.tantivy4java.core.Tantivy.initialize();
    }

    private DeltaTableReader() {
        // static utility
    }

    /**
     * List active parquet files in a Delta table at the latest version.
     *
     * @param tableUrl table location (local path, file://, s3://, or azure://)
     * @return list of active file entries with full metadata
     * @throws RuntimeException if the table cannot be read
     */
    public static List<DeltaFileEntry> listFiles(String tableUrl) {
        return listFiles(tableUrl, Collections.emptyMap(), -1, false);
    }

    /**
     * List active parquet files in a Delta table at the latest version with credentials.
     *
     * @param tableUrl table location
     * @param config   credential and storage configuration (see class javadoc for keys)
     * @return list of active file entries with full metadata
     * @throws RuntimeException if the table cannot be read
     */
    public static List<DeltaFileEntry> listFiles(String tableUrl, Map<String, String> config) {
        return listFiles(tableUrl, config, -1, false);
    }

    /**
     * List active parquet files in a Delta table at a specific version.
     *
     * @param tableUrl table location
     * @param config   credential and storage configuration
     * @param version  snapshot version to read (-1 for latest)
     * @return list of active file entries with full metadata
     * @throws RuntimeException if the table cannot be read
     */
    public static List<DeltaFileEntry> listFiles(String tableUrl, Map<String, String> config, long version) {
        return listFiles(tableUrl, config, version, false);
    }

    /**
     * List active parquet files in a Delta table at the latest version (compact mode).
     *
     * <p>Compact mode returns only core fields: path, size, modification_time,
     * num_records, and table_version. Partition values and deletion vector status
     * are omitted, reducing serialization overhead.
     *
     * @param tableUrl table location (local path, file://, s3://, or azure://)
     * @param compact  if true, skip partition_values and has_deletion_vector fields
     * @return list of active file entries
     * @throws RuntimeException if the table cannot be read
     */
    public static List<DeltaFileEntry> listFiles(String tableUrl, boolean compact) {
        return listFiles(tableUrl, Collections.emptyMap(), -1, compact);
    }

    /**
     * List active parquet files in a Delta table at the latest version (compact mode).
     *
     * @param tableUrl table location
     * @param config   credential and storage configuration
     * @param compact  if true, skip partition_values and has_deletion_vector fields
     * @return list of active file entries
     * @throws RuntimeException if the table cannot be read
     */
    public static List<DeltaFileEntry> listFiles(String tableUrl, Map<String, String> config, boolean compact) {
        return listFiles(tableUrl, config, -1, compact);
    }

    /**
     * List active parquet files in a Delta table at a specific version.
     *
     * @param tableUrl table location
     * @param config   credential and storage configuration
     * @param version  snapshot version to read (-1 for latest)
     * @param compact  if true, skip partition_values and has_deletion_vector fields
     * @return list of active file entries
     * @throws RuntimeException if the table cannot be read
     */
    public static List<DeltaFileEntry> listFiles(String tableUrl, Map<String, String> config, long version, boolean compact) {
        if (tableUrl == null || tableUrl.isEmpty()) {
            throw new IllegalArgumentException("tableUrl must not be null or empty");
        }

        // Call native layer (Rust handles URL normalization for bare paths, file://, s3://, etc.)
        byte[] bytes = nativeListFiles(tableUrl, version, config != null ? config : Collections.emptyMap(), compact);

        // Parse TANT byte buffer → List<Map<String, Object>>
        ByteBuffer buffer = ByteBuffer.wrap(bytes);
        buffer.order(ByteOrder.nativeOrder());

        BatchDocumentReader reader = new BatchDocumentReader();
        List<Map<String, Object>> maps = reader.parseToMaps(buffer);

        // Convert to DeltaFileEntry list
        List<DeltaFileEntry> entries = new ArrayList<>(maps.size());
        for (Map<String, Object> map : maps) {
            entries.add(DeltaFileEntry.fromMap(map));
        }
        return entries;
    }

    private static native byte[] nativeListFiles(String tableUrl, long version, Map<String, String> config, boolean compact);
}

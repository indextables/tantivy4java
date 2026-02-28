package io.indextables.tantivy4java.delta;

import io.indextables.tantivy4java.batch.BatchDocumentReader;
import io.indextables.tantivy4java.filter.PartitionFilter;

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
 *
 * // Compact mode — skip partition_values and has_deletion_vector for lightweight listing
 * List<DeltaFileEntry> compact = DeltaTableReader.listFiles("s3://bucket/delta_table", config, true);
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
        return listFiles(tableUrl, config, version, compact, null);
    }

    /**
     * List active parquet files with partition predicate filtering.
     *
     * @param tableUrl table location
     * @param config   credential and storage configuration
     * @param version  snapshot version to read (-1 for latest)
     * @param compact  if true, skip partition_values and has_deletion_vector fields
     * @param filter   partition filter (null for no filtering)
     * @return list of matching file entries
     * @throws RuntimeException if the table cannot be read
     */
    public static List<DeltaFileEntry> listFiles(String tableUrl, Map<String, String> config,
            long version, boolean compact, PartitionFilter filter) {
        if (tableUrl == null || tableUrl.isEmpty()) {
            throw new IllegalArgumentException("tableUrl must not be null or empty");
        }

        String predicateJson = filter != null ? filter.toJson() : null;
        byte[] bytes = nativeListFiles(tableUrl, version,
                config != null ? config : Collections.emptyMap(), compact, predicateJson);

        if (bytes == null) {
            throw new RuntimeException("Native listFiles returned null (check preceding exception)");
        }

        ByteBuffer buffer = ByteBuffer.wrap(bytes);
        buffer.order(ByteOrder.nativeOrder());

        BatchDocumentReader reader = new BatchDocumentReader();
        List<Map<String, Object>> maps = reader.parseToMaps(buffer);

        List<DeltaFileEntry> entries = new ArrayList<>(maps.size());
        for (Map<String, Object> map : maps) {
            entries.add(DeltaFileEntry.fromMap(map));
        }
        return entries;
    }

    // --- Schema reading ---

    /**
     * Read the schema of a Delta table from its transaction log (latest version).
     *
     * <p>Returns a {@link DeltaTableSchema} containing the list of top-level columns
     * with their Delta data types, nullability, and metadata, plus the raw schema JSON.
     *
     * @param tableUrl table location (local path, file://, s3://, or azure://)
     * @return the Delta table schema
     * @throws RuntimeException if the table cannot be read
     */
    public static DeltaTableSchema readSchema(String tableUrl) {
        return readSchema(tableUrl, Collections.emptyMap(), -1);
    }

    /**
     * Read the schema of a Delta table from its transaction log (latest version, with credentials).
     *
     * @param tableUrl table location
     * @param config   credential and storage configuration (see class javadoc for keys)
     * @return the Delta table schema
     * @throws RuntimeException if the table cannot be read
     */
    public static DeltaTableSchema readSchema(String tableUrl, Map<String, String> config) {
        return readSchema(tableUrl, config, -1);
    }

    /**
     * Read the schema of a Delta table from its transaction log at a specific version.
     *
     * @param tableUrl table location
     * @param config   credential and storage configuration
     * @param version  snapshot version to read (-1 for latest)
     * @return the Delta table schema
     * @throws RuntimeException if the table cannot be read
     */
    public static DeltaTableSchema readSchema(String tableUrl, Map<String, String> config, long version) {
        if (tableUrl == null || tableUrl.isEmpty()) {
            throw new IllegalArgumentException("tableUrl must not be null or empty");
        }

        byte[] bytes = nativeReadSchema(tableUrl, version, config != null ? config : Collections.emptyMap());

        if (bytes == null) {
            throw new RuntimeException("Native readSchema returned null (check preceding exception)");
        }

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

    // ── Distributed scanning primitives ──────────────────────────────────────

    /**
     * Get lightweight snapshot metadata for distributed scanning.
     *
     * <p>Reads {@code _last_checkpoint} and lists commit files — does NOT read
     * checkpoint contents. Returns paths that can be distributed to executors
     * via {@link #readCheckpointPart(String, Map, String)}.
     *
     * @param tableUrl table location (local path, file://, s3://, or azure://)
     * @param config   credential and storage configuration
     * @return snapshot metadata with checkpoint part paths and commit file paths
     */
    public static DeltaSnapshotInfo getSnapshotInfo(String tableUrl, Map<String, String> config) {
        if (tableUrl == null || tableUrl.isEmpty()) {
            throw new IllegalArgumentException("tableUrl must not be null or empty");
        }

        byte[] bytes = nativeGetSnapshotInfo(tableUrl, config != null ? config : Collections.emptyMap());

        if (bytes == null) {
            throw new RuntimeException("Native getSnapshotInfo returned null (check preceding exception)");
        }

        ByteBuffer buffer = ByteBuffer.wrap(bytes);
        buffer.order(ByteOrder.nativeOrder());

        BatchDocumentReader reader = new BatchDocumentReader();
        List<Map<String, Object>> maps = reader.parseToMaps(buffer);

        if (maps.isEmpty()) {
            throw new RuntimeException("Empty snapshot info response from native layer");
        }

        return DeltaSnapshotInfo.fromMap(maps.get(0));
    }

    /**
     * Read one checkpoint parquet part and extract file entries.
     *
     * <p>Designed for executor-side use: reads a single checkpoint parquet file
     * and returns the {@code add} file entries. Each checkpoint part typically
     * contains ~54K entries.
     *
     * @param tableUrl table location
     * @param config   credential and storage configuration
     * @param partPath checkpoint parquet file path (relative to _delta_log/)
     * @return list of file entries from this checkpoint part
     */
    public static List<DeltaFileEntry> readCheckpointPart(
            String tableUrl, Map<String, String> config, String partPath) {
        return readCheckpointPart(tableUrl, config, partPath, null);
    }

    /**
     * Read one checkpoint parquet part with partition predicate filtering.
     *
     * @param tableUrl table location
     * @param config   credential and storage configuration
     * @param partPath checkpoint parquet file path (relative to _delta_log/)
     * @param filter   partition filter (null for no filtering)
     * @return list of matching file entries from this checkpoint part
     */
    public static List<DeltaFileEntry> readCheckpointPart(
            String tableUrl, Map<String, String> config, String partPath,
            PartitionFilter filter) {
        if (tableUrl == null || tableUrl.isEmpty()) {
            throw new IllegalArgumentException("tableUrl must not be null or empty");
        }
        if (partPath == null || partPath.isEmpty()) {
            throw new IllegalArgumentException("partPath must not be null or empty");
        }

        String predicateJson = filter != null ? filter.toJson() : null;
        byte[] bytes = nativeReadCheckpointPart(tableUrl,
                config != null ? config : Collections.emptyMap(), partPath, predicateJson);

        if (bytes == null) {
            throw new RuntimeException("Native readCheckpointPart returned null (check preceding exception)");
        }

        ByteBuffer buffer = ByteBuffer.wrap(bytes);
        buffer.order(ByteOrder.nativeOrder());

        BatchDocumentReader reader = new BatchDocumentReader();
        List<Map<String, Object>> maps = reader.parseToMaps(buffer);

        List<DeltaFileEntry> entries = new ArrayList<>(maps.size());
        for (Map<String, Object> map : maps) {
            entries.add(DeltaFileEntry.fromMap(map));
        }
        return entries;
    }

    /**
     * Read post-checkpoint JSON commit changes.
     *
     * <p>Reads JSON commit files after the checkpoint and returns added/removed
     * file entries. Designed for driver-side use (typically small data).
     *
     * @param tableUrl    table location
     * @param config      credential and storage configuration
     * @param commitPaths list of JSON commit file paths (from {@link DeltaSnapshotInfo#getCommitFilePaths()})
     * @return log changes with added files and removed paths
     */
    public static DeltaLogChanges readPostCheckpointChanges(
            String tableUrl, Map<String, String> config, List<String> commitPaths) {
        return readPostCheckpointChanges(tableUrl, config, commitPaths, null);
    }

    /**
     * Read post-checkpoint JSON commit changes with partition predicate filtering.
     *
     * @param tableUrl    table location
     * @param config      credential and storage configuration
     * @param commitPaths list of JSON commit file paths
     * @param filter      partition filter (null for no filtering)
     * @return log changes with matching added files and removed paths
     */
    public static DeltaLogChanges readPostCheckpointChanges(
            String tableUrl, Map<String, String> config, List<String> commitPaths,
            PartitionFilter filter) {
        if (tableUrl == null || tableUrl.isEmpty()) {
            throw new IllegalArgumentException("tableUrl must not be null or empty");
        }

        String predicateJson = filter != null ? filter.toJson() : null;
        byte[] bytes = nativeReadPostCheckpointChanges(tableUrl,
                config != null ? config : Collections.emptyMap(),
                commitPaths != null ? commitPaths : Collections.emptyList(),
                predicateJson);

        if (bytes == null) {
            throw new RuntimeException("Native readPostCheckpointChanges returned null (check preceding exception)");
        }

        ByteBuffer buffer = ByteBuffer.wrap(bytes);
        buffer.order(ByteOrder.nativeOrder());

        BatchDocumentReader reader = new BatchDocumentReader();
        List<Map<String, Object>> maps = reader.parseToMaps(buffer);

        return DeltaLogChanges.fromMaps(maps);
    }

    // ── Arrow FFI methods ─────────────────────────────────────────────────────

    /**
     * Read a checkpoint part and export file entries via Arrow FFI (zero-copy to Spark).
     *
     * <p>Instead of the TANT serialization path, this builds a flat RecordBatch from the
     * filtered checkpoint entries and exports columns via the Arrow C Data Interface.
     * The consumer (e.g., Spark) provides pre-allocated FFI struct addresses.
     *
     * <p>Arrow schema: 6 columns — path (Utf8), size (Int64), modification_time (Int64),
     * num_records (Int64), partition_values (Utf8/JSON), has_deletion_vector (Boolean).
     *
     * @param tableUrl    table location
     * @param config      credential and storage configuration
     * @param partPath    checkpoint parquet file path (relative to _delta_log/)
     * @param filter      partition filter (null for no filtering)
     * @param arrayAddrs  pre-allocated FFI_ArrowArray addresses (6 elements)
     * @param schemaAddrs pre-allocated FFI_ArrowSchema addresses (6 elements)
     * @return number of rows written, or -1 if FFI not supported
     */
    public static int readCheckpointPartArrowFfi(
            String tableUrl, Map<String, String> config, String partPath,
            PartitionFilter filter, long[] arrayAddrs, long[] schemaAddrs) {
        if (tableUrl == null || tableUrl.isEmpty()) {
            throw new IllegalArgumentException("tableUrl must not be null or empty");
        }
        if (partPath == null || partPath.isEmpty()) {
            throw new IllegalArgumentException("partPath must not be null or empty");
        }
        if (arrayAddrs == null || arrayAddrs.length < 6) {
            throw new IllegalArgumentException("arrayAddrs must have at least 6 elements");
        }
        if (schemaAddrs == null || schemaAddrs.length < 6) {
            throw new IllegalArgumentException("schemaAddrs must have at least 6 elements");
        }

        String predicateJson = filter != null ? filter.toJson() : null;
        return nativeReadCheckpointPartArrowFfi(tableUrl,
                config != null ? config : Collections.emptyMap(),
                partPath, predicateJson, arrayAddrs, schemaAddrs);
    }

    // ── Native methods ───────────────────────────────────────────────────────

    private static native byte[] nativeListFiles(String tableUrl, long version, Map<String, String> config, boolean compact, String predicateJson);
    private static native byte[] nativeReadSchema(String tableUrl, long version, Map<String, String> config);
    private static native byte[] nativeGetSnapshotInfo(String tableUrl, Map<String, String> config);
    private static native byte[] nativeReadCheckpointPart(String tableUrl, Map<String, String> config, String partPath, String predicateJson);
    private static native byte[] nativeReadPostCheckpointChanges(String tableUrl, Map<String, String> config, List<String> commitPaths, String predicateJson);
    private static native int nativeReadCheckpointPartArrowFfi(String tableUrl, Map<String, String> config, String partPath, String predicateJson, long[] arrayAddrs, long[] schemaAddrs);
}

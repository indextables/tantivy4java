package io.indextables.tantivy4java.iceberg;

import io.indextables.tantivy4java.batch.BatchDocumentReader;
import io.indextables.tantivy4java.filter.PartitionFilter;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Static utility for reading Apache Iceberg table metadata through catalog integration.
 *
 * <p>Supports REST (including Databricks Unity Catalog), AWS Glue, and Hive Metastore
 * catalogs. All methods accept a config map that controls catalog type, credentials,
 * and storage access.
 *
 * <h3>Required Config Key</h3>
 * <ul>
 *   <li>{@code catalog_type} — {@code "rest"}, {@code "glue"}, or {@code "hms"}</li>
 * </ul>
 *
 * <h3>REST Catalog Config (including Databricks)</h3>
 * <ul>
 *   <li>{@code uri} — REST endpoint URL (required)</li>
 *   <li>{@code warehouse} — storage location or Unity Catalog name</li>
 *   <li>{@code credential} — OAuth2 {@code client_id:client_secret}</li>
 *   <li>{@code token} — bearer token (Databricks PAT or OAuth token)</li>
 *   <li>{@code oauth2-server-uri} — OAuth2 token endpoint</li>
 *   <li>{@code header.<name>} — custom HTTP headers</li>
 * </ul>
 *
 * <h3>Glue Catalog Config</h3>
 * <ul>
 *   <li>{@code warehouse} — S3 path (required)</li>
 *   <li>{@code aws_access_key_id}, {@code aws_secret_access_key}, {@code aws_session_token}</li>
 *   <li>{@code region_name} — AWS region</li>
 * </ul>
 *
 * <h3>HMS Catalog Config</h3>
 * <ul>
 *   <li>{@code uri} — thrift://host:port (required)</li>
 *   <li>{@code warehouse} — storage root (required)</li>
 *   <li>{@code thrift_transport} — "framed" or "buffered"</li>
 * </ul>
 *
 * <h3>Storage Credentials (all catalogs)</h3>
 * <ul>
 *   <li>AWS S3: {@code s3.access-key-id}, {@code s3.secret-access-key}, {@code s3.session-token},
 *       {@code s3.region}, {@code s3.endpoint}, {@code s3.path-style-access}</li>
 *   <li>Azure ADLS: {@code adls.account-name}, {@code adls.account-key},
 *       {@code adls.sas-token}, {@code adls.bearer-token}</li>
 * </ul>
 *
 * <h3>Usage Examples</h3>
 * <pre>{@code
 * // REST catalog
 * Map<String, String> config = new HashMap<>();
 * config.put("catalog_type", "rest");
 * config.put("uri", "http://localhost:8181");
 * config.put("warehouse", "s3://warehouse");
 * config.put("s3.access-key-id", "AKIA...");
 * config.put("s3.secret-access-key", "...");
 * config.put("s3.region", "us-east-1");
 * List<IcebergFileEntry> files = IcebergTableReader.listFiles(
 *     "my-catalog", "default", "my_table", config);
 *
 * // Databricks Unity Catalog via REST
 * Map<String, String> dbConfig = new HashMap<>();
 * dbConfig.put("catalog_type", "rest");
 * dbConfig.put("uri", "https://workspace.cloud.databricks.com/api/2.1/unity-catalog/iceberg-rest");
 * dbConfig.put("token", "dapi...");  // Databricks PAT
 * dbConfig.put("warehouse", "my_unity_catalog");
 * IcebergTableSchema schema = IcebergTableReader.readSchema(
 *     "databricks", "my_db", "events", dbConfig);
 *
 * // Glue catalog
 * Map<String, String> glueConfig = new HashMap<>();
 * glueConfig.put("catalog_type", "glue");
 * glueConfig.put("warehouse", "s3://my-warehouse");
 * glueConfig.put("aws_access_key_id", "AKIA...");
 * glueConfig.put("aws_secret_access_key", "...");
 * glueConfig.put("region_name", "us-east-1");
 * List<IcebergSnapshot> snapshots = IcebergTableReader.listSnapshots(
 *     "glue-catalog", "my_database", "events", glueConfig);
 * }</pre>
 */
public class IcebergTableReader {

    static {
        io.indextables.tantivy4java.core.Tantivy.initialize();
    }

    private IcebergTableReader() {
        // static utility
    }

    // ── List Files ────────────────────────────────────────────────────────────

    /**
     * List active data files at the current snapshot.
     *
     * @param catalogName catalog identifier
     * @param namespace   Iceberg namespace (e.g. "default", "db.schema")
     * @param tableName   table name
     * @param config      catalog and storage configuration
     * @return list of active data file entries
     */
    public static List<IcebergFileEntry> listFiles(
            String catalogName, String namespace, String tableName, Map<String, String> config) {
        return listFiles(catalogName, namespace, tableName, config, -1, false);
    }

    /**
     * List active data files at a specific snapshot.
     *
     * @param catalogName catalog identifier
     * @param namespace   Iceberg namespace
     * @param tableName   table name
     * @param config      catalog and storage configuration
     * @param snapshotId  snapshot ID (-1 for current)
     * @return list of active data file entries
     */
    public static List<IcebergFileEntry> listFiles(
            String catalogName, String namespace, String tableName,
            Map<String, String> config, long snapshotId) {
        return listFiles(catalogName, namespace, tableName, config, snapshotId, false);
    }

    /**
     * List active data files at the current snapshot (compact mode).
     *
     * <p>Compact mode returns only core fields: path, file_format, record_count,
     * file_size_bytes, and snapshot_id. Partition values and content type are
     * omitted, reducing serialization overhead.
     *
     * @param catalogName catalog identifier
     * @param namespace   Iceberg namespace
     * @param tableName   table name
     * @param config      catalog and storage configuration
     * @param compact     if true, skip partition_values and content_type
     * @return list of active data file entries
     */
    public static List<IcebergFileEntry> listFiles(
            String catalogName, String namespace, String tableName,
            Map<String, String> config, boolean compact) {
        return listFiles(catalogName, namespace, tableName, config, -1, compact);
    }

    /**
     * List active data files in an Iceberg table.
     *
     * @param catalogName catalog identifier
     * @param namespace   Iceberg namespace
     * @param tableName   table name
     * @param config      catalog and storage configuration
     * @param snapshotId  snapshot ID (-1 for current)
     * @param compact     if true, skip partition_values and content_type
     * @return list of active data file entries
     */
    public static List<IcebergFileEntry> listFiles(
            String catalogName, String namespace, String tableName,
            Map<String, String> config, long snapshotId, boolean compact) {
        return listFiles(catalogName, namespace, tableName, config, snapshotId, compact, null);
    }

    /**
     * List active data files with partition predicate filtering.
     *
     * @param catalogName catalog identifier
     * @param namespace   Iceberg namespace
     * @param tableName   table name
     * @param config      catalog and storage configuration
     * @param snapshotId  snapshot ID (-1 for current)
     * @param compact     if true, skip partition_values and content_type
     * @param filter      partition filter (null for no filtering)
     * @return list of matching data file entries
     */
    public static List<IcebergFileEntry> listFiles(
            String catalogName, String namespace, String tableName,
            Map<String, String> config, long snapshotId, boolean compact,
            PartitionFilter filter) {
        validateParams(catalogName, namespace, tableName, config);

        String predicateJson = filter != null ? filter.toJson() : null;
        byte[] bytes = nativeListFiles(catalogName, namespace, tableName, snapshotId,
                config != null ? config : Collections.emptyMap(), compact, predicateJson);

        if (bytes == null) {
            throw new RuntimeException("Native listFiles returned null (check preceding exception)");
        }

        ByteBuffer buffer = ByteBuffer.wrap(bytes);
        buffer.order(ByteOrder.nativeOrder());

        BatchDocumentReader reader = new BatchDocumentReader();
        List<Map<String, Object>> maps = reader.parseToMaps(buffer);

        List<IcebergFileEntry> entries = new ArrayList<>(maps.size());
        for (Map<String, Object> map : maps) {
            entries.add(IcebergFileEntry.fromMap(map));
        }
        return entries;
    }

    // ── Read Schema ───────────────────────────────────────────────────────────

    /**
     * Read the schema of an Iceberg table (current snapshot).
     *
     * @param catalogName catalog identifier
     * @param namespace   Iceberg namespace
     * @param tableName   table name
     * @param config      catalog and storage configuration
     * @return the table schema
     */
    public static IcebergTableSchema readSchema(
            String catalogName, String namespace, String tableName, Map<String, String> config) {
        return readSchema(catalogName, namespace, tableName, config, -1);
    }

    /**
     * Read the schema of an Iceberg table at a specific snapshot.
     *
     * @param catalogName catalog identifier
     * @param namespace   Iceberg namespace
     * @param tableName   table name
     * @param config      catalog and storage configuration
     * @param snapshotId  snapshot ID (-1 for current)
     * @return the table schema
     */
    public static IcebergTableSchema readSchema(
            String catalogName, String namespace, String tableName,
            Map<String, String> config, long snapshotId) {
        validateParams(catalogName, namespace, tableName, config);

        byte[] bytes = nativeReadSchema(catalogName, namespace, tableName, snapshotId,
                config != null ? config : Collections.emptyMap());

        if (bytes == null) {
            throw new RuntimeException("Native readSchema returned null (check preceding exception)");
        }

        ByteBuffer buffer = ByteBuffer.wrap(bytes);
        buffer.order(ByteOrder.nativeOrder());

        BatchDocumentReader reader = new BatchDocumentReader();
        List<Map<String, Object>> maps = reader.parseToMaps(buffer);

        if (maps.isEmpty()) {
            throw new RuntimeException("Empty schema response from native layer");
        }

        // First document is the header: schema_json, snapshot_id, field_count
        Map<String, Object> header = maps.get(0);
        String schemaJson = (String) header.get("schema_json");
        long actualSnapshotId = toLong(header.get("snapshot_id"));

        // Remaining documents are field entries
        List<IcebergSchemaField> fields = new ArrayList<>(maps.size() - 1);
        for (int i = 1; i < maps.size(); i++) {
            fields.add(IcebergSchemaField.fromMap(maps.get(i)));
        }

        return new IcebergTableSchema(fields, schemaJson, actualSnapshotId);
    }

    // ── List Snapshots ────────────────────────────────────────────────────────

    /**
     * List all snapshots (transaction history) of an Iceberg table.
     *
     * @param catalogName catalog identifier
     * @param namespace   Iceberg namespace
     * @param tableName   table name
     * @param config      catalog and storage configuration
     * @return list of snapshots
     */
    public static List<IcebergSnapshot> listSnapshots(
            String catalogName, String namespace, String tableName, Map<String, String> config) {
        validateParams(catalogName, namespace, tableName, config);

        byte[] bytes = nativeListSnapshots(catalogName, namespace, tableName,
                config != null ? config : Collections.emptyMap());

        if (bytes == null) {
            throw new RuntimeException("Native listSnapshots returned null (check preceding exception)");
        }

        ByteBuffer buffer = ByteBuffer.wrap(bytes);
        buffer.order(ByteOrder.nativeOrder());

        BatchDocumentReader reader = new BatchDocumentReader();
        List<Map<String, Object>> maps = reader.parseToMaps(buffer);

        List<IcebergSnapshot> snapshots = new ArrayList<>(maps.size());
        for (Map<String, Object> map : maps) {
            snapshots.add(IcebergSnapshot.fromMap(map));
        }
        return snapshots;
    }

    // ── Helpers ───────────────────────────────────────────────────────────────

    private static void validateParams(String catalogName, String namespace, String tableName, Map<String, String> config) {
        if (catalogName == null || catalogName.isEmpty()) {
            throw new IllegalArgumentException("catalogName must not be null or empty");
        }
        if (namespace == null || namespace.isEmpty()) {
            throw new IllegalArgumentException("namespace must not be null or empty");
        }
        if (tableName == null || tableName.isEmpty()) {
            throw new IllegalArgumentException("tableName must not be null or empty");
        }
    }

    private static long toLong(Object value) {
        if (value instanceof Number) {
            return ((Number) value).longValue();
        }
        return -1;
    }

    // ── Distributed scanning primitives ──────────────────────────────────────

    /**
     * Get lightweight snapshot metadata for distributed scanning (current snapshot).
     *
     * <p>Opens catalog, loads table metadata, and reads the manifest list.
     * Returns manifest file paths — does NOT read manifest file contents.
     *
     * @param catalogName catalog identifier
     * @param namespace   Iceberg namespace
     * @param tableName   table name
     * @param config      catalog and storage configuration
     * @return snapshot metadata with manifest file paths
     */
    public static IcebergSnapshotInfo getSnapshotInfo(
            String catalogName, String namespace, String tableName, Map<String, String> config) {
        return getSnapshotInfo(catalogName, namespace, tableName, config, -1);
    }

    /**
     * Get lightweight snapshot metadata for distributed scanning.
     *
     * @param catalogName catalog identifier
     * @param namespace   Iceberg namespace
     * @param tableName   table name
     * @param config      catalog and storage configuration
     * @param snapshotId  snapshot ID (-1 for current)
     * @return snapshot metadata with manifest file paths
     */
    public static IcebergSnapshotInfo getSnapshotInfo(
            String catalogName, String namespace, String tableName,
            Map<String, String> config, long snapshotId) {
        validateParams(catalogName, namespace, tableName, config);

        byte[] bytes = nativeGetSnapshotInfo(catalogName, namespace, tableName, snapshotId,
                config != null ? config : Collections.emptyMap());

        if (bytes == null) {
            throw new RuntimeException("Native getSnapshotInfo returned null (check preceding exception)");
        }

        ByteBuffer buffer = ByteBuffer.wrap(bytes);
        buffer.order(ByteOrder.nativeOrder());

        BatchDocumentReader reader = new BatchDocumentReader();
        List<Map<String, Object>> maps = reader.parseToMaps(buffer);

        return IcebergSnapshotInfo.fromMaps(maps);
    }

    /**
     * Read one manifest file and extract data file entries.
     *
     * <p>Designed for executor-side use: reads a single manifest avro file
     * and returns the DataFile entries (excluding deleted entries).
     *
     * @param catalogName  catalog identifier (for FileIO construction)
     * @param namespace    Iceberg namespace
     * @param tableName    table name
     * @param config       catalog and storage configuration
     * @param manifestPath full path to the manifest avro file
     * @return list of file entries from this manifest
     */
    public static List<IcebergFileEntry> readManifestFile(
            String catalogName, String namespace, String tableName,
            Map<String, String> config, String manifestPath) {
        return readManifestFile(catalogName, namespace, tableName, config, manifestPath, false);
    }

    /**
     * Read one manifest file and extract data file entries (with compact option).
     *
     * @param catalogName  catalog identifier
     * @param namespace    Iceberg namespace
     * @param tableName    table name
     * @param config       catalog and storage configuration
     * @param manifestPath full path to the manifest avro file
     * @param compact      if true, skip partition_values and content_type
     * @return list of file entries from this manifest
     */
    public static List<IcebergFileEntry> readManifestFile(
            String catalogName, String namespace, String tableName,
            Map<String, String> config, String manifestPath, boolean compact) {
        return readManifestFile(catalogName, namespace, tableName, config, manifestPath, compact, null);
    }

    /**
     * Read one manifest file with partition predicate filtering.
     *
     * @param catalogName  catalog identifier
     * @param namespace    Iceberg namespace
     * @param tableName    table name
     * @param config       catalog and storage configuration
     * @param manifestPath full path to the manifest avro file
     * @param compact      if true, skip partition_values and content_type
     * @param filter       partition filter (null for no filtering)
     * @return list of matching file entries from this manifest
     */
    public static List<IcebergFileEntry> readManifestFile(
            String catalogName, String namespace, String tableName,
            Map<String, String> config, String manifestPath, boolean compact,
            PartitionFilter filter) {
        validateParams(catalogName, namespace, tableName, config);
        if (manifestPath == null || manifestPath.isEmpty()) {
            throw new IllegalArgumentException("manifestPath must not be null or empty");
        }

        String predicateJson = filter != null ? filter.toJson() : null;
        byte[] bytes = nativeReadManifestFile(catalogName, namespace, tableName, manifestPath,
                config != null ? config : Collections.emptyMap(), compact, predicateJson);

        if (bytes == null) {
            throw new RuntimeException("Native readManifestFile returned null (check preceding exception)");
        }

        ByteBuffer buffer = ByteBuffer.wrap(bytes);
        buffer.order(ByteOrder.nativeOrder());

        BatchDocumentReader reader = new BatchDocumentReader();
        List<Map<String, Object>> maps = reader.parseToMaps(buffer);

        List<IcebergFileEntry> entries = new ArrayList<>(maps.size());
        for (Map<String, Object> map : maps) {
            entries.add(IcebergFileEntry.fromMap(map));
        }
        return entries;
    }

    // ── Arrow FFI ──────────────────────────────────────────────────────────────

    /**
     * Read an Iceberg manifest file and export entries via Arrow FFI.
     *
     * <p>Builds a flat RecordBatch with 7 columns:
     * path (Utf8), file_format (Utf8), record_count (Int64),
     * file_size_bytes (Int64), partition_values (Utf8/JSON),
     * content_type (Utf8), snapshot_id (Int64).
     *
     * <p>The caller must pre-allocate FFI_ArrowArray and FFI_ArrowSchema structs
     * for each column and pass their memory addresses.
     *
     * @param catalogName  catalog identifier
     * @param namespace    Iceberg namespace
     * @param tableName    table name
     * @param config       catalog and storage configuration
     * @param manifestPath full path to the manifest avro file
     * @param filter       partition filter (null for no filtering)
     * @param arrayAddrs   pre-allocated FFI_ArrowArray addresses (7 columns)
     * @param schemaAddrs  pre-allocated FFI_ArrowSchema addresses (7 columns)
     * @return number of rows written, or -1 on error
     */
    public static int readManifestFileArrowFfi(
            String catalogName, String namespace, String tableName,
            Map<String, String> config, String manifestPath,
            PartitionFilter filter, long[] arrayAddrs, long[] schemaAddrs) {
        validateParams(catalogName, namespace, tableName, config);
        if (manifestPath == null || manifestPath.isEmpty()) {
            throw new IllegalArgumentException("manifestPath must not be null or empty");
        }
        if (arrayAddrs == null || arrayAddrs.length < 7) {
            throw new IllegalArgumentException("arrayAddrs must have at least 7 elements");
        }
        if (schemaAddrs == null || schemaAddrs.length < 7) {
            throw new IllegalArgumentException("schemaAddrs must have at least 7 elements");
        }

        String predicateJson = filter != null ? filter.toJson() : null;
        return nativeReadManifestFileArrowFfi(catalogName, namespace, tableName,
                manifestPath, config != null ? config : Collections.emptyMap(),
                predicateJson, arrayAddrs, schemaAddrs);
    }

    // ── Native methods ────────────────────────────────────────────────────────

    private static native byte[] nativeListFiles(
            String catalogName, String namespace, String tableName,
            long snapshotId, Map<String, String> config, boolean compact,
            String predicateJson);

    private static native byte[] nativeReadSchema(
            String catalogName, String namespace, String tableName,
            long snapshotId, Map<String, String> config);

    private static native byte[] nativeListSnapshots(
            String catalogName, String namespace, String tableName,
            Map<String, String> config);

    private static native byte[] nativeGetSnapshotInfo(
            String catalogName, String namespace, String tableName,
            long snapshotId, Map<String, String> config);

    private static native byte[] nativeReadManifestFile(
            String catalogName, String namespace, String tableName,
            String manifestPath, Map<String, String> config, boolean compact,
            String predicateJson);

    private static native int nativeReadManifestFileArrowFfi(
            String catalogName, String namespace, String tableName,
            String manifestPath, Map<String, String> config,
            String predicateJson, long[] arrayAddrs, long[] schemaAddrs);
}

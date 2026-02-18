package io.indextables.tantivy4java.parquet;

import io.indextables.tantivy4java.batch.BatchDocumentReader;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Static utility for distributed scanning of Hive-style partitioned parquet directories.
 *
 * <p>Provides two primitives for driver/executor split:
 * <ul>
 *   <li>{@link #getTableInfo} — Driver: lists partition directories + reads schema</li>
 *   <li>{@link #listPartitionFiles} — Executor: lists .parquet files in one partition</li>
 * </ul>
 *
 * <p>Credentials are passed via a config map with the same keys as
 * {@link io.indextables.tantivy4java.delta.DeltaTableReader}.
 *
 * <h3>Usage Example (Spark)</h3>
 * <pre>{@code
 * // DRIVER: discover partition directories
 * ParquetTableInfo info = ParquetTableReader.getTableInfo(tableUrl, config);
 *
 * // DISTRIBUTED: list files per partition across executors
 * JavaRDD<String> partitionsRDD = sc.parallelize(info.getPartitionDirectories(), 100);
 * JavaRDD<ParquetFileEntry> filesRDD = partitionsRDD.flatMap(partDir ->
 *     ParquetTableReader.listPartitionFiles(tableUrl, config, partDir).iterator());
 * }</pre>
 */
public class ParquetTableReader {

    static {
        io.indextables.tantivy4java.core.Tantivy.initialize();
    }

    private ParquetTableReader() {
        // static utility
    }

    /**
     * Get lightweight table metadata for a Hive-style partitioned parquet directory.
     *
     * <p>Lists immediate children of the root URL using a single LIST call.
     * Returns partition directory paths and schema — does NOT recurse into partitions.
     *
     * @param tableUrl table root URL (local path, file://, s3://, or azure://)
     * @param config   credential and storage configuration
     * @return table metadata with partition directories and schema
     */
    public static ParquetTableInfo getTableInfo(String tableUrl, Map<String, String> config) {
        if (tableUrl == null || tableUrl.isEmpty()) {
            throw new IllegalArgumentException("tableUrl must not be null or empty");
        }

        byte[] bytes = nativeGetTableInfo(tableUrl,
                config != null ? config : Collections.emptyMap());

        ByteBuffer buffer = ByteBuffer.wrap(bytes);
        buffer.order(ByteOrder.nativeOrder());

        BatchDocumentReader reader = new BatchDocumentReader();
        List<Map<String, Object>> maps = reader.parseToMaps(buffer);

        return ParquetTableInfo.fromMaps(maps);
    }

    /**
     * List all .parquet files under a single partition directory.
     *
     * <p>Designed for executor-side use: lists files in one partition prefix
     * and parses partition values from the path.
     *
     * @param tableUrl        table root URL
     * @param config          credential and storage configuration
     * @param partitionPrefix partition directory prefix (e.g. "year=2024/month=01/")
     * @return list of parquet file entries with partition values
     */
    public static List<ParquetFileEntry> listPartitionFiles(
            String tableUrl, Map<String, String> config, String partitionPrefix) {
        if (tableUrl == null || tableUrl.isEmpty()) {
            throw new IllegalArgumentException("tableUrl must not be null or empty");
        }
        if (partitionPrefix == null || partitionPrefix.isEmpty()) {
            throw new IllegalArgumentException("partitionPrefix must not be null or empty");
        }

        byte[] bytes = nativeListPartitionFiles(tableUrl,
                config != null ? config : Collections.emptyMap(), partitionPrefix);

        ByteBuffer buffer = ByteBuffer.wrap(bytes);
        buffer.order(ByteOrder.nativeOrder());

        BatchDocumentReader reader = new BatchDocumentReader();
        List<Map<String, Object>> maps = reader.parseToMaps(buffer);

        List<ParquetFileEntry> entries = new ArrayList<>(maps.size());
        for (Map<String, Object> map : maps) {
            entries.add(ParquetFileEntry.fromMap(map));
        }
        return entries;
    }

    // ── Native methods ───────────────────────────────────────────────────────

    private static native byte[] nativeGetTableInfo(String tableUrl, Map<String, String> config);
    private static native byte[] nativeListPartitionFiles(
            String tableUrl, Map<String, String> config, String partitionPrefix);
}

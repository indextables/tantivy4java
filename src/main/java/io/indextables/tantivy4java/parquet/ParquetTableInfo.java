package io.indextables.tantivy4java.parquet;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

/**
 * Lightweight table metadata for distributed Hive-style parquet directory scanning.
 *
 * <p>Contains partition directory paths — does NOT list files within partitions.
 * Use {@link ParquetTableReader#listPartitionFiles} to list files in individual
 * partition directories on executors.
 *
 * <p>Implements {@link Serializable} for Spark broadcast/shuffle.
 */
public class ParquetTableInfo implements Serializable {

    private static final long serialVersionUID = 1L;
    private static final transient Logger LOG = Logger.getLogger(ParquetTableInfo.class.getName());

    private final String schemaJson;
    private final List<String> partitionColumns;
    private final List<String> partitionDirectories;
    private final List<ParquetFileEntry> rootFiles;
    private final boolean partitioned;

    ParquetTableInfo(String schemaJson, List<String> partitionColumns,
                     List<String> partitionDirectories, List<ParquetFileEntry> rootFiles,
                     boolean partitioned) {
        this.schemaJson = schemaJson;
        this.partitionColumns = partitionColumns != null
                ? Collections.unmodifiableList(partitionColumns)
                : Collections.emptyList();
        this.partitionDirectories = partitionDirectories != null
                ? Collections.unmodifiableList(partitionDirectories)
                : Collections.emptyList();
        this.rootFiles = rootFiles != null
                ? Collections.unmodifiableList(rootFiles)
                : Collections.emptyList();
        this.partitioned = partitioned;
    }

    /** Arrow schema as JSON string (from first parquet file footer). */
    public String getSchemaJson() { return schemaJson; }

    /** Inferred partition column names (from directory key=value patterns). */
    public List<String> getPartitionColumns() { return partitionColumns; }

    /** Partition directory paths for distribution to executors. */
    public List<String> getPartitionDirectories() { return partitionDirectories; }

    /** Root-level parquet files (for unpartitioned tables). */
    public List<ParquetFileEntry> getRootFiles() { return rootFiles; }

    /** Whether the table is partitioned. */
    public boolean isPartitioned() { return partitioned; }

    @Override
    public String toString() {
        return String.format("ParquetTableInfo{partitions=%d, rootFiles=%d, partitioned=%b}",
                partitionDirectories.size(), rootFiles.size(), partitioned);
    }

    /**
     * Construct from parsed TANT byte buffer maps.
     *
     * <p>Map 0 is the header: schema_json, partition_columns_json, num_partitions, num_root_files, is_partitioned.
     * Maps 1..num_partitions are partition directory paths.
     * Maps num_partitions+1..end are root file entries.
     */
    static ParquetTableInfo fromMaps(List<Map<String, Object>> maps) {
        if (maps.isEmpty()) {
            throw new RuntimeException("Empty table info response");
        }

        Map<String, Object> header = maps.get(0);
        String schemaJson = (String) header.get("schema_json");
        List<String> partitionColumns = parseJsonList(header.get("partition_columns_json"));
        int numPartitions = (int) toLong(header.get("num_partitions"));
        int numRootFiles = (int) toLong(header.get("num_root_files"));
        boolean isPartitioned = toBoolean(header.get("is_partitioned"));

        List<String> partitionDirs = new ArrayList<>(numPartitions);
        for (int i = 1; i <= numPartitions && i < maps.size(); i++) {
            String path = (String) maps.get(i).get("path");
            if (path != null) {
                partitionDirs.add(path);
            }
        }

        List<ParquetFileEntry> rootFiles = new ArrayList<>(numRootFiles);
        for (int i = 1 + numPartitions; i < maps.size(); i++) {
            rootFiles.add(ParquetFileEntry.fromMap(maps.get(i)));
        }

        return new ParquetTableInfo(schemaJson, partitionColumns, partitionDirs,
                rootFiles, isPartitioned);
    }

    /** Parse a Number to long, returning 0 for non-numeric values. */
    static long toLong(Object value) {
        if (value instanceof Number) return ((Number) value).longValue();
        return 0;
    }

    private static boolean toBoolean(Object value) {
        if (value instanceof Boolean) return (Boolean) value;
        return false;
    }

    private static List<String> parseJsonList(Object value) {
        if (value == null) return Collections.emptyList();
        String json = value.toString();
        if (json.isEmpty() || "[]".equals(json)) return Collections.emptyList();
        try {
            return new ObjectMapper().readValue(json,
                    new TypeReference<List<String>>() {});
        } catch (Exception e) {
            LOG.fine("Failed to parse JSON list: " + e.getMessage());
            return Collections.emptyList();
        }
    }
}

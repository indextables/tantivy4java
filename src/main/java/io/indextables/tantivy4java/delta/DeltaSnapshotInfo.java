package io.indextables.tantivy4java.delta;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.Serializable;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Lightweight snapshot metadata for distributed Delta table scanning.
 *
 * <p>Contains checkpoint part paths and commit file paths — does NOT contain
 * the actual file entries. Use {@link DeltaTableReader#readCheckpointPart(String, Map, String)}
 * to read individual checkpoint parts on executors.
 *
 * <p>Implements {@link Serializable} for Spark broadcast/shuffle.
 */
public class DeltaSnapshotInfo implements Serializable {

    private static final long serialVersionUID = 1L;
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final TypeReference<List<String>> LIST_TYPE = new TypeReference<List<String>>() {};

    private final long version;
    private final String schemaJson;
    private final List<String> partitionColumns;
    private final List<String> checkpointPartPaths;
    private final List<String> commitFilePaths;
    private final long numAddFiles;

    DeltaSnapshotInfo(long version, String schemaJson, List<String> partitionColumns,
                      List<String> checkpointPartPaths, List<String> commitFilePaths,
                      long numAddFiles) {
        this.version = version;
        this.schemaJson = schemaJson;
        this.partitionColumns = partitionColumns != null
                ? Collections.unmodifiableList(partitionColumns)
                : Collections.emptyList();
        this.checkpointPartPaths = checkpointPartPaths != null
                ? Collections.unmodifiableList(checkpointPartPaths)
                : Collections.emptyList();
        this.commitFilePaths = commitFilePaths != null
                ? Collections.unmodifiableList(commitFilePaths)
                : Collections.emptyList();
        this.numAddFiles = numAddFiles;
    }

    /** Checkpoint version. */
    public long getVersion() { return version; }

    /** Delta schema as JSON string. */
    public String getSchemaJson() { return schemaJson; }

    /** Partition column names from the Delta table metadata. */
    public List<String> getPartitionColumns() { return partitionColumns; }

    /** Checkpoint parquet file paths (relative to _delta_log/). */
    public List<String> getCheckpointPartPaths() { return checkpointPartPaths; }

    /** Post-checkpoint JSON commit file paths (relative to _delta_log/). */
    public List<String> getCommitFilePaths() { return commitFilePaths; }

    /** Number of add file entries recorded in _last_checkpoint, or -1 if unknown. */
    public long getNumAddFiles() { return numAddFiles; }

    @Override
    public String toString() {
        return String.format("DeltaSnapshotInfo{version=%d, checkpoints=%d, commits=%d, numAddFiles=%d}",
                version, checkpointPartPaths.size(), commitFilePaths.size(), numAddFiles);
    }

    /**
     * Construct from a parsed TANT byte buffer map.
     */
    static DeltaSnapshotInfo fromMap(Map<String, Object> map) {
        long version = toLong(map.get("version"));
        String schemaJson = (String) map.get("schema_json");
        List<String> partitionColumns = parseJsonList(map.get("partition_columns_json"));
        List<String> checkpointPaths = parseJsonList(map.get("checkpoint_part_paths_json"));
        List<String> commitPaths = parseJsonList(map.get("commit_file_paths_json"));
        long numAddFiles = toLong(map.get("num_add_files"));

        return new DeltaSnapshotInfo(version, schemaJson, partitionColumns,
                checkpointPaths, commitPaths, numAddFiles);
    }

    private static long toLong(Object value) {
        if (value instanceof Number) {
            return ((Number) value).longValue();
        }
        return -1;
    }

    private static List<String> parseJsonList(Object value) {
        if (value == null) return Collections.emptyList();
        String json = value.toString();
        if (json.isEmpty() || "[]".equals(json)) return Collections.emptyList();
        try {
            return MAPPER.readValue(json, LIST_TYPE);
        } catch (Exception e) {
            return Collections.emptyList();
        }
    }
}

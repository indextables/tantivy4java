package io.indextables.tantivy4java.delta;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.Serializable;
import java.util.Collections;
import java.util.Map;

/**
 * Immutable data class representing a single active parquet file in a Delta table.
 *
 * <p>Instances are returned by {@link DeltaTableReader#listFiles(String)} and contain
 * metadata from the Delta transaction log including file path, size, record count,
 * partition values, and deletion vector status.
 *
 * <p>Implements {@link Serializable} for Spark broadcast/shuffle.
 */
public class DeltaFileEntry implements Serializable {

    private static final long serialVersionUID = 1L;

    private final String path;
    private final long size;
    private final long modificationTime;
    private final long numRecords;
    private final Map<String, String> partitionValues;
    private final boolean hasDeletionVector;
    private final long tableVersion;

    public DeltaFileEntry(String path, long size, long modificationTime, long numRecords,
                          Map<String, String> partitionValues, boolean hasDeletionVector, long tableVersion) {
        this.path = path;
        this.size = size;
        this.modificationTime = modificationTime;
        this.numRecords = numRecords;
        this.partitionValues = partitionValues != null
                ? Collections.unmodifiableMap(partitionValues)
                : Collections.emptyMap();
        this.hasDeletionVector = hasDeletionVector;
        this.tableVersion = tableVersion;
    }

    /** @return parquet file path relative to the table root */
    public String getPath() { return path; }

    /** @return file size in bytes */
    public long getSize() { return size; }

    /** @return epoch milliseconds when the file was created */
    public long getModificationTime() { return modificationTime; }

    /** @return number of records in the file, or -1 if unknown */
    public long getNumRecords() { return numRecords; }

    /** @return true if record count information is available */
    public boolean hasNumRecords() { return numRecords >= 0; }

    /** @return partition column name to value mapping (empty if unpartitioned) */
    public Map<String, String> getPartitionValues() { return partitionValues; }

    /** @return true if this file has an associated deletion vector */
    public boolean hasDeletionVector() { return hasDeletionVector; }

    /** @return the Delta table snapshot version this file listing was read from */
    public long getTableVersion() { return tableVersion; }

    @Override
    public String toString() {
        return String.format("DeltaFileEntry{path='%s', size=%d, records=%s, version=%d}",
                path, size, hasNumRecords() ? String.valueOf(numRecords) : "unknown", tableVersion);
    }

    /**
     * Construct a DeltaFileEntry from a parsed TANT byte buffer map.
     * Package-private; used by DeltaTableReader.
     */
    static DeltaFileEntry fromMap(Map<String, Object> map) {
        String path = (String) map.get("path");
        if (path == null || path.isEmpty()) {
            throw new IllegalStateException("DeltaFileEntry missing required 'path' field");
        }
        long size = DeltaSnapshotInfo.toLong(map.get("size"));
        long modificationTime = DeltaSnapshotInfo.toLong(map.get("modification_time"));
        long numRecords = DeltaSnapshotInfo.toLong(map.get("num_records"));
        boolean hasDv = toBoolean(map.get("has_deletion_vector"));
        long version = DeltaSnapshotInfo.toLong(map.get("table_version"));

        Map<String, String> partitionValues = parsePartitionValues(map.get("partition_values"));

        return new DeltaFileEntry(path, size, modificationTime, numRecords, partitionValues, hasDv, version);
    }

    private static boolean toBoolean(Object value) {
        if (value instanceof Boolean) {
            return (Boolean) value;
        }
        return false;
    }

    private static Map<String, String> parsePartitionValues(Object value) {
        if (value == null) return Collections.emptyMap();
        String jsonStr = value.toString();
        if (jsonStr.isEmpty() || "{}".equals(jsonStr)) return Collections.emptyMap();
        try {
            return new ObjectMapper().readValue(jsonStr,
                    new TypeReference<Map<String, String>>() {});
        } catch (Exception e) {
            return Collections.emptyMap();
        }
    }
}

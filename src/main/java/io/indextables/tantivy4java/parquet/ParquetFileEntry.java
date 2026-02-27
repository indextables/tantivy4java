package io.indextables.tantivy4java.parquet;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.Serializable;
import java.util.Collections;
import java.util.Map;
import java.util.logging.Logger;

/**
 * A single parquet file entry with metadata, for Hive-style partitioned directories.
 *
 * <p>Implements {@link Serializable} for Spark broadcast/shuffle.
 */
public class ParquetFileEntry implements Serializable {

    private static final long serialVersionUID = 1L;
    private static final transient Logger LOG = Logger.getLogger(ParquetFileEntry.class.getName());

    private final String path;
    private final long size;
    private final long lastModified;
    private final Map<String, String> partitionValues;

    public ParquetFileEntry(String path, long size, long lastModified, Map<String, String> partitionValues) {
        this.path = path;
        this.size = size;
        this.lastModified = lastModified;
        this.partitionValues = partitionValues != null
                ? Collections.unmodifiableMap(partitionValues)
                : Collections.emptyMap();
    }

    /** Full path to the parquet file. */
    public String getPath() { return path; }

    /** File size in bytes. */
    public long getSize() { return size; }

    /** Last modified timestamp (epoch millis). */
    public long getLastModified() { return lastModified; }

    /** Partition values parsed from path (key=value segments). */
    public Map<String, String> getPartitionValues() { return partitionValues; }

    @Override
    public String toString() {
        return String.format("ParquetFileEntry{path='%s', size=%d}", path, size);
    }

    /**
     * Construct from a parsed TANT byte buffer map.
     */
    static ParquetFileEntry fromMap(Map<String, Object> map) {
        String path = (String) map.get("path");
        if (path == null || path.isEmpty()) {
            throw new IllegalStateException("ParquetFileEntry missing required 'path' field");
        }
        long size = ParquetTableInfo.toLong(map.get("size"));
        long lastModified = ParquetTableInfo.toLong(map.get("last_modified"));
        Map<String, String> partitionValues = parsePartitionValues(map.get("partition_values"));
        return new ParquetFileEntry(path, size, lastModified, partitionValues);
    }

    private static Map<String, String> parsePartitionValues(Object value) {
        if (value == null) return Collections.emptyMap();
        String jsonStr = value.toString();
        if (jsonStr.isEmpty() || "{}".equals(jsonStr)) return Collections.emptyMap();
        try {
            return new ObjectMapper().readValue(jsonStr,
                    new TypeReference<Map<String, String>>() {});
        } catch (Exception e) {
            LOG.fine("Failed to parse partition values JSON: " + e.getMessage());
            return Collections.emptyMap();
        }
    }
}

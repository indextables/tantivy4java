package io.indextables.tantivy4java.iceberg;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.Serializable;
import java.util.Collections;
import java.util.Map;

/**
 * Immutable data class representing a single active data file in an Iceberg table.
 *
 * <p>Instances are returned by {@link IcebergTableReader#listFiles} and contain
 * metadata from the Iceberg manifest including file path, format, record count,
 * size, partition values, content type, and the snapshot that added the file.
 *
 * <p>Implements {@link Serializable} for Spark broadcast/shuffle.
 */
public class IcebergFileEntry implements Serializable {

    private static final long serialVersionUID = 1L;

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final TypeReference<Map<String, String>> MAP_TYPE = new TypeReference<Map<String, String>>() {};

    private final String path;
    private final String fileFormat;
    private final long recordCount;
    private final long fileSizeBytes;
    private final Map<String, String> partitionValues;
    private final String contentType;
    private final long snapshotId;

    public IcebergFileEntry(String path, String fileFormat, long recordCount, long fileSizeBytes,
                            Map<String, String> partitionValues, String contentType, long snapshotId) {
        this.path = path;
        this.fileFormat = fileFormat;
        this.recordCount = recordCount;
        this.fileSizeBytes = fileSizeBytes;
        this.partitionValues = partitionValues != null
                ? Collections.unmodifiableMap(partitionValues)
                : Collections.emptyMap();
        this.contentType = contentType;
        this.snapshotId = snapshotId;
    }

    /**
     * @return data file path (full URI with scheme, e.g. s3://bucket/data/part-00000.parquet)
     */
    public String getPath() {
        return path;
    }

    /**
     * @return file format: "parquet", "orc", "avro", or "puffin"
     */
    public String getFileFormat() {
        return fileFormat;
    }

    /**
     * @return number of records in the file
     */
    public long getRecordCount() {
        return recordCount;
    }

    /**
     * @return file size in bytes
     */
    public long getFileSizeBytes() {
        return fileSizeBytes;
    }

    /**
     * @return partition column name to value mapping (empty if unpartitioned)
     */
    public Map<String, String> getPartitionValues() {
        return partitionValues;
    }

    /**
     * @return content type: "data", "equality_deletes", or "position_deletes"
     */
    public String getContentType() {
        return contentType;
    }

    /**
     * @return snapshot ID that added this file
     */
    public long getSnapshotId() {
        return snapshotId;
    }

    @Override
    public String toString() {
        return String.format("IcebergFileEntry{path='%s', format='%s', records=%d, size=%d, snapshot=%d}",
                path, fileFormat, recordCount, fileSizeBytes, snapshotId);
    }

    /**
     * Construct from a parsed TANT byte buffer map.
     * Package-private; used by IcebergTableReader.
     */
    static IcebergFileEntry fromMap(Map<String, Object> map) {
        String path = (String) map.get("path");
        String fileFormat = (String) map.getOrDefault("file_format", "parquet");
        long recordCount = toLong(map.get("record_count"));
        long fileSizeBytes = toLong(map.get("file_size_bytes"));
        String contentType = (String) map.getOrDefault("content_type", "data");
        long snapshotId = toLong(map.get("snapshot_id"));

        Map<String, String> partitionValues = parsePartitionValues(map.get("partition_values"));

        return new IcebergFileEntry(path, fileFormat, recordCount, fileSizeBytes,
                partitionValues, contentType, snapshotId);
    }

    private static long toLong(Object value) {
        if (value instanceof Number) {
            return ((Number) value).longValue();
        }
        return -1;
    }

    private static Map<String, String> parsePartitionValues(Object value) {
        if (value == null) {
            return Collections.emptyMap();
        }
        String jsonStr = value.toString();
        if (jsonStr.isEmpty() || "{}".equals(jsonStr)) {
            return Collections.emptyMap();
        }
        try {
            return MAPPER.readValue(jsonStr, MAP_TYPE);
        } catch (Exception e) {
            return Collections.emptyMap();
        }
    }
}

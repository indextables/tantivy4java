package io.indextables.jni.txlog;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.Serializable;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Immutable data class representing a file entry in the transaction log.
 *
 * <p>Contains all {@code AddAction} fields plus streaming metadata
 * ({@code addedAtVersion}, {@code addedAtTimestamp}).
 *
 * <p>Instances are constructed from parsed TANT byte buffers via
 * {@link #fromMap(Map)}.
 *
 * <p>Implements {@link Serializable} for Spark broadcast/shuffle.
 */
public class TxLogFileEntry implements Serializable {

    private static final long serialVersionUID = 1L;
    private static final ObjectMapper MAPPER = new ObjectMapper();

    // Required AddAction fields
    private final String path;
    private final long size;
    private final long modificationTime;
    private final boolean dataChange;
    private final long numRecords;

    // Optional AddAction fields
    private final Map<String, String> partitionValues;
    private final String stats;
    private final Map<String, String> minValues;
    private final Map<String, String> maxValues;
    private final long footerStartOffset;
    private final long footerEndOffset;
    private final boolean hasFooterOffsets;
    private final Map<String, String> splitTags;
    private final int numMergeOps;
    private final String docMappingJson;
    private final long uncompressedSizeBytes;
    private final long timeRangeStart;
    private final long timeRangeEnd;
    private final List<String> companionSourceFiles;
    private final long companionDeltaVersion;
    private final String companionFastFieldMode;

    // Streaming metadata
    private final long addedAtVersion;
    private final long addedAtTimestamp;

    public TxLogFileEntry(
            String path, long size, long modificationTime, boolean dataChange, long numRecords,
            Map<String, String> partitionValues, String stats,
            Map<String, String> minValues, Map<String, String> maxValues,
            long footerStartOffset, long footerEndOffset, boolean hasFooterOffsets,
            Map<String, String> splitTags, int numMergeOps,
            String docMappingJson, long uncompressedSizeBytes,
            long timeRangeStart, long timeRangeEnd,
            List<String> companionSourceFiles, long companionDeltaVersion,
            String companionFastFieldMode,
            long addedAtVersion, long addedAtTimestamp) {
        this.path = path;
        this.size = size;
        this.modificationTime = modificationTime;
        this.dataChange = dataChange;
        this.numRecords = numRecords;
        this.partitionValues = partitionValues != null
                ? Collections.unmodifiableMap(partitionValues)
                : Collections.emptyMap();
        this.stats = stats;
        this.minValues = minValues != null
                ? Collections.unmodifiableMap(minValues)
                : Collections.emptyMap();
        this.maxValues = maxValues != null
                ? Collections.unmodifiableMap(maxValues)
                : Collections.emptyMap();
        this.footerStartOffset = footerStartOffset;
        this.footerEndOffset = footerEndOffset;
        this.hasFooterOffsets = hasFooterOffsets;
        this.splitTags = splitTags != null
                ? Collections.unmodifiableMap(splitTags)
                : Collections.emptyMap();
        this.numMergeOps = numMergeOps;
        this.docMappingJson = docMappingJson;
        this.uncompressedSizeBytes = uncompressedSizeBytes;
        this.timeRangeStart = timeRangeStart;
        this.timeRangeEnd = timeRangeEnd;
        this.companionSourceFiles = companionSourceFiles != null
                ? Collections.unmodifiableList(companionSourceFiles)
                : Collections.emptyList();
        this.companionDeltaVersion = companionDeltaVersion;
        this.companionFastFieldMode = companionFastFieldMode;
        this.addedAtVersion = addedAtVersion;
        this.addedAtTimestamp = addedAtTimestamp;
    }

    /** @return split/parquet file path relative to the table root */
    public String getPath() { return path; }

    /** @return file size in bytes */
    public long getSize() { return size; }

    /** @return epoch milliseconds when the file was created */
    public long getModificationTime() { return modificationTime; }

    /** @return true if this action represents a data change */
    public boolean isDataChange() { return dataChange; }

    /** @return number of records in the file, or -1 if unknown */
    public long getNumRecords() { return numRecords; }

    /** @return partition column name to value mapping (empty if unpartitioned) */
    public Map<String, String> getPartitionValues() { return partitionValues; }

    /** @return JSON statistics string, or null if not present */
    public String getStats() { return stats; }

    /** @return per-column minimum values (empty if not present) */
    public Map<String, String> getMinValues() { return minValues; }

    /** @return per-column maximum values (empty if not present) */
    public Map<String, String> getMaxValues() { return maxValues; }

    /** @return footer start byte offset, or -1 if not present */
    public long getFooterStartOffset() { return footerStartOffset; }

    /** @return footer end byte offset, or -1 if not present */
    public long getFooterEndOffset() { return footerEndOffset; }

    /** @return true if footer offsets are present */
    public boolean getHasFooterOffsets() { return hasFooterOffsets; }

    /** @return split tags (empty if not present) */
    public Map<String, String> getSplitTags() { return splitTags; }

    /** @return number of merge operations, or 0 if not present */
    public int getNumMergeOps() { return numMergeOps; }

    /** @return doc mapping JSON string, or null if not present */
    public String getDocMappingJson() { return docMappingJson; }

    /** @return uncompressed size in bytes, or -1 if not present */
    public long getUncompressedSizeBytes() { return uncompressedSizeBytes; }

    /** @return time range start (epoch micros), or -1 if not present */
    public long getTimeRangeStart() { return timeRangeStart; }

    /** @return time range end (epoch micros), or -1 if not present */
    public long getTimeRangeEnd() { return timeRangeEnd; }

    /** @return companion source file paths (empty if not present) */
    public List<String> getCompanionSourceFiles() { return companionSourceFiles; }

    /** @return companion delta version, or -1 if not present */
    public long getCompanionDeltaVersion() { return companionDeltaVersion; }

    /** @return companion fast field mode string, or null if not present */
    public String getCompanionFastFieldMode() { return companionFastFieldMode; }

    /** @return version at which this file was added to the log */
    public long getAddedAtVersion() { return addedAtVersion; }

    /** @return epoch milliseconds when this file was added to the log */
    public long getAddedAtTimestamp() { return addedAtTimestamp; }

    @Override
    public String toString() {
        return String.format("TxLogFileEntry{path='%s', size=%d, records=%d, addedAtVersion=%d}",
                path, size, numRecords, addedAtVersion);
    }

    /**
     * Construct a TxLogFileEntry from a parsed TANT byte buffer map.
     *
     * @param map field name to value map from {@code BatchDocumentReader.parseToMaps()}
     * @return a new TxLogFileEntry
     * @throws IllegalStateException if the required 'path' field is missing
     */
    public static TxLogFileEntry fromMap(Map<String, Object> map) {
        String path = (String) map.get("path");
        if (path == null || path.isEmpty()) {
            throw new IllegalStateException("TxLogFileEntry missing required 'path' field");
        }

        long size = toLong(map.get("size"));
        long modificationTime = toLong(map.get("modification_time"));
        boolean dataChange = toBoolean(map.get("data_change"));
        long numRecords = toLong(map.get("num_records"));
        Map<String, String> partitionValues = parseJsonMap(map.get("partition_values"));
        String stats = (String) map.get("stats");
        Map<String, String> minValues = parseJsonMap(map.get("min_values"));
        Map<String, String> maxValues = parseJsonMap(map.get("max_values"));
        long footerStartOffset = toLong(map.get("footer_start_offset"));
        long footerEndOffset = toLong(map.get("footer_end_offset"));
        boolean hasFooterOffsets = toBoolean(map.get("has_footer_offsets"));
        Map<String, String> splitTags = parseJsonMap(map.get("split_tags"));
        int numMergeOps = toInt(map.get("num_merge_ops"));
        String docMappingJson = (String) map.get("doc_mapping_json");
        long uncompressedSizeBytes = toLong(map.get("uncompressed_size_bytes"));
        long timeRangeStart = toLong(map.get("time_range_start"));
        long timeRangeEnd = toLong(map.get("time_range_end"));
        List<String> companionSourceFiles = parseJsonList(map.get("companion_source_files"));
        long companionDeltaVersion = toLong(map.get("companion_delta_version"));
        String companionFastFieldMode = (String) map.get("companion_fast_field_mode");
        long addedAtVersion = toLong(map.get("added_at_version"));
        long addedAtTimestamp = toLong(map.get("added_at_timestamp"));

        return new TxLogFileEntry(
                path, size, modificationTime, dataChange, numRecords,
                partitionValues, stats, minValues, maxValues,
                footerStartOffset, footerEndOffset, hasFooterOffsets,
                splitTags, numMergeOps, docMappingJson, uncompressedSizeBytes,
                timeRangeStart, timeRangeEnd,
                companionSourceFiles, companionDeltaVersion, companionFastFieldMode,
                addedAtVersion, addedAtTimestamp);
    }

    // --- Helpers ---

    static long toLong(Object value) {
        if (value instanceof Number) {
            return ((Number) value).longValue();
        }
        return -1;
    }

    static int toInt(Object value) {
        if (value instanceof Number) {
            return ((Number) value).intValue();
        }
        return 0;
    }

    static boolean toBoolean(Object value) {
        if (value instanceof Boolean) {
            return (Boolean) value;
        }
        return false;
    }

    static Map<String, String> parseJsonMap(Object value) {
        if (value == null) return Collections.emptyMap();
        String jsonStr = value.toString();
        if (jsonStr.isEmpty() || "{}".equals(jsonStr)) return Collections.emptyMap();
        try {
            return MAPPER.readValue(jsonStr,
                    new TypeReference<Map<String, String>>() {});
        } catch (Exception e) {
            return Collections.emptyMap();
        }
    }

    static List<String> parseJsonList(Object value) {
        if (value == null) return Collections.emptyList();
        String jsonStr = value.toString();
        if (jsonStr.isEmpty() || "[]".equals(jsonStr)) return Collections.emptyList();
        try {
            return MAPPER.readValue(jsonStr,
                    new TypeReference<List<String>>() {});
        } catch (Exception e) {
            return Collections.emptyList();
        }
    }
}

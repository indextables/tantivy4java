package io.indextables.tantivy4java.iceberg;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Immutable data class representing an Iceberg table snapshot (transaction).
 *
 * <p>Instances are returned by {@link IcebergTableReader#listSnapshots} and contain
 * snapshot metadata including ID, parent, timestamp, operation type, and summary
 * properties (file counts, row counts, etc.).
 *
 * <p>Usage:
 * <pre>{@code
 * List<IcebergSnapshot> snapshots = IcebergTableReader.listSnapshots(
 *     "my-catalog", "default", "my_table", config);
 * for (IcebergSnapshot snap : snapshots) {
 *     System.out.printf("Snapshot %d: %s at %d%n",
 *         snap.getSnapshotId(), snap.getOperation(), snap.getTimestampMs());
 * }
 * // List files at a specific historical snapshot
 * long historicalId = snapshots.get(0).getSnapshotId();
 * List<IcebergFileEntry> files = IcebergTableReader.listFiles(
 *     "my-catalog", "default", "my_table", config, historicalId);
 * }</pre>
 */
public class IcebergSnapshot {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final TypeReference<Map<String, String>> MAP_TYPE = new TypeReference<Map<String, String>>() {};

    private final long snapshotId;
    private final long parentSnapshotId;
    private final long sequenceNumber;
    private final long timestampMs;
    private final String manifestList;
    private final String operation;
    private final Map<String, String> summary;

    IcebergSnapshot(long snapshotId, long parentSnapshotId, long sequenceNumber,
                    long timestampMs, String manifestList, String operation,
                    Map<String, String> summary) {
        this.snapshotId = snapshotId;
        this.parentSnapshotId = parentSnapshotId;
        this.sequenceNumber = sequenceNumber;
        this.timestampMs = timestampMs;
        this.manifestList = manifestList;
        this.operation = operation;
        this.summary = summary != null
                ? Collections.unmodifiableMap(summary)
                : Collections.emptyMap();
    }

    /**
     * @return unique snapshot identifier
     */
    public long getSnapshotId() {
        return snapshotId;
    }

    /**
     * @return parent snapshot ID (-1 if this is the first snapshot)
     */
    public long getParentSnapshotId() {
        return parentSnapshotId;
    }

    /**
     * @return true if this snapshot has a parent
     */
    public boolean hasParent() {
        return parentSnapshotId >= 0;
    }

    /**
     * @return monotonically increasing sequence number
     */
    public long getSequenceNumber() {
        return sequenceNumber;
    }

    /**
     * @return timestamp in milliseconds since epoch when snapshot was created
     */
    public long getTimestampMs() {
        return timestampMs;
    }

    /**
     * @return path to the manifest list file
     */
    public String getManifestList() {
        return manifestList;
    }

    /**
     * @return operation type: "append", "overwrite", "replace", "delete"
     */
    public String getOperation() {
        return operation;
    }

    /**
     * @return snapshot summary properties (operation, file counts, row counts, etc.)
     */
    public Map<String, String> getSummary() {
        return summary;
    }

    @Override
    public String toString() {
        return String.format("IcebergSnapshot{id=%d, op='%s', timestamp=%d, seq=%d}",
                snapshotId, operation, timestampMs, sequenceNumber);
    }

    /**
     * Construct from a parsed TANT byte buffer map.
     * Package-private; used by IcebergTableReader.
     */
    static IcebergSnapshot fromMap(Map<String, Object> map) {
        long snapshotId = toLong(map.get("snapshot_id"));
        long parentSnapshotId = toLong(map.get("parent_snapshot_id"));
        long sequenceNumber = toLong(map.get("sequence_number"));
        long timestampMs = toLong(map.get("timestamp_ms"));
        String manifestList = (String) map.getOrDefault("manifest_list", "");
        String operation = (String) map.getOrDefault("operation", "unknown");

        Map<String, String> summary = parseSummary(map.get("summary"));

        return new IcebergSnapshot(snapshotId, parentSnapshotId, sequenceNumber,
                timestampMs, manifestList, operation, summary);
    }

    private static long toLong(Object value) {
        if (value instanceof Number) {
            return ((Number) value).longValue();
        }
        return -1;
    }

    private static Map<String, String> parseSummary(Object value) {
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

package io.indextables.jni.txlog;

import java.io.Serializable;
import java.util.Collections;
import java.util.Map;

/**
 * Immutable data class representing a skip action in the transaction log.
 *
 * <p>Skip actions record files that were skipped during merge or other operations,
 * with cooldown/retry metadata for the retry mechanism.
 *
 * <p>Implements {@link Serializable} for Spark broadcast/shuffle.
 */
public class TxLogSkipAction implements Serializable {

    private static final long serialVersionUID = 2L;

    private final String path;
    private final long skipTimestamp;
    private final String reason;
    private final String operation;
    private final Map<String, String> partitionValues;
    private final long size;
    private final long retryAfter;
    private final int skipCount;

    public TxLogSkipAction(String path, long skipTimestamp, String reason, String operation,
                            Map<String, String> partitionValues, long size,
                            long retryAfter, int skipCount) {
        this.path = path;
        this.skipTimestamp = skipTimestamp;
        this.reason = reason;
        this.operation = operation;
        this.partitionValues = partitionValues != null
                ? Collections.unmodifiableMap(partitionValues)
                : Collections.emptyMap();
        this.size = size;
        this.retryAfter = retryAfter;
        this.skipCount = skipCount;
    }

    /** @return path of the skipped file */
    public String getPath() { return path; }

    /** @return epoch millis when the skip was recorded */
    public long getSkipTimestamp() { return skipTimestamp; }

    /** @return reason for skipping */
    public String getReason() { return reason; }

    /** @return operation that triggered the skip (e.g., "merge", "read"), or null */
    public String getOperation() { return operation; }

    /** @return partition values for the skipped file (empty if not present) */
    public Map<String, String> getPartitionValues() { return partitionValues; }

    /** @return file size in bytes, or -1 if unknown */
    public long getSize() { return size; }

    /** @return epoch millis after which the file can be retried, or -1 if not set */
    public long getRetryAfter() { return retryAfter; }

    /** @return true if the file is currently in cooldown */
    public boolean isInCooldown() {
        return retryAfter > 0 && retryAfter > System.currentTimeMillis();
    }

    /** @return number of times this file has been skipped */
    public int getSkipCount() { return skipCount; }

    @Override
    public String toString() {
        return String.format("TxLogSkipAction{path='%s', reason='%s', skipCount=%d, retryAfter=%d}",
                path, reason, skipCount, retryAfter);
    }

    /**
     * Construct a TxLogSkipAction from a parsed TANT byte buffer map.
     *
     * @param map field name to value map from {@code BatchDocumentReader.parseToMaps()}
     * @return a new TxLogSkipAction
     */
    public static TxLogSkipAction fromMap(Map<String, Object> map) {
        String path = (String) map.get("path");
        long skipTimestamp = TxLogFileEntry.toLong(map.get("skip_timestamp"));
        String reason = (String) map.get("reason");
        String operation = (String) map.get("operation");
        Map<String, String> partitionValues = TxLogFileEntry.parseJsonMap(map.get("partition_values"));
        long size = TxLogFileEntry.toLong(map.get("size"));
        long retryAfter = TxLogFileEntry.toLong(map.get("retry_after"));
        int skipCount = TxLogFileEntry.toInt(map.get("skip_count"));
        return new TxLogSkipAction(path, skipTimestamp, reason, operation,
                partitionValues, size, retryAfter, skipCount);
    }
}

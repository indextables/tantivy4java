package io.indextables.jni.txlog;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.Serializable;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Immutable data class representing the result of a transaction log write operation.
 *
 * <p>Contains the committed version number, the number of retries needed,
 * and any conflicted versions encountered during optimistic concurrency.
 *
 * <p>Implements {@link Serializable} for Spark broadcast/shuffle.
 */
public class WriteResult implements Serializable {

    private static final long serialVersionUID = 1L;
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final long version;
    private final int retries;
    private final List<Long> conflictedVersions;

    public WriteResult(long version, int retries, List<Long> conflictedVersions) {
        this.version = version;
        this.retries = retries;
        this.conflictedVersions = conflictedVersions != null
                ? Collections.unmodifiableList(conflictedVersions)
                : Collections.emptyList();
    }

    /** @return the committed version number */
    public long getVersion() { return version; }

    /** @return the number of retries needed to commit */
    public int getRetries() { return retries; }

    /** @return list of version numbers that conflicted during write attempts */
    public List<Long> getConflictedVersions() { return conflictedVersions; }

    @Override
    public String toString() {
        return String.format("WriteResult{version=%d, retries=%d, conflicts=%d}",
                version, retries, conflictedVersions.size());
    }

    /**
     * Construct a WriteResult from a parsed TANT byte buffer map.
     *
     * @param map field name to value map from {@code BatchDocumentReader.parseToMaps()}
     * @return a new WriteResult
     */
    public static WriteResult fromMap(Map<String, Object> map) {
        long version = TxLogFileEntry.toLong(map.get("version"));
        int retries = TxLogFileEntry.toInt(map.get("retries"));
        List<Long> conflictedVersions = parseJsonLongList(map.get("conflicted_versions_json"));
        return new WriteResult(version, retries, conflictedVersions);
    }

    private static List<Long> parseJsonLongList(Object value) {
        if (value == null) return Collections.emptyList();
        String jsonStr = value.toString();
        if (jsonStr.isEmpty() || "[]".equals(jsonStr)) return Collections.emptyList();
        try {
            return MAPPER.readValue(jsonStr,
                    new TypeReference<List<Long>>() {});
        } catch (Exception e) {
            return Collections.emptyList();
        }
    }
}

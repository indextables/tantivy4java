package io.indextables.jni.txlog;

import java.io.Serializable;
import java.util.Map;

/**
 * Immutable data class representing a skip action in the transaction log.
 *
 * <p>Skip actions record files that were skipped during merge or other operations.
 *
 * <p>Implements {@link Serializable} for Spark broadcast/shuffle.
 */
public class TxLogSkipAction implements Serializable {

    private static final long serialVersionUID = 1L;

    private final String path;
    private final String reason;
    private final int skipCount;

    public TxLogSkipAction(String path, String reason, int skipCount) {
        this.path = path;
        this.reason = reason;
        this.skipCount = skipCount;
    }

    /** @return path of the skipped file */
    public String getPath() { return path; }

    /** @return reason for skipping */
    public String getReason() { return reason; }

    /** @return number of times this file has been skipped */
    public int getSkipCount() { return skipCount; }

    @Override
    public String toString() {
        return String.format("TxLogSkipAction{path='%s', reason='%s', skipCount=%d}",
                path, reason, skipCount);
    }

    /**
     * Construct a TxLogSkipAction from a parsed TANT byte buffer map.
     *
     * @param map field name to value map from {@code BatchDocumentReader.parseToMaps()}
     * @return a new TxLogSkipAction
     */
    public static TxLogSkipAction fromMap(Map<String, Object> map) {
        String path = (String) map.get("path");
        String reason = (String) map.get("reason");
        int skipCount = TxLogFileEntry.toInt(map.get("skip_count"));
        return new TxLogSkipAction(path, reason, skipCount);
    }
}

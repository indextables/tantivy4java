package io.indextables.jni.txlog;

import java.io.Serializable;
import java.util.Map;

/**
 * Immutable data class representing checkpoint metadata from the transaction log.
 *
 * <p>Returned by {@link TransactionLogWriter#createCheckpoint} and contains
 * the checkpoint version, size, format, and file count.
 *
 * <p>Implements {@link Serializable} for Spark broadcast/shuffle.
 */
public class LastCheckpointInfo implements Serializable {

    private static final long serialVersionUID = 1L;

    private final long version;
    private final long size;
    private final String format;
    private final long numFiles;

    public LastCheckpointInfo(long version, long size, String format, long numFiles) {
        this.version = version;
        this.size = size;
        this.format = format;
        this.numFiles = numFiles;
    }

    /** @return the checkpoint version number */
    public long getVersion() { return version; }

    /** @return the checkpoint size (number of actions) */
    public long getSize() { return size; }

    /** @return the checkpoint format (e.g., "avro-state") */
    public String getFormat() { return format; }

    /** @return number of active files at this checkpoint */
    public long getNumFiles() { return numFiles; }

    @Override
    public String toString() {
        return String.format("LastCheckpointInfo{version=%d, size=%d, format='%s', numFiles=%d}",
                version, size, format, numFiles);
    }

    /**
     * Construct a LastCheckpointInfo from a parsed TANT byte buffer map.
     *
     * @param map field name to value map from {@code BatchDocumentReader.parseToMaps()}
     * @return a new LastCheckpointInfo
     */
    public static LastCheckpointInfo fromMap(Map<String, Object> map) {
        long version = TxLogFileEntry.toLong(map.get("version"));
        long size = TxLogFileEntry.toLong(map.get("size"));
        String format = (String) map.get("format");
        long numFiles = TxLogFileEntry.toLong(map.get("num_files"));
        return new LastCheckpointInfo(version, size, format, numFiles);
    }
}

package io.indextables.jni.txlog;

import io.indextables.tantivy4java.batch.BatchDocumentReader;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Static utility for writing to the transaction log.
 *
 * <p>Provides write operations with automatic optimistic concurrency retry:
 * <ul>
 *   <li>{@link #addFiles} - Add file entries to the log</li>
 *   <li>{@link #removeFile} - Remove a file entry from the log</li>
 *   <li>{@link #skipFile} - Record a skip action for a file</li>
 *   <li>{@link #createCheckpoint} - Write a new checkpoint</li>
 * </ul>
 *
 * <p>Credentials and storage configuration are passed via a config map (see
 * {@link TransactionLogReader} for key documentation).
 */
public class TransactionLogWriter {

    static {
        io.indextables.tantivy4java.core.Tantivy.initialize();
    }

    private TransactionLogWriter() {
        // static utility
    }

    /**
     * Add file entries to the transaction log.
     *
     * <p>Writes a new version file containing the specified add actions.
     * Uses optimistic concurrency with automatic retry on conflict.
     *
     * @param tablePath table location (local path, file://, s3://, or azure://)
     * @param config    credential and storage configuration
     * @param addsJson  JSON array of AddAction objects to write
     * @return write result with committed version, retry count, and any conflicted versions
     * @throws IllegalArgumentException if tablePath or addsJson is null or empty
     * @throws RuntimeException         if the native call fails
     */
    public static WriteResult addFiles(String tablePath, Map<String, String> config, String addsJson) {
        if (tablePath == null || tablePath.isEmpty()) {
            throw new IllegalArgumentException("tablePath must not be null or empty");
        }
        if (addsJson == null || addsJson.isEmpty()) {
            throw new IllegalArgumentException("addsJson must not be null or empty");
        }

        byte[] bytes = nativeAddFiles(tablePath,
                config != null ? config : Collections.emptyMap(),
                addsJson);

        if (bytes == null) {
            throw new RuntimeException("Native addFiles returned null (check preceding exception)");
        }

        ByteBuffer buffer = ByteBuffer.wrap(bytes);
        buffer.order(ByteOrder.nativeOrder());

        BatchDocumentReader reader = new BatchDocumentReader();
        List<Map<String, Object>> maps = reader.parseToMaps(buffer);

        if (maps.isEmpty()) {
            throw new RuntimeException("Empty write result response from native layer");
        }

        return WriteResult.fromMap(maps.get(0));
    }

    /**
     * Remove a file entry from the transaction log.
     *
     * <p>Writes a new version file containing a remove action for the specified path.
     * Uses optimistic concurrency with automatic retry on conflict.
     *
     * @param tablePath table location
     * @param config    credential and storage configuration
     * @param path      file path to remove (relative to table root)
     * @return the committed version number
     * @throws IllegalArgumentException if tablePath or path is null or empty
     * @throws RuntimeException         if the native call fails
     */
    public static long removeFile(String tablePath, Map<String, String> config, String path) {
        if (tablePath == null || tablePath.isEmpty()) {
            throw new IllegalArgumentException("tablePath must not be null or empty");
        }
        if (path == null || path.isEmpty()) {
            throw new IllegalArgumentException("path must not be null or empty");
        }

        long version = nativeRemoveFile(tablePath,
                config != null ? config : Collections.emptyMap(),
                path);

        if (version < 0) {
            throw new RuntimeException("Native removeFile returned error (check preceding exception)");
        }
        return version;
    }

    /**
     * Record a skip action in the transaction log.
     *
     * <p>Writes a new version file containing a skip action. Skip actions record
     * files that were skipped during merge or other operations.
     *
     * @param tablePath table location
     * @param config    credential and storage configuration
     * @param skipJson  JSON string representing a SkipAction
     * @return the committed version number
     * @throws IllegalArgumentException if tablePath or skipJson is null or empty
     * @throws RuntimeException         if the native call fails
     */
    public static long skipFile(String tablePath, Map<String, String> config, String skipJson) {
        if (tablePath == null || tablePath.isEmpty()) {
            throw new IllegalArgumentException("tablePath must not be null or empty");
        }
        if (skipJson == null || skipJson.isEmpty()) {
            throw new IllegalArgumentException("skipJson must not be null or empty");
        }

        long version = nativeSkipFile(tablePath,
                config != null ? config : Collections.emptyMap(),
                skipJson);

        if (version < 0) {
            throw new RuntimeException("Native skipFile returned error (check preceding exception)");
        }
        return version;
    }

    /**
     * Create a new checkpoint in the transaction log.
     *
     * <p>Writes a checkpoint containing the full set of active file entries,
     * metadata, and protocol information.
     *
     * @param tablePath    table location
     * @param config       credential and storage configuration
     * @param entriesJson  JSON array of AddAction objects representing all active files
     * @param metadataJson JSON string representing the MetadataAction
     * @param protocolJson JSON string representing the ProtocolAction
     * @return checkpoint info with version, size, and format
     * @throws IllegalArgumentException if required parameters are null or empty
     * @throws RuntimeException         if the native call fails
     */
    public static LastCheckpointInfo createCheckpoint(String tablePath, Map<String, String> config,
                                                       String entriesJson, String metadataJson,
                                                       String protocolJson) {
        if (tablePath == null || tablePath.isEmpty()) {
            throw new IllegalArgumentException("tablePath must not be null or empty");
        }
        if (entriesJson == null || entriesJson.isEmpty()) {
            throw new IllegalArgumentException("entriesJson must not be null or empty");
        }
        if (metadataJson == null || metadataJson.isEmpty()) {
            throw new IllegalArgumentException("metadataJson must not be null or empty");
        }
        if (protocolJson == null || protocolJson.isEmpty()) {
            throw new IllegalArgumentException("protocolJson must not be null or empty");
        }

        byte[] bytes = nativeCreateCheckpoint(tablePath,
                config != null ? config : Collections.emptyMap(),
                entriesJson, metadataJson, protocolJson);

        if (bytes == null) {
            throw new RuntimeException("Native createCheckpoint returned null (check preceding exception)");
        }

        ByteBuffer buffer = ByteBuffer.wrap(bytes);
        buffer.order(ByteOrder.nativeOrder());

        BatchDocumentReader reader = new BatchDocumentReader();
        List<Map<String, Object>> maps = reader.parseToMaps(buffer);

        if (maps.isEmpty()) {
            throw new RuntimeException("Empty checkpoint result response from native layer");
        }

        return LastCheckpointInfo.fromMap(maps.get(0));
    }

    /**
     * Write a version file with arbitrary mixed actions and automatic retry.
     *
     * <p>The actionsJson is standard Delta-compatible JSON-lines format:
     * <pre>
     * {"protocol":{"minReaderVersion":4,"minWriterVersion":4}}
     * {"metaData":{"id":"abc","schemaString":"{...}","partitionColumns":[],"configuration":{}}}
     * {"add":{"path":"file.split","size":1000,"partitionValues":{},"modificationTime":0,"dataChange":true}}
     * </pre>
     *
     * @param tablePath   table location
     * @param config      credential and storage configuration
     * @param actionsJson JSON-lines with one action per line
     * @return WriteResult with version, retries, and conflicted versions
     */
    public static WriteResult writeVersion(String tablePath, Map<String, String> config,
                                            String actionsJson) {
        if (tablePath == null || tablePath.isEmpty()) {
            throw new IllegalArgumentException("tablePath must not be null or empty");
        }
        byte[] bytes = nativeWriteVersion(tablePath,
                config != null ? config : Collections.emptyMap(), actionsJson);
        if (bytes == null) {
            throw new RuntimeException("Native writeVersion returned null (check preceding exception)");
        }
        ByteBuffer buffer = ByteBuffer.wrap(bytes);
        buffer.order(ByteOrder.nativeOrder());
        BatchDocumentReader reader = new BatchDocumentReader();
        List<Map<String, Object>> maps = reader.parseToMaps(buffer);
        if (maps.isEmpty()) {
            throw new RuntimeException("Empty writeVersion response");
        }
        return WriteResult.fromMap(maps.get(0));
    }

    /**
     * Write a version file with a single attempt (no retry).
     * Returns WriteResult with version=-1 on conflict.
     *
     * @param tablePath   table location
     * @param config      credential and storage configuration
     * @param actionsJson JSON-lines with one action per line
     * @return WriteResult (version=-1 if conflict)
     */
    public static WriteResult writeVersionOnce(String tablePath, Map<String, String> config,
                                                String actionsJson) {
        if (tablePath == null || tablePath.isEmpty()) {
            throw new IllegalArgumentException("tablePath must not be null or empty");
        }
        byte[] bytes = nativeWriteVersionOnce(tablePath,
                config != null ? config : Collections.emptyMap(), actionsJson);
        if (bytes == null) {
            throw new RuntimeException("Native writeVersionOnce returned null (check preceding exception)");
        }
        ByteBuffer buffer = ByteBuffer.wrap(bytes);
        buffer.order(ByteOrder.nativeOrder());
        BatchDocumentReader reader = new BatchDocumentReader();
        List<Map<String, Object>> maps = reader.parseToMaps(buffer);
        if (maps.isEmpty()) {
            throw new RuntimeException("Empty writeVersionOnce response");
        }
        return WriteResult.fromMap(maps.get(0));
    }

    /**
     * Initialize a new table by writing version 0 with Protocol + Metadata.
     * Fails if version 0 already exists.
     *
     * @param tablePath    table location
     * @param config       credential and storage configuration
     * @param protocolJson JSON representation of ProtocolAction
     * @param metadataJson JSON representation of MetadataAction
     */
    public static void initializeTable(String tablePath, Map<String, String> config,
                                        String protocolJson, String metadataJson) {
        if (tablePath == null || tablePath.isEmpty()) {
            throw new IllegalArgumentException("tablePath must not be null or empty");
        }
        nativeInitializeTable(tablePath,
                config != null ? config : Collections.emptyMap(),
                protocolJson, metadataJson);
    }

    // --- Native methods ---

    private static native byte[] nativeAddFiles(String tablePath, Map<String, String> config, String addsJson);
    private static native long nativeRemoveFile(String tablePath, Map<String, String> config, String path);
    private static native long nativeSkipFile(String tablePath, Map<String, String> config, String skipJson);
    private static native byte[] nativeCreateCheckpoint(String tablePath, Map<String, String> config, String entriesJson, String metadataJson, String protocolJson);
    private static native byte[] nativeWriteVersion(String tablePath, Map<String, String> config, String actionsJson);
    private static native byte[] nativeWriteVersionOnce(String tablePath, Map<String, String> config, String actionsJson);
    private static native void nativeInitializeTable(String tablePath, Map<String, String> config, String protocolJson, String metadataJson);
}

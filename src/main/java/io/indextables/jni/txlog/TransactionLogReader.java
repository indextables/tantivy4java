package io.indextables.jni.txlog;

import io.indextables.tantivy4java.batch.BatchDocumentReader;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Static utility for reading from the transaction log.
 *
 * <p>Provides stateless read primitives for distributed scanning:
 * <ul>
 *   <li>{@link #getSnapshotInfo} - Driver-side: get checkpoint metadata and manifest paths</li>
 *   <li>{@link #readManifest} - Executor-side: read one manifest file and return file entries</li>
 *   <li>{@link #readPostCheckpointChanges} - Driver-side: read incremental changes after checkpoint</li>
 *   <li>{@link #getCurrentVersion} - Lightweight version probe</li>
 * </ul>
 *
 * <p>Credentials and storage configuration are passed via a config map with keys:
 * <ul>
 *   <li>{@code aws_access_key_id}, {@code aws_secret_access_key}, {@code aws_session_token},
 *       {@code aws_region}, {@code aws_endpoint}, {@code aws_force_path_style}</li>
 *   <li>{@code azure_account_name}, {@code azure_access_key}, {@code azure_bearer_token}</li>
 * </ul>
 */
public class TransactionLogReader {

    static {
        io.indextables.tantivy4java.core.Tantivy.initialize();
    }

    private TransactionLogReader() {
        // static utility
    }

    /**
     * Get lightweight snapshot metadata for distributed scanning.
     *
     * <p>Reads the last checkpoint and lists post-checkpoint version files.
     * Does NOT read manifest contents. Returns paths that can be distributed
     * to executors via {@link #readManifest}.
     *
     * @param tablePath table location (local path, file://, s3://, or azure://)
     * @param config    credential and storage configuration
     * @return snapshot metadata with manifest paths and post-checkpoint version paths
     * @throws IllegalArgumentException if tablePath is null or empty
     * @throws RuntimeException         if the native call fails
     */
    public static TxLogSnapshotInfo getSnapshotInfo(String tablePath, Map<String, String> config) {
        if (tablePath == null || tablePath.isEmpty()) {
            throw new IllegalArgumentException("tablePath must not be null or empty");
        }

        byte[] bytes = nativeGetSnapshotInfo(tablePath,
                config != null ? config : Collections.emptyMap());

        if (bytes == null) {
            throw new RuntimeException("Native getSnapshotInfo returned null (check preceding exception)");
        }

        ByteBuffer buffer = ByteBuffer.wrap(bytes);
        buffer.order(ByteOrder.nativeOrder());

        BatchDocumentReader reader = new BatchDocumentReader();
        List<Map<String, Object>> maps = reader.parseToMaps(buffer);

        if (maps.isEmpty()) {
            throw new RuntimeException("Empty snapshot info response from native layer");
        }

        return TxLogSnapshotInfo.fromMap(maps.get(0));
    }

    /**
     * Read one manifest file and return file entries.
     *
     * <p>Designed for executor-side use: reads a single manifest and returns
     * the file entries it contains.
     *
     * @param tablePath          table location
     * @param config             credential and storage configuration
     * @param stateDir           state directory from {@link TxLogSnapshotInfo#getStateDir()}
     * @param manifestPath       manifest file path from {@link TxLogSnapshotInfo#getManifestPaths()}
     * @param metadataConfigJson optional JSON string with metadata configuration (may be null)
     * @return list of file entries from this manifest
     * @throws IllegalArgumentException if tablePath or manifestPath is null or empty
     * @throws RuntimeException         if the native call fails
     */
    public static List<TxLogFileEntry> readManifest(String tablePath, Map<String, String> config,
                                                     String stateDir, String manifestPath,
                                                     String metadataConfigJson) {
        if (tablePath == null || tablePath.isEmpty()) {
            throw new IllegalArgumentException("tablePath must not be null or empty");
        }
        if (manifestPath == null || manifestPath.isEmpty()) {
            throw new IllegalArgumentException("manifestPath must not be null or empty");
        }

        byte[] bytes = nativeReadManifest(tablePath,
                config != null ? config : Collections.emptyMap(),
                stateDir != null ? stateDir : "",
                manifestPath,
                metadataConfigJson);

        if (bytes == null) {
            throw new RuntimeException("Native readManifest returned null (check preceding exception)");
        }

        ByteBuffer buffer = ByteBuffer.wrap(bytes);
        buffer.order(ByteOrder.nativeOrder());

        BatchDocumentReader reader = new BatchDocumentReader();
        List<Map<String, Object>> maps = reader.parseToMaps(buffer);

        List<TxLogFileEntry> entries = new ArrayList<>(maps.size());
        for (Map<String, Object> map : maps) {
            entries.add(TxLogFileEntry.fromMap(map));
        }
        return entries;
    }

    /**
     * Read post-checkpoint changes from version files.
     *
     * <p>Reads version files after the checkpoint and returns added files,
     * removed paths, and skip actions. Designed for driver-side use.
     *
     * @param tablePath          table location
     * @param config             credential and storage configuration
     * @param versionPathsJson   JSON array of version file paths (from
     *                           {@link TxLogSnapshotInfo#getPostCheckpointPaths()})
     * @param metadataConfigJson optional JSON string with metadata configuration (may be null)
     * @return changes with added files, removed paths, and skip actions
     * @throws IllegalArgumentException if tablePath is null or empty
     * @throws RuntimeException         if the native call fails
     */
    public static TxLogChanges readPostCheckpointChanges(String tablePath, Map<String, String> config,
                                                          String versionPathsJson,
                                                          String metadataConfigJson) {
        if (tablePath == null || tablePath.isEmpty()) {
            throw new IllegalArgumentException("tablePath must not be null or empty");
        }

        byte[] bytes = nativeReadPostCheckpointChanges(tablePath,
                config != null ? config : Collections.emptyMap(),
                versionPathsJson,
                metadataConfigJson);

        if (bytes == null) {
            throw new RuntimeException("Native readPostCheckpointChanges returned null (check preceding exception)");
        }

        ByteBuffer buffer = ByteBuffer.wrap(bytes);
        buffer.order(ByteOrder.nativeOrder());

        BatchDocumentReader reader = new BatchDocumentReader();
        List<Map<String, Object>> maps = reader.parseToMaps(buffer);

        return TxLogChanges.fromMaps(maps);
    }

    /**
     * Return the current (latest committed) version of the transaction log.
     *
     * <p>Lightweight probe that reads only the last checkpoint and probes
     * for post-checkpoint version files.
     *
     * @param tablePath table location (local path, file://, s3://, or azure://)
     * @param config    credential and storage configuration
     * @return current version number
     * @throws IllegalArgumentException if tablePath is null or empty
     * @throws RuntimeException         if the native call fails or returns an error
     */
    public static long getCurrentVersion(String tablePath, Map<String, String> config) {
        if (tablePath == null || tablePath.isEmpty()) {
            throw new IllegalArgumentException("tablePath must not be null or empty");
        }
        long version = nativeGetCurrentVersion(tablePath,
                config != null ? config : Collections.emptyMap());
        if (version < 0) {
            throw new RuntimeException("Native getCurrentVersion returned error (check preceding exception)");
        }
        return version;
    }

    /**
     * List all version numbers present in the transaction log.
     *
     * @param tablePath table location
     * @param config    credential and storage configuration
     * @return sorted array of version numbers
     */
    public static long[] listVersions(String tablePath, Map<String, String> config) {
        if (tablePath == null || tablePath.isEmpty()) {
            throw new IllegalArgumentException("tablePath must not be null or empty");
        }
        String json = nativeListVersions(tablePath,
                config != null ? config : Collections.emptyMap());
        if (json == null) {
            throw new RuntimeException("Native listVersions returned null (check preceding exception)");
        }
        try {
            com.fasterxml.jackson.databind.ObjectMapper mapper = new com.fasterxml.jackson.databind.ObjectMapper();
            long[] versions = mapper.readValue(json, long[].class);
            return versions;
        } catch (Exception e) {
            throw new RuntimeException("Failed to parse listVersions response: " + e.getMessage(), e);
        }
    }

    /**
     * Read the raw JSON-lines content from a specific version file.
     *
     * @param tablePath table location
     * @param config    credential and storage configuration
     * @param version   version number to read
     * @return raw JSON-lines content of the version file
     */
    public static String readVersion(String tablePath, Map<String, String> config, long version) {
        if (tablePath == null || tablePath.isEmpty()) {
            throw new IllegalArgumentException("tablePath must not be null or empty");
        }
        String content = nativeReadVersion(tablePath,
                config != null ? config : Collections.emptyMap(), version);
        if (content == null) {
            throw new RuntimeException("Native readVersion returned null (check preceding exception)");
        }
        return content;
    }

    // ========================================================================
    // Purge Primitives
    // ========================================================================

    /**
     * List versions within the retention window (non-expired).
     *
     * @param tablePath    table location
     * @param config       credential and storage configuration
     * @param retentionMs  retention duration in milliseconds (0 = only keep latest checkpoint)
     * @return sorted array of retained version numbers
     */
    public static long[] listRetainedVersions(String tablePath, Map<String, String> config,
                                               long retentionMs) {
        if (tablePath == null || tablePath.isEmpty()) {
            throw new IllegalArgumentException("tablePath must not be null or empty");
        }
        String json = nativeListRetainedVersions(tablePath,
                config != null ? config : Collections.emptyMap(), retentionMs);
        if (json == null) {
            throw new RuntimeException("Native listRetainedVersions returned null");
        }
        try {
            return new com.fasterxml.jackson.databind.ObjectMapper().readValue(json, long[].class);
        } catch (Exception e) {
            throw new RuntimeException("Failed to parse listRetainedVersions: " + e.getMessage(), e);
        }
    }

    /**
     * Open a cursor over all file paths referenced by non-expired versions.
     * Results are streamed as TANT byte buffer batches via {@link #readNextRetainedFilesBatch}.
     *
     * @param tablePath    table location
     * @param config       credential and storage configuration
     * @param retentionMs  retention duration in milliseconds
     * @return cursor handle for streaming reads
     */
    public static long openRetainedFilesCursor(String tablePath, Map<String, String> config,
                                                long retentionMs) {
        if (tablePath == null || tablePath.isEmpty()) {
            throw new IllegalArgumentException("tablePath must not be null or empty");
        }
        long handle = nativeOpenRetainedFilesCursor(tablePath,
                config != null ? config : Collections.emptyMap(), retentionMs);
        if (handle < 0) {
            throw new RuntimeException("Failed to open retained files cursor");
        }
        return handle;
    }

    /**
     * Read the next batch of retained files from the cursor.
     * Each entry has fields: path (String), size (long), version (long).
     *
     * @param cursorHandle handle from {@link #openRetainedFilesCursor}
     * @param batchSize    max rows per batch
     * @return list of maps with path/size/version, or null when exhausted
     */
    public static List<Map<String, Object>> readNextRetainedFilesBatch(long cursorHandle,
                                                                        int batchSize) {
        byte[] bytes = nativeReadNextRetainedFilesBatch(cursorHandle, batchSize);
        if (bytes == null) {
            return null; // Cursor exhausted
        }
        java.nio.ByteBuffer buffer = java.nio.ByteBuffer.wrap(bytes);
        buffer.order(java.nio.ByteOrder.nativeOrder());
        io.indextables.tantivy4java.batch.BatchDocumentReader reader =
                new io.indextables.tantivy4java.batch.BatchDocumentReader();
        return reader.parseToMaps(buffer);
    }

    /**
     * Close a retained files cursor and release resources.
     *
     * @param cursorHandle handle from {@link #openRetainedFilesCursor}
     */
    public static void closeRetainedFilesCursor(long cursorHandle) {
        nativeCloseRetainedFilesCursor(cursorHandle);
    }

    /**
     * List skip actions from recent version files within a time window.
     * Scans version files backward from latest, stopping when actions are
     * older than maxAgeMs.
     *
     * @param tablePath  table location
     * @param config     credential and storage configuration
     * @param maxAgeMs   maximum age in milliseconds (0 = all skip actions)
     * @return list of TxLogSkipAction
     */
    public static List<TxLogSkipAction> listSkipActions(String tablePath, Map<String, String> config,
                                                         long maxAgeMs) {
        if (tablePath == null || tablePath.isEmpty()) {
            throw new IllegalArgumentException("tablePath must not be null or empty");
        }
        byte[] bytes = nativeListSkipActions(tablePath,
                config != null ? config : Collections.emptyMap(), maxAgeMs);
        if (bytes == null) {
            return Collections.emptyList();
        }
        java.nio.ByteBuffer buffer = java.nio.ByteBuffer.wrap(bytes);
        buffer.order(java.nio.ByteOrder.nativeOrder());
        io.indextables.tantivy4java.batch.BatchDocumentReader reader =
                new io.indextables.tantivy4java.batch.BatchDocumentReader();
        List<Map<String, Object>> maps = reader.parseToMaps(buffer);
        List<TxLogSkipAction> result = new ArrayList<>();
        for (Map<String, Object> map : maps) {
            result.add(TxLogSkipAction.fromMap(map));
        }
        return result;
    }

    // ========================================================================
    // Cache Management
    // ========================================================================

    /**
     * Explicitly invalidate all cached data for a table path.
     * Call this after purge/truncate operations to ensure subsequent reads
     * get fresh data from storage.
     *
     * <p>Handles path normalization internally (file:// vs file:///).
     *
     * @param tablePath table location
     */
    public static void invalidateCache(String tablePath) {
        if (tablePath != null && !tablePath.isEmpty()) {
            nativeInvalidateCache(tablePath);
        }
    }

    // --- Native methods ---

    private static native byte[] nativeGetSnapshotInfo(String tablePath, Map<String, String> config);
    private static native byte[] nativeReadManifest(String tablePath, Map<String, String> config, String stateDir, String manifestPath, String metadataConfigJson);
    private static native byte[] nativeReadPostCheckpointChanges(String tablePath, Map<String, String> config, String versionPathsJson, String metadataConfigJson);
    private static native long nativeGetCurrentVersion(String tablePath, Map<String, String> config);
    private static native String nativeListVersions(String tablePath, Map<String, String> config);
    private static native String nativeReadVersion(String tablePath, Map<String, String> config, long version);
    private static native String nativeListRetainedVersions(String tablePath, Map<String, String> config, long retentionMs);
    private static native long nativeOpenRetainedFilesCursor(String tablePath, Map<String, String> config, long retentionMs);
    private static native byte[] nativeReadNextRetainedFilesBatch(long cursorHandle, int batchSize);
    private static native void nativeCloseRetainedFilesCursor(long cursorHandle);
    private static native byte[] nativeListSkipActions(String tablePath, Map<String, String> config, long maxAgeMs);
    private static native void nativeInvalidateCache(String tablePath);

    // FR1: List files with partition/data skipping, return via Arrow FFI
    private static native String nativeListFilesArrowFfi(
        String tablePath, Map<String, String> config,
        String partitionFilterJson, String dataFilterJson,
        String fieldTypesJson,
        int excludeCooldown, int includeStats,
        long[] arrayAddrs, long[] schemaAddrs);

    /**
     * List files with all filtering applied natively, exported via Arrow FFI.
     * Single-call replacement for getSnapshotInfo + readManifest + partition pruning + data skipping.
     *
     * @param tablePath           table location (s3://, file://, azure://)
     * @param config              storage credentials + cache config
     * @param partitionFilterJson partition filter as JSON (null = no partition filtering)
     * @param dataFilterJson      data filter for min/max skipping as JSON (null = no data skipping)
     * @param excludeCooldown     if true, exclude files in cooldown state
     * @param includeStats        if true, include min_values/max_values columns in output
     * @param arrayAddrs          pre-allocated Arrow C ArrowArray struct addresses
     * @param schemaAddrs         pre-allocated Arrow C ArrowSchema struct addresses
     * @return JSON with numRows, partitionColumns, protocolJson, metrics
     */
    public static String listFilesArrowFfi(String tablePath, Map<String, String> config,
                                           String partitionFilterJson, String dataFilterJson,
                                           String fieldTypesJson,
                                           boolean excludeCooldown, boolean includeStats,
                                           long[] arrayAddrs, long[] schemaAddrs) {
        if (tablePath == null || tablePath.isEmpty()) {
            throw new IllegalArgumentException("tablePath must not be null or empty");
        }
        if (arrayAddrs == null || schemaAddrs == null) {
            throw new IllegalArgumentException("arrayAddrs and schemaAddrs must not be null");
        }
        return nativeListFilesArrowFfi(tablePath,
                config != null ? config : java.util.Collections.emptyMap(),
                partitionFilterJson, dataFilterJson,
                fieldTypesJson,
                excludeCooldown ? 1 : 0, includeStats ? 1 : 0,
                arrayAddrs, schemaAddrs);
    }

    // FR3: Read next batch of retained files via Arrow FFI
    private static native int nativeReadNextRetainedFilesBatchArrowFfi(
        long cursorHandle, int batchSize,
        long[] arrayAddrs, long[] schemaAddrs);

    /**
     * Fetch the next batch of retained files from cursor, exported via Arrow FFI.
     *
     * @param cursorHandle opaque cursor from openRetainedFilesCursor()
     * @param batchSize    maximum rows per batch
     * @param arrayAddrs   pre-allocated Arrow C ArrowArray struct addresses
     * @param schemaAddrs  pre-allocated Arrow C ArrowSchema struct addresses
     * @return number of rows (0 = exhausted, -1 = error)
     */
    public static int readNextRetainedFilesBatchArrowFfi(long cursorHandle, int batchSize,
                                                          long[] arrayAddrs, long[] schemaAddrs) {
        if (arrayAddrs == null || schemaAddrs == null) {
            throw new IllegalArgumentException("arrayAddrs and schemaAddrs must not be null");
        }
        return nativeReadNextRetainedFilesBatchArrowFfi(cursorHandle, batchSize, arrayAddrs, schemaAddrs);
    }

    // Test helper: list files with Rust-side FFI allocation, returns JSON summary
    static native String nativeTestListFilesRoundtrip(
        String tablePath, Map<String, String> config,
        String partitionFilterJson, String dataFilterJson,
        int excludeCooldown);
}

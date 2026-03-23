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

    // --- Native methods ---

    private static native byte[] nativeGetSnapshotInfo(String tablePath, Map<String, String> config);
    private static native byte[] nativeReadManifest(String tablePath, Map<String, String> config, String stateDir, String manifestPath, String metadataConfigJson);
    private static native byte[] nativeReadPostCheckpointChanges(String tablePath, Map<String, String> config, String versionPathsJson, String metadataConfigJson);
    private static native long nativeGetCurrentVersion(String tablePath, Map<String, String> config);
    private static native String nativeListVersions(String tablePath, Map<String, String> config);
    private static native String nativeReadVersion(String tablePath, Map<String, String> config, long version);
}

package io.indextables.jni.txlog;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.Serializable;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Immutable data class representing distributed snapshot info from the transaction log.
 *
 * <p>Returned by {@link TransactionLogReader#getSnapshotInfo(String, Map)} and contains
 * the checkpoint version, state directory, manifest paths for executor-side reads,
 * and post-checkpoint version paths for driver-side replay.
 *
 * <p>Implements {@link Serializable} for Spark broadcast/shuffle.
 */
public class TxLogSnapshotInfo implements Serializable {

    private static final long serialVersionUID = 1L;
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final long checkpointVersion;
    private final String stateDir;
    private final List<String> manifestPaths;
    private final List<String> postCheckpointPaths;
    private final String protocolJson;
    private final String metadataJson;
    private final long numManifests;

    public TxLogSnapshotInfo(long checkpointVersion, String stateDir,
                             List<String> manifestPaths, List<String> postCheckpointPaths,
                             String protocolJson, String metadataJson, long numManifests) {
        this.checkpointVersion = checkpointVersion;
        this.stateDir = stateDir;
        this.manifestPaths = manifestPaths != null
                ? Collections.unmodifiableList(manifestPaths)
                : Collections.emptyList();
        this.postCheckpointPaths = postCheckpointPaths != null
                ? Collections.unmodifiableList(postCheckpointPaths)
                : Collections.emptyList();
        this.protocolJson = protocolJson;
        this.metadataJson = metadataJson;
        this.numManifests = numManifests;
    }

    /** @return checkpoint version number */
    public long getCheckpointVersion() { return checkpointVersion; }

    /** @return state directory path (relative to _txlog/) */
    public String getStateDir() { return stateDir; }

    /** @return list of manifest file paths for executor-side reads */
    public List<String> getManifestPaths() { return manifestPaths; }

    /** @return list of post-checkpoint version file paths for driver-side replay */
    public List<String> getPostCheckpointPaths() { return postCheckpointPaths; }

    /** @return protocol action as JSON string */
    public String getProtocolJson() { return protocolJson; }

    /** @return metadata action as JSON string */
    public String getMetadataJson() { return metadataJson; }

    /** @return number of manifest files */
    public long getNumManifests() { return numManifests; }

    @Override
    public String toString() {
        return String.format("TxLogSnapshotInfo{checkpointVersion=%d, stateDir='%s', manifests=%d, postCheckpoint=%d}",
                checkpointVersion, stateDir, manifestPaths.size(), postCheckpointPaths.size());
    }

    /**
     * Construct a TxLogSnapshotInfo from a parsed TANT byte buffer map.
     *
     * @param map field name to value map from {@code BatchDocumentReader.parseToMaps()}
     * @return a new TxLogSnapshotInfo
     */
    public static TxLogSnapshotInfo fromMap(Map<String, Object> map) {
        long checkpointVersion = TxLogFileEntry.toLong(map.get("checkpoint_version"));
        String stateDir = (String) map.get("state_dir");
        List<String> manifestPaths = parseJsonList(map.get("manifest_paths_json"));
        List<String> postCheckpointPaths = parseJsonList(map.get("post_checkpoint_paths_json"));
        String protocolJson = asString(map.get("protocol_json"));
        String metadataJson = asString(map.get("metadata_json"));
        long numManifests = TxLogFileEntry.toLong(map.get("num_manifests"));

        return new TxLogSnapshotInfo(checkpointVersion, stateDir,
                manifestPaths, postCheckpointPaths,
                protocolJson, metadataJson, numManifests);
    }

    private static String asString(Object value) {
        return value != null ? value.toString() : null;
    }

    private static List<String> parseJsonList(Object value) {
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

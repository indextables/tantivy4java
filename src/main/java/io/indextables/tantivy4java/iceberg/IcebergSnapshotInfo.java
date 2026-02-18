package io.indextables.tantivy4java.iceberg;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Lightweight snapshot metadata for distributed Iceberg table scanning.
 *
 * <p>Contains manifest file paths — does NOT read manifest contents.
 * Use {@link IcebergTableReader#readManifestFile} to read individual
 * manifests on executors.
 *
 * <p>Implements {@link Serializable} for Spark broadcast/shuffle.
 */
public class IcebergSnapshotInfo implements Serializable {

    private static final long serialVersionUID = 1L;

    private final long snapshotId;
    private final String schemaJson;
    private final String partitionSpecJson;
    private final List<ManifestFileInfo> manifestFiles;

    IcebergSnapshotInfo(long snapshotId, String schemaJson, String partitionSpecJson,
                        List<ManifestFileInfo> manifestFiles) {
        this.snapshotId = snapshotId;
        this.schemaJson = schemaJson;
        this.partitionSpecJson = partitionSpecJson;
        this.manifestFiles = manifestFiles != null
                ? Collections.unmodifiableList(manifestFiles)
                : Collections.emptyList();
    }

    /** Resolved snapshot ID. */
    public long getSnapshotId() { return snapshotId; }

    /** Full Iceberg schema as JSON. */
    public String getSchemaJson() { return schemaJson; }

    /** Default partition spec as JSON. */
    public String getPartitionSpecJson() { return partitionSpecJson; }

    /** Manifest file metadata entries. */
    public List<ManifestFileInfo> getManifestFiles() { return manifestFiles; }

    /** Convenience: get manifest file paths for distribution to executors. */
    public List<String> getManifestFilePaths() {
        List<String> paths = new ArrayList<>(manifestFiles.size());
        for (ManifestFileInfo mf : manifestFiles) {
            paths.add(mf.getManifestPath());
        }
        return paths;
    }

    @Override
    public String toString() {
        return String.format("IcebergSnapshotInfo{snapshotId=%d, manifests=%d}",
                snapshotId, manifestFiles.size());
    }

    /**
     * Construct from parsed TANT byte buffer maps.
     *
     * <p>Map 0 is the header: snapshot_id, schema_json, partition_spec_json, manifest_count.
     * Maps 1..N are manifest entries.
     */
    static IcebergSnapshotInfo fromMaps(List<Map<String, Object>> maps) {
        if (maps.isEmpty()) {
            throw new RuntimeException("Empty snapshot info response");
        }

        Map<String, Object> header = maps.get(0);
        long snapshotId = toLong(header.get("snapshot_id"));
        String schemaJson = (String) header.get("schema_json");
        String partitionSpecJson = (String) header.get("partition_spec_json");

        List<ManifestFileInfo> manifests = new ArrayList<>(maps.size() - 1);
        for (int i = 1; i < maps.size(); i++) {
            manifests.add(ManifestFileInfo.fromMap(maps.get(i)));
        }

        return new IcebergSnapshotInfo(snapshotId, schemaJson, partitionSpecJson, manifests);
    }

    private static long toLong(Object value) {
        if (value instanceof Number) return ((Number) value).longValue();
        return -1;
    }

    /**
     * Metadata about a single manifest file from the manifest list.
     */
    public static class ManifestFileInfo implements Serializable {

        private static final long serialVersionUID = 1L;

        private final String manifestPath;
        private final long manifestLength;
        private final long addedSnapshotId;
        private final long addedFilesCount;
        private final long existingFilesCount;
        private final long deletedFilesCount;
        private final int partitionSpecId;

        ManifestFileInfo(String manifestPath, long manifestLength, long addedSnapshotId,
                         long addedFilesCount, long existingFilesCount, long deletedFilesCount,
                         int partitionSpecId) {
            this.manifestPath = manifestPath;
            this.manifestLength = manifestLength;
            this.addedSnapshotId = addedSnapshotId;
            this.addedFilesCount = addedFilesCount;
            this.existingFilesCount = existingFilesCount;
            this.deletedFilesCount = deletedFilesCount;
            this.partitionSpecId = partitionSpecId;
        }

        /** Full path to the manifest avro file. */
        public String getManifestPath() { return manifestPath; }

        /** File size in bytes. */
        public long getManifestLength() { return manifestLength; }

        /** Snapshot that added this manifest. */
        public long getAddedSnapshotId() { return addedSnapshotId; }

        /** Number of files with Added status. */
        public long getAddedFilesCount() { return addedFilesCount; }

        /** Number of files with Existing status. */
        public long getExistingFilesCount() { return existingFilesCount; }

        /** Number of files with Deleted status. */
        public long getDeletedFilesCount() { return deletedFilesCount; }

        /** Partition spec ID for this manifest. */
        public int getPartitionSpecId() { return partitionSpecId; }

        @Override
        public String toString() {
            return String.format("ManifestFileInfo{path='%s', added=%d, existing=%d, deleted=%d}",
                    manifestPath, addedFilesCount, existingFilesCount, deletedFilesCount);
        }

        static ManifestFileInfo fromMap(Map<String, Object> map) {
            return new ManifestFileInfo(
                    (String) map.get("manifest_path"),
                    toLong(map.get("manifest_length")),
                    toLong(map.get("added_snapshot_id")),
                    toLong(map.get("added_files_count")),
                    toLong(map.get("existing_files_count")),
                    toLong(map.get("deleted_files_count")),
                    (int) toLong(map.get("partition_spec_id"))
            );
        }

        private static long toLong(Object value) {
            if (value instanceof Number) return ((Number) value).longValue();
            return 0;
        }
    }
}

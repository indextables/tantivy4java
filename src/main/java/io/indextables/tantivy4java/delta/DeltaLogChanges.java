package io.indextables.tantivy4java.delta;

import java.io.Serializable;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.logging.Logger;

/**
 * Post-checkpoint log changes for distributed Delta table scanning.
 *
 * <p>Contains files added and paths removed by JSON commits after the checkpoint.
 * Used to reconcile checkpoint-based file listings on the driver side.
 *
 * <p>Implements {@link Serializable} for Spark broadcast.
 */
public class DeltaLogChanges implements Serializable {

    private static final long serialVersionUID = 1L;
    private static final transient Logger LOG = Logger.getLogger(DeltaLogChanges.class.getName());

    private final List<DeltaFileEntry> addedFiles;
    private final Set<String> removedPaths;

    DeltaLogChanges(List<DeltaFileEntry> addedFiles, Set<String> removedPaths) {
        this.addedFiles = addedFiles != null
                ? Collections.unmodifiableList(addedFiles)
                : Collections.emptyList();
        this.removedPaths = removedPaths != null
                ? Collections.unmodifiableSet(removedPaths)
                : Collections.emptySet();
    }

    /** Files added by post-checkpoint commits. */
    public List<DeltaFileEntry> getAddedFiles() { return addedFiles; }

    /** File paths removed by post-checkpoint commits. */
    public Set<String> getRemovedPaths() { return removedPaths; }

    @Override
    public String toString() {
        return String.format("DeltaLogChanges{added=%d, removed=%d}",
                addedFiles.size(), removedPaths.size());
    }

    /**
     * Construct from parsed TANT byte buffer maps.
     *
     * <p>The first map is the header with num_added and num_removed counts.
     * Maps 1..num_added are added file entries. Maps num_added+1..end are removed paths.
     */
    static DeltaLogChanges fromMaps(List<Map<String, Object>> maps) {
        if (maps.isEmpty()) {
            return new DeltaLogChanges(Collections.emptyList(), Collections.emptySet());
        }

        Map<String, Object> header = maps.get(0);
        int numAdded = (int) DeltaSnapshotInfo.toLong(header.get("num_added"));
        int numRemoved = (int) DeltaSnapshotInfo.toLong(header.get("num_removed"));

        int availableAdded = Math.min(numAdded, maps.size() - 1);
        if (availableAdded < numAdded) {
            LOG.warning(String.format(
                    "DeltaLogChanges: header declares %d added files but only %d available in buffer",
                    numAdded, availableAdded));
        }

        List<DeltaFileEntry> addedFiles = new java.util.ArrayList<>(availableAdded);
        for (int i = 1; i <= numAdded && i < maps.size(); i++) {
            addedFiles.add(DeltaFileEntry.fromMap(maps.get(i)));
        }

        Set<String> removedPaths = new HashSet<>(numRemoved);
        for (int i = 1 + numAdded; i < maps.size(); i++) {
            String path = (String) maps.get(i).get("path");
            if (path != null) {
                removedPaths.add(path);
            }
        }

        return new DeltaLogChanges(addedFiles, removedPaths);
    }
}

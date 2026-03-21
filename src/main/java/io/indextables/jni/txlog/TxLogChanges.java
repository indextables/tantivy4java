package io.indextables.jni.txlog;

import io.indextables.tantivy4java.batch.BatchDocumentReader;

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Immutable data class representing post-checkpoint changes from the transaction log.
 *
 * <p>Contains added files, removed paths, and skip actions that occurred after
 * the last checkpoint, along with the maximum version observed.
 *
 * <p>The TANT buffer layout is:
 * <ol>
 *   <li>Header document with {@code num_added}, {@code num_removed}, {@code num_skips},
 *       {@code max_version}</li>
 *   <li>{@code num_added} file entry documents</li>
 *   <li>{@code num_removed} documents each with a single {@code path} field</li>
 *   <li>{@code num_skips} skip action documents with {@code path}, {@code reason},
 *       {@code skip_count}</li>
 * </ol>
 *
 * <p>Implements {@link Serializable} for Spark broadcast/shuffle.
 */
public class TxLogChanges implements Serializable {

    private static final long serialVersionUID = 1L;

    private final List<TxLogFileEntry> addedFiles;
    private final List<String> removedPaths;
    private final List<TxLogSkipAction> skipActions;
    private final long maxVersion;

    public TxLogChanges(List<TxLogFileEntry> addedFiles, List<String> removedPaths,
                        List<TxLogSkipAction> skipActions, long maxVersion) {
        this.addedFiles = addedFiles != null
                ? Collections.unmodifiableList(addedFiles)
                : Collections.emptyList();
        this.removedPaths = removedPaths != null
                ? Collections.unmodifiableList(removedPaths)
                : Collections.emptyList();
        this.skipActions = skipActions != null
                ? Collections.unmodifiableList(skipActions)
                : Collections.emptyList();
        this.maxVersion = maxVersion;
    }

    /** @return list of files added since the checkpoint */
    public List<TxLogFileEntry> getAddedFiles() { return addedFiles; }

    /** @return list of file paths removed since the checkpoint */
    public List<String> getRemovedPaths() { return removedPaths; }

    /** @return list of skip actions since the checkpoint */
    public List<TxLogSkipAction> getSkipActions() { return skipActions; }

    /** @return maximum version number observed in the changes */
    public long getMaxVersion() { return maxVersion; }

    @Override
    public String toString() {
        return String.format("TxLogChanges{added=%d, removed=%d, skips=%d, maxVersion=%d}",
                addedFiles.size(), removedPaths.size(), skipActions.size(), maxVersion);
    }

    /**
     * Parse a TANT byte buffer into a TxLogChanges instance.
     *
     * <p>The buffer contains a header document followed by added file entries,
     * removed path documents, and skip action documents.
     *
     * @param buffer raw TANT byte buffer from native layer
     * @return a new TxLogChanges
     * @throws RuntimeException if the buffer is null or empty
     */
    public static TxLogChanges fromBuffer(byte[] buffer) {
        if (buffer == null) {
            throw new RuntimeException("TxLogChanges buffer is null");
        }

        ByteBuffer bb = ByteBuffer.wrap(buffer);
        bb.order(ByteOrder.nativeOrder());

        BatchDocumentReader reader = new BatchDocumentReader();
        List<Map<String, Object>> maps = reader.parseToMaps(bb);

        return fromMaps(maps);
    }

    /**
     * Construct a TxLogChanges from a list of parsed TANT maps.
     *
     * <p>The first map is the header with counts; subsequent maps are entries
     * in order: added files, removed paths, skip actions.
     *
     * @param maps parsed documents from {@code BatchDocumentReader.parseToMaps()}
     * @return a new TxLogChanges
     */
    static TxLogChanges fromMaps(List<Map<String, Object>> maps) {
        if (maps.isEmpty()) {
            return new TxLogChanges(Collections.emptyList(), Collections.emptyList(),
                    Collections.emptyList(), -1);
        }

        // Header document
        Map<String, Object> header = maps.get(0);
        int numAdded = (int) TxLogFileEntry.toLong(header.get("num_added"));
        int numRemoved = (int) TxLogFileEntry.toLong(header.get("num_removed"));
        int numSkips = (int) TxLogFileEntry.toLong(header.get("num_skips"));
        long maxVersion = TxLogFileEntry.toLong(header.get("max_version"));

        int idx = 1;

        // Added files
        List<TxLogFileEntry> addedFiles = new ArrayList<>(numAdded);
        for (int i = 0; i < numAdded && idx < maps.size(); i++, idx++) {
            addedFiles.add(TxLogFileEntry.fromMap(maps.get(idx)));
        }

        // Removed paths
        List<String> removedPaths = new ArrayList<>(numRemoved);
        for (int i = 0; i < numRemoved && idx < maps.size(); i++, idx++) {
            String path = (String) maps.get(idx).get("path");
            if (path != null) {
                removedPaths.add(path);
            }
        }

        // Skip actions
        List<TxLogSkipAction> skipActions = new ArrayList<>(numSkips);
        for (int i = 0; i < numSkips && idx < maps.size(); i++, idx++) {
            skipActions.add(TxLogSkipAction.fromMap(maps.get(idx)));
        }

        return new TxLogChanges(addedFiles, removedPaths, skipActions, maxVersion);
    }
}

package io.indextables.tantivy4java;

import io.indextables.tantivy4java.delta.DeltaFileEntry;
import io.indextables.tantivy4java.delta.DeltaLogChanges;
import io.indextables.tantivy4java.delta.DeltaTableReader;

import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

/**
 * End-to-end tests for the streaming sync primitives:
 * {@link DeltaTableReader#getCurrentVersion} and {@link DeltaTableReader#getChangesBetween}.
 *
 * <p>Each test constructs a real Delta log on the local filesystem (same technique as
 * {@code DeltaTableReaderTest}) so no external infrastructure is required. The tests
 * exercise the full JNI path from Java through Rust and back.
 *
 * <p>Scenario covered:
 * <ol>
 *   <li>Write version 0 → verify getCurrentVersion returns 0.</li>
 *   <li>Append version 1 while "streaming is running" → verify getCurrentVersion returns 1.</li>
 *   <li>getChangesBetween(0, 1) returns only the file added in version 1.</li>
 *   <li>No-change check: getCurrentVersion still returns 1 (no new commits).</li>
 *   <li>Append version 2 → getCurrentVersion returns 2.</li>
 *   <li>getChangesBetween(1, 2) returns only the file added in version 2.</li>
 * </ol>
 */
public class DeltaStreamingPrimitivesTest {

    @TempDir
    Path tempDir;

    private static final String PROTOCOL =
        "{\"protocol\":{\"minReaderVersion\":1,\"minWriterVersion\":2}}";

    private static final String METADATA =
        "{\"metaData\":{\"id\":\"streaming-test-table\"," +
            "\"format\":{\"provider\":\"parquet\",\"options\":{}}," +
            "\"schemaString\":\"{\\\"type\\\":\\\"struct\\\",\\\"fields\\\":[" +
                "{\\\"name\\\":\\\"id\\\",\\\"type\\\":\\\"long\\\",\\\"nullable\\\":true,\\\"metadata\\\":{}}," +
                "{\\\"name\\\":\\\"name\\\",\\\"type\\\":\\\"string\\\",\\\"nullable\\\":true,\\\"metadata\\\":{}}" +
            "]}\"," +
            "\"partitionColumns\":[]," +
            "\"configuration\":{}," +
            "\"createdTime\":1700000000000}}";

    /** Add-file action helper. */
    private static String addAction(String path, long size) {
        return "{\"add\":{\"path\":\"" + path + "\"," +
            "\"partitionValues\":{}," +
            "\"size\":" + size + "," +
            "\"modificationTime\":1700000000000," +
            "\"dataChange\":true," +
            "\"stats\":\"{\\\"numRecords\\\":10}\"}}";
    }

    /** Commit-file name for a given version (zero-padded to 20 digits). */
    private static String commitName(long version) {
        return String.format("%020d.json", version);
    }

    /**
     * Build a Delta table with version 0 (protocol + metadata + one Add file).
     * Returns the table directory path.
     */
    private Path createInitialDeltaTable() throws IOException {
        Path tableDir = tempDir.resolve("streaming_delta_" + System.nanoTime());
        Files.createDirectories(tableDir);
        Path deltaLog = tableDir.resolve("_delta_log");
        Files.createDirectories(deltaLog);

        String commit0 = String.join("\n", PROTOCOL, METADATA, addAction("part-v0.parquet", 5000));
        Files.write(deltaLog.resolve(commitName(0)), commit0.getBytes());
        Files.write(tableDir.resolve("part-v0.parquet"), new byte[]{0});

        return tableDir;
    }

    /** Append a new commit to an existing Delta log. */
    private void appendCommit(Path tableDir, long version, String... actions) throws IOException {
        Path deltaLog = tableDir.resolve("_delta_log");
        String content = String.join("\n", actions);
        Files.write(deltaLog.resolve(commitName(version)), content.getBytes());
    }

    // ── getCurrentVersion ────────────────────────────────────────────────────

    @Test
    @DisplayName("getCurrentVersion returns 0 for a single-commit table")
    void testGetCurrentVersionSingleCommit() throws Exception {
        Path tableDir = createInitialDeltaTable();

        long version = DeltaTableReader.getCurrentVersion(
            tableDir.toString(), Collections.emptyMap());

        assertEquals(0L, version, "Single-commit table should be at version 0");
    }

    @Test
    @DisplayName("getCurrentVersion reflects each new commit as it is appended")
    void testGetCurrentVersionIncrementsWithCommits() throws Exception {
        Path tableDir = createInitialDeltaTable();
        String url = tableDir.toString();

        assertEquals(0L, DeltaTableReader.getCurrentVersion(url, Collections.emptyMap()));

        appendCommit(tableDir, 1L, addAction("part-v1.parquet", 6000));
        assertEquals(1L, DeltaTableReader.getCurrentVersion(url, Collections.emptyMap()),
            "After appending commit 1, version should be 1");

        appendCommit(tableDir, 2L, addAction("part-v2.parquet", 7000));
        assertEquals(2L, DeltaTableReader.getCurrentVersion(url, Collections.emptyMap()),
            "After appending commit 2, version should be 2");
    }

    @Test
    @DisplayName("getCurrentVersion is stable when no new commits are written (no-change poll)")
    void testGetCurrentVersionStableWithNoNewCommits() throws Exception {
        Path tableDir = createInitialDeltaTable();
        String url = tableDir.toString();

        appendCommit(tableDir, 1L, addAction("part-v1.parquet", 6000));
        long versionAfterCommit1 = DeltaTableReader.getCurrentVersion(url, Collections.emptyMap());
        assertEquals(1L, versionAfterCommit1);

        // Poll again without writing anything — version must not change
        long versionAfterNoPoll = DeltaTableReader.getCurrentVersion(url, Collections.emptyMap());
        assertEquals(versionAfterCommit1, versionAfterNoPoll,
            "getCurrentVersion should be stable when no new commits are written");
    }

    @Test
    @DisplayName("getCurrentVersion null URL throws")
    void testGetCurrentVersionNullUrlThrows() {
        assertThrows(IllegalArgumentException.class, () ->
            DeltaTableReader.getCurrentVersion(null, Collections.emptyMap()));
    }

    @Test
    @DisplayName("getCurrentVersion empty URL throws")
    void testGetCurrentVersionEmptyUrlThrows() {
        assertThrows(IllegalArgumentException.class, () ->
            DeltaTableReader.getCurrentVersion("", Collections.emptyMap()));
    }

    // ── getChangesBetween ────────────────────────────────────────────────────

    @Test
    @DisplayName("getChangesBetween(0,1) returns only the file added in commit 1")
    void testGetChangesBetweenSingleCommit() throws Exception {
        Path tableDir = createInitialDeltaTable();
        appendCommit(tableDir, 1L, addAction("part-v1.parquet", 6000));

        DeltaLogChanges changes = DeltaTableReader.getChangesBetween(
            tableDir.toString(), Collections.emptyMap(), 0L, 1L, null);

        assertNotNull(changes);
        List<DeltaFileEntry> added = changes.getAddedFiles();
        assertEquals(1, added.size(), "Exactly one file should be in the incremental changeset");
        assertEquals("part-v1.parquet", added.get(0).getPath());
        assertTrue(changes.getRemovedPaths().isEmpty(),
            "No removes in an append-only commit");
    }

    @Test
    @DisplayName("getChangesBetween detects removed files in an overwrite commit")
    void testGetChangesBetweenWithRemove() throws Exception {
        Path tableDir = createInitialDeltaTable();

        // Commit 1: remove version-0 file and replace with a new one
        appendCommit(tableDir, 1L,
            "{\"remove\":{\"path\":\"part-v0.parquet\",\"deletionTimestamp\":1700000001000,\"dataChange\":true}}",
            addAction("part-v1-replacement.parquet", 8000));

        DeltaLogChanges changes = DeltaTableReader.getChangesBetween(
            tableDir.toString(), Collections.emptyMap(), 0L, 1L, null);

        assertNotNull(changes);
        Set<String> removed = changes.getRemovedPaths();
        assertTrue(removed.contains("part-v0.parquet"),
            "Removed file should appear in removedPaths");
        assertEquals(1, changes.getAddedFiles().size());
        assertEquals("part-v1-replacement.parquet", changes.getAddedFiles().get(0).getPath());
    }

    @Test
    @DisplayName("getChangesBetween spans multiple commits correctly")
    void testGetChangesBetweenMultipleCommits() throws Exception {
        Path tableDir = createInitialDeltaTable();
        appendCommit(tableDir, 1L, addAction("part-v1.parquet", 6000));
        appendCommit(tableDir, 2L, addAction("part-v2.parquet", 7000));

        // Incremental range (0,2]: should include files from commits 1 and 2 only
        DeltaLogChanges changes = DeltaTableReader.getChangesBetween(
            tableDir.toString(), Collections.emptyMap(), 0L, 2L, null);

        assertNotNull(changes);
        List<DeltaFileEntry> added = changes.getAddedFiles();
        assertEquals(2, added.size(), "Both commit-1 and commit-2 files should appear");

        Set<String> paths = new java.util.HashSet<>();
        for (DeltaFileEntry f : added) paths.add(f.getPath());
        assertTrue(paths.contains("part-v1.parquet"));
        assertTrue(paths.contains("part-v2.parquet"));
        assertFalse(paths.contains("part-v0.parquet"),
            "File from version 0 (before fromVersion) must not appear");
    }

    @Test
    @DisplayName("getChangesBetween with fromVersion == toVersion returns empty changes")
    void testGetChangesBetweenSameVersionReturnsEmpty() throws Exception {
        Path tableDir = createInitialDeltaTable();
        appendCommit(tableDir, 1L, addAction("part-v1.parquet", 6000));

        DeltaLogChanges changes = DeltaTableReader.getChangesBetween(
            tableDir.toString(), Collections.emptyMap(), 1L, 1L, null);

        assertNotNull(changes);
        assertTrue(changes.getAddedFiles().isEmpty(),
            "No changes expected when fromVersion == toVersion");
        assertTrue(changes.getRemovedPaths().isEmpty());
    }

    @Test
    @DisplayName("Full streaming cycle: getCurrentVersion detects new commit, getChangesBetween fetches it")
    void testFullStreamingCycle() throws Exception {
        Path tableDir = createInitialDeltaTable();
        String url = tableDir.toString();

        // Initial sync: version 0
        long syncedVersion = DeltaTableReader.getCurrentVersion(url, Collections.emptyMap());
        assertEquals(0L, syncedVersion);

        // Simulate streaming: poll — no new commits
        long polledVersion = DeltaTableReader.getCurrentVersion(url, Collections.emptyMap());
        assertEquals(syncedVersion, polledVersion, "No new commits — version unchanged");

        // Writer appends commit 1 (simulates external write while stream is running)
        appendCommit(tableDir, 1L, addAction("part-v1.parquet", 6000));

        // Next poll detects change
        polledVersion = DeltaTableReader.getCurrentVersion(url, Collections.emptyMap());
        assertEquals(1L, polledVersion, "Commit 1 should be detected");
        assertNotEquals(syncedVersion, polledVersion);

        // Incremental fetch: only files added after syncedVersion
        DeltaLogChanges changes = DeltaTableReader.getChangesBetween(
            url, Collections.emptyMap(), syncedVersion, polledVersion, null);
        assertEquals(1, changes.getAddedFiles().size());
        assertEquals("part-v1.parquet", changes.getAddedFiles().get(0).getPath());
        syncedVersion = polledVersion;

        // Writer appends commit 2
        appendCommit(tableDir, 2L, addAction("part-v2.parquet", 7000));

        // Poll again — picks up commit 2
        polledVersion = DeltaTableReader.getCurrentVersion(url, Collections.emptyMap());
        assertEquals(2L, polledVersion);

        DeltaLogChanges changes2 = DeltaTableReader.getChangesBetween(
            url, Collections.emptyMap(), syncedVersion, polledVersion, null);
        assertEquals(1, changes2.getAddedFiles().size());
        assertEquals("part-v2.parquet", changes2.getAddedFiles().get(0).getPath());
    }
}

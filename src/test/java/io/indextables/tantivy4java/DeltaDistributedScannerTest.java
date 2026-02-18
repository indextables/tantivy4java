package io.indextables.tantivy4java;

import io.indextables.tantivy4java.delta.*;

import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Integration tests for the Delta distributed scanning primitives:
 * getSnapshotInfo(), readCheckpointPart(), readPostCheckpointChanges().
 */
public class DeltaDistributedScannerTest {

    @TempDir
    Path tempDir;

    /**
     * Create a minimal Delta table with one commit (no checkpoint).
     */
    private Path createMinimalDeltaTable() throws IOException {
        Path tableDir = tempDir.resolve("delta_distributed_" + System.nanoTime());
        Files.createDirectories(tableDir);
        Path deltaLog = tableDir.resolve("_delta_log");
        Files.createDirectories(deltaLog);

        String commit0 = String.join("\n",
            "{\"protocol\":{\"minReaderVersion\":1,\"minWriterVersion\":2}}",
            "{\"metaData\":{\"id\":\"dist-test-table\"," +
                "\"format\":{\"provider\":\"parquet\",\"options\":{}}," +
                "\"schemaString\":\"{\\\"type\\\":\\\"struct\\\",\\\"fields\\\":[" +
                    "{\\\"name\\\":\\\"id\\\",\\\"type\\\":\\\"long\\\",\\\"nullable\\\":true,\\\"metadata\\\":{}}," +
                    "{\\\"name\\\":\\\"name\\\",\\\"type\\\":\\\"string\\\",\\\"nullable\\\":true,\\\"metadata\\\":{}}" +
                "]}\"," +
                "\"partitionColumns\":[]," +
                "\"configuration\":{}," +
                "\"createdTime\":1700000000000}}",
            "{\"add\":{\"path\":\"part-00000.parquet\"," +
                "\"partitionValues\":{}," +
                "\"size\":5000," +
                "\"modificationTime\":1700000000000," +
                "\"dataChange\":true," +
                "\"stats\":\"{\\\"numRecords\\\":50}\"}}"
        );
        Files.write(deltaLog.resolve("00000000000000000000.json"), commit0.getBytes());
        Files.write(tableDir.resolve("part-00000.parquet"), new byte[]{0});

        return tableDir;
    }

    /**
     * Create a Delta table with two commits (no checkpoint).
     */
    private Path createTwoCommitDeltaTable() throws IOException {
        Path tableDir = tempDir.resolve("delta_distributed_2c_" + System.nanoTime());
        Files.createDirectories(tableDir);
        Path deltaLog = tableDir.resolve("_delta_log");
        Files.createDirectories(deltaLog);

        String commit0 = String.join("\n",
            "{\"protocol\":{\"minReaderVersion\":1,\"minWriterVersion\":2}}",
            "{\"metaData\":{\"id\":\"dist-2c-table\"," +
                "\"format\":{\"provider\":\"parquet\",\"options\":{}}," +
                "\"schemaString\":\"{\\\"type\\\":\\\"struct\\\",\\\"fields\\\":[" +
                    "{\\\"name\\\":\\\"id\\\",\\\"type\\\":\\\"long\\\",\\\"nullable\\\":true,\\\"metadata\\\":{}}" +
                "]}\"," +
                "\"partitionColumns\":[]," +
                "\"configuration\":{}," +
                "\"createdTime\":1700000000000}}",
            "{\"add\":{\"path\":\"part-00000.parquet\"," +
                "\"partitionValues\":{}," +
                "\"size\":5000," +
                "\"modificationTime\":1700000000000," +
                "\"dataChange\":true," +
                "\"stats\":\"{\\\"numRecords\\\":50}\"}}"
        );
        Files.write(deltaLog.resolve("00000000000000000000.json"), commit0.getBytes());

        // Commit 1: add second file, remove first
        String commit1 = String.join("\n",
            "{\"add\":{\"path\":\"part-00001.parquet\"," +
                "\"partitionValues\":{}," +
                "\"size\":7000," +
                "\"modificationTime\":1700000001000," +
                "\"dataChange\":true," +
                "\"stats\":\"{\\\"numRecords\\\":75}\"}}",
            "{\"remove\":{\"path\":\"part-00000.parquet\"," +
                "\"deletionTimestamp\":1700000002000," +
                "\"dataChange\":true}}"
        );
        Files.write(deltaLog.resolve("00000000000000000001.json"), commit1.getBytes());

        return tableDir;
    }

    // ── getSnapshotInfo tests ────────────────────────────────────────────────

    @Test
    @DisplayName("getSnapshotInfo throws for table without checkpoint")
    void testGetSnapshotInfoNoCheckpointThrows() throws Exception {
        Path tableDir = createMinimalDeltaTable();

        // Tables without _last_checkpoint should throw — use listFiles() instead
        RuntimeException ex = assertThrows(RuntimeException.class, () ->
            DeltaTableReader.getSnapshotInfo(tableDir.toString(), Collections.emptyMap()));
        assertTrue(ex.getMessage().contains("_last_checkpoint"),
            "Error should mention missing _last_checkpoint");
    }

    @Test
    @DisplayName("getSnapshotInfo null URL throws")
    void testGetSnapshotInfoNullUrlThrows() {
        assertThrows(IllegalArgumentException.class, () ->
            DeltaTableReader.getSnapshotInfo(null, Collections.emptyMap()));
    }

    @Test
    @DisplayName("getSnapshotInfo empty URL throws")
    void testGetSnapshotInfoEmptyUrlThrows() {
        assertThrows(IllegalArgumentException.class, () ->
            DeltaTableReader.getSnapshotInfo("", Collections.emptyMap()));
    }

    // ── readPostCheckpointChanges tests ──────────────────────────────────────

    @Test
    @DisplayName("readPostCheckpointChanges with commit files returns adds and removes")
    void testReadPostCheckpointChanges() throws Exception {
        Path tableDir = createTwoCommitDeltaTable();

        // Read both commit files as "post-checkpoint" changes
        List<String> commitPaths = Arrays.asList(
            "00000000000000000000.json",
            "00000000000000000001.json"
        );

        DeltaLogChanges changes = DeltaTableReader.readPostCheckpointChanges(
            tableDir.toString(), Collections.emptyMap(), commitPaths);

        assertNotNull(changes);
        // After log replay: part-00001 added (wins), part-00000 removed (wins)
        assertNotNull(changes.getAddedFiles());
        assertNotNull(changes.getRemovedPaths());
    }

    @Test
    @DisplayName("readPostCheckpointChanges with empty commit list returns empty changes")
    void testReadPostCheckpointChangesEmpty() throws Exception {
        Path tableDir = createMinimalDeltaTable();

        DeltaLogChanges changes = DeltaTableReader.readPostCheckpointChanges(
            tableDir.toString(), Collections.emptyMap(), Collections.emptyList());

        assertNotNull(changes);
        assertTrue(changes.getAddedFiles().isEmpty());
        assertTrue(changes.getRemovedPaths().isEmpty());
    }

    @Test
    @DisplayName("readPostCheckpointChanges null URL throws")
    void testReadPostCheckpointChangesNullUrlThrows() {
        assertThrows(IllegalArgumentException.class, () ->
            DeltaTableReader.readPostCheckpointChanges(null, Collections.emptyMap(),
                Collections.emptyList()));
    }

    // ── readCheckpointPart tests ─────────────────────────────────────────────

    @Test
    @DisplayName("readCheckpointPart null URL throws")
    void testReadCheckpointPartNullUrlThrows() {
        assertThrows(IllegalArgumentException.class, () ->
            DeltaTableReader.readCheckpointPart(null, Collections.emptyMap(), "part.parquet"));
    }

    @Test
    @DisplayName("readCheckpointPart null partPath throws")
    void testReadCheckpointPartNullPartPathThrows() throws Exception {
        Path tableDir = createMinimalDeltaTable();
        assertThrows(IllegalArgumentException.class, () ->
            DeltaTableReader.readCheckpointPart(tableDir.toString(),
                Collections.emptyMap(), null));
    }

    @Test
    @DisplayName("readCheckpointPart empty partPath throws")
    void testReadCheckpointPartEmptyPartPathThrows() throws Exception {
        Path tableDir = createMinimalDeltaTable();
        assertThrows(IllegalArgumentException.class, () ->
            DeltaTableReader.readCheckpointPart(tableDir.toString(),
                Collections.emptyMap(), ""));
    }

    // ── DeltaSnapshotInfo data class tests ───────────────────────────────────
    // Note: getSnapshotInfo() requires _last_checkpoint which our test tables lack.
    // Data class parsing is tested via the Iceberg/Parquet patterns (same-package tests).

    // ── DeltaLogChanges via readPostCheckpointChanges ────────────────────────

    @Test
    @DisplayName("DeltaLogChanges toString works")
    void testDeltaLogChangesToString() throws Exception {
        Path tableDir = createMinimalDeltaTable();

        DeltaLogChanges changes = DeltaTableReader.readPostCheckpointChanges(
            tableDir.toString(), Collections.emptyMap(), Collections.emptyList());

        assertNotNull(changes.toString());
        assertTrue(changes.toString().contains("added="));
        assertTrue(changes.toString().contains("removed="));
    }

    // ── Serializable tests ───────────────────────────────────────────────────

    @Test
    @DisplayName("DeltaFileEntry is Serializable")
    void testDeltaFileEntrySerializable() throws Exception {
        DeltaFileEntry entry = new DeltaFileEntry("test.parquet", 1000, 1700000000000L,
            50, Collections.singletonMap("date", "2024-01-01"), false, 0);

        java.io.ByteArrayOutputStream baos = new java.io.ByteArrayOutputStream();
        java.io.ObjectOutputStream oos = new java.io.ObjectOutputStream(baos);
        oos.writeObject(entry);
        oos.close();

        java.io.ObjectInputStream ois = new java.io.ObjectInputStream(
            new java.io.ByteArrayInputStream(baos.toByteArray()));
        DeltaFileEntry deserialized = (DeltaFileEntry) ois.readObject();

        assertEquals("test.parquet", deserialized.getPath());
        assertEquals(1000, deserialized.getSize());
        assertEquals(50, deserialized.getNumRecords());
        assertEquals("2024-01-01", deserialized.getPartitionValues().get("date"));
    }

    // ── Round-trip: readPostCheckpointChanges matches listFiles ─────────────

    @Test
    @DisplayName("Round-trip: readPostCheckpointChanges with all commits matches listFiles result")
    void testRoundTripMatchesListFiles() throws Exception {
        Path tableDir = createTwoCommitDeltaTable();
        String url = tableDir.toString();
        Map<String, String> config = Collections.emptyMap();

        // Get full list via old API
        List<DeltaFileEntry> fullList = DeltaTableReader.listFiles(url, config);

        // Read all commits as "post-checkpoint" changes (no checkpoint exists,
        // so we provide the known commit paths directly)
        List<String> commitPaths = Arrays.asList(
            "00000000000000000000.json",
            "00000000000000000001.json"
        );

        DeltaLogChanges changes = DeltaTableReader.readPostCheckpointChanges(
            url, config, commitPaths);

        // After log replay, active files from changes should match listFiles
        Set<String> removedPaths = changes.getRemovedPaths();
        List<DeltaFileEntry> activeFromDistributed = new ArrayList<>();
        for (DeltaFileEntry f : changes.getAddedFiles()) {
            if (!removedPaths.contains(f.getPath())) {
                activeFromDistributed.add(f);
            }
        }

        // Both should yield the same active file paths
        Set<String> fullPaths = new HashSet<>();
        for (DeltaFileEntry f : fullList) {
            fullPaths.add(f.getPath());
        }
        Set<String> distPaths = new HashSet<>();
        for (DeltaFileEntry f : activeFromDistributed) {
            distPaths.add(f.getPath());
        }

        assertEquals(fullPaths, distPaths,
            "Distributed primitives should produce the same active file set as listFiles()");
    }
}

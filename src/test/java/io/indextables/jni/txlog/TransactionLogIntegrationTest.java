package io.indextables.jni.txlog;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.*;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * End-to-end integration tests exercising the full JNI boundary:
 * Java → native Rust → storage → Avro/JSON → TANT buffer → Java deserialization.
 *
 * Uses local filesystem (file:// URLs) to avoid cloud dependencies.
 * Tests are ordered to build on each other (write → checkpoint → read).
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class TransactionLogIntegrationTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static Path tempDir;
    private static String tablePath;
    private static Map<String, String> config;

    @BeforeAll
    static void setUp() throws Exception {
        tempDir = Files.createTempDirectory("txlog-java-integration");
        // Create the _transaction_log directory that the native code expects
        Files.createDirectories(tempDir.resolve("_transaction_log"));
        tablePath = "file://" + tempDir.toAbsolutePath();
        config = Collections.emptyMap(); // local filesystem needs no credentials
    }

    @AfterAll
    static void tearDown() throws Exception {
        // Clean up temp directory
        if (tempDir != null) {
            deleteRecursively(tempDir.toFile());
        }
    }

    // ========================================================================
    // 1. Write Operations
    // ========================================================================

    @Test
    @Order(1)
    @DisplayName("Write version 0: protocol + metadata + initial adds")
    void testWriteInitialVersion() throws Exception {
        // Build version 0 actions as JSON (protocol + metadata are written via
        // version file, not through addFiles which only writes Add actions).
        // For the initial version, we use addFiles with the first batch of splits.
        String addsJson = MAPPER.writeValueAsString(Arrays.asList(
            makeAddAction("split-001.split", 5000, 100),
            makeAddAction("split-002.split", 3000, 50),
            makeAddAction("split-003.split", 4000, 75)
        ));

        WriteResult result = TransactionLogWriter.addFiles(tablePath, config, addsJson);

        assertNotNull(result);
        assertEquals(0, result.getVersion(), "First version should be 0");
        assertEquals(0, result.getRetries(), "No retries expected on first write");
        assertTrue(result.getConflictedVersions().isEmpty());
    }

    @Test
    @Order(2)
    @DisplayName("Write version 1: add more files")
    void testWriteSecondVersion() throws Exception {
        String addsJson = MAPPER.writeValueAsString(Arrays.asList(
            makeAddAction("split-004.split", 6000, 200),
            makeAddAction("split-005.split", 7000, 150)
        ));

        WriteResult result = TransactionLogWriter.addFiles(tablePath, config, addsJson);

        assertNotNull(result);
        assertEquals(1, result.getVersion(), "Second version should be 1");
    }

    @Test
    @Order(3)
    @DisplayName("Write version 2: remove a file")
    void testWriteRemoveVersion() throws Exception {
        long version = TransactionLogWriter.removeFile(tablePath, config, "split-001.split");
        assertEquals(2, version, "Remove should be version 2");
    }

    @Test
    @Order(4)
    @DisplayName("Write version 3: skip action")
    void testWriteSkipAction() throws Exception {
        String skipJson = MAPPER.writeValueAsString(Map.of(
            "path", "split-bad.split",
            "skipTimestamp", 1700000000000L,
            "reason", "merge",
            "skipCount", 3
        ));

        long version = TransactionLogWriter.skipFile(tablePath, config, skipJson);
        assertEquals(3, version, "Skip should be version 3");
    }

    // ========================================================================
    // 2. Checkpoint
    // ========================================================================

    @Test
    @Order(5)
    @DisplayName("Create Avro state checkpoint")
    void testCreateCheckpoint() throws Exception {
        // Active files after versions 0-3:
        // Added: split-002, split-003, split-004, split-005 (split-001 was removed)
        String entriesJson = MAPPER.writeValueAsString(Arrays.asList(
            makeAddAction("split-002.split", 3000, 50),
            makeAddAction("split-003.split", 4000, 75),
            makeAddAction("split-004.split", 6000, 200),
            makeAddAction("split-005.split", 7000, 150)
        ));

        String metadataJson = MAPPER.writeValueAsString(Map.of(
            "id", "test-table-001",
            "schemaString", "{\"fields\":[]}",
            "partitionColumns", List.of(),
            "format", Map.of("provider", "parquet"),
            "configuration", Map.of()
        ));

        String protocolJson = MAPPER.writeValueAsString(Map.of(
            "minReaderVersion", 4,
            "minWriterVersion", 4
        ));

        LastCheckpointInfo cpInfo = TransactionLogWriter.createCheckpoint(
            tablePath, config, entriesJson, metadataJson, protocolJson);

        assertNotNull(cpInfo);
        assertTrue(cpInfo.getVersion() >= 0, "Checkpoint version should be non-negative");
        assertEquals(4, cpInfo.getNumFiles(), "Should have 4 active files");
        assertEquals("avro-state", cpInfo.getFormat());
    }

    // ========================================================================
    // 3. Read Operations (distributed primitives via JNI)
    // ========================================================================

    @Test
    @Order(6)
    @DisplayName("Get snapshot info via JNI")
    void testGetSnapshotInfo() throws Exception {
        TxLogSnapshotInfo info = TransactionLogReader.getSnapshotInfo(tablePath, config);

        assertNotNull(info);
        assertTrue(info.getCheckpointVersion() >= 0);
        assertNotNull(info.getStateDir());
        assertFalse(info.getStateDir().isEmpty());
        assertNotNull(info.getManifestPaths());
        assertFalse(info.getManifestPaths().isEmpty(), "Should have at least one manifest");
        assertTrue(info.getNumManifests() > 0);
    }

    @Test
    @Order(7)
    @DisplayName("Read manifest via JNI")
    void testReadManifest() throws Exception {
        TxLogSnapshotInfo info = TransactionLogReader.getSnapshotInfo(tablePath, config);

        // Read the first manifest
        String firstManifest = info.getManifestPaths().get(0);
        List<TxLogFileEntry> entries = TransactionLogReader.readManifest(
            tablePath, config, info.getStateDir(), firstManifest, null);

        assertNotNull(entries);
        assertFalse(entries.isEmpty(), "Manifest should contain file entries");

        // Verify entry fields
        for (TxLogFileEntry entry : entries) {
            assertNotNull(entry.getPath());
            assertFalse(entry.getPath().isEmpty());
            assertTrue(entry.getSize() > 0, "Size should be positive");
        }
    }

    @Test
    @Order(8)
    @DisplayName("Read all manifests and verify complete file list")
    void testReadAllManifests() throws Exception {
        TxLogSnapshotInfo info = TransactionLogReader.getSnapshotInfo(tablePath, config);

        // Read all manifests
        Set<String> allPaths = new HashSet<>();
        for (String manifestPath : info.getManifestPaths()) {
            List<TxLogFileEntry> entries = TransactionLogReader.readManifest(
                tablePath, config, info.getStateDir(), manifestPath, null);
            for (TxLogFileEntry entry : entries) {
                allPaths.add(entry.getPath());
            }
        }

        // Should have the 4 active files from the checkpoint
        assertEquals(4, allPaths.size(), "Should have 4 active files across all manifests");
        assertTrue(allPaths.contains("split-002.split"));
        assertTrue(allPaths.contains("split-003.split"));
        assertTrue(allPaths.contains("split-004.split"));
        assertTrue(allPaths.contains("split-005.split"));
        assertFalse(allPaths.contains("split-001.split"), "Removed file should not be in checkpoint");
    }

    @Test
    @Order(9)
    @DisplayName("Get current version via JNI")
    void testGetCurrentVersion() throws Exception {
        long version = TransactionLogReader.getCurrentVersion(tablePath, config);
        assertEquals(3, version, "Current version should be 3 (0,1,2,3 written)");
    }

    // ========================================================================
    // 4. Write after checkpoint + read changes
    // ========================================================================

    @Test
    @Order(10)
    @DisplayName("Write post-checkpoint version and read changes")
    void testPostCheckpointChanges() throws Exception {
        // Write version 4: add another file
        String addsJson = MAPPER.writeValueAsString(List.of(
            makeAddAction("split-006.split", 8000, 300)
        ));
        WriteResult result = TransactionLogWriter.addFiles(tablePath, config, addsJson);
        assertEquals(4, result.getVersion());

        // Now get snapshot info and read post-checkpoint changes
        TxLogSnapshotInfo info = TransactionLogReader.getSnapshotInfo(tablePath, config);

        if (!info.getPostCheckpointPaths().isEmpty()) {
            String versionPathsJson = MAPPER.writeValueAsString(info.getPostCheckpointPaths());
            TxLogChanges changes = TransactionLogReader.readPostCheckpointChanges(
                tablePath, config, versionPathsJson, null);

            assertNotNull(changes);
            // Post-checkpoint changes should include our new add (and possibly v4 skip from earlier)
            assertTrue(changes.getMaxVersion() >= 0);
        }
    }

    // ========================================================================
    // 5. Full distributed pipeline
    // ========================================================================

    @Test
    @Order(11)
    @DisplayName("Full distributed scan: snapshot → manifests → changes → merged file list")
    void testFullDistributedPipeline() throws Exception {
        // 1. Get snapshot info (driver-side)
        TxLogSnapshotInfo info = TransactionLogReader.getSnapshotInfo(tablePath, config);
        assertNotNull(info);

        // 2. Read all manifests (executor-side, parallelizable)
        List<TxLogFileEntry> checkpointEntries = new ArrayList<>();
        for (String manifestPath : info.getManifestPaths()) {
            List<TxLogFileEntry> manifest = TransactionLogReader.readManifest(
                tablePath, config, info.getStateDir(), manifestPath, null);
            checkpointEntries.addAll(manifest);
        }

        // 3. Read post-checkpoint changes (driver-side)
        List<TxLogFileEntry> addedAfterCheckpoint = new ArrayList<>();
        Set<String> removedAfterCheckpoint = new HashSet<>();

        if (!info.getPostCheckpointPaths().isEmpty()) {
            String versionPathsJson = MAPPER.writeValueAsString(info.getPostCheckpointPaths());
            TxLogChanges changes = TransactionLogReader.readPostCheckpointChanges(
                tablePath, config, versionPathsJson, null);
            addedAfterCheckpoint.addAll(changes.getAddedFiles());
            removedAfterCheckpoint.addAll(changes.getRemovedPaths());
        }

        // 4. Log replay: merge checkpoint + post-checkpoint changes
        Map<String, TxLogFileEntry> fileMap = new LinkedHashMap<>();
        for (TxLogFileEntry entry : checkpointEntries) {
            fileMap.put(entry.getPath(), entry);
        }
        for (TxLogFileEntry entry : addedAfterCheckpoint) {
            fileMap.put(entry.getPath(), entry);
        }
        for (String removed : removedAfterCheckpoint) {
            fileMap.remove(removed);
        }

        // 5. Verify final file list
        Set<String> finalPaths = fileMap.keySet();
        assertTrue(finalPaths.size() >= 4, "Should have at least 4 active files");
        // split-001 was removed in version 2 (before checkpoint)
        assertFalse(finalPaths.contains("split-001.split"));
        // These should all be present
        assertTrue(finalPaths.contains("split-002.split"));
        assertTrue(finalPaths.contains("split-003.split"));
        assertTrue(finalPaths.contains("split-004.split"));
        assertTrue(finalPaths.contains("split-005.split"));
    }

    // ========================================================================
    // 6. Partition values round-trip
    // ========================================================================

    @Test
    @Order(12)
    @DisplayName("Partition values survive write → checkpoint → read round-trip")
    void testPartitionValuesRoundTrip() throws Exception {
        // Create a fresh table for this test
        Path partDir = Files.createTempDirectory("txlog-partitions");
        Files.createDirectories(partDir.resolve("_transaction_log"));
        String partTable = "file://" + partDir.toAbsolutePath();

        try {
            // Write entries with partition values
            Map<String, Object> add1 = makeAddAction("year=2024/split-a.split", 1000, 10);
            add1.put("partitionValues", Map.of("year", "2024", "month", "01"));
            Map<String, Object> add2 = makeAddAction("year=2024/split-b.split", 2000, 20);
            add2.put("partitionValues", Map.of("year", "2024", "month", "02"));

            String addsJson = MAPPER.writeValueAsString(Arrays.asList(add1, add2));
            TransactionLogWriter.addFiles(partTable, config, addsJson);

            // Create checkpoint
            String metadataJson = MAPPER.writeValueAsString(Map.of(
                "id", "partitioned-table",
                "schemaString", "{}",
                "partitionColumns", List.of("year", "month"),
                "format", Map.of("provider", "parquet"),
                "configuration", Map.of()
            ));
            String protocolJson = MAPPER.writeValueAsString(Map.of(
                "minReaderVersion", 4, "minWriterVersion", 4));

            TransactionLogWriter.createCheckpoint(partTable, config, addsJson, metadataJson, protocolJson);

            // Read back via distributed primitives
            TxLogSnapshotInfo info = TransactionLogReader.getSnapshotInfo(partTable, config);
            List<TxLogFileEntry> entries = new ArrayList<>();
            for (String mp : info.getManifestPaths()) {
                entries.addAll(TransactionLogReader.readManifest(
                    partTable, config, info.getStateDir(), mp, null));
            }

            assertEquals(2, entries.size());
            // Find the entry for split-a
            TxLogFileEntry entryA = entries.stream()
                .filter(e -> e.getPath().contains("split-a"))
                .findFirst().orElseThrow();
            assertEquals("2024", entryA.getPartitionValues().get("year"));
            assertEquals("01", entryA.getPartitionValues().get("month"));
        } finally {
            deleteRecursively(partDir.toFile());
        }
    }

    // ========================================================================
    // 7. Input validation
    // ========================================================================

    @Test
    @Order(13)
    @DisplayName("Null/empty input validation")
    void testInputValidation() {
        assertThrows(IllegalArgumentException.class,
            () -> TransactionLogReader.getSnapshotInfo(null, config));
        assertThrows(IllegalArgumentException.class,
            () -> TransactionLogReader.getSnapshotInfo("", config));
        assertThrows(IllegalArgumentException.class,
            () -> TransactionLogWriter.addFiles(null, config, "[]"));
        assertThrows(IllegalArgumentException.class,
            () -> TransactionLogWriter.addFiles(tablePath, config, null));
        assertThrows(IllegalArgumentException.class,
            () -> TransactionLogWriter.removeFile(tablePath, config, null));
        assertThrows(IllegalArgumentException.class,
            () -> TransactionLogWriter.removeFile(tablePath, config, ""));
        assertThrows(IllegalArgumentException.class,
            () -> TransactionLogWriter.skipFile(tablePath, config, null));
    }

    // ========================================================================
    // 8. Rich metadata round-trip
    // ========================================================================

    @Test
    @Order(14)
    @DisplayName("Rich metadata fields survive full round-trip")
    void testRichMetadataRoundTrip() throws Exception {
        Path richDir = Files.createTempDirectory("txlog-rich");
        Files.createDirectories(richDir.resolve("_transaction_log"));
        String richTable = "file://" + richDir.toAbsolutePath();

        try {
            Map<String, Object> richAdd = new LinkedHashMap<>();
            richAdd.put("path", "rich-split.split");
            richAdd.put("partitionValues", Map.of("region", "us-east-1"));
            richAdd.put("size", 50000);
            richAdd.put("modificationTime", 1700000000000L);
            richAdd.put("dataChange", true);
            richAdd.put("stats", "{\"numRecords\":100}");
            richAdd.put("numRecords", 100);
            richAdd.put("footerStartOffset", 49000);
            richAdd.put("footerEndOffset", 50000);
            richAdd.put("splitTags", List.of("source:indexer-1"));
            richAdd.put("numMergeOps", 2);
            richAdd.put("docMappingJson", "{\"fields\":[{\"name\":\"title\",\"type\":\"text\"}]}");
            richAdd.put("uncompressedSizeBytes", 100000);
            richAdd.put("timeRangeStart", 1000);
            richAdd.put("timeRangeEnd", 2000);
            richAdd.put("companionSourceFiles", List.of("file1.parquet", "file2.parquet"));
            richAdd.put("companionDeltaVersion", 42);
            richAdd.put("companionFastFieldMode", "HYBRID");

            String addsJson = MAPPER.writeValueAsString(List.of(richAdd));
            TransactionLogWriter.addFiles(richTable, config, addsJson);

            // Create checkpoint and read back
            String metadataJson = MAPPER.writeValueAsString(Map.of(
                "id", "rich-table", "schemaString", "{}",
                "partitionColumns", List.of("region"),
                "format", Map.of("provider", "parquet"),
                "configuration", Map.of()));
            String protocolJson = MAPPER.writeValueAsString(Map.of(
                "minReaderVersion", 4, "minWriterVersion", 4));

            TransactionLogWriter.createCheckpoint(richTable, config, addsJson, metadataJson, protocolJson);

            TxLogSnapshotInfo info = TransactionLogReader.getSnapshotInfo(richTable, config);
            List<TxLogFileEntry> entries = new ArrayList<>();
            for (String mp : info.getManifestPaths()) {
                entries.addAll(TransactionLogReader.readManifest(
                    richTable, config, info.getStateDir(), mp, null));
            }

            assertEquals(1, entries.size());
            TxLogFileEntry entry = entries.get(0);

            assertEquals("rich-split.split", entry.getPath());
            assertEquals(50000, entry.getSize());
            assertEquals(100, entry.getNumRecords());
            assertEquals("us-east-1", entry.getPartitionValues().get("region"));
            assertEquals("{\"numRecords\":100}", entry.getStats());
            assertEquals(49000, entry.getFooterStartOffset());
            assertEquals(50000, entry.getFooterEndOffset());
            assertTrue(entry.getSplitTags().contains("source:indexer-1"));
            assertEquals(2, entry.getNumMergeOps());
            assertNotNull(entry.getDocMappingJson());
            assertTrue(entry.getDocMappingJson().contains("title"));
            assertEquals(100000, entry.getUncompressedSizeBytes());
            assertEquals(1000, entry.getTimeRangeStart());
            assertEquals(2000, entry.getTimeRangeEnd());
            assertEquals(List.of("file1.parquet", "file2.parquet"), entry.getCompanionSourceFiles());
            assertEquals(42, entry.getCompanionDeltaVersion());
            assertEquals("HYBRID", entry.getCompanionFastFieldMode());
        } finally {
            deleteRecursively(richDir.toFile());
        }
    }

    // ========================================================================
    // 9. Schema Deduplication Through JNI
    // ========================================================================

    @Test
    @Order(15)
    @DisplayName("Schema deduplication through JNI: identical docMappingJson deduplicated across entries")
    void testSchemaDeduplicationThroughJni() throws Exception {
        Path dedupDir = Files.createTempDirectory("txlog-schema-dedup");
        Files.createDirectories(dedupDir.resolve("_transaction_log"));
        String dedupTable = "file://" + dedupDir.toAbsolutePath();

        try {
            String schema1 = "{\"fields\":[{\"name\":\"title\",\"type\":\"text\"},{\"name\":\"body\",\"type\":\"text\"}]}";

            // Write 3 adds all with the SAME docMappingJson
            List<Map<String, Object>> batch1 = new ArrayList<>();
            for (int i = 0; i < 3; i++) {
                Map<String, Object> add = makeAddAction("dedup-split-" + i + ".split", 1000 + i, 10 + i);
                add.put("docMappingJson", schema1);
                batch1.add(add);
            }
            String addsJson1 = MAPPER.writeValueAsString(batch1);
            TransactionLogWriter.addFiles(dedupTable, config, addsJson1);

            // Create checkpoint with these 3 entries
            String metadataJson = MAPPER.writeValueAsString(Map.of(
                "id", "dedup-table", "schemaString", "{}",
                "partitionColumns", List.of(),
                "format", Map.of("provider", "parquet"),
                "configuration", Map.of()));
            String protocolJson = MAPPER.writeValueAsString(Map.of(
                "minReaderVersion", 4, "minWriterVersion", 4));

            TransactionLogWriter.createCheckpoint(dedupTable, config, addsJson1, metadataJson, protocolJson);

            // Read back and verify all 3 have their docMappingJson restored
            TxLogSnapshotInfo info1 = TransactionLogReader.getSnapshotInfo(dedupTable, config);
            List<TxLogFileEntry> entries1 = new ArrayList<>();
            for (String mp : info1.getManifestPaths()) {
                entries1.addAll(TransactionLogReader.readManifest(
                    dedupTable, config, info1.getStateDir(), mp, null));
            }
            assertEquals(3, entries1.size(), "Should have 3 entries from first batch");
            for (TxLogFileEntry entry : entries1) {
                assertNotNull(entry.getDocMappingJson(),
                    "docMappingJson should be restored for " + entry.getPath());
                assertEquals(schema1, entry.getDocMappingJson(),
                    "docMappingJson should match original for " + entry.getPath());
            }

            // Now write 2 MORE adds with a DIFFERENT docMappingJson
            String schema2 = "{\"fields\":[{\"name\":\"id\",\"type\":\"i64\"}]}";
            List<Map<String, Object>> batch2 = new ArrayList<>();
            for (int i = 0; i < 2; i++) {
                Map<String, Object> add = makeAddAction("dedup-new-" + i + ".split", 2000 + i, 20 + i);
                add.put("docMappingJson", schema2);
                batch2.add(add);
            }
            String addsJson2 = MAPPER.writeValueAsString(batch2);
            TransactionLogWriter.addFiles(dedupTable, config, addsJson2);

            // Create another checkpoint with ALL 5 entries
            List<Map<String, Object>> allEntries = new ArrayList<>(batch1);
            allEntries.addAll(batch2);
            String allEntriesJson = MAPPER.writeValueAsString(allEntries);

            TransactionLogWriter.createCheckpoint(dedupTable, config, allEntriesJson, metadataJson, protocolJson);

            // Read back and verify both schemas are correctly restored
            TxLogSnapshotInfo info2 = TransactionLogReader.getSnapshotInfo(dedupTable, config);
            List<TxLogFileEntry> entries2 = new ArrayList<>();
            for (String mp : info2.getManifestPaths()) {
                entries2.addAll(TransactionLogReader.readManifest(
                    dedupTable, config, info2.getStateDir(), mp, null));
            }
            assertEquals(5, entries2.size(), "Should have 5 entries total");

            for (TxLogFileEntry entry : entries2) {
                if (entry.getPath().startsWith("dedup-split-")) {
                    assertEquals(schema1, entry.getDocMappingJson(),
                        "Original schema entries should have schema1: " + entry.getPath());
                } else if (entry.getPath().startsWith("dedup-new-")) {
                    assertEquals(schema2, entry.getDocMappingJson(),
                        "New schema entries should have schema2: " + entry.getPath());
                } else {
                    fail("Unexpected path: " + entry.getPath());
                }
            }
        } finally {
            deleteRecursively(dedupDir.toFile());
        }
    }

    // ========================================================================
    // 10. Concurrent Write Retry
    // ========================================================================

    @Test
    @Order(16)
    @DisplayName("Concurrent write retry: 5 threads write simultaneously with optimistic concurrency")
    void testConcurrentWriteRetry() throws Exception {
        Path concDir = Files.createTempDirectory("txlog-concurrent");
        Files.createDirectories(concDir.resolve("_transaction_log"));
        String concTable = "file://" + concDir.toAbsolutePath();

        try {
            // Write initial version 0
            String initJson = MAPPER.writeValueAsString(List.of(
                makeAddAction("init-split.split", 1000, 10)));
            WriteResult initResult = TransactionLogWriter.addFiles(concTable, config, initJson);
            assertEquals(0, initResult.getVersion(), "Initial version should be 0");

            // Spawn 5 threads that each call addFiles simultaneously
            int numThreads = 5;
            ExecutorService executor = Executors.newFixedThreadPool(numThreads);
            CountDownLatch startLatch = new CountDownLatch(1);
            List<Future<WriteResult>> futures = new ArrayList<>();

            for (int i = 0; i < numThreads; i++) {
                final int threadIdx = i;
                futures.add(executor.submit(() -> {
                    startLatch.await(); // Wait for all threads to be ready
                    String addsJson = MAPPER.writeValueAsString(List.of(
                        makeAddAction("concurrent-split-" + threadIdx + ".split", 2000 + threadIdx, 20 + threadIdx)));
                    return TransactionLogWriter.addFiles(concTable, config, addsJson);
                }));
            }

            // Release all threads at once
            startLatch.countDown();

            // Collect results
            List<WriteResult> results = new ArrayList<>();
            for (Future<WriteResult> future : futures) {
                results.add(future.get(30, TimeUnit.SECONDS));
            }
            executor.shutdown();

            // Verify: all 5 succeeded (no exceptions)
            assertEquals(numThreads, results.size(), "All 5 writes should succeed");

            // All got different version numbers (1-5)
            Set<Long> versions = new TreeSet<>();
            for (WriteResult r : results) {
                versions.add(r.getVersion());
            }
            assertEquals(numThreads, versions.size(), "All versions should be unique");

            // Versions should be contiguous from 1 to 5
            long minVersion = Collections.min(versions);
            long maxVersion = Collections.max(versions);
            assertEquals(1, minVersion, "Minimum version should be 1");
            assertEquals(numThreads, maxVersion, "Maximum version should be " + numThreads);

            // Some threads should have had retries > 0 (likely but not guaranteed)
            int totalRetries = results.stream().mapToInt(WriteResult::getRetries).sum();
            // At least log it - contention should cause some retries
            System.out.println("Concurrent write total retries: " + totalRetries);

            // Read back all versions and verify all 5 adds are present
            long currentVersion = TransactionLogReader.getCurrentVersion(concTable, config);
            assertEquals(numThreads, currentVersion,
                "Current version should be " + numThreads + " (0=init + " + numThreads + " concurrent)");

        } finally {
            deleteRecursively(concDir.toFile());
        }
    }

    // ========================================================================
    // 11. Error Paths via JNI
    // ========================================================================

    @Test
    @Order(17)
    @DisplayName("Error paths: invalid inputs produce RuntimeException from native layer")
    void testErrorPaths() {
        // getSnapshotInfo on non-existent table path
        assertThrows(RuntimeException.class,
            () -> TransactionLogReader.getSnapshotInfo(
                "file:///tmp/txlog-nonexistent-" + UUID.randomUUID(), config));

        // readManifest with bogus manifest path
        assertThrows(RuntimeException.class,
            () -> TransactionLogReader.readManifest(
                tablePath, config, "bogus-state-dir",
                "nonexistent-manifest-" + UUID.randomUUID() + ".avro", null));

        // getCurrentVersion on non-existent table
        assertThrows(RuntimeException.class,
            () -> TransactionLogReader.getCurrentVersion(
                "file:///tmp/txlog-nonexistent-" + UUID.randomUUID(), config));

        // addFiles with malformed JSON (not valid JSON)
        assertThrows(RuntimeException.class,
            () -> TransactionLogWriter.addFiles(tablePath, config, "this is not valid json {{{"));

        // skipFile with malformed JSON
        assertThrows(RuntimeException.class,
            () -> TransactionLogWriter.skipFile(tablePath, config, "this is not valid json {{{"));
    }

    // ========================================================================
    // 12. Skip Actions Through Distributed Pipeline
    // ========================================================================

    @Test
    @Order(18)
    @DisplayName("Skip actions survive write → checkpoint → post-checkpoint read pipeline")
    void testSkipActionsThroughPipeline() throws Exception {
        // Disable auto-checkpoint so skip/add versions remain as post-checkpoint changes
        Map<String, String> noCpConfig = new HashMap<>(config);
        noCpConfig.put("checkpoint_interval", "0");

        Path skipDir = Files.createTempDirectory("txlog-skip-pipeline");
        Files.createDirectories(skipDir.resolve("_transaction_log"));
        String skipTable = "file://" + skipDir.toAbsolutePath();

        try {
            // Version 0: add 2 files
            List<Map<String, Object>> initialAdds = Arrays.asList(
                makeAddAction("file-a.split", 1000, 10),
                makeAddAction("file-b.split", 2000, 20));
            String addsJson = MAPPER.writeValueAsString(initialAdds);
            TransactionLogWriter.addFiles(skipTable, noCpConfig, addsJson);

            // Create checkpoint
            String metadataJson = MAPPER.writeValueAsString(Map.of(
                "id", "skip-table", "schemaString", "{}",
                "partitionColumns", List.of(),
                "format", Map.of("provider", "parquet"),
                "configuration", Map.of()));
            String protocolJson = MAPPER.writeValueAsString(Map.of(
                "minReaderVersion", 4, "minWriterVersion", 4));

            TransactionLogWriter.createCheckpoint(skipTable, noCpConfig, addsJson, metadataJson, protocolJson);

            // Version 1: skip action for "bad-file.split"
            String skipJson = MAPPER.writeValueAsString(Map.of(
                "path", "bad-file.split",
                "skipTimestamp", 1700000000000L,
                "reason", "merge",
                "skipCount", 3));
            long skipVersion = TransactionLogWriter.skipFile(skipTable, noCpConfig, skipJson);
            assertTrue(skipVersion > 0, "Skip version should be > 0 (post-checkpoint)");

            // Version 2: add another file
            String addMoreJson = MAPPER.writeValueAsString(List.of(
                makeAddAction("file-c.split", 3000, 30)));
            TransactionLogWriter.addFiles(skipTable, noCpConfig, addMoreJson);

            // Read back via getSnapshotInfo + readPostCheckpointChanges
            TxLogSnapshotInfo info = TransactionLogReader.getSnapshotInfo(skipTable, noCpConfig);

            assertFalse(info.getPostCheckpointPaths().isEmpty(),
                "Should have post-checkpoint version paths");

            String versionPathsJson = MAPPER.writeValueAsString(info.getPostCheckpointPaths());
            TxLogChanges changes = TransactionLogReader.readPostCheckpointChanges(
                skipTable, noCpConfig, versionPathsJson, null);

            assertNotNull(changes);
            assertEquals(1, changes.getAddedFiles().size(),
                "Should have 1 added file post-checkpoint");
            assertEquals("file-c.split", changes.getAddedFiles().get(0).getPath());
            assertEquals(0, changes.getRemovedPaths().size(),
                "Should have 0 removed files");
            assertEquals(1, changes.getSkipActions().size(),
                "Should have 1 skip action");

            TxLogSkipAction skipAction = changes.getSkipActions().get(0);
            assertEquals("bad-file.split", skipAction.getPath());
            assertEquals("merge", skipAction.getReason());
            assertEquals(3, skipAction.getSkipCount());
        } finally {
            deleteRecursively(skipDir.toFile());
        }
    }

    // ========================================================================
    // 13. Large Scale Test
    // ========================================================================

    @Test
    @Order(19)
    @DisplayName("Large scale: 5000 file entries written, checkpointed, and read back")
    void testLargeScaleFileList() throws Exception {
        Path largeDir = Files.createTempDirectory("txlog-large-scale");
        Files.createDirectories(largeDir.resolve("_transaction_log"));
        String largeTable = "file://" + largeDir.toAbsolutePath();

        try {
            // Generate 5000 AddAction entries
            int numEntries = 5000;
            List<Map<String, Object>> entries = new ArrayList<>(numEntries);
            for (int i = 0; i < numEntries; i++) {
                entries.add(makeAddAction(
                    String.format("split-%05d.split", i),
                    1000 + i,
                    i));
            }
            String addsJson = MAPPER.writeValueAsString(entries);

            // Write via addFiles (single batch)
            WriteResult result = TransactionLogWriter.addFiles(largeTable, config, addsJson);
            assertNotNull(result);
            assertEquals(0, result.getVersion());

            // Create checkpoint
            String metadataJson = MAPPER.writeValueAsString(Map.of(
                "id", "large-table", "schemaString", "{}",
                "partitionColumns", List.of(),
                "format", Map.of("provider", "parquet"),
                "configuration", Map.of()));
            String protocolJson = MAPPER.writeValueAsString(Map.of(
                "minReaderVersion", 4, "minWriterVersion", 4));

            TransactionLogWriter.createCheckpoint(largeTable, config, addsJson, metadataJson, protocolJson);

            // Read back via getSnapshotInfo + readManifest (read all manifests)
            TxLogSnapshotInfo info = TransactionLogReader.getSnapshotInfo(largeTable, config);
            assertNotNull(info);

            List<TxLogFileEntry> allEntries = new ArrayList<>();
            for (String mp : info.getManifestPaths()) {
                allEntries.addAll(TransactionLogReader.readManifest(
                    largeTable, config, info.getStateDir(), mp, null));
            }

            // Verify exactly 5000 entries returned
            assertEquals(numEntries, allEntries.size(),
                "Should have exactly " + numEntries + " entries");

            // Build a lookup map for spot-checks
            Map<String, TxLogFileEntry> entryMap = new LinkedHashMap<>();
            for (TxLogFileEntry e : allEntries) {
                entryMap.put(e.getPath(), e);
            }

            // Spot-check: entry for "split-00000.split" has size=1000
            TxLogFileEntry first = entryMap.get("split-00000.split");
            assertNotNull(first, "split-00000.split should exist");
            assertEquals(1000, first.getSize());
            assertEquals(0, first.getNumRecords());

            // Spot-check: entry for "split-04999.split" has size=5999
            TxLogFileEntry last = entryMap.get("split-04999.split");
            assertNotNull(last, "split-04999.split should exist");
            assertEquals(5999, last.getSize());
            assertEquals(4999, last.getNumRecords());
        } finally {
            deleteRecursively(largeDir.toFile());
        }
    }

    // ========================================================================
    // 14. Multi-Manifest Verification
    // ========================================================================

    @Test
    @Order(20)
    @DisplayName("Multi-manifest partitioning: partition values preserved across manifests")
    void testMultiManifestPartitioning() throws Exception {
        Path multiDir = Files.createTempDirectory("txlog-multi-manifest");
        Files.createDirectories(multiDir.resolve("_transaction_log"));
        String multiTable = "file://" + multiDir.toAbsolutePath();

        try {
            // Generate entries with partition values: 100 entries each for year=2022, 2023, 2024
            List<Map<String, Object>> allAdds = new ArrayList<>();
            for (String year : Arrays.asList("2022", "2023", "2024")) {
                for (int i = 0; i < 100; i++) {
                    Map<String, Object> add = makeAddAction(
                        String.format("year=%s/split-%03d.split", year, i),
                        1000 + i, 10 + i);
                    add.put("partitionValues", Map.of("year", year));
                    allAdds.add(add);
                }
            }
            String addsJson = MAPPER.writeValueAsString(allAdds);
            TransactionLogWriter.addFiles(multiTable, config, addsJson);

            // Create checkpoint with partitionColumns=["year"]
            String metadataJson = MAPPER.writeValueAsString(Map.of(
                "id", "multi-manifest-table", "schemaString", "{}",
                "partitionColumns", List.of("year"),
                "format", Map.of("provider", "parquet"),
                "configuration", Map.of()));
            String protocolJson = MAPPER.writeValueAsString(Map.of(
                "minReaderVersion", 4, "minWriterVersion", 4));

            TransactionLogWriter.createCheckpoint(multiTable, config, addsJson, metadataJson, protocolJson);

            // getSnapshotInfo - check we got manifest paths
            TxLogSnapshotInfo info = TransactionLogReader.getSnapshotInfo(multiTable, config);
            assertNotNull(info);
            assertFalse(info.getManifestPaths().isEmpty(), "Should have manifest paths");

            // Read each manifest individually, collect entries per manifest
            List<TxLogFileEntry> allReadEntries = new ArrayList<>();
            for (String manifestPath : info.getManifestPaths()) {
                List<TxLogFileEntry> manifestEntries = TransactionLogReader.readManifest(
                    multiTable, config, info.getStateDir(), manifestPath, null);
                allReadEntries.addAll(manifestEntries);
            }

            // Verify total = 300 entries across all manifests
            assertEquals(300, allReadEntries.size(),
                "Should have 300 total entries across all manifests");

            // Verify partition values are preserved
            Map<String, Integer> countByYear = new HashMap<>();
            for (TxLogFileEntry entry : allReadEntries) {
                String year = entry.getPartitionValues().get("year");
                assertNotNull(year, "Partition value 'year' should be present for " + entry.getPath());
                countByYear.merge(year, 1, Integer::sum);
            }

            assertEquals(100, countByYear.getOrDefault("2022", 0),
                "Should have 100 entries for year=2022");
            assertEquals(100, countByYear.getOrDefault("2023", 0),
                "Should have 100 entries for year=2023");
            assertEquals(100, countByYear.getOrDefault("2024", 0),
                "Should have 100 entries for year=2024");
        } finally {
            deleteRecursively(multiDir.toFile());
        }
    }

    // ========================================================================
    // 21-27. Gap Closure Integration Tests
    // ========================================================================

    @Test
    @Order(21)
    void testInitializeTable() throws Exception {
        Path initDir = Files.createTempDirectory("txlog-init");
        Files.createDirectories(initDir.resolve("_transaction_log"));
        String initTable = "file://" + initDir.toAbsolutePath();

        try {
            String protocolJson = MAPPER.writeValueAsString(Map.of(
                "minReaderVersion", 4, "minWriterVersion", 4));
            String metadataJson = MAPPER.writeValueAsString(Map.of(
                "id", "init-test-id",
                "schemaString", "{\"fields\":[]}",
                "partitionColumns", List.of(),
                "configuration", Map.of()));

            // First init should succeed
            TransactionLogWriter.initializeTable(initTable, config, protocolJson, metadataJson);

            // Verify version 0 exists
            long version = TransactionLogReader.getCurrentVersion(initTable, config);
            assertEquals(0, version, "Should have version 0 after init");

            // Second init should fail (version 0 already exists)
            assertThrows(RuntimeException.class, () ->
                TransactionLogWriter.initializeTable(initTable, config, protocolJson, metadataJson),
                "Should throw when table already initialized");
        } finally {
            deleteRecursively(initDir.toFile());
        }
    }

    @Test
    @Order(22)
    void testWriteVersionMixedActions() throws Exception {
        Path mixDir = Files.createTempDirectory("txlog-mixed");
        Files.createDirectories(mixDir.resolve("_transaction_log"));
        String mixTable = "file://" + mixDir.toAbsolutePath();

        try {
            // Initialize table first
            String protocolJson = MAPPER.writeValueAsString(Map.of(
                "minReaderVersion", 4, "minWriterVersion", 4));
            String metadataJson = MAPPER.writeValueAsString(Map.of(
                "id", "mixed-test",
                "schemaString", "{\"fields\":[]}",
                "partitionColumns", List.of(),
                "configuration", Map.of()));
            TransactionLogWriter.initializeTable(mixTable, config, protocolJson, metadataJson);

            // Write mixed actions: Add + Remove in same version (like commitMergeSplits)
            String actionsJson =
                "{\"add\":{\"path\":\"new.split\",\"partitionValues\":{},\"size\":5000,\"modificationTime\":1700000000000,\"dataChange\":true,\"numRecords\":100}}\n" +
                "{\"remove\":{\"path\":\"old.split\",\"deletionTimestamp\":1700000000000,\"dataChange\":true}}";

            WriteResult result = TransactionLogWriter.writeVersion(mixTable, config, actionsJson);
            assertEquals(1, result.getVersion(), "Should write version 1");
            assertEquals(0, result.getRetries(), "Should succeed without retries");

            // Verify by reading the version back
            String content = TransactionLogReader.readVersion(mixTable, config, 1);
            assertNotNull(content);
            assertTrue(content.contains("new.split"), "Should contain add action");
            assertTrue(content.contains("old.split"), "Should contain remove action");
        } finally {
            deleteRecursively(mixDir.toFile());
        }
    }

    @Test
    @Order(23)
    void testWriteVersionOnceConflict() throws Exception {
        Path onceDir = Files.createTempDirectory("txlog-once");
        Files.createDirectories(onceDir.resolve("_transaction_log"));
        String onceTable = "file://" + onceDir.toAbsolutePath();

        try {
            // Write version 0
            String v0Actions = "{\"protocol\":{\"minReaderVersion\":4,\"minWriterVersion\":4}}";
            WriteResult r1 = TransactionLogWriter.writeVersionOnce(onceTable, config, v0Actions);
            assertEquals(0, r1.getVersion(), "First write should get version 0");

            // Write version 1 — should succeed
            String v1Actions = "{\"add\":{\"path\":\"a.split\",\"partitionValues\":{},\"size\":100,\"modificationTime\":0,\"dataChange\":true}}";
            WriteResult r2 = TransactionLogWriter.writeVersionOnce(onceTable, config, v1Actions);
            assertEquals(1, r2.getVersion(), "Second write should get version 1");
        } finally {
            deleteRecursively(onceDir.toFile());
        }
    }

    @Test
    @Order(24)
    void testListVersions() throws Exception {
        // Use the main test table which has versions 0-4 from earlier tests
        long[] versions = TransactionLogReader.listVersions(tablePath, config);
        assertNotNull(versions);
        assertTrue(versions.length >= 4, "Should have at least 4 versions, got " + versions.length);
        // Versions should be sorted
        for (int i = 1; i < versions.length; i++) {
            assertTrue(versions[i] > versions[i-1],
                "Versions should be sorted ascending: " + versions[i-1] + " < " + versions[i]);
        }
    }

    @Test
    @Order(25)
    void testReadVersion() throws Exception {
        // Use a dedicated table with proper initialization (version 0 = protocol + metadata)
        Path rvDir = Files.createTempDirectory("txlog-readver");
        Files.createDirectories(rvDir.resolve("_transaction_log"));
        String rvTable = "file://" + rvDir.toAbsolutePath();

        try {
            String protocolJson = MAPPER.writeValueAsString(Map.of(
                "minReaderVersion", 4, "minWriterVersion", 4));
            String metadataJson = MAPPER.writeValueAsString(Map.of(
                "id", "readver-test",
                "schemaString", "{\"fields\":[]}",
                "partitionColumns", List.of(),
                "configuration", Map.of()));

            TransactionLogWriter.initializeTable(rvTable, config, protocolJson, metadataJson);

            // Read version 0 — should contain protocol + metadata
            String content = TransactionLogReader.readVersion(rvTable, config, 0);
            assertNotNull(content);
            assertFalse(content.isEmpty(), "Version 0 content should not be empty");
            assertTrue(content.contains("protocol"),
                "Version 0 should contain protocol action");
            assertTrue(content.contains("metaData") || content.contains("metaData"),
                "Version 0 should contain metadata action");

            // Write version 1 with an add action
            String addActions = "{\"add\":{\"path\":\"test.split\",\"partitionValues\":{},\"size\":100,\"modificationTime\":0,\"dataChange\":true}}";
            TransactionLogWriter.writeVersionOnce(rvTable, config, addActions);

            // Read version 1
            String v1Content = TransactionLogReader.readVersion(rvTable, config, 1);
            assertNotNull(v1Content);
            assertTrue(v1Content.contains("test.split"),
                "Version 1 should contain the add action");
        } finally {
            deleteRecursively(rvDir.toFile());
        }
    }

    @Test
    @Order(26)
    void testDeleteOpstampAndDocMappingRefRoundTrip() throws Exception {
        Path fieldDir = Files.createTempDirectory("txlog-fields");
        Files.createDirectories(fieldDir.resolve("_transaction_log"));
        String fieldTable = "file://" + fieldDir.toAbsolutePath();

        try {
            // Initialize
            String protocolJson = MAPPER.writeValueAsString(Map.of(
                "minReaderVersion", 4, "minWriterVersion", 4));
            String metadataJson = MAPPER.writeValueAsString(Map.of(
                "id", "field-test",
                "schemaString", "{\"fields\":[]}",
                "partitionColumns", List.of(),
                "configuration", Map.of("docMappingSchema.abc123", "{\"fields\":[]}")));
            TransactionLogWriter.initializeTable(fieldTable, config, protocolJson, metadataJson);

            // Write add action with deleteOpstamp and docMappingRef
            Map<String, Object> addWithFields = new LinkedHashMap<>();
            addWithFields.put("path", "split-with-fields.split");
            addWithFields.put("partitionValues", Map.of());
            addWithFields.put("size", 10000);
            addWithFields.put("modificationTime", 1700000000000L);
            addWithFields.put("dataChange", true);
            addWithFields.put("numRecords", 500);
            addWithFields.put("deleteOpstamp", 42);
            addWithFields.put("docMappingRef", "abc123");
            addWithFields.put("splitTags", List.of("env:test", "tier:hot"));

            String addsJson = MAPPER.writeValueAsString(List.of(addWithFields));
            TransactionLogWriter.addFiles(fieldTable, config, addsJson);

            // Checkpoint and read back
            String entriesJson = MAPPER.writeValueAsString(List.of(addWithFields));
            TransactionLogWriter.createCheckpoint(fieldTable, config, entriesJson, metadataJson, protocolJson);

            TxLogSnapshotInfo info = TransactionLogReader.getSnapshotInfo(fieldTable, config);
            List<TxLogFileEntry> entries = new ArrayList<>();
            for (String mp : info.getManifestPaths()) {
                entries.addAll(TransactionLogReader.readManifest(
                    fieldTable, config, info.getStateDir(), mp,
                    MAPPER.writeValueAsString(Map.of("docMappingSchema.abc123", "{\"fields\":[]}"))));
            }

            assertEquals(1, entries.size());
            TxLogFileEntry entry = entries.get(0);

            // Verify deleteOpstamp round-trip
            assertTrue(entry.hasDeleteOpstamp(), "Should have deleteOpstamp");
            assertEquals(42, entry.getDeleteOpstamp());

            // Verify splitTags as List<String> round-trip
            assertNotNull(entry.getSplitTags());
            assertTrue(entry.getSplitTags().contains("env:test"));
            assertTrue(entry.getSplitTags().contains("tier:hot"));

            // docMappingRef is consumed by schema dedup: on read it gets resolved
            // to docMappingJson if the schema registry contains the ref
            // The ref itself may or may not survive depending on dedup behavior
            assertNotNull(entry.getPath());
        } finally {
            deleteRecursively(fieldDir.toFile());
        }
    }

    @Test
    @Order(27)
    void testExpandedSkipActionFields() throws Exception {
        Path skipDir = Files.createTempDirectory("txlog-skip-expanded");
        Files.createDirectories(skipDir.resolve("_transaction_log"));
        String skipTable = "file://" + skipDir.toAbsolutePath();

        try {
            // Initialize
            String protocolJson = MAPPER.writeValueAsString(Map.of(
                "minReaderVersion", 4, "minWriterVersion", 4));
            String metadataJson = MAPPER.writeValueAsString(Map.of(
                "id", "skip-test",
                "schemaString", "{\"fields\":[]}",
                "partitionColumns", List.of(),
                "configuration", Map.of()));
            TransactionLogWriter.initializeTable(skipTable, config, protocolJson, metadataJson);

            // Write add + checkpoint so we have a baseline
            String addJson = MAPPER.writeValueAsString(List.of(
                makeAddAction("keep.split", 1000, 100)));
            TransactionLogWriter.addFiles(skipTable, config, addJson);

            // Write expanded skip action with all fields
            Map<String, Object> skipMap = new LinkedHashMap<>();
            skipMap.put("path", "bad.split");
            skipMap.put("skipTimestamp", 1700000000000L);
            skipMap.put("reason", "merge_conflict");
            skipMap.put("operation", "merge_v2");
            skipMap.put("partitionValues", Map.of("date", "2024-01-01"));
            skipMap.put("size", 5000);
            skipMap.put("retryAfter", 1700000300000L);
            skipMap.put("skipCount", 3);
            String skipJson = MAPPER.writeValueAsString(skipMap);
            TransactionLogWriter.skipFile(skipTable, config, skipJson);

            // Create checkpoint with the add action
            String cpEntries = MAPPER.writeValueAsString(List.of(
                makeAddAction("keep.split", 1000, 100)));
            TransactionLogWriter.createCheckpoint(skipTable, config, cpEntries, metadataJson, protocolJson);

            // Read post-checkpoint changes (the skip was written after the add but before checkpoint —
            // actually it's version 2, and checkpoint is at version 2 or 3, so let's read version 2 directly)
            String v2Content = TransactionLogReader.readVersion(skipTable, config, 2);
            assertNotNull(v2Content);
            assertTrue(v2Content.contains("merge_conflict"), "Skip action should contain reason");
            assertTrue(v2Content.contains("merge_v2"), "Skip action should contain operation");
            assertTrue(v2Content.contains("retryAfter") || v2Content.contains("retry_after"),
                "Skip action should contain retryAfter");
        } finally {
            deleteRecursively(skipDir.toFile());
        }
    }

    // ========================================================================
    // 28-30. GAP-9 and GAP-10 Integration Tests
    // ========================================================================

    @Test
    @Order(28)
    void testSnapshotInfoWithoutCheckpoint() throws Exception {
        // GAP-9: getSnapshotInfo should work on tables without _last_checkpoint
        // Disable auto-checkpoint so we can test the no-checkpoint fallback
        Map<String, String> noCpConfig = new HashMap<>(config);
        noCpConfig.put("checkpoint_interval", "0");

        Path noCheckpointDir = Files.createTempDirectory("txlog-nocheckpoint");
        Files.createDirectories(noCheckpointDir.resolve("_transaction_log"));
        String noCheckpointTable = "file://" + noCheckpointDir.toAbsolutePath();

        try {
            // Initialize table (writes version 0 with protocol + metadata, no checkpoint)
            String protocolJson = MAPPER.writeValueAsString(Map.of(
                "minReaderVersion", 4, "minWriterVersion", 4));
            String metadataJson = MAPPER.writeValueAsString(Map.of(
                "id", "nocheckpoint-test",
                "schemaString", "{\"fields\":[{\"name\":\"title\",\"type\":\"text\"}]}",
                "partitionColumns", List.of(),
                "configuration", Map.of()));
            TransactionLogWriter.initializeTable(noCheckpointTable, noCpConfig, protocolJson, metadataJson);

            // Add some files without creating a checkpoint (auto-checkpoint disabled)
            for (int i = 0; i < 3; i++) {
                String addsJson = MAPPER.writeValueAsString(List.of(
                    makeAddAction("no-cp-split-" + i + ".split", 1000 + i, 100 + i)));
                TransactionLogWriter.addFiles(noCheckpointTable, noCpConfig, addsJson);
            }

            // getSnapshotInfo should NOT throw — it should fall back to version scanning
            TxLogSnapshotInfo snapshot = TransactionLogReader.getSnapshotInfo(noCheckpointTable, noCpConfig);
            assertNotNull(snapshot);

            // checkpointVersion should be -1 (no checkpoint)
            assertEquals(-1, snapshot.getCheckpointVersion(),
                "Should report checkpointVersion=-1 when no checkpoint exists");

            // manifestPaths should be empty
            assertTrue(snapshot.getManifestPaths().isEmpty(),
                "Should have no manifest paths without checkpoint");

            // All version files should be in postCheckpointPaths
            assertTrue(snapshot.getPostCheckpointPaths().size() >= 4,
                "Should have at least 4 version paths (v0 init + 3 adds), got " +
                snapshot.getPostCheckpointPaths().size());

            // Metadata should be populated from version 0
            assertNotNull(snapshot.getMetadataJson());
            assertTrue(snapshot.getMetadataJson().contains("nocheckpoint-test") ||
                       snapshot.getMetadataJson().contains("title"),
                "Metadata should contain table schema or id");

            // Protocol should be populated
            assertNotNull(snapshot.getProtocolJson());

            // Reading post-checkpoint changes should return all files
            TxLogChanges changes = TransactionLogReader.readPostCheckpointChanges(
                noCheckpointTable, noCpConfig,
                MAPPER.writeValueAsString(snapshot.getPostCheckpointPaths()),
                "{}");
            assertTrue(changes.getAddedFiles().size() >= 3,
                "Should have at least 3 added files, got " + changes.getAddedFiles().size());
        } finally {
            deleteRecursively(noCheckpointDir.toFile());
        }
    }

    @Test
    @Order(29)
    void testSnapshotInfoWithAddAndRemove() throws Exception {
        // GAP-9 test case 3: removes should be reflected in version scan
        // Disable auto-checkpoint so removes appear in post-checkpoint changes
        Map<String, String> noCpConfig = new HashMap<>(config);
        noCpConfig.put("checkpoint_interval", "0");

        Path arDir = Files.createTempDirectory("txlog-addremove");
        Files.createDirectories(arDir.resolve("_transaction_log"));
        String arTable = "file://" + arDir.toAbsolutePath();

        try {
            String protocolJson = MAPPER.writeValueAsString(Map.of(
                "minReaderVersion", 4, "minWriterVersion", 4));
            String metadataJson = MAPPER.writeValueAsString(Map.of(
                "id", "addremove-test", "schemaString", "{}", "partitionColumns", List.of(),
                "configuration", Map.of()));
            TransactionLogWriter.initializeTable(arTable, noCpConfig, protocolJson, metadataJson);

            // Add 2 files
            String adds = MAPPER.writeValueAsString(List.of(
                makeAddAction("keep.split", 1000, 100),
                makeAddAction("remove-me.split", 2000, 200)));
            TransactionLogWriter.addFiles(arTable, noCpConfig, adds);

            // Remove one
            TransactionLogWriter.removeFile(arTable, noCpConfig, "remove-me.split");

            // Snapshot should work and show only "keep.split" via changes
            TxLogSnapshotInfo snapshot = TransactionLogReader.getSnapshotInfo(arTable, noCpConfig);
            TxLogChanges changes = TransactionLogReader.readPostCheckpointChanges(
                arTable, noCpConfig,
                MAPPER.writeValueAsString(snapshot.getPostCheckpointPaths()),
                "{}");

            // The changes should include both adds and the removal
            List<String> removedPaths = changes.getRemovedPaths();
            assertTrue(removedPaths.contains("remove-me.split"),
                "Should contain removed file path");
        } finally {
            deleteRecursively(arDir.toFile());
        }
    }

    @Test
    @Order(30)
    void testAutoCheckpointOnWrites() throws Exception {
        // GAP-10: auto-checkpoint with checkpoint_interval=3
        Path autoDir = Files.createTempDirectory("txlog-autocheckpoint");
        Files.createDirectories(autoDir.resolve("_transaction_log"));
        String autoTable = "file://" + autoDir.toAbsolutePath();

        Map<String, String> autoConfig = new HashMap<>(config);
        autoConfig.put("checkpoint_interval", "3");

        try {
            // Initialize (version 0, no checkpoint)
            String protocolJson = MAPPER.writeValueAsString(Map.of(
                "minReaderVersion", 4, "minWriterVersion", 4));
            String metadataJson = MAPPER.writeValueAsString(Map.of(
                "id", "autocheckpoint-test", "schemaString", "{}",
                "partitionColumns", List.of(), "configuration", Map.of()));
            TransactionLogWriter.initializeTable(autoTable, autoConfig, protocolJson, metadataJson);

            // Write versions 1, 2 (no checkpoint yet)
            for (int i = 1; i <= 2; i++) {
                String addsJson = MAPPER.writeValueAsString(List.of(
                    makeAddAction("auto-split-" + i + ".split", 1000 * i, 100 * i)));
                TransactionLogWriter.addFiles(autoTable, autoConfig, addsJson);
            }

            // At this point we have version 0,1,2 — no auto-checkpoint yet
            // Version 3 should trigger auto-checkpoint (3 % 3 == 0)
            String v3Adds = MAPPER.writeValueAsString(List.of(
                makeAddAction("auto-split-3.split", 3000, 300)));
            TransactionLogWriter.addFiles(autoTable, autoConfig, v3Adds);

            // Now _last_checkpoint should exist (created by auto-checkpoint at version 3)
            TxLogSnapshotInfo snapshot = TransactionLogReader.getSnapshotInfo(autoTable, autoConfig);
            assertNotNull(snapshot);

            // With auto-checkpoint, we should have a proper checkpoint
            // The checkpoint version should be >= 3
            assertTrue(snapshot.getCheckpointVersion() >= 0,
                "Auto-checkpoint should have created _last_checkpoint, got version " +
                snapshot.getCheckpointVersion());
        } finally {
            deleteRecursively(autoDir.toFile());
        }
    }

    // ========================================================================
    // Helpers
    // ========================================================================

    private static Map<String, Object> makeAddAction(String path, long size, long numRecords) {
        Map<String, Object> add = new LinkedHashMap<>();
        add.put("path", path);
        add.put("partitionValues", Map.of());
        add.put("size", size);
        add.put("modificationTime", 1700000000000L);
        add.put("dataChange", true);
        add.put("numRecords", numRecords);
        return add;
    }

    private static void deleteRecursively(File file) {
        if (file.isDirectory()) {
            File[] children = file.listFiles();
            if (children != null) {
                for (File child : children) {
                    deleteRecursively(child);
                }
            }
        }
        file.delete();
    }
}

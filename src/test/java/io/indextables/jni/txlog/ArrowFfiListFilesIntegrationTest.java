package io.indextables.jni.txlog;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.indextables.tantivy4java.filter.PartitionFilter;
import org.junit.jupiter.api.*;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Integration tests for FR1 (nativeListFilesArrowFfi) exercising the full pipeline:
 * Java → JNI → Rust list_files_arrow_ffi → Arrow FFI (Rust-side alloc) → read back → JSON.
 *
 * Uses nativeTestListFilesRoundtrip which allocates Arrow FFI memory on the Rust side,
 * avoiding any Arrow Java dependency.
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class ArrowFfiListFilesIntegrationTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static Path tempDir;
    private static String tablePath;
    private static Map<String, String> config;

    @BeforeAll
    static void setUp() throws Exception {
        tempDir = Files.createTempDirectory("txlog-arrowffi-test");
        Files.createDirectories(tempDir.resolve("_transaction_log"));
        tablePath = "file://" + tempDir.toAbsolutePath();
        config = Collections.emptyMap();

        // Initialize table with protocol + metadata (partitioned by "year")
        String protocolJson = MAPPER.writeValueAsString(Map.of(
            "minReaderVersion", 4, "minWriterVersion", 4));
        String metadataJson = MAPPER.writeValueAsString(Map.of(
            "id", "arrow-ffi-test",
            "schemaString", "{}",
            "partitionColumns", List.of("year"),
            "format", Map.of("provider", "parquet"),
            "configuration", Map.of()));
        TransactionLogWriter.initializeTable(tablePath, config, protocolJson, metadataJson);

        // Write files with partition values and min/max stats
        List<Map<String, Object>> adds = new ArrayList<>();

        Map<String, Object> add1 = makeAdd("year=2023/split-a.split", 1000, 50);
        add1.put("partitionValues", Map.of("year", "2023"));
        add1.put("minValues", Map.of("price", "10"));
        add1.put("maxValues", Map.of("price", "50"));
        add1.put("footerStartOffset", 1024L);
        add1.put("footerEndOffset", 2048L);
        add1.put("hasFooterOffsets", true);
        adds.add(add1);

        Map<String, Object> add2 = makeAdd("year=2024/split-b.split", 2000, 100);
        add2.put("partitionValues", Map.of("year", "2024"));
        add2.put("minValues", Map.of("price", "100"));
        add2.put("maxValues", Map.of("price", "500"));
        add2.put("footerStartOffset", 3000L);
        add2.put("footerEndOffset", 5000L);
        add2.put("hasFooterOffsets", true);
        adds.add(add2);

        Map<String, Object> add3 = makeAdd("year=2024/split-c.split", 3000, 200);
        add3.put("partitionValues", Map.of("year", "2024"));
        add3.put("minValues", Map.of("price", "200"));
        add3.put("maxValues", Map.of("price", "300"));
        add3.put("footerStartOffset", 6000L);
        add3.put("footerEndOffset", 9000L);
        add3.put("hasFooterOffsets", true);
        adds.add(add3);

        TransactionLogWriter.addFiles(tablePath, config, MAPPER.writeValueAsString(adds));

        // Create checkpoint so manifests exist
        String entriesJson = MAPPER.writeValueAsString(adds);
        TransactionLogWriter.createCheckpoint(tablePath, config, entriesJson, metadataJson, protocolJson);
    }

    @AfterAll
    static void tearDown() {
        if (tempDir != null) {
            deleteRecursively(tempDir.toFile());
        }
    }

    // ========================================================================
    // Test: List all files (no filter)
    // ========================================================================

    @Test
    @Order(1)
    @DisplayName("List all files without any filters")
    void testListAllFiles() throws Exception {
        String result = TransactionLogReader.nativeTestListFilesRoundtrip(
            tablePath, config, null, null, 0);

        assertNotNull(result, "Result should not be null");
        JsonNode json = MAPPER.readTree(result);

        assertEquals(3, json.get("numRows").asLong(), "Should list all 3 files");

        List<String> paths = new ArrayList<>();
        for (JsonNode p : json.get("paths")) {
            paths.add(p.asText());
        }
        Collections.sort(paths);
        assertEquals("year=2023/split-a.split", paths.get(0));
        assertEquals("year=2024/split-b.split", paths.get(1));
        assertEquals("year=2024/split-c.split", paths.get(2));

        // Partition columns should be returned
        assertTrue(json.has("partitionColumns"));
        assertEquals(1, json.get("partitionColumns").size());
        assertEquals("year", json.get("partitionColumns").get(0).asText());

        // Metrics
        JsonNode metrics = json.get("metrics");
        assertEquals(3, metrics.get("totalFilesBeforeFiltering").asLong());
        assertEquals(3, metrics.get("filesAfterPartitionPruning").asLong());
    }

    // ========================================================================
    // Test: Partition filter
    // ========================================================================

    @Test
    @Order(2)
    @DisplayName("Partition filter: year = 2024")
    void testPartitionFilter() throws Exception {
        String filterJson = PartitionFilter.eq("year", "2024").toJson();

        String result = TransactionLogReader.nativeTestListFilesRoundtrip(
            tablePath, config, filterJson, null, 0);

        JsonNode json = MAPPER.readTree(result);
        assertEquals(2, json.get("numRows").asLong(), "Should match 2 files for year=2024");

        List<String> paths = new ArrayList<>();
        for (JsonNode p : json.get("paths")) {
            paths.add(p.asText());
        }
        Collections.sort(paths);
        assertEquals("year=2024/split-b.split", paths.get(0));
        assertEquals("year=2024/split-c.split", paths.get(1));

        // Metrics should show filtering
        JsonNode metrics = json.get("metrics");
        assertEquals(3, metrics.get("totalFilesBeforeFiltering").asLong());
        assertEquals(2, metrics.get("filesAfterPartitionPruning").asLong());
    }

    // ========================================================================
    // Test: Data skipping with min/max stats
    // ========================================================================

    @Test
    @Order(3)
    @DisplayName("Data skip filter: price > 150 (skips split-a with max=50)")
    void testDataSkippingFilter() throws Exception {
        // Data filter: price > 150 — should skip split-a (max=50) and split-c (max=300 > 150 → keep)
        String dataFilterJson = PartitionFilter.gt("price", "150").toJson();

        String result = TransactionLogReader.nativeTestListFilesRoundtrip(
            tablePath, config, null, dataFilterJson, 0);

        JsonNode json = MAPPER.readTree(result);
        // split-a: max(price)=50, 50 not > 150 → SKIP
        // split-b: max(price)=500, 500 > 150 → keep
        // split-c: max(price)=300, 300 > 150 → keep
        assertEquals(2, json.get("numRows").asLong(), "Should keep 2 files after data skipping");

        List<String> paths = new ArrayList<>();
        for (JsonNode p : json.get("paths")) {
            paths.add(p.asText());
        }
        assertFalse(paths.contains("year=2023/split-a.split"), "split-a should be skipped");
        assertTrue(paths.contains("year=2024/split-b.split"), "split-b should be kept");
        assertTrue(paths.contains("year=2024/split-c.split"), "split-c should be kept");

        JsonNode metrics = json.get("metrics");
        assertEquals(3, metrics.get("totalFilesBeforeFiltering").asLong());
        assertEquals(2, metrics.get("filesAfterDataSkipping").asLong());
    }

    // ========================================================================
    // Test: Combined partition + data skipping
    // ========================================================================

    @Test
    @Order(4)
    @DisplayName("Combined: year=2024 AND price > 250 (only split-b remains)")
    void testCombinedFilters() throws Exception {
        String partFilterJson = PartitionFilter.eq("year", "2024").toJson();
        // price > 250 → skip split-c (max=300 > 250 → keep), split-b (max=500 > 250 → keep)
        // Actually: price > 250 → skip files where max(price) <= 250
        // split-b max=500 > 250 → keep; split-c max=300 > 250 → keep
        // Let's use price > 400 instead to get split-b only
        String dataFilterJson = PartitionFilter.gt("price", "400").toJson();

        String result = TransactionLogReader.nativeTestListFilesRoundtrip(
            tablePath, config, partFilterJson, dataFilterJson, 0);

        JsonNode json = MAPPER.readTree(result);
        // Partition: keeps split-b, split-c (year=2024)
        // Data skip: price > 400 → max(price) must be > 400
        //   split-b max=500 > 400 → keep
        //   split-c max=300, 300 not > 400 → SKIP
        assertEquals(1, json.get("numRows").asLong(), "Should keep 1 file after combined filtering");
        assertEquals("year=2024/split-b.split", json.get("paths").get(0).asText());

        JsonNode metrics = json.get("metrics");
        assertEquals(3, metrics.get("totalFilesBeforeFiltering").asLong());
        assertEquals(2, metrics.get("filesAfterPartitionPruning").asLong());
        assertEquals(1, metrics.get("filesAfterDataSkipping").asLong());
    }

    // ========================================================================
    // Test: No matching files
    // ========================================================================

    @Test
    @Order(5)
    @DisplayName("Partition filter with no matches: year=2025")
    void testNoMatchingFiles() throws Exception {
        String filterJson = PartitionFilter.eq("year", "2025").toJson();

        String result = TransactionLogReader.nativeTestListFilesRoundtrip(
            tablePath, config, filterJson, null, 0);

        JsonNode json = MAPPER.readTree(result);
        assertEquals(0, json.get("numRows").asLong(), "No files should match year=2025");
        assertEquals(0, json.get("paths").size());
    }

    // ========================================================================
    // Test: Arrow schema includes partition columns
    // ========================================================================

    @Test
    @Order(6)
    @DisplayName("Column count includes dynamic partition columns")
    void testColumnCount() throws Exception {
        String result = TransactionLogReader.nativeTestListFilesRoundtrip(
            tablePath, config, null, null, 0);

        JsonNode json = MAPPER.readTree(result);
        // 19 base + 1 partition column ("year") = 20
        assertEquals(20, json.get("numColumns").asInt(), "Should have 20 columns (19 base + 1 partition)");
    }

    // ========================================================================
    // Test: Footer offsets survive Arrow FFI round-trip
    // ========================================================================

    @Test
    @Order(7)
    @DisplayName("Footer offsets are correctly exported via Arrow FFI")
    void testFooterOffsetsRoundtrip() throws Exception {
        String result = TransactionLogReader.nativeTestListFilesRoundtrip(
            tablePath, config, null, null, 0);

        JsonNode json = MAPPER.readTree(result);
        assertEquals(3, json.get("numRows").asLong());

        // Verify footer offsets are present and non-zero
        JsonNode footerStarts = json.get("footerStartOffsets");
        JsonNode footerEnds = json.get("footerEndOffsets");
        JsonNode numRecords = json.get("numRecords");

        assertNotNull(footerStarts, "footerStartOffsets should be in result JSON");
        assertNotNull(footerEnds, "footerEndOffsets should be in result JSON");
        assertNotNull(numRecords, "numRecords should be in result JSON");

        assertEquals(3, footerStarts.size());
        assertEquals(3, footerEnds.size());

        // All entries should have non-null, non-zero footer offsets
        for (int i = 0; i < 3; i++) {
            assertFalse(footerStarts.get(i).isNull(),
                "footerStartOffset should not be null for entry " + i);
            assertFalse(footerEnds.get(i).isNull(),
                "footerEndOffset should not be null for entry " + i);
            assertTrue(footerStarts.get(i).asLong() > 0,
                "footerStartOffset should be > 0 for entry " + i + ", got: " + footerStarts.get(i));
            assertTrue(footerEnds.get(i).asLong() > 0,
                "footerEndOffset should be > 0 for entry " + i + ", got: " + footerEnds.get(i));
            assertTrue(footerEnds.get(i).asLong() > footerStarts.get(i).asLong(),
                "footerEndOffset should be > footerStartOffset for entry " + i);
        }

        // Verify specific values match what we wrote
        // Entries are sorted by path, so order is: split-a (1024/2048), split-b (3000/5000), split-c (6000/9000)
        List<String> paths = new ArrayList<>();
        for (JsonNode p : json.get("paths")) paths.add(p.asText());

        int aIdx = paths.indexOf("year=2023/split-a.split");
        int bIdx = paths.indexOf("year=2024/split-b.split");
        int cIdx = paths.indexOf("year=2024/split-c.split");

        assertEquals(1024L, footerStarts.get(aIdx).asLong(), "split-a footerStart");
        assertEquals(2048L, footerEnds.get(aIdx).asLong(), "split-a footerEnd");
        assertEquals(3000L, footerStarts.get(bIdx).asLong(), "split-b footerStart");
        assertEquals(5000L, footerEnds.get(bIdx).asLong(), "split-b footerEnd");
        assertEquals(6000L, footerStarts.get(cIdx).asLong(), "split-c footerStart");
        assertEquals(9000L, footerEnds.get(cIdx).asLong(), "split-c footerEnd");

        // Verify numRecords too
        assertEquals(50, numRecords.get(aIdx).asLong(), "split-a numRecords");
        assertEquals(100, numRecords.get(bIdx).asLong(), "split-b numRecords");
        assertEquals(200, numRecords.get(cIdx).asLong(), "split-c numRecords");
    }

    // ========================================================================
    // Helpers
    // ========================================================================

    private static Map<String, Object> makeAdd(String path, long size, long numRecords) {
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

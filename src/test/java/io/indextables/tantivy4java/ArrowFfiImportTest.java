package io.indextables.tantivy4java;

import io.indextables.tantivy4java.core.*;
import io.indextables.tantivy4java.result.SearchResult;
import io.indextables.tantivy4java.split.*;
import io.indextables.tantivy4java.split.merge.*;

import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Integration tests for Arrow FFI import path — creating Quickwit splits from
 * Arrow columnar batches via the Arrow C Data Interface.
 *
 * Tests the begin/addBatch/finish/cancel lifecycle exposed through:
 *   QuickwitSplit.beginSplitFromArrow()
 *   QuickwitSplit.addArrowBatch()
 *   QuickwitSplit.finishAllSplits()
 *   QuickwitSplit.cancelSplit()
 *
 * Uses Rust-side test helpers (nativeTestExport*Ffi) to create valid FFI structs
 * with proper release callbacks, since tantivy4java has zero Arrow Java dependencies.
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class ArrowFfiImportTest {

    private static SplitCacheManager cacheManager;

    @TempDir
    Path tempDir;

    @BeforeAll
    static void setUp() {
        SplitCacheManager.CacheConfig config = new SplitCacheManager.CacheConfig("arrow-ffi-import-test")
                .withMaxCacheSize(100_000_000);
        cacheManager = SplitCacheManager.getInstance(config);
    }

    @AfterAll
    static void tearDown() {
        if (cacheManager != null) {
            try {
                cacheManager.close();
            } catch (Exception e) {
                // ignore
            }
        }
    }

    // ====================================================================
    // Non-partitioned tests
    // ====================================================================

    @Test
    @Order(1)
    void testNonPartitionedSingleBatch() throws Exception {
        // Schema: id:Int64, name:Utf8, score:Float64, active:Boolean
        long schemaAddr = QuickwitSplit.nativeTestExportSchemaFfi(
                new String[]{"id", "name", "score", "active"},
                new String[]{"int64", "utf8", "float64", "boolean"});
        assertTrue(schemaAddr != 0, "Schema FFI export should return non-zero address");

        // Begin split (non-partitioned)
        long handle = QuickwitSplit.beginSplitFromArrow(schemaAddr, new String[]{},
                Index.Memory.DEFAULT_HEAP_SIZE);
        assertTrue(handle != 0, "beginSplitFromArrow should return non-zero handle");

        // Add a batch of 10 rows
        long[] batchAddrs = QuickwitSplit.nativeTestExportStandardBatchFfi(10, 0);
        assertNotNull(batchAddrs);
        assertEquals(2, batchAddrs.length);

        long docCount = QuickwitSplit.addArrowBatch(handle, batchAddrs[0], batchAddrs[1]);
        assertEquals(10, docCount, "Should have 10 docs after first batch");

        // Finish
        String outputDir = tempDir.resolve("single_batch").toString();
        Files.createDirectories(Path.of(outputDir));
        List<QuickwitSplit.PartitionSplitResult> results = QuickwitSplit.finishAllSplits(handle, outputDir);

        // Verify results
        assertNotNull(results);
        assertEquals(1, results.size(), "Non-partitioned should produce exactly 1 split");

        QuickwitSplit.PartitionSplitResult result = results.get(0);
        assertEquals("", result.getPartitionKey(), "Non-partitioned should have empty partition key");
        assertEquals(10, result.getNumDocs(), "Split should contain 10 docs");
        assertTrue(result.getSplitPath().endsWith(".split"), "Split path should end with .split");
        assertTrue(Files.exists(Path.of(result.getSplitPath())), "Split file should exist");
        assertNotNull(result.getSplitId(), "Split ID should not be null");
        assertTrue(result.getFooterStartOffset() > 0, "Footer start offset should be positive");
        assertTrue(result.getFooterEndOffset() > result.getFooterStartOffset(),
                "Footer end offset should be after start offset");

        System.out.println("testNonPartitionedSingleBatch PASSED: " + result);
    }

    @Test
    @Order(2)
    void testNonPartitionedMultipleBatches() throws Exception {
        long schemaAddr = QuickwitSplit.nativeTestExportSchemaFfi(
                new String[]{"id", "name", "score", "active"},
                new String[]{"int64", "utf8", "float64", "boolean"});

        long handle = QuickwitSplit.beginSplitFromArrow(schemaAddr, new String[]{},
                Index.Memory.DEFAULT_HEAP_SIZE);

        // Add 3 batches of 100 rows each
        for (int i = 0; i < 3; i++) {
            long[] batchAddrs = QuickwitSplit.nativeTestExportStandardBatchFfi(100, i * 100);
            long docCount = QuickwitSplit.addArrowBatch(handle, batchAddrs[0], batchAddrs[1]);
            assertEquals((i + 1) * 100, docCount,
                    "Cumulative doc count should be " + ((i + 1) * 100) + " after batch " + (i + 1));
        }

        String outputDir = tempDir.resolve("multi_batch").toString();
        Files.createDirectories(Path.of(outputDir));
        List<QuickwitSplit.PartitionSplitResult> results = QuickwitSplit.finishAllSplits(handle, outputDir);

        assertEquals(1, results.size());
        assertEquals(300, results.get(0).getNumDocs(), "Split should contain 300 docs total");

        System.out.println("testNonPartitionedMultipleBatches PASSED: 300 docs in 1 split");
    }

    @Test
    @Order(3)
    void testNonPartitionedSplitSearchable() throws Exception {
        long schemaAddr = QuickwitSplit.nativeTestExportSchemaFfi(
                new String[]{"id", "name", "score", "active"},
                new String[]{"int64", "utf8", "float64", "boolean"});

        long handle = QuickwitSplit.beginSplitFromArrow(schemaAddr, new String[]{},
                Index.Memory.DEFAULT_HEAP_SIZE);

        // Add 50 rows
        long[] batchAddrs = QuickwitSplit.nativeTestExportStandardBatchFfi(50, 0);
        QuickwitSplit.addArrowBatch(handle, batchAddrs[0], batchAddrs[1]);

        String outputDir = tempDir.resolve("searchable").toString();
        Files.createDirectories(Path.of(outputDir));
        List<QuickwitSplit.PartitionSplitResult> results = QuickwitSplit.finishAllSplits(handle, outputDir);

        assertEquals(1, results.size());
        QuickwitSplit.PartitionSplitResult result = results.get(0);

        // Open the split with SplitSearcher and verify searchability
        QuickwitSplit.SplitMetadata metadata = new QuickwitSplit.SplitMetadata(
                result.getFooterStartOffset(), result.getFooterEndOffset(), 0, 0);

        String splitUri = "file://" + result.getSplitPath();
        try (SplitSearcher searcher = cacheManager.createSplitSearcher(splitUri, metadata)) {
            // Verify schema
            Schema schema = searcher.getSchema();
            assertNotNull(schema);
            assertTrue(schema.getFieldNames().contains("id"), "Schema should have 'id' field");
            assertTrue(schema.getFieldNames().contains("name"), "Schema should have 'name' field");
            assertTrue(schema.getFieldNames().contains("score"), "Schema should have 'score' field");

            // Search for all documents
            SplitQuery allQuery = searcher.parseQuery("*");
            SearchResult allResults = searcher.search(allQuery, 100);
            assertEquals(50, allResults.getHits().size(), "Should find all 50 documents");

            // Search for specific document by name
            SplitQuery nameQuery = new SplitTermQuery("name", "name_0");
            SearchResult nameResults = searcher.search(nameQuery, 10);
            assertTrue(nameResults.getHits().size() > 0,
                    "Should find document with name 'name_0'");

            // Verify document retrieval
            for (SearchResult.Hit hit : nameResults.getHits()) {
                try (Document doc = searcher.doc(hit.getDocAddress())) {
                    assertNotNull(doc, "Document should not be null");
                    // Verify stored fields are accessible
                    Object nameVal = doc.getFirst("name");
                    assertNotNull(nameVal, "name field should be stored and retrievable");
                }
            }
        }

        System.out.println("testNonPartitionedSplitSearchable PASSED: split searchable with SplitSearcher");
    }

    @Test
    @Order(4)
    void testCancelCleansUp() throws Exception {
        long schemaAddr = QuickwitSplit.nativeTestExportSchemaFfi(
                new String[]{"id", "name", "score", "active"},
                new String[]{"int64", "utf8", "float64", "boolean"});

        long handle = QuickwitSplit.beginSplitFromArrow(schemaAddr, new String[]{},
                Index.Memory.DEFAULT_HEAP_SIZE);

        // Add some data
        long[] batchAddrs = QuickwitSplit.nativeTestExportStandardBatchFfi(10, 0);
        QuickwitSplit.addArrowBatch(handle, batchAddrs[0], batchAddrs[1]);

        // Cancel — should not throw
        assertDoesNotThrow(() -> QuickwitSplit.cancelSplit(handle),
                "cancelSplit should not throw");

        System.out.println("testCancelCleansUp PASSED");
    }

    @Test
    @Order(5)
    void testEmptyBatch() throws Exception {
        long schemaAddr = QuickwitSplit.nativeTestExportSchemaFfi(
                new String[]{"id", "name", "score", "active"},
                new String[]{"int64", "utf8", "float64", "boolean"});

        long handle = QuickwitSplit.beginSplitFromArrow(schemaAddr, new String[]{},
                Index.Memory.DEFAULT_HEAP_SIZE);

        // Add a batch of 0 rows
        long[] batchAddrs = QuickwitSplit.nativeTestExportStandardBatchFfi(0, 0);
        long docCount = QuickwitSplit.addArrowBatch(handle, batchAddrs[0], batchAddrs[1]);
        assertEquals(0, docCount, "Empty batch should have 0 docs");

        // Finish with 0 docs — should still create a valid split
        String outputDir = tempDir.resolve("empty_batch").toString();
        Files.createDirectories(Path.of(outputDir));
        List<QuickwitSplit.PartitionSplitResult> results = QuickwitSplit.finishAllSplits(handle, outputDir);

        assertEquals(1, results.size());
        assertEquals(0, results.get(0).getNumDocs(), "Empty split should have 0 docs");

        System.out.println("testEmptyBatch PASSED: empty split created successfully");
    }

    // ====================================================================
    // Partitioned tests
    // ====================================================================

    @Test
    @Order(10)
    void testPartitionedSinglePartition() throws Exception {
        // Schema: id:Int64, name:Utf8, event_date:Utf8 (partition column)
        long schemaAddr = QuickwitSplit.nativeTestExportSchemaFfi(
                new String[]{"id", "name", "event_date"},
                new String[]{"int64", "utf8", "utf8"});

        long handle = QuickwitSplit.beginSplitFromArrow(schemaAddr,
                new String[]{"event_date"}, Index.Memory.DEFAULT_HEAP_SIZE);

        // All rows have the same partition value
        long[] batchAddrs = QuickwitSplit.nativeTestExportPartitionBatchFfi(
                new long[]{1, 2, 3},
                new String[]{"alice", "bob", "charlie"},
                "event_date",
                new String[]{"2023-01-15", "2023-01-15", "2023-01-15"});
        QuickwitSplit.addArrowBatch(handle, batchAddrs[0], batchAddrs[1]);

        String outputDir = tempDir.resolve("single_partition").toString();
        Files.createDirectories(Path.of(outputDir));
        List<QuickwitSplit.PartitionSplitResult> results = QuickwitSplit.finishAllSplits(handle, outputDir);

        assertEquals(1, results.size(), "Single partition value should produce 1 split");

        QuickwitSplit.PartitionSplitResult result = results.get(0);
        assertEquals("event_date=2023-01-15", result.getPartitionKey());
        assertEquals(3, result.getNumDocs());
        assertEquals("2023-01-15", result.getPartitionValues().get("event_date"));
        assertTrue(Files.exists(Path.of(result.getSplitPath())));

        System.out.println("testPartitionedSinglePartition PASSED: " + result);
    }

    @Test
    @Order(11)
    void testPartitionedMultiplePartitions() throws Exception {
        long schemaAddr = QuickwitSplit.nativeTestExportSchemaFfi(
                new String[]{"id", "name", "event_date"},
                new String[]{"int64", "utf8", "utf8"});

        long handle = QuickwitSplit.beginSplitFromArrow(schemaAddr,
                new String[]{"event_date"}, Index.Memory.DEFAULT_HEAP_SIZE);

        // 6 rows with 3 distinct partition values (2 rows each)
        long[] batchAddrs = QuickwitSplit.nativeTestExportPartitionBatchFfi(
                new long[]{1, 2, 3, 4, 5, 6},
                new String[]{"a", "b", "c", "d", "e", "f"},
                "event_date",
                new String[]{"2023-01-15", "2023-01-16", "2023-01-17",
                             "2023-01-15", "2023-01-16", "2023-01-17"});
        QuickwitSplit.addArrowBatch(handle, batchAddrs[0], batchAddrs[1]);

        String outputDir = tempDir.resolve("multi_partition").toString();
        Files.createDirectories(Path.of(outputDir));
        List<QuickwitSplit.PartitionSplitResult> results = QuickwitSplit.finishAllSplits(handle, outputDir);

        assertEquals(3, results.size(), "3 distinct partition values should produce 3 splits");

        // Verify each partition has 2 docs
        Map<String, Long> docCounts = new HashMap<>();
        for (QuickwitSplit.PartitionSplitResult r : results) {
            docCounts.put(r.getPartitionKey(), r.getNumDocs());
            assertTrue(Files.exists(Path.of(r.getSplitPath())),
                    "Split file should exist for partition: " + r.getPartitionKey());
        }
        assertEquals(2, docCounts.get("event_date=2023-01-15"));
        assertEquals(2, docCounts.get("event_date=2023-01-16"));
        assertEquals(2, docCounts.get("event_date=2023-01-17"));

        System.out.println("testPartitionedMultiplePartitions PASSED: 3 partitions, 2 docs each");
    }

    @Test
    @Order(12)
    void testPartitionedAcrossBatches() throws Exception {
        long schemaAddr = QuickwitSplit.nativeTestExportSchemaFfi(
                new String[]{"id", "name", "event_date"},
                new String[]{"int64", "utf8", "utf8"});

        long handle = QuickwitSplit.beginSplitFromArrow(schemaAddr,
                new String[]{"event_date"}, Index.Memory.DEFAULT_HEAP_SIZE);

        // Batch 1: partition A rows
        long[] batch1 = QuickwitSplit.nativeTestExportPartitionBatchFfi(
                new long[]{1, 2},
                new String[]{"a", "b"},
                "event_date",
                new String[]{"2023-01-15", "2023-01-15"});
        QuickwitSplit.addArrowBatch(handle, batch1[0], batch1[1]);

        // Batch 2: partition B rows
        long[] batch2 = QuickwitSplit.nativeTestExportPartitionBatchFfi(
                new long[]{3, 4},
                new String[]{"c", "d"},
                "event_date",
                new String[]{"2023-01-16", "2023-01-16"});
        QuickwitSplit.addArrowBatch(handle, batch2[0], batch2[1]);

        // Batch 3: more partition A rows
        long[] batch3 = QuickwitSplit.nativeTestExportPartitionBatchFfi(
                new long[]{5},
                new String[]{"e"},
                "event_date",
                new String[]{"2023-01-15"});
        QuickwitSplit.addArrowBatch(handle, batch3[0], batch3[1]);

        String outputDir = tempDir.resolve("across_batches").toString();
        Files.createDirectories(Path.of(outputDir));
        List<QuickwitSplit.PartitionSplitResult> results = QuickwitSplit.finishAllSplits(handle, outputDir);

        assertEquals(2, results.size(), "Should produce 2 partitions");

        Map<String, Long> docCounts = new HashMap<>();
        for (QuickwitSplit.PartitionSplitResult r : results) {
            docCounts.put(r.getPartitionKey(), r.getNumDocs());
        }
        // Partition A (2023-01-15): 2 from batch 1 + 1 from batch 3 = 3
        assertEquals(3, docCounts.get("event_date=2023-01-15"),
                "Partition A should accumulate docs across batches 1 and 3");
        // Partition B (2023-01-16): 2 from batch 2
        assertEquals(2, docCounts.get("event_date=2023-01-16"));

        System.out.println("testPartitionedAcrossBatches PASSED: partition A accumulated across batches");
    }

    @Test
    @Order(13)
    void testPartitionedSplitSearchable() throws Exception {
        long schemaAddr = QuickwitSplit.nativeTestExportSchemaFfi(
                new String[]{"id", "name", "event_date"},
                new String[]{"int64", "utf8", "utf8"});

        long handle = QuickwitSplit.beginSplitFromArrow(schemaAddr,
                new String[]{"event_date"}, Index.Memory.DEFAULT_HEAP_SIZE);

        long[] batchAddrs = QuickwitSplit.nativeTestExportPartitionBatchFfi(
                new long[]{100, 200, 300},
                new String[]{"alice", "bob", "charlie"},
                "event_date",
                new String[]{"2023-01-15", "2023-01-16", "2023-01-15"});
        QuickwitSplit.addArrowBatch(handle, batchAddrs[0], batchAddrs[1]);

        String outputDir = tempDir.resolve("partition_searchable").toString();
        Files.createDirectories(Path.of(outputDir));
        List<QuickwitSplit.PartitionSplitResult> results = QuickwitSplit.finishAllSplits(handle, outputDir);

        assertEquals(2, results.size());

        // Find the partition with 2 docs (2023-01-15)
        QuickwitSplit.PartitionSplitResult partition15 = results.stream()
                .filter(r -> r.getPartitionKey().equals("event_date=2023-01-15"))
                .findFirst().orElseThrow();

        assertEquals(2, partition15.getNumDocs());

        // Open partition's split with SplitSearcher
        QuickwitSplit.SplitMetadata metadata = new QuickwitSplit.SplitMetadata(
                partition15.getFooterStartOffset(), partition15.getFooterEndOffset(), 0, 0);
        String splitUri = "file://" + partition15.getSplitPath();

        try (SplitSearcher searcher = cacheManager.createSplitSearcher(splitUri, metadata)) {
            // Verify partition column is NOT in the index
            Schema schema = searcher.getSchema();
            assertFalse(schema.getFieldNames().contains("event_date"),
                    "Partition column 'event_date' should NOT be in the tantivy index");
            assertTrue(schema.getFieldNames().contains("id"), "Data column 'id' should be in index");
            assertTrue(schema.getFieldNames().contains("name"), "Data column 'name' should be in index");

            // Search all docs in this partition
            SplitQuery allQuery = searcher.parseQuery("*");
            SearchResult allResults = searcher.search(allQuery, 10);
            assertEquals(2, allResults.getHits().size(),
                    "Partition 2023-01-15 should have exactly 2 docs");

            // Search for specific name
            SplitQuery nameQuery = new SplitTermQuery("name", "alice");
            SearchResult nameResults = searcher.search(nameQuery, 10);
            assertTrue(nameResults.getHits().size() > 0,
                    "Should find 'alice' in partition 2023-01-15");
        }

        System.out.println("testPartitionedSplitSearchable PASSED");
    }

    @Test
    @Order(14)
    void testPartitionedCancel() throws Exception {
        long schemaAddr = QuickwitSplit.nativeTestExportSchemaFfi(
                new String[]{"id", "name", "event_date"},
                new String[]{"int64", "utf8", "utf8"});

        long handle = QuickwitSplit.beginSplitFromArrow(schemaAddr,
                new String[]{"event_date"}, Index.Memory.DEFAULT_HEAP_SIZE);

        long[] batchAddrs = QuickwitSplit.nativeTestExportPartitionBatchFfi(
                new long[]{1, 2, 3},
                new String[]{"a", "b", "c"},
                "event_date",
                new String[]{"2023-01-15", "2023-01-16", "2023-01-17"});
        QuickwitSplit.addArrowBatch(handle, batchAddrs[0], batchAddrs[1]);

        // Cancel should not throw even with active partition writers
        assertDoesNotThrow(() -> QuickwitSplit.cancelSplit(handle),
                "cancelSplit should not throw with active partitions");

        System.out.println("testPartitionedCancel PASSED");
    }

    // ====================================================================
    // Metadata and lifecycle tests
    // ====================================================================

    @Test
    @Order(20)
    void testPartitionSplitResultMetadata() throws Exception {
        long schemaAddr = QuickwitSplit.nativeTestExportSchemaFfi(
                new String[]{"id", "name", "score", "active"},
                new String[]{"int64", "utf8", "float64", "boolean"});

        long handle = QuickwitSplit.beginSplitFromArrow(schemaAddr, new String[]{},
                Index.Memory.DEFAULT_HEAP_SIZE);

        long[] batchAddrs = QuickwitSplit.nativeTestExportStandardBatchFfi(25, 0);
        QuickwitSplit.addArrowBatch(handle, batchAddrs[0], batchAddrs[1]);

        String outputDir = tempDir.resolve("metadata_test").toString();
        Files.createDirectories(Path.of(outputDir));
        List<QuickwitSplit.PartitionSplitResult> results = QuickwitSplit.finishAllSplits(handle, outputDir);

        assertEquals(1, results.size());
        QuickwitSplit.PartitionSplitResult result = results.get(0);

        // Verify all metadata fields
        assertNotNull(result.getSplitId(), "splitId should not be null");
        assertFalse(result.getSplitId().isEmpty(), "splitId should not be empty");
        assertEquals(25, result.getNumDocs());
        assertTrue(result.getFooterStartOffset() > 0);
        assertTrue(result.getFooterEndOffset() > result.getFooterStartOffset());
        assertEquals("", result.getPartitionKey(), "Non-partitioned partition key should be empty");
        assertTrue(result.getPartitionValues().isEmpty(), "Non-partitioned should have empty partition values");

        // Verify split file size is reasonable
        long fileSize = Files.size(Path.of(result.getSplitPath()));
        assertTrue(fileSize > 0, "Split file should have non-zero size");
        assertTrue(fileSize >= result.getFooterEndOffset(),
                "File size should be at least as large as footer end offset");

        System.out.println("testPartitionSplitResultMetadata PASSED: splitId=" + result.getSplitId() +
                ", fileSize=" + fileSize);
    }

    @Test
    @Order(21)
    void testLargerBatch() throws Exception {
        long schemaAddr = QuickwitSplit.nativeTestExportSchemaFfi(
                new String[]{"id", "name", "score", "active"},
                new String[]{"int64", "utf8", "float64", "boolean"});

        long handle = QuickwitSplit.beginSplitFromArrow(schemaAddr, new String[]{},
                Index.Memory.DEFAULT_HEAP_SIZE);

        // Add 1000 rows in 10 batches of 100
        for (int i = 0; i < 10; i++) {
            long[] batchAddrs = QuickwitSplit.nativeTestExportStandardBatchFfi(100, i * 100);
            long docCount = QuickwitSplit.addArrowBatch(handle, batchAddrs[0], batchAddrs[1]);
            assertEquals((i + 1) * 100, docCount);
        }

        String outputDir = tempDir.resolve("larger_batch").toString();
        Files.createDirectories(Path.of(outputDir));
        List<QuickwitSplit.PartitionSplitResult> results = QuickwitSplit.finishAllSplits(handle, outputDir);

        assertEquals(1, results.size());
        assertEquals(1000, results.get(0).getNumDocs(), "Should have 1000 docs");

        // Verify searchable
        QuickwitSplit.PartitionSplitResult result = results.get(0);
        QuickwitSplit.SplitMetadata metadata = new QuickwitSplit.SplitMetadata(
                result.getFooterStartOffset(), result.getFooterEndOffset(), 0, 0);
        try (SplitSearcher searcher = cacheManager.createSplitSearcher(
                "file://" + result.getSplitPath(), metadata)) {
            SplitQuery allQuery = searcher.parseQuery("*");
            SearchResult allResults = searcher.search(allQuery, 1001);
            assertEquals(1000, allResults.getHits().size(), "Should find all 1000 docs");
        }

        System.out.println("testLargerBatch PASSED: 1000 docs across 10 batches");
    }
}

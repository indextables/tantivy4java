package io.indextables.tantivy4java;

import io.indextables.tantivy4java.core.DocAddress;
import io.indextables.tantivy4java.core.Document;
import io.indextables.tantivy4java.split.*;
import io.indextables.tantivy4java.split.merge.*;

import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Reproduction test for "Unknown field type: -80" when batch-retrieving documents
 * from companion splits that contain List/Array columns.
 *
 * Tests two array patterns:
 * 1. Original: alternating 1-element and 3-element arrays
 * 2. Varied: cycling null, empty[], 1-element, 2-element, 3-element arrays
 *
 * Both use 4KB data page size (many pages) and NO offset index (v1 parquet).
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class ParquetCompanionArrayBatchTest {

    /** Helper: create a companion split using the original alternating 1/3 array pattern. */
    private static SplitSearcher createOriginalArraySplit(
            Path dir, int numRows, long idOffset, String testName) throws Exception {
        Path parquetFile = dir.resolve(testName + ".parquet");
        Path splitFile = dir.resolve(testName + ".split");

        QuickwitSplit.nativeWriteTestParquetArrayNoPageIndex(
                parquetFile.toString(), numRows, idOffset);

        ParquetCompanionConfig config = new ParquetCompanionConfig(dir.toString());
        QuickwitSplit.SplitMetadata metadata = QuickwitSplit.createFromParquet(
                Collections.singletonList(parquetFile.toString()),
                splitFile.toString(), config);

        assertEquals(numRows, metadata.getNumDocs(), "should index all rows");

        SplitCacheManager.CacheConfig cacheConfig =
                new SplitCacheManager.CacheConfig("orig-" + testName + "-" + System.nanoTime())
                        .withMaxCacheSize(200_000_000);
        SplitCacheManager cacheManager = SplitCacheManager.getInstance(cacheConfig);

        String splitUrl = "file://" + splitFile.toAbsolutePath();
        return cacheManager.createSplitSearcher(splitUrl, metadata, dir.toString());
    }

    /** Helper: create a companion split using varied array patterns (null, empty, 1, 2, 3 elements). */
    private static SplitSearcher createVariedArraySplit(
            Path dir, int numRows, long idOffset, String testName) throws Exception {
        Path parquetFile = dir.resolve(testName + ".parquet");
        Path splitFile = dir.resolve(testName + ".split");

        QuickwitSplit.nativeWriteTestParquetArrayVariedNoPageIndex(
                parquetFile.toString(), numRows, idOffset);

        ParquetCompanionConfig config = new ParquetCompanionConfig(dir.toString());
        QuickwitSplit.SplitMetadata metadata = QuickwitSplit.createFromParquet(
                Collections.singletonList(parquetFile.toString()),
                splitFile.toString(), config);

        assertEquals(numRows, metadata.getNumDocs(), "should index all rows");

        SplitCacheManager.CacheConfig cacheConfig =
                new SplitCacheManager.CacheConfig("varied-" + testName + "-" + System.nanoTime())
                        .withMaxCacheSize(200_000_000);
        SplitCacheManager cacheManager = SplitCacheManager.getInstance(cacheConfig);

        String splitUrl = "file://" + splitFile.toAbsolutePath();
        return cacheManager.createSplitSearcher(splitUrl, metadata, dir.toString());
    }

    /** Construct DocAddress array: segment 0, doc IDs 0..numRows-1. */
    private static DocAddress[] makeAllAddresses(int numRows) {
        DocAddress[] addrs = new DocAddress[numRows];
        for (int i = 0; i < numRows; i++) {
            addrs[i] = new DocAddress(0, i);
        }
        return addrs;
    }

    /**
     * Validate correctness for the ORIGINAL alternating 1/3 pattern.
     *
     * Expected:
     *   even rows: id=offset+i, name="item_{offset+i}", event_type=["evt_{offset+i}"]
     *   odd rows:  id=offset+i, name="item_{offset+i}", event_type=["evt_{offset+i}","login","auth"]
     */
    private static void validateOriginalPattern(List<Document> docs, int numRows, long idOffset) {
        assertEquals(numRows, docs.size(), "batch should return all " + numRows + " documents");

        for (int i = 0; i < docs.size(); i++) {
            Document doc = docs.get(i);
            assertNotNull(doc, "doc at index " + i + " should not be null");

            long expectedId = idOffset + i;
            long actualId = ((Number) doc.getFirst("id")).longValue();
            assertEquals(expectedId, actualId, "doc " + i + " id mismatch");

            assertEquals("item_" + expectedId, doc.getFirst("name").toString(),
                    "doc " + i + " name mismatch");

            Object eventType = doc.getFirst("event_type");
            assertNotNull(eventType, "doc " + i + " should have event_type");
            String etStr = eventType.toString();

            String expectedMarker = "evt_" + expectedId;
            assertTrue(etStr.contains(expectedMarker),
                    "doc " + i + " event_type should contain '" + expectedMarker + "' but got: " + etStr);

            if (i % 2 == 0) {
                assertFalse(etStr.contains("login"),
                        "doc " + i + " (even) should have 1-element array, got: " + etStr);
            } else {
                assertTrue(etStr.contains("login") && etStr.contains("auth"),
                        "doc " + i + " (odd) should have 3-element array, got: " + etStr);
            }
        }
    }

    /**
     * Validate correctness for the VARIED pattern (null, empty, 1, 2, 3 elements).
     *
     * Pattern cycles every 5 rows:
     *   i%5==0: null array
     *   i%5==1: empty array []
     *   i%5==2: ["evt_{offset+i}"]
     *   i%5==3: ["evt_{offset+i}", "login"]
     *   i%5==4: ["evt_{offset+i}", "login", "auth"]
     */
    private static void validateVariedPattern(List<Document> docs, int numRows, long idOffset) {
        assertEquals(numRows, docs.size(), "batch should return all " + numRows + " documents");

        for (int i = 0; i < docs.size(); i++) {
            Document doc = docs.get(i);
            assertNotNull(doc, "doc at index " + i + " should not be null");

            long expectedId = idOffset + i;
            long actualId = ((Number) doc.getFirst("id")).longValue();
            assertEquals(expectedId, actualId, "doc " + i + " id mismatch");

            assertEquals("item_" + expectedId, doc.getFirst("name").toString(),
                    "doc " + i + " name mismatch");

            Object eventType = doc.getFirst("event_type");
            String etStr = eventType != null ? eventType.toString() : null;

            switch (i % 5) {
                case 0:
                    // null array — might be null or absent in retrieved doc
                    // (parquet nulls may become absent fields)
                    break;
                case 1:
                    // empty array []
                    if (etStr != null) {
                        assertTrue(etStr.equals("[]") || etStr.equals(""),
                                "doc " + i + " (i%5==1) should be empty array, got: " + etStr);
                    }
                    break;
                case 2:
                    // 1-element: ["evt_N"]
                    assertNotNull(eventType, "doc " + i + " (i%5==2) should have event_type");
                    assertTrue(etStr.contains("evt_" + expectedId),
                            "doc " + i + " event_type should contain 'evt_" + expectedId + "', got: " + etStr);
                    assertFalse(etStr.contains("login"),
                            "doc " + i + " (i%5==2) should be 1-element, got: " + etStr);
                    break;
                case 3:
                    // 2-element: ["evt_N", "login"]
                    assertNotNull(eventType, "doc " + i + " (i%5==3) should have event_type");
                    assertTrue(etStr.contains("evt_" + expectedId),
                            "doc " + i + " event_type should contain 'evt_" + expectedId + "', got: " + etStr);
                    assertTrue(etStr.contains("login"),
                            "doc " + i + " (i%5==3) should have 'login', got: " + etStr);
                    assertFalse(etStr.contains("auth"),
                            "doc " + i + " (i%5==3) should NOT have 'auth', got: " + etStr);
                    break;
                case 4:
                    // 3-element: ["evt_N", "login", "auth"]
                    assertNotNull(eventType, "doc " + i + " (i%5==4) should have event_type");
                    assertTrue(etStr.contains("evt_" + expectedId),
                            "doc " + i + " event_type should contain 'evt_" + expectedId + "', got: " + etStr);
                    assertTrue(etStr.contains("login") && etStr.contains("auth"),
                            "doc " + i + " (i%5==4) should have login+auth, got: " + etStr);
                    break;
            }
        }
    }

    // ===================================================================
    // ORIGINAL PATTERN TESTS (alternating 1/3 element arrays)
    // ===================================================================

    @Test
    @Order(1)
    void testOriginal_100rows(@TempDir Path dir) throws Exception {
        int n = 100;
        try (SplitSearcher s = createOriginalArraySplit(dir, n, 0, "orig100")) {
            validateOriginalPattern(s.docBatchProjected(makeAllAddresses(n), "id", "name", "event_type"), n, 0);
        }
    }

    @Test
    @Order(2)
    void testOriginal_627rows(@TempDir Path dir) throws Exception {
        int n = 627;
        try (SplitSearcher s = createOriginalArraySplit(dir, n, 0, "orig627")) {
            validateOriginalPattern(s.docBatchProjected(makeAllAddresses(n), "id", "name", "event_type"), n, 0);
        }
    }

    @Test
    @Order(3)
    void testOriginal_5000rows(@TempDir Path dir) throws Exception {
        int n = 5000;
        try (SplitSearcher s = createOriginalArraySplit(dir, n, 0, "orig5k")) {
            validateOriginalPattern(s.docBatchProjected(makeAllAddresses(n), "id", "name", "event_type"), n, 0);
        }
    }

    // ===================================================================
    // VARIED PATTERN TESTS (null, empty, 1, 2, 3 element arrays)
    // ===================================================================

    @Test
    @Order(10)
    void testVaried_100rows(@TempDir Path dir) throws Exception {
        int n = 100;
        try (SplitSearcher s = createVariedArraySplit(dir, n, 0, "var100")) {
            validateVariedPattern(s.docBatchProjected(makeAllAddresses(n), "id", "name", "event_type"), n, 0);
        }
    }

    @Test
    @Order(11)
    void testVaried_627rows(@TempDir Path dir) throws Exception {
        int n = 627;
        try (SplitSearcher s = createVariedArraySplit(dir, n, 0, "var627")) {
            validateVariedPattern(s.docBatchProjected(makeAllAddresses(n), "id", "name", "event_type"), n, 0);
        }
    }

    @Test
    @Order(12)
    void testVaried_1000rows(@TempDir Path dir) throws Exception {
        int n = 1000;
        try (SplitSearcher s = createVariedArraySplit(dir, n, 0, "var1k")) {
            validateVariedPattern(s.docBatchProjected(makeAllAddresses(n), "id", "name", "event_type"), n, 0);
        }
    }

    @Test
    @Order(13)
    void testVaried_5000rows(@TempDir Path dir) throws Exception {
        int n = 5000;
        try (SplitSearcher s = createVariedArraySplit(dir, n, 0, "var5k")) {
            validateVariedPattern(s.docBatchProjected(makeAllAddresses(n), "id", "name", "event_type"), n, 0);
        }
    }

    @Test
    @Order(14)
    void testVaried_5000rows_withOffset(@TempDir Path dir) throws Exception {
        int n = 5000;
        long offset = 10000;
        try (SplitSearcher s = createVariedArraySplit(dir, n, offset, "var5koff")) {
            validateVariedPattern(s.docBatchProjected(makeAllAddresses(n), "id", "name", "event_type"), n, offset);
        }
    }

    // ===================================================================
    // STRESS TESTS — 256B pages, arrays up to 10 elements, no offset index
    // ===================================================================

    /** Helper: create a stress-test companion split (256B pages, arrays 0..10 elements). */
    private static SplitSearcher createStressArraySplit(
            Path dir, int numRows, long idOffset, String testName) throws Exception {
        Path parquetFile = dir.resolve(testName + ".parquet");
        Path splitFile = dir.resolve(testName + ".split");

        QuickwitSplit.nativeWriteTestParquetArrayStress(
                parquetFile.toString(), numRows, idOffset);

        ParquetCompanionConfig config = new ParquetCompanionConfig(dir.toString());
        QuickwitSplit.SplitMetadata metadata = QuickwitSplit.createFromParquet(
                Collections.singletonList(parquetFile.toString()),
                splitFile.toString(), config);

        assertEquals(numRows, metadata.getNumDocs(), "should index all rows");

        SplitCacheManager.CacheConfig cacheConfig =
                new SplitCacheManager.CacheConfig("stress-" + testName + "-" + System.nanoTime())
                        .withMaxCacheSize(200_000_000);
        SplitCacheManager cacheManager = SplitCacheManager.getInstance(cacheConfig);

        String splitUrl = "file://" + splitFile.toAbsolutePath();
        return cacheManager.createSplitSearcher(splitUrl, metadata, dir.toString());
    }

    /**
     * Validate stress pattern: cycles null, empty, arrays of size 1..10.
     * Pattern repeats every 12 rows:
     *   i%12==0: null, i%12==1: empty[], i%12==2: 1-element, ... i%12==11: 10-element
     */
    private static void validateStressPattern(List<Document> docs, int numRows, long idOffset) {
        assertEquals(numRows, docs.size(), "batch should return all " + numRows + " documents");

        for (int i = 0; i < docs.size(); i++) {
            Document doc = docs.get(i);
            assertNotNull(doc, "doc at index " + i + " should not be null");

            long expectedId = idOffset + i;
            long actualId = ((Number) doc.getFirst("id")).longValue();
            assertEquals(expectedId, actualId, "doc " + i + " id mismatch");
            assertEquals("item_" + expectedId, doc.getFirst("name").toString(),
                    "doc " + i + " name mismatch");

            Object eventType = doc.getFirst("event_type");
            String etStr = eventType != null ? eventType.toString() : null;
            int pattern = i % 12;

            if (pattern == 0) {
                // null — may be absent
            } else if (pattern == 1) {
                // empty []
                if (etStr != null) {
                    assertTrue(etStr.equals("[]") || etStr.equals(""),
                            "doc " + i + " (pattern=1) should be empty, got: " + etStr);
                }
            } else {
                // arrays of size pattern-1 (1..10), first element is "evt_N"
                assertNotNull(eventType, "doc " + i + " (pattern=" + pattern + ") should have event_type");
                assertTrue(etStr.contains("evt_" + expectedId),
                        "doc " + i + " event_type should contain 'evt_" + expectedId + "', got: " + etStr);
            }
        }
    }

    @Test
    @Order(30)
    void testStress_100rows(@TempDir Path dir) throws Exception {
        int n = 100;
        try (SplitSearcher s = createStressArraySplit(dir, n, 0, "stress100")) {
            validateStressPattern(s.docBatchProjected(makeAllAddresses(n), "id", "name", "event_type"), n, 0);
        }
    }

    @Test
    @Order(31)
    void testStress_627rows(@TempDir Path dir) throws Exception {
        int n = 627;
        try (SplitSearcher s = createStressArraySplit(dir, n, 0, "stress627")) {
            validateStressPattern(s.docBatchProjected(makeAllAddresses(n), "id", "name", "event_type"), n, 0);
        }
    }

    @Test
    @Order(32)
    void testStress_1000rows(@TempDir Path dir) throws Exception {
        int n = 1000;
        try (SplitSearcher s = createStressArraySplit(dir, n, 0, "stress1k")) {
            validateStressPattern(s.docBatchProjected(makeAllAddresses(n), "id", "name", "event_type"), n, 0);
        }
    }

    @Test
    @Order(33)
    void testStress_5000rows(@TempDir Path dir) throws Exception {
        int n = 5000;
        try (SplitSearcher s = createStressArraySplit(dir, n, 0, "stress5k")) {
            validateStressPattern(s.docBatchProjected(makeAllAddresses(n), "id", "name", "event_type"), n, 0);
        }
    }

    @Test
    @Order(34)
    void testStress_20000rows(@TempDir Path dir) throws Exception {
        int n = 20000;
        try (SplitSearcher s = createStressArraySplit(dir, n, 0, "stress20k")) {
            validateStressPattern(s.docBatchProjected(makeAllAddresses(n), "id", "name", "event_type"), n, 0);
        }
    }

    // ===================================================================
    // LAST-ELEMENT BOUNDARY TESTS — null/empty as final row in batch
    // ===================================================================

    @Test
    @Order(15)
    void testVaried_lastElementNull(@TempDir Path dir) throws Exception {
        // 621 rows: last i=620, 620%5==0 → null array as last element
        int n = 621;
        try (SplitSearcher s = createVariedArraySplit(dir, n, 0, "var_lastNull")) {
            validateVariedPattern(s.docBatchProjected(makeAllAddresses(n), "id", "name", "event_type"), n, 0);
        }
    }

    @Test
    @Order(16)
    void testVaried_lastElementEmpty(@TempDir Path dir) throws Exception {
        // 622 rows: last i=621, 621%5==1 → empty array [] as last element
        int n = 622;
        try (SplitSearcher s = createVariedArraySplit(dir, n, 0, "var_lastEmpty")) {
            validateVariedPattern(s.docBatchProjected(makeAllAddresses(n), "id", "name", "event_type"), n, 0);
        }
    }

    @Test
    @Order(17)
    void testStress_lastElementNull(@TempDir Path dir) throws Exception {
        // 625 rows: last i=624, 624%12==0 → null array as last element
        int n = 625;
        try (SplitSearcher s = createStressArraySplit(dir, n, 0, "stress_lastNull")) {
            validateStressPattern(s.docBatchProjected(makeAllAddresses(n), "id", "name", "event_type"), n, 0);
        }
    }

    @Test
    @Order(18)
    void testStress_lastElementEmpty(@TempDir Path dir) throws Exception {
        // 626 rows: last i=625, 625%12==1 → empty array [] as last element
        int n = 626;
        try (SplitSearcher s = createStressArraySplit(dir, n, 0, "stress_lastEmpty")) {
            validateStressPattern(s.docBatchProjected(makeAllAddresses(n), "id", "name", "event_type"), n, 0);
        }
    }

    // ===================================================================
    // ARRAY-ONLY PROJECTION (no flat columns — exercises different code path)
    // ===================================================================

    @Test
    @Order(20)
    void testVaried_arrayOnlyProjection(@TempDir Path dir) throws Exception {
        int n = 1000;
        try (SplitSearcher s = createVariedArraySplit(dir, n, 0, "var_arronly")) {
            List<Document> docs = s.docBatchProjected(makeAllAddresses(n), "event_type");
            assertEquals(n, docs.size(), "batch should return all " + n + " documents");
            for (int i = 0; i < docs.size(); i++) {
                assertNotNull(docs.get(i), "doc at index " + i + " should not be null");
            }
        }
    }

    // ===================================================================
    // NON-ARRAY PROJECTION (flat columns only — baseline)
    // ===================================================================

    @Test
    @Order(21)
    void testVaried_flatColumnsOnly(@TempDir Path dir) throws Exception {
        int n = 1000;
        try (SplitSearcher s = createVariedArraySplit(dir, n, 0, "var_flat")) {
            List<Document> docs = s.docBatchProjected(makeAllAddresses(n), "id", "name");
            assertEquals(n, docs.size());
            for (int i = 0; i < docs.size(); i++) {
                long actualId = ((Number) docs.get(i).getFirst("id")).longValue();
                assertEquals(i, actualId, "doc " + i + " id mismatch");
                assertEquals("item_" + i, docs.get(i).getFirst("name").toString());
            }
        }
    }
}

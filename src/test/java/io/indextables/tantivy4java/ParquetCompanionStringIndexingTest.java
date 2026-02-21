/*
 * Validates the compact string indexing modes for parquet companion splits.
 *
 * Tests all 5 string indexing modes end-to-end:
 *   - exact_only:              U64 hash replaces full Str field; term queries auto-hashed
 *   - text_uuid_exactonly:     Strip UUIDs → "default" tokenizer text; UUIDs → companion U64 hash
 *   - text_uuid_strip:         Strip UUIDs → "default" tokenizer text; UUIDs discarded
 *   - text_custom_exactonly:   Strip custom regex → text; matches → companion U64 hash
 *   - text_custom_strip:       Strip custom regex → text; matches discarded
 *
 * Test data schema (from nativeWriteTestParquetForStringIndexing):
 *   id (i64), trace_id (utf8 — pure UUIDs), message (utf8 — text with embedded UUID),
 *   error_log (utf8 — text with ERR-XXXX pattern), category (utf8 — cycling info/warn/error)
 *
 * Run:
 *   mvn test -pl . -Dtest=ParquetCompanionStringIndexingTest
 */
package io.indextables.tantivy4java;

import io.indextables.tantivy4java.aggregation.*;
import io.indextables.tantivy4java.core.Document;
import io.indextables.tantivy4java.result.*;
import io.indextables.tantivy4java.split.*;
import io.indextables.tantivy4java.split.merge.*;

import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.*;
import java.util.regex.*;

import static org.junit.jupiter.api.Assertions.*;

@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class ParquetCompanionStringIndexingTest {

    private static final int NUM_ROWS = 30;
    private static final String ERR_PATTERN = "ERR-\\d{4}";

    private static SplitCacheManager cacheManager;

    @BeforeAll
    static void setupCache() {
        SplitCacheManager.CacheConfig config =
                new SplitCacheManager.CacheConfig("pq-stridx-test-" + System.nanoTime());
        cacheManager = SplitCacheManager.getInstance(config);
    }

    @AfterAll
    static void teardownCache() {
        if (cacheManager != null) cacheManager.close();
    }

    // ═══════════════════════════════════════════════════════════════
    //  Helpers
    // ═══════════════════════════════════════════════════════════════

    /** Write test parquet and create a companion split with the given tokenizer overrides. */
    private SplitSearcher createSearcher(
            Path dir, Map<String, String> tokenizerOverrides,
            ParquetCompanionConfig.FastFieldMode mode, String tag) throws Exception {
        Path parquetFile = dir.resolve(tag + ".parquet");
        Path splitFile = dir.resolve(tag + ".split");

        QuickwitSplit.nativeWriteTestParquetForStringIndexing(
                parquetFile.toString(), NUM_ROWS, 0);

        ParquetCompanionConfig config = new ParquetCompanionConfig(dir.toString())
                .withFastFieldMode(mode)
                .withTokenizerOverrides(tokenizerOverrides)
                .withStringHashOptimization(true);

        QuickwitSplit.SplitMetadata metadata = QuickwitSplit.createFromParquet(
                Collections.singletonList(parquetFile.toString()),
                splitFile.toString(), config);

        String splitUrl = "file://" + splitFile.toAbsolutePath();
        return cacheManager.createSplitSearcher(splitUrl, metadata, dir.toString());
    }

    /** Convenience: create with DISABLED fast field mode. */
    private SplitSearcher createSearcher(
            Path dir, Map<String, String> tokenizerOverrides, String tag) throws Exception {
        return createSearcher(dir, tokenizerOverrides,
                ParquetCompanionConfig.FastFieldMode.DISABLED, tag);
    }

    /** Extract a UUID from a text string using the standard UUID regex. */
    private static String extractUuid(String text) {
        Matcher m = Pattern.compile(
                "[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}"
        ).matcher(text);
        return m.find() ? m.group() : null;
    }

    /** Extract ERR-XXXX from a text string. */
    private static String extractErrCode(String text) {
        Matcher m = Pattern.compile(ERR_PATTERN).matcher(text);
        return m.find() ? m.group() : null;
    }

    // ═══════════════════════════════════════════════════════════════
    //  1. exact_only: Split creation succeeds
    // ═══════════════════════════════════════════════════════════════

    @Test @Order(1)
    @DisplayName("exact_only — split creation and match-all returns correct doc count")
    void exactOnlySplitCreation(@TempDir Path dir) throws Exception {
        Map<String, String> overrides = new HashMap<>();
        overrides.put("trace_id", ParquetCompanionConfig.StringIndexingMode.EXACT_ONLY);

        try (SplitSearcher s = createSearcher(dir, overrides, "eo_create")) {
            SearchResult result = s.search(new SplitMatchAllQuery(), NUM_ROWS);
            assertEquals(NUM_ROWS, result.getHits().size(),
                    "exact_only split should contain all " + NUM_ROWS + " documents");
        }
    }

    // ═══════════════════════════════════════════════════════════════
    //  2. exact_only: Term query finds document by UUID hash
    // ═══════════════════════════════════════════════════════════════

    @Test @Order(2)
    @DisplayName("exact_only — term query for a known UUID finds exactly 1 document")
    void exactOnlyTermQueryFindsDoc(@TempDir Path dir) throws Exception {
        Map<String, String> overrides = new HashMap<>();
        overrides.put("trace_id", ParquetCompanionConfig.StringIndexingMode.EXACT_ONLY);

        try (SplitSearcher s = createSearcher(dir, overrides, "eo_term")) {
            // Get a doc to learn its trace_id from parquet retrieval
            SearchResult allDocs = s.search(new SplitMatchAllQuery(), 1);
            assertEquals(1, allDocs.getHits().size());

            String traceId;
            // Use docProjected() — companion splits store data in parquet, not tantivy
            try (Document doc = s.docProjected(allDocs.getHits().get(0).getDocAddress())) {
                traceId = (String) doc.getFirst("trace_id");
                assertNotNull(traceId, "trace_id should be retrievable from parquet");
            }

            // Now search for that exact trace_id — should be rewritten to hash
            SplitTermQuery q = new SplitTermQuery("trace_id", traceId);
            SearchResult result = s.search(q, 10);
            assertEquals(1, result.getHits().size(),
                    "exact_only term query should find exactly 1 doc for UUID: " + traceId);
        }
    }

    // ═══════════════════════════════════════════════════════════════
    //  3. exact_only: Term query for non-existent UUID returns 0
    // ═══════════════════════════════════════════════════════════════

    @Test @Order(3)
    @DisplayName("exact_only — term query for non-existent UUID returns 0 results")
    void exactOnlyTermQueryMiss(@TempDir Path dir) throws Exception {
        Map<String, String> overrides = new HashMap<>();
        overrides.put("trace_id", ParquetCompanionConfig.StringIndexingMode.EXACT_ONLY);

        try (SplitSearcher s = createSearcher(dir, overrides, "eo_miss")) {
            SplitTermQuery q = new SplitTermQuery("trace_id", "ffffffff-ffff-ffff-ffff-ffffffffffff");
            SearchResult result = s.search(q, 10);
            assertEquals(0, result.getHits().size(),
                    "Non-existent UUID should return 0 results");
        }
    }

    // ═══════════════════════════════════════════════════════════════
    //  4. text_uuid_exactonly: Split creation and doc count
    // ═══════════════════════════════════════════════════════════════

    @Test @Order(4)
    @DisplayName("text_uuid_exactonly — split creation with correct doc count")
    void textUuidExactOnlySplitCreation(@TempDir Path dir) throws Exception {
        Map<String, String> overrides = new HashMap<>();
        overrides.put("message", ParquetCompanionConfig.StringIndexingMode.TEXT_UUID_EXACTONLY);

        try (SplitSearcher s = createSearcher(dir, overrides, "tue_create")) {
            SearchResult result = s.search(new SplitMatchAllQuery(), NUM_ROWS);
            assertEquals(NUM_ROWS, result.getHits().size());
        }
    }

    // ═══════════════════════════════════════════════════════════════
    //  5. text_uuid_exactonly: UUID term redirected to companion field
    // ═══════════════════════════════════════════════════════════════

    @Test @Order(5)
    @DisplayName("text_uuid_exactonly — term query for embedded UUID finds document via companion")
    void textUuidExactOnlyUuidTermQuery(@TempDir Path dir) throws Exception {
        Map<String, String> overrides = new HashMap<>();
        overrides.put("message", ParquetCompanionConfig.StringIndexingMode.TEXT_UUID_EXACTONLY);

        try (SplitSearcher s = createSearcher(dir, overrides, "tue_uuid_term")) {
            // Get first doc's message to extract its UUID
            SearchResult allDocs = s.search(new SplitMatchAllQuery(), 1);
            String message;
            try (Document doc = s.docProjected(allDocs.getHits().get(0).getDocAddress())) {
                message = (String) doc.getFirst("message");
                assertNotNull(message);
            }

            String uuid = extractUuid(message);
            assertNotNull(uuid, "Message should contain a UUID: " + message);

            // Term query for the UUID should be redirected to companion field
            SplitTermQuery q = new SplitTermQuery("message", uuid);
            SearchResult result = s.search(q, 10);
            assertEquals(1, result.getHits().size(),
                    "UUID term query should find exactly 1 doc via companion redirect for: " + uuid);
        }
    }

    // ═══════════════════════════════════════════════════════════════
    //  6. text_uuid_exactonly: Text search still works after UUID stripping
    // ═══════════════════════════════════════════════════════════════

    @Test @Order(6)
    @DisplayName("text_uuid_exactonly — parseQuery finds docs on non-UUID text content")
    void textUuidExactOnlyTextSearch(@TempDir Path dir) throws Exception {
        Map<String, String> overrides = new HashMap<>();
        overrides.put("message", ParquetCompanionConfig.StringIndexingMode.TEXT_UUID_EXACTONLY);

        try (SplitSearcher s = createSearcher(dir, overrides, "tue_text")) {
            // "completed" appears in every message: "Request {uuid} completed action ..."
            // After UUID stripping: "Request  completed action ..."
            // "default" tokenizer: ["request", "completed", "action", ...]
            SplitQuery q = s.parseQuery("message:completed");
            SearchResult result = s.search(q, NUM_ROWS);
            assertEquals(NUM_ROWS, result.getHits().size(),
                    "Text search for 'completed' should find all " + NUM_ROWS + " docs");
        }
    }

    // ═══════════════════════════════════════════════════════════════
    //  7. text_uuid_exactonly: Non-UUID text term not redirected
    // ═══════════════════════════════════════════════════════════════

    @Test @Order(7)
    @DisplayName("text_uuid_exactonly — parseQuery for action word finds subset of docs")
    void textUuidExactOnlyActionSubset(@TempDir Path dir) throws Exception {
        Map<String, String> overrides = new HashMap<>();
        overrides.put("message", ParquetCompanionConfig.StringIndexingMode.TEXT_UUID_EXACTONLY);

        try (SplitSearcher s = createSearcher(dir, overrides, "tue_subset")) {
            // "login" appears as action for every 5th doc (i%5==0)
            SplitQuery q = s.parseQuery("message:login");
            SearchResult result = s.search(q, NUM_ROWS);
            assertEquals(NUM_ROWS / 5, result.getHits().size(),
                    "Text search for 'login' should find " + (NUM_ROWS / 5) + " docs (every 5th)");
        }
    }

    // ═══════════════════════════════════════════════════════════════
    //  8. text_uuid_strip: UUID term query returns 0 (UUIDs discarded)
    // ═══════════════════════════════════════════════════════════════

    @Test @Order(8)
    @DisplayName("text_uuid_strip — UUID term query returns 0 results (UUIDs discarded)")
    void textUuidStripUuidGone(@TempDir Path dir) throws Exception {
        Map<String, String> overrides = new HashMap<>();
        overrides.put("message", ParquetCompanionConfig.StringIndexingMode.TEXT_UUID_STRIP);

        try (SplitSearcher s = createSearcher(dir, overrides, "tus_gone")) {
            // Get a UUID from the parquet data
            SearchResult allDocs = s.search(new SplitMatchAllQuery(), 1);
            String message;
            try (Document doc = s.docProjected(allDocs.getHits().get(0).getDocAddress())) {
                message = (String) doc.getFirst("message");
            }
            String uuid = extractUuid(message);
            assertNotNull(uuid);

            // With text_uuid_strip, the UUID is discarded — not in text index, no companion field
            SplitTermQuery q = new SplitTermQuery("message", uuid);
            SearchResult result = s.search(q, 10);
            assertEquals(0, result.getHits().size(),
                    "UUID term query should return 0 results when mode is text_uuid_strip");
        }
    }

    // ═══════════════════════════════════════════════════════════════
    //  9. text_uuid_strip: Text search still works after stripping
    // ═══════════════════════════════════════════════════════════════

    @Test @Order(9)
    @DisplayName("text_uuid_strip — text search for non-UUID content works")
    void textUuidStripTextSearch(@TempDir Path dir) throws Exception {
        Map<String, String> overrides = new HashMap<>();
        overrides.put("message", ParquetCompanionConfig.StringIndexingMode.TEXT_UUID_STRIP);

        try (SplitSearcher s = createSearcher(dir, overrides, "tus_text")) {
            SplitQuery q = s.parseQuery("message:request");
            SearchResult result = s.search(q, NUM_ROWS);
            assertEquals(NUM_ROWS, result.getHits().size(),
                    "Text search for 'request' should find all docs after UUID stripping");
        }
    }

    // ═══════════════════════════════════════════════════════════════
    //  10. text_custom_exactonly: Split creation and ERR-XXXX redirect
    // ═══════════════════════════════════════════════════════════════

    @Test @Order(10)
    @DisplayName("text_custom_exactonly — error code term query finds doc via companion")
    void textCustomExactOnlyPatternRedirect(@TempDir Path dir) throws Exception {
        Map<String, String> overrides = new HashMap<>();
        overrides.put("error_log",
                ParquetCompanionConfig.StringIndexingMode.textCustomExactonly(ERR_PATTERN));

        try (SplitSearcher s = createSearcher(dir, overrides, "tce_redirect")) {
            // Get first doc's error_log to extract ERR-XXXX
            SearchResult allDocs = s.search(new SplitMatchAllQuery(), 1);
            String errorLog;
            try (Document doc = s.docProjected(allDocs.getHits().get(0).getDocAddress())) {
                errorLog = (String) doc.getFirst("error_log");
                assertNotNull(errorLog);
            }

            String errCode = extractErrCode(errorLog);
            assertNotNull(errCode, "error_log should contain ERR-XXXX: " + errorLog);

            // Term query for the error code should redirect to companion
            SplitTermQuery q = new SplitTermQuery("error_log", errCode);
            SearchResult result = s.search(q, 10);
            assertEquals(1, result.getHits().size(),
                    "Custom pattern term query should find doc via companion: " + errCode);
        }
    }

    // ═══════════════════════════════════════════════════════════════
    //  11. text_custom_exactonly: Text search on non-pattern content
    // ═══════════════════════════════════════════════════════════════

    @Test @Order(11)
    @DisplayName("text_custom_exactonly — text search for 'connection' finds all docs")
    void textCustomExactOnlyTextSearch(@TempDir Path dir) throws Exception {
        Map<String, String> overrides = new HashMap<>();
        overrides.put("error_log",
                ParquetCompanionConfig.StringIndexingMode.textCustomExactonly(ERR_PATTERN));

        try (SplitSearcher s = createSearcher(dir, overrides, "tce_text")) {
            // "Connection" appears in every error_log after ERR-XXXX stripping
            SplitQuery q = s.parseQuery("error_log:connection");
            SearchResult result = s.search(q, NUM_ROWS);
            assertEquals(NUM_ROWS, result.getHits().size(),
                    "Text search for 'connection' should find all docs after pattern stripping");
        }
    }

    // ═══════════════════════════════════════════════════════════════
    //  12. text_custom_strip: Pattern term query returns 0
    // ═══════════════════════════════════════════════════════════════

    @Test @Order(12)
    @DisplayName("text_custom_strip — ERR-XXXX term query returns 0 (patterns discarded)")
    void textCustomStripPatternGone(@TempDir Path dir) throws Exception {
        Map<String, String> overrides = new HashMap<>();
        overrides.put("error_log",
                ParquetCompanionConfig.StringIndexingMode.textCustomStrip(ERR_PATTERN));

        try (SplitSearcher s = createSearcher(dir, overrides, "tcs_gone")) {
            SearchResult allDocs = s.search(new SplitMatchAllQuery(), 1);
            String errorLog;
            try (Document doc = s.docProjected(allDocs.getHits().get(0).getDocAddress())) {
                errorLog = (String) doc.getFirst("error_log");
            }
            String errCode = extractErrCode(errorLog);
            assertNotNull(errCode);

            SplitTermQuery q = new SplitTermQuery("error_log", errCode);
            SearchResult result = s.search(q, 10);
            assertEquals(0, result.getHits().size(),
                    "Pattern term query should return 0 when mode is text_custom_strip");
        }
    }

    // ═══════════════════════════════════════════════════════════════
    //  13. text_custom_strip: Text search on remaining content works
    // ═══════════════════════════════════════════════════════════════

    @Test @Order(13)
    @DisplayName("text_custom_strip — text search for non-pattern content works")
    void textCustomStripTextSearch(@TempDir Path dir) throws Exception {
        Map<String, String> overrides = new HashMap<>();
        overrides.put("error_log",
                ParquetCompanionConfig.StringIndexingMode.textCustomStrip(ERR_PATTERN));

        try (SplitSearcher s = createSearcher(dir, overrides, "tcs_text")) {
            SplitQuery q = s.parseQuery("error_log:timed");
            SearchResult result = s.search(q, NUM_ROWS);
            assertEquals(NUM_ROWS, result.getHits().size(),
                    "Text search for 'timed' should find all docs after pattern stripping");
        }
    }

    // ═══════════════════════════════════════════════════════════════
    //  14. Multiple modes in same split
    // ═══════════════════════════════════════════════════════════════

    @Test @Order(14)
    @DisplayName("Multiple modes — exact_only + text_uuid_exactonly in same split")
    void multipleModesInSameSplit(@TempDir Path dir) throws Exception {
        Map<String, String> overrides = new HashMap<>();
        overrides.put("trace_id", ParquetCompanionConfig.StringIndexingMode.EXACT_ONLY);
        overrides.put("message", ParquetCompanionConfig.StringIndexingMode.TEXT_UUID_EXACTONLY);

        try (SplitSearcher s = createSearcher(dir, overrides, "multi_mode")) {
            // Verify doc count
            SearchResult allDocs = s.search(new SplitMatchAllQuery(), NUM_ROWS);
            assertEquals(NUM_ROWS, allDocs.getHits().size());

            // Get first doc's trace_id and message
            String traceId;
            String message;
            try (Document doc = s.docProjected(allDocs.getHits().get(0).getDocAddress())) {
                traceId = (String) doc.getFirst("trace_id");
                message = (String) doc.getFirst("message");
            }
            assertNotNull(traceId);
            assertNotNull(message);

            // exact_only on trace_id should work
            SearchResult traceResult = s.search(new SplitTermQuery("trace_id", traceId), 10);
            assertEquals(1, traceResult.getHits().size(),
                    "exact_only trace_id query should find 1 doc");

            // text_uuid_exactonly on message should work
            String msgUuid = extractUuid(message);
            assertNotNull(msgUuid);
            SearchResult msgResult = s.search(new SplitTermQuery("message", msgUuid), 10);
            assertEquals(1, msgResult.getHits().size(),
                    "text_uuid_exactonly companion query should find 1 doc");

            // text search on message should work
            SplitQuery textQ = s.parseQuery("message:completed");
            SearchResult textResult = s.search(textQ, NUM_ROWS);
            assertEquals(NUM_ROWS, textResult.getHits().size(),
                    "Text search on message should find all docs");
        }
    }

    // ═══════════════════════════════════════════════════════════════
    //  15. All five modes in one split
    // ═══════════════════════════════════════════════════════════════

    @Test @Order(15)
    @DisplayName("All 5 modes — trace_id=exact_only, message=text_uuid_exactonly, error_log=text_custom_strip, category=raw")
    void allModesInOneSplit(@TempDir Path dir) throws Exception {
        Map<String, String> overrides = new HashMap<>();
        overrides.put("trace_id", ParquetCompanionConfig.StringIndexingMode.EXACT_ONLY);
        overrides.put("message", ParquetCompanionConfig.StringIndexingMode.TEXT_UUID_EXACTONLY);
        overrides.put("error_log", ParquetCompanionConfig.StringIndexingMode.textCustomStrip(ERR_PATTERN));
        // category uses default ("raw") — no override needed

        try (SplitSearcher s = createSearcher(dir, overrides, "all_modes")) {
            SearchResult allDocs = s.search(new SplitMatchAllQuery(), NUM_ROWS);
            assertEquals(NUM_ROWS, allDocs.getHits().size(), "All docs present");

            // Verify category (raw tokenizer, no special mode) still works
            SplitTermQuery catQ = new SplitTermQuery("category", "info");
            SearchResult catResult = s.search(catQ, NUM_ROWS);
            assertEquals(NUM_ROWS / 3, catResult.getHits().size(),
                    "category=info should match every 3rd doc");

            // Verify text search on error_log after custom strip
            SplitQuery errQ = s.parseQuery("error_log:connection");
            SearchResult errResult = s.search(errQ, NUM_ROWS);
            assertEquals(NUM_ROWS, errResult.getHits().size(),
                    "error_log text search should work after custom strip");
        }
    }

    // ═══════════════════════════════════════════════════════════════
    //  16. HYBRID mode + exact_only
    // ═══════════════════════════════════════════════════════════════

    @Test @Order(16)
    @DisplayName("HYBRID mode — exact_only works with hybrid fast field mode")
    void hybridModeExactOnly(@TempDir Path dir) throws Exception {
        Map<String, String> overrides = new HashMap<>();
        overrides.put("trace_id", ParquetCompanionConfig.StringIndexingMode.EXACT_ONLY);

        try (SplitSearcher s = createSearcher(dir, overrides,
                ParquetCompanionConfig.FastFieldMode.HYBRID, "hybrid_eo")) {
            // Prewarm fast fields for HYBRID mode
            s.preloadParquetFastFields("id", "category").join();

            SearchResult allDocs = s.search(new SplitMatchAllQuery(), 1);
            assertEquals(1, allDocs.getHits().size());

            String traceId;
            try (Document doc = s.docProjected(allDocs.getHits().get(0).getDocAddress())) {
                traceId = (String) doc.getFirst("trace_id");
            }

            SearchResult result = s.search(new SplitTermQuery("trace_id", traceId), 10);
            assertEquals(1, result.getHits().size(),
                    "exact_only should work in HYBRID mode");
        }
    }

    // ═══════════════════════════════════════════════════════════════
    //  17. HYBRID mode + text_uuid_exactonly
    // ═══════════════════════════════════════════════════════════════

    @Test @Order(17)
    @DisplayName("HYBRID mode — text_uuid_exactonly works with hybrid fast field mode")
    void hybridModeTextUuidExactOnly(@TempDir Path dir) throws Exception {
        Map<String, String> overrides = new HashMap<>();
        overrides.put("message", ParquetCompanionConfig.StringIndexingMode.TEXT_UUID_EXACTONLY);

        try (SplitSearcher s = createSearcher(dir, overrides,
                ParquetCompanionConfig.FastFieldMode.HYBRID, "hybrid_tue")) {
            s.preloadParquetFastFields("id", "category").join();

            SearchResult allDocs = s.search(new SplitMatchAllQuery(), 1);
            String message;
            try (Document doc = s.docProjected(allDocs.getHits().get(0).getDocAddress())) {
                message = (String) doc.getFirst("message");
            }

            String uuid = extractUuid(message);
            assertNotNull(uuid);

            SearchResult uuidResult = s.search(new SplitTermQuery("message", uuid), 10);
            assertEquals(1, uuidResult.getHits().size(),
                    "text_uuid_exactonly companion redirect should work in HYBRID mode");

            SplitQuery textQ = s.parseQuery("message:request");
            SearchResult textResult = s.search(textQ, NUM_ROWS);
            assertEquals(NUM_ROWS, textResult.getHits().size(),
                    "Text search should work in HYBRID mode after UUID stripping");
        }
    }

    // ═══════════════════════════════════════════════════════════════
    //  18. Document retrieval returns original parquet values
    // ═══════════════════════════════════════════════════════════════

    @Test @Order(18)
    @DisplayName("Document retrieval — original parquet values intact regardless of indexing mode")
    void documentRetrievalOriginalValues(@TempDir Path dir) throws Exception {
        Map<String, String> overrides = new HashMap<>();
        overrides.put("trace_id", ParquetCompanionConfig.StringIndexingMode.EXACT_ONLY);
        overrides.put("message", ParquetCompanionConfig.StringIndexingMode.TEXT_UUID_EXACTONLY);
        overrides.put("error_log",
                ParquetCompanionConfig.StringIndexingMode.textCustomExactonly(ERR_PATTERN));

        try (SplitSearcher s = createSearcher(dir, overrides, "doc_retrieval")) {
            SearchResult allDocs = s.search(new SplitMatchAllQuery(), 5);
            assertTrue(allDocs.getHits().size() >= 5);

            for (SearchResult.Hit hit : allDocs.getHits()) {
                try (Document doc = s.docProjected(hit.getDocAddress())) {
                    // trace_id should be a valid UUID (original from parquet)
                    String traceId = (String) doc.getFirst("trace_id");
                    assertNotNull(traceId, "trace_id should be non-null");
                    assertTrue(traceId.matches(
                            "[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-4[0-9a-fA-F]{3}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}"),
                            "trace_id should be a valid UUID: " + traceId);

                    // message should contain a UUID (original from parquet, not stripped)
                    String msg = (String) doc.getFirst("message");
                    assertNotNull(msg);
                    assertNotNull(extractUuid(msg),
                            "Retrieved message should contain the original UUID: " + msg);
                    assertTrue(msg.contains("completed"),
                            "Retrieved message should contain 'completed': " + msg);

                    // error_log should contain ERR-XXXX (original from parquet)
                    String errLog = (String) doc.getFirst("error_log");
                    assertNotNull(errLog);
                    assertNotNull(extractErrCode(errLog),
                            "Retrieved error_log should contain ERR-XXXX: " + errLog);

                    // id should be present
                    assertNotNull(doc.getFirst("id"), "id should be non-null");

                    // category should be one of the cycling values
                    String category = (String) doc.getFirst("category");
                    assertTrue(Arrays.asList("info", "warn", "error").contains(category),
                            "category should be info/warn/error: " + category);
                }
            }
        }
    }

    // ═══════════════════════════════════════════════════════════════
    //  19. exact_only: Each unique trace_id finds exactly 1 doc
    // ═══════════════════════════════════════════════════════════════

    @Test @Order(19)
    @DisplayName("exact_only — each trace_id in the dataset finds exactly 1 document")
    void exactOnlyAllUniqueValues(@TempDir Path dir) throws Exception {
        Map<String, String> overrides = new HashMap<>();
        overrides.put("trace_id", ParquetCompanionConfig.StringIndexingMode.EXACT_ONLY);

        try (SplitSearcher s = createSearcher(dir, overrides, "eo_all_unique")) {
            // Collect all trace_ids
            SearchResult allDocs = s.search(new SplitMatchAllQuery(), NUM_ROWS);
            assertEquals(NUM_ROWS, allDocs.getHits().size());

            Set<String> seenIds = new HashSet<>();
            for (SearchResult.Hit hit : allDocs.getHits()) {
                try (Document doc = s.docProjected(hit.getDocAddress())) {
                    String traceId = (String) doc.getFirst("trace_id");
                    assertFalse(seenIds.contains(traceId),
                            "Each trace_id should be unique: " + traceId);
                    seenIds.add(traceId);

                    // Verify each one finds exactly 1 doc
                    SearchResult r = s.search(new SplitTermQuery("trace_id", traceId), 10);
                    assertEquals(1, r.getHits().size(),
                            "Each trace_id should find exactly 1 doc: " + traceId);
                }
            }
            assertEquals(NUM_ROWS, seenIds.size(), "All trace_ids should be unique");
        }
    }

    // ═══════════════════════════════════════════════════════════════
    //  20. text_uuid_exactonly: Each embedded UUID finds exactly 1 doc
    // ═══════════════════════════════════════════════════════════════

    @Test @Order(20)
    @DisplayName("text_uuid_exactonly — each embedded UUID finds exactly 1 document via companion")
    void textUuidExactOnlyAllUuids(@TempDir Path dir) throws Exception {
        Map<String, String> overrides = new HashMap<>();
        overrides.put("message", ParquetCompanionConfig.StringIndexingMode.TEXT_UUID_EXACTONLY);

        try (SplitSearcher s = createSearcher(dir, overrides, "tue_all_uuids")) {
            SearchResult allDocs = s.search(new SplitMatchAllQuery(), NUM_ROWS);
            assertEquals(NUM_ROWS, allDocs.getHits().size());

            int checked = 0;
            for (SearchResult.Hit hit : allDocs.getHits()) {
                try (Document doc = s.docProjected(hit.getDocAddress())) {
                    String message = (String) doc.getFirst("message");
                    String uuid = extractUuid(message);
                    assertNotNull(uuid);

                    SearchResult r = s.search(new SplitTermQuery("message", uuid), 10);
                    assertEquals(1, r.getHits().size(),
                            "Each UUID should find exactly 1 doc via companion: " + uuid);
                    checked++;
                }
            }
            assertEquals(NUM_ROWS, checked, "Should have checked all rows");
        }
    }

    // ═══════════════════════════════════════════════════════════════
    //  21. Terms aggregation on category (unaffected by other modes)
    // ═══════════════════════════════════════════════════════════════

    @Test @Order(21)
    @DisplayName("Terms aggregation — category field works alongside string indexing modes")
    void termsAggOnCategoryWithModes(@TempDir Path dir) throws Exception {
        Map<String, String> overrides = new HashMap<>();
        overrides.put("trace_id", ParquetCompanionConfig.StringIndexingMode.EXACT_ONLY);
        overrides.put("message", ParquetCompanionConfig.StringIndexingMode.TEXT_UUID_EXACTONLY);

        try (SplitSearcher s = createSearcher(dir, overrides,
                ParquetCompanionConfig.FastFieldMode.HYBRID, "agg_cat")) {
            s.preloadParquetFastFields("category").join();

            TermsAggregation agg = new TermsAggregation("cat_terms", "category", 10, 0);
            SearchResult result = s.search(new SplitMatchAllQuery(), 0, "terms", agg);

            assertTrue(result.hasAggregations());
            TermsResult terms = (TermsResult) result.getAggregation("terms");
            assertNotNull(terms);

            List<TermsResult.TermsBucket> buckets = terms.getBuckets();
            assertEquals(3, buckets.size(), "Should have 3 buckets: info, warn, error");

            Set<String> keys = new HashSet<>();
            for (TermsResult.TermsBucket b : buckets) {
                keys.add(b.getKey().toString());
                assertEquals(NUM_ROWS / 3, b.getDocCount(),
                        "Each category should have " + (NUM_ROWS / 3) + " docs");
            }
            assertTrue(keys.contains("info"), "Should have 'info' bucket");
            assertTrue(keys.contains("warn"), "Should have 'warn' bucket");
            assertTrue(keys.contains("error"), "Should have 'error' bucket");
        }
    }

    // ═══════════════════════════════════════════════════════════════
    //  22. Non-overridden fields are NOT affected
    // ═══════════════════════════════════════════════════════════════

    @Test @Order(22)
    @DisplayName("Non-overridden fields — category raw term query works normally")
    void nonOverriddenFieldsUnaffected(@TempDir Path dir) throws Exception {
        Map<String, String> overrides = new HashMap<>();
        overrides.put("trace_id", ParquetCompanionConfig.StringIndexingMode.EXACT_ONLY);
        // category NOT overridden — should use default "raw" tokenizer

        try (SplitSearcher s = createSearcher(dir, overrides, "non_override")) {
            // Raw tokenizer: "info" is indexed as-is
            SplitTermQuery q = new SplitTermQuery("category", "warn");
            SearchResult result = s.search(q, NUM_ROWS);
            assertEquals(NUM_ROWS / 3, result.getHits().size(),
                    "Non-overridden category field should work with raw term query");
        }
    }

    // ═══════════════════════════════════════════════════════════════
    //  23. Merged split with string indexing modes
    // ═══════════════════════════════════════════════════════════════

    @Test @Order(23)
    @DisplayName("Merge — two splits with same modes merge and search correctly")
    void mergedSplitWithStringIndexing(@TempDir Path dir) throws Exception {
        Map<String, String> overrides = new HashMap<>();
        overrides.put("trace_id", ParquetCompanionConfig.StringIndexingMode.EXACT_ONLY);
        overrides.put("message", ParquetCompanionConfig.StringIndexingMode.TEXT_UUID_EXACTONLY);

        int halfRows = NUM_ROWS / 2;

        // Create first split (rows 0..halfRows-1)
        Path pq1 = dir.resolve("merge1.parquet");
        Path split1 = dir.resolve("merge1.split");
        QuickwitSplit.nativeWriteTestParquetForStringIndexing(pq1.toString(), halfRows, 0);
        ParquetCompanionConfig config1 = new ParquetCompanionConfig(dir.toString())
                .withTokenizerOverrides(overrides);
        QuickwitSplit.SplitMetadata meta1 = QuickwitSplit.createFromParquet(
                Collections.singletonList(pq1.toString()), split1.toString(), config1);

        // Create second split (rows halfRows..NUM_ROWS-1)
        Path pq2 = dir.resolve("merge2.parquet");
        Path split2 = dir.resolve("merge2.split");
        QuickwitSplit.nativeWriteTestParquetForStringIndexing(pq2.toString(), halfRows, halfRows);
        ParquetCompanionConfig config2 = new ParquetCompanionConfig(dir.toString())
                .withTokenizerOverrides(overrides);
        QuickwitSplit.SplitMetadata meta2 = QuickwitSplit.createFromParquet(
                Collections.singletonList(pq2.toString()), split2.toString(), config2);

        // Merge the two splits
        Path mergedSplit = dir.resolve("merged.split");
        QuickwitSplit.SplitMetadata mergedMeta = QuickwitSplit.mergeSplits(
                Arrays.asList(split1.toString(), split2.toString()),
                mergedSplit.toString(),
                new QuickwitSplit.MergeConfig("merge-idx", "merge-src", "merge-node"));

        assertEquals(NUM_ROWS, mergedMeta.getNumDocs(),
                "Merged split should have " + NUM_ROWS + " docs");

        // Search the merged split
        String mergedUrl = "file://" + mergedSplit.toAbsolutePath();
        try (SplitSearcher s = cacheManager.createSplitSearcher(
                mergedUrl, mergedMeta, dir.toString())) {
            // Match all
            SearchResult allDocs = s.search(new SplitMatchAllQuery(), NUM_ROWS);
            assertEquals(NUM_ROWS, allDocs.getHits().size(),
                    "Merged split match-all should return all docs");

            // exact_only trace_id lookup
            String traceId;
            try (Document doc = s.docProjected(allDocs.getHits().get(0).getDocAddress())) {
                traceId = (String) doc.getFirst("trace_id");
            }
            SearchResult trResult = s.search(new SplitTermQuery("trace_id", traceId), 10);
            assertEquals(1, trResult.getHits().size(),
                    "exact_only should work on merged split");

            // text search on message
            SplitQuery textQ = s.parseQuery("message:completed");
            SearchResult textResult = s.search(textQ, NUM_ROWS);
            assertEquals(NUM_ROWS, textResult.getHits().size(),
                    "Text search should work on merged split");
        }
    }

    // ═══════════════════════════════════════════════════════════════
    //  24. PARQUET_ONLY mode + exact_only
    // ═══════════════════════════════════════════════════════════════

    @Test @Order(24)
    @DisplayName("PARQUET_ONLY mode — exact_only works with parquet_only fast field mode")
    void parquetOnlyModeExactOnly(@TempDir Path dir) throws Exception {
        Map<String, String> overrides = new HashMap<>();
        overrides.put("trace_id", ParquetCompanionConfig.StringIndexingMode.EXACT_ONLY);

        try (SplitSearcher s = createSearcher(dir, overrides,
                ParquetCompanionConfig.FastFieldMode.PARQUET_ONLY, "pqonly_eo")) {
            SearchResult allDocs = s.search(new SplitMatchAllQuery(), NUM_ROWS);
            assertEquals(NUM_ROWS, allDocs.getHits().size(),
                    "PARQUET_ONLY mode should contain all docs");

            // Verify exact_only term query
            String traceId;
            try (Document doc = s.docProjected(allDocs.getHits().get(0).getDocAddress())) {
                traceId = (String) doc.getFirst("trace_id");
                assertNotNull(traceId, "trace_id should be retrievable");
            }

            SearchResult result = s.search(new SplitTermQuery("trace_id", traceId), 10);
            assertEquals(1, result.getHits().size(),
                    "exact_only should work in PARQUET_ONLY mode");
        }
    }

    // ═══════════════════════════════════════════════════════════════
    //  25. PARQUET_ONLY mode + text_uuid_exactonly
    // ═══════════════════════════════════════════════════════════════

    @Test @Order(25)
    @DisplayName("PARQUET_ONLY mode — text_uuid_exactonly works with parquet_only fast field mode")
    void parquetOnlyModeTextUuidExactOnly(@TempDir Path dir) throws Exception {
        Map<String, String> overrides = new HashMap<>();
        overrides.put("message", ParquetCompanionConfig.StringIndexingMode.TEXT_UUID_EXACTONLY);

        try (SplitSearcher s = createSearcher(dir, overrides,
                ParquetCompanionConfig.FastFieldMode.PARQUET_ONLY, "pqonly_tue")) {
            SearchResult allDocs = s.search(new SplitMatchAllQuery(), 1);
            String message;
            try (Document doc = s.docProjected(allDocs.getHits().get(0).getDocAddress())) {
                message = (String) doc.getFirst("message");
            }

            String uuid = extractUuid(message);
            assertNotNull(uuid);

            // UUID term query via companion
            SearchResult uuidResult = s.search(new SplitTermQuery("message", uuid), 10);
            assertEquals(1, uuidResult.getHits().size(),
                    "text_uuid_exactonly companion redirect should work in PARQUET_ONLY mode");

            // Text search
            SplitQuery textQ = s.parseQuery("message:completed");
            SearchResult textResult = s.search(textQ, NUM_ROWS);
            assertEquals(NUM_ROWS, textResult.getHits().size(),
                    "Text search should work in PARQUET_ONLY mode after UUID stripping");
        }
    }

    // ═══════════════════════════════════════════════════════════════
    //  26. Exists query on exact_only field
    // ═══════════════════════════════════════════════════════════════

    @Test @Order(26)
    @DisplayName("Exists query — on non-overridden field with string indexing active")
    void existsQueryOnNonOverriddenField(@TempDir Path dir) throws Exception {
        Map<String, String> overrides = new HashMap<>();
        overrides.put("trace_id", ParquetCompanionConfig.StringIndexingMode.EXACT_ONLY);
        overrides.put("message", ParquetCompanionConfig.StringIndexingMode.TEXT_UUID_EXACTONLY);

        try (SplitSearcher s = createSearcher(dir, overrides, "exists_nonovr")) {
            // Exists query on category (non-overridden raw Str field) — should find all docs
            SplitExistsQuery existsQ = new SplitExistsQuery("category");
            SearchResult result = s.search(existsQ, NUM_ROWS);
            assertEquals(NUM_ROWS, result.getHits().size(),
                    "Exists query on non-overridden field should find all docs");
        }
    }

    // ═══════════════════════════════════════════════════════════════
    //  27. Terms aggregation on exact_only field (trace_id)
    // ═══════════════════════════════════════════════════════════════

    @Test @Order(27)
    @DisplayName("Terms aggregation — on exact_only field with HYBRID mode")
    void termsAggOnExactOnlyField(@TempDir Path dir) throws Exception {
        Map<String, String> overrides = new HashMap<>();
        overrides.put("trace_id", ParquetCompanionConfig.StringIndexingMode.EXACT_ONLY);

        try (SplitSearcher s = createSearcher(dir, overrides,
                ParquetCompanionConfig.FastFieldMode.HYBRID, "agg_eo")) {
            s.preloadParquetFastFields("trace_id").join();

            TermsAggregation agg = new TermsAggregation("trace_terms", "trace_id", NUM_ROWS, 0);
            SearchResult result = s.search(new SplitMatchAllQuery(), 0, "terms", agg);

            assertTrue(result.hasAggregations(), "Should have aggregation results");
            TermsResult terms = (TermsResult) result.getAggregation("terms");
            assertNotNull(terms, "terms aggregation result should not be null");

            // Each trace_id is unique, so we should get NUM_ROWS buckets (each with count 1)
            List<TermsResult.TermsBucket> buckets = terms.getBuckets();
            assertEquals(NUM_ROWS, buckets.size(),
                    "exact_only terms agg should produce " + NUM_ROWS + " buckets (one per unique trace_id)");

            for (TermsResult.TermsBucket b : buckets) {
                assertEquals(1, b.getDocCount(),
                        "Each trace_id bucket should have exactly 1 doc");
            }
        }
    }

    // ═══════════════════════════════════════════════════════════════
    //  28. Exists query on id field (numeric, not affected by string indexing)
    // ═══════════════════════════════════════════════════════════════

    @Test @Order(28)
    @DisplayName("Exists query — on id (numeric) field coexists with string indexing modes")
    void existsQueryOnNumericFieldWithModes(@TempDir Path dir) throws Exception {
        Map<String, String> overrides = new HashMap<>();
        overrides.put("trace_id", ParquetCompanionConfig.StringIndexingMode.EXACT_ONLY);
        overrides.put("message", ParquetCompanionConfig.StringIndexingMode.TEXT_UUID_EXACTONLY);
        overrides.put("error_log",
                ParquetCompanionConfig.StringIndexingMode.textCustomExactonly(ERR_PATTERN));

        try (SplitSearcher s = createSearcher(dir, overrides, "exists_id")) {
            // Exists query on "id" (numeric field) should find all docs
            SplitExistsQuery existsQ = new SplitExistsQuery("id");
            SearchResult result = s.search(existsQ, NUM_ROWS);
            assertEquals(NUM_ROWS, result.getHits().size(),
                    "Exists query on numeric 'id' field should find all docs");
        }
    }

    // ═══════════════════════════════════════════════════════════════
    //  29. Merged split preserves companion hash field functionality
    // ═══════════════════════════════════════════════════════════════

    @Test @Order(29)
    @DisplayName("Merge — companion UUID redirect still works after merge")
    void mergedSplitCompanionRedirectWorks(@TempDir Path dir) throws Exception {
        Map<String, String> overrides = new HashMap<>();
        overrides.put("message", ParquetCompanionConfig.StringIndexingMode.TEXT_UUID_EXACTONLY);
        overrides.put("error_log",
                ParquetCompanionConfig.StringIndexingMode.textCustomExactonly(ERR_PATTERN));

        int halfRows = NUM_ROWS / 2;

        // Create two splits
        Path pq1 = dir.resolve("merge_comp1.parquet");
        Path split1 = dir.resolve("merge_comp1.split");
        QuickwitSplit.nativeWriteTestParquetForStringIndexing(pq1.toString(), halfRows, 0);
        ParquetCompanionConfig config1 = new ParquetCompanionConfig(dir.toString())
                .withTokenizerOverrides(overrides);
        QuickwitSplit.SplitMetadata meta1 = QuickwitSplit.createFromParquet(
                Collections.singletonList(pq1.toString()), split1.toString(), config1);

        Path pq2 = dir.resolve("merge_comp2.parquet");
        Path split2 = dir.resolve("merge_comp2.split");
        QuickwitSplit.nativeWriteTestParquetForStringIndexing(pq2.toString(), halfRows, halfRows);
        ParquetCompanionConfig config2 = new ParquetCompanionConfig(dir.toString())
                .withTokenizerOverrides(overrides);
        QuickwitSplit.SplitMetadata meta2 = QuickwitSplit.createFromParquet(
                Collections.singletonList(pq2.toString()), split2.toString(), config2);

        // Merge
        Path mergedSplit = dir.resolve("merged_comp.split");
        QuickwitSplit.SplitMetadata mergedMeta = QuickwitSplit.mergeSplits(
                Arrays.asList(split1.toString(), split2.toString()),
                mergedSplit.toString(),
                new QuickwitSplit.MergeConfig("merge-comp-idx", "merge-comp-src", "merge-comp-node"));

        assertEquals(NUM_ROWS, mergedMeta.getNumDocs());

        // Search the merged split — verify companion redirect works
        String mergedUrl = "file://" + mergedSplit.toAbsolutePath();
        try (SplitSearcher s = cacheManager.createSplitSearcher(
                mergedUrl, mergedMeta, dir.toString())) {

            // Get a message and extract its UUID
            SearchResult allDocs = s.search(new SplitMatchAllQuery(), 1);
            String message;
            try (Document doc = s.docProjected(allDocs.getHits().get(0).getDocAddress())) {
                message = (String) doc.getFirst("message");
            }
            String uuid = extractUuid(message);
            assertNotNull(uuid, "Message should contain UUID: " + message);

            // UUID term query should redirect to companion after merge
            SearchResult uuidResult = s.search(new SplitTermQuery("message", uuid), 10);
            assertEquals(1, uuidResult.getHits().size(),
                    "Companion UUID redirect should work after merge for: " + uuid);

            // Get an error_log and extract ERR-XXXX
            String errorLog;
            try (Document doc = s.docProjected(allDocs.getHits().get(0).getDocAddress())) {
                errorLog = (String) doc.getFirst("error_log");
            }
            String errCode = extractErrCode(errorLog);
            assertNotNull(errCode, "error_log should contain ERR-XXXX: " + errorLog);

            // Custom pattern redirect should work after merge
            SearchResult errResult = s.search(new SplitTermQuery("error_log", errCode), 10);
            assertEquals(1, errResult.getHits().size(),
                    "Custom pattern companion redirect should work after merge for: " + errCode);
        }
    }

    // ═══════════════════════════════════════════════════════════════
    //  30. Wildcard on exact_only field throws clear error
    // ═══════════════════════════════════════════════════════════════

    @Test @Order(30)
    @DisplayName("exact_only — wildcard query throws error with clear message")
    void wildcardOnExactOnlyThrowsError(@TempDir Path dir) throws Exception {
        Map<String, String> overrides = new HashMap<>();
        overrides.put("trace_id", ParquetCompanionConfig.StringIndexingMode.EXACT_ONLY);

        try (SplitSearcher s = createSearcher(dir, overrides, "eo_wildcard_err")) {
            SplitWildcardQuery q = new SplitWildcardQuery("trace_id", "abc*");
            RuntimeException ex = assertThrows(RuntimeException.class,
                    () -> s.search(q, 10),
                    "Wildcard on exact_only field should throw");
            String msg = ex.getMessage();
            assertTrue(msg.contains("exact_only") || msg.contains("wildcard"),
                    "Error should mention exact_only or wildcard: " + msg);
        }
    }
}

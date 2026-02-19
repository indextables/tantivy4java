package io.indextables.tantivy4java;

import io.indextables.tantivy4java.core.FieldInfo;
import io.indextables.tantivy4java.core.FieldType;
import io.indextables.tantivy4java.core.Schema;
import io.indextables.tantivy4java.result.SearchResult;
import io.indextables.tantivy4java.split.*;
import io.indextables.tantivy4java.split.merge.*;

import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.nio.file.Path;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests that parquet companion indexing produces docMappingJson with the
 * __pq_file_hash and __pq_row_in_file tracking fields, and that
 * Schema.fromDocMappingJson() correctly reconstructs them as u64 fields.
 *
 * This reproduces the bug where:
 * 1. create_schema_from_doc_mapping is missing a "u64" type handler,
 *    so __pq fields are silently turned into text fields
 * 2. Opening a newly-indexed split fails with "indexed before merge-safe"
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class CompanionIndexerTypesTest {

    private static SplitCacheManager cacheManager;
    private static final ObjectMapper MAPPER = new ObjectMapper();

    @BeforeAll
    static void setUp() {
        SplitCacheManager.CacheConfig config = new SplitCacheManager.CacheConfig("companion-indexer-types-test")
                .withMaxCacheSize(100_000_000);
        cacheManager = SplitCacheManager.getInstance(config);
    }

    @AfterAll
    static void tearDown() {
        if (cacheManager != null) {
            try { cacheManager.close(); } catch (Exception e) { /* ignore */ }
        }
    }

    // ---------------------------------------------------------------
    // Test 1: docMappingJson from createFromParquet contains __pq fields
    // ---------------------------------------------------------------
    @Test
    @Order(1)
    void testDocMappingJsonContainsPqFields(@TempDir Path dir) throws Exception {
        Path parquetFile = dir.resolve("data.parquet");
        Path splitFile = dir.resolve("pq_fields.split");

        QuickwitSplit.nativeWriteTestParquet(parquetFile.toString(), 20, 0);

        ParquetCompanionConfig config = new ParquetCompanionConfig(dir.toString());

        QuickwitSplit.SplitMetadata metadata = QuickwitSplit.createFromParquet(
                Collections.singletonList(parquetFile.toString()),
                splitFile.toString(),
                config);

        assertNotNull(metadata);

        // The docMappingJson must contain the __pq tracking fields
        String docMapping = metadata.getDocMappingJson();
        assertNotNull(docMapping, "docMappingJson should not be null");

        JsonNode fields = MAPPER.readTree(docMapping);
        assertTrue(fields.isArray(), "docMappingJson should be a JSON array");

        boolean foundFileHash = false;
        boolean foundRowInFile = false;
        String fileHashType = null;
        String rowInFileType = null;

        for (JsonNode field : fields) {
            String name = field.get("name").asText();
            if ("__pq_file_hash".equals(name)) {
                foundFileHash = true;
                fileHashType = field.get("type").asText();
            }
            if ("__pq_row_in_file".equals(name)) {
                foundRowInFile = true;
                rowInFileType = field.get("type").asText();
            }
        }

        assertTrue(foundFileHash, "docMappingJson must contain __pq_file_hash field");
        assertTrue(foundRowInFile, "docMappingJson must contain __pq_row_in_file field");
        assertEquals("u64", fileHashType, "__pq_file_hash must have type 'u64'");
        assertEquals("u64", rowInFileType, "__pq_row_in_file must have type 'u64'");
    }

    // ---------------------------------------------------------------
    // Test 2: Schema.fromDocMappingJson correctly handles u64 type
    // (This is the core bug: missing "u64" handler in schema_creation.rs)
    // ---------------------------------------------------------------
    @Test
    @Order(2)
    void testSchemaFromDocMappingHandlesU64(@TempDir Path dir) throws Exception {
        Path parquetFile = dir.resolve("data.parquet");
        Path splitFile = dir.resolve("u64_schema.split");

        QuickwitSplit.nativeWriteTestParquet(parquetFile.toString(), 10, 0);

        ParquetCompanionConfig config = new ParquetCompanionConfig(dir.toString());

        QuickwitSplit.SplitMetadata metadata = QuickwitSplit.createFromParquet(
                Collections.singletonList(parquetFile.toString()),
                splitFile.toString(),
                config);

        String docMapping = metadata.getDocMappingJson();
        assertNotNull(docMapping);

        // Reconstruct schema from docMappingJson - this is the code path
        // used by the Spark layer (Schema.fromDocMappingJson)
        try (Schema schema = Schema.fromDocMappingJson(docMapping)) {
            // The __pq fields must exist
            assertTrue(schema.hasField("__pq_file_hash"),
                    "Schema from docMapping must have __pq_file_hash");
            assertTrue(schema.hasField("__pq_row_in_file"),
                    "Schema from docMapping must have __pq_row_in_file");

            // They must be UNSIGNED (u64), NOT TEXT
            FieldInfo fileHashInfo = schema.getFieldInfo("__pq_file_hash");
            assertNotNull(fileHashInfo, "FieldInfo for __pq_file_hash should not be null");
            assertEquals(FieldType.UNSIGNED, fileHashInfo.getType(),
                    "__pq_file_hash must be UNSIGNED type, not " + fileHashInfo.getType());

            FieldInfo rowInFileInfo = schema.getFieldInfo("__pq_row_in_file");
            assertNotNull(rowInFileInfo, "FieldInfo for __pq_row_in_file should not be null");
            assertEquals(FieldType.UNSIGNED, rowInFileInfo.getType(),
                    "__pq_row_in_file must be UNSIGNED type, not " + rowInFileInfo.getType());

            // Both should be fast fields
            assertTrue(fileHashInfo.isFast(), "__pq_file_hash must be a fast field");
            assertTrue(rowInFileInfo.isFast(), "__pq_row_in_file must be a fast field");
        }
    }

    // ---------------------------------------------------------------
    // Test 3: Minimal u64 docMapping round-trip (no parquet needed)
    // ---------------------------------------------------------------
    @Test
    @Order(3)
    void testMinimalU64DocMappingRoundTrip() {
        // Directly test Schema.fromDocMappingJson with u64 fields
        String minimalDocMapping = "[" +
                "{\"name\":\"my_u64_field\",\"type\":\"u64\",\"stored\":false,\"indexed\":true,\"fast\":true}," +
                "{\"name\":\"my_text_field\",\"type\":\"text\",\"stored\":true,\"indexed\":true,\"fast\":false}" +
                "]";

        try (Schema schema = Schema.fromDocMappingJson(minimalDocMapping)) {
            assertTrue(schema.hasField("my_u64_field"), "u64 field must exist");
            assertTrue(schema.hasField("my_text_field"), "text field must exist");

            FieldInfo u64Info = schema.getFieldInfo("my_u64_field");
            assertNotNull(u64Info);
            assertEquals(FieldType.UNSIGNED, u64Info.getType(),
                    "u64 field must be UNSIGNED, got " + u64Info.getType());
            assertTrue(u64Info.isFast(), "u64 field should be fast");

            FieldInfo textInfo = schema.getFieldInfo("my_text_field");
            assertNotNull(textInfo);
            assertEquals(FieldType.TEXT, textInfo.getType());
        }
    }

    // ---------------------------------------------------------------
    // Test 4: Minimal datetime, bytes, ip docMapping round-trip
    // ---------------------------------------------------------------
    @Test
    @Order(4)
    void testAllMissingTypesDocMappingRoundTrip() {
        String docMapping = "[" +
                "{\"name\":\"ts\",\"type\":\"datetime\",\"stored\":false,\"indexed\":true,\"fast\":true}," +
                "{\"name\":\"data\",\"type\":\"bytes\",\"stored\":true,\"indexed\":false,\"fast\":false}," +
                "{\"name\":\"addr\",\"type\":\"ip\",\"stored\":false,\"indexed\":true,\"fast\":true}" +
                "]";

        try (Schema schema = Schema.fromDocMappingJson(docMapping)) {
            FieldInfo tsInfo = schema.getFieldInfo("ts");
            assertNotNull(tsInfo, "datetime field must exist");
            assertEquals(FieldType.DATE, tsInfo.getType(),
                    "datetime field must be DATE, got " + tsInfo.getType());

            FieldInfo dataInfo = schema.getFieldInfo("data");
            assertNotNull(dataInfo, "bytes field must exist");
            assertEquals(FieldType.BYTES, dataInfo.getType(),
                    "bytes field must be BYTES, got " + dataInfo.getType());

            FieldInfo addrInfo = schema.getFieldInfo("addr");
            assertNotNull(addrInfo, "ip field must exist");
            assertEquals(FieldType.IP_ADDR, addrInfo.getType(),
                    "ip field must be IP_ADDR, got " + addrInfo.getType());
        }
    }

    // ---------------------------------------------------------------
    // Test 5: Opening a newly-indexed split must NOT throw "indexed before merge-safe"
    // ---------------------------------------------------------------
    @Test
    @Order(5)
    void testNewSplitOpensWithoutMergeSafeError(@TempDir Path dir) throws Exception {
        Path parquetFile = dir.resolve("data.parquet");
        Path splitFile = dir.resolve("merge_safe.split");

        QuickwitSplit.nativeWriteTestParquet(parquetFile.toString(), 30, 0);

        ParquetCompanionConfig config = new ParquetCompanionConfig(dir.toString());

        QuickwitSplit.SplitMetadata metadata = QuickwitSplit.createFromParquet(
                Collections.singletonList(parquetFile.toString()),
                splitFile.toString(),
                config);

        assertNotNull(metadata);
        assertEquals(30, metadata.getNumDocs());

        // This must NOT throw "This parquet companion split was indexed before merge-safe..."
        String splitUrl = "file://" + splitFile.toAbsolutePath();
        assertDoesNotThrow(() -> {
            try (SplitSearcher searcher = cacheManager.createSplitSearcher(splitUrl, metadata, dir.toString())) {
                assertTrue(searcher.hasParquetCompanion());

                // Also verify search works
                SplitQuery allQuery = searcher.parseQuery("*");
                SearchResult result = searcher.search(allQuery, 100);
                assertEquals(30, result.getHits().size());
            }
        }, "Newly-indexed companion split should NOT fail with merge-safe error");
    }

    // ---------------------------------------------------------------
    // Test 6: Re-indexing a split at a different path but SAME filename
    // must not get stale data from split_footer_cache.
    //
    // The Quickwit split_footer_cache keys by split_id (filename sans
    // ".split"), ignoring directory and footer offsets.  When a split is
    // re-indexed at a new location the cache may still hold the old
    // footer, causing the new split to be opened with the wrong meta.json.
    // ---------------------------------------------------------------
    @Test
    @Order(6)
    void testSameFilenameDifferentDirNoStaleCacheFooter(@TempDir Path dir1, @TempDir Path dir2) throws Exception {
        // ── Split A in dir1 ─────────────────────────────────────────
        Path pq1 = dir1.resolve("data.parquet");
        // Use "shard.split" as filename – the cache key will be "shard"
        Path split1 = dir1.resolve("shard.split");
        QuickwitSplit.nativeWriteTestParquet(pq1.toString(), 20, 0);

        ParquetCompanionConfig cfg1 = new ParquetCompanionConfig(dir1.toString());
        QuickwitSplit.SplitMetadata meta1 = QuickwitSplit.createFromParquet(
                Collections.singletonList(pq1.toString()), split1.toString(), cfg1);

        // Open split A – this populates split_footer_cache under key "shard"
        String url1 = "file://" + split1.toAbsolutePath();
        try (SplitSearcher s1 = cacheManager.createSplitSearcher(url1, meta1, dir1.toString())) {
            SplitQuery q = s1.parseQuery("*");
            assertEquals(20, s1.search(q, 100).getHits().size());
        }

        // ── Split B in dir2 (same filename "shard.split") ───────────
        Path pq2 = dir2.resolve("data.parquet");
        Path split2 = dir2.resolve("shard.split");
        QuickwitSplit.nativeWriteTestParquet(pq2.toString(), 40, 100);

        ParquetCompanionConfig cfg2 = new ParquetCompanionConfig(dir2.toString());
        QuickwitSplit.SplitMetadata meta2 = QuickwitSplit.createFromParquet(
                Collections.singletonList(pq2.toString()), split2.toString(), cfg2);

        // Open split B.  If the footer cache returns split A's stale footer
        // the opened index will have wrong segments / wrong doc count.
        String url2 = "file://" + split2.toAbsolutePath();
        try (SplitSearcher s2 = cacheManager.createSplitSearcher(url2, meta2, dir2.toString())) {
            assertTrue(s2.hasParquetCompanion());

            SplitQuery q = s2.parseQuery("*");
            SearchResult result = s2.search(q, 100);

            // Must see 40 docs from split B, NOT 20 from split A's stale cache
            assertEquals(40, result.getHits().size(),
                    "Split B (40 docs) must not be served stale data from split A (20 docs). " +
                    "Likely split_footer_cache collision on filename 'shard'.");
        }
    }
}

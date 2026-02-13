package io.indextables.tantivy4java;

import io.indextables.tantivy4java.core.Document;
import io.indextables.tantivy4java.core.Schema;
import io.indextables.tantivy4java.result.SearchResult;
import io.indextables.tantivy4java.split.*;
import io.indextables.tantivy4java.split.merge.*;

import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Integration tests for JSON string fields in Parquet Companion Mode.
 *
 * Tests that STRING columns containing raw JSON objects can be declared
 * via withJsonFields() and indexed as tantivy json_object fields,
 * enabling nested-path queries like payload.user:alice.
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class ParquetCompanionJsonFieldsTest {

    private static SplitCacheManager cacheManager;

    @BeforeAll
    static void setUp() {
        SplitCacheManager.CacheConfig config = new SplitCacheManager.CacheConfig("parquet-json-fields-test")
                .withMaxCacheSize(100_000_000);
        cacheManager = SplitCacheManager.getInstance(config);
    }

    @AfterAll
    static void tearDown() {
        if (cacheManager != null) {
            try { cacheManager.close(); } catch (Exception e) { /* ignore */ }
        }
    }

    /**
     * Creates a SplitSearcher with JSON string fields configured.
     * Writes 50 rows with payload and metadata as JSON string columns.
     */
    private SplitSearcher createSearcherWithJsonFields(Path dir, String tag) throws Exception {
        Path parquetFile = dir.resolve(tag + "_data.parquet");
        Path splitFile = dir.resolve(tag + ".split");

        QuickwitSplit.nativeWriteTestParquetWithJsonStrings(parquetFile.toString(), 50, 0);

        ParquetCompanionConfig config = new ParquetCompanionConfig(dir.toString())
                .withJsonFields("payload", "metadata");

        QuickwitSplit.SplitMetadata metadata = QuickwitSplit.createFromParquet(
                Collections.singletonList(parquetFile.toString()),
                splitFile.toString(),
                config);

        assertNotNull(metadata);
        assertEquals(50, metadata.getNumDocs());

        String splitUrl = "file://" + splitFile.toAbsolutePath();
        return cacheManager.createSplitSearcher(splitUrl, metadata, dir.toString());
    }

    @Test
    @Order(1)
    void testSchemaShowsJsonFields(@TempDir Path dir) throws Exception {
        try (SplitSearcher searcher = createSearcherWithJsonFields(dir, "schema")) {
            Schema schema = searcher.getSchema();
            assertNotNull(schema);
            List<String> fieldNames = schema.getFieldNames();
            assertTrue(fieldNames.contains("id"), "Should have id field");
            assertTrue(fieldNames.contains("name"), "Should have name field");
            assertTrue(fieldNames.contains("payload"), "Should have payload field");
            assertTrue(fieldNames.contains("metadata"), "Should have metadata field");
        }
    }

    @Test
    @Order(2)
    void testNestedTermQuery(@TempDir Path dir) throws Exception {
        try (SplitSearcher searcher = createSearcherWithJsonFields(dir, "nested_term")) {
            // Query nested path: payload.user:user_0
            SplitQuery query = searcher.parseQuery("payload.user:user_0");
            SearchResult results = searcher.search(query, 10);
            assertEquals(1, results.getHits().size(), "Should find exactly 1 hit for user_0");
        }
    }

    @Test
    @Order(3)
    void testNestedNumericTermQuery(@TempDir Path dir) throws Exception {
        try (SplitSearcher searcher = createSearcherWithJsonFields(dir, "nested_num")) {
            // Query nested numeric as term: payload.score:0
            // Score for idx=0 is 0
            SplitQuery query = searcher.parseQuery("payload.score:0");
            SearchResult results = searcher.search(query, 100);
            assertEquals(1, results.getHits().size(),
                    "Should find 1 hit for score:0 (idx=0)");
        }
    }

    @Test
    @Order(4)
    void testNestedBooleanQuery(@TempDir Path dir) throws Exception {
        try (SplitSearcher searcher = createSearcherWithJsonFields(dir, "nested_bool")) {
            // Query nested boolean: payload.active:true
            // active = idx % 2 == 0, so indices 0,2,4,...,48 → 25 hits
            SplitQuery query = searcher.parseQuery("payload.active:true");
            SearchResult results = searcher.search(query, 100);
            assertEquals(25, results.getHits().size(),
                    "Should find 25 hits for active:true (even indices 0..48)");
        }
    }

    @Test
    @Order(5)
    void testDeepNestedPath(@TempDir Path dir) throws Exception {
        try (SplitSearcher searcher = createSearcherWithJsonFields(dir, "deep_nested")) {
            // Query array element: payload.tags:tag_a
            // Every row has tags:["tag_a","tag_b"]
            SplitQuery query = searcher.parseQuery("payload.tags:tag_a");
            SearchResult results = searcher.search(query, 100);
            assertEquals(50, results.getHits().size(),
                    "Every row should match tag_a");
        }
    }

    @Test
    @Order(6)
    void testMetadataFieldQuery(@TempDir Path dir) throws Exception {
        try (SplitSearcher searcher = createSearcherWithJsonFields(dir, "metadata_q")) {
            // Query metadata nested path: metadata.version:0
            // version = idx, so version:0 → 1 hit (idx=0)
            SplitQuery query = searcher.parseQuery("metadata.version:0");
            SearchResult results = searcher.search(query, 100);
            assertEquals(1, results.getHits().size(),
                    "Should find 1 hit for version:0 (idx=0)");
        }
    }

    @Test
    @Order(7)
    void testCrossFieldBooleanQuery(@TempDir Path dir) throws Exception {
        try (SplitSearcher searcher = createSearcherWithJsonFields(dir, "cross_field")) {
            // payload.user:user_0 AND metadata.version:0
            // user_0 is idx=0, version:0 is idx=0
            // idx=0 satisfies both → 1 hit
            SplitQuery query = searcher.parseQuery("payload.user:user_0 AND metadata.version:0");
            SearchResult results = searcher.search(query, 10);
            assertEquals(1, results.getHits().size(),
                    "Should find 1 hit for user_0 AND version:0");
        }
    }

    @Test
    @Order(8)
    void testMatchAllWithJsonFields(@TempDir Path dir) throws Exception {
        try (SplitSearcher searcher = createSearcherWithJsonFields(dir, "matchall")) {
            SplitQuery query = searcher.parseQuery("*");
            SearchResult results = searcher.search(query, 100);
            assertEquals(50, results.getHits().size(), "Match all should return 50 hits");
        }
    }

    @Test
    @Order(9)
    void testDocRetrievalFromJsonField(@TempDir Path dir) throws Exception {
        try (SplitSearcher searcher = createSearcherWithJsonFields(dir, "doc_retr")) {
            // Retrieve a doc and check the companion parquet fields
            SplitQuery query = searcher.parseQuery("payload.user:user_0");
            SearchResult results = searcher.search(query, 1);
            assertEquals(1, results.getHits().size());

            assertTrue(searcher.hasParquetCompanion(),
                    "Should have parquet companion mode active");
        }
    }

    @Test
    @Order(10)
    void testConfigJsonSerialization() {
        ParquetCompanionConfig config = new ParquetCompanionConfig("/tmp/table")
                .withJsonFields("payload", "metadata")
                .withIpAddressFields("src_ip");

        String json = config.toIndexingConfigJson();
        assertTrue(json.contains("\"json_fields\""), "Should contain json_fields key");
        assertTrue(json.contains("\"payload\""), "Should contain payload in json_fields");
        assertTrue(json.contains("\"metadata\""), "Should contain metadata in json_fields");
        assertTrue(json.contains("\"ip_address_fields\""), "Should also contain ip_address_fields");
        assertTrue(json.contains("\"src_ip\""), "Should contain src_ip in ip_address_fields");
    }
}

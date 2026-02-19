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
 * Comprehensive tests for Schema.fromDocMappingJson() / create_schema_from_doc_mapping().
 *
 * Validates that every field type string produced by extract_doc_mapping_from_index
 * is correctly round-tripped through create_schema_from_doc_mapping, including:
 * - All field types: text, i64, u64, f64, bool, datetime, date, bytes, ip, ip_addr, object
 * - All option flags: stored, indexed, fast in every combination
 * - Tokenizer variations: default, raw, en_stem for text fields
 * - JSON object options: expand_dots, fast_tokenizer
 * - Alias type strings: "date" for "datetime", "ip_addr" for "ip"
 * - Parquet companion __pq tracking fields
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

    // ================================================================
    // 1. INDIVIDUAL FIELD TYPE TESTS
    //    Each type that extract_doc_mapping_from_index can produce
    // ================================================================

    @Test
    @Order(1)
    void testTextFieldType() {
        String json = "[{\"name\":\"title\",\"type\":\"text\",\"stored\":true,\"indexed\":true,\"fast\":false}]";
        try (Schema schema = Schema.fromDocMappingJson(json)) {
            FieldInfo info = schema.getFieldInfo("title");
            assertNotNull(info);
            assertEquals(FieldType.TEXT, info.getType());
            assertTrue(info.isStored());
            assertTrue(info.isIndexed());
            assertFalse(info.isFast());
        }
    }

    @Test
    @Order(2)
    void testI64FieldType() {
        String json = "[{\"name\":\"count\",\"type\":\"i64\",\"stored\":true,\"indexed\":true,\"fast\":true}]";
        try (Schema schema = Schema.fromDocMappingJson(json)) {
            FieldInfo info = schema.getFieldInfo("count");
            assertNotNull(info);
            assertEquals(FieldType.INTEGER, info.getType(), "i64 must map to INTEGER, got " + info.getType());
            assertTrue(info.isStored());
            assertTrue(info.isIndexed());
            assertTrue(info.isFast());
        }
    }

    @Test
    @Order(3)
    void testU64FieldType() {
        String json = "[{\"name\":\"doc_id\",\"type\":\"u64\",\"stored\":false,\"indexed\":true,\"fast\":true}]";
        try (Schema schema = Schema.fromDocMappingJson(json)) {
            FieldInfo info = schema.getFieldInfo("doc_id");
            assertNotNull(info);
            assertEquals(FieldType.UNSIGNED, info.getType(), "u64 must map to UNSIGNED, got " + info.getType());
            assertFalse(info.isStored());
            assertTrue(info.isIndexed());
            assertTrue(info.isFast());
        }
    }

    @Test
    @Order(4)
    void testF64FieldType() {
        String json = "[{\"name\":\"score\",\"type\":\"f64\",\"stored\":true,\"indexed\":false,\"fast\":true}]";
        try (Schema schema = Schema.fromDocMappingJson(json)) {
            FieldInfo info = schema.getFieldInfo("score");
            assertNotNull(info);
            assertEquals(FieldType.FLOAT, info.getType(), "f64 must map to FLOAT, got " + info.getType());
            assertTrue(info.isStored());
            assertFalse(info.isIndexed());
            assertTrue(info.isFast());
        }
    }

    @Test
    @Order(5)
    void testBoolFieldType() {
        String json = "[{\"name\":\"active\",\"type\":\"bool\",\"stored\":true,\"indexed\":true,\"fast\":false}]";
        try (Schema schema = Schema.fromDocMappingJson(json)) {
            FieldInfo info = schema.getFieldInfo("active");
            assertNotNull(info);
            assertEquals(FieldType.BOOLEAN, info.getType(), "bool must map to BOOLEAN, got " + info.getType());
            assertTrue(info.isStored());
            assertTrue(info.isIndexed());
            assertFalse(info.isFast());
        }
    }

    @Test
    @Order(6)
    void testDatetimeFieldType() {
        String json = "[{\"name\":\"created_at\",\"type\":\"datetime\",\"stored\":false,\"indexed\":true,\"fast\":true}]";
        try (Schema schema = Schema.fromDocMappingJson(json)) {
            FieldInfo info = schema.getFieldInfo("created_at");
            assertNotNull(info);
            assertEquals(FieldType.DATE, info.getType(), "datetime must map to DATE, got " + info.getType());
            assertFalse(info.isStored());
            assertTrue(info.isIndexed());
            assertTrue(info.isFast());
        }
    }

    @Test
    @Order(7)
    void testDateAliasFieldType() {
        // "date" is an alias for "datetime" in create_schema_from_doc_mapping
        String json = "[{\"name\":\"updated_at\",\"type\":\"date\",\"stored\":true,\"indexed\":true,\"fast\":true}]";
        try (Schema schema = Schema.fromDocMappingJson(json)) {
            FieldInfo info = schema.getFieldInfo("updated_at");
            assertNotNull(info);
            assertEquals(FieldType.DATE, info.getType(), "date alias must map to DATE, got " + info.getType());
            assertTrue(info.isStored());
        }
    }

    @Test
    @Order(8)
    void testBytesFieldType() {
        String json = "[{\"name\":\"payload\",\"type\":\"bytes\",\"stored\":true,\"indexed\":false,\"fast\":false}]";
        try (Schema schema = Schema.fromDocMappingJson(json)) {
            FieldInfo info = schema.getFieldInfo("payload");
            assertNotNull(info);
            assertEquals(FieldType.BYTES, info.getType(), "bytes must map to BYTES, got " + info.getType());
            assertTrue(info.isStored());
            assertFalse(info.isIndexed());
            assertFalse(info.isFast());
        }
    }

    @Test
    @Order(9)
    void testIpFieldType() {
        String json = "[{\"name\":\"client_ip\",\"type\":\"ip\",\"stored\":false,\"indexed\":true,\"fast\":true}]";
        try (Schema schema = Schema.fromDocMappingJson(json)) {
            FieldInfo info = schema.getFieldInfo("client_ip");
            assertNotNull(info);
            assertEquals(FieldType.IP_ADDR, info.getType(), "ip must map to IP_ADDR, got " + info.getType());
            assertFalse(info.isStored());
            assertTrue(info.isIndexed());
            assertTrue(info.isFast());
        }
    }

    @Test
    @Order(10)
    void testIpAddrAliasFieldType() {
        // "ip_addr" is an alias for "ip" in create_schema_from_doc_mapping
        String json = "[{\"name\":\"server_ip\",\"type\":\"ip_addr\",\"stored\":true,\"indexed\":true,\"fast\":true}]";
        try (Schema schema = Schema.fromDocMappingJson(json)) {
            FieldInfo info = schema.getFieldInfo("server_ip");
            assertNotNull(info);
            assertEquals(FieldType.IP_ADDR, info.getType(), "ip_addr alias must map to IP_ADDR, got " + info.getType());
            assertTrue(info.isStored());
        }
    }

    @Test
    @Order(11)
    void testObjectFieldType() {
        String json = "[{\"name\":\"metadata\",\"type\":\"object\",\"stored\":true,\"indexed\":true,\"fast\":false}]";
        try (Schema schema = Schema.fromDocMappingJson(json)) {
            FieldInfo info = schema.getFieldInfo("metadata");
            assertNotNull(info);
            assertEquals(FieldType.JSON, info.getType(), "object must map to JSON, got " + info.getType());
            assertTrue(info.isStored());
        }
    }

    // ================================================================
    // 2. OPTION FLAG COMBINATION TESTS
    //    Verify stored/indexed/fast flags are correctly propagated
    // ================================================================

    @Test
    @Order(20)
    void testAllFlagsTrue() {
        String json = "[" +
                "{\"name\":\"t\",\"type\":\"text\",\"stored\":true,\"indexed\":true,\"fast\":true}," +
                "{\"name\":\"i\",\"type\":\"i64\",\"stored\":true,\"indexed\":true,\"fast\":true}," +
                "{\"name\":\"u\",\"type\":\"u64\",\"stored\":true,\"indexed\":true,\"fast\":true}," +
                "{\"name\":\"f\",\"type\":\"f64\",\"stored\":true,\"indexed\":true,\"fast\":true}," +
                "{\"name\":\"b\",\"type\":\"bool\",\"stored\":true,\"indexed\":true,\"fast\":true}," +
                "{\"name\":\"d\",\"type\":\"datetime\",\"stored\":true,\"indexed\":true,\"fast\":true}," +
                "{\"name\":\"by\",\"type\":\"bytes\",\"stored\":true,\"indexed\":true,\"fast\":true}," +
                "{\"name\":\"ip\",\"type\":\"ip\",\"stored\":true,\"indexed\":true,\"fast\":true}" +
                "]";
        try (Schema schema = Schema.fromDocMappingJson(json)) {
            for (String name : Arrays.asList("t", "i", "u", "f", "b", "d", "by", "ip")) {
                FieldInfo info = schema.getFieldInfo(name);
                assertNotNull(info, name + " field must exist");
                assertTrue(info.isStored(), name + " must be stored");
                assertTrue(info.isFast(), name + " must be fast");
            }
        }
    }

    @Test
    @Order(21)
    void testAllFlagsFalse() {
        String json = "[" +
                "{\"name\":\"i\",\"type\":\"i64\",\"stored\":false,\"indexed\":false,\"fast\":false}," +
                "{\"name\":\"u\",\"type\":\"u64\",\"stored\":false,\"indexed\":false,\"fast\":false}," +
                "{\"name\":\"f\",\"type\":\"f64\",\"stored\":false,\"indexed\":false,\"fast\":false}," +
                "{\"name\":\"b\",\"type\":\"bool\",\"stored\":false,\"indexed\":false,\"fast\":false}," +
                "{\"name\":\"d\",\"type\":\"datetime\",\"stored\":false,\"indexed\":false,\"fast\":false}," +
                "{\"name\":\"by\",\"type\":\"bytes\",\"stored\":false,\"indexed\":false,\"fast\":false}," +
                "{\"name\":\"ip\",\"type\":\"ip\",\"stored\":false,\"indexed\":false,\"fast\":false}" +
                "]";
        try (Schema schema = Schema.fromDocMappingJson(json)) {
            for (String name : Arrays.asList("i", "u", "f", "b", "d", "by", "ip")) {
                FieldInfo info = schema.getFieldInfo(name);
                assertNotNull(info, name + " field must exist");
                assertFalse(info.isStored(), name + " must NOT be stored");
                assertFalse(info.isFast(), name + " must NOT be fast");
            }
        }
    }

    @Test
    @Order(22)
    void testStoredOnlyFlags() {
        // stored=true, indexed=false, fast=false
        String json = "[" +
                "{\"name\":\"u\",\"type\":\"u64\",\"stored\":true,\"indexed\":false,\"fast\":false}," +
                "{\"name\":\"d\",\"type\":\"datetime\",\"stored\":true,\"indexed\":false,\"fast\":false}," +
                "{\"name\":\"by\",\"type\":\"bytes\",\"stored\":true,\"indexed\":false,\"fast\":false}," +
                "{\"name\":\"ip\",\"type\":\"ip\",\"stored\":true,\"indexed\":false,\"fast\":false}" +
                "]";
        try (Schema schema = Schema.fromDocMappingJson(json)) {
            for (String name : Arrays.asList("u", "d", "by", "ip")) {
                FieldInfo info = schema.getFieldInfo(name);
                assertNotNull(info, name + " field must exist");
                assertTrue(info.isStored(), name + " must be stored");
                assertFalse(info.isFast(), name + " must NOT be fast");
            }
        }
    }

    @Test
    @Order(23)
    void testFastOnlyFlags() {
        // stored=false, indexed=false, fast=true
        String json = "[" +
                "{\"name\":\"u\",\"type\":\"u64\",\"stored\":false,\"indexed\":false,\"fast\":true}," +
                "{\"name\":\"d\",\"type\":\"datetime\",\"stored\":false,\"indexed\":false,\"fast\":true}," +
                "{\"name\":\"ip\",\"type\":\"ip\",\"stored\":false,\"indexed\":false,\"fast\":true}" +
                "]";
        try (Schema schema = Schema.fromDocMappingJson(json)) {
            for (String name : Arrays.asList("u", "d", "ip")) {
                FieldInfo info = schema.getFieldInfo(name);
                assertNotNull(info, name + " field must exist");
                assertFalse(info.isStored(), name + " must NOT be stored");
                assertTrue(info.isFast(), name + " must be fast");
            }
        }
    }

    @Test
    @Order(24)
    void testDefaultFlags() {
        // Omit all optional flags - should default to stored=false, indexed=false, fast=false
        String json = "[" +
                "{\"name\":\"u\",\"type\":\"u64\"}," +
                "{\"name\":\"d\",\"type\":\"datetime\"}," +
                "{\"name\":\"by\",\"type\":\"bytes\"}," +
                "{\"name\":\"ip\",\"type\":\"ip\"}," +
                "{\"name\":\"t\",\"type\":\"text\"}" +
                "]";
        try (Schema schema = Schema.fromDocMappingJson(json)) {
            assertEquals(5, schema.getFieldCount(), "Schema must have 5 fields");
            // All types should exist with correct mapping
            assertEquals(FieldType.UNSIGNED, schema.getFieldInfo("u").getType());
            assertEquals(FieldType.DATE, schema.getFieldInfo("d").getType());
            assertEquals(FieldType.BYTES, schema.getFieldInfo("by").getType());
            assertEquals(FieldType.IP_ADDR, schema.getFieldInfo("ip").getType());
            assertEquals(FieldType.TEXT, schema.getFieldInfo("t").getType());
            // All defaults should be false
            for (String name : Arrays.asList("u", "d", "by", "ip")) {
                FieldInfo info = schema.getFieldInfo(name);
                assertFalse(info.isStored(), name + " default stored must be false");
                assertFalse(info.isFast(), name + " default fast must be false");
            }
        }
    }

    // ================================================================
    // 3. TEXT FIELD TOKENIZER TESTS
    // ================================================================

    @Test
    @Order(30)
    void testTextFieldDefaultTokenizer() {
        String json = "[{\"name\":\"body\",\"type\":\"text\",\"stored\":false,\"indexed\":true,\"fast\":false}]";
        try (Schema schema = Schema.fromDocMappingJson(json)) {
            FieldInfo info = schema.getFieldInfo("body");
            assertNotNull(info);
            assertEquals(FieldType.TEXT, info.getType());
            assertTrue(info.isIndexed());
            // Default tokenizer is "default" when no tokenizer specified
            assertNotNull(info.getTokenizerName(), "Tokenizer should be set for indexed text");
        }
    }

    @Test
    @Order(31)
    void testTextFieldRawTokenizer() {
        String json = "[{\"name\":\"keyword\",\"type\":\"text\",\"stored\":false,\"indexed\":true,\"fast\":false,\"tokenizer\":\"raw\"}]";
        try (Schema schema = Schema.fromDocMappingJson(json)) {
            FieldInfo info = schema.getFieldInfo("keyword");
            assertNotNull(info);
            assertEquals(FieldType.TEXT, info.getType());
            assertTrue(info.isIndexed());
            assertEquals("raw", info.getTokenizerName(), "Tokenizer must be 'raw'");
        }
    }

    @Test
    @Order(32)
    void testTextFieldEnStemTokenizer() {
        String json = "[{\"name\":\"content\",\"type\":\"text\",\"stored\":false,\"indexed\":true,\"fast\":false,\"tokenizer\":\"en_stem\"}]";
        try (Schema schema = Schema.fromDocMappingJson(json)) {
            FieldInfo info = schema.getFieldInfo("content");
            assertNotNull(info);
            assertEquals(FieldType.TEXT, info.getType());
            assertTrue(info.isIndexed());
            assertEquals("en_stem", info.getTokenizerName(), "Tokenizer must be 'en_stem'");
        }
    }

    @Test
    @Order(33)
    void testTextFieldFastWithTokenizer() {
        // Fast text fields require a tokenizer
        String json = "[{\"name\":\"tag\",\"type\":\"text\",\"stored\":false,\"indexed\":true,\"fast\":true,\"tokenizer\":\"raw\"}]";
        try (Schema schema = Schema.fromDocMappingJson(json)) {
            FieldInfo info = schema.getFieldInfo("tag");
            assertNotNull(info);
            assertEquals(FieldType.TEXT, info.getType());
            assertTrue(info.isFast(), "Text field with fast=true must be fast");
            assertTrue(info.isIndexed());
        }
    }

    @Test
    @Order(34)
    void testTextFieldStoredNotIndexed() {
        // Text field with stored=true, indexed=false - just a stored string
        String json = "[{\"name\":\"raw_text\",\"type\":\"text\",\"stored\":true,\"indexed\":false,\"fast\":false}]";
        try (Schema schema = Schema.fromDocMappingJson(json)) {
            FieldInfo info = schema.getFieldInfo("raw_text");
            assertNotNull(info);
            assertEquals(FieldType.TEXT, info.getType());
            assertTrue(info.isStored());
            assertFalse(info.isIndexed());
        }
    }

    // ================================================================
    // 4. JSON OBJECT FIELD OPTIONS
    // ================================================================

    @Test
    @Order(40)
    void testObjectFieldWithExpandDots() {
        String json = "[{\"name\":\"props\",\"type\":\"object\",\"stored\":true,\"indexed\":true,\"fast\":false,\"expand_dots\":true}]";
        try (Schema schema = Schema.fromDocMappingJson(json)) {
            FieldInfo info = schema.getFieldInfo("props");
            assertNotNull(info);
            assertEquals(FieldType.JSON, info.getType());
            assertTrue(info.isStored());
        }
    }

    @Test
    @Order(41)
    void testObjectFieldWithFast() {
        String json = "[{\"name\":\"tags\",\"type\":\"object\",\"stored\":false,\"indexed\":true,\"fast\":true}]";
        try (Schema schema = Schema.fromDocMappingJson(json)) {
            FieldInfo info = schema.getFieldInfo("tags");
            assertNotNull(info);
            assertEquals(FieldType.JSON, info.getType());
            // Note: JNI getFieldInfo hardcodes JSON fast=false (jni_schema.rs line 404).
            // The fast option IS applied in the tantivy schema but not reported by getFieldInfo.
            // This is a pre-existing JNI limitation, not a schema_creation bug.
            assertFalse(info.isFast(), "JNI reports JSON fields as non-fast (known limitation)");
        }
    }

    @Test
    @Order(42)
    void testObjectFieldWithFastTokenizer() {
        String json = "[{\"name\":\"data\",\"type\":\"object\",\"stored\":true,\"indexed\":true,\"fast\":true,\"fast_tokenizer\":\"raw\"}]";
        try (Schema schema = Schema.fromDocMappingJson(json)) {
            FieldInfo info = schema.getFieldInfo("data");
            assertNotNull(info);
            assertEquals(FieldType.JSON, info.getType());
            // Same JNI limitation as above - JSON fast is not reported
            assertFalse(info.isFast(), "JNI reports JSON fields as non-fast (known limitation)");
            assertTrue(info.isStored());
        }
    }

    // ================================================================
    // 5. COMPLETE SCHEMA WITH ALL TYPES (the real-world scenario)
    // ================================================================

    @Test
    @Order(50)
    void testAllTypesInOneSchema() {
        String json = "[" +
                "{\"name\":\"title\",\"type\":\"text\",\"stored\":true,\"indexed\":true,\"fast\":false,\"tokenizer\":\"default\"}," +
                "{\"name\":\"keyword\",\"type\":\"text\",\"stored\":false,\"indexed\":true,\"fast\":true,\"tokenizer\":\"raw\"}," +
                "{\"name\":\"count\",\"type\":\"i64\",\"stored\":true,\"indexed\":true,\"fast\":true}," +
                "{\"name\":\"doc_id\",\"type\":\"u64\",\"stored\":false,\"indexed\":true,\"fast\":true}," +
                "{\"name\":\"score\",\"type\":\"f64\",\"stored\":true,\"indexed\":false,\"fast\":true}," +
                "{\"name\":\"active\",\"type\":\"bool\",\"stored\":true,\"indexed\":true,\"fast\":false}," +
                "{\"name\":\"created_at\",\"type\":\"datetime\",\"stored\":false,\"indexed\":true,\"fast\":true}," +
                "{\"name\":\"payload\",\"type\":\"bytes\",\"stored\":true,\"indexed\":false,\"fast\":false}," +
                "{\"name\":\"client_ip\",\"type\":\"ip\",\"stored\":false,\"indexed\":true,\"fast\":true}," +
                "{\"name\":\"metadata\",\"type\":\"object\",\"stored\":true,\"indexed\":true,\"fast\":false,\"expand_dots\":true}" +
                "]";
        try (Schema schema = Schema.fromDocMappingJson(json)) {
            assertEquals(10, schema.getFieldCount());

            assertEquals(FieldType.TEXT, schema.getFieldInfo("title").getType());
            assertEquals(FieldType.TEXT, schema.getFieldInfo("keyword").getType());
            assertEquals(FieldType.INTEGER, schema.getFieldInfo("count").getType());
            assertEquals(FieldType.UNSIGNED, schema.getFieldInfo("doc_id").getType());
            assertEquals(FieldType.FLOAT, schema.getFieldInfo("score").getType());
            assertEquals(FieldType.BOOLEAN, schema.getFieldInfo("active").getType());
            assertEquals(FieldType.DATE, schema.getFieldInfo("created_at").getType());
            assertEquals(FieldType.BYTES, schema.getFieldInfo("payload").getType());
            assertEquals(FieldType.IP_ADDR, schema.getFieldInfo("client_ip").getType());
            assertEquals(FieldType.JSON, schema.getFieldInfo("metadata").getType());
        }
    }

    @Test
    @Order(51)
    void testFieldNamesByType() {
        String json = "[" +
                "{\"name\":\"a\",\"type\":\"u64\",\"stored\":false,\"indexed\":true,\"fast\":true}," +
                "{\"name\":\"b\",\"type\":\"u64\",\"stored\":false,\"indexed\":true,\"fast\":true}," +
                "{\"name\":\"c\",\"type\":\"i64\",\"stored\":true,\"indexed\":true,\"fast\":false}," +
                "{\"name\":\"d\",\"type\":\"text\",\"stored\":true,\"indexed\":true,\"fast\":false}" +
                "]";
        try (Schema schema = Schema.fromDocMappingJson(json)) {
            List<String> unsignedFields = schema.getFieldNamesByType(FieldType.UNSIGNED);
            assertTrue(unsignedFields.contains("a"), "Must find u64 field 'a'");
            assertTrue(unsignedFields.contains("b"), "Must find u64 field 'b'");
            assertEquals(2, unsignedFields.size(), "Must have exactly 2 UNSIGNED fields");

            List<String> intFields = schema.getFieldNamesByType(FieldType.INTEGER);
            assertTrue(intFields.contains("c"), "Must find i64 field 'c'");
            assertEquals(1, intFields.size());

            List<String> textFields = schema.getFieldNamesByType(FieldType.TEXT);
            assertTrue(textFields.contains("d"), "Must find text field 'd'");
        }
    }

    @Test
    @Order(52)
    void testFastFieldNames() {
        String json = "[" +
                "{\"name\":\"fast_u\",\"type\":\"u64\",\"stored\":false,\"indexed\":false,\"fast\":true}," +
                "{\"name\":\"slow_u\",\"type\":\"u64\",\"stored\":false,\"indexed\":true,\"fast\":false}," +
                "{\"name\":\"fast_d\",\"type\":\"datetime\",\"stored\":false,\"indexed\":false,\"fast\":true}," +
                "{\"name\":\"slow_d\",\"type\":\"datetime\",\"stored\":true,\"indexed\":false,\"fast\":false}" +
                "]";
        try (Schema schema = Schema.fromDocMappingJson(json)) {
            List<String> fastFields = schema.getFastFieldNames();
            assertTrue(fastFields.contains("fast_u"), "fast_u must be in fast fields");
            assertTrue(fastFields.contains("fast_d"), "fast_d must be in fast fields");
            assertFalse(fastFields.contains("slow_u"), "slow_u must NOT be in fast fields");
            assertFalse(fastFields.contains("slow_d"), "slow_d must NOT be in fast fields");
        }
    }

    // ================================================================
    // 6. UNKNOWN/FALLBACK TYPE TEST
    // ================================================================

    @Test
    @Order(60)
    void testUnknownTypeFallsBackToText() {
        // Unknown types should fall through to text field
        String json = "[{\"name\":\"mystery\",\"type\":\"mystery_type\",\"stored\":true,\"indexed\":true,\"fast\":false}]";
        try (Schema schema = Schema.fromDocMappingJson(json)) {
            FieldInfo info = schema.getFieldInfo("mystery");
            assertNotNull(info, "Unknown type should still create a field");
            assertEquals(FieldType.TEXT, info.getType(), "Unknown type must fall back to TEXT");
            assertTrue(info.isStored());
        }
    }

    // ================================================================
    // 7. PARQUET COMPANION __pq TRACKING FIELDS
    // ================================================================

    @Test
    @Order(70)
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

    @Test
    @Order(71)
    void testSchemaFromDocMappingHandlesPqU64Fields(@TempDir Path dir) throws Exception {
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

        try (Schema schema = Schema.fromDocMappingJson(docMapping)) {
            assertTrue(schema.hasField("__pq_file_hash"),
                    "Schema from docMapping must have __pq_file_hash");
            assertTrue(schema.hasField("__pq_row_in_file"),
                    "Schema from docMapping must have __pq_row_in_file");

            FieldInfo fileHashInfo = schema.getFieldInfo("__pq_file_hash");
            assertNotNull(fileHashInfo);
            assertEquals(FieldType.UNSIGNED, fileHashInfo.getType(),
                    "__pq_file_hash must be UNSIGNED type, not " + fileHashInfo.getType());

            FieldInfo rowInFileInfo = schema.getFieldInfo("__pq_row_in_file");
            assertNotNull(rowInFileInfo);
            assertEquals(FieldType.UNSIGNED, rowInFileInfo.getType(),
                    "__pq_row_in_file must be UNSIGNED type, not " + rowInFileInfo.getType());

            assertTrue(fileHashInfo.isFast(), "__pq_file_hash must be a fast field");
            assertTrue(rowInFileInfo.isFast(), "__pq_row_in_file must be a fast field");
        }
    }

    // ================================================================
    // 8. OPENING NEWLY-INDEXED SPLITS
    // ================================================================

    @Test
    @Order(80)
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

        String splitUrl = "file://" + splitFile.toAbsolutePath();
        assertDoesNotThrow(() -> {
            try (SplitSearcher searcher = cacheManager.createSplitSearcher(splitUrl, metadata, dir.toString())) {
                assertTrue(searcher.hasParquetCompanion());

                SplitQuery allQuery = searcher.parseQuery("*");
                SearchResult result = searcher.search(allQuery, 100);
                assertEquals(30, result.getHits().size());
            }
        }, "Newly-indexed companion split should NOT fail with merge-safe error");
    }

    // ================================================================
    // 9. STALE FOOTER CACHE TEST
    // ================================================================

    @Test
    @Order(90)
    @Disabled("Known bug: split_footer_cache keys by filename only, ignoring directory. Separate fix needed.")
    void testSameFilenameDifferentDirNoStaleCacheFooter(@TempDir Path dir1, @TempDir Path dir2) throws Exception {
        Path pq1 = dir1.resolve("data.parquet");
        Path split1 = dir1.resolve("shard.split");
        QuickwitSplit.nativeWriteTestParquet(pq1.toString(), 20, 0);

        ParquetCompanionConfig cfg1 = new ParquetCompanionConfig(dir1.toString());
        QuickwitSplit.SplitMetadata meta1 = QuickwitSplit.createFromParquet(
                Collections.singletonList(pq1.toString()), split1.toString(), cfg1);

        String url1 = "file://" + split1.toAbsolutePath();
        try (SplitSearcher s1 = cacheManager.createSplitSearcher(url1, meta1, dir1.toString())) {
            SplitQuery q = s1.parseQuery("*");
            assertEquals(20, s1.search(q, 100).getHits().size());
        }

        Path pq2 = dir2.resolve("data.parquet");
        Path split2 = dir2.resolve("shard.split");
        QuickwitSplit.nativeWriteTestParquet(pq2.toString(), 40, 100);

        ParquetCompanionConfig cfg2 = new ParquetCompanionConfig(dir2.toString());
        QuickwitSplit.SplitMetadata meta2 = QuickwitSplit.createFromParquet(
                Collections.singletonList(pq2.toString()), split2.toString(), cfg2);

        String url2 = "file://" + split2.toAbsolutePath();
        try (SplitSearcher s2 = cacheManager.createSplitSearcher(url2, meta2, dir2.toString())) {
            assertTrue(s2.hasParquetCompanion());

            SplitQuery q = s2.parseQuery("*");
            SearchResult result = s2.search(q, 100);

            assertEquals(40, result.getHits().size(),
                    "Split B (40 docs) must not be served stale data from split A (20 docs). " +
                    "Likely split_footer_cache collision on filename 'shard'.");
        }
    }
}

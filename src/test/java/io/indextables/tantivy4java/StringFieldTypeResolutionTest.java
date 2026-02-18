package io.indextables.tantivy4java;

import io.indextables.tantivy4java.core.*;
import io.indextables.tantivy4java.split.*;
import io.indextables.tantivy4java.split.merge.*;
import io.indextables.tantivy4java.result.*;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;

import java.nio.file.Path;
import java.util.List;

/**
 * Regression test for: Schema.getFieldInfo() returns FieldType.TEXT for STRING (raw tokenizer) fields.
 *
 * BUG: nativeGetFieldInfo and nativeGetAllFieldInfo in jni_schema.rs always return
 * FieldType.TEXT (value 1) for all TantivyFieldType::Str fields, regardless of tokenizer.
 * Fields with "raw" tokenizer should return FieldType.STRING (value 11).
 *
 * IMPACT: When IndexTables4Spark pushes down an EqualTo filter on a string field,
 * it checks getFieldInfo().getType() to decide query type. Because STRING fields
 * are misreported as TEXT, the code generates phrase queries instead of term queries,
 * causing incorrect results and poor performance.
 */
public class StringFieldTypeResolutionTest {

    @Test
    @DisplayName("BUG: Schema.getFieldInfo() should return STRING for raw-tokenizer fields")
    public void testFieldTypeResolution_StringVsText(@TempDir Path tempDir) {
        String indexPath = tempDir.resolve("field_type_test_index").toString();
        String splitPath = tempDir.resolve("field_type_test.split").toString();

        try (SchemaBuilder builder = new SchemaBuilder()) {
            // STRING field: raw tokenizer, exact matching
            builder.addStringField("hostname", true, true, false);
            // TEXT field: default tokenizer, tokenized
            builder.addTextField("description", true, false, "default", "position");
            // ID for reference
            builder.addIntegerField("id", true, true, true);

            try (Schema schema = builder.build()) {
                // --- Test 1: Verify field types from Schema directly ---
                FieldInfo hostnameInfo = schema.getFieldInfo("hostname");
                FieldInfo descriptionInfo = schema.getFieldInfo("description");

                assertNotNull(hostnameInfo, "hostname FieldInfo should not be null");
                assertNotNull(descriptionInfo, "description FieldInfo should not be null");

                System.out.println("hostname FieldInfo: " + hostnameInfo);
                System.out.println("description FieldInfo: " + descriptionInfo);
                System.out.println("hostname type: " + hostnameInfo.getType() + " (expected: STRING)");
                System.out.println("hostname tokenizer: " + hostnameInfo.getTokenizerName() + " (expected: raw)");
                System.out.println("description type: " + descriptionInfo.getType() + " (expected: TEXT)");
                System.out.println("description tokenizer: " + descriptionInfo.getTokenizerName() + " (expected: default)");

                // THIS IS THE BUG: hostname returns TEXT instead of STRING
                assertEquals(FieldType.STRING, hostnameInfo.getType(),
                    "String field (raw tokenizer) should report FieldType.STRING, not TEXT. " +
                    "Actual tokenizer is: " + hostnameInfo.getTokenizerName());

                assertEquals(FieldType.TEXT, descriptionInfo.getType(),
                    "Text field (default tokenizer) should report FieldType.TEXT");

                assertEquals("raw", hostnameInfo.getTokenizerName(),
                    "String field should have 'raw' tokenizer");

                assertEquals("default", descriptionInfo.getTokenizerName(),
                    "Text field should have 'default' tokenizer");

                // --- Test 2: Verify getAllFieldInfo returns correct types ---
                List<FieldInfo> allFields = schema.getAllFieldInfo();
                FieldInfo hostnameFromAll = allFields.stream()
                    .filter(f -> f.getName().equals("hostname"))
                    .findFirst()
                    .orElse(null);
                FieldInfo descriptionFromAll = allFields.stream()
                    .filter(f -> f.getName().equals("description"))
                    .findFirst()
                    .orElse(null);

                assertNotNull(hostnameFromAll, "hostname should be in getAllFieldInfo");
                assertNotNull(descriptionFromAll, "description should be in getAllFieldInfo");

                assertEquals(FieldType.STRING, hostnameFromAll.getType(),
                    "getAllFieldInfo: String field should report FieldType.STRING");
                assertEquals(FieldType.TEXT, descriptionFromAll.getType(),
                    "getAllFieldInfo: Text field should report FieldType.TEXT");
            }
        }
    }

    @Test
    @DisplayName("BUG: SplitTermQuery should be used for STRING fields, not phrase queries")
    public void testSplitQueryType_StringFieldUsesTermQuery(@TempDir Path tempDir) {
        String indexPath = tempDir.resolve("query_type_test_index").toString();
        String splitPath = tempDir.resolve("query_type_test.split").toString();

        try (SchemaBuilder builder = new SchemaBuilder()) {
            builder.addStringField("hostname", true, true, false);
            builder.addTextField("description", true, false, "default", "position");
            builder.addIntegerField("id", true, true, true);

            try (Schema schema = builder.build()) {
                try (Index index = new Index(schema, indexPath, false)) {
                    try (IndexWriter writer = index.writer(Index.Memory.DEFAULT_HEAP_SIZE, 1)) {
                        try (Document doc = new Document()) {
                            doc.addInteger("id", 1);
                            doc.addString("hostname", "BLA33SL01");
                            doc.addText("description", "Production server in datacenter A");
                            writer.addDocument(doc);
                        }
                        try (Document doc = new Document()) {
                            doc.addInteger("id", 2);
                            doc.addString("hostname", "BLA33SL02");
                            doc.addText("description", "Backup server BLA33SL01 replica");
                            writer.addDocument(doc);
                        }
                        writer.commit();
                    }

                    // Convert to split
                    QuickwitSplit.SplitConfig config = new QuickwitSplit.SplitConfig(
                        "query-type-test", "test-source", "test-node");
                    QuickwitSplit.SplitMetadata splitMetadata =
                        QuickwitSplit.convertIndexFromPath(indexPath, splitPath, config);

                    SplitCacheManager.CacheConfig cacheConfig =
                        new SplitCacheManager.CacheConfig("query-type-cache")
                            .withMaxCacheSize(20_000_000);
                    SplitCacheManager cacheManager = SplitCacheManager.getInstance(cacheConfig);

                    try (SplitSearcher searcher = cacheManager.createSplitSearcher(
                            "file://" + splitPath, splitMetadata)) {

                        // Verify field type from split schema
                        Schema splitSchema = searcher.getSchema();
                        FieldInfo hostnameInfo = splitSchema.getFieldInfo("hostname");
                        System.out.println("Split schema hostname type: " + hostnameInfo.getType());
                        System.out.println("Split schema hostname tokenizer: " + hostnameInfo.getTokenizerName());

                        // The downstream code (IndexTables4Spark) does this check:
                        //   if (fieldType == FieldType.TEXT) → phrase query (WRONG for string fields)
                        //   else → SplitTermQuery (CORRECT for string fields)
                        //
                        // If the schema reports STRING correctly, the code takes the right path.
                        assertEquals(FieldType.STRING, hostnameInfo.getType(),
                            "Split schema should report STRING for raw-tokenizer field");

                        // Verify term query produces correct results
                        SplitTermQuery termQuery = new SplitTermQuery("hostname", "BLA33SL01");
                        SearchResult termResult = searcher.search(termQuery, 10);
                        System.out.println("SplitTermQuery for hostname=BLA33SL01: " +
                            termResult.getHits().size() + " results");
                        System.out.println("SplitTermQuery AST: " + termQuery.toQueryAstJson());

                        assertEquals(1, termResult.getHits().size(),
                            "SplitTermQuery should find exactly 1 match for hostname=BLA33SL01");

                        // Verify that the phrase query path (what the bug causes) behaves differently
                        // This is what happens when the field is misidentified as TEXT:
                        // parseQuery("hostname:\"BLA33SL01\"") produces a full_text phrase query
                        SplitQuery phraseQuery = searcher.parseQuery("hostname:\"BLA33SL01\"");
                        String phraseAst = phraseQuery.toQueryAstJson();
                        System.out.println("parseQuery phrase AST: " + phraseAst);

                        // The phrase query AST should NOT be used for string fields
                        // It uses {"type":"full_text","params":{"mode":{"type":"phrase"}}}
                        // which is wrong for raw-tokenized fields
                        assertTrue(phraseAst.contains("full_text") || phraseAst.contains("phrase"),
                            "parseQuery with quotes produces a full_text/phrase query (this is the wrong query type for string fields)");

                        // The term query AST is what should be used for string fields
                        String termAst = termQuery.toQueryAstJson();
                        System.out.println("SplitTermQuery AST: " + termAst);
                        assertTrue(termAst.contains("\"type\":\"term\""),
                            "SplitTermQuery should produce a term query AST");
                    }
                }
            }
        }
    }

    @Test
    @DisplayName("BUG: getFieldNamesByType(STRING) should return raw-tokenizer fields, not TEXT")
    public void testGetFieldNamesByType_StringVsText(@TempDir Path tempDir) {
        try (SchemaBuilder builder = new SchemaBuilder()) {
            // String fields (raw tokenizer)
            builder.addStringField("hostname", true, true, false);
            builder.addStringField("user_name", true, true, false);
            // Text fields (default tokenizer)
            builder.addTextField("description", true, false, "default", "position");
            builder.addTextField("content", true, false, "default", "position");
            // Non-string field
            builder.addIntegerField("id", true, true, true);

            try (Schema schema = builder.build()) {
                List<String> textFields = schema.getFieldNamesByType(FieldType.TEXT);
                List<String> stringFields = schema.getFieldNamesByType(FieldType.STRING);

                System.out.println("getFieldNamesByType(TEXT):   " + textFields);
                System.out.println("getFieldNamesByType(STRING): " + stringFields);

                // STRING fields should NOT appear in TEXT results
                assertFalse(textFields.contains("hostname"),
                    "hostname (raw tokenizer) should NOT be in getFieldNamesByType(TEXT)");
                assertFalse(textFields.contains("user_name"),
                    "user_name (raw tokenizer) should NOT be in getFieldNamesByType(TEXT)");

                // TEXT fields should still appear in TEXT results
                assertTrue(textFields.contains("description"),
                    "description (default tokenizer) should be in getFieldNamesByType(TEXT)");
                assertTrue(textFields.contains("content"),
                    "content (default tokenizer) should be in getFieldNamesByType(TEXT)");

                // STRING fields should appear in STRING results
                assertTrue(stringFields.contains("hostname"),
                    "hostname (raw tokenizer) should be in getFieldNamesByType(STRING)");
                assertTrue(stringFields.contains("user_name"),
                    "user_name (raw tokenizer) should be in getFieldNamesByType(STRING)");

                // TEXT fields should NOT appear in STRING results
                assertFalse(stringFields.contains("description"),
                    "description (default tokenizer) should NOT be in getFieldNamesByType(STRING)");
                assertFalse(stringFields.contains("content"),
                    "content (default tokenizer) should NOT be in getFieldNamesByType(STRING)");
            }
        }
    }

    @Test
    @DisplayName("BUG: Companion-style mixed schema should correctly distinguish field types")
    public void testCompanionStyleMixedSchema(@TempDir Path tempDir) {
        // Simulates a companion split with many default (string) fields and a few text fields
        String indexPath = tempDir.resolve("companion_mixed_index").toString();
        String splitPath = tempDir.resolve("companion_mixed.split").toString();

        try (SchemaBuilder builder = new SchemaBuilder()) {
            // Text fields (explicitly configured as text in companion INDEXING MODES)
            builder.addTextField("winlog_message", true, false, "default", "position");
            builder.addTextField("process_command_line", true, false, "default", "position");

            // String fields (default for all other columns in companion mode)
            builder.addStringField("host_hostname", true, true, false);
            builder.addStringField("source_ip", true, true, false);
            builder.addStringField("user_name", true, true, false);
            builder.addStringField("event_category", true, true, false);

            builder.addIntegerField("id", true, true, true);

            try (Schema schema = builder.build()) {
                // Verify ALL field types are correctly reported
                List<FieldInfo> allFields = schema.getAllFieldInfo();
                System.out.println("=== Companion-style schema field types ===");
                for (FieldInfo fi : allFields) {
                    System.out.printf("  %-25s type=%-8s tokenizer=%-10s%n",
                        fi.getName(), fi.getType(),
                        fi.getTokenizerName() != null ? fi.getTokenizerName() : "N/A");
                }

                // Text fields should be TEXT
                assertEquals(FieldType.TEXT, schema.getFieldInfo("winlog_message").getType(),
                    "winlog_message (text field) should be TEXT");
                assertEquals(FieldType.TEXT, schema.getFieldInfo("process_command_line").getType(),
                    "process_command_line (text field) should be TEXT");

                // String fields should be STRING (THIS IS THE BUG - they return TEXT)
                assertEquals(FieldType.STRING, schema.getFieldInfo("host_hostname").getType(),
                    "host_hostname (string/raw field) should be STRING, not TEXT");
                assertEquals(FieldType.STRING, schema.getFieldInfo("source_ip").getType(),
                    "source_ip (string/raw field) should be STRING, not TEXT");
                assertEquals(FieldType.STRING, schema.getFieldInfo("user_name").getType(),
                    "user_name (string/raw field) should be STRING, not TEXT");
                assertEquals(FieldType.STRING, schema.getFieldInfo("event_category").getType(),
                    "event_category (string/raw field) should be STRING, not TEXT");

                // Non-text fields should remain their correct type
                assertEquals(FieldType.INTEGER, schema.getFieldInfo("id").getType(),
                    "id should be INTEGER");
            }
        }
    }

    @Test
    @DisplayName("BUG: Doc mapping round-trip loses tokenizer for Str fields")
    public void testDocMappingRoundTrip_TokenizerPreserved() {
        // Simulate what extract_doc_mapping_from_index() currently produces:
        // It emits "type":"text" WITHOUT a "tokenizer" key for Str fields.
        // When create_schema_from_doc_mapping() consumes this, it defaults to "default".
        String docMappingWithoutTokenizer = "[" +
            "{\"name\":\"hostname\",\"type\":\"text\",\"stored\":true,\"indexed\":true,\"fast\":false}," +
            "{\"name\":\"description\",\"type\":\"text\",\"stored\":true,\"indexed\":true,\"fast\":false}," +
            "{\"name\":\"id\",\"type\":\"i64\",\"stored\":true,\"indexed\":true,\"fast\":true}" +
            "]";

        try (Schema schema = Schema.fromDocMappingJson(docMappingWithoutTokenizer)) {
            FieldInfo hostnameInfo = schema.getFieldInfo("hostname");
            FieldInfo descriptionInfo = schema.getFieldInfo("description");

            System.out.println("=== Doc mapping WITHOUT tokenizer (current bug) ===");
            System.out.println("hostname tokenizer: " + hostnameInfo.getTokenizerName() + " (should be raw, but defaults to default)");
            System.out.println("description tokenizer: " + descriptionInfo.getTokenizerName());

            // Both get "default" because the tokenizer was lost — this demonstrates the bug.
            // hostname should have been "raw" if extract_doc_mapping_from_index had included it.
            assertEquals("default", hostnameInfo.getTokenizerName(),
                "Without explicit tokenizer in doc_mapping, hostname defaults to 'default' (the bug)");
        }

        // Now simulate what extract_doc_mapping_from_index() SHOULD produce:
        // Include "tokenizer":"raw" for string fields.
        String docMappingWithTokenizer = "[" +
            "{\"name\":\"hostname\",\"type\":\"text\",\"stored\":true,\"indexed\":true,\"fast\":false,\"tokenizer\":\"raw\"}," +
            "{\"name\":\"description\",\"type\":\"text\",\"stored\":true,\"indexed\":true,\"fast\":false,\"tokenizer\":\"default\"}," +
            "{\"name\":\"id\",\"type\":\"i64\",\"stored\":true,\"indexed\":true,\"fast\":true}" +
            "]";

        try (Schema schema = Schema.fromDocMappingJson(docMappingWithTokenizer)) {
            FieldInfo hostnameInfo = schema.getFieldInfo("hostname");
            FieldInfo descriptionInfo = schema.getFieldInfo("description");

            System.out.println("=== Doc mapping WITH tokenizer (after fix) ===");
            System.out.println("hostname tokenizer: " + hostnameInfo.getTokenizerName());
            System.out.println("description tokenizer: " + descriptionInfo.getTokenizerName());

            assertEquals("raw", hostnameInfo.getTokenizerName(),
                "With explicit tokenizer in doc_mapping, hostname should be 'raw'");
            assertEquals("default", descriptionInfo.getTokenizerName(),
                "description should remain 'default'");
        }
    }

    @Test
    @DisplayName("BUG: Split doc_mapping should preserve tokenizer for Str fields")
    public void testSplitDocMapping_TokenizerPreserved(@TempDir Path tempDir) {
        // End-to-end: index → split → read doc_mapping from metadata → reconstruct schema
        // The split's doc_mapping is generated by extract_doc_mapping_from_index().
        // SplitSearcher.getSchema() bypasses doc_mapping (uses cached_index.schema() directly),
        // so it doesn't expose this bug. But any code that reconstructs a schema from the
        // doc_mapping (e.g. Schema.fromDocMappingJson with the split's stored doc_mapping)
        // will lose the tokenizer.
        String indexPath = tempDir.resolve("docmap_rt_index").toString();
        String splitPath = tempDir.resolve("docmap_rt.split").toString();

        try (SchemaBuilder builder = new SchemaBuilder()) {
            builder.addStringField("hostname", true, true, false);  // raw tokenizer
            builder.addTextField("description", true, false, "default", "position");  // default tokenizer
            builder.addIntegerField("id", true, true, true);

            try (Schema schema = builder.build()) {
                try (Index index = new Index(schema, indexPath, false)) {
                    try (IndexWriter writer = index.writer(Index.Memory.DEFAULT_HEAP_SIZE, 1)) {
                        try (Document doc = new Document()) {
                            doc.addInteger("id", 1);
                            doc.addString("hostname", "SRV001");
                            doc.addText("description", "A test server");
                            writer.addDocument(doc);
                        }
                        writer.commit();
                    }

                    // Convert to split (this calls extract_doc_mapping_from_index internally)
                    QuickwitSplit.SplitConfig config = new QuickwitSplit.SplitConfig(
                        "docmap-rt-test", "test-source", "test-node");
                    QuickwitSplit.SplitMetadata splitMetadata =
                        QuickwitSplit.convertIndexFromPath(indexPath, splitPath, config);

                    // The split metadata contains the doc_mapping JSON produced by
                    // extract_doc_mapping_from_index(). Get it and reconstruct a schema.
                    String docMappingJson = splitMetadata.getDocMappingJson();
                    System.out.println("=== Doc mapping from split metadata ===");
                    System.out.println("doc_mapping JSON: " + docMappingJson);

                    assertNotNull(docMappingJson, "Split metadata should contain doc_mapping JSON");

                    // BUG: The doc_mapping JSON should contain "tokenizer":"raw" for hostname,
                    // but extract_doc_mapping_from_index() omits it.
                    assertTrue(docMappingJson.contains("\"tokenizer\":\"raw\"")
                            || docMappingJson.contains("\"tokenizer\": \"raw\""),
                        "doc_mapping should include tokenizer:raw for string fields. " +
                        "extract_doc_mapping_from_index() must emit the tokenizer for Str fields. " +
                        "Actual doc_mapping: " + docMappingJson);

                    // Reconstruct schema from doc_mapping and verify tokenizer
                    try (Schema reconstructed = Schema.fromDocMappingJson(docMappingJson)) {
                        FieldInfo hostnameInfo = reconstructed.getFieldInfo("hostname");
                        System.out.println("Reconstructed hostname tokenizer: " + hostnameInfo.getTokenizerName());

                        assertEquals("raw", hostnameInfo.getTokenizerName(),
                            "Schema reconstructed from doc_mapping should have 'raw' tokenizer for hostname");
                    }
                }
            }
        }
    }
}

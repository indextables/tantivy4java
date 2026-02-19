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
 * Tests for tokenizer preservation in schema introspection and doc_mapping round-trips.
 *
 * The tokenizer ("raw" vs "default") is the key distinction between string fields
 * (exact matching) and text fields (tokenized matching). These tests verify that
 * the tokenizer information is correctly preserved through:
 * - Schema.getFieldInfo() / getAllFieldInfo()
 * - extract_doc_mapping_from_index() in split creation
 * - Schema.fromDocMappingJson() reconstruction
 */
public class StringFieldTypeResolutionTest {

    @Test
    @DisplayName("getFieldInfo reports correct tokenizer for string vs text fields")
    public void testTokenizerReportedCorrectly() {
        try (SchemaBuilder builder = new SchemaBuilder()) {
            builder.addStringField("hostname", true, true, false);
            builder.addTextField("description", true, false, "default", "position");
            builder.addIntegerField("id", true, true, true);

            try (Schema schema = builder.build()) {
                FieldInfo hostnameInfo = schema.getFieldInfo("hostname");
                FieldInfo descriptionInfo = schema.getFieldInfo("description");

                assertNotNull(hostnameInfo);
                assertNotNull(descriptionInfo);

                // Both report as TEXT (Str fields always return TEXT for backward compatibility)
                assertEquals(FieldType.TEXT, hostnameInfo.getType());
                assertEquals(FieldType.TEXT, descriptionInfo.getType());

                // The tokenizer distinguishes string from text
                assertEquals("raw", hostnameInfo.getTokenizerName(),
                    "String field should report 'raw' tokenizer");
                assertEquals("default", descriptionInfo.getTokenizerName(),
                    "Text field should report 'default' tokenizer");
            }
        }
    }

    @Test
    @DisplayName("getAllFieldInfo reports correct tokenizers for companion-style mixed schema")
    public void testCompanionStyleMixedSchema() {
        try (SchemaBuilder builder = new SchemaBuilder()) {
            builder.addTextField("winlog_message", true, false, "default", "position");
            builder.addTextField("process_command_line", true, false, "default", "position");
            builder.addStringField("host_hostname", true, true, false);
            builder.addStringField("source_ip", true, true, false);
            builder.addStringField("user_name", true, true, false);
            builder.addStringField("event_category", true, true, false);
            builder.addIntegerField("id", true, true, true);

            try (Schema schema = builder.build()) {
                List<FieldInfo> allFields = schema.getAllFieldInfo();

                for (FieldInfo fi : allFields) {
                    if (fi.getTokenizerName() != null) {
                        System.out.printf("  %-25s tokenizer=%-10s%n", fi.getName(), fi.getTokenizerName());
                    }
                }

                // Text fields should have "default" tokenizer
                assertEquals("default", schema.getFieldInfo("winlog_message").getTokenizerName());
                assertEquals("default", schema.getFieldInfo("process_command_line").getTokenizerName());

                // String fields should have "raw" tokenizer
                assertEquals("raw", schema.getFieldInfo("host_hostname").getTokenizerName());
                assertEquals("raw", schema.getFieldInfo("source_ip").getTokenizerName());
                assertEquals("raw", schema.getFieldInfo("user_name").getTokenizerName());
                assertEquals("raw", schema.getFieldInfo("event_category").getTokenizerName());
            }
        }
    }

    @Test
    @DisplayName("Doc mapping round-trip preserves tokenizer when explicitly included")
    public void testDocMappingRoundTrip_TokenizerPreserved() {
        // Without tokenizer in doc_mapping: defaults to "default"
        String docMappingWithoutTokenizer = "[" +
            "{\"name\":\"hostname\",\"type\":\"text\",\"stored\":true,\"indexed\":true,\"fast\":false}," +
            "{\"name\":\"description\",\"type\":\"text\",\"stored\":true,\"indexed\":true,\"fast\":false}," +
            "{\"name\":\"id\",\"type\":\"i64\",\"stored\":true,\"indexed\":true,\"fast\":true}" +
            "]";

        try (Schema schema = Schema.fromDocMappingJson(docMappingWithoutTokenizer)) {
            assertEquals("default", schema.getFieldInfo("hostname").getTokenizerName(),
                "Without explicit tokenizer, hostname defaults to 'default'");
        }

        // With tokenizer in doc_mapping: correctly preserved
        String docMappingWithTokenizer = "[" +
            "{\"name\":\"hostname\",\"type\":\"text\",\"stored\":true,\"indexed\":true,\"fast\":false,\"tokenizer\":\"raw\"}," +
            "{\"name\":\"description\",\"type\":\"text\",\"stored\":true,\"indexed\":true,\"fast\":false,\"tokenizer\":\"default\"}," +
            "{\"name\":\"id\",\"type\":\"i64\",\"stored\":true,\"indexed\":true,\"fast\":true}" +
            "]";

        try (Schema schema = Schema.fromDocMappingJson(docMappingWithTokenizer)) {
            assertEquals("raw", schema.getFieldInfo("hostname").getTokenizerName(),
                "With explicit tokenizer, hostname should be 'raw'");
            assertEquals("default", schema.getFieldInfo("description").getTokenizerName(),
                "description should remain 'default'");
        }
    }

    @Test
    @DisplayName("Split doc_mapping includes tokenizer for Str fields")
    public void testSplitDocMapping_TokenizerPreserved(@TempDir Path tempDir) {
        String indexPath = tempDir.resolve("docmap_rt_index").toString();
        String splitPath = tempDir.resolve("docmap_rt.split").toString();

        try (SchemaBuilder builder = new SchemaBuilder()) {
            builder.addStringField("hostname", true, true, false);
            builder.addTextField("description", true, false, "default", "position");
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

                    QuickwitSplit.SplitConfig config = new QuickwitSplit.SplitConfig(
                        "docmap-rt-test", "test-source", "test-node");
                    QuickwitSplit.SplitMetadata splitMetadata =
                        QuickwitSplit.convertIndexFromPath(indexPath, splitPath, config);

                    String docMappingJson = splitMetadata.getDocMappingJson();
                    System.out.println("doc_mapping JSON: " + docMappingJson);

                    assertNotNull(docMappingJson, "Split metadata should contain doc_mapping JSON");

                    // doc_mapping must include tokenizer for Str fields
                    assertTrue(docMappingJson.contains("\"tokenizer\":\"raw\"")
                            || docMappingJson.contains("\"tokenizer\": \"raw\""),
                        "doc_mapping should include tokenizer:raw for string fields. " +
                        "Actual: " + docMappingJson);

                    // Reconstruct schema from doc_mapping and verify tokenizer survives
                    try (Schema reconstructed = Schema.fromDocMappingJson(docMappingJson)) {
                        assertEquals("raw", reconstructed.getFieldInfo("hostname").getTokenizerName(),
                            "Reconstructed schema should preserve 'raw' tokenizer");
                        assertEquals("default", reconstructed.getFieldInfo("description").getTokenizerName(),
                            "Reconstructed schema should preserve 'default' tokenizer");
                    }
                }
            }
        }
    }
}

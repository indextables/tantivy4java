package io.indextables.tantivy4java;

import io.indextables.tantivy4java.core.*;
import io.indextables.tantivy4java.split.merge.*;


import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;

import java.nio.file.Path;
import java.time.LocalDateTime;

/**
 * Test to verify that date fields appear properly in the docMapping structure.
 */
public class DocMappingDateFieldTest {

    @Test
    public void testDateFieldInDocMapping(@TempDir Path tempDir) throws Exception {
        String uniqueId = "doc_mapping_test_" + System.nanoTime();
        String indexPath = tempDir.resolve(uniqueId + "_index").toString();
        String splitPath = tempDir.resolve(uniqueId + ".split").toString();

            try (SchemaBuilder builder = new SchemaBuilder()) {
                // Build schema with date field (stored, indexed, fast)
                builder.addTextField("event_name", true, false, "default", "position")
                       .addDateField("event_date", true, true, true)  // stored, indexed, fast
                       .addTextField("category", true, true, "raw", "position")
                       .addIntegerField("priority", true, true, true);

                try (Schema schema = builder.build()) {
                    try (Index index = new Index(schema, indexPath, false)) {
                        try (IndexWriter writer = index.writer(Index.Memory.DEFAULT_HEAP_SIZE, 1)) {

                            // Add a test document with date field
                            try (Document doc = new Document()) {
                                doc.addText("event_name", "Test Event");
                                doc.addDate("event_date", LocalDateTime.of(2023, 6, 15, 14, 30, 0));
                                doc.addText("category", "Testing");
                                doc.addInteger("priority", 1);
                                writer.addDocument(doc);
                            }

                            writer.commit();
                        }

                        QuickwitSplit.SplitConfig config = new QuickwitSplit.SplitConfig(
                            uniqueId, "test-source", "test-node");

                        // Convert index to split and get metadata with docMapping
                        QuickwitSplit.SplitMetadata metadata = QuickwitSplit.convertIndexFromPath(
                            indexPath, splitPath, config);

                        // Examine the docMapping
                        String docMapping = metadata.getDocMappingJson();
                        assertNotNull(docMapping, "DocMapping should not be null");

                        System.out.println("🔍 DocMapping structure:");
                        System.out.println(docMapping);

                        // Verify date field is present and properly configured
                        assertTrue(docMapping.contains("event_date"),
                            "DocMapping should contain event_date field");

                        // Check for date field type (Quickwit uses "datetime" for date fields)
                        assertTrue(docMapping.contains("\"type\":\"datetime\"") ||
                                  docMapping.contains("\"type\": \"datetime\""),
                            "DocMapping should specify datetime type for event_date field");

                        // Check for fast field configuration
                        System.out.println("\n📅 Date field analysis:");
                        if (docMapping.contains("event_date")) {
                            System.out.println("✅ event_date field found in docMapping");

                            if (docMapping.contains("\"type\":\"datetime\"") || docMapping.contains("\"type\": \"datetime\"")) {
                                System.out.println("✅ Date field has correct type: datetime");
                            } else {
                                System.out.println("❌ Date field type not found or incorrect");
                            }

                            if (docMapping.contains("\"fast\":true") || docMapping.contains("\"fast\": true")) {
                                System.out.println("✅ Date field is configured as fast field");
                            } else if (docMapping.contains("\"fast\":false") || docMapping.contains("\"fast\": false")) {
                                System.out.println("❌ Date field is NOT configured as fast field");
                            } else {
                                System.out.println("⚠️  Date field fast configuration unclear");
                            }

                            if (docMapping.contains("\"indexed\":true") || docMapping.contains("\"indexed\": true")) {
                                System.out.println("✅ Date field is indexed");
                            } else {
                                System.out.println("⚠️  Date field indexing configuration unclear");
                            }

                            if (docMapping.contains("\"stored\":true") || docMapping.contains("\"stored\": true")) {
                                System.out.println("✅ Date field is stored");
                            } else {
                                System.out.println("⚠️  Date field storage configuration unclear");
                            }
                        } else {
                            fail("event_date field not found in docMapping!");
                        }

                        System.out.println("\n✅ DocMapping date field verification completed");
                    }
                }
            }
    }
}

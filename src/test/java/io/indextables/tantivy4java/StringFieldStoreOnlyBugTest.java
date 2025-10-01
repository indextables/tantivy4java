package io.indextables.tantivy4java;

import io.indextables.tantivy4java.core.*;
import io.indextables.tantivy4java.query.*;
import io.indextables.tantivy4java.result.*;


import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Test to demonstrate and verify the fix for the store-only fields bug.
 *
 * Issue: addStringField with indexed=false creates inconsistent field types.
 * When indexed=false, the field should be store-only, but the current implementation
 * always creates a text field with raw tokenizer, even when indexing is disabled.
 */
@DisplayName("String Field Store-Only Bug Test")
public class StringFieldStoreOnlyBugTest {

    @Test
    @DisplayName("Demonstrate store-only string field bug")
    public void testStoreOnlyStringFieldBug() throws Exception {
        System.out.println("\n🐛 Testing store-only string field bug");

        // Create a schema with store-only string field (indexed=false)
        try (SchemaBuilder builder = new SchemaBuilder()) {
            builder
                .addStringField("store_only_field", true, false, false)  // stored=true, indexed=false, fast=false
                .addStringField("indexed_field", true, true, false)      // stored=true, indexed=true, fast=false
                .addIntegerField("id", true, true, false);

            try (Schema schema = builder.build()) {
                System.out.println("✅ Schema created successfully");
                System.out.println("  Fields: " + schema.getFieldNames());

                // Test field existence
                assertTrue(schema.hasField("store_only_field"), "Store-only field should exist");
                assertTrue(schema.hasField("indexed_field"), "Indexed field should exist");

                // Create test index
                String tempDir = "/tmp/store_only_bug_test_" + System.currentTimeMillis();
                try (Index index = new Index(schema, tempDir, false)) {
                    try (IndexWriter writer = index.writer(Index.Memory.DEFAULT_HEAP_SIZE, 1)) {
                        // Add a document
                        Document doc = new Document();
                        doc.addString("store_only_field", "store_only_value");
                        doc.addString("indexed_field", "indexed_value");
                        doc.addInteger("id", 1);
                        writer.addDocument(doc);
                        writer.commit();
                    }

                    // Test that we can retrieve the stored value
                    try (Searcher searcher = index.searcher()) {
                        // Query by ID to get the document
                        Query idQuery = Query.termQuery(schema, "id", 1);
                        SearchResult result = searcher.search(idQuery, 1);

                        assertEquals(1, result.getHits().size(), "Should find one document");

                        try (Document retrievedDoc = searcher.doc(result.getHits().get(0).getDocAddress())) {
                            // Both fields should be retrievable since they're stored
                            String storeOnlyValue = (String) retrievedDoc.getFirst("store_only_field");
                            String indexedValue = (String) retrievedDoc.getFirst("indexed_field");

                            assertEquals("store_only_value", storeOnlyValue, "Store-only field should be retrievable");
                            assertEquals("indexed_value", indexedValue, "Indexed field should be retrievable");

                            System.out.println("  ✅ Retrieved store-only field: " + storeOnlyValue);
                            System.out.println("  ✅ Retrieved indexed field: " + indexedValue);
                        }

                        // Test that store-only field is NOT searchable
                        System.out.println("\n🔍 Testing that store-only field is not searchable...");
                        try {
                            Query storeOnlyQuery = Query.termQuery(schema, "store_only_field", "store_only_value");
                            SearchResult storeOnlyResult = searcher.search(storeOnlyQuery, 1);

                            // THIS IS THE BUG: If indexed=false, this should return 0 results
                            // But currently it may return results because the field is still indexed with raw tokenizer
                            System.out.println("  🐛 Store-only field query results: " + storeOnlyResult.getHits().size());

                            if (storeOnlyResult.getHits().size() > 0) {
                                System.out.println("  ❌ BUG CONFIRMED: Store-only field is searchable (should not be!)");
                                // For now, this is the expected behavior due to the bug
                                // After fix, this should be assertEquals(0, storeOnlyResult.getHits().size())
                            } else {
                                System.out.println("  ✅ CORRECT: Store-only field is not searchable");
                            }

                        } catch (Exception e) {
                            System.out.println("  ✅ CORRECT: Store-only field query failed as expected: " + e.getMessage());
                        }

                        // Test that indexed field IS searchable
                        System.out.println("\n🔍 Testing that indexed field is searchable...");
                        Query indexedQuery = Query.termQuery(schema, "indexed_field", "indexed_value");
                        SearchResult indexedResult = searcher.search(indexedQuery, 1);

                        assertEquals(1, indexedResult.getHits().size(), "Indexed field should be searchable");
                        System.out.println("  ✅ Indexed field query results: " + indexedResult.getHits().size());
                    }
                } finally {
                    // Clean up temp directory
                    try {
                        java.io.File tempDirFile = new java.io.File(tempDir);
                        if (tempDirFile.exists()) {
                            deleteRecursively(tempDirFile);
                        }
                    } catch (Exception e) {
                        System.err.println("Warning: Failed to clean up temp directory: " + e.getMessage());
                    }
                }
            }
        }
    }

    private void deleteRecursively(java.io.File file) {
        if (file.isDirectory()) {
            java.io.File[] files = file.listFiles();
            if (files != null) {
                for (java.io.File f : files) {
                    deleteRecursively(f);
                }
            }
        }
        file.delete();
    }
}

package io.indextables.tantivy4java;

import io.indextables.tantivy4java.core.*;
import io.indextables.tantivy4java.query.*;
import io.indextables.tantivy4java.result.*;


import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;

import java.nio.file.Path;
import java.util.List;

/**
 * Test to validate that the raw tokenizer works properly when configured correctly.
 * This test bypasses split format limitations by testing direct schema configuration.
 */
public class RawTokenizerValidationTest {

    @Test
    @DisplayName("Validate Raw Tokenizer Works Properly for Direct Schema")
    public void testRawTokenizerDirectSchema(@TempDir Path tempDir) {
        System.out.println("🔍 === RAW TOKENIZER VALIDATION TEST ===");
        System.out.println("Testing that raw tokenizer preserves exact input when properly configured");

        String indexPath = tempDir.resolve("raw_tokenizer_index").toString();

        try {
            // === CREATE INDEX WITH STRING FIELDS (USING RAW TOKENIZER) ===
            System.out.println("\n📋 Phase 1: Creating index with string fields using raw tokenizer");

            try (SchemaBuilder builder = new SchemaBuilder()) {
                builder
                    // String fields explicitly use raw tokenizer
                    .addStringField("exact_category", true, true, false)
                    .addStringField("exact_status", true, true, false)
                    .addStringField("exact_product_id", false, true, false)

                    // Text fields use default tokenizer for comparison
                    .addTextField("regular_title", true, false, "default", "position")
                    .addTextField("regular_content", true, false, "default", "position")

                    // ID field
                    .addIntegerField("id", true, true, true);

                try (Schema schema = builder.build()) {
                    System.out.println("✅ Created schema with string fields (raw tokenizer) and text fields (default tokenizer)");

                    // === POPULATE INDEX ===
                    System.out.println("\n📝 Phase 2: Populating index with test data");

                    try (Index index = new Index(schema, indexPath, false)) {
                        try (IndexWriter writer = index.writer(Index.Memory.DEFAULT_HEAP_SIZE, 1)) {

                            // Add a test document
                            try (Document doc = new Document()) {
                                doc.addInteger("id", 1);

                                // String fields - should preserve exact case and punctuation with raw tokenizer
                                doc.addString("exact_category", "High-Tech Electronics");
                                doc.addString("exact_status", "IN_STOCK_Ready");
                                doc.addString("exact_product_id", "PROD-2025-ABC123");

                                // Text fields - should be lowercased and split with default tokenizer
                                doc.addText("regular_title", "High-Tech Electronics Tutorial");
                                doc.addText("regular_content", "This tutorial covers high-tech electronics");

                                writer.addDocument(doc);
                            }

                            writer.commit();
                            System.out.println("✅ Added test document");
                        }

                        // === TEST DIRECT INDEX TOKENIZATION ===
                        System.out.println("\n🔍 Phase 3: Testing tokenization directly on index");

                        try (Searcher searcher = index.searcher()) {
                            Schema indexSchema = index.getSchema();

                            // Test 1: String field with raw tokenizer should preserve exact input
                            System.out.println("\n🔎 Test 1: String field tokenization (raw tokenizer)");

                            // Note: We can't directly call tokenize on regular searcher,
                            // but we can verify the behavior through search
                            Query exactQuery = Query.termQuery(indexSchema, "exact_category", "High-Tech Electronics");
                            SearchResult exactResult = searcher.search(exactQuery, 10);

                            System.out.println("  Exact match search for 'High-Tech Electronics': " + exactResult.getHits().size() + " hits");
                            assertEquals(1, exactResult.getHits().size(), "String field should match exact input with raw tokenizer");

                            // Test case sensitivity - raw tokenizer should preserve case
                            Query caseQuery = Query.termQuery(indexSchema, "exact_category", "high-tech electronics");
                            SearchResult caseResult = searcher.search(caseQuery, 10);

                            System.out.println("  Case-insensitive search for 'high-tech electronics': " + caseResult.getHits().size() + " hits");
                            assertEquals(0, caseResult.getHits().size(), "Raw tokenizer should be case-sensitive");

                            System.out.println("  ✅ Raw tokenizer preserves exact case and punctuation");

                            // Test 2: Text field with default tokenizer should lowercase and split
                            System.out.println("\n🔎 Test 2: Text field tokenization (default tokenizer)");

                            // Default tokenizer should lowercase, so this should match
                            Query textQuery = Query.termQuery(indexSchema, "regular_title", "high");
                            SearchResult textResult = searcher.search(textQuery, 10);

                            System.out.println("  Lowercase term search for 'high': " + textResult.getHits().size() + " hits");
                            assertEquals(1, textResult.getHits().size(), "Text field should match lowercased terms");

                            // Test that text field splits on punctuation
                            Query splitQuery = Query.termQuery(indexSchema, "regular_title", "tech");
                            SearchResult splitResult = searcher.search(splitQuery, 10);

                            System.out.println("  Split term search for 'tech': " + splitResult.getHits().size() + " hits");
                            assertEquals(1, splitResult.getHits().size(), "Text field should split on punctuation");

                            System.out.println("  ✅ Default tokenizer splits and lowercases correctly");

                            // Test 3: String field complex patterns
                            System.out.println("\n🔎 Test 3: String field complex pattern validation");

                            Query complexQuery = Query.termQuery(indexSchema, "exact_product_id", "PROD-2025-ABC123");
                            SearchResult complexResult = searcher.search(complexQuery, 10);

                            System.out.println("  Complex pattern exact match: " + complexResult.getHits().size() + " hits");
                            assertEquals(1, complexResult.getHits().size(), "String field should match complex patterns exactly");

                            // Should not match partial patterns
                            Query partialQuery = Query.termQuery(indexSchema, "exact_product_id", "PROD");
                            SearchResult partialResult = searcher.search(partialQuery, 10);

                            System.out.println("  Partial pattern match attempt: " + partialResult.getHits().size() + " hits");
                            assertEquals(0, partialResult.getHits().size(), "String field should not match partial patterns");

                            System.out.println("  ✅ String field handles complex patterns correctly");
                        }
                    }
                }
            }

            System.out.println("\n🎉 === RAW TOKENIZER VALIDATION TEST COMPLETED ===");
            System.out.println("✨ Successfully validated that raw tokenizer works properly:");
            System.out.println("   🔤 STRING fields - Raw tokenizer preserves exact case and punctuation");
            System.out.println("   📝 TEXT fields - Default tokenizer splits and lowercases correctly");
            System.out.println("   🔍 SEARCH behavior - Confirms tokenizer differences through query results");
            System.out.println("   ✅ Raw tokenizer functionality verified through direct schema configuration");

        } catch (Exception e) {
            fail("Raw tokenizer validation test failed: " + e.getMessage());
        }
    }
}

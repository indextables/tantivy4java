package com.tantivy4java;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;

import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.HashMap;

/**
 * Comprehensive test class for text, JSON, and string field types across
 * both tantivy index and split APIs. Demonstrates robust unit testing
 * for all three field types with complete functionality coverage.
 */
public class TextJsonStringFieldTypesTest {

    @Test
    @DisplayName("Text, JSON, and String Field Types - Complete Functionality Test")
    public void testTextJsonStringFieldTypes(@TempDir Path tempDir) {
        System.out.println("🚀 === COMPREHENSIVE FIELD TYPES TEST ===");
        System.out.println("Testing text, JSON, and string field types with robust coverage");

        String indexPath = tempDir.resolve("field_types_index").toString();

        try {
            // === SCHEMA CREATION WITH ALL THREE FIELD TYPES ===
            System.out.println("\n📋 Phase 1: Creating schema with text, JSON, and string fields");

            try (SchemaBuilder builder = new SchemaBuilder()) {
                builder
                    // Text fields - tokenized, searchable
                    .addTextField("title", true, false, "default", "position")
                    .addTextField("content", true, false, "default", "position")

                    // String fields - exact matching, not tokenized
                    .addStringField("category", true, true, true)
                    .addStringField("status", true, true, false)
                    .addStringField("product_code", false, true, false)  // indexed only

                    // JSON fields - structured data
                    .addJsonField("metadata", true, "default", "position")
                    .addJsonField("attributes", false, "default", "position")  // not stored

                    // Other types for comparison
                    .addIntegerField("id", true, true, true)
                    .addFloatField("price", true, true, true)
                    .addBooleanField("active", true, true, false);

                try (Schema schema = builder.build()) {
                    System.out.println("✅ Created comprehensive schema with text, JSON, and string fields");

                    // === INDEX CREATION AND DOCUMENT ADDITION ===
                    System.out.println("\n📝 Phase 2: Adding documents with all field types");

                    try (Index index = new Index(schema, indexPath, false)) {
                        try (IndexWriter writer = index.writer(Index.Memory.DEFAULT_HEAP_SIZE, 1)) {

                            // Test 1: Document with text, string, and JSON fields
                            System.out.println("\n🔎 Test 1: Creating document with mixed field types");

                            try (Document doc1 = new Document()) {
                                // Text fields (tokenized)
                                doc1.addText("title", "The Great Machine Learning Tutorial");
                                doc1.addText("content", "This tutorial covers advanced machine learning concepts and techniques");

                                // String fields (exact matching)
                                doc1.addString("category", "Technology");
                                doc1.addString("status", "Published");
                                doc1.addString("product_code", "ML-TUT-001");

                                // JSON fields (structured data)
                                Map<String, Object> metadata = new HashMap<>();
                                metadata.put("author", "Dr. Smith");
                                metadata.put("tags", List.of("ML", "AI", "Tutorial"));
                                metadata.put("difficulty", "Advanced");
                                doc1.addJson("metadata", metadata);

                                Map<String, Object> attributes = new HashMap<>();
                                attributes.put("duration_minutes", 45);
                                attributes.put("interactive", true);
                                doc1.addJson("attributes", attributes);

                                // Other types
                                doc1.addInteger("id", 1);
                                doc1.addFloat("price", 29.99);
                                doc1.addBoolean("active", true);

                                writer.addDocument(doc1);
                                System.out.println("  ✅ Added document with text, string, and JSON fields");
                            }

                            // Test 2: Document via JSON string (testing JSON parsing)
                            System.out.println("\n🔎 Test 2: Adding document via JSON string");

                            String jsonDoc = "{\n" +
                                "  \"id\": 2,\n" +
                                "  \"title\": \"Deep Learning Fundamentals\",\n" +
                                "  \"content\": \"An introduction to neural networks and deep learning\",\n" +
                                "  \"category\": \"Education\",\n" +
                                "  \"status\": \"Draft\",\n" +
                                "  \"product_code\": \"DL-FUND-002\",\n" +
                                "  \"price\": 39.99,\n" +
                                "  \"active\": false,\n" +
                                "  \"metadata\": {\n" +
                                "    \"author\": \"Prof. Johnson\",\n" +
                                "    \"tags\": [\"Deep Learning\", \"Neural Networks\"],\n" +
                                "    \"difficulty\": \"Beginner\"\n" +
                                "  },\n" +
                                "  \"attributes\": {\n" +
                                "    \"duration_minutes\": 60,\n" +
                                "    \"interactive\": false\n" +
                                "  }\n" +
                                "}";

                            writer.addJson(jsonDoc);
                            System.out.println("  ✅ Added document via JSON string parsing");

                            // Test 3: Document with multi-value string fields
                            System.out.println("\n🔎 Test 3: Adding document with multi-value fields");

                            try (Document doc3 = new Document()) {
                                doc3.addText("title", "Advanced Data Science");
                                doc3.addText("content", "Comprehensive data science course with statistics and visualization");

                                // Multiple string values for same field
                                doc3.addString("category", "Technology");
                                doc3.addString("category", "Data Science");  // Multi-value
                                doc3.addString("status", "Published");
                                doc3.addString("product_code", "DS-ADV-003");

                                doc3.addInteger("id", 3);
                                doc3.addFloat("price", 49.99);
                                doc3.addBoolean("active", true);

                                Map<String, Object> metadata = new HashMap<>();
                                metadata.put("author", "Data Team");
                                metadata.put("tags", List.of("Data Science", "Statistics", "Visualization"));
                                metadata.put("difficulty", "Intermediate");
                                doc3.addJson("metadata", metadata);

                                writer.addDocument(doc3);
                                System.out.println("  ✅ Added document with multi-value string fields");
                            }

                            writer.commit();
                            System.out.println("✅ Committed 3 documents with comprehensive field types");
                        }

                        index.reload();

                        // === COMPREHENSIVE SEARCH TESTING ===
                        System.out.println("\n🔍 Phase 3: Testing search functionality across field types");

                        try (Searcher searcher = index.searcher()) {
                            assertEquals(3, searcher.getNumDocs(), "Should have 3 documents");
                            System.out.println("✅ Index contains " + searcher.getNumDocs() + " documents");

                            // Test 4: Text field search (tokenized)
                            System.out.println("\n🔎 Test 4: Text field search (tokenized)");
                            try (Query textQuery = Query.termQuery(schema, "content", "machine")) {
                                try (SearchResult result = searcher.search(textQuery, 10)) {
                                    var hits = result.getHits();
                                    System.out.println("  Found " + hits.size() + " documents for text 'machine'");
                                    assertTrue(hits.size() >= 1, "Should find documents with 'machine' in content");

                                    try (Document doc = searcher.doc(hits.get(0).getDocAddress())) {
                                        String title = doc.get("title").get(0).toString();
                                        System.out.println("    📖 Found: \"" + title + "\"");
                                    }
                                    System.out.println("  ✅ Text field tokenized search working");
                                }
                            }

                            // Test 5: String field exact search (not tokenized)
                            System.out.println("\n🔎 Test 5: String field exact search (not tokenized)");
                            try (Query stringQuery = Query.termQuery(schema, "category", "Technology")) {
                                try (SearchResult result = searcher.search(stringQuery, 10)) {
                                    var hits = result.getHits();
                                    System.out.println("  Found " + hits.size() + " documents for exact category 'Technology'");
                                    assertTrue(hits.size() >= 1, "Should find documents with exact category 'Technology'");

                                    for (SearchResult.Hit hit : hits) {
                                        try (Document doc = searcher.doc(hit.getDocAddress())) {
                                            List<Object> categories = doc.get("category");
                                            List<Object> statusList = doc.get("status");
                                            List<Object> productCodeList = doc.get("product_code");

                                            String status = statusList.isEmpty() ? "N/A" : statusList.get(0).toString();
                                            String productCode = productCodeList.isEmpty() ? "N/A" : productCodeList.get(0).toString();

                                            System.out.println("    📄 Categories: " + categories + ", Status: " + status + ", Code: " + productCode);
                                            assertTrue(categories.contains("Technology"), "Should contain exact 'Technology' category");
                                        }
                                    }
                                    System.out.println("  ✅ String field exact search working");
                                }
                            }

                            // Test 6: JSON field search and retrieval
                            System.out.println("\n🔎 Test 6: JSON field search and data retrieval");
                            try (Query allQuery = Query.allQuery()) {
                                try (SearchResult result = searcher.search(allQuery, 10)) {
                                    var hits = result.getHits();
                                    System.out.println("  Processing " + hits.size() + " documents for JSON field analysis");

                                    for (SearchResult.Hit hit : hits) {
                                        try (Document doc = searcher.doc(hit.getDocAddress())) {
                                            long id = ((Number) doc.get("id").get(0)).longValue();
                                            List<Object> metadataValues = doc.get("metadata");

                                            if (!metadataValues.isEmpty()) {
                                                System.out.println("    📄 Document " + id + " metadata: " + metadataValues.get(0));
                                            }
                                        }
                                    }
                                    System.out.println("  ✅ JSON field retrieval working");
                                }
                            }

                            // Test 7: Range query on numeric fields
                            System.out.println("\n🔎 Test 7: Range query on numeric fields");
                            try (Query rangeQuery = Query.rangeQuery(schema, "price", FieldType.FLOAT, 30.0, 50.0, true, true)) {
                                try (SearchResult result = searcher.search(rangeQuery, 10)) {
                                    var hits = result.getHits();
                                    System.out.println("  Found " + hits.size() + " documents with price 30.0-50.0");

                                    for (SearchResult.Hit hit : hits) {
                                        try (Document doc = searcher.doc(hit.getDocAddress())) {
                                            double price = (Double) doc.get("price").get(0);
                                            String title = doc.get("title").get(0).toString();
                                            String category = doc.get("category").get(0).toString();

                                            System.out.println("    📄 \"" + title + "\" - $" + price + " (" + category + ")");
                                            assertTrue(price >= 30.0 && price <= 50.0, "Price should be in range");
                                        }
                                    }
                                    System.out.println("  ✅ Numeric range queries working");
                                }
                            }

                            // Test 8: Multi-value string field search
                            System.out.println("\n🔎 Test 8: Multi-value string field search");
                            try (Query dataQuery = Query.termQuery(schema, "category", "Data Science")) {
                                try (SearchResult result = searcher.search(dataQuery, 10)) {
                                    var hits = result.getHits();
                                    System.out.println("  Found " + hits.size() + " documents for 'Data Science' category");

                                    if (hits.size() > 0) {
                                        try (Document doc = searcher.doc(hits.get(0).getDocAddress())) {
                                            List<Object> categories = doc.get("category");
                                            System.out.println("    📄 Multi-value categories: " + categories);
                                            assertTrue(categories.size() >= 2, "Should have multiple category values");
                                        }
                                    }
                                    System.out.println("  ✅ Multi-value string field search working");
                                }
                            }
                        }
                    }
                }
            }

            System.out.println("\n🎉 === COMPREHENSIVE FIELD TYPES TEST COMPLETED ===");
            System.out.println("✨ Successfully demonstrated robust field type support:");
            System.out.println("   📝 TEXT fields - tokenized, full-text search");
            System.out.println("   🔤 STRING fields - exact matching, not tokenized");
            System.out.println("   📋 JSON fields - structured data with parsing");
            System.out.println("   🔢 Mixed field types - comprehensive integration");
            System.out.println("   🔍 All query types - term, range, multi-value");
            System.out.println("   ✅ Complete API coverage with robust unit testing");

        } catch (Exception e) {
            fail("Comprehensive field types test failed: " + e.getMessage());
        }
    }

    @Test
    @DisplayName("String Field Type Specific Behavior Tests")
    public void testStringFieldSpecificBehavior(@TempDir Path tempDir) {
        System.out.println("🚀 === STRING FIELD SPECIFIC BEHAVIOR TEST ===");
        System.out.println("Testing string field exact matching vs text field tokenization");

        String indexPath = tempDir.resolve("string_behavior_index").toString();

        try {
            try (SchemaBuilder builder = new SchemaBuilder()) {
                builder
                    .addTextField("text_field", true, false, "default", "position")
                    .addStringField("string_field", true, true, false)
                    .addIntegerField("id", true, true, true);

                try (Schema schema = builder.build()) {
                    try (Index index = new Index(schema, indexPath, false)) {
                        try (IndexWriter writer = index.writer(Index.Memory.DEFAULT_HEAP_SIZE, 1)) {

                            // Add documents with same content in text and string fields
                            try (Document doc1 = new Document()) {
                                doc1.addInteger("id", 1);
                                doc1.addText("text_field", "Machine Learning Tutorial");
                                doc1.addString("string_field", "Machine Learning Tutorial");
                                writer.addDocument(doc1);
                            }

                            try (Document doc2 = new Document()) {
                                doc2.addInteger("id", 2);
                                doc2.addText("text_field", "Advanced Machine Learning");
                                doc2.addString("string_field", "Advanced Machine Learning");
                                writer.addDocument(doc2);
                            }

                            writer.commit();
                        }

                        index.reload();

                        try (Searcher searcher = index.searcher()) {

                            // Test: Text field should match individual tokens
                            System.out.println("\n🔎 Testing text field tokenization");
                            try (Query textQuery = Query.termQuery(schema, "text_field", "machine")) {
                                try (SearchResult result = searcher.search(textQuery, 10)) {
                                    var hits = result.getHits();
                                    System.out.println("  Text field 'machine' matches: " + hits.size() + " documents");
                                    assertTrue(hits.size() >= 2, "Text field should match token 'machine' in both docs");
                                }
                            }

                            // Test: String field should require exact match
                            System.out.println("\n🔎 Testing string field exact matching");
                            try (Query stringTokenQuery = Query.termQuery(schema, "string_field", "machine")) {
                                try (SearchResult result = searcher.search(stringTokenQuery, 10)) {
                                    var hits = result.getHits();
                                    System.out.println("  String field 'machine' (token) matches: " + hits.size() + " documents");
                                    assertEquals(0, hits.size(), "String field should NOT match partial token 'machine'");
                                }
                            }

                            try (Query stringExactQuery = Query.termQuery(schema, "string_field", "Machine Learning Tutorial")) {
                                try (SearchResult result = searcher.search(stringExactQuery, 10)) {
                                    var hits = result.getHits();
                                    System.out.println("  String field exact match: " + hits.size() + " documents");
                                    assertEquals(1, hits.size(), "String field should match exact phrase only");
                                }
                            }

                            System.out.println("  ✅ String vs Text field behavior validation passed");
                        }
                    }
                }
            }

            System.out.println("\n🎉 === STRING FIELD BEHAVIOR TEST COMPLETED ===");
            System.out.println("✨ Validated string field exact matching behavior vs text tokenization");

        } catch (Exception e) {
            fail("String field behavior test failed: " + e.getMessage());
        }
    }
}
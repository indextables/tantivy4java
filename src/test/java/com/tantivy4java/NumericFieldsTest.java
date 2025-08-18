package com.tantivy4java;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;
import static org.junit.jupiter.api.Assertions.*;

import java.util.Arrays;
import java.util.List;

/**
 * Test for numeric field types following Python tantivy library patterns
 */
public class NumericFieldsTest {
    
    @Test
    @DisplayName("Numeric field types test")
    public void testNumericFields() {
        System.out.println("=== Numeric Fields Test ===");
        System.out.println("Testing integer, float, and boolean field types");
        
        try (SchemaBuilder builder = new SchemaBuilder()) {
            // Build schema with numeric fields like in Python tests
            builder.addIntegerField("id", true, true, true)
                   .addFloatField("rating", true, true, true)
                   .addBooleanField("is_good", true, true, false)
                   .addTextField("body", true, false, "default", "position");
            
            try (Schema schema = builder.build()) {
                System.out.println("✓ Numeric schema created successfully");
                
                // Create an in-memory index
                try (Index index = new Index(schema, "", true)) {
                    System.out.println("✓ Index created successfully");
                    
                    // Create IndexWriter and add documents with numeric data
                    try (IndexWriter writer = index.writer(50, 1)) {
                        System.out.println("✓ IndexWriter created successfully");
                        
                        // Document 1: high rating, good
                        try (Document doc1 = new Document()) {
                            doc1.addInteger("id", 1);
                            doc1.addFloat("rating", 4.8);
                            doc1.addBoolean("is_good", true);
                            doc1.addText("body", "This is an excellent product");
                            long opstamp1 = writer.addDocument(doc1);
                            System.out.println("✓ Added document 1 (excellent), opstamp: " + opstamp1);
                        }
                        
                        // Document 2: medium rating, good
                        try (Document doc2 = new Document()) {
                            doc2.addInteger("id", 2);
                            doc2.addFloat("rating", 3.5);
                            doc2.addBoolean("is_good", true);
                            doc2.addText("body", "This is a good product");
                            long opstamp2 = writer.addDocument(doc2);
                            System.out.println("✓ Added document 2 (good), opstamp: " + opstamp2);
                        }
                        
                        // Document 3: low rating, bad
                        try (Document doc3 = new Document()) {
                            doc3.addInteger("id", 3);
                            doc3.addFloat("rating", 1.2);
                            doc3.addBoolean("is_good", false);
                            doc3.addText("body", "This is a poor product");
                            long opstamp3 = writer.addDocument(doc3);
                            System.out.println("✓ Added document 3 (poor), opstamp: " + opstamp3);
                        }
                        
                        // Commit the changes
                        long commitStamp = writer.commit();
                        System.out.println("✓ Committed documents, commit stamp: " + commitStamp);
                    }
                    
                    // Reload to see committed documents
                    index.reload();
                    System.out.println("✓ Reloaded index");
                    
                    // Create searcher and perform searches
                    try (Searcher searcher = index.searcher()) {
                        System.out.println("✓ Searcher created successfully");
                        assertEquals(3, searcher.getNumDocs(), "Should have 3 documents");
                        
                        // Search for "product" in text field
                        try (Query query = index.parseQuery("product", Arrays.asList("body"))) {
                            System.out.println("✓ Query created for 'product'");
                            
                            // Execute the search
                            try (SearchResult result = searcher.search(query, 10)) {
                                System.out.println("✓ Search executed successfully");
                                
                                var hits = result.getHits();
                                assertEquals(3, hits.size(), "Should find all 3 documents with 'product'");
                                System.out.println("Found " + hits.size() + " hits for 'product' search");
                                
                                // Verify numeric field retrieval
                                for (SearchResult.Hit hit : hits) {
                                    try (Document doc = searcher.doc(hit.getDocAddress())) {
                                        List<Object> ids = doc.get("id");
                                        assertFalse(ids.isEmpty(), "ID should not be empty");
                                        Long id = (Long) ids.get(0);
                                        
                                        List<Object> ratings = doc.get("rating");
                                        assertFalse(ratings.isEmpty(), "Rating should not be empty");
                                        Double rating = (Double) ratings.get(0);
                                        
                                        List<Object> isGoodValues = doc.get("is_good");
                                        assertFalse(isGoodValues.isEmpty(), "is_good should not be empty");
                                        Boolean isGood = (Boolean) isGoodValues.get(0);
                                        
                                        List<Object> bodies = doc.get("body");
                                        String body = bodies.isEmpty() ? "No body" : bodies.get(0).toString();
                                        
                                        String quality = isGood ? "Good" : "Poor";
                                        System.out.println("  📦 Product " + id + ": " + quality + 
                                                         " (rating: " + String.format("%.1f", rating) + 
                                                         ") - " + body.substring(0, Math.min(30, body.length())) + "...");
                                        
                                        // Validate expected ranges
                                        assertTrue(id >= 1 && id <= 3, "ID should be 1-3");
                                        assertTrue(rating >= 0.0 && rating <= 5.0, "Rating should be 0-5");
                                    }
                                }
                            }
                        }
                    }
                }
            }
            
            System.out.println("\n🎉 Numeric fields test PASSED!");
            System.out.println("✅ Successfully Demonstrated:");
            System.out.println("  🔢 Integer field support (addInteger)");
            System.out.println("  💯 Float field support (addFloat)");
            System.out.println("  ✅ Boolean field support (addBoolean)");
            System.out.println("  📊 Mixed field type documents");
            System.out.println("  🔍 Search across numeric schemas");
            System.out.println("  🏷️  Numeric field value extraction and validation");
            
        } catch (Exception e) {
            fail("Numeric fields test failed: " + e.getMessage());
        }
    }
}
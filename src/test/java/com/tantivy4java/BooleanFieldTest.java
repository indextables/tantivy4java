package com.tantivy4java;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;
import static org.junit.jupiter.api.Assertions.*;

import java.util.Arrays;
import java.util.List;

/**
 * Test for Boolean field support following Python tantivy library patterns
 */
public class BooleanFieldTest {
    
    @Test
    @DisplayName("Boolean field creation, indexing, and retrieval")
    public void testBooleanFieldSupport() {
        System.out.println("=== Boolean Field Support Test ===");
        System.out.println("Testing boolean field creation, indexing, and retrieval");
        
        try (SchemaBuilder builder = new SchemaBuilder()) {
            // Build schema with boolean field
            builder.addTextField("title", true, false, "default", "position")
                   .addBooleanField("is_active", true, true, true)  // stored, indexed, fast
                   .addBooleanField("featured", true, true, false)
                   .addIntegerField("id", true, true, true);
            
            try (Schema schema = builder.build()) {
                try (Index index = new Index(schema, "", true)) {
                    // Index documents with boolean fields
                    try (IndexWriter writer = index.writer(Index.Memory.DEFAULT_HEAP_SIZE, 1)) {
                        
                        // Document 1: Active and featured
                        try (Document doc1 = new Document()) {
                            doc1.addText("title", "Product A");
                            doc1.addBoolean("is_active", true);
                            doc1.addBoolean("featured", true);
                            doc1.addInteger("id", 1);
                            writer.addDocument(doc1);
                        }
                        
                        // Document 2: Active but not featured
                        try (Document doc2 = new Document()) {
                            doc2.addText("title", "Product B");
                            doc2.addBoolean("is_active", true);
                            doc2.addBoolean("featured", false);
                            doc2.addInteger("id", 2);
                            writer.addDocument(doc2);
                        }
                        
                        // Document 3: Not active and not featured
                        try (Document doc3 = new Document()) {
                            doc3.addText("title", "Product C");
                            doc3.addBoolean("is_active", false);
                            doc3.addBoolean("featured", false);
                            doc3.addInteger("id", 3);
                            writer.addDocument(doc3);
                        }
                        
                        writer.commit();
                        System.out.println("✓ Indexed 3 documents with boolean fields");
                    }
                    
                    // Reload to see committed documents
                    index.reload();
                    System.out.println("✓ Reloaded index");
                    
                    try (Searcher searcher = index.searcher()) {
                        System.out.println("✓ Created searcher");
                        assertEquals(3, searcher.getNumDocs(), "Should have 3 documents");
                        
                        // Test basic search functionality with boolean fields
                        System.out.println("\n=== Testing Document Retrieval with Boolean Fields ===");
                        
                        try (Query allQuery = Query.allQuery()) {
                            try (SearchResult result = searcher.search(allQuery, 10)) {
                                var hits = result.getHits();
                                assertEquals(3, hits.size(), "Should find all 3 documents");
                                
                                for (int i = 0; i < hits.size(); i++) {
                                    SearchResult.Hit hit = hits.get(i);
                                    System.out.println("\n--- Document " + (i+1) + " ---");
                                    System.out.println("Score: " + hit.getScore());
                                    
                                    // Retrieve document with boolean fields
                                    try (Document retrievedDoc = searcher.doc(hit.getDocAddress())) {
                                        System.out.println("Document field extraction:");
                                        
                                        // Extract text field
                                        List<Object> titleValues = retrievedDoc.get("title");
                                        assertFalse(titleValues.isEmpty(), "Title field should not be empty");
                                        System.out.println("  📖 Title: " + titleValues.get(0));
                                        
                                        // Extract boolean fields
                                        List<Object> activeValues = retrievedDoc.get("is_active");
                                        assertFalse(activeValues.isEmpty(), "Active field should not be empty");
                                        Object activeValue = activeValues.get(0);
                                        assertInstanceOf(Boolean.class, activeValue, "Active value should be Boolean");
                                        System.out.println("  ✅ Is Active: " + activeValue + " (type: " + activeValue.getClass().getSimpleName() + ")");
                                        
                                        List<Object> featuredValues = retrievedDoc.get("featured");
                                        assertFalse(featuredValues.isEmpty(), "Featured field should not be empty");
                                        Object featuredValue = featuredValues.get(0);
                                        assertInstanceOf(Boolean.class, featuredValue, "Featured value should be Boolean");
                                        System.out.println("  🌟 Featured: " + featuredValue + " (type: " + featuredValue.getClass().getSimpleName() + ")");
                                        
                                        // Extract integer field
                                        List<Object> idValues = retrievedDoc.get("id");
                                        assertFalse(idValues.isEmpty(), "ID field should not be empty");
                                        System.out.println("  🔢 ID: " + idValues.get(0));
                                    }
                                }
                            }
                        }
                        
                        // Test search by title (to verify basic search still works)
                        System.out.println("\n=== Testing Search Functionality ===");
                        try (Query titleQuery = index.parseQuery("Product", Arrays.asList("title"))) {
                            try (SearchResult result = searcher.search(titleQuery, 5)) {
                                var hits = result.getHits();
                                assertEquals(3, hits.size(), "Should find all 3 products");
                                System.out.println("Product search found " + hits.size() + " documents");
                                
                                for (SearchResult.Hit hit : hits) {
                                    try (Document doc = searcher.doc(hit.getDocAddress())) {
                                        List<Object> titles = doc.get("title");
                                        String title = titles.isEmpty() ? "No title" : titles.get(0).toString();
                                        assertTrue(title.contains("Product"), "Title should contain 'Product'");
                                        
                                        List<Object> activeValues = doc.get("is_active");
                                        Boolean isActive = activeValues.isEmpty() ? false : (Boolean) activeValues.get(0);
                                        
                                        List<Object> featuredValues = doc.get("featured");
                                        Boolean isFeatured = featuredValues.isEmpty() ? false : (Boolean) featuredValues.get(0);
                                        
                                        String status = isActive ? "Active" : "Inactive";
                                        String featured = isFeatured ? "Featured" : "Standard";
                                        
                                        System.out.println("  📦 Found: \"" + title + "\" (" + status + ", " + featured + 
                                                         ") (score: " + String.format("%.3f", hit.getScore()) + ")");
                                    }
                                }
                            }
                        }
                    }
                }
            }
            
            System.out.println("\n🎉 Boolean Field Test COMPLETED!");
            System.out.println("\n✅ Successfully Demonstrated:");
            System.out.println("  ✅ Boolean field creation in schema (addBooleanField)");
            System.out.println("  📝 Boolean value addition to documents (addBoolean)");
            System.out.println("  🔍 Document indexing and retrieval with boolean fields");
            System.out.println("  🏷️  Boolean field value extraction as Boolean objects");
            System.out.println("  🔧 Python tantivy library compatibility for boolean fields");
            
        } catch (Exception e) {
            fail("Boolean field test failed: " + e.getMessage());
        }
    }
    
    @Test
    @DisplayName("Boolean field edge cases and mixed values")
    public void testBooleanFieldEdgeCases() {
        System.out.println("\n=== Boolean Field Edge Cases Test ===");
        
        try (SchemaBuilder builder = new SchemaBuilder()) {
            builder.addBooleanField("flag", true, true, true)
                   .addTextField("description", true, false, "default", "position");
            
            try (Schema schema = builder.build()) {
                try (Index index = new Index(schema, "", true)) {
                    try (IndexWriter writer = index.writer(Index.Memory.DEFAULT_HEAP_SIZE, 1)) {
                        
                        // Test with mixed boolean values
                        boolean[] testValues = {true, false, true, false, true};
                        String[] descriptions = {"First", "Second", "Third", "Fourth", "Fifth"};
                        
                        for (int i = 0; i < testValues.length; i++) {
                            try (Document doc = new Document()) {
                                doc.addBoolean("flag", testValues[i]);
                                doc.addText("description", descriptions[i]);
                                writer.addDocument(doc);
                            }
                        }
                        
                        writer.commit();
                        System.out.println("✓ Indexed 5 documents with mixed boolean values");
                    }
                    
                    index.reload();
                    
                    try (Searcher searcher = index.searcher()) {
                        assertEquals(5, searcher.getNumDocs(), "Should have all test documents");
                        
                        try (Query allQuery = Query.allQuery()) {
                            try (SearchResult result = searcher.search(allQuery, 10)) {
                                var hits = result.getHits();
                                assertEquals(5, hits.size(), "Should find all documents");
                                
                                int trueCount = 0, falseCount = 0;
                                
                                for (SearchResult.Hit hit : hits) {
                                    try (Document doc = searcher.doc(hit.getDocAddress())) {
                                        List<Object> flagValues = doc.get("flag");
                                        assertFalse(flagValues.isEmpty(), "Flag field should not be empty");
                                        Boolean flag = (Boolean) flagValues.get(0);
                                        
                                        List<Object> descValues = doc.get("description");
                                        String description = descValues.isEmpty() ? "Unknown" : descValues.get(0).toString();
                                        
                                        if (flag) {
                                            trueCount++;
                                        } else {
                                            falseCount++;
                                        }
                                        
                                        System.out.println("Document: " + description + " -> " + flag);
                                    }
                                }
                                
                                assertEquals(3, trueCount, "Should have 3 true values");
                                assertEquals(2, falseCount, "Should have 2 false values");
                            }
                        }
                    }
                }
            }
            
            System.out.println("✅ Boolean field edge cases test passed");
            
        } catch (Exception e) {
            fail("Boolean field edge cases test failed: " + e.getMessage());
        }
    }
}
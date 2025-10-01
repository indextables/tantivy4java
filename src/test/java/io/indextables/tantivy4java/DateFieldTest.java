package io.indextables.tantivy4java;

import io.indextables.tantivy4java.core.*;
import io.indextables.tantivy4java.query.*;
import io.indextables.tantivy4java.result.*;


import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;
import static org.junit.jupiter.api.Assertions.*;

import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.List;

/**
 * Test for Date field support following Python tantivy library patterns
 */
public class DateFieldTest {
    
    @Test
    @DisplayName("Date field creation, indexing, and retrieval")
    public void testDateFieldSupport() {
        System.out.println("=== Date Field Support Test ===");
        System.out.println("Testing date field creation, indexing, and retrieval");
        
        try (SchemaBuilder builder = new SchemaBuilder()) {
            // Build schema with date field
            builder.addTextField("title", true, false, "default", "position")
                   .addDateField("order", true, true, true)  // stored, indexed, fast
                   .addIntegerField("id", true, true, true);
            
            try (Schema schema = builder.build()) {
                try (Index index = new Index(schema, "", true)) {
                    // Index documents with date fields
                    try (IndexWriter writer = index.writer(Index.Memory.DEFAULT_HEAP_SIZE, 1)) {
                        
                        // Document 1: 2020-01-01
                        try (Document doc1 = new Document()) {
                            doc1.addText("title", "Test title");
                            doc1.addDate("order", LocalDateTime.of(2020, 1, 1, 0, 0, 0));
                            doc1.addInteger("id", 1);
                            writer.addDocument(doc1);
                        }
                        
                        // Document 2: 2021-01-01  
                        try (Document doc2 = new Document()) {
                            doc2.addText("title", "Another test title");
                            doc2.addDate("order", LocalDateTime.of(2021, 1, 1, 0, 0, 0));
                            doc2.addInteger("id", 2);
                            writer.addDocument(doc2);
                        }
                        
                        // Document 3: 2022-01-01
                        try (Document doc3 = new Document()) {
                            doc3.addText("title", "Final test title");
                            doc3.addDate("order", LocalDateTime.of(2022, 1, 1, 0, 0, 0));
                            doc3.addInteger("id", 3);
                            writer.addDocument(doc3);
                        }
                        
                        writer.commit();
                        System.out.println("✓ Indexed 3 documents with date fields");
                    }
                    
                    // Reload to see committed documents
                    index.reload();
                    System.out.println("✓ Reloaded index");
                    
                    try (Searcher searcher = index.searcher()) {
                        System.out.println("✓ Created searcher");
                        assertEquals(3, searcher.getNumDocs(), "Should have 3 documents");
                        
                        // Test basic search functionality with date fields
                        System.out.println("\n=== Testing Document Retrieval with Date Fields ===");
                        
                        try (Query allQuery = Query.allQuery()) {
                            try (SearchResult result = searcher.search(allQuery, 10)) {
                                var hits = result.getHits();
                                assertEquals(3, hits.size(), "Should find all 3 documents");
                                
                                for (int i = 0; i < hits.size(); i++) {
                                    SearchResult.Hit hit = hits.get(i);
                                    System.out.println("\n--- Document " + (i+1) + " ---");
                                    System.out.println("Score: " + hit.getScore());
                                    
                                    // Retrieve document with date field
                                    try (Document retrievedDoc = searcher.doc(hit.getDocAddress())) {
                                        System.out.println("Document field extraction:");
                                        
                                        // Extract text field
                                        List<Object> titleValues = retrievedDoc.get("title");
                                        assertFalse(titleValues.isEmpty(), "Title field should not be empty");
                                        System.out.println("  📖 Title: " + titleValues.get(0));
                                        
                                        // Extract date field
                                        List<Object> dateValues = retrievedDoc.get("order");
                                        assertFalse(dateValues.isEmpty(), "Date field should not be empty");
                                        Object dateValue = dateValues.get(0);
                                        assertInstanceOf(LocalDateTime.class, dateValue, "Date value should be LocalDateTime");
                                        System.out.println("  📅 Order Date: " + dateValue + " (type: " + dateValue.getClass().getSimpleName() + ")");
                                        
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
                        try (Query titleQuery = index.parseQuery("title", Arrays.asList("title"))) {
                            try (SearchResult result = searcher.search(titleQuery, 5)) {
                                var hits = result.getHits();
                                assertTrue(hits.size() > 0, "Title search should find documents");
                                System.out.println("Title search found " + hits.size() + " documents");
                                
                                for (SearchResult.Hit hit : hits) {
                                    try (Document doc = searcher.doc(hit.getDocAddress())) {
                                        List<Object> titles = doc.get("title");
                                        String title = titles.isEmpty() ? "No title" : titles.get(0).toString();
                                        assertTrue(title.contains("title"), "Title should contain 'title'");
                                        System.out.println("  📖 Found: \"" + title + "\" (score: " + String.format("%.3f", hit.getScore()) + ")");
                                    }
                                }
                            }
                        }
                    }
                }
            }
            
            System.out.println("\n🎉 Date Field Test COMPLETED!");
            System.out.println("\n✅ Successfully Demonstrated:");
            System.out.println("  📅 Date field creation in schema (addDateField)");
            System.out.println("  📝 Date value addition to documents (addDate)");
            System.out.println("  🔍 Document indexing and retrieval with date fields");
            System.out.println("  🏷️  Date field value extraction as LocalDateTime objects");
            System.out.println("  🔧 Python tantivy library compatibility for date fields");
            
        } catch (Exception e) {
            fail("Date field test failed: " + e.getMessage());
        }
    }
    
    @Test
    @DisplayName("Date field edge cases and validation")
    public void testDateFieldEdgeCases() {
        System.out.println("\n=== Date Field Edge Cases Test ===");
        
        try (SchemaBuilder builder = new SchemaBuilder()) {
            builder.addDateField("date", true, true, true);
            
            try (Schema schema = builder.build()) {
                try (Index index = new Index(schema, "", true)) {
                    try (IndexWriter writer = index.writer(Index.Memory.DEFAULT_HEAP_SIZE, 1)) {
                        
                        // Test various date values
                        try (Document doc1 = new Document()) {
                            doc1.addDate("date", LocalDateTime.of(1970, 1, 1, 0, 0, 0)); // Unix epoch
                            writer.addDocument(doc1);
                        }
                        
                        try (Document doc2 = new Document()) {
                            doc2.addDate("date", LocalDateTime.of(2038, 1, 19, 3, 14, 7)); // Near Y2038 problem
                            writer.addDocument(doc2);
                        }
                        
                        try (Document doc3 = new Document()) {
                            doc3.addDate("date", LocalDateTime.of(2023, 12, 31, 23, 59, 59)); // End of year
                            writer.addDocument(doc3);
                        }
                        
                        writer.commit();
                    }
                    
                    index.reload();
                    
                    try (Searcher searcher = index.searcher()) {
                        assertEquals(3, searcher.getNumDocs(), "Should have 3 documents with edge case dates");
                        
                        try (Query allQuery = Query.allQuery()) {
                            try (SearchResult result = searcher.search(allQuery, 10)) {
                                var hits = result.getHits();
                                assertEquals(3, hits.size(), "Should find all edge case documents");
                                
                                for (SearchResult.Hit hit : hits) {
                                    try (Document doc = searcher.doc(hit.getDocAddress())) {
                                        List<Object> dateValues = doc.get("date");
                                        assertFalse(dateValues.isEmpty(), "Date field should not be empty");
                                        assertInstanceOf(LocalDateTime.class, dateValues.get(0), "Date should be LocalDateTime");
                                        LocalDateTime date = (LocalDateTime) dateValues.get(0);
                                        System.out.println("Edge case date: " + date);
                                    }
                                }
                            }
                        }
                    }
                }
            }
            
            System.out.println("✅ Date field edge cases passed");
            
        } catch (Exception e) {
            fail("Date field edge cases test failed: " + e.getMessage());
        }
    }
}

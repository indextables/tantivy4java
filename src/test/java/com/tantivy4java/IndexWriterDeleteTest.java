package com.tantivy4java;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;
import static org.junit.jupiter.api.Assertions.*;

import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.List;

/**
 * Test for IndexWriter delete operations following Python tantivy library patterns
 */
public class IndexWriterDeleteTest {
    
    @Test
    @DisplayName("Delete all documents test")
    public void testDeleteAllDocuments() {
        System.out.println("=== Delete All Documents Test ===");
        System.out.println("Testing deleteAllDocuments functionality");
        
        try (SchemaBuilder builder = new SchemaBuilder()) {
            builder.addTextField("title", true, false, "default", "position")
                   .addIntegerField("id", true, true, true);
            
            try (Schema schema = builder.build()) {
                try (Index index = new Index(schema, "", true)) {
                    
                    // Add some documents first
                    try (IndexWriter writer = index.writer(Index.Memory.DEFAULT_HEAP_SIZE, 1)) {
                        for (int i = 1; i <= 5; i++) {
                            try (Document doc = new Document()) {
                                doc.addText("title", "Test Document " + i);
                                doc.addInteger("id", i);
                                writer.addDocument(doc);
                            }
                        }
                        writer.commit();
                        System.out.println("✓ Added 5 documents");
                    }
                    
                    index.reload();
                    
                    // Verify documents are there
                    try (Searcher searcher = index.searcher()) {
                        assertEquals(5, searcher.getNumDocs(), "Should have 5 documents before delete");
                        System.out.println("✓ Verified 5 documents present");
                    }
                    
                    // Delete all documents
                    try (IndexWriter writer = index.writer(Index.Memory.DEFAULT_HEAP_SIZE, 1)) {
                        writer.deleteAllDocuments();
                        writer.commit();
                        System.out.println("✓ Deleted all documents");
                    }
                    
                    index.reload();
                    
                    // Verify all documents are deleted
                    try (Searcher searcher = index.searcher()) {
                        assertEquals(0, searcher.getNumDocs(), "Should have 0 documents after delete all");
                        System.out.println("✓ Verified all documents deleted");
                    }
                }
            }
            
            System.out.println("\n🎉 Delete All Documents Test PASSED!");
            
        } catch (Exception e) {
            fail("Delete all documents test failed: " + e.getMessage());
        }
    }
    
    @Test
    @DisplayName("Delete documents by term test")
    public void testDeleteDocumentsByTerm() {
        System.out.println("\n=== Delete Documents By Term Test ===");
        System.out.println("Testing deleteDocumentsByTerm functionality");
        
        try (SchemaBuilder builder = new SchemaBuilder()) {
            builder.addTextField("title", true, false, "default", "position")
                   .addIntegerField("id", true, true, true)
                   .addBooleanField("active", true, true, true);
            
            try (Schema schema = builder.build()) {
                try (Index index = new Index(schema, "", true)) {
                    
                    // Add documents with different values
                    try (IndexWriter writer = index.writer(Index.Memory.DEFAULT_HEAP_SIZE, 1)) {
                        // Active documents
                        for (int i = 1; i <= 3; i++) {
                            try (Document doc = new Document()) {
                                doc.addText("title", "Active Document " + i);
                                doc.addInteger("id", i);
                                doc.addBoolean("active", true);
                                writer.addDocument(doc);
                            }
                        }
                        
                        // Inactive documents
                        for (int i = 4; i <= 6; i++) {
                            try (Document doc = new Document()) {
                                doc.addText("title", "Inactive Document " + i);
                                doc.addInteger("id", i);
                                doc.addBoolean("active", false);
                                writer.addDocument(doc);
                            }
                        }
                        
                        writer.commit();
                        System.out.println("✓ Added 6 documents (3 active, 3 inactive)");
                    }
                    
                    index.reload();
                    
                    // Verify initial count
                    try (Searcher searcher = index.searcher()) {
                        assertEquals(6, searcher.getNumDocs(), "Should have 6 documents initially");
                    }
                    
                    // Delete documents by boolean term (delete inactive documents)
                    try (IndexWriter writer = index.writer(Index.Memory.DEFAULT_HEAP_SIZE, 1)) {
                        long opstamp = writer.deleteDocumentsByTerm("active", false);
                        writer.commit();
                        System.out.println("✓ Delete operation completed with opstamp: " + opstamp);
                        // Note: Return value is opstamp, not document count
                    }
                    
                    index.reload();
                    
                    // Verify only active documents remain
                    try (Searcher searcher = index.searcher()) {
                        assertEquals(3, searcher.getNumDocs(), "Should have 3 documents after deleting inactive");
                        
                        // Verify remaining documents are all active
                        try (Query allQuery = Query.allQuery()) {
                            try (SearchResult result = searcher.search(allQuery, 10)) {
                                var hits = result.getHits();
                                assertEquals(3, hits.size(), "Should find all 3 remaining documents");
                                
                                for (SearchResult.Hit hit : hits) {
                                    try (Document doc = searcher.doc(hit.getDocAddress())) {
                                        List<Object> activeValues = doc.get("active");
                                        assertFalse(activeValues.isEmpty(), "Active field should not be empty");
                                        Boolean isActive = (Boolean) activeValues.get(0);
                                        assertTrue(isActive, "All remaining documents should be active");
                                        
                                        List<Object> titles = doc.get("title");
                                        String title = titles.isEmpty() ? "No title" : titles.get(0).toString();
                                        System.out.println("  📄 Remaining: " + title + " (active: " + isActive + ")");
                                    }
                                }
                            }
                        }
                    }
                    
                    // Delete documents by integer term (delete id = 2)
                    try (IndexWriter writer = index.writer(Index.Memory.DEFAULT_HEAP_SIZE, 1)) {
                        long opstamp = writer.deleteDocumentsByTerm("id", 2L);
                        writer.commit();
                        System.out.println("✓ Delete operation completed with opstamp: " + opstamp);
                        // Note: Return value is opstamp, not document count
                    }
                    
                    index.reload();
                    
                    // Verify final count
                    try (Searcher searcher = index.searcher()) {
                        assertEquals(2, searcher.getNumDocs(), "Should have 2 documents after deleting id=2");
                        System.out.println("✓ Final document count: 2");
                    }
                }
            }
            
            System.out.println("\n🎉 Delete Documents By Term Test PASSED!");
            
        } catch (Exception e) {
            fail("Delete documents by term test failed: " + e.getMessage());
        }
    }
    
    @Test
    @DisplayName("Delete documents by query test")
    public void testDeleteDocumentsByQuery() {
        System.out.println("\n=== Delete Documents By Query Test ===");
        System.out.println("Testing deleteDocumentsByQuery functionality");
        
        try (SchemaBuilder builder = new SchemaBuilder()) {
            builder.addTextField("title", true, false, "default", "position")
                   .addTextField("category", true, false, "default", "position")
                   .addIntegerField("id", true, true, true);
            
            try (Schema schema = builder.build()) {
                try (Index index = new Index(schema, "", true)) {
                    
                    // Add documents in different categories
                    try (IndexWriter writer = index.writer(Index.Memory.DEFAULT_HEAP_SIZE, 1)) {
                        // Tech category
                        for (int i = 1; i <= 3; i++) {
                            try (Document doc = new Document()) {
                                doc.addText("title", "Tech Article " + i);
                                doc.addText("category", "technology");
                                doc.addInteger("id", i);
                                writer.addDocument(doc);
                            }
                        }
                        
                        // Science category
                        for (int i = 4; i <= 6; i++) {
                            try (Document doc = new Document()) {
                                doc.addText("title", "Science Paper " + i);
                                doc.addText("category", "science");
                                doc.addInteger("id", i);
                                writer.addDocument(doc);
                            }
                        }
                        
                        // Sports category
                        for (int i = 7; i <= 9; i++) {
                            try (Document doc = new Document()) {
                                doc.addText("title", "Sports News " + i);
                                doc.addText("category", "sports");
                                doc.addInteger("id", i);
                                writer.addDocument(doc);
                            }
                        }
                        
                        writer.commit();
                        System.out.println("✓ Added 9 documents (3 tech, 3 science, 3 sports)");
                    }
                    
                    index.reload();
                    
                    // Verify initial count
                    try (Searcher searcher = index.searcher()) {
                        assertEquals(9, searcher.getNumDocs(), "Should have 9 documents initially");
                    }
                    
                    // Delete documents by query (delete all technology articles)
                    try (IndexWriter writer = index.writer(Index.Memory.DEFAULT_HEAP_SIZE, 1)) {
                        try (Query deleteQuery = index.parseQuery("technology", Arrays.asList("category"))) {
                            long opstamp = writer.deleteDocumentsByQuery(deleteQuery);
                            writer.commit();
                            System.out.println("✓ Delete operation completed with opstamp: " + opstamp);
                            // Note: Return value is opstamp, not document count
                        }
                    }
                    
                    index.reload();
                    
                    // Verify remaining documents
                    try (Searcher searcher = index.searcher()) {
                        int remainingCount = searcher.getNumDocs();
                        System.out.println("✓ Remaining document count after delete: " + remainingCount);
                        assertEquals(6, remainingCount, "Should have 6 documents after deleting technology");
                        
                        // Verify no technology documents remain
                        try (Query techQuery = index.parseQuery("technology", Arrays.asList("category"))) {
                            try (SearchResult result = searcher.search(techQuery, 10)) {
                                var hits = result.getHits();
                                assertEquals(0, hits.size(), "Should find no technology documents");
                                System.out.println("✓ No technology documents found after deletion");
                            }
                        }
                        
                        // Verify science documents remain
                        try (Query scienceQuery = index.parseQuery("science", Arrays.asList("category"))) {
                            try (SearchResult result = searcher.search(scienceQuery, 10)) {
                                var hits = result.getHits();
                                assertEquals(3, hits.size(), "Should find 3 science documents");
                                System.out.println("✓ Found " + hits.size() + " science documents");
                            }
                        }
                        
                        // Verify sports documents remain
                        try (Query sportsQuery = index.parseQuery("sports", Arrays.asList("category"))) {
                            try (SearchResult result = searcher.search(sportsQuery, 10)) {
                                var hits = result.getHits();
                                assertEquals(3, hits.size(), "Should find 3 sports documents");
                                System.out.println("✓ Found " + hits.size() + " sports documents");
                            }
                        }
                    }
                }
            }
            
            System.out.println("\n🎉 Delete Documents By Query Test PASSED!");
            
        } catch (Exception e) {
            fail("Delete documents by query test failed: " + e.getMessage());
        }
    }
    
    @Test
    @DisplayName("Delete operations with mixed field types test")
    public void testDeleteMixedFieldTypes() {
        System.out.println("\n=== Delete Mixed Field Types Test ===");
        System.out.println("Testing delete operations with various field types");
        
        try (SchemaBuilder builder = new SchemaBuilder()) {
            builder.addTextField("title", true, false, "default", "position")
                   .addIntegerField("id", true, true, true)
                   .addFloatField("rating", true, true, true)
                   .addBooleanField("featured", true, true, true)
                   .addDateField("created", true, true, true)
                   .addIpAddrField("ip", true, true, true);
            
            try (Schema schema = builder.build()) {
                try (Index index = new Index(schema, "", true)) {
                    
                    // Add documents with various field types
                    try (IndexWriter writer = index.writer(Index.Memory.DEFAULT_HEAP_SIZE, 1)) {
                        try (Document doc1 = new Document()) {
                            doc1.addText("title", "First Document");
                            doc1.addInteger("id", 1);
                            doc1.addFloat("rating", 4.5);
                            doc1.addBoolean("featured", true);
                            doc1.addDate("created", LocalDateTime.of(2023, 1, 1, 12, 0, 0));
                            doc1.addIpAddr("ip", "192.168.1.1");
                            writer.addDocument(doc1);
                        }
                        
                        try (Document doc2 = new Document()) {
                            doc2.addText("title", "Second Document");
                            doc2.addInteger("id", 2);
                            doc2.addFloat("rating", 3.5);
                            doc2.addBoolean("featured", false);
                            doc2.addDate("created", LocalDateTime.of(2023, 6, 15, 14, 30, 0));
                            doc2.addIpAddr("ip", "10.0.0.1");
                            writer.addDocument(doc2);
                        }
                        
                        try (Document doc3 = new Document()) {
                            doc3.addText("title", "Third Document");
                            doc3.addInteger("id", 3);
                            doc3.addFloat("rating", 4.5);
                            doc3.addBoolean("featured", true);
                            doc3.addDate("created", LocalDateTime.of(2023, 12, 31, 23, 59, 59));
                            doc3.addIpAddr("ip", "127.0.0.1");
                            writer.addDocument(doc3);
                        }
                        
                        writer.commit();
                        System.out.println("✓ Added 3 documents with mixed field types");
                    }
                    
                    index.reload();
                    
                    // Test delete by float field
                    try (IndexWriter writer = index.writer(Index.Memory.DEFAULT_HEAP_SIZE, 1)) {
                        long opstamp = writer.deleteDocumentsByTerm("rating", 4.5);
                        writer.commit();
                        System.out.println("✓ Delete operation completed with opstamp: " + opstamp);
                        // Note: Return value is opstamp, not document count
                    }
                    
                    index.reload();
                    
                    // Verify documents were deleted properly (should have 1 remaining)
                    try (Searcher searcher = index.searcher()) {
                        int finalCount = searcher.getNumDocs();
                        System.out.println("✓ Final document count: " + finalCount);
                        assertEquals(1, finalCount, "Should have 1 document after deleting rating=4.5");
                        
                        try (Query allQuery = Query.allQuery()) {
                            try (SearchResult result = searcher.search(allQuery, 10)) {
                                var hits = result.getHits();
                                assertEquals(1, hits.size(), "Should find 1 remaining document");
                                
                                try (Document doc = searcher.doc(hits.get(0).getDocAddress())) {
                                    List<Object> ratingValues = doc.get("rating");
                                    Double rating = (Double) ratingValues.get(0);
                                    assertEquals(3.5, rating, 0.01, "Remaining document should have rating=3.5");
                                    
                                    List<Object> titleValues = doc.get("title");
                                    String title = titleValues.get(0).toString();
                                    assertEquals("Second Document", title, "Should be the second document");
                                    
                                    System.out.println("✓ Remaining document: " + title + " (rating: " + rating + ")");
                                }
                            }
                        }
                    }
                }
            }
            
            System.out.println("\n🎉 Delete Mixed Field Types Test PASSED!");
            
        } catch (Exception e) {
            fail("Delete mixed field types test failed: " + e.getMessage());
        }
    }
}
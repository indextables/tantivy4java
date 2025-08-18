package com.tantivy4java;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;
import static org.junit.jupiter.api.Assertions.*;

import java.util.Arrays;
import java.util.List;

/**
 * Test for complete workflow functionality following Python tantivy library patterns
 */
public class WorkflowTest {
    
    @Test
    @DisplayName("Complete search workflow test")
    public void testCompleteWorkflow() {
        System.out.println("=== Complete Workflow Test ===");
        System.out.println("Testing complete Tantivy4Java search workflow");
        
        try (SchemaBuilder builder = new SchemaBuilder()) {
            // Build schema with text fields
            builder.addTextField("title", true, false, "default", "position")
                   .addTextField("body", true, false, "default", "position");
            
            try (Schema schema = builder.build()) {
                System.out.println("✓ Schema created successfully");
                
                // Create an in-memory index
                try (Index index = new Index(schema, "", true)) {
                    System.out.println("✓ Index created successfully");
                    
                    // Create IndexWriter and add documents
                    try (IndexWriter writer = index.writer(50, 1)) {
                        System.out.println("✓ IndexWriter created successfully");
                        
                        // Create and add test documents
                        try (Document doc1 = new Document()) {
                            doc1.addText("title", "Test Document One");
                            doc1.addText("body", "This is a test document for search functionality");
                            long opstamp1 = writer.addDocument(doc1);
                            System.out.println("✓ Added document 1, opstamp: " + opstamp1);
                        }
                        
                        try (Document doc2 = new Document()) {
                            doc2.addText("title", "Test Document Two");
                            doc2.addText("body", "Another test document with different content");
                            long opstamp2 = writer.addDocument(doc2);
                            System.out.println("✓ Added document 2, opstamp: " + opstamp2);
                        }
                        
                        // Commit the changes
                        long commitStamp = writer.commit();
                        System.out.println("✓ Committed documents, commit stamp: " + commitStamp);
                    }
                    
                    // Reload to see committed documents
                    index.reload();
                    System.out.println("✓ Reloaded index");
                    
                    // Create searcher and perform search
                    try (Searcher searcher = index.searcher()) {
                        System.out.println("✓ Searcher created successfully");
                        assertEquals(2, searcher.getNumDocs(), "Should have 2 documents");
                        
                        // Search using parseQuery for "document"
                        try (Query query = index.parseQuery("document", Arrays.asList("body"))) {
                            System.out.println("✓ Query created for 'document'");
                            
                            // Execute the search
                            try (SearchResult result = searcher.search(query, 10)) {
                                System.out.println("✓ Search executed successfully");
                                
                                var hits = result.getHits();
                                assertEquals(2, hits.size(), "Should find both documents");
                                System.out.println("Found " + hits.size() + " hits for 'document' search");
                                
                                // Verify document retrieval
                                for (int i = 0; i < hits.size(); i++) {
                                    SearchResult.Hit hit = hits.get(i);
                                    try (Document doc = searcher.doc(hit.getDocAddress())) {
                                        List<Object> titles = doc.get("title");
                                        assertFalse(titles.isEmpty(), "Title should not be empty");
                                        String title = titles.get(0).toString();
                                        assertTrue(title.contains("Document"), "Title should contain 'Document'");
                                        System.out.println("  📖 Found: \"" + title + "\" (score: " + 
                                                         String.format("%.3f", hit.getScore()) + ")");
                                    }
                                }
                            }
                        }
                    }
                }
            }
            
            System.out.println("\n🎉 Complete workflow test PASSED!");
            System.out.println("✅ Successfully Demonstrated:");
            System.out.println("  ✅ Schema creation with multiple fields");
            System.out.println("  📝 Document indexing with text fields");
            System.out.println("  🔍 Query parsing and execution");
            System.out.println("  🏷️  Document retrieval and field extraction");
            System.out.println("  🔧 End-to-end search workflow");
            
        } catch (Exception e) {
            fail("Complete workflow test failed: " + e.getMessage());
        }
    }
}
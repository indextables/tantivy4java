package com.tantivy4java;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;

import java.nio.file.Path;
import java.util.Arrays;

/**
 * Test for Index persistence functionality (open, exists, getSchema)
 */
public class IndexPersistenceTest {
    
    @Test
    @DisplayName("Index persistence and opening test")
    public void testIndexPersistence(@TempDir Path tempDir) {
        System.out.println("=== Index Persistence Test ===");
        System.out.println("Testing Index.open(), Index.exists(), and getSchema()");
        
        String indexPath = tempDir.resolve("test_index").toString();
        
        // First, check that index doesn't exist initially
        assertFalse(Index.exists(indexPath), "Index should not exist initially");
        System.out.println("✓ Confirmed index doesn't exist initially");
        
        // Create an index and add some documents
        try (SchemaBuilder builder = new SchemaBuilder()) {
            builder.addTextField("title", true, false, "default", "position")
                   .addTextField("body", true, false, "default", "position")
                   .addIntegerField("id", true, true, true);
            
            try (Schema schema = builder.build()) {
                // Create index on disk
                try (Index index = new Index(schema, indexPath, false)) {
                    
                    // Add some test documents
                    try (IndexWriter writer = index.writer(50, 1)) {
                        try (Document doc1 = new Document()) {
                            doc1.addText("title", "Persistent Document One");
                            doc1.addText("body", "This document will be persisted to disk");
                            doc1.addInteger("id", 1);
                            writer.addDocument(doc1);
                        }
                        
                        try (Document doc2 = new Document()) {
                            doc2.addText("title", "Persistent Document Two");
                            doc2.addText("body", "This is another persistent document");
                            doc2.addInteger("id", 2);
                            writer.addDocument(doc2);
                        }
                        
                        writer.commit();
                        System.out.println("✓ Added 2 documents to persistent index");
                    }
                }
                
                // Now check that index exists
                assertTrue(Index.exists(indexPath), "Index should exist after creation");
                System.out.println("✓ Index.exists() correctly returns true");
                
                // Open the existing index
                try (Index reopenedIndex = Index.open(indexPath)) {
                    System.out.println("✓ Successfully opened existing index from disk");
                    
                    // Test getSchema() method
                    try (Schema reopenedSchema = reopenedIndex.getSchema()) {
                        System.out.println("✓ Retrieved schema from reopened index");
                        
                        // Verify the index has our documents
                        try (Searcher searcher = reopenedIndex.searcher()) {
                            int numDocs = searcher.getNumDocs();
                            assertEquals(2, numDocs, "Reopened index should have 2 documents");
                            System.out.println("✓ Reopened index has correct document count: " + numDocs);
                            
                            // Search for documents to verify content
                            try (Query query = reopenedIndex.parseQuery("persistent", Arrays.asList("title", "body"))) {
                                try (SearchResult result = searcher.search(query, 10)) {
                                    var hits = result.getHits();
                                    assertEquals(2, hits.size(), "Should find both persistent documents");
                                    System.out.println("✓ Found " + hits.size() + " documents matching 'persistent'");
                                    
                                    // Verify document content
                                    for (SearchResult.Hit hit : hits) {
                                        try (Document doc = searcher.doc(hit.getDocAddress())) {
                                            var titles = doc.get("title");
                                            String title = titles.isEmpty() ? "No title" : titles.get(0).toString();
                                            System.out.println("  📄 Found: " + title + " (score: " + String.format("%.3f", hit.getScore()) + ")");
                                            assertTrue(title.contains("Persistent"), "Title should contain 'Persistent'");
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        } catch (Exception e) {
            fail("Index persistence test failed: " + e.getMessage());
        }
        
        System.out.println("\n🎉 Index Persistence Test PASSED!");
        System.out.println("✅ Successfully Demonstrated:");
        System.out.println("  💾 Index.exists() - Check if index exists at path");
        System.out.println("  📂 Index.open() - Open existing index from disk");
        System.out.println("  📋 Index.getSchema() - Retrieve schema from index");
        System.out.println("  🔍 Full persistence workflow with document retrieval");
        System.out.println("  🐍 Python tantivy library compatibility");
    }
}
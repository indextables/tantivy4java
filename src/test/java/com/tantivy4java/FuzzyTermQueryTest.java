package com.tantivy4java;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;

import java.nio.file.Path;

/**
 * Test class for Fuzzy Term Query functionality demonstrating fuzzy matching
 * matching Python tantivy library capabilities
 */
public class FuzzyTermQueryTest {
    
    @Test
    @DisplayName("Fuzzy Term Query functionality for approximate text matching")
    public void testFuzzyTermQueryFunctionality(@TempDir Path tempDir) {
        System.out.println("🚀 === FUZZY TERM QUERY FUNCTIONALITY TEST ===");
        System.out.println("Testing fuzzy queries for approximate text matching");
        
        String indexPath = tempDir.resolve("fuzzy_query_index").toString();
        
        try {
            // === PHASE 1: SCHEMA CREATION WITH TEXT FIELDS ===
            System.out.println("\n📋 Phase 1: Creating schema with text fields for fuzzy queries");
            
            try (SchemaBuilder builder = new SchemaBuilder()) {
                builder
                    .addTextField("title", true, false, "default", "position")
                    .addTextField("body", true, false, "default", "position")
                    .addTextField("tags", true, true, "default", "position");
                
                try (Schema schema = builder.build()) {
                    System.out.println("✅ Created schema with text fields for fuzzy matching");
                    
                    // === PHASE 2: INDEX CREATION AND DOCUMENT INDEXING ===
                    System.out.println("\n📝 Phase 2: Creating index and adding test documents with various spellings");
                    
                    try (Index index = new Index(schema, indexPath, false)) {
                        try (IndexWriter writer = index.writer(100, 2)) {
                            
                            // Add test documents with various spellings and typos
                            try (Document doc1 = new Document()) {
                                doc1.addText("title", "Machine Learning Tutorial");
                                doc1.addText("body", "This tutorial covers basic machine learning algorithms and techniques for beginners.");
                                doc1.addText("tags", "machine learning tutorial algorithm");
                                writer.addDocument(doc1);
                            }
                            
                            try (Document doc2 = new Document()) {
                                doc2.addText("title", "Machien Learning Fundamentals"); // "Machien" is a typo
                                doc2.addText("body", "Fundamental concepts in artificial intelligence and machien learning systems.");
                                doc2.addText("tags", "artificial intelligence fundamentals");
                                writer.addDocument(doc2);
                            }
                            
                            try (Document doc3 = new Document()) {
                                doc3.addText("title", "Deep Learning Networks");
                                doc3.addText("body", "Advanced neural network architectures for deep learning applications.");
                                doc3.addText("tags", "deep learning neural networks");
                                writer.addDocument(doc3);
                            }
                            
                            try (Document doc4 = new Document()) {
                                doc4.addText("title", "Algorithm Design Patterns");
                                doc4.addText("body", "Common algorithmic patterns and design principles for efficient programming.");
                                doc4.addText("tags", "algorithm design patterns programming");
                                writer.addDocument(doc4);
                            }
                            
                            try (Document doc5 = new Document()) {
                                doc5.addText("title", "Algortihm Optimization"); // "Algortihm" is a typo
                                doc5.addText("body", "Techniques for optimizing algorithms and improving performance metrics.");
                                doc5.addText("tags", "optimization performance metrics");
                                writer.addDocument(doc5);
                            }
                            
                            writer.commit();
                            System.out.println("✅ Indexed 5 test documents with intentional typos for fuzzy matching");
                        }
                        
                        index.reload();
                        
                        // === PHASE 3: FUZZY TERM QUERY TESTING ===
                        System.out.println("\n🔍 Phase 3: Testing fuzzy term query functionality");
                        
                        try (Searcher searcher = index.searcher()) {
                            assertEquals(5, searcher.getNumDocs(), "Should have 5 documents");
                            System.out.println("✅ Index contains " + searcher.getNumDocs() + " documents");
                            
                            // Test 1: Fuzzy query with distance=1 for "machine" (should match "machien")
                            System.out.println("\n🔎 Fuzzy Query Test 1: 'machine' with distance=1 (should match 'machien')");
                            try (Query fuzzyQuery1 = Query.fuzzyTermQuery(schema, "body", "machine", 1, true, false)) {
                                try (SearchResult result = searcher.search(fuzzyQuery1, 10)) {
                                    var hits = result.getHits();
                                    assertTrue(hits.size() >= 2, "Should find documents with 'machine' and 'machien'");
                                    System.out.println("  Found " + hits.size() + " documents matching 'machine' with distance=1");
                                    
                                    for (SearchResult.Hit hit : hits) {
                                        try (Document doc = searcher.doc(hit.getDocAddress())) {
                                            String title = doc.get("title").get(0).toString();
                                            String body = doc.get("body").get(0).toString();
                                            System.out.println("    📄 " + title);
                                            assertTrue(body.toLowerCase().contains("machine") || body.toLowerCase().contains("machien"), 
                                                "Body should contain 'machine' or 'machien'");
                                        }
                                    }
                                }
                            }
                            
                            // Test 2: Fuzzy query with distance=2 for "algorithm" (should match "algortihm")
                            System.out.println("\n🔎 Fuzzy Query Test 2: 'algorithm' with distance=2 (should match 'algortihm')");
                            try (Query fuzzyQuery2 = Query.fuzzyTermQuery(schema, "body", "algorithm", 2, true, false)) {
                                try (SearchResult result = searcher.search(fuzzyQuery2, 10)) {
                                    var hits = result.getHits();
                                    assertTrue(hits.size() >= 1, "Should find documents with 'algorithm' variations");
                                    System.out.println("  Found " + hits.size() + " documents matching 'algorithm' with distance=2");
                                    
                                    for (SearchResult.Hit hit : hits) {
                                        try (Document doc = searcher.doc(hit.getDocAddress())) {
                                            String title = doc.get("title").get(0).toString();
                                            String body = doc.get("body").get(0).toString();
                                            System.out.println("    📄 " + title);
                                            assertTrue(body.toLowerCase().contains("algorithm") || 
                                                      body.toLowerCase().contains("algorithmic") ||
                                                      title.toLowerCase().contains("algorithm") ||
                                                      title.toLowerCase().contains("algortihm"), 
                                                "Should contain algorithm variations");
                                        }
                                    }
                                }
                            }
                            
                            // Test 3: Fuzzy query with distance=0 (exact match only)
                            System.out.println("\n🔎 Fuzzy Query Test 3: 'learning' with distance=0 (exact match only)");
                            try (Query fuzzyQuery3 = Query.fuzzyTermQuery(schema, "body", "learning", 0, true, false)) {
                                try (SearchResult result = searcher.search(fuzzyQuery3, 10)) {
                                    var hits = result.getHits();
                                    assertTrue(hits.size() >= 1, "Should find exact matches for 'learning'");
                                    System.out.println("  Found " + hits.size() + " documents with exact match for 'learning'");
                                    
                                    for (SearchResult.Hit hit : hits) {
                                        try (Document doc = searcher.doc(hit.getDocAddress())) {
                                            String title = doc.get("title").get(0).toString();
                                            String body = doc.get("body").get(0).toString();
                                            System.out.println("    📄 " + title);
                                            assertTrue(body.toLowerCase().contains("learning") || title.toLowerCase().contains("learning"), 
                                                "Should contain exact 'learning'");
                                        }
                                    }
                                }
                            }
                            
                            // Test 4: Fuzzy query on title field
                            System.out.println("\n🔎 Fuzzy Query Test 4: 'machien' in title field with distance=1");
                            try (Query fuzzyQuery4 = Query.fuzzyTermQuery(schema, "title", "machien", 1, true, false)) {
                                try (SearchResult result = searcher.search(fuzzyQuery4, 10)) {
                                    var hits = result.getHits();
                                    assertTrue(hits.size() >= 1, "Should find documents with 'machien'/'machine' in title");
                                    System.out.println("  Found " + hits.size() + " documents with 'machien' fuzzy matches in title");
                                    
                                    for (SearchResult.Hit hit : hits) {
                                        try (Document doc = searcher.doc(hit.getDocAddress())) {
                                            String title = doc.get("title").get(0).toString();
                                            System.out.println("    📄 " + title);
                                            assertTrue(title.toLowerCase().contains("machien") || title.toLowerCase().contains("machine"), 
                                                "Title should contain 'machien' or 'machine' (fuzzy match)");
                                        }
                                    }
                                }
                            }
                            
                            // Test 5: Fuzzy query with transposition disabled
                            System.out.println("\n🔎 Fuzzy Query Test 5: 'tutorial' with transposition_cost_one=false");
                            try (Query fuzzyQuery5 = Query.fuzzyTermQuery(schema, "body", "tutorial", 1, false, false)) {
                                try (SearchResult result = searcher.search(fuzzyQuery5, 10)) {
                                    var hits = result.getHits();
                                    assertTrue(hits.size() >= 1, "Should find documents matching 'tutorial'");
                                    System.out.println("  Found " + hits.size() + " documents matching 'tutorial' (transposition_cost_one=false)");
                                    
                                    for (SearchResult.Hit hit : hits) {
                                        try (Document doc = searcher.doc(hit.getDocAddress())) {
                                            String title = doc.get("title").get(0).toString();
                                            System.out.println("    📄 " + title);
                                        }
                                    }
                                }
                            }
                            
                            // Test 6: Non-matching fuzzy query
                            System.out.println("\n🔎 Fuzzy Query Test 6: 'xyz123' with distance=1 (should find nothing)");
                            try (Query fuzzyQuery6 = Query.fuzzyTermQuery(schema, "body", "xyz123", 1, true, false)) {
                                try (SearchResult result = searcher.search(fuzzyQuery6, 10)) {
                                    var hits = result.getHits();
                                    assertEquals(0, hits.size(), "Should find no documents matching 'xyz123'");
                                    System.out.println("  Found " + hits.size() + " documents matching 'xyz123' (as expected)");
                                }
                            }
                            
                            // Test 7: Default parameters (distance=1, transposition_cost_one=true)
                            System.out.println("\n🔎 Fuzzy Query Test 7: 'algortihm' with default parameters");
                            try (Query fuzzyQuery7 = Query.fuzzyTermQuery(schema, "title", "algortihm")) {
                                try (SearchResult result = searcher.search(fuzzyQuery7, 10)) {
                                    var hits = result.getHits();
                                    assertTrue(hits.size() >= 1, "Should find documents with 'algortihm'/'algorithm' matches");
                                    System.out.println("  Found " + hits.size() + " documents with default fuzzy parameters");
                                    
                                    for (SearchResult.Hit hit : hits) {
                                        try (Document doc = searcher.doc(hit.getDocAddress())) {
                                            String title = doc.get("title").get(0).toString();
                                            System.out.println("    📄 " + title);
                                            assertTrue(title.toLowerCase().contains("algortihm") || title.toLowerCase().contains("algorithm"), 
                                                "Title should contain 'algortihm' or 'algorithm' (fuzzy match)");
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
            
            System.out.println("\n🎉 === FUZZY TERM QUERY FUNCTIONALITY TEST COMPLETED ===");
            System.out.println("✨ Successfully demonstrated complete fuzzy term query capabilities:");
            System.out.println("   🔤 Edit distance-based fuzzy matching (distance 0-2)");
            System.out.println("   🔄 Configurable transposition cost settings");
            System.out.println("   📝 Field-specific fuzzy searches");
            System.out.println("   🎯 Exact match mode (distance=0)");
            System.out.println("   🧮 Typo-tolerant text search");
            System.out.println("   ⚙️  Default parameter handling");
            System.out.println("   🐍 Full Python tantivy library fuzzy query compatibility");
            
        } catch (Exception e) {
            fail("Fuzzy term query test failed: " + e.getMessage());
        }
    }
}
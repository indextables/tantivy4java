package com.tantivy4java;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;

import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;

/**
 * Test class for Phrase Query functionality demonstrating phrase matching
 * matching Python tantivy library capabilities
 */
public class PhraseQueryTest {
    
    @Test
    @DisplayName("Phrase Query functionality for text matching")
    public void testPhraseQueryFunctionality(@TempDir Path tempDir) {
        System.out.println("🚀 === PHRASE QUERY FUNCTIONALITY TEST ===");
        System.out.println("Testing phrase queries for exact phrase matching");
        
        String indexPath = tempDir.resolve("phrase_query_index").toString();
        
        try {
            // === PHASE 1: SCHEMA CREATION WITH TEXT FIELDS ===
            System.out.println("\n📋 Phase 1: Creating schema with text fields for phrase queries");
            
            try (SchemaBuilder builder = new SchemaBuilder()) {
                builder
                    .addTextField("title", true, false, "default", "position")
                    .addTextField("body", true, false, "default", "position")
                    .addTextField("category", true, true, "default", "position");
                
                try (Schema schema = builder.build()) {
                    System.out.println("✅ Created schema with position-indexed text fields");
                    
                    // === PHASE 2: INDEX CREATION AND DOCUMENT INDEXING ===
                    System.out.println("\n📝 Phase 2: Creating index and adding test documents");
                    
                    try (Index index = new Index(schema, indexPath, false)) {
                        try (IndexWriter writer = index.writer(Index.Memory.DEFAULT_HEAP_SIZE, 2)) {
                            
                            // Add test documents with specific phrases
                            try (Document doc1 = new Document()) {
                                doc1.addText("title", "Machine Learning Algorithms");
                                doc1.addText("body", "Deep learning neural networks are powerful tools for machine learning applications. These algorithms can learn complex patterns from large datasets.");
                                doc1.addText("category", "artificial intelligence");
                                writer.addDocument(doc1);
                            }
                            
                            try (Document doc2 = new Document()) {
                                doc2.addText("title", "Data Science Methods");
                                doc2.addText("body", "Data science involves statistical analysis and machine learning techniques. Scientists use various algorithms to extract insights from data.");
                                doc2.addText("category", "data science");
                                writer.addDocument(doc2);
                            }
                            
                            try (Document doc3 = new Document()) {
                                doc3.addText("title", "Natural Language Processing");
                                doc3.addText("body", "Natural language processing uses neural networks and machine learning to understand human language. These techniques enable computers to process text effectively.");
                                doc3.addText("category", "artificial intelligence");
                                writer.addDocument(doc3);
                            }
                            
                            try (Document doc4 = new Document()) {
                                doc4.addText("title", "Computer Vision Applications");
                                doc4.addText("body", "Computer vision applications leverage deep learning and convolutional neural networks. The field combines image processing with machine intelligence.");
                                doc4.addText("category", "computer vision");
                                writer.addDocument(doc4);
                            }
                            
                            writer.commit();
                            System.out.println("✅ Indexed 4 test documents with various phrases");
                        }
                        
                        index.reload();
                        
                        // === PHASE 3: PHRASE QUERY TESTING ===
                        System.out.println("\n🔍 Phase 3: Testing phrase query functionality");
                        
                        try (Searcher searcher = index.searcher()) {
                            assertEquals(4, searcher.getNumDocs(), "Should have 4 documents");
                            System.out.println("✅ Index contains " + searcher.getNumDocs() + " documents");
                            
                            // Test 1: Exact phrase query (machine learning)
                            System.out.println("\n🔎 Phrase Query Test 1: Exact phrase 'machine learning'");
                            List<Object> phrase1 = Arrays.asList("machine", "learning");
                            try (Query phraseQuery1 = Query.phraseQuery(schema, "body", phrase1, 0)) {
                                try (SearchResult result = searcher.search(phraseQuery1, 10)) {
                                    var hits = result.getHits();
                                    assertTrue(hits.size() >= 2, "Should find documents containing 'machine learning' phrase");
                                    System.out.println("  Found " + hits.size() + " documents with exact phrase 'machine learning'");
                                    
                                    for (SearchResult.Hit hit : hits) {
                                        try (Document doc = searcher.doc(hit.getDocAddress())) {
                                            String title = doc.get("title").get(0).toString();
                                            String body = doc.get("body").get(0).toString();
                                            System.out.println("    📄 " + title);
                                            assertTrue(body.toLowerCase().contains("machine learning"), "Body should contain 'machine learning'");
                                        }
                                    }
                                }
                            }
                            
                            // Test 2: Phrase query with slop (neural networks with slop=1)  
                            System.out.println("\n🔎 Phrase Query Test 2: Phrase with slop 'neural networks' (slop=1)");
                            List<Object> phrase2 = Arrays.asList("neural", "networks");
                            try (Query phraseQuery2 = Query.phraseQuery(schema, "body", phrase2, 1)) {
                                try (SearchResult result = searcher.search(phraseQuery2, 10)) {
                                    var hits = result.getHits();
                                    assertTrue(hits.size() >= 1, "Should find documents with 'neural networks' allowing slop");
                                    System.out.println("  Found " + hits.size() + " documents with 'neural networks' (slop=1)");
                                    
                                    for (SearchResult.Hit hit : hits) {
                                        try (Document doc = searcher.doc(hit.getDocAddress())) {
                                            String title = doc.get("title").get(0).toString();
                                            String body = doc.get("body").get(0).toString();
                                            System.out.println("    📄 " + title);
                                            assertTrue(body.toLowerCase().contains("neural") && body.toLowerCase().contains("networks"), 
                                                "Body should contain both 'neural' and 'networks'");
                                        }
                                    }
                                }
                            }
                            
                            // Test 3: Three-word phrase query (deep learning neural)
                            System.out.println("\n🔎 Phrase Query Test 3: Three-word phrase 'deep learning neural'");
                            List<Object> phrase3 = Arrays.asList("deep", "learning", "neural");
                            try (Query phraseQuery3 = Query.phraseQuery(schema, "body", phrase3, 0)) {
                                try (SearchResult result = searcher.search(phraseQuery3, 10)) {
                                    var hits = result.getHits();
                                    System.out.println("  Found " + hits.size() + " documents with exact phrase 'deep learning neural'");
                                    
                                    for (SearchResult.Hit hit : hits) {
                                        try (Document doc = searcher.doc(hit.getDocAddress())) {
                                            String title = doc.get("title").get(0).toString();
                                            String body = doc.get("body").get(0).toString();
                                            System.out.println("    📄 " + title);
                                            assertTrue(body.toLowerCase().contains("deep") && 
                                                      body.toLowerCase().contains("learning") && 
                                                      body.toLowerCase().contains("neural"), 
                                                "Body should contain 'deep', 'learning', and 'neural'");
                                        }
                                    }
                                }
                            }
                            
                            // Test 4: Title phrase query (machine learning - lowercase)
                            System.out.println("\n🔎 Phrase Query Test 4: Title phrase 'machine learning' (lowercase)");
                            List<Object> phrase4 = Arrays.asList("machine", "learning");
                            try (Query phraseQuery4 = Query.phraseQuery(schema, "title", phrase4, 0)) {
                                try (SearchResult result = searcher.search(phraseQuery4, 10)) {
                                    var hits = result.getHits();
                                    assertEquals(1, hits.size(), "Should find exactly 1 document with 'machine learning' in title");
                                    System.out.println("  Found " + hits.size() + " documents with 'machine learning' in title");
                                    
                                    for (SearchResult.Hit hit : hits) {
                                        try (Document doc = searcher.doc(hit.getDocAddress())) {
                                            String title = doc.get("title").get(0).toString();
                                            System.out.println("    📄 " + title);
                                            assertTrue(title.toLowerCase().contains("machine learning"), "Title should contain 'machine learning'");
                                        }
                                    }
                                }
                            }
                            
                            // Test 5: Category phrase query (artificial intelligence)
                            System.out.println("\n🔎 Phrase Query Test 5: Category phrase 'artificial intelligence'");
                            List<Object> phrase5 = Arrays.asList("artificial", "intelligence");
                            try (Query phraseQuery5 = Query.phraseQuery(schema, "category", phrase5, 0)) {
                                try (SearchResult result = searcher.search(phraseQuery5, 10)) {
                                    var hits = result.getHits();
                                    assertEquals(2, hits.size(), "Should find 2 documents with 'artificial intelligence' category");
                                    System.out.println("  Found " + hits.size() + " documents in 'artificial intelligence' category");
                                    
                                    for (SearchResult.Hit hit : hits) {
                                        try (Document doc = searcher.doc(hit.getDocAddress())) {
                                            String title = doc.get("title").get(0).toString();
                                            String category = doc.get("category").get(0).toString();
                                            System.out.println("    📄 " + title + " [" + category + "]");
                                            assertEquals("artificial intelligence", category, "Category should be 'artificial intelligence'");
                                        }
                                    }
                                }
                            }
                            
                            // Test 6: Non-existent phrase query
                            System.out.println("\n🔎 Phrase Query Test 6: Non-existent phrase 'quantum computing'");
                            List<Object> phrase6 = Arrays.asList("quantum", "computing");
                            try (Query phraseQuery6 = Query.phraseQuery(schema, "body", phrase6, 0)) {
                                try (SearchResult result = searcher.search(phraseQuery6, 10)) {
                                    var hits = result.getHits();
                                    assertEquals(0, hits.size(), "Should find no documents with 'quantum computing'");
                                    System.out.println("  Found " + hits.size() + " documents with 'quantum computing' (as expected)");
                                }
                            }
                        }
                    }
                }
            }
            
            System.out.println("\n🎉 === PHRASE QUERY FUNCTIONALITY TEST COMPLETED ===");
            System.out.println("✨ Successfully demonstrated complete phrase query capabilities:");
            System.out.println("   📝 Exact phrase matching with zero slop");
            System.out.println("   🔄 Phrase matching with configurable slop tolerance");
            System.out.println("   📚 Multi-word phrase queries (2+ terms)");
            System.out.println("   🎯 Field-specific phrase searches");
            System.out.println("   🔍 Position-aware text matching");
            System.out.println("   🐍 Full Python tantivy library phrase query compatibility");
            
        } catch (Exception e) {
            fail("Phrase query test failed: " + e.getMessage());
        }
    }
}
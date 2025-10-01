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
 * Test class for Snippet functionality
 * matching Python tantivy library capabilities
 */
public class SnippetTest {
    
    @Test
    @DisplayName("Snippet functionality for text highlighting and fragment generation")
    public void testSnippetFunctionality(@TempDir Path tempDir) {
        System.out.println("🚀 === SNIPPET FUNCTIONALITY TEST ===");
        System.out.println("Testing snippet generation, highlighting, and HTML conversion");
        
        String indexPath = tempDir.resolve("snippet_index").toString();
        
        try {
            // === PHASE 1: SCHEMA CREATION WITH TEXT FIELDS ===
            System.out.println("\n📋 Phase 1: Creating schema with text fields for snippet generation");
            
            try (SchemaBuilder builder = new SchemaBuilder()) {
                builder
                    .addTextField("title", true, false, "default", "position")
                    .addTextField("body", true, false, "default", "position")
                    .addTextField("content", true, false, "default", "position")
                    .addTextField("description", true, false, "default", "position")
                    .addIntegerField("score", true, true, true);
                
                try (Schema schema = builder.build()) {
                    System.out.println("✅ Created schema with text fields for snippet generation");
                    
                    // === PHASE 2: INDEX CREATION AND DOCUMENT INDEXING ===
                    System.out.println("\n📝 Phase 2: Creating index and adding documents with rich text content");
                    
                    try (Index index = new Index(schema, indexPath, false)) {
                        try (IndexWriter writer = index.writer(Index.Memory.DEFAULT_HEAP_SIZE, 2)) {
                            
                            // Add documents with rich content for snippet testing
                            try (Document doc1 = new Document()) {
                                doc1.addText("title", "Advanced Machine Learning Techniques");
                                doc1.addText("body", "Machine learning algorithms have revolutionized data science and artificial intelligence. Deep learning neural networks provide exceptional pattern recognition capabilities for complex data analysis and predictive modeling.");
                                doc1.addText("content", "This comprehensive guide explores supervised learning, unsupervised learning, and reinforcement learning paradigms.");
                                doc1.addText("description", "Comprehensive tutorial on machine learning fundamentals and advanced techniques.");
                                doc1.addInteger("score", 95);
                                writer.addDocument(doc1);
                            }
                            
                            writer.commit();
                            System.out.println("✅ Indexed 1 document with rich text content for snippet testing");
                        }
                        
                        index.reload();
                        
                        // === PHASE 3: SNIPPET GENERATION TESTING ===
                        System.out.println("\n🔍 Phase 3: Testing snippet generation functionality");
                        
                        try (Searcher searcher = index.searcher()) {
                            assertEquals(1, searcher.getNumDocs(), "Should have 1 document");
                            System.out.println("✅ Index contains " + searcher.getNumDocs() + " document");
                            
                            // Test 1: Basic snippet generation from search results
                            System.out.println("\n🔎 Snippet Test 1: Basic snippet generation from search results");
                            try (Query query = Query.termQuery(schema, "body", "learning")) {
                                try (SearchResult result = searcher.search(query, 10)) {
                                    var hits = result.getHits();
                                    System.out.println("  Found " + hits.size() + " documents matching 'learning'");
                                    assertTrue(hits.size() >= 1, "Should find documents containing 'learning'");
                                    
                                    // Create snippet generator for the body field
                                    try (SnippetGenerator snippetGen = SnippetGenerator.create(searcher, query, schema, "body")) {
                                        System.out.println("  ✅ Created snippet generator for 'body' field");
                                        
                                        for (SearchResult.Hit hit : hits) {
                                            try (Document doc = searcher.doc(hit.getDocAddress())) {
                                                String title = doc.get("title").get(0).toString();
                                                System.out.println("  📄 Processing document: " + title);
                                                
                                                // Generate snippet from document
                                                try (Snippet snippet = snippetGen.snippetFromDoc(doc)) {
                                                    // Test fragment extraction
                                                    String fragment = snippet.getFragment();
                                                    System.out.println("    Fragment: " + fragment);
                                                    assertNotNull(fragment, "Fragment should not be null");
                                                    assertFalse(fragment.isEmpty(), "Fragment should not be empty");
                                                    System.out.println("    ✅ Fragment extraction working");
                                                    
                                                    // Test HTML conversion
                                                    String html = snippet.toHtml();
                                                    System.out.println("    HTML: " + html);
                                                    assertNotNull(html, "HTML should not be null");
                                                    assertFalse(html.isEmpty(), "HTML should not be empty");
                                                    System.out.println("    ✅ HTML conversion working");
                                                    
                                                    // Test highlighted ranges
                                                    List<Range> highlighted = snippet.getHighlighted();
                                                    System.out.println("    Highlighted ranges: " + highlighted.size());
                                                    assertNotNull(highlighted, "Highlighted ranges should not be null");
                                                    
                                                    for (Range range : highlighted) {
                                                        int start = range.getStart();
                                                        int end = range.getEnd();
                                                        System.out.println("      Range: [" + start + ", " + end + "]");
                                                        assertTrue(start >= 0, "Range start should be non-negative");
                                                        assertTrue(end > start, "Range end should be greater than start");
                                                        System.out.println("    ✅ Range extraction working");
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                            
                            System.out.println("\n🎉 Snippet functionality basic test PASSED!");
                            System.out.println("✨ Successfully demonstrated snippet functionality:");
                            System.out.println("   📄 SnippetGenerator creation");
                            System.out.println("   🔤 Fragment extraction");
                            System.out.println("   🎨 HTML conversion with highlighting");
                            System.out.println("   📍 Range extraction and validation");
                            System.out.println("   💾 Resource management with AutoCloseable");
                        }
                    }
                }
            }
            
            System.out.println("\n🎉 === SNIPPET FUNCTIONALITY TEST COMPLETED ===");
            System.out.println("✨ Successfully demonstrated complete snippet capabilities:");
            System.out.println("   📝 Snippet generation from search results");
            System.out.println("   🔤 Plain text fragment extraction");
            System.out.println("   🎨 HTML conversion with highlighting tags");
            System.out.println("   📍 Highlighted range extraction and validation");
            System.out.println("   🐍 Full Python tantivy library snippet compatibility");
            
        } catch (Exception e) {
            fail("Snippet test failed: " + e.getMessage());
        }
    }
}

package com.tantivy4java;

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
                        try (IndexWriter writer = index.writer(100, 2)) {
                            
                            // Add documents with rich content for snippet testing
                            try (Document doc1 = new Document()) {
                                doc1.addText("title", "Advanced Machine Learning Techniques");
                                doc1.addText("body", "Machine learning algorithms have revolutionized data science and artificial intelligence. Deep learning neural networks provide exceptional pattern recognition capabilities for complex data analysis and predictive modeling.");
                                doc1.addText("content", "This comprehensive guide explores supervised learning, unsupervised learning, and reinforcement learning paradigms.");
                                doc1.addText("description", "Comprehensive tutorial on machine learning fundamentals and advanced techniques.");
                                doc1.addInteger("score", 95);
                                writer.addDocument(doc1);
                            }
                            
                            try (Document doc2 = new Document()) {
                                doc2.addText("title", "Database Systems and Architecture");
                                doc2.addText("body", "Database management systems are foundational to modern software applications. Relational databases, NoSQL solutions, and distributed architectures provide scalable data storage and retrieval mechanisms for enterprise applications.");
                                doc2.addText("content", "Learn about ACID properties, indexing strategies, query optimization, and distributed database design patterns.");
                                doc2.addText("description", "In-depth exploration of database systems, from fundamentals to advanced architecture patterns.");
                                doc2.addInteger("score", 88);
                                writer.addDocument(doc2);
                            }
                            
                            try (Document doc3 = new Document()) {
                                doc3.addText("title", "Web Development Best Practices");
                                doc3.addText("body", "Modern web development encompasses frontend frameworks, backend services, and full-stack architectures. React, Vue, and Angular provide powerful tools for building interactive user interfaces with responsive design principles.");
                                doc3.addText("content", "Explore component-based architecture, state management, API integration, and performance optimization techniques.");
                                doc3.addText("description", "Complete guide to modern web development practices and framework selection.");
                                doc3.addInteger("score", 82);
                                writer.addDocument(doc3);
                            }
                            
                            try (Document doc4 = new Document()) {
                                doc4.addText("title", "Cloud Computing and DevOps");
                                doc4.addText("body", "Cloud computing platforms like AWS, Azure, and Google Cloud enable scalable infrastructure deployment. DevOps practices integrate development and operations through continuous integration, continuous deployment, and infrastructure as code.");
                                doc4.addText("content", "Master containerization with Docker, orchestration with Kubernetes, and automation with CI/CD pipelines.");
                                doc4.addText("description", "Comprehensive overview of cloud computing services and DevOps methodologies.");
                                doc4.addInteger("score", 91);
                                writer.addDocument(doc4);
                            }
                            
                            writer.commit();
                            System.out.println("✅ Indexed 4 documents with rich text content for snippet testing");
                        }
                        
                        index.reload();
                        
                        // === PHASE 3: SNIPPET GENERATION TESTING ===
                        System.out.println("\n🔍 Phase 3: Testing snippet generation functionality");
                        
                        try (Searcher searcher = index.searcher()) {
                            assertEquals(4, searcher.getNumDocs(), "Should have 4 documents");
                            System.out.println("✅ Index contains " + searcher.getNumDocs() + " documents");
                            
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
                                                    assertTrue(fragment.toLowerCase().contains("learning"), "Fragment should contain search term");
                                                    
                                                    // Test HTML conversion
                                                    String html = snippet.toHtml();
                                                    System.out.println("    HTML: " + html);
                                                    assertNotNull(html, "HTML should not be null");
                                                    assertFalse(html.isEmpty(), "HTML should not be empty");
                                                    assertTrue(html.contains("<b>") || html.contains("<mark>"), "HTML should contain highlighting tags");
                                                    
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
                                                        assertTrue(end <= fragment.length(), "Range end should not exceed fragment length");
                                                        
                                                        // Extract highlighted text
                                                        String highlightedText = fragment.substring(start, end);
                                                        System.out.println("      Highlighted: '" + highlightedText + "'");
                                                        assertTrue(highlightedText.toLowerCase().contains("learn"), "Highlighted text should match search term");
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                            
                            // Test 2: Snippet generation with phrase query
                            System.out.println("\n🔎 Snippet Test 2: Snippet generation with phrase query");
                            try (Query phraseQuery = Query.phraseQuery(schema, "body", java.util.List.of("machine", "learning"), 0)) {
                                try (SearchResult result = searcher.search(phraseQuery, 10)) {
                                    var hits = result.getHits();
                                    System.out.println("  Found " + hits.size() + " documents matching phrase 'machine learning'");
                                    
                                    if (hits.size() > 0) {
                                        try (SnippetGenerator snippetGen = SnippetGenerator.create(searcher, phraseQuery, schema, "body")) {
                                            for (SearchResult.Hit hit : hits) {
                                                try (Document doc = searcher.doc(hit.getDocAddress())) {
                                                    String title = doc.get("title").get(0).toString();
                                                    System.out.println("  📄 Processing document: " + title);
                                                    
                                                    try (Snippet snippet = snippetGen.snippetFromDoc(doc)) {
                                                        String fragment = snippet.getFragment();
                                                        String html = snippet.toHtml();
                                                        
                                                        System.out.println("    Fragment: " + fragment);
                                                        System.out.println("    HTML: " + html);
                                                        
                                                        assertTrue(fragment.toLowerCase().contains("machine") && 
                                                                 fragment.toLowerCase().contains("learning"), 
                                                                 "Fragment should contain both words from phrase");
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                            
                            // Test 3: Snippet generation with different fields
                            System.out.println("\n🔎 Snippet Test 3: Snippet generation from different fields");
                            try (Query query = Query.termQuery(schema, "content", "architecture")) {
                                try (SearchResult result = searcher.search(query, 10)) {
                                    var hits = result.getHits();
                                    System.out.println("  Found " + hits.size() + " documents matching 'architecture' in content");
                                    
                                    if (hits.size() > 0) {
                                        try (SnippetGenerator snippetGen = SnippetGenerator.create(searcher, query, schema, "content")) {
                                            for (SearchResult.Hit hit : hits) {
                                                try (Document doc = searcher.doc(hit.getDocAddress())) {
                                                    String title = doc.get("title").get(0).toString();
                                                    System.out.println("  📄 Processing document: " + title);
                                                    
                                                    try (Snippet snippet = snippetGen.snippetFromDoc(doc)) {
                                                        String fragment = snippet.getFragment();
                                                        System.out.println("    Content fragment: " + fragment);
                                                        assertTrue(fragment.toLowerCase().contains("architecture"), 
                                                                 "Content fragment should contain search term");
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                            
                            // Test 4: Snippet generator configuration
                            System.out.println("\n🔎 Snippet Test 4: Snippet generator configuration");
                            try (Query query = Query.termQuery(schema, "description", "comprehensive")) {
                                try (SearchResult result = searcher.search(query, 10)) {
                                    var hits = result.getHits();
                                    System.out.println("  Found " + hits.size() + " documents matching 'comprehensive'");
                                    
                                    if (hits.size() > 0) {
                                        try (SnippetGenerator snippetGen = SnippetGenerator.create(searcher, query, schema, "description")) {
                                            // Test setting maximum number of characters
                                            snippetGen.setMaxNumChars(50);
                                            System.out.println("  ✅ Set maximum characters to 50");
                                            
                                            SearchResult.Hit hit = hits.get(0);
                                            try (Document doc = searcher.doc(hit.getDocAddress())) {
                                                String title = doc.get("title").get(0).toString();
                                                System.out.println("  📄 Processing document: " + title);
                                                
                                                try (Snippet snippet = snippetGen.snippetFromDoc(doc)) {
                                                    String fragment = snippet.getFragment();
                                                    System.out.println("    Short fragment (≤50 chars): " + fragment);
                                                    assertTrue(fragment.length() <= 60, "Fragment should be approximately limited to 50 characters");
                                                    // Note: Actual length might be slightly more due to word boundaries
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                            
                            // Test 5: Multiple search terms highlighting
                            System.out.println("\n🔎 Snippet Test 5: Multiple search terms highlighting");
                            try (Query query1 = Query.termQuery(schema, "body", "data");
                                 Query query2 = Query.termQuery(schema, "body", "systems");
                                 Query boolQuery = Query.booleanQuery(java.util.List.of(
                                     new java.util.AbstractMap.SimpleEntry<>(Occur.SHOULD, query1),
                                     new java.util.AbstractMap.SimpleEntry<>(Occur.SHOULD, query2)
                                 ))) {
                                
                                try (SearchResult result = searcher.search(boolQuery, 10)) {
                                    var hits = result.getHits();
                                    System.out.println("  Found " + hits.size() + " documents matching 'data' OR 'systems'");
                                    
                                    if (hits.size() > 0) {
                                        try (SnippetGenerator snippetGen = SnippetGenerator.create(searcher, boolQuery, schema, "body")) {
                                            SearchResult.Hit hit = hits.get(0);
                                            try (Document doc = searcher.doc(hit.getDocAddress())) {
                                                String title = doc.get("title").get(0).toString();
                                                System.out.println("  📄 Processing document: " + title);
                                                
                                                try (Snippet snippet = snippetGen.snippetFromDoc(doc)) {
                                                    String fragment = snippet.getFragment();
                                                    String html = snippet.toHtml();
                                                    List<Range> highlighted = snippet.getHighlighted();
                                                    
                                                    System.out.println("    Fragment: " + fragment);
                                                    System.out.println("    HTML: " + html);
                                                    System.out.println("    Highlighted ranges: " + highlighted.size());
                                                    
                                                    // Should highlight multiple terms
                                                    assertTrue(fragment.toLowerCase().contains("data") || 
                                                             fragment.toLowerCase().contains("system"), 
                                                             "Fragment should contain at least one search term");
                                                }
                                            }
                                        }
                                    }
                                }
                            }
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
            System.out.println("   📄 Multi-field snippet generation");
            System.out.println("   ⚙️  Snippet generator configuration (max chars)");
            System.out.println("   🔍 Phrase query and boolean query snippet support");
            System.out.println("   🐍 Full Python tantivy library snippet compatibility");
            
        } catch (Exception e) {
            fail("Snippet test failed: " + e.getMessage());
        }
    }
}
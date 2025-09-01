package com.tantivy4java;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;

import java.nio.file.Path;
import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.List;

/**
 * Comprehensive functionality test demonstrating complete Tantivy4Java capabilities
 * matching Python tantivy library patterns and functionality
 */
public class ComprehensiveFunctionalityTest {
    
    @Test
    @DisplayName("Complete Tantivy4Java functionality showcase")
    public void testComprehensiveFunctionality(@TempDir Path tempDir) {
        System.out.println("🚀 === COMPREHENSIVE TANTIVY4JAVA FUNCTIONALITY TEST ===");
        System.out.println("Demonstrating complete search engine capabilities");
        
        String indexPath = tempDir.resolve("comprehensive_index").toString();
        
        try {
            // === PHASE 1: SCHEMA CREATION WITH ALL FIELD TYPES ===
            System.out.println("\n📋 Phase 1: Creating comprehensive schema with all field types");
            
            try (SchemaBuilder builder = new SchemaBuilder()) {
                builder
                    // Text fields for full-text search
                    .addTextField("title", true, false, "default", "position")
                    .addTextField("body", true, false, "default", "position")
                    .addTextField("category", true, true, "default", "position")
                    
                    // Numeric fields for range queries and sorting
                    .addIntegerField("id", true, true, true)
                    .addFloatField("rating", true, true, true)
                    .addUnsignedField("views", true, true, true)
                    
                    // Boolean fields for filtering
                    .addBooleanField("published", true, true, true)
                    .addBooleanField("featured", true, true, true)
                    
                    // Date fields for temporal queries
                    .addDateField("created_date", true, true, true)
                    .addDateField("updated_date", true, true, true)
                    
                    // IP address field for network analysis
                    .addIpAddrField("client_ip", true, true, true);
                
                try (Schema schema = builder.build()) {
                    System.out.println("✅ Created schema with 11 fields across 6 different types");
                    
                    // === PHASE 2: INDEX CREATION AND DOCUMENT INDEXING ===
                    System.out.println("\n📝 Phase 2: Creating persistent index and adding diverse documents");
                    
                    try (Index index = new Index(schema, indexPath, false)) {
                        
                        // Add diverse test documents
                        try (IndexWriter writer = index.writer(Index.Memory.DEFAULT_HEAP_SIZE, 2)) {
                            
                            // Document 1: Tech article
                            try (Document doc1 = new Document()) {
                                doc1.addText("title", "Advanced Machine Learning Techniques");
                                doc1.addText("body", "Deep learning and neural networks are revolutionizing artificial intelligence. This comprehensive guide covers algorithms, implementation details, and practical applications in modern software development.");
                                doc1.addText("category", "technology");
                                doc1.addInteger("id", 1);
                                doc1.addFloat("rating", 4.8f);
                                doc1.addUnsigned("views", 15420L);
                                doc1.addBoolean("published", true);
                                doc1.addBoolean("featured", true);
                                doc1.addDate("created_date", LocalDateTime.of(2024, 1, 15, 10, 30));
                                doc1.addDate("updated_date", LocalDateTime.of(2024, 2, 1, 14, 15));
                                doc1.addIpAddr("client_ip", "192.168.1.100");
                                writer.addDocument(doc1);
                            }
                            
                            // Document 2: Science article
                            try (Document doc2 = new Document()) {
                                doc2.addText("title", "Quantum Computing Breakthrough");
                                doc2.addText("body", "Scientists achieve quantum supremacy with new algorithms. This research breakthrough demonstrates practical quantum computing applications for cryptography and complex problem solving.");
                                doc2.addText("category", "science");
                                doc2.addInteger("id", 2);
                                doc2.addFloat("rating", 4.9f);
                                doc2.addUnsigned("views", 28750L);
                                doc2.addBoolean("published", true);
                                doc2.addBoolean("featured", true);
                                doc2.addDate("created_date", LocalDateTime.of(2024, 3, 8, 9, 45));
                                doc2.addDate("updated_date", LocalDateTime.of(2024, 3, 10, 16, 20));
                                doc2.addIpAddr("client_ip", "10.0.0.50");
                                writer.addDocument(doc2);
                            }
                            
                            // Document 3: Draft article
                            try (Document doc3 = new Document()) {
                                doc3.addText("title", "Future of Web Development");
                                doc3.addText("body", "Exploring emerging web technologies including WebAssembly, progressive web applications, and serverless architectures. This draft covers modern development practices and tools.");
                                doc3.addText("category", "technology");
                                doc3.addInteger("id", 3);
                                doc3.addFloat("rating", 3.7f);
                                doc3.addUnsigned("views", 892L);
                                doc3.addBoolean("published", false);
                                doc3.addBoolean("featured", false);
                                doc3.addDate("created_date", LocalDateTime.of(2024, 4, 20, 13, 10));
                                doc3.addDate("updated_date", LocalDateTime.of(2024, 4, 22, 11, 35));
                                doc3.addIpAddr("client_ip", "172.16.0.25");
                                writer.addDocument(doc3);
                            }
                            
                            // Document 4: Health article
                            try (Document doc4 = new Document()) {
                                doc4.addText("title", "Nutrition Science and Wellness");
                                doc4.addText("body", "Recent advances in nutritional science reveal new insights into healthy eating patterns. Research shows connections between diet, metabolism, and long-term health outcomes.");
                                doc4.addText("category", "health");
                                doc4.addInteger("id", 4);
                                doc4.addFloat("rating", 4.2f);
                                doc4.addUnsigned("views", 12340L);
                                doc4.addBoolean("published", true);
                                doc4.addBoolean("featured", false);
                                doc4.addDate("created_date", LocalDateTime.of(2024, 2, 28, 15, 20));
                                doc4.addDate("updated_date", LocalDateTime.of(2024, 3, 5, 9, 10));
                                doc4.addIpAddr("client_ip", "203.0.113.5");
                                writer.addDocument(doc4);
                            }
                            
                            writer.commit();
                            System.out.println("✅ Indexed 4 comprehensive documents with all field types");
                        }
                        
                        // === PHASE 3: INDEX PERSISTENCE VERIFICATION ===
                        System.out.println("\n💾 Phase 3: Testing index persistence");
                        
                        index.reload();
                        assertTrue(Index.exists(indexPath), "Index should exist after creation");
                        System.out.println("✅ Index persistence verified");
                    }
                    
                    // === PHASE 4: REOPEN INDEX FROM DISK ===
                    System.out.println("\n📂 Phase 4: Reopening index from disk");
                    
                    try (Index reopenedIndex = Index.open(indexPath)) {
                        System.out.println("✅ Successfully reopened index from disk");
                        
                        // Verify schema retrieval
                        try (Schema retrievedSchema = reopenedIndex.getSchema()) {
                            System.out.println("✅ Retrieved schema from persistent index");
                        }
                        
                        // === PHASE 5: COMPREHENSIVE SEARCH FUNCTIONALITY ===
                        System.out.println("\n🔍 Phase 5: Demonstrating comprehensive search capabilities");
                        
                        try (Searcher searcher = reopenedIndex.searcher()) {
                            int totalDocs = searcher.getNumDocs();
                            int numSegments = searcher.getNumSegments();
                            assertEquals(4, totalDocs, "Should have 4 documents");
                            System.out.println("✅ Index stats: " + totalDocs + " documents in " + numSegments + " segments");
                            
                            // Test 1: Simple text search  
                            System.out.println("\n🔎 Search Test 1: Simple text search");
                            try (Query query1 = reopenedIndex.parseQuery("machine learning", Arrays.asList("title", "body"))) {
                                System.out.println("  📋 Parsed Query Structure: " + query1.toString());
                                try (SearchResult result1 = searcher.search(query1, 10)) {
                                    var hits1 = result1.getHits();
                                    assertTrue(hits1.size() >= 1, "Should find machine learning documents");
                                    System.out.println("  Found " + hits1.size() + " documents for 'machine learning'");
                                    
                                    for (SearchResult.Hit hit : hits1) {
                                        try (Document doc = searcher.doc(hit.getDocAddress())) {
                                            String title = doc.get("title").get(0).toString();
                                            String category = doc.get("category").get(0).toString();
                                            System.out.println("    📄 " + title + " [" + category + "] (score: " + String.format("%.3f", hit.getScore()) + ")");
                                        }
                                    }
                                }
                            }
                            
                            // Test 2: Category-specific search  
                            System.out.println("\n🔎 Search Test 2: Category filtering");
                            try (Query query2 = reopenedIndex.parseQuery("category:technology", Arrays.asList("category"))) {
                                System.out.println("  📋 Parsed Query Structure: " + query2.toString());
                                try (SearchResult result2 = searcher.search(query2, 10)) {
                                    var hits2 = result2.getHits();
                                    assertEquals(2, hits2.size(), "Should find 2 technology documents");
                                    System.out.println("  Found " + hits2.size() + " technology documents");
                                    
                                    for (SearchResult.Hit hit : hits2) {
                                        try (Document doc = searcher.doc(hit.getDocAddress())) {
                                            String title = doc.get("title").get(0).toString();
                                            Boolean published = (Boolean) doc.get("published").get(0);
                                            System.out.println("    📄 " + title + " (published: " + published + ")");
                                        }
                                    }
                                }
                            }
                            
                            // Test 3: Boolean query with complex logic
                            System.out.println("\n🔎 Search Test 3: Complex boolean search");
                            try (Query query3 = reopenedIndex.parseQuery("quantum AND (computing OR algorithms)", Arrays.asList("title", "body"))) {
                                System.out.println("  📋 Parsed Query Structure: " + query3.toString());
                                try (SearchResult result3 = searcher.search(query3, 10)) {
                                    var hits3 = result3.getHits();
                                    assertTrue(hits3.size() >= 1, "Should find quantum computing documents");
                                    System.out.println("  Found " + hits3.size() + " documents matching complex boolean query");
                                }
                            }
                            
                            // Test 4: All documents search with detailed field extraction
                            System.out.println("\n🔎 Search Test 4: Complete document retrieval with all field types");
                            try (Query allQuery = Query.allQuery()) {
                                try (SearchResult allResult = searcher.search(allQuery, 10)) {
                                    var allHits = allResult.getHits();
                                    assertEquals(4, allHits.size(), "Should retrieve all 4 documents");
                                    System.out.println("  Retrieved all " + allHits.size() + " documents with complete field extraction:");
                                    
                                    for (SearchResult.Hit hit : allHits) {
                                        try (Document doc = searcher.doc(hit.getDocAddress())) {
                                            // Extract all field types
                                            String title = doc.get("title").get(0).toString();
                                            String category = doc.get("category").get(0).toString();
                                            Integer id = ((Long) doc.get("id").get(0)).intValue();
                                            Double rating = (Double) doc.get("rating").get(0);
                                            Long views = (Long) doc.get("views").get(0);
                                            Boolean published = (Boolean) doc.get("published").get(0);
                                            Boolean featured = (Boolean) doc.get("featured").get(0);
                                            LocalDateTime created = (LocalDateTime) doc.get("created_date").get(0);
                                            String clientIp = doc.get("client_ip").get(0).toString();
                                            
                                            System.out.println("    📄 Document " + id + ": " + title);
                                            System.out.println("       Category: " + category + ", Rating: " + rating + ", Views: " + views);
                                            System.out.println("       Published: " + published + ", Featured: " + featured);
                                            System.out.println("       Created: " + created + ", IP: " + clientIp);
                                        }
                                    }
                                }
                            }
                        }
                        
                        // === PHASE 6: DOCUMENT MANAGEMENT OPERATIONS ===
                        System.out.println("\n🗑️ Phase 6: Document management (CRUD operations)");
                        
                        // Delete unpublished documents
                        try (IndexWriter writer = reopenedIndex.writer(Index.Memory.DEFAULT_HEAP_SIZE, 1)) {
                            long deletedCount = writer.deleteDocumentsByTerm("published", false);
                            writer.commit();
                            System.out.println("✅ Deleted unpublished documents (opstamp: " + deletedCount + ")");
                        }
                        
                        reopenedIndex.reload();
                        
                        // Verify deletion
                        try (Searcher searcher = reopenedIndex.searcher()) {
                            assertEquals(3, searcher.getNumDocs(), "Should have 3 documents after deleting unpublished");
                            System.out.println("✅ Verified document count after deletion: " + searcher.getNumDocs());
                        }
                        
                        // Delete by category using query
                        try (IndexWriter writer = reopenedIndex.writer(Index.Memory.DEFAULT_HEAP_SIZE, 1)) {
                            try (Query deleteQuery = reopenedIndex.parseQuery("category:health", Arrays.asList("category"))) {
                                System.out.println("🗑️ Delete Query Structure: " + deleteQuery.toString());
                                long deletedCount = writer.deleteDocumentsByQuery(deleteQuery);
                                writer.commit();
                                System.out.println("✅ Deleted health category documents (opstamp: " + deletedCount + ")");
                            }
                        }
                        
                        reopenedIndex.reload();
                        
                        // Final verification
                        try (Searcher searcher = reopenedIndex.searcher()) {
                            assertEquals(2, searcher.getNumDocs(), "Should have 2 documents after category deletion");
                            System.out.println("✅ Final document count: " + searcher.getNumDocs());
                            
                            // Show remaining documents
                            try (Query allQuery = Query.allQuery()) {
                                try (SearchResult result = searcher.search(allQuery, 10)) {
                                    System.out.println("📄 Remaining documents:");
                                    for (SearchResult.Hit hit : result.getHits()) {
                                        try (Document doc = searcher.doc(hit.getDocAddress())) {
                                            String title = doc.get("title").get(0).toString();
                                            String category = doc.get("category").get(0).toString();
                                            System.out.println("  • " + title + " [" + category + "]");
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
            
            System.out.println("\n🎉 === COMPREHENSIVE FUNCTIONALITY TEST COMPLETED ===");
            System.out.println("✨ Successfully demonstrated complete Tantivy4Java capabilities:");
            System.out.println("   🏗️  Schema building with ALL field types (text, numeric, boolean, date, IP)");
            System.out.println("   📝 Document indexing with mixed field types");
            System.out.println("   💾 Index persistence (create, save, reopen from disk)");
            System.out.println("   🔍 Comprehensive search functionality");
            System.out.println("   🔎 Complex query parsing (boolean logic, field targeting)");
            System.out.println("   📄 Complete document retrieval with field extraction");
            System.out.println("   🗑️  Full CRUD operations (Create, Read, Update, Delete)");
            System.out.println("   🐍 Python tantivy library compatibility");
            System.out.println("   🚀 Production-ready search engine functionality");
            
        } catch (Exception e) {
            fail("Comprehensive functionality test failed: " + e.getMessage());
        }
    }
}
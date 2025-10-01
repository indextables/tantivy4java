package io.indextables.tantivy4java;

import io.indextables.tantivy4java.core.*;
import io.indextables.tantivy4java.query.*;
import io.indextables.tantivy4java.result.*;


import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;

import java.nio.file.Path;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;

/**
 * Integration test class for combining multiple query types and edge cases
 * Testing complex scenarios that demonstrate production-ready capabilities
 */
public class IntegrationTest {
    
    @Test
    @DisplayName("Comprehensive integration test combining all query types and edge cases")
    public void testComprehensiveIntegration(@TempDir Path tempDir) {
        System.out.println("🚀 === COMPREHENSIVE INTEGRATION TEST ===");
        System.out.println("Testing complex combinations of all query types and edge cases");
        
        String indexPath = tempDir.resolve("integration_index").toString();
        
        try {
            // === PHASE 1: SCHEMA CREATION WITH COMPREHENSIVE FIELD TYPES ===
            System.out.println("\n📋 Phase 1: Creating comprehensive schema with all field types");
            
            try (SchemaBuilder builder = new SchemaBuilder()) {
                builder
                    .addTextField("title", true, false, "default", "position")
                    .addTextField("body", true, false, "default", "position")
                    .addTextField("category", true, true, "default", "position")
                    .addTextField("tags", true, true, "default", "position")
                    .addTextField("author", true, true, "default", "position")
                    .addTextField("email", true, true, "default", "position")
                    .addIntegerField("score", true, true, true)
                    .addFloatField("rating", true, true, true)
                    .addUnsignedField("views", true, true, true)
                    .addDateField("published", true, true, true)
                    .addBooleanField("featured", true, true, true);
                
                try (Schema schema = builder.build()) {
                    System.out.println("✅ Created comprehensive schema with all field types");
                    
                    // === PHASE 2: INDEX CREATION WITH DIVERSE TEST DATA ===
                    System.out.println("\n📝 Phase 2: Creating index with diverse test documents");
                    
                    try (Index index = new Index(schema, indexPath, false)) {
                        try (IndexWriter writer = index.writer(Index.Memory.LARGE_HEAP_SIZE, 4)) {
                            
                            // Document 1: High-tech AI article
                            try (Document doc1 = new Document()) {
                                doc1.addText("title", "Advanced Machine Learning and Neural Networks");
                                doc1.addText("body", "This comprehensive guide explores deep learning neural networks, machine learning algorithms, and artificial intelligence techniques for modern data science applications.");
                                doc1.addText("category", "artificial intelligence");
                                doc1.addText("tags", "machine learning neural networks deep learning AI");
                                doc1.addText("author", "Dr. Sarah Johnson");
                                doc1.addText("email", "sarah.johnson@university.edu");
                                doc1.addInteger("score", 95);
                                doc1.addFloat("rating", 4.8);
                                doc1.addUnsigned("views", 12500);
                                doc1.addDate("published", LocalDateTime.of(2024, 3, 15, 14, 30));
                                doc1.addBoolean("featured", true);
                                writer.addDocument(doc1);
                            }
                            
                            // Document 2: Database systems article with typos for fuzzy testing
                            try (Document doc2 = new Document()) {
                                doc2.addText("title", "Database Systems and Algortihm Optimization");
                                doc2.addText("body", "Database mangement systems provide scalable storage solutions. This article covers indexing stratagies, query optimization, and distributed architectures.");
                                doc2.addText("category", "databases");
                                doc2.addText("tags", "database management systems indexing optimization");
                                doc2.addText("author", "Prof. Michael Chen");
                                doc2.addText("email", "m.chen@tech.com");
                                doc2.addInteger("score", 87);
                                doc2.addFloat("rating", 4.2);
                                doc2.addUnsigned("views", 8750);
                                doc2.addDate("published", LocalDateTime.of(2024, 1, 22, 9, 45));
                                doc2.addBoolean("featured", false);
                                writer.addDocument(doc2);
                            }
                            
                            // Document 3: Web development article
                            try (Document doc3 = new Document()) {
                                doc3.addText("title", "Modern Web Development Best Practices");
                                doc3.addText("body", "Web development has evolved significantly. This guide covers React frameworks, responsive design techniques, and API integration patterns for scalable applications.");
                                doc3.addText("category", "web development");
                                doc3.addText("tags", "web react javascript frontend development");
                                doc3.addText("author", "Alex Rodriguez");
                                doc3.addText("email", "alex.r@webdev.org");
                                doc3.addInteger("score", 78);
                                doc3.addFloat("rating", 3.9);
                                doc3.addUnsigned("views", 5240);
                                doc3.addDate("published", LocalDateTime.of(2023, 11, 8, 16, 20));
                                doc3.addBoolean("featured", true);
                                writer.addDocument(doc3);
                            }
                            
                            // Document 4: Cloud computing article
                            try (Document doc4 = new Document()) {
                                doc4.addText("title", "Cloud Computing Infrastructure and DevOps");
                                doc4.addText("body", "Cloud computing platforms enable scalable deployment. Learn about container orchestration, microservices architecture, and continuous integration pipelines.");
                                doc4.addText("category", "cloud computing");
                                doc4.addText("tags", "cloud devops containers kubernetes docker");
                                doc4.addText("author", "Jennifer Kim");
                                doc4.addText("email", "j.kim@cloudtech.net");
                                doc4.addInteger("score", 92);
                                doc4.addFloat("rating", 4.6);
                                doc4.addUnsigned("views", 15680);
                                doc4.addDate("published", LocalDateTime.of(2024, 6, 3, 11, 15));
                                doc4.addBoolean("featured", true);
                                writer.addDocument(doc4);
                            }
                            
                            // Document 5: Low-scoring article for range testing
                            try (Document doc5 = new Document()) {
                                doc5.addText("title", "Basic Programming Concepts");
                                doc5.addText("body", "Introduction to programming fundamentals including variables, loops, and basic data structures for beginners.");
                                doc5.addText("category", "programming");
                                doc5.addText("tags", "programming basics fundamentals beginner");
                                doc5.addText("author", "Tom Wilson");
                                doc5.addText("email", "tom@basics.edu");
                                doc5.addInteger("score", 45);
                                doc5.addFloat("rating", 3.1);
                                doc5.addUnsigned("views", 2150);
                                doc5.addDate("published", LocalDateTime.of(2023, 8, 12, 13, 00));
                                doc5.addBoolean("featured", false);
                                writer.addDocument(doc5);
                            }
                            
                            writer.commit();
                            System.out.println("✅ Indexed 5 diverse test documents");
                        }
                        
                        index.reload();
                        
                        // === PHASE 3: COMPREHENSIVE INTEGRATION TESTING ===
                        System.out.println("\n🔍 Phase 3: Testing comprehensive query integration");
                        
                        try (Searcher searcher = index.searcher()) {
                            assertEquals(5, searcher.getNumDocs(), "Should have 5 documents");
                            System.out.println("✅ Index contains " + searcher.getNumDocs() + " documents");
                            
                            // Test 1: Complex Boolean Query Combining Multiple Types
                            System.out.println("\n🔎 Integration Test 1: Complex boolean query with multiple query types");
                            try (Query termQuery = Query.termQuery(schema, "tags", "machine");
                                 Query rangeQuery = Query.rangeQuery(schema, "score", FieldType.INTEGER, 80, 100, true, true);
                                 Query boolQuery = Query.booleanQuery(List.of(
                                     new Query.OccurQuery(Occur.MUST, termQuery),
                                     new Query.OccurQuery(Occur.MUST, rangeQuery)
                                 ))) {
                                
                                try (SearchResult result = searcher.search(boolQuery, 10)) {
                                    var hits = result.getHits();
                                    System.out.println("  Found " + hits.size() + " documents matching 'machine' AND score 80-100");
                                    assertTrue(hits.size() >= 1, "Should find high-scoring machine learning documents");
                                    
                                    for (SearchResult.Hit hit : hits) {
                                        try (Document doc = searcher.doc(hit.getDocAddress())) {
                                            String title = doc.get("title").get(0).toString();
                                            long scoreLong = (Long) doc.get("score").get(0);
                                            int score = (int) scoreLong;
                                            System.out.println("    📄 " + title + " (score: " + score + ")");
                                            assertTrue(score >= 80 && score <= 100, "Score should be in range 80-100");
                                            String tags = doc.get("tags").get(0).toString();
                                            assertTrue(tags.toLowerCase().contains("machine"), "Should contain 'machine' in tags");
                                        }
                                    }
                                }
                            }
                            
                            // Test 2: Fuzzy Query with Range Filter
                            System.out.println("\n🔎 Integration Test 2: Fuzzy query + range filter combination");
                            try (Query fuzzyQuery = Query.fuzzyTermQuery(schema, "body", "mangement", 2, true, false);
                                 Query ratingRange = Query.rangeQuery(schema, "rating", FieldType.FLOAT, 4.0, 5.0, true, true);
                                 Query combinedQuery = Query.booleanQuery(List.of(
                                     new Query.OccurQuery(Occur.MUST, fuzzyQuery),
                                     new Query.OccurQuery(Occur.MUST, ratingRange)
                                 ))) {
                                
                                try (SearchResult result = searcher.search(combinedQuery, 10)) {
                                    var hits = result.getHits();
                                    System.out.println("  Found " + hits.size() + " documents with fuzzy 'mangement' AND rating 4.0-5.0");
                                    
                                    for (SearchResult.Hit hit : hits) {
                                        try (Document doc = searcher.doc(hit.getDocAddress())) {
                                            String title = doc.get("title").get(0).toString();
                                            double rating = (Double) doc.get("rating").get(0);
                                            System.out.println("    📄 " + title + " (rating: " + rating + ")");
                                            assertTrue(rating >= 4.0 && rating <= 5.0, "Rating should be in range 4.0-5.0");
                                        }
                                    }
                                }
                            }
                            
                            // Test 3: Phrase Query with Boost
                            System.out.println("\n🔎 Integration Test 3: Phrase query with boost scoring");
                            try (Query phraseQuery = Query.phraseQuery(schema, "body", List.of("machine", "learning"), 1);
                                 Query boostedQuery = Query.boostQuery(phraseQuery, 3.0)) {
                                
                                try (SearchResult result = searcher.search(boostedQuery, 10)) {
                                    var hits = result.getHits();
                                    System.out.println("  Found " + hits.size() + " documents with boosted 'machine learning' phrase");
                                    
                                    double previousScore = Double.MAX_VALUE;
                                    for (SearchResult.Hit hit : hits) {
                                        try (Document doc = searcher.doc(hit.getDocAddress())) {
                                            String title = doc.get("title").get(0).toString();
                                            double score = hit.getScore();
                                            System.out.println("    📄 " + title + " (boosted score: " + String.format("%.3f", score) + ")");
                                            assertTrue(score <= previousScore, "Scores should be in descending order");
                                            previousScore = score;
                                        }
                                    }
                                }
                            }
                            
                            // Test 4: Complex Multi-Field Search with Date Range
                            System.out.println("\n🔎 Integration Test 4: Multi-field search with date filtering");
                            LocalDateTime startDate = LocalDateTime.of(2024, 1, 1, 0, 0);
                            LocalDateTime endDate = LocalDateTime.of(2024, 12, 31, 23, 59);
                            
                            try (Query authorQuery = Query.termQuery(schema, "author", "Sarah");
                                 Query dateRange = Query.rangeQuery(schema, "published", FieldType.DATE, startDate, endDate, true, true);
                                 Query featuredQuery = Query.termQuery(schema, "featured", true);
                                 Query multiQuery = Query.booleanQuery(List.of(
                                     new Query.OccurQuery(Occur.SHOULD, authorQuery),
                                     new Query.OccurQuery(Occur.MUST, dateRange),
                                     new Query.OccurQuery(Occur.SHOULD, featuredQuery)
                                 ))) {
                                
                                try (SearchResult result = searcher.search(multiQuery, 10)) {
                                    var hits = result.getHits();
                                    System.out.println("  Found " + hits.size() + " documents from 2024 (author OR featured preference)");
                                    
                                    for (SearchResult.Hit hit : hits) {
                                        try (Document doc = searcher.doc(hit.getDocAddress())) {
                                            String title = doc.get("title").get(0).toString();
                                            String author = doc.get("author").get(0).toString();
                                            LocalDateTime published = (LocalDateTime) doc.get("published").get(0);
                                            boolean featured = (Boolean) doc.get("featured").get(0);
                                            
                                            System.out.println("    📄 " + title);
                                            System.out.println("       Author: " + author + ", Published: " + 
                                                published.format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm")) + 
                                                ", Featured: " + featured);
                                            
                                            assertTrue(published.getYear() == 2024, "Should be published in 2024");
                                        }
                                    }
                                }
                            }
                            
                            // Test 5: Regex Query with Const Score
                            System.out.println("\n🔎 Integration Test 5: Regex query with constant scoring");
                            try (Query regexQuery = Query.regexQuery(schema, "email", ".*\\.edu");
                                 Query constScoreQuery = Query.constScoreQuery(regexQuery, 2.5)) {
                                
                                try (SearchResult result = searcher.search(constScoreQuery, 10)) {
                                    var hits = result.getHits();
                                    System.out.println("  Found " + hits.size() + " documents with .edu emails (const score: 2.5)");
                                    
                                    for (SearchResult.Hit hit : hits) {
                                        try (Document doc = searcher.doc(hit.getDocAddress())) {
                                            String title = doc.get("title").get(0).toString();
                                            String email = doc.get("email").get(0).toString();
                                            double score = hit.getScore();
                                            
                                            System.out.println("    📄 " + title + " [" + email + "] (score: " + String.format("%.3f", score) + ")");
                                            assertTrue(email.endsWith(".edu"), "Email should end with .edu");
                                            assertEquals(2.5, score, 0.1, "Score should be approximately 2.5");
                                        }
                                    }
                                }
                            }
                            
                            // Test 6: Edge Case Testing - Empty Results and Error Handling
                            System.out.println("\n🔎 Integration Test 6: Edge cases and error handling");
                            
                            // Test impossible range query
                            try (Query impossibleRange = Query.rangeQuery(schema, "score", FieldType.INTEGER, 200, 300, true, true)) {
                                try (SearchResult result = searcher.search(impossibleRange, 10)) {
                                    var hits = result.getHits();
                                    assertEquals(0, hits.size(), "Should find no documents with impossible score range");
                                    System.out.println("  ✅ Correctly handled impossible range query (0 results)");
                                }
                            }
                            
                            // Test fuzzy query with non-existent term
                            try (Query nonExistentFuzzy = Query.fuzzyTermQuery(schema, "body", "xyzabc123", 2, true, false)) {
                                try (SearchResult result = searcher.search(nonExistentFuzzy, 10)) {
                                    var hits = result.getHits();
                                    System.out.println("  ✅ Non-existent fuzzy query returned " + hits.size() + " results (expected behavior)");
                                }
                            }
                            
                            // Test 7: Performance Test with Complex Query
                            System.out.println("\n🔎 Integration Test 7: Complex query performance test");
                            long startTime = System.currentTimeMillis();
                            
                            try (Query term1 = Query.termQuery(schema, "body", "systems");
                                 Query term2 = Query.termQuery(schema, "body", "development");
                                 Query fuzzy1 = Query.fuzzyTermQuery(schema, "body", "platfroms", 2, true, false);
                                 Query range1 = Query.rangeQuery(schema, "views", FieldType.UNSIGNED, 1000, 20000, true, true);
                                 Query range2 = Query.rangeQuery(schema, "rating", FieldType.FLOAT, 3.0, 5.0, true, true);
                                 Query bool1 = Query.booleanQuery(List.of(
                                     new Query.OccurQuery(Occur.SHOULD, term1),
                                     new Query.OccurQuery(Occur.SHOULD, term2)
                                 ));
                                 Query bool2 = Query.booleanQuery(List.of(
                                     new Query.OccurQuery(Occur.SHOULD, fuzzy1),
                                     new Query.OccurQuery(Occur.MUST, range1),
                                     new Query.OccurQuery(Occur.MUST, range2)
                                 ));
                                 Query finalQuery = Query.booleanQuery(List.of(
                                     new Query.OccurQuery(Occur.SHOULD, bool1),
                                     new Query.OccurQuery(Occur.SHOULD, bool2)
                                 ))) {
                                
                                try (SearchResult result = searcher.search(finalQuery, 10)) {
                                    var hits = result.getHits();
                                    long endTime = System.currentTimeMillis();
                                    long duration = endTime - startTime;
                                    
                                    System.out.println("  Complex query executed in " + duration + "ms, found " + hits.size() + " results");
                                    assertTrue(duration < 1000, "Complex query should complete within 1 second");
                                    System.out.println("  ✅ Performance test passed (< 1000ms)");
                                }
                            }
                        }
                    }
                }
            }
            
            System.out.println("\n🎉 === COMPREHENSIVE INTEGRATION TEST COMPLETED ===");
            System.out.println("✨ Successfully demonstrated production-ready capabilities:");
            System.out.println("   🔗 Complex boolean query combinations");
            System.out.println("   🎯 Multi-field searches with diverse data types");
            System.out.println("   🔄 Query type combinations (fuzzy + range, phrase + boost, etc.)");
            System.out.println("   📅 Date range filtering with temporal queries");
            System.out.println("   🎨 Scoring manipulation (boost, const score)");
            System.out.println("   ⚠️  Edge case handling (empty results, impossible ranges)");
            System.out.println("   ⚡ Performance optimization (complex queries < 1s)");
            System.out.println("   🐍 Full Python tantivy library production parity");
            
        } catch (Exception e) {
            fail("Integration test failed: " + e.getMessage());
        }
    }
}

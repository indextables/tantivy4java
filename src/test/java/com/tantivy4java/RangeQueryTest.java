package com.tantivy4java;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;

import java.nio.file.Path;
import java.time.LocalDateTime;
import java.util.Arrays;

/**
 * Test class for Range Query functionality demonstrating numeric and date range searches
 * matching Python tantivy library capabilities
 */
public class RangeQueryTest {
    
    @Test
    @DisplayName("Range Query functionality for numeric and date fields")
    public void testRangeQueryFunctionality(@TempDir Path tempDir) {
        System.out.println("🚀 === RANGE QUERY FUNCTIONALITY TEST ===");
        System.out.println("Testing range queries on numeric and date fields");
        
        String indexPath = tempDir.resolve("range_query_index").toString();
        
        try {
            // === PHASE 1: SCHEMA CREATION WITH RANGE-QUERYABLE FIELDS ===
            System.out.println("\n📋 Phase 1: Creating schema with range-queryable fields");
            
            try (SchemaBuilder builder = new SchemaBuilder()) {
                builder
                    .addTextField("title", true, false, "default", "position")
                    .addIntegerField("score", true, true, true)        // For integer range queries
                    .addFloatField("rating", true, true, true)         // For float range queries  
                    .addUnsignedField("views", true, true, true)       // For unsigned range queries
                    .addDateField("published_date", true, true, true); // For date range queries
                
                try (Schema schema = builder.build()) {
                    System.out.println("✅ Created schema with range-queryable fields");
                    
                    // === PHASE 2: INDEX CREATION AND DOCUMENT INDEXING ===
                    System.out.println("\n📝 Phase 2: Creating index and adding test documents");
                    
                    try (Index index = new Index(schema, indexPath, false)) {
                        try (IndexWriter writer = index.writer(100, 2)) {
                            
                            // Add test documents with varying numeric and date values
                            try (Document doc1 = new Document()) {
                                doc1.addText("title", "Low Score Article");
                                doc1.addInteger("score", 25);
                                doc1.addFloat("rating", 2.1f);
                                doc1.addUnsigned("views", 150L);
                                doc1.addDate("published_date", LocalDateTime.of(2023, 1, 15, 10, 30));
                                writer.addDocument(doc1);
                            }
                            
                            try (Document doc2 = new Document()) {
                                doc2.addText("title", "Medium Score Article");
                                doc2.addInteger("score", 75);
                                doc2.addFloat("rating", 3.5f);
                                doc2.addUnsigned("views", 1250L);
                                doc2.addDate("published_date", LocalDateTime.of(2023, 6, 10, 14, 15));
                                writer.addDocument(doc2);
                            }
                            
                            try (Document doc3 = new Document()) {
                                doc3.addText("title", "High Score Article");
                                doc3.addInteger("score", 95);
                                doc3.addFloat("rating", 4.8f);
                                doc3.addUnsigned("views", 5280L);
                                doc3.addDate("published_date", LocalDateTime.of(2024, 2, 20, 9, 45));
                                writer.addDocument(doc3);
                            }
                            
                            try (Document doc4 = new Document()) {
                                doc4.addText("title", "Super High Score Article");
                                doc4.addInteger("score", 100);
                                doc4.addFloat("rating", 5.0f);
                                doc4.addUnsigned("views", 10500L);
                                doc4.addDate("published_date", LocalDateTime.of(2024, 8, 5, 16, 20));
                                writer.addDocument(doc4);
                            }
                            
                            writer.commit();
                            System.out.println("✅ Indexed 4 test documents with range-queryable values");
                        }
                        
                        index.reload();
                        
                        // === PHASE 3: RANGE QUERY TESTING ===
                        System.out.println("\n🔍 Phase 3: Testing range query functionality");
                        
                        try (Searcher searcher = index.searcher()) {
                            assertEquals(4, searcher.getNumDocs(), "Should have 4 documents");
                            System.out.println("✅ Index contains " + searcher.getNumDocs() + " documents");
                            
                            // Test 1: Integer range query (score between 50 and 90)
                            System.out.println("\n🔎 Range Query Test 1: Integer range (score: 50-90)");
                            try (Query intRangeQuery = Query.rangeQuery(schema, "score", FieldType.INTEGER, 50, 90, true, true)) {
                                try (SearchResult result = searcher.search(intRangeQuery, 10)) {
                                    var hits = result.getHits();
                                    assertEquals(1, hits.size(), "Should find 1 document with score 50-90");
                                    System.out.println("  Found " + hits.size() + " documents with score between 50-90");
                                    
                                    for (SearchResult.Hit hit : hits) {
                                        try (Document doc = searcher.doc(hit.getDocAddress())) {
                                            String title = doc.get("title").get(0).toString();
                                            Integer score = ((Long) doc.get("score").get(0)).intValue();
                                            System.out.println("    📄 " + title + " (score: " + score + ")");
                                            assertTrue(score >= 50 && score <= 90, "Score should be in range 50-90");
                                        }
                                    }
                                }
                            }
                            
                            // Test 2: Float range query (rating between 3.0 and 4.5)
                            System.out.println("\n🔎 Range Query Test 2: Float range (rating: 3.0-4.5)");
                            try (Query floatRangeQuery = Query.rangeQuery(schema, "rating", FieldType.FLOAT, 3.0, 4.5, true, true)) {
                                try (SearchResult result = searcher.search(floatRangeQuery, 10)) {
                                    var hits = result.getHits();
                                    assertEquals(1, hits.size(), "Should find 1 document with rating 3.0-4.5");
                                    System.out.println("  Found " + hits.size() + " documents with rating between 3.0-4.5");
                                    
                                    for (SearchResult.Hit hit : hits) {
                                        try (Document doc = searcher.doc(hit.getDocAddress())) {
                                            String title = doc.get("title").get(0).toString();
                                            Double rating = (Double) doc.get("rating").get(0);
                                            System.out.println("    📄 " + title + " (rating: " + rating + ")");
                                            assertTrue(rating >= 3.0 && rating <= 4.5, "Rating should be in range 3.0-4.5");
                                        }
                                    }
                                }
                            }
                            
                            // Test 3: Unsigned range query (views between 1000 and 6000)
                            System.out.println("\n🔎 Range Query Test 3: Unsigned range (views: 1000-6000)");
                            try (Query unsignedRangeQuery = Query.rangeQuery(schema, "views", FieldType.UNSIGNED, 1000L, 6000L, true, true)) {
                                try (SearchResult result = searcher.search(unsignedRangeQuery, 10)) {
                                    var hits = result.getHits();
                                    assertEquals(2, hits.size(), "Should find 2 documents with views 1000-6000");
                                    System.out.println("  Found " + hits.size() + " documents with views between 1000-6000");
                                    
                                    for (SearchResult.Hit hit : hits) {
                                        try (Document doc = searcher.doc(hit.getDocAddress())) {
                                            String title = doc.get("title").get(0).toString();
                                            Long views = (Long) doc.get("views").get(0);
                                            System.out.println("    📄 " + title + " (views: " + views + ")");
                                            assertTrue(views >= 1000L && views <= 6000L, "Views should be in range 1000-6000");
                                        }
                                    }
                                }
                            }
                            
                            // Test 4: Date range query (published in 2024)
                            System.out.println("\n🔎 Range Query Test 4: Date range (published in 2024)");
                            LocalDateTime start2024 = LocalDateTime.of(2024, 1, 1, 0, 0);
                            LocalDateTime end2024 = LocalDateTime.of(2024, 12, 31, 23, 59);
                            
                            try (Query dateRangeQuery = Query.rangeQuery(schema, "published_date", FieldType.DATE, start2024, end2024, true, true)) {
                                try (SearchResult result = searcher.search(dateRangeQuery, 10)) {
                                    var hits = result.getHits();
                                    assertEquals(2, hits.size(), "Should find 2 documents published in 2024");
                                    System.out.println("  Found " + hits.size() + " documents published in 2024");
                                    
                                    for (SearchResult.Hit hit : hits) {
                                        try (Document doc = searcher.doc(hit.getDocAddress())) {
                                            String title = doc.get("title").get(0).toString();
                                            LocalDateTime publishedDate = (LocalDateTime) doc.get("published_date").get(0);
                                            System.out.println("    📄 " + title + " (published: " + publishedDate + ")");
                                            assertTrue(publishedDate.getYear() == 2024, "Published date should be in 2024");
                                        }
                                    }
                                }
                            }
                            
                            // Test 5: Exclusive range query (score > 25 and < 95)
                            System.out.println("\n🔎 Range Query Test 5: Exclusive range (score > 25 and < 95)");
                            try (Query exclusiveRangeQuery = Query.rangeQuery(schema, "score", FieldType.INTEGER, 25, 95, false, false)) {
                                try (SearchResult result = searcher.search(exclusiveRangeQuery, 10)) {
                                    var hits = result.getHits();
                                    assertEquals(1, hits.size(), "Should find 1 document with score > 25 and < 95");
                                    System.out.println("  Found " + hits.size() + " documents with score > 25 and < 95");
                                    
                                    for (SearchResult.Hit hit : hits) {
                                        try (Document doc = searcher.doc(hit.getDocAddress())) {
                                            String title = doc.get("title").get(0).toString();
                                            Integer score = ((Long) doc.get("score").get(0)).intValue();
                                            System.out.println("    📄 " + title + " (score: " + score + ")");
                                            assertTrue(score > 25 && score < 95, "Score should be > 25 and < 95");
                                        }
                                    }
                                }
                            }
                            
                            // Test 6: Open-ended range query (score >= 90)
                            System.out.println("\n🔎 Range Query Test 6: Open-ended range (score >= 90)");
                            try (Query openRangeQuery = Query.rangeQuery(schema, "score", FieldType.INTEGER, 90, null, true, true)) {
                                try (SearchResult result = searcher.search(openRangeQuery, 10)) {
                                    var hits = result.getHits();
                                    assertEquals(2, hits.size(), "Should find 2 documents with score >= 90");
                                    System.out.println("  Found " + hits.size() + " documents with score >= 90");
                                    
                                    for (SearchResult.Hit hit : hits) {
                                        try (Document doc = searcher.doc(hit.getDocAddress())) {
                                            String title = doc.get("title").get(0).toString();
                                            Integer score = ((Long) doc.get("score").get(0)).intValue();
                                            System.out.println("    📄 " + title + " (score: " + score + ")");
                                            assertTrue(score >= 90, "Score should be >= 90");
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
            
            System.out.println("\n🎉 === RANGE QUERY FUNCTIONALITY TEST COMPLETED ===");
            System.out.println("✨ Successfully demonstrated complete range query capabilities:");
            System.out.println("   🔢 Integer range queries with inclusive/exclusive bounds");
            System.out.println("   🔢 Float range queries for decimal values");
            System.out.println("   🔢 Unsigned range queries for positive integers");
            System.out.println("   📅 Date range queries for temporal data");
            System.out.println("   🎯 Exclusive and inclusive bound handling");
            System.out.println("   📈 Open-ended range queries (unbounded)");
            System.out.println("   🐍 Full Python tantivy library range query compatibility");
            
        } catch (Exception e) {
            fail("Range query test failed: " + e.getMessage());
        }
    }
}
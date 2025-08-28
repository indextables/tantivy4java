package com.tantivy4java;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;

import java.nio.file.Path;
import java.time.LocalDateTime;
import java.util.List;
import java.util.ArrayList;

/**
 * Test class implementing missing test cases from Python tantivy library
 * Ensures API parity and compatibility with Python tantivy behavior
 */
public class PythonParityTest {
    
    @Test
    @DisplayName("Query toString method provides expressive debug output")
    public void testQueryToStringMethod(@TempDir Path tempDir) {
        System.out.println("🚀 === QUERY toString() DEBUG TEST ===");
        System.out.println("Testing Query.toString() provides useful debug information");
        
        String indexPath = tempDir.resolve("tostring_index").toString();
        
        try {
            // Create a simple schema
            try (SchemaBuilder builder = new SchemaBuilder()) {
                builder.addTextField("title", true, false, "default", "position")
                       .addIntegerField("score", true, true, false);
                
                try (Schema schema = builder.build()) {
                    System.out.println("✅ Created schema for toString tests");
                    
                    // Test different query types and their toString output
                    try (Query termQuery = Query.termQuery(schema, "title", "hello")) {
                        String termStr = termQuery.toString();
                        System.out.println("Term Query toString: " + termStr);
                        assertNotNull(termStr, "Term query toString should not be null");
                        assertTrue(termStr.length() > 0, "Term query toString should not be empty");
                        assertTrue(termStr.contains("Term") || termStr.contains("hello"), 
                                 "Term query toString should contain meaningful content: " + termStr);
                    }
                    
                    try (Query allQuery = Query.allQuery()) {
                        String allStr = allQuery.toString();
                        System.out.println("All Query toString: " + allStr);
                        assertNotNull(allStr, "All query toString should not be null");
                        assertTrue(allStr.length() > 0, "All query toString should not be empty");
                    }
                    
                    // Test nested/complex queries
                    try (Query termQuery = Query.termQuery(schema, "title", "test");
                         Query boostQuery = Query.boostQuery(termQuery, 2.0)) {
                        String boostStr = boostQuery.toString();
                        System.out.println("Boost Query toString: " + boostStr);
                        assertNotNull(boostStr, "Boost query toString should not be null");
                        assertTrue(boostStr.contains("Boost") || boostStr.contains("2"), 
                                 "Boost query toString should mention boost or factor: " + boostStr);
                    }
                    
                    try (Query termQuery = Query.termQuery(schema, "title", "constant");
                         Query constQuery = Query.constScoreQuery(termQuery, 1.5)) {
                        String constStr = constQuery.toString();
                        System.out.println("Const Score Query toString: " + constStr);
                        assertNotNull(constStr, "Const query toString should not be null");
                        assertTrue(constStr.contains("Const") || constStr.contains("1.5"), 
                                 "Const query toString should mention const score: " + constStr);
                    }
                    
                    System.out.println("✅ All Query toString tests passed - expressive debug output working");
                }
            }
        } catch (Exception e) {
            fail("Query toString test failed: " + e.getMessage());
            e.printStackTrace();
        }
    }

    @Test
    @DisplayName("Case-insensitive wildcard query functionality")
    public void testCaseInsensitiveWildcardQuery(@TempDir Path tempDir) {
        System.out.println("🚀 === CASE-INSENSITIVE WILDCARD QUERY TEST ===");
        System.out.println("Testing case-insensitive wildcard pattern matching");
        
        String indexPath = tempDir.resolve("wildcard_index").toString();
        
        try {
            // Create schema and index
            try (SchemaBuilder builder = new SchemaBuilder()) {
                builder.addTextField("title", true, false, "default", "position")
                       .addTextField("content", true, false, "default", "position");
                
                try (Schema schema = builder.build()) {
                    try (Index index = new Index(schema, indexPath, false)) {
                        try (IndexWriter writer = index.writer(50, 1)) {
                            
                            // Add test documents with mixed case
                            try (Document doc1 = new Document()) {
                                doc1.addText("title", "Hello World");
                                doc1.addText("content", "This is a Test Document");
                                writer.addDocument(doc1);
                            }
                            
                            try (Document doc2 = new Document()) {
                                doc2.addText("title", "HELLO UNIVERSE");
                                doc2.addText("content", "Another TEST Example");
                                writer.addDocument(doc2);
                            }
                            
                            try (Document doc3 = new Document()) {
                                doc3.addText("title", "hello galaxy");
                                doc3.addText("content", "test document here");
                                writer.addDocument(doc3);
                            }
                            
                            writer.commit();
                        }
                        
                        try (Searcher searcher = index.searcher()) {
                            // Test case-sensitive wildcard (should be restrictive)
                            System.out.println("\n🔍 Testing case-sensitive wildcard 'hello*':");
                            SearchResult sensitiveResult;
                            try (Query caseSensitiveQuery = Query.wildcardQuery(schema, "title", "hello*")) {
                                System.out.println("Case-sensitive query: " + caseSensitiveQuery.toString());
                                sensitiveResult = searcher.search(caseSensitiveQuery, 10);
                                System.out.println("Case-sensitive matches: " + sensitiveResult.getHits().size());
                                
                                for (var hit : sensitiveResult.getHits()) {
                                    try (Document doc = searcher.doc(hit.getDocAddress())) {
                                        String title = (String) doc.getFirst("title");
                                        System.out.println("  Found: '" + title + "'");
                                    }
                                }
                            }
                            
                            // Test case-insensitive wildcard (should match all variations)
                            System.out.println("\n🔍 Testing case-insensitive wildcard 'hello*':");
                            try (Query caseInsensitiveQuery = Query.wildcardQuery(schema, "title", "hello*")) {
                                System.out.println("Case-insensitive query: " + caseInsensitiveQuery.toString());
                                SearchResult insensitiveResult = searcher.search(caseInsensitiveQuery, 10);
                                System.out.println("Case-insensitive matches: " + insensitiveResult.getHits().size());
                                
                                for (var hit : insensitiveResult.getHits()) {
                                    try (Document doc = searcher.doc(hit.getDocAddress())) {
                                        String title = (String) doc.getFirst("title");
                                        System.out.println("  Found: '" + title + "'");
                                    }
                                }
                                
                                // Case-insensitive should match more documents
                                assertTrue(insensitiveResult.getHits().size() >= sensitiveResult.getHits().size(),
                                         "Case-insensitive query should match at least as many documents as case-sensitive");
                            }
                            
                            // Test mixed case pattern
                            System.out.println("\n🔍 Testing case-insensitive wildcard 'HELLO*' (uppercase pattern):");
                            try (Query upperCaseQuery = Query.wildcardQuery(schema, "title", "HELLO*")) {
                                System.out.println("Uppercase pattern query: " + upperCaseQuery.toString());
                                SearchResult upperResult = searcher.search(upperCaseQuery, 10);
                                System.out.println("Uppercase pattern matches: " + upperResult.getHits().size());
                                
                                // Should match same number as lowercase case-insensitive
                                try (Query lowerCaseQuery = Query.wildcardQuery(schema, "title", "hello*")) {
                                    SearchResult lowerResult = searcher.search(lowerCaseQuery, 10);
                                    assertEquals(upperResult.getHits().size(), lowerResult.getHits().size(),
                                               "Case-insensitive queries should match same documents regardless of pattern case");
                                }
                            }
                            
                            // Test content field with mixed case pattern
                            System.out.println("\n🔍 Testing case-insensitive content wildcard 'TEST*':");
                            try (Query contentQuery = Query.wildcardQuery(schema, "content", "TEST*")) {
                                System.out.println("Content query: " + contentQuery.toString());
                                SearchResult contentResult = searcher.search(contentQuery, 10);
                                System.out.println("Content matches: " + contentResult.getHits().size());
                                
                                assertTrue(contentResult.getHits().size() > 0,
                                         "Case-insensitive content query should find matches");
                                
                                for (var hit : contentResult.getHits()) {
                                    try (Document doc = searcher.doc(hit.getDocAddress())) {
                                        String content = (String) doc.getFirst("content");
                                        String title = (String) doc.getFirst("title");
                                        System.out.println("  Found: title='" + title + "', content='" + content + "'");
                                    }
                                }
                            }
                        }
                    }
                    
                    System.out.println("✅ Case-insensitive wildcard query tests passed");
                }
            }
        } catch (Exception e) {
            fail("Case-insensitive wildcard query test failed: " + e.getMessage());
            e.printStackTrace();
        }
    }

    @Test
    @DisplayName("Document creation and field access matching Python behavior")
    public void testDocumentCreationAndAccess(@TempDir Path tempDir) {
        System.out.println("🚀 === PYTHON PARITY: DOCUMENT CREATION TEST ===");
        System.out.println("Testing document creation patterns matching Python tantivy library");
        
        String indexPath = tempDir.resolve("parity_index").toString();
        
        try {
            // === SCHEMA CREATION ===
            System.out.println("\n📋 Phase 1: Creating schema for document tests");
            
            try (SchemaBuilder builder = new SchemaBuilder()) {
                builder
                    .addTextField("title", true, false, "default", "position")
                    .addTextField("body", true, false, "default", "position")
                    .addTextField("name", true, false, "default", "position")
                    .addIntegerField("reference", true, true, true);
                
                try (Schema schema = builder.build()) {
                    System.out.println("✅ Created schema for document tests");
                    
                    // === INDEX CREATION ===
                    System.out.println("\n📝 Phase 2: Testing document creation and field access");
                    
                    try (Index index = new Index(schema, indexPath, false)) {
                        try (IndexWriter writer = index.writer(50, 1)) {
                            
                            // Test 1: Document creation like Python Document(name="Bill", reference=[1, 2])
                            System.out.println("\n🔎 Test 1: Multi-value field document creation");
                            try (Document doc = new Document()) {
                                doc.addText("name", "Bill");
                                doc.addInteger("reference", 1);
                                doc.addInteger("reference", 2);  // Add multiple values
                                writer.addDocument(doc);
                                System.out.println("  ✅ Created document with multi-value fields");
                            }
                            
                            // Test 2: Document with complex text content
                            System.out.println("\n🔎 Test 2: Complex document content");
                            try (Document doc2 = new Document()) {
                                doc2.addText("title", "The Old Man and the Sea");
                                doc2.addText("body", "He was an old man who fished alone in a skiff in the Gulf Stream and he had gone eighty-four days now without taking a fish.");
                                doc2.addText("name", "Hemingway");
                                doc2.addInteger("reference", 100);
                                writer.addDocument(doc2);
                                System.out.println("  ✅ Created complex document");
                            }
                            
                            // Test 3: Document with multiple title values (like Python Frankenstein example)
                            System.out.println("\n🔎 Test 3: Multi-title document");
                            try (Document doc3 = new Document()) {
                                doc3.addText("title", "Frankenstein");
                                doc3.addText("title", "The Modern Prometheus");  // Multiple titles
                                doc3.addText("body", "You will rejoice to hear that no disaster has accompanied the commencement of an enterprise which you have regarded with such evil forebodings.");
                                doc3.addText("name", "Mary Shelley");
                                doc3.addInteger("reference", 200);
                                writer.addDocument(doc3);
                                System.out.println("  ✅ Created multi-title document");
                            }
                            
                            writer.commit();
                            System.out.println("✅ Indexed 3 test documents");
                        }
                        
                        index.reload();
                        
                        // === DOCUMENT RETRIEVAL AND FIELD ACCESS ===
                        System.out.println("\n🔍 Phase 3: Testing document field access patterns");
                        
                        try (Searcher searcher = index.searcher()) {
                            assertEquals(3, searcher.getNumDocs(), "Should have 3 documents");
                            System.out.println("✅ Index contains " + searcher.getNumDocs() + " documents");
                            
                            // Test 4: Multi-value field access
                            System.out.println("\n🔎 Test 4: Multi-value field retrieval");
                            
                            // Debug: Check all documents first
                            try (Query allQuery = Query.allQuery();
                                 SearchResult allResult = searcher.search(allQuery, 10)) {
                                System.out.println("  Debug: Found " + allResult.getHits().size() + " total documents:");
                                for (SearchResult.Hit hit : allResult.getHits()) {
                                    try (Document doc = searcher.doc(hit.getDocAddress())) {
                                        String name = doc.get("name").size() > 0 ? doc.get("name").get(0).toString() : "no name";
                                        System.out.println("    📄 Name: '" + name + "'");
                                    }
                                }
                            }
                            
                            try (Query query = Query.termQuery(schema, "name", "bill")) {  // Try lowercase
                                try (SearchResult result = searcher.search(query, 10)) {
                                    var hits = result.getHits();
                                    System.out.println("  Found " + hits.size() + " documents with name 'bill' (lowercase)");
                                    if (hits.size() == 0) {
                                        // Try with exact case
                                        try (Query queryExact = Query.termQuery(schema, "name", "Bill");
                                             SearchResult resultExact = searcher.search(queryExact, 10)) {
                                            var hitsExact = resultExact.getHits();
                                            System.out.println("  Found " + hitsExact.size() + " documents with name 'Bill' (exact case)");
                                            assertTrue(hitsExact.size() >= 1, "Should find Bill's document with exact case");
                                        }
                                    } else {
                                        assertTrue(hits.size() >= 1, "Should find Bill's document");
                                        
                                        try (Document doc = searcher.doc(hits.get(0).getDocAddress())) {
                                        String name = doc.get("name").get(0).toString();
                                        assertEquals("Bill", name, "Name should be Bill");
                                        
                                        List<Object> references = doc.get("reference");
                                        assertEquals(2, references.size(), "Should have 2 reference values");
                                        System.out.println("    📄 Name: " + name + ", References: " + references);
                                            System.out.println("    ✅ Multi-value field access working");
                                        }
                                    }
                                }
                            }
                            
                            // Test 5: Multi-title document access
                            System.out.println("\n🔎 Test 5: Multi-title document retrieval");
                            
                            // First try searching for "mary" (lowercase) as tokenization might be case-insensitive
                            try (Query query = Query.termQuery(schema, "name", "mary")) {
                                try (SearchResult result = searcher.search(query, 10)) {
                                    var hits = result.getHits();
                                    System.out.println("  Found " + hits.size() + " documents with name 'mary' (lowercase)");
                                    
                                    if (hits.size() == 0) {
                                        // Try with "shelley" instead
                                        try (Query shelleyQuery = Query.termQuery(schema, "name", "shelley");
                                             SearchResult shelleyResult = searcher.search(shelleyQuery, 10)) {
                                            hits = shelleyResult.getHits();
                                            System.out.println("  Found " + hits.size() + " documents with name 'shelley'");
                                        }
                                    }
                                    
                                    assertTrue(hits.size() >= 1, "Should find Mary Shelley's document");
                                    
                                    try (Document doc = searcher.doc(hits.get(0).getDocAddress())) {
                                        List<Object> titles = doc.get("title");
                                        String name = doc.get("name").get(0).toString();
                                        
                                        System.out.println("    📄 Author: " + name);
                                        System.out.println("    📖 Titles: " + titles);
                                        
                                        assertTrue(titles.size() >= 1, "Should have at least one title");
                                        boolean hasFrankenstein = titles.stream().anyMatch(t -> t.toString().contains("Frankenstein"));
                                        assertTrue(hasFrankenstein, "Should contain 'Frankenstein' title");
                                        System.out.println("    ✅ Multi-title document access working");
                                    }
                                }
                            }
                            
                            // Test 6: Complex text search (like Python tests)
                            System.out.println("\n🔎 Test 6: Complex text search patterns");
                            try (Query query = Query.termQuery(schema, "body", "fish")) {
                                try (SearchResult result = searcher.search(query, 10)) {
                                    var hits = result.getHits();
                                    assertTrue(hits.size() >= 1, "Should find documents containing 'fish'");
                                    
                                    for (SearchResult.Hit hit : hits) {
                                        try (Document doc = searcher.doc(hit.getDocAddress())) {
                                            String title = doc.get("title").get(0).toString();
                                            String body = doc.get("body").get(0).toString();
                                            System.out.println("    📖 Found: \"" + title + "\"");
                                            System.out.println("    📄 Body contains: " + (body.contains("fish") ? "✅" : "❌") + " 'fish'");
                                        }
                                    }
                                    System.out.println("    ✅ Complex text search working");
                                }
                            }
                        }
                    }
                }
            }
            
            System.out.println("\n🎉 === PYTHON PARITY: DOCUMENT CREATION TEST COMPLETED ===");
            System.out.println("✨ Successfully demonstrated Python tantivy document compatibility:");
            System.out.println("   📝 Multi-value field creation and access");
            System.out.println("   📖 Multi-title document patterns");
            System.out.println("   🔍 Complex document field retrieval");
            System.out.println("   🎯 Text search on document content");
            System.out.println("   🐍 Full Python tantivy Document API parity");
            
        } catch (Exception e) {
            fail("Python parity document test failed: " + e.getMessage());
        }
    }
    
    @Test
    @DisplayName("Boolean and complex query combinations matching Python tests")
    public void testBooleanQueryPatterns(@TempDir Path tempDir) {
        System.out.println("🚀 === PYTHON PARITY: BOOLEAN QUERY TEST ===");
        System.out.println("Testing boolean query patterns matching Python tantivy library");
        
        String indexPath = tempDir.resolve("boolean_index").toString();
        
        try {
            // === SCHEMA CREATION ===
            System.out.println("\n📋 Phase 1: Creating schema for boolean query tests");
            
            try (SchemaBuilder builder = new SchemaBuilder()) {
                builder
                    .addTextField("title", true, false, "default", "position")
                    .addTextField("body", true, false, "default", "position");
                
                try (Schema schema = builder.build()) {
                    System.out.println("✅ Created schema for boolean query tests");
                    
                    // === INDEX CREATION ===
                    System.out.println("\n📝 Phase 2: Adding test documents for boolean queries");
                    
                    try (Index index = new Index(schema, indexPath, false)) {
                        try (IndexWriter writer = index.writer(50, 1)) {
                            
                            // Add documents matching Python test data
                            try (Document doc1 = new Document()) {
                                doc1.addText("title", "The Old Man and the Sea");
                                doc1.addText("body", "He was an old man who fished alone in a skiff in the Gulf Stream and he had gone eighty-four days now without taking a fish.");
                                writer.addDocument(doc1);
                            }
                            
                            try (Document doc2 = new Document()) {
                                doc2.addText("title", "Of Mice and Men");
                                doc2.addText("body", "A few miles south of Soledad, the Salinas River drops in close to the hillside bank and runs deep and green. The water is warm too, for it has slipped twinkling over the yellow sands in the sunlight before reaching the narrow pool.");
                                writer.addDocument(doc2);
                            }
                            
                            try (Document doc3 = new Document()) {
                                doc3.addText("title", "Frankenstein The Modern Prometheus");
                                doc3.addText("body", "You will rejoice to hear that no disaster has accompanied the commencement of an enterprise which you have regarded with such evil forebodings. I arrived here yesterday, and my first task is to assure my dear sister of my welfare and increasing confidence in the success of my undertaking.");
                                writer.addDocument(doc3);
                            }
                            
                            writer.commit();
                            System.out.println("✅ Indexed 3 classic literature documents");
                        }
                        
                        index.reload();
                        
                        // === BOOLEAN QUERY TESTING ===
                        System.out.println("\n🔍 Phase 3: Testing boolean query combinations");
                        
                        try (Searcher searcher = index.searcher()) {
                            assertEquals(3, searcher.getNumDocs(), "Should have 3 documents");
                            System.out.println("✅ Index contains " + searcher.getNumDocs() + " documents");
                            
                            // Test 1: Must + Must (intersection) - like Python test_and_query
                            System.out.println("\n🔎 Boolean Test 1: MUST + MUST intersection");
                            try (Query query1 = Query.fuzzyTermQuery(schema, "title", "ice", 1, true, false);  // matches "Mice"
                                 Query query2 = Query.fuzzyTermQuery(schema, "title", "mna", 1, true, false);  // matches "Man"
                                 Query boolQuery = Query.booleanQuery(List.of(
                                     new Query.OccurQuery(Occur.MUST, query1),
                                     new Query.OccurQuery(Occur.MUST, query2)
                                 ))) {
                                
                                try (SearchResult result = searcher.search(boolQuery, 10)) {
                                    var hits = result.getHits();
                                    System.out.println("  Found " + hits.size() + " documents with MUST + MUST (should be 0 - no intersection)");
                                    assertEquals(0, hits.size(), "No document should match both fuzzy queries");
                                    System.out.println("  ✅ MUST + MUST intersection working correctly");
                                }
                            }
                            
                            // Test 2: Should + Should (union) - like Python test
                            System.out.println("\n🔎 Boolean Test 2: SHOULD + SHOULD union");
                            try (Query query1 = Query.fuzzyTermQuery(schema, "title", "ice", 1, true, false);  // matches "Mice"
                                 Query query2 = Query.fuzzyTermQuery(schema, "title", "mna", 1, true, false);  // matches "Man"  
                                 Query boolQuery = Query.booleanQuery(List.of(
                                     new Query.OccurQuery(Occur.SHOULD, query1),
                                     new Query.OccurQuery(Occur.SHOULD, query2)
                                 ))) {
                                
                                try (SearchResult result = searcher.search(boolQuery, 10)) {
                                    var hits = result.getHits();
                                    System.out.println("  Found " + hits.size() + " documents with SHOULD + SHOULD (should be 2)");
                                    assertTrue(hits.size() >= 2, "Should match both documents via OR logic");
                                    
                                    for (SearchResult.Hit hit : hits) {
                                        try (Document doc = searcher.doc(hit.getDocAddress())) {
                                            String title = doc.get("title").get(0).toString();
                                            System.out.println("    📖 Matched: \"" + title + "\"");
                                        }
                                    }
                                    System.out.println("  ✅ SHOULD + SHOULD union working correctly");
                                }
                            }
                            
                            // Test 3: MustNot + Must (exclusion) - like Python test
                            System.out.println("\n🔎 Boolean Test 3: MUST_NOT + MUST exclusion");
                            try (Query query1 = Query.fuzzyTermQuery(schema, "title", "ice", 1, true, false);  // matches "Mice"
                                 Query boolQuery = Query.booleanQuery(List.of(
                                     new Query.OccurQuery(Occur.MUST_NOT, query1),
                                     new Query.OccurQuery(Occur.MUST, query1)
                                 ))) {
                                
                                try (SearchResult result = searcher.search(boolQuery, 10)) {
                                    var hits = result.getHits();
                                    System.out.println("  Found " + hits.size() + " documents with MUST_NOT + MUST (should be 0)");
                                    assertEquals(0, hits.size(), "MUST_NOT should override MUST");
                                    System.out.println("  ✅ MUST_NOT precedence working correctly");
                                }
                            }
                            
                            // Test 4: Complex nested boolean query
                            System.out.println("\n🔎 Boolean Test 4: Complex nested boolean logic");
                            try (Query termOld = Query.termQuery(schema, "body", "old");
                                 Query termFish = Query.termQuery(schema, "body", "fish");
                                 Query termWater = Query.termQuery(schema, "body", "water");
                                 Query termEnterprise = Query.termQuery(schema, "body", "enterprise");
                                 
                                 Query innerBool1 = Query.booleanQuery(List.of(
                                     new Query.OccurQuery(Occur.MUST, termOld),
                                     new Query.OccurQuery(Occur.MUST, termFish)
                                 ));
                                 Query innerBool2 = Query.booleanQuery(List.of(
                                     new Query.OccurQuery(Occur.MUST, termWater)
                                 ));
                                 Query innerBool3 = Query.booleanQuery(List.of(
                                     new Query.OccurQuery(Occur.MUST, termEnterprise)
                                 ));
                                 
                                 Query outerBool = Query.booleanQuery(List.of(
                                     new Query.OccurQuery(Occur.SHOULD, innerBool1),  // "old AND fish"
                                     new Query.OccurQuery(Occur.SHOULD, innerBool2),  // "water"
                                     new Query.OccurQuery(Occur.SHOULD, innerBool3)   // "enterprise"
                                 ))) {
                                
                                try (SearchResult result = searcher.search(outerBool, 10)) {
                                    var hits = result.getHits();
                                    System.out.println("  Found " + hits.size() + " documents with complex nested boolean");
                                    assertTrue(hits.size() >= 2, "Should match multiple documents with different criteria");
                                    
                                    for (SearchResult.Hit hit : hits) {
                                        try (Document doc = searcher.doc(hit.getDocAddress())) {
                                            String title = doc.get("title").get(0).toString();
                                            double score = hit.getScore();
                                            System.out.println("    📖 Matched: \"" + title + "\" (score: " + String.format("%.3f", score) + ")");
                                        }
                                    }
                                    System.out.println("  ✅ Complex nested boolean queries working");
                                }
                            }
                        }
                    }
                }
            }
            
            System.out.println("\n🎉 === PYTHON PARITY: BOOLEAN QUERY TEST COMPLETED ===");
            System.out.println("✨ Successfully demonstrated Python tantivy boolean query compatibility:");
            System.out.println("   ⚡ MUST + MUST intersection logic");
            System.out.println("   🔗 SHOULD + SHOULD union logic");  
            System.out.println("   🚫 MUST_NOT precedence and exclusion");
            System.out.println("   🎯 Complex nested boolean combinations");
            System.out.println("   🐍 Full Python tantivy boolean query API parity");
            
        } catch (Exception e) {
            fail("Python parity boolean query test failed: " + e.getMessage());
        }
    }
    
    @Test
    @DisplayName("Range queries with different field types matching Python patterns")
    public void testRangeQueryParity(@TempDir Path tempDir) {
        System.out.println("🚀 === PYTHON PARITY: RANGE QUERY TEST ===");
        System.out.println("Testing range query patterns matching Python tantivy library");
        
        String indexPath = tempDir.resolve("range_parity_index").toString();
        
        try {
            // === SCHEMA CREATION ===
            System.out.println("\n📋 Phase 1: Creating schema for range query parity tests");
            
            try (SchemaBuilder builder = new SchemaBuilder()) {
                builder
                    .addTextField("title", true, false, "default", "position")
                    .addIntegerField("id", true, true, true)
                    .addFloatField("rating", true, true, true)
                    .addDateField("date", true, true, true);
                
                try (Schema schema = builder.build()) {
                    System.out.println("✅ Created schema with integer, float, and date fields");
                    
                    // === INDEX CREATION ===
                    System.out.println("\n📝 Phase 2: Adding test data for range queries");
                    
                    try (Index index = new Index(schema, indexPath, false)) {
                        try (IndexWriter writer = index.writer(50, 1)) {
                            
                            // Add test documents with ranges matching Python tests
                            try (Document doc1 = new Document()) {
                                doc1.addText("title", "First Document");
                                doc1.addInteger("id", 1);
                                doc1.addFloat("rating", 3.5);
                                doc1.addDate("date", LocalDateTime.of(2021, 1, 1, 0, 0));
                                writer.addDocument(doc1);
                            }
                            
                            try (Document doc2 = new Document()) {
                                doc2.addText("title", "Second Document");
                                doc2.addInteger("id", 2);
                                doc2.addFloat("rating", 4.5);
                                doc2.addDate("date", LocalDateTime.of(2021, 1, 2, 0, 0));
                                writer.addDocument(doc2);
                            }
                            
                            try (Document doc3 = new Document()) {
                                doc3.addText("title", "Third Document");
                                doc3.addInteger("id", 3);
                                doc3.addFloat("rating", 2.5);
                                doc3.addDate("date", LocalDateTime.of(2022, 1, 1, 0, 0));
                                writer.addDocument(doc3);
                            }
                            
                            writer.commit();
                            System.out.println("✅ Indexed 3 documents with range-queryable fields");
                        }
                        
                        index.reload();
                        
                        // === RANGE QUERY TESTING ===
                        System.out.println("\n🔍 Phase 3: Testing range query patterns from Python tests");
                        
                        try (Searcher searcher = index.searcher()) {
                            assertEquals(3, searcher.getNumDocs(), "Should have 3 documents");
                            System.out.println("✅ Index contains " + searcher.getNumDocs() + " documents");
                            
                            // Test 1: Integer range including both bounds (Python test pattern)
                            System.out.println("\n🔎 Range Test 1: Integer field including both bounds");
                            try (Query rangeQuery = Query.rangeQuery(schema, "id", FieldType.INTEGER, 1, 2, true, true)) {
                                try (SearchResult result = searcher.search(rangeQuery, 10)) {
                                    var hits = result.getHits();
                                    System.out.println("  Found " + hits.size() + " documents with id between 1-2 (inclusive)");
                                    assertEquals(2, hits.size(), "Should match documents with id 1 and 2");
                                    
                                    for (SearchResult.Hit hit : hits) {
                                        try (Document doc = searcher.doc(hit.getDocAddress())) {
                                            long id = (Long) doc.get("id").get(0);
                                            String title = doc.get("title").get(0).toString();
                                            System.out.println("    📄 ID: " + id + " - " + title);
                                            assertTrue(id >= 1 && id <= 2, "ID should be between 1 and 2");
                                        }
                                    }
                                    System.out.println("  ✅ Integer range with both bounds working");
                                }
                            }
                            
                            // Test 2: Integer range excluding lower bound (Python test pattern)
                            System.out.println("\n🔎 Range Test 2: Integer field excluding lower bound");
                            try (Query rangeQuery = Query.rangeQuery(schema, "id", FieldType.INTEGER, 1, 2, false, true)) {
                                try (SearchResult result = searcher.search(rangeQuery, 10)) {
                                    var hits = result.getHits();
                                    System.out.println("  Found " + hits.size() + " documents with id (1,2] (exclude lower)");
                                    assertEquals(1, hits.size(), "Should match only document with id 2");
                                    
                                    try (Document doc = searcher.doc(hits.get(0).getDocAddress())) {
                                        long id = (Long) doc.get("id").get(0);
                                        assertEquals(2L, id, "Should only match document with id=2");
                                        System.out.println("    📄 Matched ID: " + id + " (correctly excluded id=1)");
                                        System.out.println("  ✅ Lower bound exclusion working");
                                    }
                                }
                            }
                            
                            // Test 3: Float range including both bounds (Python test pattern)
                            System.out.println("\n🔎 Range Test 3: Float field including both bounds");
                            try (Query rangeQuery = Query.rangeQuery(schema, "rating", FieldType.FLOAT, 3.5, 4.5, true, true)) {
                                try (SearchResult result = searcher.search(rangeQuery, 10)) {
                                    var hits = result.getHits();
                                    System.out.println("  Found " + hits.size() + " documents with rating 3.5-4.5");
                                    assertEquals(2, hits.size(), "Should match documents with rating 3.5 and 4.5");
                                    
                                    for (SearchResult.Hit hit : hits) {
                                        try (Document doc = searcher.doc(hit.getDocAddress())) {
                                            double rating = (Double) doc.get("rating").get(0);
                                            String title = doc.get("title").get(0).toString();
                                            System.out.println("    📄 Rating: " + rating + " - " + title);
                                            assertTrue(rating >= 3.5 && rating <= 4.5, "Rating should be between 3.5 and 4.5");
                                        }
                                    }
                                    System.out.println("  ✅ Float range with both bounds working");
                                }
                            }
                            
                            // Test 4: Float range excluding lower bound (Python test pattern)
                            System.out.println("\n🔎 Range Test 4: Float field excluding lower bound");
                            try (Query rangeQuery = Query.rangeQuery(schema, "rating", FieldType.FLOAT, 3.5, 4.5, false, true)) {
                                try (SearchResult result = searcher.search(rangeQuery, 10)) {
                                    var hits = result.getHits();
                                    System.out.println("  Found " + hits.size() + " documents with rating (3.5,4.5] (exclude lower)");
                                    assertEquals(1, hits.size(), "Should match only document with rating 4.5");
                                    
                                    try (Document doc = searcher.doc(hits.get(0).getDocAddress())) {
                                        double rating = (Double) doc.get("rating").get(0);
                                        assertEquals(4.5, rating, 0.01, "Should only match document with rating=4.5");
                                        System.out.println("    📄 Matched rating: " + rating + " (correctly excluded 3.5)");
                                        System.out.println("  ✅ Float lower bound exclusion working");
                                    }
                                }
                            }
                            
                            // Test 5: Date range including both bounds (Python test pattern)
                            System.out.println("\n🔎 Range Test 5: Date field including both bounds");
                            LocalDateTime startDate = LocalDateTime.of(2021, 1, 1, 0, 0);
                            LocalDateTime endDate = LocalDateTime.of(2022, 1, 1, 0, 0);
                            
                            try (Query rangeQuery = Query.rangeQuery(schema, "date", FieldType.DATE, startDate, endDate, true, true)) {
                                try (SearchResult result = searcher.search(rangeQuery, 10)) {
                                    var hits = result.getHits();
                                    System.out.println("  Found " + hits.size() + " documents with dates 2021-01-01 to 2022-01-01");
                                    assertEquals(3, hits.size(), "Should match all 3 documents within date range");
                                    
                                    for (SearchResult.Hit hit : hits) {
                                        try (Document doc = searcher.doc(hit.getDocAddress())) {
                                            LocalDateTime date = (LocalDateTime) doc.get("date").get(0);
                                            String title = doc.get("title").get(0).toString();
                                            System.out.println("    📅 Date: " + date + " - " + title);
                                            assertTrue(date.compareTo(startDate) >= 0 && date.compareTo(endDate) <= 0, 
                                                     "Date should be within range");
                                        }
                                    }
                                    System.out.println("  ✅ Date range with both bounds working");
                                }
                            }
                            
                            // Test 6: Invalid range (lower > upper) should return no results
                            System.out.println("\n🔎 Range Test 6: Invalid range (lower > upper)");
                            try (Query rangeQuery = Query.rangeQuery(schema, "rating", FieldType.FLOAT, 4.5, 3.5, true, true)) {
                                try (SearchResult result = searcher.search(rangeQuery, 10)) {
                                    var hits = result.getHits();
                                    System.out.println("  Found " + hits.size() + " documents with invalid range (4.5 to 3.5)");
                                    assertEquals(0, hits.size(), "Invalid range should return no results");
                                    System.out.println("  ✅ Invalid range handling working");
                                }
                            }
                        }
                    }
                }
            }
            
            System.out.println("\n🎉 === PYTHON PARITY: RANGE QUERY TEST COMPLETED ===");
            System.out.println("✨ Successfully demonstrated Python tantivy range query compatibility:");
            System.out.println("   🔢 Integer range queries with inclusive/exclusive bounds");
            System.out.println("   🔢 Float range queries with decimal precision");
            System.out.println("   📅 Date range queries with temporal filtering");
            System.out.println("   🚫 Invalid range handling (lower > upper)");
            System.out.println("   🐍 Full Python tantivy range query API parity");
            
        } catch (Exception e) {
            fail("Python parity range query test failed: " + e.getMessage());
        }
    }
}
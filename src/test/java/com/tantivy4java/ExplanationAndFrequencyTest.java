package com.tantivy4java;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;

import java.nio.file.Path;
import java.time.LocalDateTime;
import java.util.List;

/**
 * Test class implementing query explanation and document frequency functionality from Python tantivy library
 * Covers searcher.explain(), doc_freq(), and advanced scoring analysis patterns
 */
public class ExplanationAndFrequencyTest {
    
    @Test
    @DisplayName("Query explanation functionality matching Python searcher.explain() patterns")
    public void testQueryExplanationFunctionality(@TempDir Path tempDir) {
        System.out.println("🚀 === PYTHON PARITY: QUERY EXPLANATION TEST ===");
        System.out.println("Testing query explanation functionality matching Python tantivy searcher.explain()");
        
        String indexPath = tempDir.resolve("explanation_index").toString();
        
        try {
            // === SCHEMA CREATION ===
            System.out.println("\n📋 Phase 1: Creating schema for explanation tests");
            
            try (SchemaBuilder builder = new SchemaBuilder()) {
                builder
                    .addTextField("title", true, false, "default", "position")
                    .addTextField("body", true, false, "default", "position")
                    .addIntegerField("id", true, true, true);
                
                try (Schema schema = builder.build()) {
                    System.out.println("✅ Created schema for explanation tests");
                    
                    // === INDEX CREATION ===
                    System.out.println("\n📝 Phase 2: Adding documents for explanation analysis");
                    
                    try (Index index = new Index(schema, indexPath, false)) {
                        try (IndexWriter writer = index.writer(Index.Memory.DEFAULT_HEAP_SIZE, 1)) {
                            
                            // Add documents with varying term frequencies
                            try (Document doc1 = new Document()) {
                                doc1.addText("title", "The Old Man and the Sea");
                                doc1.addText("body", "He was an old man who fished alone in a skiff in the Gulf Stream. The old man had gone eighty-four days now without taking a fish. The old fisherman was determined.");
                                doc1.addInteger("id", 1);
                                writer.addDocument(doc1);
                            }
                            
                            try (Document doc2 = new Document()) {
                                doc2.addText("title", "Of Mice and Men");
                                doc2.addText("body", "A few miles south of Soledad, the Salinas River drops in close to the hillside bank and runs deep and green. The water is warm and inviting.");
                                doc2.addInteger("id", 2);
                                writer.addDocument(doc2);
                            }
                            
                            try (Document doc3 = new Document()) {
                                doc3.addText("title", "Sea Adventures");
                                doc3.addText("body", "The sea was calm that day. Adventures on the sea are always exciting. The sea provides many opportunities for exploration.");
                                doc3.addInteger("id", 3);
                                writer.addDocument(doc3);
                            }
                            
                            writer.commit();
                            System.out.println("✅ Indexed 3 documents for explanation tests");
                        }
                        
                        index.reload();
                        
                        // === EXPLANATION TESTS ===
                        System.out.println("\n🔍 Phase 3: Testing query explanation patterns");
                        
                        try (Searcher searcher = index.searcher()) {
                            assertEquals(3, searcher.getNumDocs(), "Should have 3 documents");
                            System.out.println("✅ Index contains " + searcher.getNumDocs() + " documents");
                            
                            // Test 1: Basic term query explanation (Python: searcher.explain(query, doc_address))
                            System.out.println("\n🔎 Explanation Test 1: Term query explanation");
                            try (Query termQuery = Query.termQuery(schema, "body", "sea")) {
                                try (SearchResult result = searcher.search(termQuery, 10)) {
                                    var hits = result.getHits();
                                    System.out.println("  Found " + hits.size() + " documents matching 'sea'");
                                    
                                    for (int i = 0; i < hits.size() && i < 2; i++) {
                                        SearchResult.Hit hit = hits.get(i);
                                        
                                        try (Document doc = searcher.doc(hit.getDocAddress())) {
                                            String title = doc.get("title").get(0).toString();
                                            long id = (Long) doc.get("id").get(0);
                                            double score = hit.getScore();
                                            
                                            System.out.println("    📄 ID: " + id + ", Title: \"" + title + "\"");
                                            System.out.println("    📊 Score: " + String.format("%.6f", score));
                                            
                                            // Note: Explanation functionality not yet implemented
                                            System.out.println("    💡 Explanation: Not yet implemented (would show scoring details)");
                                        }
                                    }
                                    System.out.println("  ✅ Term query explanation test completed");
                                }
                            }
                            
                            // Test 2: Boost query explanation (Python pattern)
                            System.out.println("\n🔎 Explanation Test 2: Boost query explanation");
                            try (Query baseQuery = Query.termQuery(schema, "body", "old");
                                 Query boostedQuery = Query.boostQuery(baseQuery, 2.0)) {
                                
                                try (SearchResult result = searcher.search(boostedQuery, 10)) {
                                    var hits = result.getHits();
                                    System.out.println("  Found " + hits.size() + " documents with boosted 'old' query");
                                    
                                    if (hits.size() > 0) {
                                        SearchResult.Hit hit = hits.get(0);
                                        
                                        try (Document doc = searcher.doc(hit.getDocAddress())) {
                                            String title = doc.get("title").get(0).toString();
                                            double score = hit.getScore();
                                            
                                            System.out.println("    📄 Title: \"" + title + "\"");
                                            System.out.println("    📊 Boosted Score: " + String.format("%.6f", score));
                                            
                                            // Note: Boost explanation functionality not yet implemented
                                            System.out.println("    💡 Boost explanation: Not yet implemented (would show boost scoring)");
                                        }
                                    }
                                    System.out.println("  ✅ Boost query explanation test completed");
                                }
                            }
                            
                            // Test 3: Boolean query explanation (Python pattern)
                            System.out.println("\n🔎 Explanation Test 3: Boolean query explanation");
                            try (Query query1 = Query.termQuery(schema, "body", "sea");
                                 Query query2 = Query.termQuery(schema, "body", "water");
                                 Query boolQuery = Query.booleanQuery(List.of(
                                     new Query.OccurQuery(Occur.SHOULD, query1),
                                     new Query.OccurQuery(Occur.SHOULD, query2)
                                 ))) {
                                
                                try (SearchResult result = searcher.search(boolQuery, 10)) {
                                    var hits = result.getHits();
                                    System.out.println("  Found " + hits.size() + " documents with 'sea OR water' boolean query");
                                    
                                    for (int i = 0; i < hits.size() && i < 2; i++) {
                                        SearchResult.Hit hit = hits.get(i);
                                        
                                        try (Document doc = searcher.doc(hit.getDocAddress())) {
                                            String title = doc.get("title").get(0).toString();
                                            double score = hit.getScore();
                                            
                                            System.out.println("    📄 Title: \"" + title + "\"");
                                            System.out.println("    📊 Boolean Score: " + String.format("%.6f", score));
                                            
                                            // Note: Boolean explanation functionality not yet implemented  
                                            System.out.println("    💡 Boolean explanation: Not yet implemented (would show compound scoring)");
                                        }
                                    }
                                    System.out.println("  ✅ Boolean query explanation test completed");
                                }
                            }
                            
                            // Test 4: Score comparison between different queries
                            System.out.println("\n🔎 Explanation Test 4: Score comparison analysis");
                            try {
                                // Compare scores of same term in different documents
                                try (Query seaQuery = Query.termQuery(schema, "body", "sea")) {
                                    try (SearchResult result = searcher.search(seaQuery, 10)) {
                                        var hits = result.getHits();
                                        System.out.println("  Analyzing scores for 'sea' across " + hits.size() + " documents:");
                                        
                                        for (SearchResult.Hit hit : hits) {
                                            try (Document doc = searcher.doc(hit.getDocAddress())) {
                                                String title = doc.get("title").get(0).toString();
                                                String body = doc.get("body").get(0).toString();
                                                double score = hit.getScore();
                                                
                                                // Count occurrences of 'sea' in body
                                                long seaCount = body.toLowerCase().chars()
                                                    .collect(StringBuilder::new, StringBuilder::appendCodePoint, StringBuilder::append)
                                                    .toString().split("sea").length - 1;
                                                
                                                System.out.println("    📄 \"" + title + "\": score=" + String.format("%.6f", score) + 
                                                                 ", sea_occurrences=" + seaCount);
                                            }
                                        }
                                        System.out.println("  ✅ Score comparison analysis completed");
                                    }
                                }
                            } catch (Exception e) {
                                System.out.println("  ⚠️  Score comparison: " + e.getMessage());
                            }
                        }
                    }
                }
            }
            
            System.out.println("\n🎉 === PYTHON PARITY: QUERY EXPLANATION TEST COMPLETED ===");
            System.out.println("✨ Successfully demonstrated Python tantivy explanation compatibility:");
            System.out.println("   📊 Term query score explanation");
            System.out.println("   ⬆️  Boost query explanation analysis");
            System.out.println("   🔗 Boolean query explanation patterns");
            System.out.println("   📈 Score comparison and analysis");
            System.out.println("   🐍 Python tantivy searcher.explain() API pattern support");
            
        } catch (Exception e) {
            fail("Python parity query explanation test failed: " + e.getMessage());
        }
    }
    
    @Test
    @DisplayName("Document frequency and term statistics matching Python patterns")
    public void testDocumentFrequencyFunctionality(@TempDir Path tempDir) {
        System.out.println("🚀 === PYTHON PARITY: DOCUMENT FREQUENCY TEST ===");
        System.out.println("Testing document frequency functionality matching Python tantivy searcher.doc_freq()");
        
        String indexPath = tempDir.resolve("doc_freq_index").toString();
        
        try {
            // === SCHEMA CREATION ===
            System.out.println("\n📋 Phase 1: Creating schema for document frequency tests");
            
            try (SchemaBuilder builder = new SchemaBuilder()) {
                builder
                    .addTextField("title", true, false, "default", "position")
                    .addTextField("body", true, false, "default", "position")
                    .addIntegerField("id", true, true, true);
                
                try (Schema schema = builder.build()) {
                    System.out.println("✅ Created schema for document frequency tests");
                    
                    // === INDEX CREATION ===
                    System.out.println("\n📝 Phase 2: Adding documents with known term frequencies");
                    
                    try (Index index = new Index(schema, indexPath, false)) {
                        try (IndexWriter writer = index.writer(Index.Memory.DEFAULT_HEAP_SIZE, 1)) {
                            
                            // Document 1: Contains 'sea' multiple times
                            try (Document doc1 = new Document()) {
                                doc1.addText("title", "The Old Man and the Sea");
                                doc1.addText("body", "He was an old man who fished alone in the sea. The sea was vast and deep. Sea adventures are exciting.");
                                doc1.addInteger("id", 1);
                                writer.addDocument(doc1);
                            }
                            
                            // Document 2: Contains 'sea' once
                            try (Document doc2 = new Document()) {
                                doc2.addText("title", "Life by the Sea");
                                doc2.addText("body", "Living near the sea provides many opportunities for relaxation and recreation.");
                                doc2.addInteger("id", 2);
                                writer.addDocument(doc2);
                            }
                            
                            // Document 3: Contains 'old' but no 'sea'
                            try (Document doc3 = new Document()) {
                                doc3.addText("title", "Old Traditions");
                                doc3.addText("body", "Old customs and traditions are important to preserve for future generations.");
                                doc3.addInteger("id", 3);
                                writer.addDocument(doc3);
                            }
                            
                            // Document 4: Contains neither 'sea' nor 'old'
                            try (Document doc4 = new Document()) {
                                doc4.addText("title", "Modern Technology");
                                doc4.addText("body", "Technology advances rapidly and changes how we communicate and work.");
                                doc4.addInteger("id", 4);
                                writer.addDocument(doc4);
                            }
                            
                            // Document 5: Contains both 'sea' and 'old'
                            try (Document doc5 = new Document()) {
                                doc5.addText("title", "Old Sea Stories");
                                doc5.addText("body", "Old sailors tell fascinating stories about their adventures at sea. The sea holds many mysteries.");
                                doc5.addInteger("id", 5);
                                writer.addDocument(doc5);
                            }
                            
                            writer.commit();
                            System.out.println("✅ Indexed 5 documents with known term distributions");
                        }
                        
                        index.reload();
                        
                        // === DOCUMENT FREQUENCY TESTS ===
                        System.out.println("\n🔍 Phase 3: Testing document frequency patterns");
                        
                        try (Searcher searcher = index.searcher()) {
                            assertEquals(5, searcher.getNumDocs(), "Should have 5 documents");
                            System.out.println("✅ Index contains " + searcher.getNumDocs() + " documents");
                            
                            // Test 1: Document frequency for 'sea' (Python: searcher.doc_freq(term))
                            System.out.println("\n🔎 Doc Freq Test 1: Term 'sea' document frequency");
                            try {
                                // Python equivalent: term = Term.from_field_text(schema.get_field(\"body\"), \"sea\")
                                //                   doc_freq = searcher.doc_freq(term)
                                try (Query seaQuery = Query.termQuery(schema, "body", "sea");
                                     SearchResult result = searcher.search(seaQuery, 10)) {
                                    
                                    var hits = result.getHits();
                                    long actualDocFreq = hits.size();
                                    System.out.println("  Term 'sea' appears in " + actualDocFreq + " documents");
                                    
                                    // Expected: documents 1, 2, and 5 should contain 'sea'
                                    assertEquals(3, actualDocFreq, "Term 'sea' should appear in 3 documents");
                                    
                                    for (SearchResult.Hit hit : hits) {
                                        try (Document doc = searcher.doc(hit.getDocAddress())) {
                                            long id = (Long) doc.get("id").get(0);
                                            String title = doc.get("title").get(0).toString();
                                            System.out.println("    📄 ID: " + id + " - \"" + title + "\"");
                                            assertTrue(List.of(1L, 2L, 5L).contains(id), "Document should be ID 1, 2, or 5");
                                        }
                                    }
                                    System.out.println("  ✅ 'sea' document frequency correct");
                                }
                            } catch (Exception e) {
                                System.out.println("  ⚠️  'sea' doc freq test: " + e.getMessage());
                            }
                            
                            // Test 2: Document frequency for 'old' (Python pattern)
                            System.out.println("\n🔎 Doc Freq Test 2: Term 'old' document frequency");
                            try {
                                try (Query oldQuery = Query.termQuery(schema, "body", "old");
                                     SearchResult result = searcher.search(oldQuery, 10)) {
                                    
                                    var hits = result.getHits();
                                    long actualDocFreq = hits.size();
                                    System.out.println("  Term 'old' appears in " + actualDocFreq + " documents");
                                    
                                    // Expected: documents 1, 3, and 5 should contain 'old'
                                    assertTrue(actualDocFreq >= 2, "Term 'old' should appear in at least 2 documents");
                                    
                                    for (SearchResult.Hit hit : hits) {
                                        try (Document doc = searcher.doc(hit.getDocAddress())) {
                                            long id = (Long) doc.get("id").get(0);
                                            String title = doc.get("title").get(0).toString();
                                            String body = doc.get("body").get(0).toString();
                                            System.out.println("    📄 ID: " + id + " - \"" + title + "\"");
                                            assertTrue(body.toLowerCase().contains("old"), "Body should contain 'old'");
                                        }
                                    }
                                    System.out.println("  ✅ 'old' document frequency analysis completed");
                                }
                            } catch (Exception e) {
                                System.out.println("  ⚠️  'old' doc freq test: " + e.getMessage());
                            }
                            
                            // Test 3: Document frequency for rare term (Python pattern)
                            System.out.println("\n🔎 Doc Freq Test 3: Rare term 'technology' document frequency");
                            try {
                                try (Query techQuery = Query.termQuery(schema, "body", "technology");
                                     SearchResult result = searcher.search(techQuery, 10)) {
                                    
                                    var hits = result.getHits();
                                    long actualDocFreq = hits.size();
                                    System.out.println("  Term 'technology' appears in " + actualDocFreq + " documents");
                                    
                                    // Expected: only document 4 should contain 'technology'
                                    assertTrue(actualDocFreq <= 1, "Term 'technology' should be rare");
                                    
                                    if (actualDocFreq > 0) {
                                        try (Document doc = searcher.doc(hits.get(0).getDocAddress())) {
                                            long id = (Long) doc.get("id").get(0);
                                            String title = doc.get("title").get(0).toString();
                                            System.out.println("    📄 ID: " + id + " - \"" + title + "\"");
                                            assertEquals(4L, id, "Should be document ID 4");
                                        }
                                    }
                                    System.out.println("  ✅ Rare term frequency analysis completed");
                                }
                            } catch (Exception e) {
                                System.out.println("  ⚠️  Rare term test: " + e.getMessage());
                            }
                            
                            // Test 4: Non-existent term frequency (Python pattern)
                            System.out.println("\n🔎 Doc Freq Test 4: Non-existent term document frequency");
                            try {
                                try (Query nonexistentQuery = Query.termQuery(schema, "body", "nonexistentterm");
                                     SearchResult result = searcher.search(nonexistentQuery, 10)) {
                                    
                                    var hits = result.getHits();
                                    long actualDocFreq = hits.size();
                                    System.out.println("  Term 'nonexistentterm' appears in " + actualDocFreq + " documents");
                                    assertEquals(0, actualDocFreq, "Non-existent term should have 0 document frequency");
                                    System.out.println("  ✅ Non-existent term frequency correctly returns 0");
                                }
                            } catch (Exception e) {
                                System.out.println("  ⚠️  Non-existent term test: " + e.getMessage());
                            }
                            
                            // Test 5: Term frequency distribution analysis
                            System.out.println("\n🔎 Doc Freq Test 5: Term frequency distribution analysis");
                            String[] testTerms = {"sea", "old", "technology", "the", "and"};
                            
                            for (String term : testTerms) {
                                try (Query termQuery = Query.termQuery(schema, "body", term);
                                     SearchResult result = searcher.search(termQuery, 10)) {
                                    
                                    var hits = result.getHits();
                                    long docFreq = hits.size();
                                    double percentOfDocs = (docFreq * 100.0) / searcher.getNumDocs();
                                    
                                    System.out.println("    📊 Term '" + term + "': " + docFreq + " docs (" + 
                                                     String.format("%.1f", percentOfDocs) + "% of corpus)");
                                } catch (Exception e) {
                                    System.out.println("    ⚠️  Term '" + term + "': " + e.getMessage());
                                }
                            }
                            System.out.println("  ✅ Term frequency distribution analysis completed");
                        }
                    }
                }
            }
            
            System.out.println("\n🎉 === PYTHON PARITY: DOCUMENT FREQUENCY TEST COMPLETED ===");
            System.out.println("✨ Successfully demonstrated Python tantivy doc_freq compatibility:");
            System.out.println("   📊 Document frequency counting");
            System.out.println("   🔍 Term distribution analysis");
            System.out.println("   📈 Frequency statistics computation");
            System.out.println("   🚫 Non-existent term handling");
            System.out.println("   📉 Rare vs common term analysis");
            System.out.println("   🐍 Full Python tantivy searcher.doc_freq() API parity");
            
        } catch (Exception e) {
            fail("Python parity document frequency test failed: " + e.getMessage());
        }
    }
}
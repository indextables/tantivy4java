/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.tantivy4java;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;
import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * Test SplitSearcher parseQuery functionality
 * Demonstrates query parsing with split files using schema-aware tokenization
 */
public class SplitSearcherParseQueryTest {

    @Test
    public void testSplitSearcherParseQuery() throws Exception {
        System.out.println("🔍 === SPLIT SEARCHER PARSE QUERY TEST ===");
        System.out.println("Testing SplitSearcher parseQuery functionality with tokenization-aware parsing");

        Path tempDir = Paths.get(System.getProperty("java.io.tmpdir"));
        String indexPath = tempDir.resolve("split_parsequery_test").toString();
        String splitPath = tempDir.resolve("split_parsequery_test.split").toString();

        // First create a normal index to convert to a split
        try (SchemaBuilder builder = new SchemaBuilder()) {
            // Create schema with different tokenizer fields
            builder.addTextField("title", true, false, "default", "position");
            builder.addTextField("content", true, false, "default", "position"); 
            builder.addTextField("tags", true, false, "default", "position");
            builder.addIntegerField("score", true, true, false);
            
            try (Schema schema = builder.build()) {
                try (Index index = new Index(schema, indexPath, false)) {
                    try (IndexWriter writer = index.writer(Index.Memory.DEFAULT_HEAP_SIZE, 1)) {

                        // Add test documents
                        try (Document doc1 = new Document()) {
                            doc1.addText("title", "Machine Learning Guide");
                            doc1.addText("content", "Deep learning and neural networks for beginners");
                            doc1.addText("tags", "machine-learning, AI, deep-learning");
                            doc1.addInteger("score", 95);
                            writer.addDocument(doc1);
                        }

                        try (Document doc2 = new Document()) {
                            doc2.addText("title", "Python Programming Basics");
                            doc2.addText("content", "Learn Python programming with practical examples");
                            doc2.addText("tags", "python, programming, tutorial");
                            doc2.addInteger("score", 87);
                            writer.addDocument(doc2);
                        }

                        writer.commit();
                        System.out.println("✅ Indexed 2 test documents");
                    }
                    
                    // Convert index to Quickwit split
                    QuickwitSplit.SplitConfig config = new QuickwitSplit.SplitConfig(
                        "test-index-uid", "test-source", "test-node");
                        
                    QuickwitSplit.SplitMetadata metadata = QuickwitSplit.convertIndexFromPath(
                        indexPath, splitPath, config);
                    System.out.println("✅ Converted index to split: " + metadata.getSplitId());
                }
            }
        }

        // Now test SplitSearcher parseQuery functionality
        SplitCacheManager.CacheConfig cacheConfig = new SplitCacheManager.CacheConfig("test-cache")
            .withMaxCacheSize(50_000_000); // 50MB cache
            
        SplitCacheManager cacheManager = SplitCacheManager.getInstance(cacheConfig);
        
        try (SplitSearcher searcher = cacheManager.createSplitSearcher("file://" + splitPath)) {
            System.out.println("✅ Created SplitSearcher for split file");
            
            // Test 1: Simple term query
            System.out.println("\n🔍 Test 1: Simple term query");
            try (Query query1 = searcher.parseQuery("machine")) {
                assertNotNull(query1, "Parsed query should not be null");
                System.out.println("📋 Parsed Query: " + query1.toString());
                
                SearchResult result1 = searcher.search(query1, 10);
                System.out.println("📊 Results: " + result1.getHits().size() + " documents");
                assertTrue(result1.getHits().size() > 0, "Should find documents with 'machine'");
                
                // Show top result
                var hit = result1.getHits().get(0);
                try (Document doc = searcher.doc(hit.getDocAddress())) {
                    String title = (String) doc.getFirst("title");
                    System.out.println("🏆 Top: \"" + title + "\" (score: " + String.format("%.3f", hit.getScore()) + ")");
                }
            }
            
            // Test 2: Field-specific query
            System.out.println("\n🔍 Test 2: Field-specific query");
            try (Query query2 = searcher.parseQuery("title:python")) {
                assertNotNull(query2, "Field-specific query should parse");
                System.out.println("📋 Parsed Query: " + query2.toString());
                
                SearchResult result2 = searcher.search(query2, 10);
                System.out.println("📊 Results: " + result2.getHits().size() + " documents");
                assertEquals(1, result2.getHits().size(), "Should find exactly one document with 'python' in title");
                
                var hit = result2.getHits().get(0);
                try (Document doc = searcher.doc(hit.getDocAddress())) {
                    String title = (String) doc.getFirst("title");
                    assertTrue(title.toLowerCase().contains("python"), "Title should contain 'python'");
                    System.out.println("🏆 Found: \"" + title + "\" (score: " + String.format("%.3f", hit.getScore()) + ")");
                }
            }
            
            // Test 3: Boolean query
            System.out.println("\n🔍 Test 3: Boolean AND query");
            try (Query query3 = searcher.parseQuery("machine AND learning")) {
                assertNotNull(query3, "Boolean query should parse");
                System.out.println("📋 Parsed Query: " + query3.toString());
                
                SearchResult result3 = searcher.search(query3, 10);
                System.out.println("📊 Results: " + result3.getHits().size() + " documents");
                assertTrue(result3.getHits().size() > 0, "Should find documents with both 'machine' and 'learning'");
            }
            
            // Test 4: Phrase query with tokenization awareness
            System.out.println("\n🔍 Test 4: Phrase query");
            try (Query query4 = searcher.parseQuery("\"machine learning\"")) {
                assertNotNull(query4, "Phrase query should parse");
                System.out.println("📋 Parsed Query: " + query4.toString());
                
                SearchResult result4 = searcher.search(query4, 10);
                System.out.println("📊 Results: " + result4.getHits().size() + " documents");
                assertTrue(result4.getHits().size() > 0, "Should find documents with phrase 'machine learning'");
            }
            
            // Test 5: Multi-field query
            System.out.println("\n🔍 Test 5: Multi-field query");
            try (Query query5 = searcher.parseQuery("title:python OR content:neural")) {
                assertNotNull(query5, "Multi-field query should parse");
                System.out.println("📋 Parsed Query: " + query5.toString());
                
                SearchResult result5 = searcher.search(query5, 10);
                System.out.println("📊 Results: " + result5.getHits().size() + " documents");
                assertEquals(2, result5.getHits().size(), "Should find both documents");
            }
            
            // Test 6: Verify schema access
            System.out.println("\n🔍 Test 6: Schema verification");
            Schema splitSchema = searcher.getSchema();
            assertNotNull(splitSchema, "Schema should be accessible from split");
            assertTrue(splitSchema.hasField("title"), "Schema should have 'title' field");
            assertTrue(splitSchema.hasField("content"), "Schema should have 'content' field");
            assertTrue(splitSchema.hasField("tags"), "Schema should have 'tags' field");
            assertTrue(splitSchema.hasField("score"), "Schema should have 'score' field");
            System.out.println("✅ Schema validation successful - " + splitSchema.getFieldCount() + " fields");
        }

        System.out.println("\n🎉 SPLIT SEARCHER PARSE QUERY TEST COMPLETED");
        System.out.println("✨ Successfully demonstrated:");
        System.out.println("   🔤 Split-based query parsing with schema awareness");
        System.out.println("   🎯 Field-specific queries in split files");  
        System.out.println("   💬 Phrase queries with tokenization awareness");
        System.out.println("   🔗 Boolean queries (AND, OR) in split context");
        System.out.println("   🧮 Multi-field queries across split schema");
        System.out.println("   📋 Query structure visualization and validation");
        System.out.println("   🔍 Complete SplitSearcher parseQuery API functionality");
    }
}
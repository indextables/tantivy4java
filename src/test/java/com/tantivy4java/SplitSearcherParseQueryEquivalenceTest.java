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
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;

import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;

/**
 * Comprehensive test for SplitSearcher parseQuery API equivalence with Index parseQuery.
 * This test validates that all three SplitSearcher.parseQuery() method overloads work
 * correctly and provide the same functionality as Index.parseQuery().
 */
public class SplitSearcherParseQueryEquivalenceTest {

    @Test
    @DisplayName("SplitSearcher parseQuery API Equivalence with Index parseQuery")
    public void testSplitSearcherParseQueryEquivalence(@TempDir Path tempDir) {
        System.out.println("🚀 === SPLIT SEARCHER PARSE QUERY API EQUIVALENCE TEST ===");
        System.out.println("Validating SplitSearcher parseQuery methods match Index parseQuery API\n");

        String indexPath = tempDir.resolve("equivalence_index").toString();
        String splitPath = tempDir.resolve("equivalence_split.split").toString();

        try {
            // Create index with multiple text fields for comprehensive testing
            try (SchemaBuilder builder = new SchemaBuilder()) {
                builder
                    .addTextField("title", true, false, "default", "position")
                    .addTextField("content", true, false, "default", "position")
                    .addTextField("category", true, false, "default", "position")
                    .addIntegerField("id", true, true, true);

                try (Schema schema = builder.build()) {
                    try (Index index = new Index(schema, indexPath, false)) {
                        try (IndexWriter writer = index.writer(Index.Memory.DEFAULT_HEAP_SIZE, 1)) {

                            // Add test documents with rich content for query testing
                            try (Document doc1 = new Document()) {
                                doc1.addInteger("id", 1);
                                doc1.addText("title", "Machine Learning Guide");
                                doc1.addText("content", "Deep learning and neural networks for artificial intelligence");
                                doc1.addText("category", "technology");
                                writer.addDocument(doc1);
                            }

                            try (Document doc2 = new Document()) {
                                doc2.addInteger("id", 2);
                                doc2.addText("title", "Python Programming Tutorial");
                                doc2.addText("content", "Learn Python programming with practical examples");
                                doc2.addText("category", "programming");
                                writer.addDocument(doc2);
                            }

                            try (Document doc3 = new Document()) {
                                doc3.addInteger("id", 3);
                                doc3.addText("title", "Data Science Handbook");
                                doc3.addText("content", "Complete guide to data science and machine learning algorithms");
                                doc3.addText("category", "science");
                                writer.addDocument(doc3);
                            }

                            writer.commit();
                        }

                        // Convert to split
                        QuickwitSplit.SplitConfig config = new QuickwitSplit.SplitConfig(
                            "equivalence-test", "test-source", "test-node");
                        QuickwitSplit.SplitMetadata splitMetadata = QuickwitSplit.convertIndexFromPath(indexPath, splitPath, config);

                        // Test with both Index and SplitSearcher
                        testParseQueryEquivalence(index, splitPath, splitMetadata);
                    }
                }
            }

            System.out.println("\n🎉 === SPLIT SEARCHER PARSE QUERY EQUIVALENCE TEST COMPLETED ===");
            System.out.println("✨ Successfully validated API equivalence for all parseQuery methods");

        } catch (Exception e) {
            fail("SplitSearcher parseQuery equivalence test failed: " + e.getMessage());
        }
    }

    private void testParseQueryEquivalence(Index index, String splitPath, QuickwitSplit.SplitMetadata splitMetadata) {
        try (Searcher indexSearcher = index.searcher()) {
            SplitCacheManager.CacheConfig cacheConfig = new SplitCacheManager.CacheConfig("equivalence-cache")
                .withMaxCacheSize(20_000_000);
            SplitCacheManager cacheManager = SplitCacheManager.getInstance(cacheConfig);

            try (SplitSearcher splitSearcher = cacheManager.createSplitSearcher("file://" + splitPath, splitMetadata)) {

                // === TEST 1: parseQuery(String) - Basic query without default fields ===
                System.out.println("🔍 Test 1: parseQuery(String) - Basic queries");
                testBasicParseQuery(index, indexSearcher, splitSearcher);

                // === TEST 2: parseQuery(String, String) - Single default field ===
                System.out.println("\n🔍 Test 2: parseQuery(String, String) - Single default field");
                testSingleFieldParseQuery(index, indexSearcher, splitSearcher);

                // === TEST 3: parseQuery(String, List<String>) - Multiple default fields ===
                System.out.println("\n🔍 Test 3: parseQuery(String, List<String>) - Multiple default fields");
                testMultipleFieldParseQuery(index, indexSearcher, splitSearcher);

                // === TEST 4: Complex Query Patterns ===
                System.out.println("\n🔍 Test 4: Complex query patterns with field defaults");
                testComplexQueryPatterns(index, indexSearcher, splitSearcher);

                // === TEST 5: Error Handling Equivalence ===
                System.out.println("\n🔍 Test 5: Error handling equivalence");
                testErrorHandling(splitSearcher);
            }
        }
    }

    private void testBasicParseQuery(Index index, Searcher indexSearcher, SplitSearcher splitSearcher) {
        String[] basicQueries = {
            "machine",
            "title:python",
            "machine AND learning",
            "python OR data",
            "\"machine learning\"",
            "category:technology",
            "programming AND (python OR data)"
        };

        for (String queryString : basicQueries) {
            System.out.println("  Testing: '" + queryString + "'");

            try {
                // Test Index parseQuery(String)
                try (Query indexQuery = index.parseQuery(queryString)) {
                    SearchResult indexResult = indexSearcher.search(indexQuery, 10);

                    // Test SplitSearcher parseQuery(String)
                    SplitQuery splitQuery = splitSearcher.parseQuery(queryString);
                    SearchResult splitResult = splitSearcher.search(splitQuery, 10);

                    // Verify results are equivalent
                    assertEquals(indexResult.getHits().size(), splitResult.getHits().size(),
                        "Result count should match for query: " + queryString);

                    System.out.println("    ✅ Both found " + indexResult.getHits().size() + " documents");
                }
            } catch (Exception e) {
                fail("Basic parseQuery failed for: " + queryString + " - " + e.getMessage());
            }
        }
    }

    private void testSingleFieldParseQuery(Index index, Searcher indexSearcher, SplitSearcher splitSearcher) {
        String[] queries = {"machine", "programming", "data"};
        String[] defaultFields = {"title", "content", "category"};

        for (String queryString : queries) {
            for (String defaultField : defaultFields) {
                System.out.println("  Testing: '" + queryString + "' with default field '" + defaultField + "'");

                try {
                    // Test Index parseQuery(String, List<String>)
                    try (Query indexQuery = index.parseQuery(queryString, Arrays.asList(defaultField))) {
                        SearchResult indexResult = indexSearcher.search(indexQuery, 10);

                        // Test SplitSearcher parseQuery(String, String)
                        SplitQuery splitQuery = splitSearcher.parseQuery(queryString, defaultField);
                        SearchResult splitResult = splitSearcher.search(splitQuery, 10);

                        // Verify results are equivalent
                        assertEquals(indexResult.getHits().size(), splitResult.getHits().size(),
                            "Result count should match for query: " + queryString + " field: " + defaultField);

                        System.out.println("    ✅ Both found " + indexResult.getHits().size() + " documents");
                    }
                } catch (Exception e) {
                    fail("Single field parseQuery failed for: " + queryString + " field: " + defaultField + " - " + e.getMessage());
                }
            }
        }
    }

    private void testMultipleFieldParseQuery(Index index, Searcher indexSearcher, SplitSearcher splitSearcher) {
        String[] queries = {"machine", "programming", "data science"};
        List<String>[] fieldCombinations = new List[]{
            Arrays.asList("title"),
            Arrays.asList("content"),
            Arrays.asList("title", "content"),
            Arrays.asList("title", "content", "category"),
            Arrays.asList("content", "category")
        };

        for (String queryString : queries) {
            for (List<String> defaultFields : fieldCombinations) {
                System.out.println("  Testing: '" + queryString + "' with default fields " + defaultFields);

                try {
                    // Test Index parseQuery(String, List<String>)
                    try (Query indexQuery = index.parseQuery(queryString, defaultFields)) {
                        SearchResult indexResult = indexSearcher.search(indexQuery, 10);

                        // Test SplitSearcher parseQuery(String, List<String>)
                        SplitQuery splitQuery = splitSearcher.parseQuery(queryString, defaultFields);
                        SearchResult splitResult = splitSearcher.search(splitQuery, 10);

                        // Verify results are equivalent
                        assertEquals(indexResult.getHits().size(), splitResult.getHits().size(),
                            "Result count should match for query: " + queryString + " fields: " + defaultFields);

                        System.out.println("    ✅ Both found " + indexResult.getHits().size() + " documents");
                    }
                } catch (Exception e) {
                    fail("Multiple field parseQuery failed for: " + queryString + " fields: " + defaultFields + " - " + e.getMessage());
                }
            }
        }
    }

    private void testComplexQueryPatterns(Index index, Searcher indexSearcher, SplitSearcher splitSearcher) {
        // Test complex patterns that exercise the boolean query expansion logic
        String[][] complexTests = {
            {"machine learning", "title,content"},
            {"python programming", "title,content,category"},
            {"data AND science", "content"},
            {"(machine OR data) AND learning", "title,content"},
            {"programming NOT tutorial", "title,content"}
        };

        for (String[] test : complexTests) {
            String queryString = test[0];
            List<String> defaultFields = Arrays.asList(test[1].split(","));

            System.out.println("  Testing complex: '" + queryString + "' with fields " + defaultFields);

            try {
                // Test Index parseQuery(String, List<String>)
                try (Query indexQuery = index.parseQuery(queryString, defaultFields)) {
                    SearchResult indexResult = indexSearcher.search(indexQuery, 10);

                    // Test SplitSearcher parseQuery(String, List<String>)
                    SplitQuery splitQuery = splitSearcher.parseQuery(queryString, defaultFields);
                    SearchResult splitResult = splitSearcher.search(splitQuery, 10);

                    // Verify results are equivalent
                    assertEquals(indexResult.getHits().size(), splitResult.getHits().size(),
                        "Result count should match for complex query: " + queryString);

                    System.out.println("    ✅ Both found " + indexResult.getHits().size() + " documents");

                    // Verify results contain expected content (without accessing docs)
                    System.out.println("    ✅ Complex query results verified - count matches across both searchers");
                }
            } catch (Exception e) {
                fail("Complex parseQuery failed for: " + queryString + " - " + e.getMessage());
            }
        }
    }

    private void testErrorHandling(SplitSearcher splitSearcher) {
        // Test error cases that should be handled consistently

        // Test 1: Null query string
        System.out.println("  Testing null query string handling");
        assertThrows(IllegalArgumentException.class, () -> {
            splitSearcher.parseQuery(null);
        }, "Null query should throw IllegalArgumentException");

        assertThrows(IllegalArgumentException.class, () -> {
            splitSearcher.parseQuery(null, "title");
        }, "Null query with single field should throw IllegalArgumentException");

        assertThrows(IllegalArgumentException.class, () -> {
            splitSearcher.parseQuery(null, Arrays.asList("title"));
        }, "Null query with field list should throw IllegalArgumentException");

        // Test 2: Empty query string
        System.out.println("  Testing empty query string handling");
        assertThrows(IllegalArgumentException.class, () -> {
            splitSearcher.parseQuery("");
        }, "Empty query should throw IllegalArgumentException");

        assertThrows(IllegalArgumentException.class, () -> {
            splitSearcher.parseQuery("   ", "title");
        }, "Whitespace-only query should throw IllegalArgumentException");

        // Test 3: Null/empty default field name
        System.out.println("  Testing null/empty default field handling");
        assertThrows(IllegalArgumentException.class, () -> {
            splitSearcher.parseQuery("machine", (String) null);
        }, "Null default field should throw IllegalArgumentException");

        assertThrows(IllegalArgumentException.class, () -> {
            splitSearcher.parseQuery("machine", "");
        }, "Empty default field should throw IllegalArgumentException");

        assertThrows(IllegalArgumentException.class, () -> {
            splitSearcher.parseQuery("machine", "   ");
        }, "Whitespace-only default field should throw IllegalArgumentException");

        // Test 4: Valid queries with null field list (should use empty list)
        System.out.println("  Testing null field list handling (should succeed)");
        try {
            SplitQuery query = splitSearcher.parseQuery("machine", (List<String>) null);
            assertNotNull(query, "Query with null field list should succeed");
            System.out.println("    ✅ Null field list handled correctly");
        } catch (Exception e) {
            fail("parseQuery with null field list should succeed: " + e.getMessage());
        }

        System.out.println("  ✅ All error handling tests passed");
    }
}
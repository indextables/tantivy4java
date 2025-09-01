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
 * Test QueryParser functionality with toString() debug output
 */
public class QueryParserBasicTest {

    @Test
    public void testQueryParserWithToString() throws Exception {
        System.out.println("🚀 === QUERY PARSER BASIC FUNCTIONALITY TEST ===");
        System.out.println("Testing Tantivy QueryParser integration\n");

        Path tempDir = Paths.get(System.getProperty("java.io.tmpdir"));
        String indexPath = tempDir.resolve("query_parser_basic_test").toString();

        try (SchemaBuilder builder = new SchemaBuilder()) {
            // Create simple schema with text fields
            builder.addTextField("title", true, false, "default", "position");
            builder.addTextField("content", true, false, "default", "position");

            try (Schema schema = builder.build()) {
                try (Index index = new Index(schema, indexPath, false)) {
                    try (IndexWriter writer = index.writer(Index.Memory.DEFAULT_HEAP_SIZE, 1)) {

                        // Add test documents
                        try (Document doc1 = new Document()) {
                            doc1.addText("title", "Machine Learning Guide");
                            doc1.addText("content", "Deep learning and neural networks");
                            writer.addDocument(doc1);
                        }

                        try (Document doc2 = new Document()) {
                            doc2.addText("title", "Python Programming");
                            doc2.addText("content", "Learn Python programming with examples");
                            writer.addDocument(doc2);
                        }

                        writer.commit();
                        System.out.println("✅ Indexed 2 test documents");
                    }

                    try (Searcher searcher = index.searcher()) {

                        // Test 1: Simple term query
                        System.out.println("\n🔍 Test 1: Simple term query");
                        testQueryParserWithAssertions(index, searcher, "machine", "Simple term search");

                        // Test 2: Boolean AND query  
                        System.out.println("\n🔍 Test 2: Boolean AND query");
                        testQueryParserWithAssertions(index, searcher, "machine AND learning", "Boolean AND operation");

                        // Test 3: Field-specific query
                        System.out.println("\n🔍 Test 3: Field-specific query");
                        testQueryParserWithAssertions(index, searcher, "title:python", "Field-specific search");

                        // Test 4: Phrase query
                        System.out.println("\n🔍 Test 4: Phrase query");
                        testQueryParserWithAssertions(index, searcher, "\"machine learning\"", "Phrase search");

                        // Test 5: Boolean OR query
                        System.out.println("\n🔍 Test 5: Boolean OR query");
                        testQueryParserWithAssertions(index, searcher, "python OR machine", "Boolean OR operation");

                        // Test 6: Complex boolean query  
                        System.out.println("\n🔍 Test 6: Complex boolean query");
                        testQueryParserWithAssertions(index, searcher, "(python OR machine) AND programming", "Complex boolean with parentheses");

                        // Test 7: Wildcard query
                        System.out.println("\n🔍 Test 7: Wildcard query");
                        testQueryParserWithAssertions(index, searcher, "prog*", "Wildcard search");

                        // Test 8: Multiple field query
                        System.out.println("\n🔍 Test 8: Multiple field query");
                        testQueryParserWithAssertions(index, searcher, "title:python OR content:neural", "Multiple field query");

                    }
                }
            }
        }

        System.out.println("\n🎉 QUERY PARSER COMPREHENSIVE TEST COMPLETED");
        System.out.println("✨ Successfully demonstrated:");
        System.out.println("   🔤 Simple term parsing with toString() debug output");
        System.out.println("   🔗 Boolean operators (AND, OR) with complex nesting");
        System.out.println("   🎯 Field-specific queries");
        System.out.println("   💬 Phrase queries");
        System.out.println("   🌟 Wildcard queries");
        System.out.println("   🧮 Complex boolean logic with parentheses");
        System.out.println("   📋 Query structure visualization via toString()");
        System.out.println("   🐍 Complete Tantivy QueryParser integration");
    }

    private void testQueryParserWithAssertions(Index index, Searcher searcher, String queryString, String description) {
        try {
            System.out.println("   Query: \"" + queryString + "\" (" + description + ")");

            // Parse the query using Index.parseQuery()
            try (Query parsedQuery = index.parseQuery(queryString)) {

                // Assert that query parsing succeeded
                assertNotNull(parsedQuery, "Parsed query should not be null");
                
                // Show the parsed query structure
                String queryStructure = parsedQuery.toString();
                System.out.println("   📋 Parsed Query: " + queryStructure);
                
                // Assert that toString() produces meaningful output
                assertNotNull(queryStructure, "Query toString() should not be null");
                assertFalse(queryStructure.trim().isEmpty(), "Query toString() should not be empty");

                // Execute the parsed query
                SearchResult result = searcher.search(parsedQuery, 10);
                
                // Assert that search execution succeeded
                assertNotNull(result, "Search result should not be null");
                assertNotNull(result.getHits(), "Search hits should not be null");

                System.out.println("   Results: " + result.getHits().size() + " documents found");

                // Show results
                for (var hit : result.getHits()) {
                    try (Document doc = searcher.doc(hit.getDocAddress())) {
                        String title = (String) doc.getFirst("title");
                        assertNotNull(title, "Document title should not be null");
                        System.out.println("     📄 \"" + title + "\" (score: " + String.format("%.3f", hit.getScore()) + ")");
                    }
                }

                System.out.println("   ✅ Query parsing and execution successful");

            }

        } catch (Exception e) {
            System.out.println("   ❌ Query parsing failed: " + e.getMessage());
            fail("Query parsing should not fail for: " + queryString + " - " + e.getMessage());
        }
    }
}
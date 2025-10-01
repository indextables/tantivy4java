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

package io.indextables.tantivy4java;

import io.indextables.tantivy4java.core.*;
import io.indextables.tantivy4java.query.*;
import io.indextables.tantivy4java.result.*;


import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;
import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * Test QueryParser functionality with comprehensive toString() debug output
 * Demonstrates how different query strings are parsed into Tantivy query structures
 */
public class QueryParserToStringTest {

    @Test
    public void testComprehensiveQueryParserToString() throws Exception {
        System.out.println("🔍 === QUERY PARSER toString() DEBUG TEST ===");
        System.out.println("Showing how different query strings are parsed into internal Tantivy structures\n");

        Path tempDir = Paths.get(System.getProperty("java.io.tmpdir"));
        String indexPath = tempDir.resolve("query_parser_tostring_test").toString();

        try (SchemaBuilder builder = new SchemaBuilder()) {
            // Create comprehensive schema
            builder.addTextField("title", true, false, "default", "position");
            builder.addTextField("content", true, false, "default", "position");
            builder.addTextField("author", true, false, "default", "position");
            builder.addIntegerField("score", true, true, false);
            
            try (Schema schema = builder.build()) {
                try (Index index = new Index(schema, indexPath, false)) {
                    try (IndexWriter writer = index.writer(Index.Memory.DEFAULT_HEAP_SIZE, 1)) {

                        // Add comprehensive test documents
                        try (Document doc1 = new Document()) {
                            doc1.addText("title", "Advanced Machine Learning");
                            doc1.addText("content", "Deep learning and neural networks for beginners");
                            doc1.addText("author", "John Smith");
                            doc1.addInteger("score", 95);
                            writer.addDocument(doc1);
                        }

                        try (Document doc2 = new Document()) {
                            doc2.addText("title", "Python Programming Guide");
                            doc2.addText("content", "Learn Python programming with practical examples");
                            doc2.addText("author", "Jane Doe");
                            doc2.addInteger("score", 87);
                            writer.addDocument(doc2);
                        }

                        try (Document doc3 = new Document()) {
                            doc3.addText("title", "Data Science Fundamentals");
                            doc3.addText("content", "Statistics, machine learning, and data analysis");
                            doc3.addText("author", "Bob Wilson");
                            doc3.addInteger("score", 92);
                            writer.addDocument(doc3);
                        }

                        writer.commit();
                        System.out.println("✅ Indexed 3 comprehensive test documents\n");
                    }

                    try (Searcher searcher = index.searcher()) {

                        System.out.println("🧪 QUERY PARSING ANALYSIS - Input String → Internal Structure\n");

                        // Simple term queries
                        System.out.println("📝 === SIMPLE TERM QUERIES ===");
                        analyzeQuery(index, searcher, "machine");
                        analyzeQuery(index, searcher, "python");

                        // Boolean queries  
                        System.out.println("\n🔗 === BOOLEAN QUERIES ===");
                        analyzeQuery(index, searcher, "machine AND learning");
                        analyzeQuery(index, searcher, "python OR java");
                        analyzeQuery(index, searcher, "machine AND NOT python");
                        
                        // Complex boolean with parentheses
                        System.out.println("\n🧮 === COMPLEX BOOLEAN QUERIES ===");
                        analyzeQuery(index, searcher, "(machine OR data) AND learning");
                        analyzeQuery(index, searcher, "python AND (programming OR development)");
                        analyzeQuery(index, searcher, "(machine AND learning) OR (data AND science)");

                        // Field-specific queries
                        System.out.println("\n🎯 === FIELD-SPECIFIC QUERIES ===");
                        analyzeQuery(index, searcher, "title:python");
                        analyzeQuery(index, searcher, "author:john");
                        analyzeQuery(index, searcher, "content:neural");

                        // Phrase queries
                        System.out.println("\n💬 === PHRASE QUERIES ===");
                        analyzeQuery(index, searcher, "\"machine learning\"");
                        analyzeQuery(index, searcher, "\"neural networks\"");
                        analyzeQuery(index, searcher, "\"data science\"");

                        // Wildcard queries
                        System.out.println("\n🌟 === WILDCARD QUERIES ===");
                        analyzeQuery(index, searcher, "mach*");
                        analyzeQuery(index, searcher, "prog*");
                        analyzeQuery(index, searcher, "learn*");

                        // Multi-field boolean queries
                        System.out.println("\n🔄 === MULTI-FIELD BOOLEAN QUERIES ===");
                        analyzeQuery(index, searcher, "title:machine OR content:python");
                        analyzeQuery(index, searcher, "author:john AND title:machine");
                        analyzeQuery(index, searcher, "content:learning AND author:smith");

                        // Complex multi-field queries
                        System.out.println("\n⚡ === ADVANCED MULTI-FIELD QUERIES ===");
                        analyzeQuery(index, searcher, "(title:machine OR title:python) AND content:learning");
                        analyzeQuery(index, searcher, "author:john AND (title:advanced OR content:neural)");

                    }
                }
            }
        }

        System.out.println("\n🎉 QUERY PARSER toString() ANALYSIS COMPLETED");
        System.out.println("✨ Demonstrated comprehensive query parsing with internal structure visualization:");
        System.out.println("   🔤 Term query structures");
        System.out.println("   🔗 Boolean query composition (AND, OR, NOT)");
        System.out.println("   🧮 Nested boolean logic with parentheses");
        System.out.println("   🎯 Field-specific query targeting");
        System.out.println("   💬 Phrase query structures");
        System.out.println("   🌟 Wildcard pattern matching");
        System.out.println("   🔄 Multi-field boolean combinations");
        System.out.println("   📋 Complete internal query structure visualization");
        System.out.println("   🐍 Production-ready Tantivy QueryParser debugging");
    }

    private void analyzeQuery(Index index, Searcher searcher, String queryString) {
        try {
            System.out.println("🔍 Input: \"" + queryString + "\"");
            
            // Parse the query and show structure
            try (Query parsedQuery = index.parseQuery(queryString)) {
                // Assert that parsing succeeded
                assertNotNull(parsedQuery, "Query should parse successfully for: " + queryString);
                
                String structure = parsedQuery.toString();
                System.out.println("📋 Structure: " + structure);
                
                // Assert that structure is meaningful
                assertNotNull(structure, "Query structure should not be null");
                assertFalse(structure.trim().isEmpty(), "Query structure should not be empty");
                
                // Execute and show results count
                SearchResult result = searcher.search(parsedQuery, 10);
                assertNotNull(result, "Search result should not be null");
                
                System.out.println("📊 Results: " + result.getHits().size() + " documents");
                
                // Show first result if any
                if (result.getHits().size() > 0) {
                    var firstHit = result.getHits().get(0);
                    try (Document doc = searcher.doc(firstHit.getDocAddress())) {
                        String title = (String) doc.getFirst("title");
                        assertNotNull(title, "Document title should not be null");
                        System.out.println("🏆 Top: \"" + title + "\" (score: " + String.format("%.3f", firstHit.getScore()) + ")");
                    }
                }
                
                System.out.println(); // Blank line for readability
            }
            
        } catch (Exception e) {
            System.out.println("❌ Parse Error: " + e.getMessage());
            fail("Query parsing should not fail for: " + queryString + " - " + e.getMessage());
            System.out.println(); // Blank line for readability
        }
    }
}

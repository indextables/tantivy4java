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

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

import java.nio.file.Files;
import java.nio.file.Path;

/**
 * Debug test to understand why regex queries work in AdvancedQueryTest but not in WildcardQueryTest.
 * This recreates the EXACT setup from AdvancedQueryTest to isolate the issue.
 */
public class RegexDebugTest {
    private Path tempDir;
    private SchemaBuilder schemaBuilder;
    private Schema schema;
    private Index index;
    private IndexWriter writer;
    private Searcher searcher;
    
    @BeforeEach
    public void setUp() throws Exception {
        tempDir = Files.createTempDirectory("regex_debug");
        
        // EXACT schema setup from AdvancedQueryTest
        schemaBuilder = new SchemaBuilder();
        schemaBuilder
            .addTextField("title", true, false, "default", "position")
            .addTextField("category", true, true, "default", "position");
        schema = schemaBuilder.build();
        
        // EXACT index setup from AdvancedQueryTest  
        index = new Index(schema, tempDir.toString(), false);
        writer = index.writer(Index.Memory.DEFAULT_HEAP_SIZE, 2);
        
        // Add test document with EXACT data from AdvancedQueryTest
        try (Document doc = new Document()) {
            doc.addText("title", "Important Software Engineering");
            doc.addText("category", "engineering");
            writer.addDocument(doc);
        }
        
        writer.commit();
        writer.close();
        
        // Don't reload the index - just get a searcher directly
        // index.reload();
        searcher = index.searcher();
    }
    
    @AfterEach
    public void tearDown() throws Exception {
        if (searcher != null) searcher.close();
        if (index != null) index.close();
        if (schema != null) schema.close();
        if (schemaBuilder != null) schemaBuilder.close();
        
        // Clean up temp directory
        Files.walk(tempDir)
            .sorted((a, b) -> b.compareTo(a))
            .forEach(path -> {
                try {
                    Files.deleteIfExists(path);
                } catch (Exception e) {
                    // Ignore cleanup errors
                }
            });
    }
    
    @Test
    public void testRegexQueryExactMatch() {
        System.out.println("=== REGEX DEBUG TEST ===");
        
        // Use the original schema that was used to create the index
        // Schema currentSchema = index.getSchema();
        
        // Test the EXACT same regex query that works in AdvancedQueryTest
        try (Query regexQuery = Query.regexQuery(schema, "category", "eng.*");
             SearchResult result = searcher.search(regexQuery, 10)) {
            System.out.println("Regex query 'eng.*' on category field found: " + result.getHits().size() + " hits");
            
            for (SearchResult.Hit hit : result.getHits()) {
                try (Document doc = searcher.doc(hit.getDocAddress())) {
                    System.out.println("  - Title: " + doc.getFirst("title"));
                    System.out.println("  - Category: " + doc.getFirst("category"));
                }
            }
            
            // This should work based on AdvancedQueryTest
            assertTrue(result.getHits().size() >= 1, "Regex query should find the 'engineering' document");
        }
        
        // Test term query to verify indexing works
        try (Query termQuery = Query.termQuery(schema, "category", "engineering");
             SearchResult result = searcher.search(termQuery, 10)) {
            System.out.println("Term query 'engineering' on category field found: " + result.getHits().size() + " hits");
            assertEquals(1, result.getHits().size(), "Term query should find the document");
        }
        
        // Test wildcard query using same setup
        try (Query wildcardQuery = Query.wildcardQuery(schema, "category", "eng*");
             SearchResult result = searcher.search(wildcardQuery, 10)) {
            System.out.println("Wildcard query 'eng*' on category field found: " + result.getHits().size() + " hits");
            // This should work if wildcard implementation is correct
            assertEquals(1, result.getHits().size(), "Wildcard query should find the 'engineering' document");
        }
    }
}
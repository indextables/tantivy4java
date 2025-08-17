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

package com.tantivy4java.examples;

import com.tantivy4java.*;

/**
 * Basic example demonstrating Tantivy4Java usage.
 * This example shows how to create an index, add documents, and search.
 */
public class BasicExample {
    
    public static void main(String[] args) {
        System.out.println("Tantivy4Java Basic Example");
        System.out.println("Version: " + getVersionSafely());
        
        // Example of API usage (will fail until native implementation is complete)
        try {
            createAndSearchIndex();
        } catch (Exception e) {
            System.err.println("Expected error (native implementation not complete): " + e.getMessage());
        }
    }
    
    private static String getVersionSafely() {
        try {
            return Tantivy.getVersion();
        } catch (Exception e) {
            return "Unknown (native library not loaded)";
        }
    }
    
    private static void createAndSearchIndex() {
        // Create schema
        try (SchemaBuilder builder = new SchemaBuilder()) {
            builder.addTextField("title", true, false, "default", "position")
                   .addTextField("body", true, false, "default", "position")
                   .addIntegerField("year", true, true, false);
            
            try (Schema schema = builder.build()) {
                // Create in-memory index
                try (Index index = new Index(schema)) {
                    
                    // Add documents
                    try (IndexWriter writer = index.writer()) {
                        Document doc1 = new Document();
                        doc1.addText("title", "The Old Man and the Sea");
                        doc1.addText("body", "A story of an old fisherman's struggle with a giant marlin");
                        doc1.addInteger("year", 1952);
                        writer.addDocument(doc1);
                        
                        Document doc2 = new Document();
                        doc2.addText("title", "Of Mice and Men");
                        doc2.addText("body", "A story of two displaced migrant workers");
                        doc2.addInteger("year", 1937);
                        writer.addDocument(doc2);
                        
                        Document doc3 = new Document();
                        doc3.addText("title", "The Great Gatsby");
                        doc3.addText("body", "A story of the Jazz Age and the American Dream");
                        doc3.addInteger("year", 1925);
                        writer.addDocument(doc3);
                        
                        writer.commit();
                    }
                    
                    // Search the index
                    try (Searcher searcher = index.searcher()) {
                        System.out.println("Total documents: " + searcher.getNumDocs());
                        
                        // Simple term search
                        Query query = Query.termQuery(schema, "body", "story");
                        SearchResult result = searcher.search(query, 10);
                        
                        System.out.println("Found " + result.getHits().size() + " documents for 'story'");
                        for (SearchResult.Hit hit : result.getHits()) {
                            Document doc = searcher.doc(hit.getDocAddress());
                            System.out.println("Score: " + hit.getScore() + 
                                             ", Title: " + doc.getFirst("title"));
                        }
                        
                        // Boolean query
                        Query termQuery1 = Query.termQuery(schema, "body", "story");
                        Query termQuery2 = Query.termQuery(schema, "title", "Old");
                        Query boolQuery = Query.booleanQuery(java.util.List.of(
                            new Query.OccurQuery(Occur.MUST, termQuery1),
                            new Query.OccurQuery(Occur.MUST, termQuery2)
                        ));
                        
                        SearchResult boolResult = searcher.search(boolQuery, 10);
                        System.out.println("Boolean query results: " + boolResult.getHits().size());
                        
                        // Range query
                        Query rangeQuery = Query.rangeQuery(schema, "year", FieldType.INTEGER, 
                                                          1930, 1950, true, true);
                        SearchResult rangeResult = searcher.search(rangeQuery, 10);
                        System.out.println("Books published 1930-1950: " + rangeResult.getHits().size());
                    }
                }
            }
        }
    }
}
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
import io.indextables.tantivy4java.util.*;


import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Basic tests for Tantivy4Java library.
 * These tests verify the API structure and basic functionality.
 */
public class TantivyTest {

    @BeforeAll
    static void setUp() {
        // Initialize the library
        Tantivy.initialize();
    }

    @Test
    void testLibraryVersion() {
        // Test that we can get the version without native library
        // (this will fail until native lib is implemented)
        assertDoesNotThrow(() -> {
            String version = Tantivy.getVersion();
            assertNotNull(version);
        });
    }

    @Test 
    void testEnums() {
        // Test that enums work correctly
        assertEquals(1, Occur.MUST.getValue());
        assertEquals(2, Occur.SHOULD.getValue());
        assertEquals(3, Occur.MUST_NOT.getValue());
        
        assertEquals(1, FieldType.TEXT.getValue());
        assertEquals(2, FieldType.UNSIGNED.getValue());
        assertEquals(10, FieldType.IP_ADDR.getValue());
        
        assertEquals(1, Order.ASC.getValue());
        assertEquals(2, Order.DESC.getValue());
    }

    @Test
    void testSchemaBuilderValidation() {
        // Test field name validation
        assertTrue(SchemaBuilder.isValidFieldName("title"));
        assertTrue(SchemaBuilder.isValidFieldName("body"));
        assertTrue(SchemaBuilder.isValidFieldName("field_with_underscores"));
        assertTrue(SchemaBuilder.isValidFieldName("field123"));
    }

    @Disabled("Native implementation not complete")
    @Test
    void testBasicIndexing() {
        // This test will be enabled once native implementation is complete
        try (SchemaBuilder builder = new SchemaBuilder()) {
            builder.addTextField("title", true, false, "default", "position");
            builder.addTextField("body", true, false, "default", "position");
            
            try (Schema schema = builder.build()) {
                try (Index index = new Index(schema)) {
                    try (IndexWriter writer = index.writer()) {
                        Document doc = new Document();
                        doc.addText("title", "Test Document");
                        doc.addText("body", "This is a test document for Tantivy4Java");
                        
                        writer.addDocument(doc);
                        writer.commit();
                        
                        try (Searcher searcher = index.searcher()) {
                            Query query = Query.termQuery(schema, "title", "Test");
                            SearchResult result = searcher.search(query);
                            
                            assertEquals(1, result.getHits().size());
                        }
                    }
                }
            }
        }
    }

    @Disabled("Native implementation not complete") 
    @Test
    void testQueryTypes() {
        // Test different query types once implementation is complete
        try (SchemaBuilder builder = new SchemaBuilder()) {
            builder.addTextField("title");
            
            try (Schema schema = builder.build()) {
                // Test all query
                Query allQuery = Query.allQuery();
                assertNotNull(allQuery);
                
                // Test term query
                Query termQuery = Query.termQuery(schema, "title", "test");
                assertNotNull(termQuery);
                
                // Test fuzzy query
                Query fuzzyQuery = Query.fuzzyTermQuery(schema, "title", "test");
                assertNotNull(fuzzyQuery);
            }
        }
    }

    @Test
    void testDocAddress() {
        // Test DocAddress creation and methods
        DocAddress addr = new DocAddress(0, 123);
        assertEquals(0, addr.getSegmentOrd());
        assertEquals(123, addr.getDoc());
        
        DocAddress addr2 = new DocAddress(0, 123);
        assertEquals(addr, addr2);
        assertEquals(addr.hashCode(), addr2.hashCode());
        
        DocAddress addr3 = new DocAddress(1, 123);
        assertNotEquals(addr, addr3);
    }

    @Test
    void testFacetCreation() {
        // Test Facet static methods (will throw until implemented)
        assertThrows(RuntimeException.class, () -> {
            Facet root = Facet.root();
        });
        
        assertThrows(RuntimeException.class, () -> {
            Facet facet = Facet.fromString("/category/electronics");
        });
    }
}

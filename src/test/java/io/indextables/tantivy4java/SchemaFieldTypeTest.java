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


import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;
import java.util.List;
import java.util.Set;
import java.util.HashSet;

/**
 * Test for Schema field type filtering methods.
 * Tests the newly implemented nativeGetFieldNamesByType method.
 */
public class SchemaFieldTypeTest {
    
    private SchemaBuilder builder;
    private Schema schema;
    
    @BeforeEach
    void setUp() {
        Tantivy.initialize();
        builder = new SchemaBuilder();
    }
    
    @AfterEach
    void tearDown() {
        if (schema != null) {
            schema.close();
            schema = null;
        }
        if (builder != null) {
            builder.close();
            builder = null;
        }
    }
    
    @Test
    void testGetFieldNamesByType() {
        // Build a schema with various field types
        builder.addTextField("title", true, false, "default", "position");
        builder.addTextField("content", true, false, "default", "position");
        builder.addIntegerField("age", true, true, true);
        builder.addIntegerField("count", true, false, false);
        builder.addFloatField("score", true, true, true);
        builder.addBooleanField("active", true, true, false);
        builder.addDateField("created_at", true, true, true);
        builder.addIpAddrField("ip_address", true, true, false);
        
        schema = builder.build();
        builder = null; // builder is consumed by build()
        
        // Test TEXT fields
        List<String> textFields = schema.getFieldNamesByType(FieldType.TEXT);
        assertEquals(2, textFields.size());
        assertTrue(textFields.contains("title"));
        assertTrue(textFields.contains("content"));
        
        // Test INTEGER fields
        List<String> integerFields = schema.getFieldNamesByType(FieldType.INTEGER);
        assertEquals(2, integerFields.size());
        assertTrue(integerFields.contains("age"));
        assertTrue(integerFields.contains("count"));
        
        // Test FLOAT fields
        List<String> floatFields = schema.getFieldNamesByType(FieldType.FLOAT);
        assertEquals(1, floatFields.size());
        assertTrue(floatFields.contains("score"));
        
        // Test BOOLEAN fields
        List<String> booleanFields = schema.getFieldNamesByType(FieldType.BOOLEAN);
        assertEquals(1, booleanFields.size());
        assertTrue(booleanFields.contains("active"));
        
        // Test DATE fields
        List<String> dateFields = schema.getFieldNamesByType(FieldType.DATE);
        assertEquals(1, dateFields.size());
        assertTrue(dateFields.contains("created_at"));
        
        // Test IP_ADDR fields
        List<String> ipFields = schema.getFieldNamesByType(FieldType.IP_ADDR);
        assertEquals(1, ipFields.size());
        assertTrue(ipFields.contains("ip_address"));
        
        // Test a type that doesn't exist in the schema
        List<String> jsonFields = schema.getFieldNamesByType(FieldType.JSON);
        assertNotNull(jsonFields);
        assertEquals(0, jsonFields.size());
    }
    
    @Test
    void testGetFieldNamesByTypeEmptySchema() {
        // Test with an empty schema
        schema = builder.build();
        builder = null;
        
        // All field type queries should return empty lists
        for (FieldType type : FieldType.values()) {
            List<String> fields = schema.getFieldNamesByType(type);
            assertNotNull(fields);
            assertEquals(0, fields.size());
        }
    }
    
    @Test
    void testGetFieldNamesByTypeConsistency() {
        // Build a schema with multiple fields of the same type
        builder.addTextField("field1", true, false, "default", "position");
        builder.addTextField("field2", false, true, "default", "position");
        builder.addTextField("field3", true, true, "default", "position");
        builder.addIntegerField("num1", true, false, false);
        builder.addIntegerField("num2", false, true, true);
        
        schema = builder.build();
        builder = null;
        
        // Test that all text fields are returned
        List<String> textFields = schema.getFieldNamesByType(FieldType.TEXT);
        assertEquals(3, textFields.size());
        Set<String> textFieldSet = new HashSet<>(textFields);
        assertTrue(textFieldSet.contains("field1"));
        assertTrue(textFieldSet.contains("field2"));
        assertTrue(textFieldSet.contains("field3"));
        
        // Test that all integer fields are returned
        List<String> intFields = schema.getFieldNamesByType(FieldType.INTEGER);
        assertEquals(2, intFields.size());
        Set<String> intFieldSet = new HashSet<>(intFields);
        assertTrue(intFieldSet.contains("num1"));
        assertTrue(intFieldSet.contains("num2"));
        
        // Verify total field count matches
        int totalFields = 0;
        for (FieldType type : FieldType.values()) {
            totalFields += schema.getFieldNamesByType(type).size();
        }
        assertEquals(5, totalFields);
    }
    
    @Test
    void testGetFieldNamesByCapabilities() {
        // Build a schema with various field capabilities
        // Note: addTextField(name, stored, fast, tokenizer, indexOption)
        // All text fields are indexed by default (matching tantivy-py behavior)
        builder.addTextField("text_stored", true, false, "default", "position");    // stored + indexed (always)
        builder.addTextField("text_fast", false, true, "default", "position");      // fast + indexed (always)
        builder.addTextField("text_basic", false, false, "default", "position");    // indexed only (always)
        builder.addIntegerField("int_all", true, true, true);                       // integer with all capabilities
        builder.addIntegerField("int_none", false, false, false);                   // integer with no capabilities
        
        schema = builder.build();
        builder = null;
        
        // Test stored fields
        List<String> storedFields = schema.getStoredFieldNames();
        assertEquals(2, storedFields.size());
        assertTrue(storedFields.contains("text_stored"));
        assertTrue(storedFields.contains("int_all"));
        
        // Test indexed fields - all text fields are always indexed (tantivy-py behavior)
        List<String> indexedFields = schema.getIndexedFieldNames();
        assertEquals(4, indexedFields.size());
        assertTrue(indexedFields.contains("text_stored"));   // text fields always indexed
        assertTrue(indexedFields.contains("text_fast"));     // text fields always indexed  
        assertTrue(indexedFields.contains("text_basic"));    // text fields always indexed
        assertTrue(indexedFields.contains("int_all"));       // integer field with indexed=true
        
        // Test fast fields - only fields with fast=true parameter
        List<String> fastFields = schema.getFastFieldNames();
        assertEquals(2, fastFields.size());
        assertTrue(fastFields.contains("text_fast"));  // text field with fast=true
        assertTrue(fastFields.contains("int_all"));    // integer field with fast=true
    }
}

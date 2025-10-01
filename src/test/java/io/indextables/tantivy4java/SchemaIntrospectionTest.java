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


import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;
import static org.junit.jupiter.api.Assertions.*;

import java.util.List;

/**
 * Tests for schema field introspection capabilities.
 * Validates that schemas can be examined to discover field names, types, and configurations.
 */
public class SchemaIntrospectionTest {

    @Test
    @DisplayName("Test basic field name enumeration")
    void testGetFieldNames() {
        // Create a schema with several field types
        Schema schema = new SchemaBuilder()
            .addTextField("title", true, false, "default", "position")
            .addTextField("content", false, true, "default", "position") 
            .addIntegerField("score", true, true, true)
            .addBooleanField("published", true, true, true)
            .addDateField("created_at", true, true, false)
            .build();

        // Test field name retrieval
        List<String> fieldNames = schema.getFieldNames();
        assertNotNull(fieldNames, "Field names list should not be null");
        assertEquals(5, fieldNames.size(), "Should have 5 fields");
        
        // Verify expected fields are present
        assertTrue(fieldNames.contains("title"), "Should contain title field");
        assertTrue(fieldNames.contains("content"), "Should contain content field");
        assertTrue(fieldNames.contains("score"), "Should contain score field");
        assertTrue(fieldNames.contains("published"), "Should contain published field");
        assertTrue(fieldNames.contains("created_at"), "Should contain created_at field");
        
        System.out.println("📋 Schema field names: " + fieldNames);
        
        schema.close();
    }

    @Test
    @DisplayName("Test field existence checking")
    void testHasField() {
        Schema schema = new SchemaBuilder()
            .addTextField("title")
            .addIntegerField("count")
            .build();
        
        // Test existing fields
        assertTrue(schema.hasField("title"), "Should have title field");
        assertTrue(schema.hasField("count"), "Should have count field");
        
        // Test non-existent fields
        assertFalse(schema.hasField("nonexistent"), "Should not have nonexistent field");
        assertFalse(schema.hasField("missing"), "Should not have missing field");
        
        System.out.println("✅ Field existence checks passed");
        
        schema.close();
    }

    @Test
    @DisplayName("Test field count")
    void testGetFieldCount() {
        // Test empty schema
        Schema emptySchema = new SchemaBuilder().build();
        assertEquals(0, emptySchema.getFieldCount(), "Empty schema should have 0 fields");
        emptySchema.close();
        
        // Test schema with multiple fields
        Schema schema = new SchemaBuilder()
            .addTextField("field1")
            .addTextField("field2")
            .addIntegerField("field3")
            .build();
            
        assertEquals(3, schema.getFieldCount(), "Schema should have 3 fields");
        
        System.out.println("📊 Field count: " + schema.getFieldCount());
        
        schema.close();
    }

    @Test
    @DisplayName("Test schema summary")
    void testGetSchemaSummary() {
        Schema schema = new SchemaBuilder()
            .addTextField("title", true, false, "default", "position")
            .addIntegerField("score", true, true, true)
            .addBooleanField("published", false, true, false)
            .build();
        
        String summary = schema.getSchemaSummary();
        assertNotNull(summary, "Schema summary should not be null");
        assertTrue(summary.contains("Schema with 3 fields"), "Summary should contain field count");
        assertTrue(summary.contains("title"), "Summary should contain title field");
        assertTrue(summary.contains("score"), "Summary should contain score field");
        assertTrue(summary.contains("published"), "Summary should contain published field");
        
        System.out.println("📝 Schema Summary:");
        System.out.println(summary);
        
        schema.close();
    }

    @Test
    @DisplayName("Test introspection with complex schema")
    void testComplexSchemaIntrospection() {
        // Create a comprehensive schema similar to a real search index
        Schema schema = new SchemaBuilder()
            .addTextField("title", true, false, "default", "position")
            .addTextField("content", true, false, "default", "position")
            .addTextField("category", false, true, "keyword", "basic")
            .addIntegerField("view_count", true, true, true)
            .addFloatField("rating", true, true, true)
            .addBooleanField("is_published", true, true, true)
            .addBooleanField("is_featured", false, true, false)
            .addDateField("created_at", true, true, true)
            .addDateField("updated_at", false, true, false)
            .build();

        // Test comprehensive introspection
        List<String> fieldNames = schema.getFieldNames();
        assertEquals(9, fieldNames.size(), "Should have 9 fields");
        assertEquals(9, schema.getFieldCount(), "Field count should match field names size");
        
        // Test all expected fields exist
        String[] expectedFields = {
            "title", "content", "category", "view_count", "rating", 
            "is_published", "is_featured", "created_at", "updated_at"
        };
        
        for (String fieldName : expectedFields) {
            assertTrue(schema.hasField(fieldName), "Should have field: " + fieldName);
            assertTrue(fieldNames.contains(fieldName), "Field names should contain: " + fieldName);
        }
        
        // Test schema summary contains key information
        String summary = schema.getSchemaSummary();
        assertTrue(summary.contains("Schema with 9 fields"), "Summary should show correct field count");
        
        System.out.println("🎯 Complex Schema Introspection Results:");
        System.out.println("  Field Names: " + fieldNames);
        System.out.println("  Field Count: " + schema.getFieldCount());
        System.out.println("  Summary Preview: " + summary.split("\\n")[0]);
        
        schema.close();
    }

    @Test
    @DisplayName("Test introspection with closed schema")
    void testClosedSchemaIntrospection() {
        Schema schema = new SchemaBuilder()
            .addTextField("test_field")
            .build();
        
        // Verify it works before closing
        assertEquals(1, schema.getFieldCount());
        assertTrue(schema.hasField("test_field"));
        
        // Close the schema
        schema.close();
        
        // Test that operations throw IllegalStateException
        assertThrows(IllegalStateException.class, () -> {
            schema.getFieldCount();
        }, "Should throw IllegalStateException for closed schema");
        
        assertThrows(IllegalStateException.class, () -> {
            schema.getFieldNames();
        }, "Should throw IllegalStateException for closed schema");
        
        assertThrows(IllegalStateException.class, () -> {
            schema.hasField("test_field");
        }, "Should throw IllegalStateException for closed schema");
        
        System.out.println("🔒 Closed schema properly throws exceptions");
    }
}

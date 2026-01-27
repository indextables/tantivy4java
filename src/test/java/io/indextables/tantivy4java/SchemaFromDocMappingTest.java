package io.indextables.tantivy4java;

import io.indextables.tantivy4java.core.*;
import io.indextables.tantivy4java.split.SplitQuery;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;
import static org.junit.jupiter.api.Assertions.*;

import java.util.List;

/**
 * Test class for Schema.fromDocMappingJson() functionality.
 * Tests creating a schema from JSON doc mapping and using it with SplitQuery.parseQuery().
 */
public class SchemaFromDocMappingTest {

    @Test
    @DisplayName("Create schema from JSON doc mapping - basic fields")
    public void testBasicSchemaCreation() {
        System.out.println("=== Testing Schema.fromDocMappingJson() with basic fields ===");

        String json = "[\n" +
            "  {\"name\": \"title\", \"type\": \"text\", \"stored\": true, \"indexed\": true, \"tokenizer\": \"default\"},\n" +
            "  {\"name\": \"body\", \"type\": \"text\", \"stored\": true, \"indexed\": true},\n" +
            "  {\"name\": \"count\", \"type\": \"i64\", \"stored\": true, \"indexed\": true, \"fast\": true},\n" +
            "  {\"name\": \"price\", \"type\": \"f64\", \"stored\": true, \"fast\": true},\n" +
            "  {\"name\": \"active\", \"type\": \"bool\", \"stored\": true, \"indexed\": true}\n" +
            "]";

        try (Schema schema = Schema.fromDocMappingJson(json)) {
            assertNotNull(schema, "Schema should not be null");
            assertEquals(5, schema.getFieldCount(), "Schema should have 5 fields");

            List<String> fieldNames = schema.getFieldNames();
            assertTrue(fieldNames.contains("title"), "Schema should have 'title' field");
            assertTrue(fieldNames.contains("body"), "Schema should have 'body' field");
            assertTrue(fieldNames.contains("count"), "Schema should have 'count' field");
            assertTrue(fieldNames.contains("price"), "Schema should have 'price' field");
            assertTrue(fieldNames.contains("active"), "Schema should have 'active' field");

            System.out.println("Created schema with fields: " + fieldNames);
            System.out.println("Test passed!");
        }
    }

    @Test
    @DisplayName("Create schema from JSON doc mapping - with JSON object field")
    public void testSchemaWithObjectField() {
        System.out.println("=== Testing Schema.fromDocMappingJson() with object field ===");

        String json = "[\n" +
            "  {\"name\": \"id\", \"type\": \"text\", \"stored\": true, \"indexed\": true, \"tokenizer\": \"raw\"},\n" +
            "  {\"name\": \"metadata\", \"type\": \"object\", \"stored\": true, \"indexed\": true, \"expand_dots\": true}\n" +
            "]";

        try (Schema schema = Schema.fromDocMappingJson(json)) {
            assertNotNull(schema, "Schema should not be null");
            assertEquals(2, schema.getFieldCount(), "Schema should have 2 fields");

            assertTrue(schema.hasField("id"), "Schema should have 'id' field");
            assertTrue(schema.hasField("metadata"), "Schema should have 'metadata' field");

            System.out.println("Schema summary: " + schema.getSchemaSummary());
            System.out.println("Test passed!");
        }
    }

    @Test
    @DisplayName("Use schema from JSON with SplitQuery.parseQuery")
    public void testSchemaWithSplitQueryParseQuery() {
        System.out.println("=== Testing Schema.fromDocMappingJson() with SplitQuery.parseQuery() ===");

        String json = "[\n" +
            "  {\"name\": \"title\", \"type\": \"text\", \"stored\": true, \"indexed\": true, \"tokenizer\": \"default\"},\n" +
            "  {\"name\": \"content\", \"type\": \"text\", \"stored\": true, \"indexed\": true},\n" +
            "  {\"name\": \"author\", \"type\": \"text\", \"stored\": true, \"indexed\": true, \"tokenizer\": \"raw\"},\n" +
            "  {\"name\": \"year\", \"type\": \"i64\", \"stored\": true, \"indexed\": true, \"fast\": true}\n" +
            "]";

        try (Schema schema = Schema.fromDocMappingJson(json)) {
            assertNotNull(schema, "Schema should not be null");
            System.out.println("Created schema with " + schema.getFieldCount() + " fields");

            // Test parsing a simple term query
            SplitQuery query1 = SplitQuery.parseQuery("title:hello", schema);
            assertNotNull(query1, "Parsed query should not be null");
            String queryAst1 = query1.toQueryAstJson();
            assertNotNull(queryAst1, "Query AST JSON should not be null");
            System.out.println("Query 'title:hello' -> AST: " + queryAst1);

            // Test parsing a phrase query
            SplitQuery query2 = SplitQuery.parseQuery("title:\"hello world\"", schema);
            assertNotNull(query2, "Parsed phrase query should not be null");
            String queryAst2 = query2.toQueryAstJson();
            assertNotNull(queryAst2, "Phrase query AST JSON should not be null");
            System.out.println("Query 'title:\"hello world\"' -> AST: " + queryAst2);

            // Test parsing a range query on numeric field
            SplitQuery query3 = SplitQuery.parseQuery("year:[2020 TO 2024]", schema);
            assertNotNull(query3, "Parsed range query should not be null");
            String queryAst3 = query3.toQueryAstJson();
            assertNotNull(queryAst3, "Range query AST JSON should not be null");
            System.out.println("Query 'year:[2020 TO 2024]' -> AST: " + queryAst3);

            // Test parsing with default search fields
            SplitQuery query4 = SplitQuery.parseQuery("test", schema, new String[]{"title", "content"});
            assertNotNull(query4, "Parsed query with default fields should not be null");
            String queryAst4 = query4.toQueryAstJson();
            assertNotNull(queryAst4, "Query with default fields AST JSON should not be null");
            System.out.println("Query 'test' (default fields: title, content) -> AST: " + queryAst4);

            System.out.println("All SplitQuery.parseQuery() tests passed!");
        }
    }

    @Test
    @DisplayName("Schema from JSON - error handling for invalid JSON")
    public void testInvalidJsonHandling() {
        System.out.println("=== Testing error handling for invalid JSON ===");

        // Test null JSON - throws IllegalArgumentException from Java validation
        assertThrows(IllegalArgumentException.class, () -> {
            Schema.fromDocMappingJson(null);
        }, "Should throw for null JSON");

        // Test empty JSON - throws IllegalArgumentException from Java validation
        assertThrows(IllegalArgumentException.class, () -> {
            Schema.fromDocMappingJson("");
        }, "Should throw for empty JSON");

        // Test malformed JSON - throws RuntimeException from native layer
        assertThrows(RuntimeException.class, () -> {
            Schema.fromDocMappingJson("not valid json");
        }, "Should throw for malformed JSON");

        System.out.println("Error handling tests passed!");
    }

    @Test
    @DisplayName("Schema from JSON - field introspection")
    public void testFieldIntrospection() {
        System.out.println("=== Testing field introspection on schema from JSON ===");

        String json = "[\n" +
            "  {\"name\": \"title\", \"type\": \"text\", \"stored\": true, \"indexed\": true, \"tokenizer\": \"default\"},\n" +
            "  {\"name\": \"count\", \"type\": \"i64\", \"stored\": true, \"indexed\": true, \"fast\": true},\n" +
            "  {\"name\": \"score\", \"type\": \"f64\", \"stored\": false, \"fast\": true}\n" +
            "]";

        try (Schema schema = Schema.fromDocMappingJson(json)) {
            // Check stored fields
            List<String> storedFields = schema.getStoredFieldNames();
            assertTrue(storedFields.contains("title"), "title should be stored");
            assertTrue(storedFields.contains("count"), "count should be stored");
            assertFalse(storedFields.contains("score"), "score should not be stored");
            System.out.println("Stored fields: " + storedFields);

            // Check indexed fields
            List<String> indexedFields = schema.getIndexedFieldNames();
            assertTrue(indexedFields.contains("title"), "title should be indexed");
            assertTrue(indexedFields.contains("count"), "count should be indexed");
            System.out.println("Indexed fields: " + indexedFields);

            // Check fast fields
            List<String> fastFields = schema.getFastFieldNames();
            assertTrue(fastFields.contains("count"), "count should be fast");
            assertTrue(fastFields.contains("score"), "score should be fast");
            System.out.println("Fast fields: " + fastFields);

            // Check individual field info
            FieldInfo titleInfo = schema.getFieldInfo("title");
            assertNotNull(titleInfo, "Should get field info for title");
            assertEquals(FieldType.TEXT, titleInfo.getType(), "title should be TEXT type");
            assertTrue(titleInfo.isStored(), "title should be stored");
            assertTrue(titleInfo.isIndexed(), "title should be indexed");

            FieldInfo countInfo = schema.getFieldInfo("count");
            assertNotNull(countInfo, "Should get field info for count");
            assertEquals(FieldType.INTEGER, countInfo.getType(), "count should be INTEGER type");
            assertTrue(countInfo.isFast(), "count should be fast");

            System.out.println("Field introspection tests passed!");
        }
    }
}

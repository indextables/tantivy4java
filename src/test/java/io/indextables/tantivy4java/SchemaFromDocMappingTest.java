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
    @DisplayName("SplitQuery.parseQuery - error handling for invalid queries")
    public void testParseQueryErrorHandling() {
        System.out.println("=== Testing SplitQuery.parseQuery() error handling ===");

        String json = "[\n" +
            "  {\"name\": \"title\", \"type\": \"text\", \"stored\": true, \"indexed\": true, \"tokenizer\": \"default\"},\n" +
            "  {\"name\": \"count\", \"type\": \"i64\", \"stored\": true, \"indexed\": true, \"fast\": true}\n" +
            "]";

        try (Schema schema = Schema.fromDocMappingJson(json)) {
            // Test malformed query syntax - unclosed parenthesis
            RuntimeException ex1 = assertThrows(RuntimeException.class, () -> {
                SplitQuery.parseQuery("title:(unclosed", schema);
            }, "Should throw for unclosed parenthesis");
            System.out.println("Error for unclosed parenthesis: " + ex1.getMessage());
            assertNotNull(ex1.getMessage(), "Exception should have a message");
            assertTrue(ex1.getMessage().length() > 10, "Error message should be descriptive");

            // Test malformed range query - unclosed bracket
            RuntimeException ex2 = assertThrows(RuntimeException.class, () -> {
                SplitQuery.parseQuery("count:[10 TO", schema);
            }, "Should throw for malformed range query");
            System.out.println("Error for malformed range: " + ex2.getMessage());
            assertNotNull(ex2.getMessage(), "Exception should have a message");

            System.out.println("Parse query error handling tests passed!");
        }
    }

    @Test
    @DisplayName("SplitQuery.countQueryFields - count fields impacted by query")
    public void testCountQueryFields() {
        System.out.println("=== Testing SplitQuery.countQueryFields() ===");

        String json = "[\n" +
            "  {\"name\": \"title\", \"type\": \"text\", \"stored\": true, \"indexed\": true, \"tokenizer\": \"default\"},\n" +
            "  {\"name\": \"body\", \"type\": \"text\", \"stored\": true, \"indexed\": true},\n" +
            "  {\"name\": \"author\", \"type\": \"text\", \"stored\": true, \"indexed\": true},\n" +
            "  {\"name\": \"year\", \"type\": \"i64\", \"stored\": true, \"indexed\": true, \"fast\": true}\n" +
            "]";

        try (Schema schema = Schema.fromDocMappingJson(json)) {
            // Single field query
            int count1 = SplitQuery.countQueryFields("title:hello", schema);
            assertEquals(1, count1, "Single field query should impact 1 field");
            System.out.println("Query 'title:hello' impacts " + count1 + " field(s)");

            // Two field query with AND
            int count2 = SplitQuery.countQueryFields("title:hello AND body:world", schema);
            assertEquals(2, count2, "Two field AND query should impact 2 fields");
            System.out.println("Query 'title:hello AND body:world' impacts " + count2 + " field(s)");

            // Query with same field twice should count as 1
            int count3 = SplitQuery.countQueryFields("title:hello AND title:world", schema);
            assertEquals(1, count3, "Same field twice should still count as 1");
            System.out.println("Query 'title:hello AND title:world' impacts " + count3 + " field(s)");

            // Range query on numeric field
            int count4 = SplitQuery.countQueryFields("year:[2020 TO 2024]", schema);
            assertEquals(1, count4, "Range query should impact 1 field");
            System.out.println("Query 'year:[2020 TO 2024]' impacts " + count4 + " field(s)");

            // Mixed text and numeric fields
            int count5 = SplitQuery.countQueryFields("title:test AND year:[2020 TO 2024]", schema);
            assertEquals(2, count5, "Mixed query should impact 2 fields");
            System.out.println("Query 'title:test AND year:[2020 TO 2024]' impacts " + count5 + " field(s)");

            // Query without field specification uses default fields (all text fields)
            int count6 = SplitQuery.countQueryFields("searchterm", schema);
            System.out.println("Query 'searchterm' (no field) impacts " + count6 + " field(s)");
            assertTrue(count6 >= 1, "Unfielded query should impact at least 1 default field");

            System.out.println("countQueryFields tests passed!");
        }
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

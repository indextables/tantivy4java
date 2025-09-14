package com.tantivy4java;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeEach;
import static org.junit.jupiter.api.Assertions.*;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Test class to validate that all SplitQuery objects generate valid QueryAst JSON
 * with the required "type" field. This prevents regressions of the critical bug
 * where SplitQuery JSON was missing the type field, causing parsing failures.
 * 
 * Bug Report Reference: TANTIVY4JAVA_SPLITQUERY_JSON_BUG_REPORT.md
 * Error: "Failed to parse QueryAst JSON: missing field `type` at line 1 column 74"
 */
public class SplitQueryJSONValidationTest {
    
    private Schema schema;
    private ObjectMapper objectMapper;
    
    @BeforeEach
    public void setUp() {
        // Create test schema
        SchemaBuilder schemaBuilder = new SchemaBuilder();
        schemaBuilder.addTextField("title", true, false, "default", "position");
        schemaBuilder.addTextField("content", true, false, "default", "position");
        schemaBuilder.addIntegerField("price", true, true, false);
        schemaBuilder.addIntegerField("quantity", true, true, false);
        schemaBuilder.addBooleanField("available", true, true, false);
        schema = schemaBuilder.build();
        
        objectMapper = new ObjectMapper();
    }
    
    @Test
    public void testSplitTermQueryHasTypeField() throws Exception {
        SplitTermQuery termQuery = new SplitTermQuery("title", "test");
        String json = termQuery.toQueryAstJson();
        
        assertNotNull(json, "JSON should not be null");
        assertFalse(json.isEmpty(), "JSON should not be empty");
        
        // Parse JSON and validate structure
        JsonNode jsonNode = objectMapper.readTree(json);
        
        // Critical validation: type field must be present
        assertTrue(jsonNode.has("type"), "JSON must contain 'type' field");
        assertEquals("term", jsonNode.get("type").asText(), "Type field must be 'term'");
        
        // Validate other required fields for term queries
        assertTrue(jsonNode.has("field"), "JSON must contain 'field' field");
        assertTrue(jsonNode.has("value"), "JSON must contain 'value' field");
        assertEquals("title", jsonNode.get("field").asText(), "Field should match input");
        assertEquals("test", jsonNode.get("value").asText(), "Value should match input");
        
        System.out.println("✅ SplitTermQuery JSON validation passed: " + json);
    }
    
    @Test
    public void testSplitMatchAllQueryHasTypeField() throws Exception {
        SplitMatchAllQuery matchAllQuery = new SplitMatchAllQuery();
        String json = matchAllQuery.toQueryAstJson();
        
        assertNotNull(json, "JSON should not be null");
        assertFalse(json.isEmpty(), "JSON should not be empty");
        
        // Parse JSON and validate structure
        JsonNode jsonNode = objectMapper.readTree(json);
        
        // Critical validation: type field must be present
        assertTrue(jsonNode.has("type"), "JSON must contain 'type' field");
        assertEquals("match_all", jsonNode.get("type").asText(), "Type field must be 'match_all'");
        
        System.out.println("✅ SplitMatchAllQuery JSON validation passed: " + json);
    }
    
    @Test
    public void testSplitBooleanQueryHasTypeField() throws Exception {
        // Create boolean query with must and should clauses
        SplitBooleanQuery booleanQuery = new SplitBooleanQuery();
        booleanQuery.addMust(new SplitTermQuery("title", "hello"));
        booleanQuery.addShould(new SplitTermQuery("content", "world"));
        
        String json = booleanQuery.toQueryAstJson();
        
        assertNotNull(json, "JSON should not be null");
        assertFalse(json.isEmpty(), "JSON should not be empty");
        
        // Parse JSON and validate structure
        JsonNode jsonNode = objectMapper.readTree(json);
        
        // Critical validation: type field must be present
        assertTrue(jsonNode.has("type"), "JSON must contain 'type' field");
        assertEquals("bool", jsonNode.get("type").asText(), "Type field must be 'bool'");
        
        // Validate boolean query structure
        assertTrue(jsonNode.has("must") || jsonNode.has("should") || jsonNode.has("must_not"), 
                  "Boolean query must have at least one clause type");
        
        System.out.println("✅ SplitBooleanQuery JSON validation passed: " + json);
    }
    
    @Test
    public void testSplitRangeQueryHasTypeField() throws Exception {
        // Create range query with bounds
        SplitRangeQuery rangeQuery = new SplitRangeQuery("price", 
            SplitRangeQuery.RangeBound.inclusive("10"), 
            SplitRangeQuery.RangeBound.exclusive("100"), 
            "i64");
        
        String json = rangeQuery.toQueryAstJson();
        
        assertNotNull(json, "JSON should not be null");
        assertFalse(json.isEmpty(), "JSON should not be empty");
        
        // Parse JSON and validate structure
        JsonNode jsonNode = objectMapper.readTree(json);
        
        // Critical validation: type field must be present
        assertTrue(jsonNode.has("type"), "JSON must contain 'type' field");
        assertEquals("range", jsonNode.get("type").asText(), "Type field must be 'range'");
        
        // Validate range query structure
        assertTrue(jsonNode.has("field"), "Range query must contain 'field' field");
        assertEquals("price", jsonNode.get("field").asText(), "Field should match input");
        
        // Should have bounds (Quickwit uses lower_bound and upper_bound)
        assertTrue(jsonNode.has("lower_bound") || jsonNode.has("upper_bound"),
                  "Range query must have at least one bound");
        
        System.out.println("✅ SplitRangeQuery JSON validation passed: " + json);
    }
    
    @Test
    public void testMultipleSplitQueriesAllHaveTypeField() throws Exception {
        // Test multiple instances to ensure consistency
        SplitQuery[] queries = {
            new SplitTermQuery("title", "regression"),
            new SplitTermQuery("content", "test"),
            new SplitMatchAllQuery(),
            new SplitBooleanQuery() // Empty boolean query
        };
        
        for (int i = 0; i < queries.length; i++) {
            String json = queries[i].toQueryAstJson();
            JsonNode jsonNode = objectMapper.readTree(json);
            
            assertTrue(jsonNode.has("type"), 
                      String.format("Query %d (%s) must contain 'type' field. JSON: %s", 
                                  i, queries[i].getClass().getSimpleName(), json));
            
            String typeValue = jsonNode.get("type").asText();
            assertNotNull(typeValue, "Type field value must not be null");
            assertFalse(typeValue.isEmpty(), "Type field value must not be empty");
            
            System.out.println(String.format("✅ Query %d (%s) has valid type '%s': %s", 
                              i, queries[i].getClass().getSimpleName(), typeValue, json));
        }
    }
    
    @Test
    public void testJSONStructureValidation() throws Exception {
        // Comprehensive validation of JSON structure
        SplitTermQuery termQuery = new SplitTermQuery("title", "validation");
        String json = termQuery.toQueryAstJson();
        
        // Validate JSON is well-formed
        JsonNode jsonNode = objectMapper.readTree(json);
        assertNotNull(jsonNode, "JSON must be parseable");
        
        // Validate it's a JSON object, not array or primitive
        assertTrue(jsonNode.isObject(), "Root JSON must be an object");
        
        // Validate all required fields are strings (except for complex values)
        assertTrue(jsonNode.get("type").isTextual(), "Type field must be a string");
        assertTrue(jsonNode.get("field").isTextual(), "Field field must be a string");
        assertTrue(jsonNode.get("value").isTextual(), "Value field must be a string");
        
        // Validate no unexpected fields (this prevents accidentally including debug info)
        assertEquals(3, jsonNode.size(), "Term query JSON should have exactly 3 fields");
        
        System.out.println("✅ JSON structure validation passed: " + json);
    }
    
    @Test 
    public void testBugRegressionValidation() throws Exception {
        // This test specifically validates the bug that was fixed:
        // "Failed to parse QueryAst JSON: missing field `type`"
        
        SplitTermQuery termQuery = new SplitTermQuery("content", "engine");
        String json = termQuery.toQueryAstJson();
        
        // The original bug produced JSON like: {"field": "review_text", "value": "engine"}
        // After the fix it should be: {"type": "term", "field": "review_text", "value": "engine"}
        
        assertTrue(json.contains("\"type\""), 
                  "JSON must contain 'type' field to prevent regression of critical bug");
        assertTrue(json.contains("\"term\""), 
                  "JSON must contain 'term' type value");
        
        // Validate the exact structure that caused the bug is no longer present
        assertFalse(json.matches("^\\{\"field\":\\s*\"[^\"]+\",\\s*\"value\":\\s*\"[^\"]+\"\\}$"),
                   "JSON must not match the buggy format that was missing type field");
        
        // Validate the corrected format is present
        JsonNode jsonNode = objectMapper.readTree(json);
        assertEquals("term", jsonNode.get("type").asText());
        assertEquals("content", jsonNode.get("field").asText());
        assertEquals("engine", jsonNode.get("value").asText());
        
        System.out.println("✅ Bug regression validation passed - type field present: " + json);
    }
}
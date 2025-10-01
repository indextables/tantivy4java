package io.indextables.tantivy4java;

import io.indextables.tantivy4java.query.*;
import io.indextables.tantivy4java.split.*;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeEach;
import static org.junit.jupiter.api.Assertions.*;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Critical regression test for SplitQuery JSON serialization bug.
 * 
 * This test validates that all SplitQuery objects generate valid QueryAst JSON
 * with the required "type" field, preventing regression of the critical bug:
 * 
 * Bug: "Failed to parse QueryAst JSON: missing field `type` at line 1 column 74"
 * 
 * Before Fix: {"field": "review_text", "value": "engine"}
 * After Fix:  {"type": "term", "field": "review_text", "value": "engine"}
 * 
 * Reference: TANTIVY4JAVA_SPLITQUERY_JSON_BUG_REPORT.md
 */
public class SplitQueryTypeFieldValidationTest {
    
    private ObjectMapper objectMapper;
    
    @BeforeEach
    public void setUp() {
        objectMapper = new ObjectMapper();
    }
    
    @Test
    public void testSplitTermQueryTypeField() throws Exception {
        SplitTermQuery termQuery = new SplitTermQuery("title", "test");
        String json = termQuery.toQueryAstJson();
        
        assertNotNull(json, "JSON should not be null");
        JsonNode jsonNode = objectMapper.readTree(json);
        
        // CRITICAL: Validate type field is present and correct
        assertTrue(jsonNode.has("type"), "❌ REGRESSION: JSON missing 'type' field");
        assertEquals("term", jsonNode.get("type").asText(), "Type field must be 'term'");
        
        // Validate complete structure
        assertTrue(jsonNode.has("field"), "JSON must contain 'field' field");
        assertTrue(jsonNode.has("value"), "JSON must contain 'value' field");
        
        System.out.println("✅ SplitTermQuery type field validation PASSED: " + json);
    }
    
    @Test
    public void testSplitMatchAllQueryTypeField() throws Exception {
        SplitMatchAllQuery matchAllQuery = new SplitMatchAllQuery();
        String json = matchAllQuery.toQueryAstJson();
        
        assertNotNull(json, "JSON should not be null");
        JsonNode jsonNode = objectMapper.readTree(json);
        
        // CRITICAL: Validate type field is present and correct
        assertTrue(jsonNode.has("type"), "❌ REGRESSION: JSON missing 'type' field");
        assertEquals("match_all", jsonNode.get("type").asText(), "Type field must be 'match_all'");
        
        System.out.println("✅ SplitMatchAllQuery type field validation PASSED: " + json);
    }
    
    @Test
    public void testSplitBooleanQueryTypeField() throws Exception {
        SplitBooleanQuery booleanQuery = new SplitBooleanQuery();
        booleanQuery.addMust(new SplitTermQuery("title", "hello"));
        
        String json = booleanQuery.toQueryAstJson();
        
        assertNotNull(json, "JSON should not be null");
        JsonNode jsonNode = objectMapper.readTree(json);
        
        // CRITICAL: Validate type field is present and correct
        assertTrue(jsonNode.has("type"), "❌ REGRESSION: JSON missing 'type' field");
        assertEquals("bool", jsonNode.get("type").asText(), "Type field must be 'bool'");
        
        System.out.println("✅ SplitBooleanQuery type field validation PASSED: " + json);
    }
    
    @Test
    public void testRegressionPreventionExactBugScenario() throws Exception {
        // This test replicates the exact scenario from the bug report
        SplitTermQuery termQuery = new SplitTermQuery("review_text", "engine");
        String json = termQuery.toQueryAstJson();
        
        // Parse JSON
        JsonNode jsonNode = objectMapper.readTree(json);
        
        // CRITICAL BUG PREVENTION: The original bug produced JSON like:
        // {"field": "review_text", "value": "engine"}
        // This MUST NOT happen again!
        
        // 1. Validate type field exists
        assertTrue(jsonNode.has("type"), 
                  "❌ CRITICAL REGRESSION: Missing 'type' field - exact same bug as reported!");
        
        // 2. Validate type field value is correct
        assertEquals("term", jsonNode.get("type").asText(), 
                    "Type field must be 'term' for term queries");
        
        // 3. Validate field and value are present
        assertTrue(jsonNode.has("field"), "JSON must contain 'field' field");
        assertTrue(jsonNode.has("value"), "JSON must contain 'value' field");
        assertEquals("review_text", jsonNode.get("field").asText(), "Field should be 'review_text'");
        assertEquals("engine", jsonNode.get("value").asText(), "Value should be 'engine'");
        
        // 4. Validate the JSON does NOT match the buggy pattern
        assertFalse(json.matches("^\\{\"field\":[^,]+,\"value\":[^}]+\\}$"),
                   "❌ CRITICAL: JSON matches the original buggy format without type field!");
        
        // 5. Validate the JSON DOES match the correct pattern  
        assertTrue(json.contains("\"type\":\"term\"") || json.contains("\"type\": \"term\""),
                  "JSON must contain correct type field");
        
        System.out.println("✅ CRITICAL BUG REGRESSION TEST PASSED: " + json);
        System.out.println("✅ Original bug scenario successfully prevented!");
    }
    
    @Test
    public void testAllQueryTypesHaveTypeField() throws Exception {
        // Test all major SplitQuery types to ensure comprehensive coverage
        SplitQuery[] queries = {
            new SplitTermQuery("field1", "value1"),
            new SplitTermQuery("field2", "value2"),
            new SplitMatchAllQuery()
        };
        
        for (int i = 0; i < queries.length; i++) {
            String json = queries[i].toQueryAstJson();
            JsonNode jsonNode = objectMapper.readTree(json);
            
            // Every single SplitQuery MUST have a type field
            assertTrue(jsonNode.has("type"), 
                      String.format("❌ REGRESSION: Query %d (%s) missing 'type' field. JSON: %s", 
                                  i, queries[i].getClass().getSimpleName(), json));
            
            String typeValue = jsonNode.get("type").asText();
            assertNotNull(typeValue, "Type field value must not be null");
            assertFalse(typeValue.isEmpty(), "Type field value must not be empty");
            
            System.out.println(String.format("✅ Query %d (%s) type validation PASSED: %s", 
                              i, queries[i].getClass().getSimpleName(), json));
        }
    }
    
    @Test
    public void testJSONWellFormedness() throws Exception {
        // Ensure all generated JSON is well-formed and parseable
        SplitTermQuery termQuery = new SplitTermQuery("test_field", "test_value");
        String json = termQuery.toQueryAstJson();
        
        // Should parse without throwing exception
        JsonNode jsonNode = objectMapper.readTree(json);
        assertNotNull(jsonNode, "JSON must be parseable");
        assertTrue(jsonNode.isObject(), "JSON must be an object");
        
        // Should have exactly the expected fields for term query (no extra debug fields)
        assertEquals(3, jsonNode.size(), "Term query should have exactly 3 fields: type, field, value");
        
        System.out.println("✅ JSON well-formedness validation PASSED: " + json);
    }
}

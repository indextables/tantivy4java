package com.tantivy4java;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

public class JsonDebugTest {

    @Test
    void testToJsonOnly() {
        System.out.println("Creating schema builder...");
        SchemaBuilder builder = Schema.builder();
        builder.addTextField("title", FieldType.TEXT);
        
        System.out.println("Building schema...");
        try (Schema schema = builder.build()) {
            System.out.println("Schema built, calling toJson...");
            String json = schema.toJson();
            System.out.println("toJson returned: " + json);
            assertNotNull(json);
        }
        System.out.println("Test completed");
    }

    @Test
    void testFromJsonOnly() {
        System.out.println("Testing fromJson with simple JSON...");
        try (Schema schema = Schema.fromJson("{}")) {
            System.out.println("fromJson completed");
            assertNotNull(schema);
        }
        System.out.println("fromJson test completed");
    }

    @Test
    void testRoundTripJson() {
        System.out.println("Testing round-trip JSON...");
        
        SchemaBuilder builder = Schema.builder();
        builder.addTextField("title", FieldType.TEXT);
        
        try (Schema schema = builder.build()) {
            System.out.println("Original schema built");
            
            String json = schema.toJson();
            System.out.println("toJson returned: " + json);
            
            System.out.println("Calling fromJson with returned JSON...");
            try (Schema deserializedSchema = Schema.fromJson(json)) {
                System.out.println("fromJson completed successfully");
                assertNotNull(deserializedSchema);
                
                System.out.println("Testing getField on deserialized schema...");
                Field titleField = deserializedSchema.getField("title");
                System.out.println("getField completed: " + (titleField != null ? "found" : "null"));
            }
        }
        System.out.println("Round-trip test completed");
    }

    @Test
    void testExactSchemaTestPattern() {
        System.out.println("Testing exact SchemaTest pattern...");
        
        Schema schema = null;
        try {
            SchemaBuilder builder = Schema.builder();
            builder.addTextField("title", FieldType.TEXT)
                   .addTextField("body", FieldType.TEXT);
            
            schema = builder.build();
            System.out.println("Schema built");
            
            String json = schema.toJson();
            System.out.println("toJson returned: " + json);
            assertNotNull(json);
            assertFalse(json.isEmpty());
            
            System.out.println("Creating deserializedSchema from JSON...");
            try (Schema deserializedSchema = Schema.fromJson(json)) {
                System.out.println("deserializedSchema created");
                assertNotNull(deserializedSchema);
                
                System.out.println("Getting title field...");
                Field titleField = deserializedSchema.getField("title");
                System.out.println("Title field: " + (titleField != null ? "found" : "null"));
                assertNotNull(titleField);
                
                System.out.println("Getting body field...");
                Field bodyField = deserializedSchema.getField("body");
                System.out.println("Body field: " + (bodyField != null ? "found" : "null"));
                assertNotNull(bodyField);
            }
            System.out.println("deserializedSchema closed");
        } finally {
            if (schema != null) {
                System.out.println("Closing original schema...");
                schema.close();
                System.out.println("Original schema closed");
            }
        }
        System.out.println("Exact SchemaTest pattern completed");
    }
}
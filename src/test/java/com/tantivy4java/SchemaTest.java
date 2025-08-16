package com.tantivy4java;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.AfterEach;
import static org.junit.jupiter.api.Assertions.*;

public class SchemaTest {

    private Schema schema;

    @AfterEach
    void tearDown() {
        if (schema != null) {
            schema.close();
        }
    }

    @Test
    void testSchemaBuilder() {
        SchemaBuilder builder = Schema.builder();
        assertNotNull(builder);
        
        builder.addTextField("title", FieldType.TEXT)
               .addTextField("body", FieldType.TEXT)
               .addIntField("score", FieldType.INTEGER)
               .addFloatField("rating", FieldType.FLOAT)
               .addDateField("created_at", FieldType.DATE)
               .addBytesField("data", FieldType.BYTES);
        
        schema = builder.build();
        assertNotNull(schema);
    }

    @Test
    void testSchemaFields() {
        SchemaBuilder builder = Schema.builder();
        builder.addTextField("title", FieldType.TEXT)
               .addIntField("count", FieldType.INTEGER);
        
        schema = builder.build();
        
        Field titleField = schema.getField("title");
        assertNotNull(titleField);
        assertEquals("title", titleField.getName());
        
        Field countField = schema.getField("count");
        assertNotNull(countField);
        assertEquals("count", countField.getName());
        
        Field nonExistentField = schema.getField("non_existent");
        assertNull(nonExistentField);
    }

    @Test
    void testSchemaJsonSerialization() {
        SchemaBuilder builder = Schema.builder();
        builder.addTextField("title", FieldType.TEXT)
               .addTextField("body", FieldType.TEXT);
        
        schema = builder.build();
        
        String json = schema.toJson();
        assertNotNull(json);
        assertFalse(json.isEmpty());
        
        try (Schema deserializedSchema = Schema.fromJson(json)) {
            assertNotNull(deserializedSchema);
            
            Field titleField = deserializedSchema.getField("title");
            assertNotNull(titleField);
            
            Field bodyField = deserializedSchema.getField("body");
            assertNotNull(bodyField);
        }
    }

    @Test
    void testFieldTypes() {
        SchemaBuilder builder = Schema.builder();
        builder.addTextField("text_field", FieldType.TEXT)
               .addIntField("int_field", FieldType.INTEGER)
               .addUIntField("uint_field", FieldType.UNSIGNED_INTEGER)
               .addFloatField("float_field", FieldType.FLOAT)
               .addDateField("date_field", FieldType.DATE)
               .addBytesField("bytes_field", FieldType.BYTES);
        
        schema = builder.build();
        
        Field textField = schema.getField("text_field");
        assertNotNull(textField);
        assertEquals(FieldType.TEXT, textField.getType());
        
        Field intField = schema.getField("int_field");
        assertNotNull(intField);
        assertEquals(FieldType.INTEGER, intField.getType());
        
        Field uintField = schema.getField("uint_field");
        assertNotNull(uintField);
        assertEquals(FieldType.UNSIGNED_INTEGER, uintField.getType());
        
        Field floatField = schema.getField("float_field");
        assertNotNull(floatField);
        assertEquals(FieldType.FLOAT, floatField.getType());
        
        Field dateField = schema.getField("date_field");
        assertNotNull(dateField);
        assertEquals(FieldType.DATE, dateField.getType());
        
        Field bytesField = schema.getField("bytes_field");
        assertNotNull(bytesField);
        assertEquals(FieldType.BYTES, bytesField.getType());
    }
}
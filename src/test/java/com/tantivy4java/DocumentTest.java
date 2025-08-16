package com.tantivy4java;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.AfterEach;
import static org.junit.jupiter.api.Assertions.*;

import java.time.Instant;

public class DocumentTest {

    private Schema schema;
    private Field textField;
    private Field intField;
    private Field uintField;
    private Field floatField;
    private Field dateField;
    private Field bytesField;

    @BeforeEach
    void setUp() {
        SchemaBuilder builder = Schema.builder();
        builder.addTextField("text", FieldType.TEXT)
               .addIntField("int", FieldType.INTEGER)
               .addUIntField("uint", FieldType.UNSIGNED_INTEGER)
               .addFloatField("float", FieldType.FLOAT)
               .addDateField("date", FieldType.DATE)
               .addBytesField("bytes", FieldType.BYTES);
        
        schema = builder.build();
        textField = schema.getField("text");
        intField = schema.getField("int");
        uintField = schema.getField("uint");
        floatField = schema.getField("float");
        dateField = schema.getField("date");
        bytesField = schema.getField("bytes");
    }

    @AfterEach
    void tearDown() {
        if (schema != null) {
            schema.close();
        }
    }

    @Test
    void testDocumentCreation() {
        try (Document doc = new Document()) {
            assertNotNull(doc);
        }
    }

    @Test
    void testAddAndGetText() {
        try (Document doc = new Document()) {
            String textValue = "Hello, World!";
            doc.addText(textField, textValue);
            
            String retrievedValue = doc.getText(textField);
            assertEquals(textValue, retrievedValue);
        }
    }

    @Test
    void testAddAndGetInteger() {
        try (Document doc = new Document()) {
            long intValue = 42L;
            doc.addInteger(intField, intValue);
            
            long retrievedValue = doc.getInteger(intField);
            assertEquals(intValue, retrievedValue);
        }
    }

    @Test
    void testAddAndGetUnsignedInteger() {
        try (Document doc = new Document()) {
            long uintValue = 123456789L;
            doc.addUInteger(uintField, uintValue);
            
            long retrievedValue = doc.getUInteger(uintField);
            assertEquals(uintValue, retrievedValue);
        }
    }

    @Test
    void testAddAndGetFloat() {
        try (Document doc = new Document()) {
            double floatValue = 3.14159;
            doc.addFloat(floatField, floatValue);
            
            double retrievedValue = doc.getFloat(floatField);
            assertEquals(floatValue, retrievedValue, 0.00001);
        }
    }

    @Test
    void testAddAndGetDate() {
        try (Document doc = new Document()) {
            long timestamp = Instant.now().getEpochSecond();
            doc.addDate(dateField, timestamp);
            
            long retrievedValue = doc.getDate(dateField);
            assertEquals(timestamp, retrievedValue);
        }
    }

    @Test
    void testAddAndGetBytes() {
        try (Document doc = new Document()) {
            byte[] bytesValue = {1, 2, 3, 4, 5};
            doc.addBytes(bytesField, bytesValue);
            
            byte[] retrievedValue = doc.getBytes(bytesField);
            assertArrayEquals(bytesValue, retrievedValue);
        }
    }

    @Test
    void testMultipleFieldValues() {
        try (Document doc = new Document()) {
            doc.addText(textField, "First text");
            doc.addText(textField, "Second text");
            
            String retrievedValue = doc.getText(textField);
            assertNotNull(retrievedValue);
            assertTrue(retrievedValue.equals("First text") || retrievedValue.equals("Second text"));
        }
    }

    @Test
    void testComplexDocument() {
        try (Document doc = new Document()) {
            doc.addText(textField, "Complex document");
            doc.addInteger(intField, 100L);
            doc.addUInteger(uintField, 200L);
            doc.addFloat(floatField, 1.5);
            doc.addDate(dateField, Instant.now().getEpochSecond());
            doc.addBytes(bytesField, new byte[]{10, 20, 30});
            
            assertEquals("Complex document", doc.getText(textField));
            assertEquals(100L, doc.getInteger(intField));
            assertEquals(200L, doc.getUInteger(uintField));
            assertEquals(1.5, doc.getFloat(floatField), 0.001);
            assertTrue(doc.getDate(dateField) > 0);
            assertArrayEquals(new byte[]{10, 20, 30}, doc.getBytes(bytesField));
        }
    }
}
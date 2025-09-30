package com.tantivy4java;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;
import static org.junit.jupiter.api.Assertions.*;

import java.time.LocalDateTime;

/**
 * Test to reproduce the date type mismatch panic reported in the bug.
 * This tests what happens when a DATE value is added to a non-DATE field.
 */
public class BatchDateTypeMismatchTest {

    @Test
    @DisplayName("Test adding DATE value to INTEGER field via batch (should fail gracefully)")
    public void testDateValueToIntegerFieldBatch() {
        System.out.println("=== Testing DATE value to INTEGER field (batch) ===");

        // Create schema with INTEGER field (not DATE)
        SchemaBuilder builder = new SchemaBuilder();
        builder.addIntegerField("timestamp", true, true, false);
        Schema schema = builder.build();

        Index index = new Index(schema, "", false);
        IndexWriter writer = index.writer(Index.Memory.DEFAULT_HEAP_SIZE, 1);

        try {
            // Try to add DATE value to INTEGER field via batch
            BatchDocument doc = new BatchDocument();
            doc.addDate("timestamp", LocalDateTime.now());  // WRONG TYPE!

            BatchDocumentBuilder batchBuilder = new BatchDocumentBuilder();
            batchBuilder.addDocument(doc);

            // This should now throw a proper exception instead of panicking
            long[] ops = writer.addDocumentsBatch(batchBuilder);

            System.out.println("Unexpectedly succeeded: " + ops.length);
            fail("Should have thrown an exception for type mismatch");

        } catch (RuntimeException e) {
            // Expected - should fail gracefully with an exception, not panic
            System.out.println("✓ Correctly threw exception: " + e.getMessage());
            assertTrue(e.getMessage().contains("Type mismatch") || e.getMessage().contains("DATE"),
                      "Exception should mention type mismatch, got: " + e.getMessage());
        } finally {
            writer.close();
            index.close();
        }
    }

    @Test
    @DisplayName("Test adding INTEGER value to DATE field via batch (reverse mismatch)")
    public void testIntegerValueToDateFieldBatch() {
        System.out.println("=== Testing INTEGER value to DATE field (batch) ===");

        // Create schema with DATE field
        SchemaBuilder builder = new SchemaBuilder();
        builder.addDateField("timestamp", true, true, false);
        Schema schema = builder.build();

        Index index = new Index(schema, "", false);
        IndexWriter writer = index.writer(Index.Memory.DEFAULT_HEAP_SIZE, 1);

        try {
            // Try to add INTEGER value to DATE field via batch
            BatchDocument doc = new BatchDocument();
            doc.addInteger("timestamp", System.currentTimeMillis());  // WRONG TYPE!

            BatchDocumentBuilder batchBuilder = new BatchDocumentBuilder();
            batchBuilder.addDocument(doc);

            // This should now throw a proper exception instead of panicking
            long[] ops = writer.addDocumentsBatch(batchBuilder);

            System.out.println("Unexpectedly succeeded: " + ops.length);
            fail("Should have thrown an exception for type mismatch");

        } catch (RuntimeException e) {
            // Expected - should fail gracefully with an exception, not panic
            System.out.println("✓ Correctly threw exception: " + e.getMessage());
            assertTrue(e.getMessage().contains("Type mismatch") || e.getMessage().contains("INTEGER"),
                      "Exception should mention type mismatch, got: " + e.getMessage());
        } finally {
            writer.close();
            index.close();
        }
    }

    @Test
    @DisplayName("Test correct DATE value to DATE field via batch (should succeed)")
    public void testCorrectDateValueBatch() {
        System.out.println("=== Testing correct DATE value to DATE field (batch) ===");

        // Create schema with DATE field
        SchemaBuilder builder = new SchemaBuilder();
        builder.addDateField("timestamp", true, true, false);
        Schema schema = builder.build();

        Index index = new Index(schema, "", false);
        IndexWriter writer = index.writer(Index.Memory.DEFAULT_HEAP_SIZE, 1);

        try {
            // Add correct DATE value to DATE field via batch
            BatchDocument doc = new BatchDocument();
            doc.addDate("timestamp", LocalDateTime.now());  // CORRECT TYPE!

            BatchDocumentBuilder batchBuilder = new BatchDocumentBuilder();
            batchBuilder.addDocument(doc);

            long[] ops = writer.addDocumentsBatch(batchBuilder);

            assertEquals(1, ops.length);
            System.out.println("✓ Correctly succeeded with proper type");

        } catch (Exception e) {
            fail("Should not have thrown exception for correct type: " + e.getMessage());
        } finally {
            writer.close();
            index.close();
        }
    }
}
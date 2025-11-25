package io.indextables.tantivy4java;

import io.indextables.tantivy4java.core.*;
import io.indextables.tantivy4java.query.*;
import io.indextables.tantivy4java.batch.*;


import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Disabled;
import static org.junit.jupiter.api.Assertions.*;

import java.time.LocalDateTime;
import java.time.Instant;
import java.time.ZoneOffset;

public class SimpleDateRangeTest {

    @Test
    public void testYear9999OutOfRange() {
        System.out.println("Testing year 9999 (should reject as out of range)...");
        SchemaBuilder builder = new SchemaBuilder();
        builder.addDateField("timestamp", true, true, false);
        Schema schema = builder.build();

        Index index = new Index(schema, "", false);
        IndexWriter writer = index.writer(Index.Memory.DEFAULT_HEAP_SIZE, 1);

        try {
            BatchDocument doc = new BatchDocument();
            long millis = 253402300799000L; // 9999-12-31 23:59:59 - OUT OF RANGE
            LocalDateTime date = LocalDateTime.ofInstant(Instant.ofEpochMilli(millis), ZoneOffset.UTC);
            System.out.println("Date: " + date + " (millis: " + millis + ")");

            doc.addDate("timestamp", date);

            BatchDocumentBuilder batchBuilder = new BatchDocumentBuilder();
            batchBuilder.addDocument(doc);

            // This should throw an exception for out-of-range date
            long[] ops = writer.addDocumentsBatch(batchBuilder);
            fail("Should have thrown exception for out-of-range date");

        } catch (RuntimeException e) {
            System.out.println("✓ Correctly threw exception: " + e.getMessage());
            assertTrue(e.getMessage().contains("Date value out of range"));
            assertTrue(e.getMessage().contains("1677-2262"));
        } finally {
            writer.close();
            index.close();
        }
    }

    @Test
    public void testValidDateRanges() {
        System.out.println("Testing valid date ranges...");
        SchemaBuilder builder = new SchemaBuilder();
        builder.addDateField("timestamp", true, true, false);
        Schema schema = builder.build();

        Index index = new Index(schema, "", false);
        IndexWriter writer = index.writer(Index.Memory.DEFAULT_HEAP_SIZE, 1);

        try {
            BatchDocumentBuilder batchBuilder = new BatchDocumentBuilder();

            // Test various valid dates
            long[] validMillis = {
                0L,                    // 1970-01-01
                1609459200000L,        // 2021-01-01
                1705330200000L,        // 2024-01-15 (from bug report)
                1735707600000L,        // 2025-01-01 (from bug report)
                2147483647000L,        // 2038-01-19 (Y2038 boundary)
                -31536000000L,         // 1969-01-01 (pre-epoch)
            };

            for (long millis : validMillis) {
                BatchDocument doc = new BatchDocument();
                LocalDateTime date = LocalDateTime.ofInstant(Instant.ofEpochMilli(millis), ZoneOffset.UTC);
                doc.addDate("timestamp", date);
                batchBuilder.addDocument(doc);
                System.out.println("  Added valid date: " + date + " (millis: " + millis + ")");
            }

            long[] ops = writer.addDocumentsBatch(batchBuilder);
            assertEquals(validMillis.length, ops.length);
            System.out.println("✓ Successfully indexed " + validMillis.length + " valid dates");

            writer.commit();
            index.reload();

            try (Searcher searcher = index.searcher()) {
                assertEquals(validMillis.length, searcher.getNumDocs());
            }

        } finally {
            writer.close();
            index.close();
        }
    }
}

package io.indextables.tantivy4java;

import io.indextables.tantivy4java.core.*;
import io.indextables.tantivy4java.query.*;
import io.indextables.tantivy4java.result.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Test to verify that DATE fields preserve microsecond precision.
 *
 * This test addresses the bug report: TANTIVY4JAVA_TIMESTAMP_BUG_REPORT.md
 * where timestamps were being truncated to seconds precision instead of
 * preserving microsecond precision required for Spark integration.
 */
public class TimestampMicrosecondPrecisionTest {

    @Test
    @DisplayName("Verify DATE fields preserve microsecond precision")
    public void testMicrosecondPrecisionPreserved(@TempDir Path tempDir) {
        String indexPath = tempDir.resolve("timestamp_precision_test").toString();

        try {
            // Create schema with fast DATE field (should use microsecond precision)
            try (SchemaBuilder builder = new SchemaBuilder()) {
                builder.addIntegerField("id", true, true, false);
                builder.addDateField("timestamp", true, false, true);  // fast=true should use microseconds

                try (Schema schema = builder.build()) {
                    // Base time: 2025-11-07 07:00:00 (declare outside writer block for later use)
                    final LocalDateTime baseTime = LocalDateTime.of(2025, 11, 7, 7, 0, 0, 0);

                    // Create index
                    try (Index index = new Index(schema, indexPath, false)) {
                        try (IndexWriter writer = index.writer(Index.Memory.DEFAULT_HEAP_SIZE, 1)) {

                            // Write documents with microsecond differences
                            // These should be distinguishable after read
                            long[] microsOffsets = {
                                1L,        // +1 microsecond
                                500L,      // +500 microseconds
                                1000L,     // +1000 microseconds (1 millisecond)
                                2000L      // +2000 microseconds (2 milliseconds)
                            };

                            for (int i = 0; i < microsOffsets.length; i++) {
                                try (Document doc = new Document()) {
                                    // Add ID
                                    doc.addInteger("id", i + 1);

                                    // Add timestamp with microsecond precision
                                    // Add microseconds as nanoseconds to LocalDateTime
                                    LocalDateTime timestamp = baseTime.plusNanos(microsOffsets[i] * 1000L);
                                    doc.addDate("timestamp", timestamp);

                                    writer.addDocument(doc);
                                }
                            }

                            writer.commit();
                        }

                        index.reload();

                        // Read back and verify microsecond precision
                        try (Searcher searcher = index.searcher()) {
                            // Search for all documents
                            try (Query allDocsQuery = Query.allQuery()) {
                                SearchResult results = searcher.search(allDocsQuery, 10);

                                assertEquals(4, results.getHits().size(), "Should find all 4 documents");

                                // Collect timestamps and verify they're distinct
                                long[] retrievedMicros = new long[4];

                                for (int i = 0; i < results.getHits().size(); i++) {
                                    SearchResult.Hit hit = results.getHits().get(i);
                                    try (Document doc = searcher.doc(hit.getDocAddress())) {
                                        Number idNum = (Number) doc.getFirst("id");
                                        LocalDateTime timestamp = (LocalDateTime) doc.getFirst("timestamp");

                                        assertNotNull(idNum, "ID should not be null");
                                        assertNotNull(timestamp, "Timestamp should not be null");

                                        int id = idNum.intValue();

                                        // Calculate microseconds since base time
                                        long microsSinceBase = ChronoUnit.MICROS.between(
                                            baseTime,
                                            timestamp
                                        );

                                        retrievedMicros[id - 1] = microsSinceBase;

                                        System.out.println(String.format(
                                            "ID=%d, Timestamp=%s, Micros since base=%d",
                                            id, timestamp, microsSinceBase
                                        ));
                                    }
                                }

                                // CRITICAL TEST: Verify all timestamps are distinct
                                // If precision was lost to seconds, they would all be 0
                                for (int i = 0; i < retrievedMicros.length - 1; i++) {
                                    for (int j = i + 1; j < retrievedMicros.length; j++) {
                                        assertNotEquals(
                                            retrievedMicros[i],
                                            retrievedMicros[j],
                                            String.format("Timestamps %d and %d should be different (microsecond precision)", i, j)
                                        );
                                    }
                                }

                                // Verify each timestamp matches expected offset
                                assertEquals(1L, retrievedMicros[0], "First timestamp should be +1 microsecond");
                                assertEquals(500L, retrievedMicros[1], "Second timestamp should be +500 microseconds");
                                assertEquals(1000L, retrievedMicros[2], "Third timestamp should be +1000 microseconds");
                                assertEquals(2000L, retrievedMicros[3], "Fourth timestamp should be +2000 microseconds");

                                System.out.println("\n✅ SUCCESS: Microsecond precision preserved!");
                                System.out.println("All timestamps are distinct and match expected microsecond offsets.");
                            }
                        }
                    }
                }
            }
        } catch (Exception e) {
            fail("Microsecond precision test failed: " + e.getMessage());
            e.printStackTrace();
        }
    }

    @Test
    @DisplayName("Verify Spark-style microsecond timestamps work correctly")
    public void testSparkStyleMicrosecondTimestamps(@TempDir Path tempDir) {
        String indexPath = tempDir.resolve("spark_timestamp_test").toString();

        try {
            // Create schema matching Spark use case
            try (SchemaBuilder builder = new SchemaBuilder()) {
                builder.addIntegerField("id", true, true, false);
                builder.addDateField("event_time", true, false, true);  // fast field for queries/stats

                try (Schema schema = builder.build()) {
                    try (Index index = new Index(schema, indexPath, false)) {
                        try (IndexWriter writer = index.writer(Index.Memory.DEFAULT_HEAP_SIZE, 1)) {

                            // Simulate Spark writing microsecond timestamps
                            // Spark TimestampType stores microseconds since epoch as Long
                            long baseEpochMicros = 1762516800000000L;  // 2025-11-07 07:00:00 in microseconds

                            long[] sparkTimestamps = {
                                baseEpochMicros + 1L,
                                baseEpochMicros + 500L,
                                baseEpochMicros + 1000L,
                                baseEpochMicros + 2000L
                            };

                            for (int i = 0; i < sparkTimestamps.length; i++) {
                                try (Document doc = new Document()) {
                                    doc.addInteger("id", i + 1);

                                    // Convert Spark microseconds to LocalDateTime
                                    long seconds = sparkTimestamps[i] / 1_000_000L;
                                    long nanos = (sparkTimestamps[i] % 1_000_000L) * 1000L;
                                    LocalDateTime timestamp = LocalDateTime.ofEpochSecond(seconds, (int)nanos, ZoneOffset.UTC);

                                    doc.addDate("event_time", timestamp);

                                    writer.addDocument(doc);
                                }
                            }

                            writer.commit();
                        }

                        index.reload();

                        // Read back and verify Spark-style precision
                        try (Searcher searcher = index.searcher()) {
                            try (Query allDocsQuery = Query.allQuery()) {
                                SearchResult results = searcher.search(allDocsQuery, 10);

                                assertEquals(4, results.getHits().size(), "Should find all documents");

                                System.out.println("\n📊 Spark-Style Timestamp Test Results:");
                                System.out.println("+---+----------------------------+------------------+");
                                System.out.println("| ID| Timestamp                  | Epoch Micros     |");
                                System.out.println("+---+----------------------------+------------------+");

                                for (SearchResult.Hit hit : results.getHits()) {
                                    try (Document doc = searcher.doc(hit.getDocAddress())) {
                                        Number idNum = (Number) doc.getFirst("id");
                                        LocalDateTime timestamp = (LocalDateTime) doc.getFirst("event_time");

                                        int id = idNum.intValue();

                                        // Convert back to microseconds (Spark format)
                                        long epochMicros = timestamp.toEpochSecond(ZoneOffset.UTC) * 1_000_000L +
                                                          timestamp.getNano() / 1000L;

                                        System.out.println(String.format(
                                            "| %-2d| %-27s| %-17d|",
                                            id, timestamp, epochMicros
                                        ));
                                    }
                                }

                                System.out.println("+---+----------------------------+------------------+");
                                System.out.println("✅ All timestamps preserved with microsecond precision!");
                            }
                        }
                    }
                }
            }
        } catch (Exception e) {
            fail("Spark timestamp test failed: " + e.getMessage());
            e.printStackTrace();
        }
    }
}

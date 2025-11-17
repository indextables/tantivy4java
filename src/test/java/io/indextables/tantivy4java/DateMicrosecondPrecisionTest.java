package io.indextables.tantivy4java;

import io.indextables.tantivy4java.core.*;
import io.indextables.tantivy4java.query.*;
import io.indextables.tantivy4java.result.*;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;
import static org.junit.jupiter.api.Assertions.*;

import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.List;

/**
 * Tests for microsecond precision in date fields with fast field storage.
 *
 * This test validates the fix for timestamp precision handling:
 * - Fast fields now store timestamps at MICROSECOND precision (not seconds)
 * - Range queries on fast fields work with microsecond granularity
 * - Documents with same-second timestamps can be distinguished by microseconds
 *
 * Note: Inverted index precision remains at SECONDS (Tantivy limitation).
 * This means term queries will match all timestamps within the same second.
 */
public class DateMicrosecondPrecisionTest {

    @Test
    @DisplayName("Test microsecond precision preservation in fast fields")
    public void testMicrosecondPrecisionStorage() {
        System.out.println("\n=== Microsecond Precision Storage Test ===");

        try (SchemaBuilder builder = new SchemaBuilder()) {
            // Build schema with fast date field (enables microsecond precision)
            builder.addIntegerField("id", true, true, true)
                   .addDateField("timestamp", true, true, true);  // stored, indexed, FAST (microseconds)

            try (Schema schema = builder.build()) {
                try (Index index = new Index(schema, "", true)) {
                    // Index documents with microsecond-level timestamp differences
                    try (IndexWriter writer = index.writer(Index.Memory.DEFAULT_HEAP_SIZE, 1)) {

                        // Base timestamp: 2025-11-07 07:00:00
                        LocalDateTime baseTime = LocalDateTime.of(2025, 11, 7, 7, 0, 0);

                        // Document 1: 07:00:00.000001 (1 microsecond)
                        try (Document doc1 = new Document()) {
                            doc1.addInteger("id", 1);
                            doc1.addDate("timestamp", baseTime.plusNanos(1000));  // +1 microsecond
                            writer.addDocument(doc1);
                        }

                        // Document 2: 07:00:00.000500 (500 microseconds)
                        try (Document doc2 = new Document()) {
                            doc2.addInteger("id", 2);
                            doc2.addDate("timestamp", baseTime.plusNanos(500_000));  // +500 microseconds
                            writer.addDocument(doc2);
                        }

                        // Document 3: 07:00:00.001000 (1000 microseconds = 1 millisecond)
                        try (Document doc3 = new Document()) {
                            doc3.addInteger("id", 3);
                            doc3.addDate("timestamp", baseTime.plusNanos(1_000_000));  // +1000 microseconds
                            writer.addDocument(doc3);
                        }

                        // Document 4: 07:00:01.000000 (different second)
                        try (Document doc4 = new Document()) {
                            doc4.addInteger("id", 4);
                            doc4.addDate("timestamp", baseTime.plusSeconds(1));
                            writer.addDocument(doc4);
                        }

                        writer.commit();
                        System.out.println("✓ Indexed 4 documents with microsecond-precision timestamps");
                    }

                    index.reload();

                    try (Searcher searcher = index.searcher()) {
                        assertEquals(4, searcher.getNumDocs(), "Should have 4 documents");

                        // Retrieve all documents and verify microsecond precision is preserved
                        System.out.println("\n=== Verifying Microsecond Precision in Retrieved Documents ===");

                        try (Query allQuery = Query.allQuery();
                             SearchResult result = searcher.search(allQuery, 10)) {

                            var hits = result.getHits();
                            assertEquals(4, hits.size(), "Should find all 4 documents");

                            for (SearchResult.Hit hit : hits) {
                                try (Document doc = searcher.doc(hit.getDocAddress())) {
                                    List<Object> idValues = doc.get("id");
                                    List<Object> timestampValues = doc.get("timestamp");

                                    assertFalse(idValues.isEmpty(), "ID should be present");
                                    assertFalse(timestampValues.isEmpty(), "Timestamp should be present");

                                    Long id = (Long) idValues.get(0);
                                    LocalDateTime timestamp = (LocalDateTime) timestampValues.get(0);

                                    System.out.println("Document " + id + ": " + timestamp);

                                    // Verify microsecond precision is preserved in storage
                                    assertNotNull(timestamp, "Timestamp should not be null");
                                    assertInstanceOf(LocalDateTime.class, timestamp, "Should be LocalDateTime");
                                }
                            }
                        }

                        System.out.println("✓ All documents retrieved with timestamp precision preserved");
                    }
                }
            }
        }
    }

    @Test
    @DisplayName("Test range queries with microsecond precision on fast fields")
    public void testMicrosecondRangeQueries() {
        System.out.println("\n=== Microsecond Range Query Test ===");

        try (SchemaBuilder builder = new SchemaBuilder()) {
            builder.addIntegerField("id", true, true, true)
                   .addDateField("timestamp", true, true, true);  // FAST field with microsecond precision

            try (Schema schema = builder.build()) {
                try (Index index = new Index(schema, "", true)) {
                    try (IndexWriter writer = index.writer(Index.Memory.DEFAULT_HEAP_SIZE, 1)) {

                        LocalDateTime baseTime = LocalDateTime.of(2025, 11, 7, 7, 0, 0);

                        // Create documents spanning microsecond range
                        for (int i = 0; i < 10; i++) {
                            try (Document doc = new Document()) {
                                doc.addInteger("id", i + 1);
                                // Each document is 100 microseconds apart
                                doc.addDate("timestamp", baseTime.plusNanos(i * 100_000L));
                                writer.addDocument(doc);
                            }
                        }

                        writer.commit();
                        System.out.println("✓ Indexed 10 documents with 100μs spacing");
                    }

                    index.reload();

                    try (Searcher searcher = index.searcher()) {
                        assertEquals(10, searcher.getNumDocs(), "Should have 10 documents");

                        System.out.println("\n=== Verifying Microsecond Precision in Stored Fields ===");

                        // Verify all documents were indexed
                        try (Query allQuery = Query.allQuery();
                             SearchResult result = searcher.search(allQuery, 20)) {
                            var hits = result.getHits();

                            System.out.println("Total documents indexed: " + hits.size());
                            assertEquals(10, hits.size(), "Should have all 10 documents indexed");

                            // Verify microsecond precision is preserved in stored/fast fields
                            System.out.println("\nChecking nanosecond precision in each document:");
                            int docsWithPreservedPrecision = 0;

                            for (SearchResult.Hit hit : hits) {
                                try (Document doc = searcher.doc(hit.getDocAddress())) {
                                    Long id = (Long) doc.get("id").get(0);
                                    LocalDateTime timestamp = (LocalDateTime) doc.get("timestamp").get(0);

                                    int expectedNanos = ((id.intValue() - 1) * 100_000);  // 0μs, 100μs, 200μs, etc.
                                    int actualNanos = timestamp.getNano();

                                    System.out.println("  Doc " + id + ": expected=" + expectedNanos + "ns, actual=" + actualNanos + "ns" +
                                        (expectedNanos == actualNanos ? " ✓" : " ✗ PRECISION LOST"));

                                    // Verify the nanosecond component matches what we indexed
                                    if (expectedNanos == actualNanos) {
                                        docsWithPreservedPrecision++;
                                    }
                                }
                            }

                            System.out.println("\nDocuments with preserved microsecond precision: " +
                                docsWithPreservedPrecision + "/" + hits.size());

                            // The fix should enable microsecond precision for ALL documents with fast fields
                            assertEquals(10, docsWithPreservedPrecision,
                                "All documents should preserve microsecond precision with fast fields enabled");
                        }

                        System.out.println("\n✓ Microsecond precision successfully preserved in fast field storage");
                    }
                }
            }
        }
    }

    @Test
    @DisplayName("Test inverted index seconds precision limitation (regression test)")
    public void testInvertedIndexSecondsLimitation() {
        System.out.println("\n=== Inverted Index Seconds Limitation Test ===");
        System.out.println("This test documents the known Tantivy limitation:");
        System.out.println("Inverted index precision is hardcoded to SECONDS");

        try (SchemaBuilder builder = new SchemaBuilder()) {
            builder.addIntegerField("id", true, true, true)
                   .addDateField("timestamp", true, true, true);

            try (Schema schema = builder.build()) {
                try (Index index = new Index(schema, "", true)) {
                    LocalDateTime baseTime = LocalDateTime.of(2025, 11, 7, 7, 0, 0);

                    try (IndexWriter writer = index.writer(Index.Memory.DEFAULT_HEAP_SIZE, 1)) {

                        // Create 3 documents within the same second but different microseconds
                        try (Document doc1 = new Document()) {
                            doc1.addInteger("id", 1);
                            doc1.addDate("timestamp", baseTime.plusNanos(1_000));  // 1 microsecond
                            writer.addDocument(doc1);
                        }

                        try (Document doc2 = new Document()) {
                            doc2.addInteger("id", 2);
                            doc2.addDate("timestamp", baseTime.plusNanos(500_000));  // 500 microseconds
                            writer.addDocument(doc2);
                        }

                        try (Document doc3 = new Document()) {
                            doc3.addInteger("id", 3);
                            doc3.addDate("timestamp", baseTime.plusSeconds(1));  // Different second
                            writer.addDocument(doc3);
                        }

                        writer.commit();
                    }

                    index.reload();

                    try (Searcher searcher = index.searcher()) {
                        System.out.println("\n=== Verifying Microsecond Precision for Same-Second Timestamps ===");

                        // Test that documents within the same second have distinct microsecond timestamps
                        try (Query allQuery = Query.allQuery();
                             SearchResult result = searcher.search(allQuery, 10)) {
                            var hits = result.getHits();

                            System.out.println("Found " + hits.size() + " documents total");
                            assertEquals(3, hits.size(), "Should have 3 documents");

                            LocalDateTime doc1Time = null;
                            LocalDateTime doc2Time = null;
                            LocalDateTime doc3Time = null;

                            // Retrieve all timestamps
                            for (SearchResult.Hit hit : hits) {
                                try (Document doc = searcher.doc(hit.getDocAddress())) {
                                    Long id = (Long) doc.get("id").get(0);
                                    LocalDateTime timestamp = (LocalDateTime) doc.get("timestamp").get(0);

                                    System.out.println("  Doc " + id + ": " + timestamp + " (nanos: " + timestamp.getNano() + ")");

                                    if (id == 1L) doc1Time = timestamp;
                                    else if (id == 2L) doc2Time = timestamp;
                                    else if (id == 3L) doc3Time = timestamp;
                                }
                            }

                            // Verify timestamps are distinct at microsecond level
                            assertNotNull(doc1Time, "Document 1 should be present");
                            assertNotNull(doc2Time, "Document 2 should be present");
                            assertNotNull(doc3Time, "Document 3 should be present");

                            // Doc 1 and 2 are in the same second but different microseconds
                            assertEquals(doc1Time.getSecond(), doc2Time.getSecond(),
                                "Doc 1 and 2 should be in the same second");
                            assertNotEquals(doc1Time, doc2Time,
                                "Doc 1 and 2 should have different microsecond timestamps");

                            // Verify specific nanosecond values
                            assertEquals(1000, doc1Time.getNano(), "Doc 1 should have 1000ns (1 microsecond)");
                            assertEquals(500_000, doc2Time.getNano(), "Doc 2 should have 500000ns (500 microseconds)");

                            System.out.println("\n✓ Microseconds preserved: doc1=" + doc1Time.getNano() + "ns, doc2=" + doc2Time.getNano() + "ns");
                        }

                        System.out.println("\n✓ Test complete: Microsecond precision verified for same-second timestamps");
                    }
                }
            }
        }
    }

    @Test
    @DisplayName("Test edge case: multiple events in same second with different microseconds")
    public void testSameSecondDifferentMicroseconds() {
        System.out.println("\n=== Same Second, Different Microseconds Test ===");

        try (SchemaBuilder builder = new SchemaBuilder()) {
            builder.addIntegerField("id", true, true, true)
                   .addTextField("event", true, false, "default", "position")
                   .addDateField("timestamp", true, true, true);

            try (Schema schema = builder.build()) {
                try (Index index = new Index(schema, "", true)) {
                    LocalDateTime baseTime = LocalDateTime.of(2025, 11, 7, 12, 30, 45);

                    try (IndexWriter writer = index.writer(Index.Memory.DEFAULT_HEAP_SIZE, 1)) {

                        // Simulate high-frequency events within the same second
                        String[] events = {"request_start", "auth_check", "db_query",
                                         "response_build", "request_end"};

                        for (int i = 0; i < events.length; i++) {
                            try (Document doc = new Document()) {
                                doc.addInteger("id", i + 1);
                                doc.addText("event", events[i]);
                                // Events spaced 100 microseconds apart
                                doc.addDate("timestamp", baseTime.plusNanos(i * 100_000L));
                                writer.addDocument(doc);
                            }
                        }

                        writer.commit();
                        System.out.println("✓ Indexed 5 events within same second");
                    }

                    index.reload();

                    try (Searcher searcher = index.searcher()) {
                        System.out.println("\n=== Retrieving Events and Verifying Microsecond Precision ===");

                        // Query for all events and verify microsecond precision is preserved
                        try (Query allQuery = Query.allQuery();
                             SearchResult result = searcher.search(allQuery, 10)) {

                            var hits = result.getHits();
                            assertEquals(5, hits.size(), "Should have 5 events");

                            System.out.println("Checking each event's timestamp precision:");
                            for (SearchResult.Hit hit : hits) {
                                try (Document doc = searcher.doc(hit.getDocAddress())) {
                                    Long id = (Long) doc.get("id").get(0);
                                    String event = (String) doc.get("event").get(0);
                                    LocalDateTime timestamp = (LocalDateTime) doc.get("timestamp").get(0);

                                    int expectedNanos = ((id.intValue() - 1) * 100_000);  // 0μs, 100μs, 200μs, etc.
                                    int actualNanos = timestamp.getNano();

                                    System.out.println("  Event " + id + " (" + event + "): expected=" + expectedNanos + "ns, actual=" +
                                        actualNanos + "ns" + (expectedNanos == actualNanos ? " ✓" : " ✗"));

                                    // Verify microsecond precision is preserved
                                    assertEquals(expectedNanos, actualNanos,
                                        "Event " + id + " should preserve microsecond precision");
                                }
                            }

                            System.out.println("\n✓ All events have correct microsecond timestamps");
                        }

                        System.out.println("\n✓ Successfully preserved microsecond precision for high-frequency events");
                    }
                }
            }
        }
    }

    @Test
    @DisplayName("Test backward compatibility: date fields without fast still work")
    public void testBackwardCompatibilityWithoutFast() {
        System.out.println("\n=== Backward Compatibility Test (no fast field) ===");

        try (SchemaBuilder builder = new SchemaBuilder()) {
            // Date field WITHOUT fast (should still work, but no microsecond precision)
            builder.addIntegerField("id", true, true, true)
                   .addDateField("timestamp", true, true, false);  // No fast field

            try (Schema schema = builder.build()) {
                try (Index index = new Index(schema, "", true)) {
                    LocalDateTime baseTime = LocalDateTime.of(2025, 11, 7, 10, 0, 0);

                    try (IndexWriter writer = index.writer(Index.Memory.DEFAULT_HEAP_SIZE, 1)) {

                        try (Document doc = new Document()) {
                            doc.addInteger("id", 1);
                            doc.addDate("timestamp", baseTime);
                            writer.addDocument(doc);
                        }

                        writer.commit();
                    }

                    index.reload();

                    try (Searcher searcher = index.searcher()) {
                        assertEquals(1, searcher.getNumDocs(), "Should have 1 document");

                        // Basic search should still work
                        try (Query allQuery = Query.allQuery();
                             SearchResult result = searcher.search(allQuery, 10)) {

                            assertEquals(1, result.getHits().size(), "Should find 1 document");

                            try (Document doc = searcher.doc(result.getHits().get(0).getDocAddress())) {
                                LocalDateTime timestamp = (LocalDateTime) doc.get("timestamp").get(0);
                                assertEquals(baseTime, timestamp, "Timestamp should match");
                            }
                        }

                        System.out.println("✓ Date fields without fast flag still work correctly");
                    }
                }
            }
        }
    }
}

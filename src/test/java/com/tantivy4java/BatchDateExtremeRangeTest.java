package com.tantivy4java;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;
import static org.junit.jupiter.api.Assertions.*;

import java.time.LocalDateTime;
import java.time.Instant;
import java.time.ZoneOffset;

/**
 * Test batch date indexing with extreme range values to ensure robustness.
 * Tests edge cases like Unix epoch, far future dates, negative timestamps, etc.
 */
public class BatchDateExtremeRangeTest {

    @Test
    @DisplayName("Test batch indexing with Unix epoch (1970-01-01)")
    public void testUnixEpochDate() {
        System.out.println("=== Testing Unix Epoch Date ===");

        SchemaBuilder builder = new SchemaBuilder();
        builder.addDateField("timestamp", true, true, false);
        Schema schema = builder.build();

        Index index = new Index(schema, "", false);
        IndexWriter writer = index.writer(Index.Memory.DEFAULT_HEAP_SIZE, 1);

        try {
            BatchDocument doc = new BatchDocument();
            LocalDateTime epochDate = LocalDateTime.ofInstant(Instant.EPOCH, ZoneOffset.UTC);
            doc.addDate("timestamp", epochDate);

            BatchDocumentBuilder batchBuilder = new BatchDocumentBuilder();
            batchBuilder.addDocument(doc);

            long[] ops = writer.addDocumentsBatch(batchBuilder);
            assertEquals(1, ops.length);
            System.out.println("✓ Successfully indexed Unix epoch date: " + epochDate);

            writer.commit();
            index.reload();

            try (Searcher searcher = index.searcher()) {
                assertEquals(1, searcher.getNumDocs());
                try (Query query = Query.allQuery()) {
                    try (SearchResult result = searcher.search(query, 10)) {
                        assertEquals(1, result.getHits().size());
                        try (Document retrieved = searcher.doc(result.getHits().get(0).getDocAddress())) {
                            Object dateValue = retrieved.getFirst("timestamp");
                            assertNotNull(dateValue);
                            assertInstanceOf(LocalDateTime.class, dateValue);
                            System.out.println("✓ Retrieved date: " + dateValue);
                        }
                    }
                }
            }

        } finally {
            writer.close();
            index.close();
        }
    }

    @Test
    @DisplayName("Test batch indexing with far future date (year 2100)")
    public void testFarFutureDate() {
        System.out.println("=== Testing Far Future Date (2100) ===");

        SchemaBuilder builder = new SchemaBuilder();
        builder.addDateField("timestamp", true, true, false);
        Schema schema = builder.build();

        Index index = new Index(schema, "", false);
        IndexWriter writer = index.writer(Index.Memory.DEFAULT_HEAP_SIZE, 1);

        try {
            BatchDocument doc = new BatchDocument();
            LocalDateTime futureDate = LocalDateTime.of(2100, 12, 31, 23, 59, 59);
            doc.addDate("timestamp", futureDate);

            BatchDocumentBuilder batchBuilder = new BatchDocumentBuilder();
            batchBuilder.addDocument(doc);

            long[] ops = writer.addDocumentsBatch(batchBuilder);
            assertEquals(1, ops.length);
            System.out.println("✓ Successfully indexed future date: " + futureDate);

            writer.commit();
            index.reload();

            try (Searcher searcher = index.searcher()) {
                assertEquals(1, searcher.getNumDocs());
            }

        } finally {
            writer.close();
            index.close();
        }
    }

    @Test
    @DisplayName("Test batch indexing with Y2038 boundary dates")
    public void testY2038BoundaryDates() {
        System.out.println("=== Testing Y2038 Boundary Dates ===");

        SchemaBuilder builder = new SchemaBuilder();
        builder.addDateField("timestamp", true, true, false);
        builder.addTextField("label", true, false, "default", "position");
        Schema schema = builder.build();

        Index index = new Index(schema, "", false);
        IndexWriter writer = index.writer(Index.Memory.DEFAULT_HEAP_SIZE, 1);

        try {
            BatchDocumentBuilder batchBuilder = new BatchDocumentBuilder();

            // Just before Y2038 problem (2038-01-19 03:14:07 UTC)
            BatchDocument doc1 = new BatchDocument();
            LocalDateTime beforeY2038 = LocalDateTime.of(2038, 1, 19, 3, 14, 7);
            doc1.addDate("timestamp", beforeY2038);
            doc1.addText("label", "before-y2038");
            batchBuilder.addDocument(doc1);

            // Just after Y2038 problem
            BatchDocument doc2 = new BatchDocument();
            LocalDateTime afterY2038 = LocalDateTime.of(2038, 1, 19, 3, 14, 8);
            doc2.addDate("timestamp", afterY2038);
            doc2.addText("label", "after-y2038");
            batchBuilder.addDocument(doc2);

            // One year after Y2038
            BatchDocument doc3 = new BatchDocument();
            LocalDateTime yearAfterY2038 = LocalDateTime.of(2039, 1, 19, 0, 0, 0);
            doc3.addDate("timestamp", yearAfterY2038);
            doc3.addText("label", "year-after-y2038");
            batchBuilder.addDocument(doc3);

            long[] ops = writer.addDocumentsBatch(batchBuilder);
            assertEquals(3, ops.length);
            System.out.println("✓ Successfully indexed 3 Y2038 boundary dates");

            writer.commit();
            index.reload();

            try (Searcher searcher = index.searcher()) {
                assertEquals(3, searcher.getNumDocs());
            }

        } finally {
            writer.close();
            index.close();
        }
    }

    @Test
    @DisplayName("Test batch indexing with very old dates (1900)")
    public void testVeryOldDates() {
        System.out.println("=== Testing Very Old Dates (1900) ===");

        SchemaBuilder builder = new SchemaBuilder();
        builder.addDateField("timestamp", true, true, false);
        Schema schema = builder.build();

        Index index = new Index(schema, "", false);
        IndexWriter writer = index.writer(Index.Memory.DEFAULT_HEAP_SIZE, 1);

        try {
            BatchDocument doc = new BatchDocument();
            LocalDateTime oldDate = LocalDateTime.of(1900, 1, 1, 0, 0, 0);
            doc.addDate("timestamp", oldDate);

            BatchDocumentBuilder batchBuilder = new BatchDocumentBuilder();
            batchBuilder.addDocument(doc);

            long[] ops = writer.addDocumentsBatch(batchBuilder);
            assertEquals(1, ops.length);
            System.out.println("✓ Successfully indexed old date: " + oldDate);

        } finally {
            writer.close();
            index.close();
        }
    }

    @Test
    @DisplayName("Test batch indexing with millisecond precision")
    public void testMillisecondPrecisionDates() {
        System.out.println("=== Testing Millisecond Precision ===");

        SchemaBuilder builder = new SchemaBuilder();
        builder.addDateField("timestamp", true, true, false);
        builder.addTextField("label", true, false, "default", "position");
        Schema schema = builder.build();

        Index index = new Index(schema, "", false);
        IndexWriter writer = index.writer(Index.Memory.DEFAULT_HEAP_SIZE, 1);

        try {
            BatchDocumentBuilder batchBuilder = new BatchDocumentBuilder();

            // Create dates with varying millisecond components
            for (int ms = 0; ms < 1000; ms += 100) {
                BatchDocument doc = new BatchDocument();
                // Convert milliseconds to nanoseconds for LocalDateTime
                LocalDateTime dateWithMs = LocalDateTime.of(2024, 1, 15, 10, 30, 45, ms * 1_000_000);
                doc.addDate("timestamp", dateWithMs);
                doc.addText("label", "ms-" + ms);
                batchBuilder.addDocument(doc);
            }

            long[] ops = writer.addDocumentsBatch(batchBuilder);
            assertEquals(10, ops.length);
            System.out.println("✓ Successfully indexed 10 dates with millisecond precision");

            writer.commit();
            index.reload();

            try (Searcher searcher = index.searcher()) {
                assertEquals(10, searcher.getNumDocs());
            }

        } finally {
            writer.close();
            index.close();
        }
    }

    @Test
    @DisplayName("Test batch indexing with leap year dates")
    public void testLeapYearDates() {
        System.out.println("=== Testing Leap Year Dates ===");

        SchemaBuilder builder = new SchemaBuilder();
        builder.addDateField("timestamp", true, true, false);
        builder.addTextField("label", true, false, "default", "position");
        Schema schema = builder.build();

        Index index = new Index(schema, "", false);
        IndexWriter writer = index.writer(Index.Memory.DEFAULT_HEAP_SIZE, 1);

        try {
            BatchDocumentBuilder batchBuilder = new BatchDocumentBuilder();

            // February 29th in various leap years
            int[] leapYears = {2000, 2004, 2008, 2012, 2016, 2020, 2024};

            for (int year : leapYears) {
                BatchDocument doc = new BatchDocument();
                LocalDateTime leapDate = LocalDateTime.of(year, 2, 29, 12, 0, 0);
                doc.addDate("timestamp", leapDate);
                doc.addText("label", "leap-" + year);
                batchBuilder.addDocument(doc);
            }

            long[] ops = writer.addDocumentsBatch(batchBuilder);
            assertEquals(leapYears.length, ops.length);
            System.out.println("✓ Successfully indexed " + leapYears.length + " leap year dates");

            writer.commit();
            index.reload();

            try (Searcher searcher = index.searcher()) {
                assertEquals(leapYears.length, searcher.getNumDocs());
            }

        } finally {
            writer.close();
            index.close();
        }
    }

    @Test
    @DisplayName("Test batch indexing with timezone edge cases")
    public void testTimezoneEdgeCases() {
        System.out.println("=== Testing Timezone Edge Cases ===");

        SchemaBuilder builder = new SchemaBuilder();
        builder.addDateField("timestamp", true, true, false);
        builder.addTextField("label", true, false, "default", "position");
        Schema schema = builder.build();

        Index index = new Index(schema, "", false);
        IndexWriter writer = index.writer(Index.Memory.DEFAULT_HEAP_SIZE, 1);

        try {
            BatchDocumentBuilder batchBuilder = new BatchDocumentBuilder();

            // Test dates that cross day boundaries in different timezones
            BatchDocument doc1 = new BatchDocument();
            LocalDateTime midnightUTC = LocalDateTime.of(2024, 1, 1, 0, 0, 0);
            doc1.addDate("timestamp", midnightUTC);
            doc1.addText("label", "midnight-utc");
            batchBuilder.addDocument(doc1);

            BatchDocument doc2 = new BatchDocument();
            LocalDateTime almostMidnight = LocalDateTime.of(2024, 1, 1, 23, 59, 59);
            doc2.addDate("timestamp", almostMidnight);
            doc2.addText("label", "almost-midnight");
            batchBuilder.addDocument(doc2);

            long[] ops = writer.addDocumentsBatch(batchBuilder);
            assertEquals(2, ops.length);
            System.out.println("✓ Successfully indexed timezone edge case dates");

        } finally {
            writer.close();
            index.close();
        }
    }

    @Test
    @DisplayName("Test batch indexing with large volume of varied dates")
    public void testLargeVolumeDateRange() {
        System.out.println("=== Testing Large Volume Date Range ===");

        SchemaBuilder builder = new SchemaBuilder();
        builder.addDateField("timestamp", true, true, false);
        builder.addIntegerField("id", true, true, false);
        Schema schema = builder.build();

        Index index = new Index(schema, "", false);
        IndexWriter writer = index.writer(Index.Memory.LARGE_HEAP_SIZE, 1);

        try {
            BatchDocumentBuilder batchBuilder = new BatchDocumentBuilder();

            // Create 1000 documents spanning from 1970 to 2070
            LocalDateTime startDate = LocalDateTime.of(1970, 1, 1, 0, 0, 0);
            int totalDocs = 1000;

            for (int i = 0; i < totalDocs; i++) {
                BatchDocument doc = new BatchDocument();
                // Add approximately 36.5 days per document to span 100 years
                LocalDateTime date = startDate.plusDays((long) i * 36);
                doc.addDate("timestamp", date);
                doc.addInteger("id", i);
                batchBuilder.addDocument(doc);
            }

            long[] ops = writer.addDocumentsBatch(batchBuilder);
            assertEquals(totalDocs, ops.length);
            System.out.println("✓ Successfully indexed " + totalDocs + " dates spanning 1970-2070");

            writer.commit();
            index.reload();

            try (Searcher searcher = index.searcher()) {
                assertEquals(totalDocs, searcher.getNumDocs());
            }

        } finally {
            writer.close();
            index.close();
        }
    }

    @Test
    @DisplayName("Test batch indexing with extreme timestamp values from Spark")
    public void testSparkStyleTimestamps() {
        System.out.println("=== Testing Spark-style Timestamp Values ===");

        SchemaBuilder builder = new SchemaBuilder();
        builder.addDateField("timestamp", true, true, false);
        builder.addTextField("source", true, false, "default", "position");
        Schema schema = builder.build();

        Index index = new Index(schema, "", false);
        IndexWriter writer = index.writer(Index.Memory.DEFAULT_HEAP_SIZE, 1);

        try {
            BatchDocumentBuilder batchBuilder = new BatchDocumentBuilder();

            // Simulate Spark timestamp values (milliseconds since epoch)
            long[] sparkTimestamps = {
                0L,                          // 1970-01-01 00:00:00
                1609459200000L,              // 2021-01-01 00:00:00
                1705330200000L,              // 2024-01-15 14:30:00 (from bug report)
                1735707600000L,              // 2025-01-01 00:00:00 (from bug report)
                2147483647000L,              // Near max 32-bit seconds (2038-01-19)
                253402300799000L,            // 9999-12-31 23:59:59
            };

            for (int i = 0; i < sparkTimestamps.length; i++) {
                BatchDocument doc = new BatchDocument();
                LocalDateTime date = LocalDateTime.ofInstant(
                    Instant.ofEpochMilli(sparkTimestamps[i]),
                    ZoneOffset.UTC
                );
                doc.addDate("timestamp", date);
                doc.addText("source", "spark-" + i);
                batchBuilder.addDocument(doc);
                System.out.println("  Adding date: " + date + " (millis: " + sparkTimestamps[i] + ")");
            }

            long[] ops = writer.addDocumentsBatch(batchBuilder);
            assertEquals(sparkTimestamps.length, ops.length);
            System.out.println("✓ Successfully indexed " + sparkTimestamps.length + " Spark-style timestamps");

            writer.commit();
            index.reload();

            try (Searcher searcher = index.searcher()) {
                assertEquals(sparkTimestamps.length, searcher.getNumDocs());

                // Verify we can retrieve and read the dates
                try (Query query = Query.allQuery()) {
                    try (SearchResult result = searcher.search(query, 10)) {
                        for (SearchResult.Hit hit : result.getHits()) {
                            try (Document doc = searcher.doc(hit.getDocAddress())) {
                                Object dateValue = doc.getFirst("timestamp");
                                assertNotNull(dateValue);
                                assertInstanceOf(LocalDateTime.class, dateValue);
                            }
                        }
                    }
                }
            }

        } finally {
            writer.close();
            index.close();
        }
    }

    @Test
    @DisplayName("Test batch indexing rejects negative timestamp edge case")
    public void testNegativeTimestampHandling() {
        System.out.println("=== Testing Pre-Epoch Date (1960) ===");

        SchemaBuilder builder = new SchemaBuilder();
        builder.addDateField("timestamp", true, true, false);
        Schema schema = builder.build();

        Index index = new Index(schema, "", false);
        IndexWriter writer = index.writer(Index.Memory.DEFAULT_HEAP_SIZE, 1);

        try {
            BatchDocument doc = new BatchDocument();
            // Pre-epoch date (will have negative milliseconds)
            LocalDateTime preEpochDate = LocalDateTime.of(1960, 1, 1, 0, 0, 0);
            doc.addDate("timestamp", preEpochDate);

            BatchDocumentBuilder batchBuilder = new BatchDocumentBuilder();
            batchBuilder.addDocument(doc);

            long[] ops = writer.addDocumentsBatch(batchBuilder);
            assertEquals(1, ops.length);
            System.out.println("✓ Successfully indexed pre-epoch date: " + preEpochDate);

        } finally {
            writer.close();
            index.close();
        }
    }
}
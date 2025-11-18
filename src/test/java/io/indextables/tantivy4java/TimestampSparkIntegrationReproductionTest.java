package io.indextables.tantivy4java;

import io.indextables.tantivy4java.core.*;
import io.indextables.tantivy4java.query.*;
import io.indextables.tantivy4java.result.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Test to reproduce the exact issue reported by IndexTables4Spark team.
 *
 * Issue: Timestamps display microseconds correctly but getNano() returns 0,
 * causing all timestamps to have the same epoch seconds value.
 *
 * This test checks multiple schema configurations to identify the root cause.
 */
public class TimestampSparkIntegrationReproductionTest {

    @Test
    @DisplayName("Reproduce Spark issue - DATE field WITHOUT fast=true")
    public void testDateFieldWithoutFast(@TempDir Path tempDir) {
        String indexPath = tempDir.resolve("spark_reproduction_no_fast").toString();

        System.out.println("\n🔍 Testing DATE field WITHOUT fast=true (stored + indexed only)");

        try {
            try (SchemaBuilder builder = new SchemaBuilder()) {
                builder.addIntegerField("id", true, true, false);
                // Create DATE field WITHOUT fast=true (only stored + indexed)
                builder.addDateField("ts", true, true, false);  // stored=true, indexed=true, fast=FALSE

                try (Schema schema = builder.build()) {
                    final Instant baseTime = Instant.parse("2025-11-07T12:00:00.000000Z");

                    try (Index index = new Index(schema, indexPath, false)) {
                        try (IndexWriter writer = index.writer(Index.Memory.DEFAULT_HEAP_SIZE, 1)) {

                            // Write test data with microsecond differences
                            long[] nanoOffsets = {1000L, 500000L, 1000000L, 2000000L};  // 1µs, 500µs, 1ms, 2ms

                            for (int i = 0; i < nanoOffsets.length; i++) {
                                try (Document doc = new Document()) {
                                    doc.addInteger("id", i + 1);

                                    // Create LocalDateTime with microsecond precision (like Spark does)
                                    Instant instant = baseTime.plusNanos(nanoOffsets[i]);
                                    LocalDateTime localDateTime = LocalDateTime.ofInstant(instant, ZoneOffset.UTC);

                                    doc.addDate("ts", localDateTime);

                                    writer.addDocument(doc);
                                }
                            }

                            writer.commit();
                        }

                        index.reload();

                        // Read back and check precision
                        try (Searcher searcher = index.searcher()) {
                            try (Query allDocsQuery = Query.allQuery()) {
                                SearchResult results = searcher.search(allDocsQuery, 10);

                                System.out.println("\n📊 Results (DATE field WITHOUT fast):");
                                System.out.println("+---+--------------------------+------------------+----------+");
                                System.out.println("| ID| Timestamp                | Epoch Micros     | getNano()|");
                                System.out.println("+---+--------------------------+------------------+----------+");

                                for (SearchResult.Hit hit : results.getHits()) {
                                    try (Document doc = searcher.doc(hit.getDocAddress())) {
                                        Number idNum = (Number) doc.getFirst("id");
                                        LocalDateTime timestamp = (LocalDateTime) doc.getFirst("ts");

                                        int id = idNum.intValue();
                                        long epochMicros = timestamp.toEpochSecond(ZoneOffset.UTC) * 1_000_000L +
                                                          timestamp.getNano() / 1000L;

                                        System.out.println(String.format(
                                            "| %-2d| %-27s| %-17d| %-9d|",
                                            id, timestamp, epochMicros, timestamp.getNano()
                                        ));
                                    }
                                }

                                System.out.println("+---+--------------------------+------------------+----------+");
                                System.out.println("⚠️  If getNano() is 0 for all rows, microsecond precision is lost!");
                            }
                        }
                    }
                }
            }
        } catch (Exception e) {
            fail("Test failed: " + e.getMessage());
            e.printStackTrace();
        }
    }

    @Test
    @DisplayName("Verify DATE field WITH fast=true preserves microseconds")
    public void testDateFieldWithFast(@TempDir Path tempDir) {
        String indexPath = tempDir.resolve("spark_reproduction_with_fast").toString();

        System.out.println("\n✅ Testing DATE field WITH fast=true (stored + indexed + fast)");

        try {
            try (SchemaBuilder builder = new SchemaBuilder()) {
                builder.addIntegerField("id", true, true, false);
                // Create DATE field WITH fast=true
                builder.addDateField("ts", true, true, true);  // stored=true, indexed=true, fast=TRUE

                try (Schema schema = builder.build()) {
                    final Instant baseTime = Instant.parse("2025-11-07T12:00:00.000000Z");

                    try (Index index = new Index(schema, indexPath, false)) {
                        try (IndexWriter writer = index.writer(Index.Memory.DEFAULT_HEAP_SIZE, 1)) {

                            // Write test data with microsecond differences
                            long[] nanoOffsets = {1000L, 500000L, 1000000L, 2000000L};  // 1µs, 500µs, 1ms, 2ms

                            for (int i = 0; i < nanoOffsets.length; i++) {
                                try (Document doc = new Document()) {
                                    doc.addInteger("id", i + 1);

                                    // Create LocalDateTime with microsecond precision (like Spark does)
                                    Instant instant = baseTime.plusNanos(nanoOffsets[i]);
                                    LocalDateTime localDateTime = LocalDateTime.ofInstant(instant, ZoneOffset.UTC);

                                    doc.addDate("ts", localDateTime);

                                    writer.addDocument(doc);
                                }
                            }

                            writer.commit();
                        }

                        index.reload();

                        // Read back and check precision
                        try (Searcher searcher = index.searcher()) {
                            try (Query allDocsQuery = Query.allQuery()) {
                                SearchResult results = searcher.search(allDocsQuery, 10);

                                System.out.println("\n📊 Results (DATE field WITH fast):");
                                System.out.println("+---+--------------------------+------------------+----------+");
                                System.out.println("| ID| Timestamp                | Epoch Micros     | getNano()|");
                                System.out.println("+---+--------------------------+------------------+----------+");

                                boolean allDistinct = true;
                                long previousEpochMicros = -1;

                                for (SearchResult.Hit hit : results.getHits()) {
                                    try (Document doc = searcher.doc(hit.getDocAddress())) {
                                        Number idNum = (Number) doc.getFirst("id");
                                        LocalDateTime timestamp = (LocalDateTime) doc.getFirst("ts");

                                        int id = idNum.intValue();
                                        long epochMicros = timestamp.toEpochSecond(ZoneOffset.UTC) * 1_000_000L +
                                                          timestamp.getNano() / 1000L;

                                        if (previousEpochMicros >= 0 && epochMicros == previousEpochMicros) {
                                            allDistinct = false;
                                        }
                                        previousEpochMicros = epochMicros;

                                        System.out.println(String.format(
                                            "| %-2d| %-27s| %-17d| %-9d|",
                                            id, timestamp, epochMicros, timestamp.getNano()
                                        ));
                                    }
                                }

                                System.out.println("+---+--------------------------+------------------+----------+");

                                if (allDistinct) {
                                    System.out.println("✅ All epoch microseconds are DISTINCT - precision preserved!");
                                } else {
                                    System.out.println("❌ Some epoch microseconds are SAME - precision lost!");
                                    fail("Microsecond precision was not preserved with fast=true");
                                }
                            }
                        }
                    }
                }
            }
        } catch (Exception e) {
            fail("Test failed: " + e.getMessage());
            e.printStackTrace();
        }
    }

    @Test
    @DisplayName("Compare stored-only vs fast field precision")
    public void testStoredOnlyVsFastField(@TempDir Path tempDir) {
        System.out.println("\n📊 COMPARISON: Stored-only vs Fast Field Precision");
        System.out.println("=" .repeat(80));

        // Test both configurations and report findings
        testDateFieldWithoutFast(tempDir);
        testDateFieldWithFast(tempDir);

        System.out.println("\n💡 FINDINGS:");
        System.out.println("If DATE field WITHOUT fast=true shows 0 for getNano(),");
        System.out.println("then IndexTables4Spark must ensure fast=true for timestamp fields!");
    }
}

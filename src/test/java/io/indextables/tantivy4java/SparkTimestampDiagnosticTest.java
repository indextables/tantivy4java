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
 * Diagnostic test to exactly reproduce Spark's timestamp handling.
 * This mimics the exact flow from IndexTables4Spark to identify
 * why the consuming team is still seeing precision loss.
 */
public class SparkTimestampDiagnosticTest {

    @Test
    @DisplayName("Exact reproduction of Spark timestamp write/read cycle")
    public void testSparkExactReproduction(@TempDir Path tempDir) {
        String indexPath = tempDir.resolve("spark_diagnostic").toString();

        System.out.println("\n🔍 DIAGNOSTIC TEST - Exact Spark Reproduction");
        System.out.println("=" .repeat(80));

        try {
            // Test with BOTH fast=true and fast=false
            testWithFastConfig(indexPath + "_fast_true", true);
            testWithFastConfig(indexPath + "_fast_false", false);

        } catch (Exception e) {
            fail("Test failed: " + e.getMessage());
            e.printStackTrace();
        }
    }

    private void testWithFastConfig(String indexPath, boolean useFast) throws Exception {
        System.out.println("\n📊 Testing with fast=" + useFast);
        System.out.println("-".repeat(80));

        try (SchemaBuilder builder = new SchemaBuilder()) {
            builder.addIntegerField("id", true, true, false);
            builder.addDateField("ts", true, true, useFast);  // Test with different fast settings

            try (Schema schema = builder.build()) {
                // EXACTLY as Spark does it - start with Instant
                Instant baseTime = Instant.parse("2025-11-07T12:00:00.000000Z");

                try (Index index = new Index(schema, indexPath, false)) {
                    try (IndexWriter writer = index.writer(Index.Memory.DEFAULT_HEAP_SIZE, 1)) {

                        // Simulate Spark writing microseconds
                        long[] sparkMicros = {
                            1762516800000001L,  // +1 microsecond
                            1762516800000500L,  // +500 microseconds
                            1762516800001000L,  // +1000 microseconds
                            1762516800002000L   // +2000 microseconds
                        };

                        for (int i = 0; i < sparkMicros.length; i++) {
                            try (Document doc = new Document()) {
                                doc.addInteger("id", i + 1);

                                // EXACTLY how IndexTables4Spark writes timestamps
                                long micros = sparkMicros[i];
                                Instant instant = Instant.ofEpochSecond(
                                    micros / 1000000L,
                                    (micros % 1000000L) * 1000L
                                );
                                LocalDateTime localDateTime = LocalDateTime.ofInstant(instant, ZoneOffset.UTC);

                                System.out.println(String.format(
                                    "  Writing ID=%d: micros=%d, instant=%s, ldt=%s, ldt.getNano()=%d",
                                    i + 1, micros, instant, localDateTime, localDateTime.getNano()
                                ));

                                doc.addDate("ts", localDateTime);
                                writer.addDocument(doc);
                            }
                        }

                        writer.commit();
                    }

                    index.reload();

                    // Read back EXACTLY as IndexTables4Spark does
                    try (Searcher searcher = index.searcher()) {
                        try (Query allDocsQuery = Query.allQuery()) {
                            SearchResult results = searcher.search(allDocsQuery, 10);

                            System.out.println("\n  Read Results:");
                            System.out.println("  +---+--------------------------+------------+------------------+");
                            System.out.println("  | ID| LocalDateTime            | getNano()  | Epoch Micros     |");
                            System.out.println("  +---+--------------------------+------------+------------------+");

                            boolean allCorrect = true;
                            long[] expectedMicros = {
                                1762516800000001L,
                                1762516800000500L,
                                1762516800001000L,
                                1762516800002000L
                            };

                            for (SearchResult.Hit hit : results.getHits()) {
                                try (Document doc = searcher.doc(hit.getDocAddress())) {
                                    Number idNum = (Number) doc.getFirst("id");
                                    LocalDateTime ldt = (LocalDateTime) doc.getFirst("ts");

                                    assertNotNull(ldt, "LocalDateTime should not be null");

                                    int id = idNum.intValue();

                                    // EXACTLY as SchemaMapping.scala does it
                                    Instant instant = ldt.toInstant(ZoneOffset.UTC);
                                    long epochMicros = instant.getEpochSecond() * 1000000L +
                                                      instant.getNano() / 1000L;

                                    System.out.println(String.format(
                                        "  | %-2d| %-27s| %-11d| %-17d|",
                                        id, ldt, ldt.getNano(), epochMicros
                                    ));

                                    // Verify against expected
                                    long expected = expectedMicros[id - 1];
                                    if (epochMicros != expected) {
                                        System.out.println(String.format(
                                            "  ❌ MISMATCH: Expected %d, got %d (diff: %d micros)",
                                            expected, epochMicros, expected - epochMicros
                                        ));
                                        allCorrect = false;
                                    }
                                }
                            }

                            System.out.println("  +---+--------------------------+------------+------------------+");

                            if (allCorrect) {
                                System.out.println("  ✅ All timestamps CORRECT with fast=" + useFast);
                            } else {
                                System.out.println("  ❌ Precision LOST with fast=" + useFast);
                                fail("Microsecond precision lost with fast=" + useFast);
                            }
                        }
                    }
                }
            }
        }
    }

    @Test
    @DisplayName("Version information")
    public void testVersionInfo() {
        System.out.println("\n📋 Version Information:");
        System.out.println("  Library: tantivy4java");
        System.out.println("  Expected: 0.25.4 with microsecond precision fix");
        System.out.println("  Fix applied: Set precision unconditionally for all DATE fields");
        System.out.println("\n  To verify you have the fix, check native/src/schema.rs:");
        System.out.println("  - Line 365 should have: date_options.set_precision(DateTimePrecision::Microseconds)");
        System.out.println("  - This should be OUTSIDE any 'if fast' condition");
    }

    @Test
    @DisplayName("Test getNano() directly")
    public void testGetNanoDirectly(@TempDir Path tempDir) {
        String indexPath = tempDir.resolve("getnano_test").toString();

        System.out.println("\n🔬 Direct getNano() Test");
        System.out.println("=" .repeat(80));

        try {
            try (SchemaBuilder builder = new SchemaBuilder()) {
                builder.addDateField("ts", true, false, false);  // stored only, no fast

                try (Schema schema = builder.build()) {
                    try (Index index = new Index(schema, indexPath, false)) {
                        try (IndexWriter writer = index.writer(Index.Memory.DEFAULT_HEAP_SIZE, 1)) {

                            // Write ONE timestamp with known nanoseconds
                            LocalDateTime knownTime = LocalDateTime.of(2025, 11, 7, 12, 0, 0, 123456789);

                            System.out.println("Writing:");
                            System.out.println("  LocalDateTime: " + knownTime);
                            System.out.println("  getNano(): " + knownTime.getNano());
                            System.out.println("  Expected nanos: 123456789");

                            try (Document doc = new Document()) {
                                doc.addDate("ts", knownTime);
                                writer.addDocument(doc);
                            }

                            writer.commit();
                        }

                        index.reload();

                        try (Searcher searcher = index.searcher()) {
                            try (Query allDocsQuery = Query.allQuery()) {
                                SearchResult results = searcher.search(allDocsQuery, 10);

                                for (SearchResult.Hit hit : results.getHits()) {
                                    try (Document doc = searcher.doc(hit.getDocAddress())) {
                                        LocalDateTime retrieved = (LocalDateTime) doc.getFirst("ts");

                                        System.out.println("\nReading:");
                                        System.out.println("  LocalDateTime: " + retrieved);
                                        System.out.println("  getNano(): " + retrieved.getNano());

                                        if (retrieved.getNano() == 123456789) {
                                            System.out.println("  ✅ EXACT MATCH - Nanoseconds preserved!");
                                        } else if (retrieved.getNano() == 123000000) {
                                            System.out.println("  ⚠️  MILLISECOND precision (lost microseconds)");
                                        } else if (retrieved.getNano() == 0) {
                                            System.out.println("  ❌ SECONDS precision (lost all sub-second)");
                                        } else {
                                            System.out.println("  ⚠️  PARTIAL precision: " + retrieved.getNano());
                                        }

                                        // The critical assertion
                                        assertTrue(retrieved.getNano() >= 123000000,
                                            "Should preserve at least millisecond precision");
                                    }
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
}

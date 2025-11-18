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

import static org.junit.jupiter.api.Assertions.*;

/**
 * Definitive test to verify SUB-MILLISECOND precision.
 * Tests values < 1ms to verify true microsecond (not just millisecond) precision.
 */
public class SubMillisecondPrecisionTest {

    @Test
    @DisplayName("Test sub-millisecond values (1µs, 10µs, 100µs, 500µs)")
    public void testSubMillisecondValues(@TempDir Path tempDir) {
        String indexPath = tempDir.resolve("submillisecond_test").toString();

        System.out.println("\n🔬 SUB-MILLISECOND PRECISION TEST");
        System.out.println("=" .repeat(80));
        System.out.println("Testing values LESS than 1 millisecond to verify TRUE microsecond precision");
        System.out.println();

        try {
            try (SchemaBuilder builder = new SchemaBuilder()) {
                builder.addIntegerField("id", true, true, false);
                builder.addDateField("ts_fast", true, false, true);    // WITH fast
                builder.addDateField("ts_stored", true, false, false); // WITHOUT fast

                try (Schema schema = builder.build()) {
                    LocalDateTime baseTime = LocalDateTime.of(2025, 11, 7, 12, 0, 0, 0);

                    try (Index index = new Index(schema, indexPath, false)) {
                        try (IndexWriter writer = index.writer(Index.Memory.DEFAULT_HEAP_SIZE, 1)) {

                            // Test SUB-MILLISECOND values
                            long[] nanoValues = {
                                1000L,      // 1 microsecond
                                10000L,     // 10 microseconds
                                100000L,    // 100 microseconds
                                500000L     // 500 microseconds
                            };

                            for (int i = 0; i < nanoValues.length; i++) {
                                try (Document doc = new Document()) {
                                    doc.addInteger("id", i + 1);

                                    LocalDateTime timestamp = baseTime.plusNanos(nanoValues[i]);

                                    System.out.println(String.format(
                                        "Writing ID=%d: nanos=%d (%.3f µs), timestamp=%s",
                                        i + 1, nanoValues[i], nanoValues[i] / 1000.0, timestamp
                                    ));

                                    doc.addDate("ts_fast", timestamp);
                                    doc.addDate("ts_stored", timestamp);

                                    writer.addDocument(doc);
                                }
                            }

                            writer.commit();
                        }

                        index.reload();

                        System.out.println("\n" + "=".repeat(80));
                        System.out.println("READING BACK SUB-MILLISECOND VALUES");
                        System.out.println("=".repeat(80));

                        try (Searcher searcher = index.searcher()) {
                            try (Query allDocsQuery = Query.allQuery()) {
                                SearchResult results = searcher.search(allDocsQuery, 10);

                                testFieldPrecision(results, searcher, "ts_fast", true);
                                testFieldPrecision(results, searcher, "ts_stored", false);
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

    private void testFieldPrecision(SearchResult results, Searcher searcher,
                                    String fieldName, boolean isFast) throws Exception {

        System.out.println(String.format("\n📊 Field: %s (fast=%s)", fieldName, isFast));
        System.out.println("+---+----------+---------------------------+");
        System.out.println("| ID| Nanos    | Lost Precision?           |");
        System.out.println("+---+----------+---------------------------+");

        long[] expectedNanos = {1000L, 10000L, 100000L, 500000L};
        boolean allCorrect = true;
        int lostCount = 0;

        for (SearchResult.Hit hit : results.getHits()) {
            try (Document doc = searcher.doc(hit.getDocAddress())) {
                Number idNum = (Number) doc.getFirst("id");
                LocalDateTime ldt = (LocalDateTime) doc.getFirst(fieldName);

                if (ldt == null) {
                    System.out.println("| ? | NULL     | Field not retrieved!      |");
                    continue;
                }

                int id = idNum.intValue();
                int actualNanos = ldt.getNano();
                long expected = expectedNanos[id - 1];

                String status;
                if (actualNanos == expected) {
                    status = "✅ EXACT (microsecond OK)";
                } else if (actualNanos == (expected / 1000) * 1000) {
                    status = "⚠️  MILLISECOND only";
                    allCorrect = false;
                    lostCount++;
                } else if (actualNanos == 0) {
                    status = "❌ SECONDS only";
                    allCorrect = false;
                    lostCount++;
                } else {
                    status = String.format("? Got %d", actualNanos);
                    allCorrect = false;
                }

                System.out.println(String.format(
                    "| %-2d| %-9d| %-26s|",
                    id, actualNanos, status
                ));
            }
        }

        System.out.println("+---+----------+---------------------------+");

        if (allCorrect) {
            System.out.println("✅ SUCCESS: TRUE microsecond precision for " + fieldName);
        } else {
            System.out.println(String.format(
                "❌ FAILURE: Lost precision on %d/%d values for %s",
                lostCount, expectedNanos.length, fieldName
            ));

            if (isFast) {
                fail("Fast field should preserve microsecond precision but lost " + lostCount + " values");
            } else {
                fail("Stored field should preserve microsecond precision but lost " + lostCount + " values");
            }
        }
    }

    @Test
    @DisplayName("Test extreme sub-millisecond: 1µs vs 2µs vs 999µs")
    public void testExtremePrecision(@TempDir Path tempDir) {
        String indexPath = tempDir.resolve("extreme_precision_test").toString();

        System.out.println("\n🔬 EXTREME SUB-MILLISECOND TEST");
        System.out.println("Testing if we can distinguish 1µs from 2µs");
        System.out.println("=".repeat(80));

        try {
            try (SchemaBuilder builder = new SchemaBuilder()) {
                builder.addIntegerField("id", true, true, false);
                builder.addDateField("ts", true, false, true);  // fast field

                try (Schema schema = builder.build()) {
                    LocalDateTime baseTime = LocalDateTime.of(2025, 11, 7, 12, 0, 0, 0);

                    try (Index index = new Index(schema, indexPath, false)) {
                        try (IndexWriter writer = index.writer(Index.Memory.DEFAULT_HEAP_SIZE, 1)) {

                            // Extreme precision test
                            long[] extremeNanos = {1000L, 2000L, 999000L};  // 1µs, 2µs, 999µs

                            for (int i = 0; i < extremeNanos.length; i++) {
                                try (Document doc = new Document()) {
                                    doc.addInteger("id", i + 1);
                                    LocalDateTime timestamp = baseTime.plusNanos(extremeNanos[i]);
                                    doc.addDate("ts", timestamp);
                                    writer.addDocument(doc);
                                }
                            }

                            writer.commit();
                        }

                        index.reload();

                        try (Searcher searcher = index.searcher()) {
                            try (Query allDocsQuery = Query.allQuery()) {
                                SearchResult results = searcher.search(allDocsQuery, 10);

                                System.out.println("\nResults:");
                                long[] actualNanos = new long[3];

                                for (SearchResult.Hit hit : results.getHits()) {
                                    try (Document doc = searcher.doc(hit.getDocAddress())) {
                                        Number idNum = (Number) doc.getFirst("id");
                                        LocalDateTime ldt = (LocalDateTime) doc.getFirst("ts");

                                        int id = idNum.intValue();
                                        actualNanos[id - 1] = ldt.getNano();

                                        System.out.println(String.format(
                                            "ID=%d: getNano()=%d",
                                            id, ldt.getNano()
                                        ));
                                    }
                                }

                                System.out.println("\nVerification:");
                                boolean can_distinguish_1us_from_2us = (actualNanos[0] != actualNanos[1]);
                                boolean preserved_999us = (actualNanos[2] == 999000);

                                if (can_distinguish_1us_from_2us) {
                                    System.out.println("✅ Can distinguish 1µs from 2µs");
                                } else {
                                    System.out.println("❌ Cannot distinguish 1µs from 2µs (values: " +
                                        actualNanos[0] + " vs " + actualNanos[1] + ")");
                                }

                                if (preserved_999us) {
                                    System.out.println("✅ Preserved 999µs exactly");
                                } else {
                                    System.out.println("❌ Lost precision on 999µs (got: " + actualNanos[2] + ")");
                                }

                                assertTrue(can_distinguish_1us_from_2us,
                                    "Should distinguish 1µs from 2µs");
                                assertTrue(preserved_999us,
                                    "Should preserve 999µs");
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

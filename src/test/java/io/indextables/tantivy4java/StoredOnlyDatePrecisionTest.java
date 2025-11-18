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
 * Test to verify that DATE fields preserve microsecond precision
 * even when created WITHOUT fast=true (stored-only or indexed-only).
 *
 * This validates the fix that sets precision unconditionally,
 * not just for fast fields.
 */
public class StoredOnlyDatePrecisionTest {

    @Test
    @DisplayName("Stored-only DATE field preserves microsecond precision")
    public void testStoredOnlyPreservesMicroseconds(@TempDir Path tempDir) {
        String indexPath = tempDir.resolve("stored_only_precision").toString();

        System.out.println("\n🔍 Testing STORED-ONLY DATE field (fast=false)");

        try {
            try (SchemaBuilder builder = new SchemaBuilder()) {
                builder.addIntegerField("id", true, true, false);
                // Create DATE field with stored=true, indexed=false, fast=FALSE
                builder.addDateField("ts", true, false, false);

                try (Schema schema = builder.build()) {
                    final LocalDateTime baseTime = LocalDateTime.of(2025, 11, 7, 12, 0, 0, 0);

                    try (Index index = new Index(schema, indexPath, false)) {
                        try (IndexWriter writer = index.writer(Index.Memory.DEFAULT_HEAP_SIZE, 1)) {

                            // Write documents with microsecond differences
                            long[] nanoOffsets = {1000L, 500000L, 1000000L, 2000000L};

                            for (int i = 0; i < nanoOffsets.length; i++) {
                                try (Document doc = new Document()) {
                                    doc.addInteger("id", i + 1);
                                    LocalDateTime timestamp = baseTime.plusNanos(nanoOffsets[i]);
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

                                System.out.println("\n📊 Stored-Only Field Results:");
                                System.out.println("+---+--------------------------+----------+------------------+");
                                System.out.println("| ID| Timestamp                | getNano()| Epoch Micros     |");
                                System.out.println("+---+--------------------------+----------+------------------+");

                                boolean allDistinct = true;
                                long previousEpochMicros = -1;

                                for (SearchResult.Hit hit : results.getHits()) {
                                    try (Document doc = searcher.doc(hit.getDocAddress())) {
                                        Number idNum = (Number) doc.getFirst("id");
                                        LocalDateTime timestamp = (LocalDateTime) doc.getFirst("ts");

                                        assertNotNull(timestamp, "Timestamp should not be null");

                                        int id = idNum.intValue();
                                        int nanos = timestamp.getNano();
                                        long epochMicros = timestamp.toEpochSecond(ZoneOffset.UTC) * 1_000_000L +
                                                          nanos / 1000L;

                                        if (previousEpochMicros >= 0 && epochMicros == previousEpochMicros) {
                                            allDistinct = false;
                                        }
                                        previousEpochMicros = epochMicros;

                                        System.out.println(String.format(
                                            "| %-2d| %-27s| %-9d| %-17d|",
                                            id, timestamp, nanos, epochMicros
                                        ));
                                    }
                                }

                                System.out.println("+---+--------------------------+----------+------------------+");

                                if (allDistinct && previousEpochMicros > 0) {
                                    System.out.println("✅ SUCCESS: Stored-only field preserves microsecond precision!");
                                } else {
                                    System.out.println("❌ FAILURE: Microsecond precision lost in stored-only field!");
                                    fail("Stored-only DATE field did not preserve microsecond precision");
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
    @DisplayName("Indexed-only DATE field behavior")
    public void testIndexedOnlyDateField(@TempDir Path tempDir) {
        String indexPath = tempDir.resolve("indexed_only_precision").toString();

        System.out.println("\n🔍 Testing INDEXED-ONLY DATE field (stored=false, fast=false)");

        try {
            try (SchemaBuilder builder = new SchemaBuilder()) {
                builder.addIntegerField("id", true, true, false);
                // Create DATE field with stored=false, indexed=true, fast=false
                builder.addDateField("ts", false, true, false);

                try (Schema schema = builder.build()) {
                    final LocalDateTime baseTime = LocalDateTime.of(2025, 11, 7, 12, 0, 0, 0);

                    try (Index index = new Index(schema, indexPath, false)) {
                        try (IndexWriter writer = index.writer(Index.Memory.DEFAULT_HEAP_SIZE, 1)) {

                            long[] nanoOffsets = {1000L, 500000L, 1000000L, 2000000L};

                            for (int i = 0; i < nanoOffsets.length; i++) {
                                try (Document doc = new Document()) {
                                    doc.addInteger("id", i + 1);
                                    LocalDateTime timestamp = baseTime.plusNanos(nanoOffsets[i]);
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

                                System.out.println("\n📊 Indexed-Only Field Results:");
                                System.out.println("Note: Indexed-only fields (stored=false) cannot be retrieved from documents");
                                System.out.println("They are only available for querying via the inverted index.");
                                System.out.println("Inverted index precision is hardcoded to SECONDS in Tantivy core.");

                                for (SearchResult.Hit hit : results.getHits()) {
                                    try (Document doc = searcher.doc(hit.getDocAddress())) {
                                        Number idNum = (Number) doc.getFirst("id");
                                        Object timestamp = doc.getFirst("ts");

                                        System.out.println(String.format(
                                            "ID=%d, ts=%s (null expected since stored=false)",
                                            idNum.intValue(), timestamp
                                        ));

                                        assertNull(timestamp,
                                            "Indexed-only field should not be retrievable from documents");
                                    }
                                }

                                System.out.println("✅ Indexed-only behavior verified (fields not stored)");
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

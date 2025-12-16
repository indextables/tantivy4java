/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.indextables.tantivy4java;

import io.indextables.tantivy4java.core.*;
import io.indextables.tantivy4java.result.*;
import io.indextables.tantivy4java.split.*;
import io.indextables.tantivy4java.split.merge.*;
import io.indextables.tantivy4java.xref.*;

import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Comprehensive tests for XRef handling of all Tantivy field types.
 *
 * Validates that the XRef correctly captures and indexes terms from:
 * - Text fields (various tokenizers: default, raw)
 * - Integer fields (i64)
 * - Float fields (f64)
 * - Boolean fields
 * - Date/Timestamp fields
 * - JSON fields
 *
 * Each test creates splits with specific field types, builds an XRef,
 * and verifies that equality queries correctly route to the right splits.
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class XRefFieldTypeTest {

    @TempDir
    Path tempDir;

    // ========== TEXT FIELD TESTS ==========

    @Test
    @Order(1)
    @DisplayName("XRef captures text field terms with default tokenizer")
    void testTextFieldDefaultTokenizer() throws Exception {
        System.out.println("\n=== Testing XRef with TEXT fields (default tokenizer) ===");

        // Create split 1 with specific text content
        SplitInfo split1 = createTextSplit("split1", "default",
            "The quick brown fox jumps over the lazy dog",
            "Unique term: alpha bravo charlie");

        // Create split 2 with different text content
        SplitInfo split2 = createTextSplit("split2", "default",
            "A different sentence with different words",
            "Unique term: delta echo foxtrot");

        // Build XRef from both splits
        XRefMetadata xrefMetadata = buildXRef(Arrays.asList(split1, split2), "text-default-xref");

        assertNotNull(xrefMetadata);
        assertEquals(2, xrefMetadata.getNumSplits());
        assertTrue(xrefMetadata.getTotalTerms() > 0, "XRef should have captured terms");

        System.out.println("  Total terms captured: " + xrefMetadata.getTotalTerms());
        System.out.println("  XRef with default tokenizer text fields: PASS");
    }

    @Test
    @Order(2)
    @DisplayName("XRef captures text field terms with raw tokenizer")
    void testTextFieldRawTokenizer() throws Exception {
        System.out.println("\n=== Testing XRef with TEXT fields (raw tokenizer) ===");

        // Create split 1 with exact match content
        SplitInfo split1 = createTextSplit("split1-raw", "raw",
            "ExactMatch123",
            "AnotherExactValue");

        // Create split 2 with different exact match content
        SplitInfo split2 = createTextSplit("split2-raw", "raw",
            "DifferentValue456",
            "YetAnotherValue");

        // Build XRef
        XRefMetadata xrefMetadata = buildXRef(Arrays.asList(split1, split2), "text-raw-xref");

        assertNotNull(xrefMetadata);
        assertEquals(2, xrefMetadata.getNumSplits());
        assertTrue(xrefMetadata.getTotalTerms() > 0, "XRef should have captured terms");

        System.out.println("  Total terms captured: " + xrefMetadata.getTotalTerms());
        System.out.println("  XRef with raw tokenizer text fields: PASS");
    }

    // ========== INTEGER FIELD TESTS ==========

    @Test
    @Order(3)
    @DisplayName("XRef captures integer field terms for equality queries")
    void testIntegerField() throws Exception {
        System.out.println("\n=== Testing XRef with INTEGER fields ===");

        // Create split 1 with specific integer values
        SplitInfo split1 = createIntegerSplit("split1-int", new int[]{100, 200, 300, 400, 500});

        // Create split 2 with different integer values
        SplitInfo split2 = createIntegerSplit("split2-int", new int[]{1000, 2000, 3000, 4000, 5000});

        // Build XRef
        XRefMetadata xrefMetadata = buildXRef(Arrays.asList(split1, split2), "integer-xref");

        assertNotNull(xrefMetadata);
        assertEquals(2, xrefMetadata.getNumSplits());
        assertTrue(xrefMetadata.getTotalTerms() > 0, "XRef should have captured integer terms");

        System.out.println("  Total terms captured: " + xrefMetadata.getTotalTerms());
        System.out.println("  XRef with integer fields: PASS");
    }

    @Test
    @Order(4)
    @DisplayName("XRef captures negative integer values")
    void testNegativeIntegerField() throws Exception {
        System.out.println("\n=== Testing XRef with NEGATIVE INTEGER values ===");

        // Create split 1 with negative integers
        SplitInfo split1 = createIntegerSplit("split1-neg", new int[]{-100, -50, 0, 50, 100});

        // Create split 2 with different negative integers
        SplitInfo split2 = createIntegerSplit("split2-neg", new int[]{-1000, -500, -250, -125});

        // Build XRef
        XRefMetadata xrefMetadata = buildXRef(Arrays.asList(split1, split2), "negint-xref");

        assertNotNull(xrefMetadata);
        assertEquals(2, xrefMetadata.getNumSplits());
        assertTrue(xrefMetadata.getTotalTerms() > 0, "XRef should have captured negative integer terms");

        System.out.println("  Total terms captured: " + xrefMetadata.getTotalTerms());
        System.out.println("  XRef with negative integers: PASS");
    }

    // ========== FLOAT FIELD TESTS ==========

    @Test
    @Order(5)
    @DisplayName("XRef captures float field terms")
    void testFloatField() throws Exception {
        System.out.println("\n=== Testing XRef with FLOAT fields ===");

        // Create split 1 with specific float values
        SplitInfo split1 = createFloatSplit("split1-float", new double[]{1.5, 2.5, 3.14159, 99.99});

        // Create split 2 with different float values
        SplitInfo split2 = createFloatSplit("split2-float", new double[]{100.5, 200.25, 1000.001});

        // Build XRef
        XRefMetadata xrefMetadata = buildXRef(Arrays.asList(split1, split2), "float-xref");

        assertNotNull(xrefMetadata);
        assertEquals(2, xrefMetadata.getNumSplits());
        assertTrue(xrefMetadata.getTotalTerms() > 0, "XRef should have captured float terms");

        System.out.println("  Total terms captured: " + xrefMetadata.getTotalTerms());
        System.out.println("  XRef with float fields: PASS");
    }

    // ========== BOOLEAN FIELD TESTS ==========

    @Test
    @Order(6)
    @DisplayName("XRef captures boolean field terms")
    void testBooleanField() throws Exception {
        System.out.println("\n=== Testing XRef with BOOLEAN fields ===");

        // Create split 1 with only true values
        SplitInfo split1 = createBooleanSplit("split1-bool", new boolean[]{true, true, true});

        // Create split 2 with only false values
        SplitInfo split2 = createBooleanSplit("split2-bool", new boolean[]{false, false, false});

        // Create split 3 with mixed values
        SplitInfo split3 = createBooleanSplit("split3-bool", new boolean[]{true, false, true, false});

        // Build XRef
        XRefMetadata xrefMetadata = buildXRef(Arrays.asList(split1, split2, split3), "boolean-xref");

        assertNotNull(xrefMetadata);
        assertEquals(3, xrefMetadata.getNumSplits());
        assertTrue(xrefMetadata.getTotalTerms() > 0, "XRef should have captured boolean terms");

        System.out.println("  Total terms captured: " + xrefMetadata.getTotalTerms());
        System.out.println("  XRef with boolean fields: PASS");
    }

    // ========== DATE/TIMESTAMP FIELD TESTS ==========

    @Test
    @Order(7)
    @DisplayName("XRef captures date field terms")
    void testDateField() throws Exception {
        System.out.println("\n=== Testing XRef with DATE fields ===");

        // Create split 1 with dates from 2023
        SplitInfo split1 = createDateSplit("split1-date", new long[]{
            Instant.parse("2023-01-15T10:30:00Z").toEpochMilli() * 1000, // microseconds
            Instant.parse("2023-06-20T14:45:00Z").toEpochMilli() * 1000,
            Instant.parse("2023-12-31T23:59:59Z").toEpochMilli() * 1000
        });

        // Create split 2 with dates from 2024
        SplitInfo split2 = createDateSplit("split2-date", new long[]{
            Instant.parse("2024-03-01T08:00:00Z").toEpochMilli() * 1000,
            Instant.parse("2024-07-04T12:00:00Z").toEpochMilli() * 1000
        });

        // Build XRef
        XRefMetadata xrefMetadata = buildXRef(Arrays.asList(split1, split2), "date-xref");

        assertNotNull(xrefMetadata);
        assertEquals(2, xrefMetadata.getNumSplits());
        assertTrue(xrefMetadata.getTotalTerms() > 0, "XRef should have captured date terms");

        System.out.println("  Total terms captured: " + xrefMetadata.getTotalTerms());
        System.out.println("  XRef with date fields: PASS");
    }

    // ========== JSON FIELD TESTS ==========

    @Test
    @Order(8)
    @DisplayName("XRef captures JSON field terms")
    void testJsonField() throws Exception {
        System.out.println("\n=== Testing XRef with JSON fields ===");

        // Create split 1 with specific JSON content
        SplitInfo split1 = createJsonSplit("split1-json", Arrays.asList(
            Map.of("name", "Alice", "age", 30, "city", "New York"),
            Map.of("name", "Bob", "age", 25, "city", "Los Angeles")
        ));

        // Create split 2 with different JSON content
        SplitInfo split2 = createJsonSplit("split2-json", Arrays.asList(
            Map.of("name", "Charlie", "age", 35, "city", "Chicago"),
            Map.of("name", "Diana", "age", 28, "city", "Seattle")
        ));

        // Build XRef
        XRefMetadata xrefMetadata = buildXRef(Arrays.asList(split1, split2), "json-xref");

        assertNotNull(xrefMetadata);
        assertEquals(2, xrefMetadata.getNumSplits());
        assertTrue(xrefMetadata.getTotalTerms() > 0, "XRef should have captured JSON terms");

        System.out.println("  Total terms captured: " + xrefMetadata.getTotalTerms());
        System.out.println("  XRef with JSON fields: PASS");
    }

    // ========== MIXED FIELD TYPES TEST ==========

    @Test
    @Order(9)
    @DisplayName("XRef captures terms from mixed field types in single split")
    void testMixedFieldTypes() throws Exception {
        System.out.println("\n=== Testing XRef with MIXED field types ===");

        // Create splits with multiple field types
        SplitInfo split1 = createMixedFieldSplit("split1-mixed", 1);
        SplitInfo split2 = createMixedFieldSplit("split2-mixed", 2);

        // Build XRef
        XRefMetadata xrefMetadata = buildXRef(Arrays.asList(split1, split2), "mixed-xref");

        assertNotNull(xrefMetadata);
        assertEquals(2, xrefMetadata.getNumSplits());
        assertTrue(xrefMetadata.getTotalTerms() > 0, "XRef should have captured terms from all field types");

        System.out.println("  Total terms captured: " + xrefMetadata.getTotalTerms());
        System.out.println("  XRef with mixed field types: PASS");
    }

    // ========== EQUALITY SEARCH TESTS (Full round-trip validation) ==========

    @Test
    @Order(10)
    @DisplayName("XRef search finds correct splits for INTEGER equality query")
    void testIntegerEqualitySearch() throws Exception {
        System.out.println("\n=== Testing XRef INTEGER Equality Search ===");

        // Create split 1 with values: 100, 200, 300
        SplitInfo split1 = createIntegerSplit("int-search-1", new int[]{100, 200, 300});

        // Create split 2 with values: 1000, 2000, 3000
        SplitInfo split2 = createIntegerSplit("int-search-2", new int[]{1000, 2000, 3000});

        System.out.println("  Split1 values: [100, 200, 300]");
        System.out.println("  Split2 values: [1000, 2000, 3000]");

        // Build XRef
        XRefMetadata xrefMetadata = buildXRef(Arrays.asList(split1, split2), "int-search-xref");
        Path xrefPath = tempDir.resolve("int-search-xref.xref.split");

        System.out.println("  XRef built with " + xrefMetadata.getTotalTerms() + " terms");

        // Open XRef for searching
        SplitCacheManager.CacheConfig cacheConfig = new SplitCacheManager.CacheConfig("int-search-cache")
            .withMaxCacheSize(100_000_000);

        try (SplitCacheManager cacheManager = SplitCacheManager.getInstance(cacheConfig);
             XRefSearcher searcher = XRefSplit.open(cacheManager, "file://" + xrefPath.toString(), xrefMetadata)) {

            // Search for value=100 (should only match split1)
            System.out.println("\n  Searching for value:100...");
            XRefSearchResult result100 = searcher.search("value:100", 10);
            System.out.println("    Found " + result100.getNumMatchingSplits() + " matching split(s)");

            assertEquals(1, result100.getNumMatchingSplits(), "value:100 should find exactly 1 split");
            String matchingUri100 = result100.getMatchingSplits().get(0).getUri();
            assertTrue(matchingUri100.contains("int-search-1"), "value:100 should match split1, got: " + matchingUri100);

            // Search for value=1000 (should only match split2)
            System.out.println("\n  Searching for value:1000...");
            XRefSearchResult result1000 = searcher.search("value:1000", 10);
            System.out.println("    Found " + result1000.getNumMatchingSplits() + " matching split(s)");

            assertEquals(1, result1000.getNumMatchingSplits(), "value:1000 should find exactly 1 split");
            String matchingUri1000 = result1000.getMatchingSplits().get(0).getUri();
            assertTrue(matchingUri1000.contains("int-search-2"), "value:1000 should match split2, got: " + matchingUri1000);

            // Search for value=9999 (should match no splits)
            System.out.println("\n  Searching for value:9999 (non-existent)...");
            XRefSearchResult result9999 = searcher.search("value:9999", 10);
            System.out.println("    Found " + result9999.getNumMatchingSplits() + " matching split(s)");

            assertEquals(0, result9999.getNumMatchingSplits(), "value:9999 should find no splits");
        }

        System.out.println("\n  XRef INTEGER equality search: PASS");
    }

    @Test
    @Order(11)
    @DisplayName("XRef search finds correct splits for NEGATIVE INTEGER equality query")
    void testNegativeIntegerEqualitySearch() throws Exception {
        System.out.println("\n=== Testing XRef NEGATIVE INTEGER Equality Search ===");

        // Create split 1 with negative values: -100, -50, 0
        SplitInfo split1 = createIntegerSplit("negint-search-1", new int[]{-100, -50, 0});

        // Create split 2 with different negative values: -1000, -500
        SplitInfo split2 = createIntegerSplit("negint-search-2", new int[]{-1000, -500});

        System.out.println("  Split1 values: [-100, -50, 0]");
        System.out.println("  Split2 values: [-1000, -500]");

        XRefMetadata xrefMetadata = buildXRef(Arrays.asList(split1, split2), "negint-search-xref");
        Path xrefPath = tempDir.resolve("negint-search-xref.xref.split");

        System.out.println("  XRef built with " + xrefMetadata.getTotalTerms() + " terms");

        SplitCacheManager.CacheConfig cacheConfig = new SplitCacheManager.CacheConfig("negint-search-cache")
            .withMaxCacheSize(100_000_000);

        try (SplitCacheManager cacheManager = SplitCacheManager.getInstance(cacheConfig);
             XRefSearcher searcher = XRefSplit.open(cacheManager, "file://" + xrefPath.toString(), xrefMetadata)) {

            // Search for value=-100 (should only match split1)
            System.out.println("\n  Searching for value:-100...");
            XRefSearchResult resultNeg100 = searcher.search("value:-100", 10);
            System.out.println("    Found " + resultNeg100.getNumMatchingSplits() + " matching split(s)");

            assertEquals(1, resultNeg100.getNumMatchingSplits(), "value:-100 should find exactly 1 split");
            assertTrue(resultNeg100.getMatchingSplits().get(0).getUri().contains("negint-search-1"),
                "value:-100 should match split1");

            // Search for value=-1000 (should only match split2)
            System.out.println("\n  Searching for value:-1000...");
            XRefSearchResult resultNeg1000 = searcher.search("value:-1000", 10);
            System.out.println("    Found " + resultNeg1000.getNumMatchingSplits() + " matching split(s)");

            assertEquals(1, resultNeg1000.getNumMatchingSplits(), "value:-1000 should find exactly 1 split");
            assertTrue(resultNeg1000.getMatchingSplits().get(0).getUri().contains("negint-search-2"),
                "value:-1000 should match split2");

            // Search for value=0 (should only match split1)
            System.out.println("\n  Searching for value:0...");
            XRefSearchResult result0 = searcher.search("value:0", 10);
            System.out.println("    Found " + result0.getNumMatchingSplits() + " matching split(s)");

            assertEquals(1, result0.getNumMatchingSplits(), "value:0 should find exactly 1 split");
            assertTrue(result0.getMatchingSplits().get(0).getUri().contains("negint-search-1"),
                "value:0 should match split1");
        }

        System.out.println("\n  XRef NEGATIVE INTEGER equality search: PASS");
    }

    @Test
    @Order(12)
    @DisplayName("XRef search finds correct splits for FLOAT equality query")
    void testFloatEqualitySearch() throws Exception {
        System.out.println("\n=== Testing XRef FLOAT Equality Search ===");

        // Create split 1 with float values: 1.5, 2.5, 3.14159
        SplitInfo split1 = createFloatSplit("float-search-1", new double[]{1.5, 2.5, 3.14159});

        // Create split 2 with different float values: 100.5, 200.25
        SplitInfo split2 = createFloatSplit("float-search-2", new double[]{100.5, 200.25});

        System.out.println("  Split1 values: [1.5, 2.5, 3.14159]");
        System.out.println("  Split2 values: [100.5, 200.25]");

        XRefMetadata xrefMetadata = buildXRef(Arrays.asList(split1, split2), "float-search-xref");
        Path xrefPath = tempDir.resolve("float-search-xref.xref.split");

        System.out.println("  XRef built with " + xrefMetadata.getTotalTerms() + " terms");

        SplitCacheManager.CacheConfig cacheConfig = new SplitCacheManager.CacheConfig("float-search-cache")
            .withMaxCacheSize(100_000_000);

        try (SplitCacheManager cacheManager = SplitCacheManager.getInstance(cacheConfig);
             XRefSearcher searcher = XRefSplit.open(cacheManager, "file://" + xrefPath.toString(), xrefMetadata)) {

            // Search for value=1.5 (should only match split1)
            System.out.println("\n  Searching for value:1.5...");
            XRefSearchResult result15 = searcher.search("value:1.5", 10);
            System.out.println("    Found " + result15.getNumMatchingSplits() + " matching split(s)");

            assertEquals(1, result15.getNumMatchingSplits(), "value:1.5 should find exactly 1 split");
            assertTrue(result15.getMatchingSplits().get(0).getUri().contains("float-search-1"),
                "value:1.5 should match split1");

            // Search for value=100.5 (should only match split2)
            System.out.println("\n  Searching for value:100.5...");
            XRefSearchResult result1005 = searcher.search("value:100.5", 10);
            System.out.println("    Found " + result1005.getNumMatchingSplits() + " matching split(s)");

            assertEquals(1, result1005.getNumMatchingSplits(), "value:100.5 should find exactly 1 split");
            assertTrue(result1005.getMatchingSplits().get(0).getUri().contains("float-search-2"),
                "value:100.5 should match split2");

            // Search for non-existent float
            System.out.println("\n  Searching for value:999.999 (non-existent)...");
            XRefSearchResult resultNone = searcher.search("value:999.999", 10);
            System.out.println("    Found " + resultNone.getNumMatchingSplits() + " matching split(s)");

            assertEquals(0, resultNone.getNumMatchingSplits(), "value:999.999 should find no splits");
        }

        System.out.println("\n  XRef FLOAT equality search: PASS");
    }

    @Test
    @Order(13)
    @DisplayName("XRef search finds correct splits for BOOLEAN equality query")
    void testBooleanEqualitySearch() throws Exception {
        System.out.println("\n=== Testing XRef BOOLEAN Equality Search ===");

        // Create split 1 with only TRUE values
        SplitInfo split1 = createBooleanSplit("bool-search-true", new boolean[]{true, true, true});

        // Create split 2 with only FALSE values
        SplitInfo split2 = createBooleanSplit("bool-search-false", new boolean[]{false, false, false});

        System.out.println("  Split1 values: [true, true, true]");
        System.out.println("  Split2 values: [false, false, false]");

        XRefMetadata xrefMetadata = buildXRef(Arrays.asList(split1, split2), "bool-search-xref");
        Path xrefPath = tempDir.resolve("bool-search-xref.xref.split");

        System.out.println("  XRef built with " + xrefMetadata.getTotalTerms() + " terms");

        SplitCacheManager.CacheConfig cacheConfig = new SplitCacheManager.CacheConfig("bool-search-cache")
            .withMaxCacheSize(100_000_000);

        try (SplitCacheManager cacheManager = SplitCacheManager.getInstance(cacheConfig);
             XRefSearcher searcher = XRefSplit.open(cacheManager, "file://" + xrefPath.toString(), xrefMetadata)) {

            // Search for active:true (should only match split1)
            System.out.println("\n  Searching for active:true...");
            XRefSearchResult resultTrue = searcher.search("active:true", 10);
            System.out.println("    Found " + resultTrue.getNumMatchingSplits() + " matching split(s)");

            assertEquals(1, resultTrue.getNumMatchingSplits(), "active:true should find exactly 1 split");
            assertTrue(resultTrue.getMatchingSplits().get(0).getUri().contains("bool-search-true"),
                "active:true should match split1");

            // Search for active:false (should only match split2)
            System.out.println("\n  Searching for active:false...");
            XRefSearchResult resultFalse = searcher.search("active:false", 10);
            System.out.println("    Found " + resultFalse.getNumMatchingSplits() + " matching split(s)");

            assertEquals(1, resultFalse.getNumMatchingSplits(), "active:false should find exactly 1 split");
            assertTrue(resultFalse.getMatchingSplits().get(0).getUri().contains("bool-search-false"),
                "active:false should match split2");
        }

        System.out.println("\n  XRef BOOLEAN equality search: PASS");
    }

    @Test
    @Order(14)
    @DisplayName("XRef search finds correct splits for DATE/TIMESTAMP equality query")
    void testDateEqualitySearch() throws Exception {
        System.out.println("\n=== Testing XRef DATE/TIMESTAMP Equality Search ===");

        // Create split 1 with dates from 2023
        long ts2023Jan = Instant.parse("2023-01-15T10:30:00Z").toEpochMilli() * 1000; // microseconds
        long ts2023Jun = Instant.parse("2023-06-20T14:45:00Z").toEpochMilli() * 1000;
        SplitInfo split1 = createDateSplit("date-search-2023", new long[]{ts2023Jan, ts2023Jun});

        // Create split 2 with dates from 2024
        long ts2024Mar = Instant.parse("2024-03-01T08:00:00Z").toEpochMilli() * 1000;
        long ts2024Jul = Instant.parse("2024-07-04T12:00:00Z").toEpochMilli() * 1000;
        SplitInfo split2 = createDateSplit("date-search-2024", new long[]{ts2024Mar, ts2024Jul});

        System.out.println("  Split1 timestamps: [2023-01-15, 2023-06-20]");
        System.out.println("  Split2 timestamps: [2024-03-01, 2024-07-04]");

        XRefMetadata xrefMetadata = buildXRef(Arrays.asList(split1, split2), "date-search-xref");
        Path xrefPath = tempDir.resolve("date-search-xref.xref.split");

        System.out.println("  XRef built with " + xrefMetadata.getTotalTerms() + " terms");

        SplitCacheManager.CacheConfig cacheConfig = new SplitCacheManager.CacheConfig("date-search-cache")
            .withMaxCacheSize(100_000_000);

        try (SplitCacheManager cacheManager = SplitCacheManager.getInstance(cacheConfig);
             XRefSearcher searcher = XRefSplit.open(cacheManager, "file://" + xrefPath.toString(), xrefMetadata)) {

            // Search for the 2023 timestamp (should only match split1)
            // XRef stores timestamps as nanoseconds (Tantivy's internal format)
            // The test creates timestamps in microseconds, then divides by 1000 to get millis for LocalDateTime
            // Tantivy stores as nanoseconds = millis * 1_000_000 = micros * 1000
            String ts2023JanStr = String.valueOf(ts2023Jan * 1000); // Convert to nanoseconds
            System.out.println("\n  Searching for timestamp from 2023-01-15...");
            System.out.println("    Query: timestamp:" + ts2023JanStr);
            XRefSearchResult result2023 = searcher.search("timestamp:" + ts2023JanStr, 10);
            System.out.println("    Found " + result2023.getNumMatchingSplits() + " matching split(s)");

            assertEquals(1, result2023.getNumMatchingSplits(), "2023 timestamp should find exactly 1 split");
            assertTrue(result2023.getMatchingSplits().get(0).getUri().contains("date-search-2023"),
                "2023 timestamp should match split1");

            // Search for the 2024 timestamp (should only match split2)
            String ts2024MarStr = String.valueOf(ts2024Mar * 1000); // Convert to nanoseconds
            System.out.println("\n  Searching for timestamp from 2024-03-01...");
            System.out.println("    Query: timestamp:" + ts2024MarStr);
            XRefSearchResult result2024 = searcher.search("timestamp:" + ts2024MarStr, 10);
            System.out.println("    Found " + result2024.getNumMatchingSplits() + " matching split(s)");

            assertEquals(1, result2024.getNumMatchingSplits(), "2024 timestamp should find exactly 1 split");
            assertTrue(result2024.getMatchingSplits().get(0).getUri().contains("date-search-2024"),
                "2024 timestamp should match split2");
        }

        System.out.println("\n  XRef DATE/TIMESTAMP equality search: PASS");
    }

    @Test
    @Order(15)
    @DisplayName("XRef search finds correct splits for TEXT equality query")
    void testTextEqualitySearch() throws Exception {
        System.out.println("\n=== Testing XRef TEXT Equality Search ===");

        // Create split 1 with specific text content
        SplitInfo split1 = createTextSplit("text-search-1", "default",
            "The nebula is beautiful",
            "A distant quasar shines bright");

        // Create split 2 with different text content
        SplitInfo split2 = createTextSplit("text-search-2", "default",
            "The mitochondria powers the cell",
            "Ribosomes synthesize proteins");

        System.out.println("  Split1 content contains: nebula, quasar");
        System.out.println("  Split2 content contains: mitochondria, ribosomes");

        XRefMetadata xrefMetadata = buildXRef(Arrays.asList(split1, split2), "text-search-xref");
        Path xrefPath = tempDir.resolve("text-search-xref.xref.split");

        System.out.println("  XRef built with " + xrefMetadata.getTotalTerms() + " terms");

        SplitCacheManager.CacheConfig cacheConfig = new SplitCacheManager.CacheConfig("text-search-cache")
            .withMaxCacheSize(100_000_000);

        try (SplitCacheManager cacheManager = SplitCacheManager.getInstance(cacheConfig);
             XRefSearcher searcher = XRefSplit.open(cacheManager, "file://" + xrefPath.toString(), xrefMetadata)) {

            // Search for "nebula" (should only match split1)
            System.out.println("\n  Searching for content:nebula...");
            XRefSearchResult resultNebula = searcher.search("content:nebula", 10);
            System.out.println("    Found " + resultNebula.getNumMatchingSplits() + " matching split(s)");

            assertEquals(1, resultNebula.getNumMatchingSplits(), "content:nebula should find exactly 1 split");
            assertTrue(resultNebula.getMatchingSplits().get(0).getUri().contains("text-search-1"),
                "content:nebula should match split1");

            // Search for "mitochondria" (should only match split2)
            System.out.println("\n  Searching for content:mitochondria...");
            XRefSearchResult resultMito = searcher.search("content:mitochondria", 10);
            System.out.println("    Found " + resultMito.getNumMatchingSplits() + " matching split(s)");

            assertEquals(1, resultMito.getNumMatchingSplits(), "content:mitochondria should find exactly 1 split");
            assertTrue(resultMito.getMatchingSplits().get(0).getUri().contains("text-search-2"),
                "content:mitochondria should match split2");

            // Search for non-existent term
            System.out.println("\n  Searching for content:xyznonexistent (non-existent)...");
            XRefSearchResult resultNone = searcher.search("content:xyznonexistent", 10);
            System.out.println("    Found " + resultNone.getNumMatchingSplits() + " matching split(s)");

            assertEquals(0, resultNone.getNumMatchingSplits(), "Non-existent term should find no splits");
        }

        System.out.println("\n  XRef TEXT equality search: PASS");
    }

    @Test
    @Order(16)
    @DisplayName("XRef search finds correct splits for JSON field equality query")
    void testJsonEqualitySearch() throws Exception {
        System.out.println("\n=== Testing XRef JSON Field Equality Search ===");

        // Create split 1 with JSON containing "Alice"
        SplitInfo split1 = createJsonSplit("json-search-1", Arrays.asList(
            Map.of("name", "Alice", "city", "NewYork"),
            Map.of("name", "Bob", "city", "Boston")
        ));

        // Create split 2 with JSON containing "Charlie"
        SplitInfo split2 = createJsonSplit("json-search-2", Arrays.asList(
            Map.of("name", "Charlie", "city", "Chicago"),
            Map.of("name", "Diana", "city", "Denver")
        ));

        System.out.println("  Split1 JSON names: [Alice, Bob], cities: [NewYork, Boston]");
        System.out.println("  Split2 JSON names: [Charlie, Diana], cities: [Chicago, Denver]");

        XRefMetadata xrefMetadata = buildXRef(Arrays.asList(split1, split2), "json-search-xref");
        Path xrefPath = tempDir.resolve("json-search-xref.xref.split");

        System.out.println("  XRef built with " + xrefMetadata.getTotalTerms() + " terms");

        SplitCacheManager.CacheConfig cacheConfig = new SplitCacheManager.CacheConfig("json-search-cache")
            .withMaxCacheSize(100_000_000);

        try (SplitCacheManager cacheManager = SplitCacheManager.getInstance(cacheConfig);
             XRefSearcher searcher = XRefSplit.open(cacheManager, "file://" + xrefPath.toString(), xrefMetadata)) {

            // XRef stores JSON terms as "path:value" format in the base JSON field
            // So query format is: field:path:value -> data:name:alice
            // Note: values are lowercased by default tokenizer

            // Search for "alice" (should only match split1)
            System.out.println("\n  Searching for data:name:alice...");
            XRefSearchResult resultAlice = searcher.search("data:name:alice", 10);
            System.out.println("    Found " + resultAlice.getNumMatchingSplits() + " matching split(s)");

            assertEquals(1, resultAlice.getNumMatchingSplits(), "data:name:alice should find exactly 1 split");
            assertTrue(resultAlice.getMatchingSplits().get(0).getUri().contains("json-search-1"),
                "data:name:alice should match split1");

            // Search for "charlie" (should only match split2)
            System.out.println("\n  Searching for data:name:charlie...");
            XRefSearchResult resultCharlie = searcher.search("data:name:charlie", 10);
            System.out.println("    Found " + resultCharlie.getNumMatchingSplits() + " matching split(s)");

            assertEquals(1, resultCharlie.getNumMatchingSplits(), "data:name:charlie should find exactly 1 split");
            assertTrue(resultCharlie.getMatchingSplits().get(0).getUri().contains("json-search-2"),
                "data:name:charlie should match split2");

            // Search for city in split1
            System.out.println("\n  Searching for data:city:newyork...");
            XRefSearchResult resultNY = searcher.search("data:city:newyork", 10);
            System.out.println("    Found " + resultNY.getNumMatchingSplits() + " matching split(s)");

            assertEquals(1, resultNY.getNumMatchingSplits(), "data:city:newyork should find exactly 1 split");
            assertTrue(resultNY.getMatchingSplits().get(0).getUri().contains("json-search-1"),
                "data.city:newyork should match split1");
        }

        System.out.println("\n  XRef JSON equality search: PASS");
    }

    // ========== HELPER METHODS ==========

    private SplitInfo createTextSplit(String name, String tokenizer, String... contents) throws IOException {
        Path indexPath = tempDir.resolve(name + "-index");

        try (SchemaBuilder builder = new SchemaBuilder()) {
            builder.addTextField("content", true, false, tokenizer, "position");
            builder.addIntegerField("id", true, true, true);

            try (Schema schema = builder.build();
                 Index index = new Index(schema, indexPath.toString());
                 IndexWriter writer = index.writer(Index.Memory.DEFAULT_HEAP_SIZE, 1)) {

                int id = 0;
                for (String content : contents) {
                    try (Document doc = new Document()) {
                        doc.addText("content", content);
                        doc.addInteger("id", id++);
                        writer.addDocument(doc);
                    }
                }
                writer.commit();
            }
        }

        // Convert to split and get metadata with footer offsets
        Path splitPath = tempDir.resolve(name + ".split");
        QuickwitSplit.SplitConfig config = new QuickwitSplit.SplitConfig(
            "xref-test-" + name, "test-source", "test-node"
        );
        QuickwitSplit.SplitMetadata metadata = QuickwitSplit.convertIndexFromPath(
            indexPath.toString(), splitPath.toString(), config);

        return new SplitInfo(splitPath, metadata);
    }

    private SplitInfo createIntegerSplit(String name, int[] values) throws IOException {
        Path indexPath = tempDir.resolve(name + "-index");

        try (SchemaBuilder builder = new SchemaBuilder()) {
            builder.addIntegerField("value", true, true, true); // stored, indexed, fast
            builder.addIntegerField("id", true, true, true);

            try (Schema schema = builder.build();
                 Index index = new Index(schema, indexPath.toString());
                 IndexWriter writer = index.writer(Index.Memory.DEFAULT_HEAP_SIZE, 1)) {

                int id = 0;
                for (int value : values) {
                    try (Document doc = new Document()) {
                        doc.addInteger("value", value);
                        doc.addInteger("id", id++);
                        writer.addDocument(doc);
                    }
                }
                writer.commit();
            }
        }

        Path splitPath = tempDir.resolve(name + ".split");
        QuickwitSplit.SplitConfig config = new QuickwitSplit.SplitConfig(
            "xref-test-" + name, "test-source", "test-node"
        );
        QuickwitSplit.SplitMetadata metadata = QuickwitSplit.convertIndexFromPath(
            indexPath.toString(), splitPath.toString(), config);

        return new SplitInfo(splitPath, metadata);
    }

    private SplitInfo createFloatSplit(String name, double[] values) throws IOException {
        Path indexPath = tempDir.resolve(name + "-index");

        try (SchemaBuilder builder = new SchemaBuilder()) {
            builder.addFloatField("value", true, true, true); // stored, indexed, fast
            builder.addIntegerField("id", true, true, true);

            try (Schema schema = builder.build();
                 Index index = new Index(schema, indexPath.toString());
                 IndexWriter writer = index.writer(Index.Memory.DEFAULT_HEAP_SIZE, 1)) {

                int id = 0;
                for (double value : values) {
                    try (Document doc = new Document()) {
                        doc.addFloat("value", value);
                        doc.addInteger("id", id++);
                        writer.addDocument(doc);
                    }
                }
                writer.commit();
            }
        }

        Path splitPath = tempDir.resolve(name + ".split");
        QuickwitSplit.SplitConfig config = new QuickwitSplit.SplitConfig(
            "xref-test-" + name, "test-source", "test-node"
        );
        QuickwitSplit.SplitMetadata metadata = QuickwitSplit.convertIndexFromPath(
            indexPath.toString(), splitPath.toString(), config);

        return new SplitInfo(splitPath, metadata);
    }

    private SplitInfo createBooleanSplit(String name, boolean[] values) throws IOException {
        Path indexPath = tempDir.resolve(name + "-index");

        try (SchemaBuilder builder = new SchemaBuilder()) {
            builder.addBooleanField("active", true, true, false); // stored, indexed, not fast
            builder.addIntegerField("id", true, true, true);

            try (Schema schema = builder.build();
                 Index index = new Index(schema, indexPath.toString());
                 IndexWriter writer = index.writer(Index.Memory.DEFAULT_HEAP_SIZE, 1)) {

                int id = 0;
                for (boolean value : values) {
                    try (Document doc = new Document()) {
                        doc.addBoolean("active", value);
                        doc.addInteger("id", id++);
                        writer.addDocument(doc);
                    }
                }
                writer.commit();
            }
        }

        Path splitPath = tempDir.resolve(name + ".split");
        QuickwitSplit.SplitConfig config = new QuickwitSplit.SplitConfig(
            "xref-test-" + name, "test-source", "test-node"
        );
        QuickwitSplit.SplitMetadata metadata = QuickwitSplit.convertIndexFromPath(
            indexPath.toString(), splitPath.toString(), config);

        return new SplitInfo(splitPath, metadata);
    }

    private SplitInfo createDateSplit(String name, long[] timestampsMicros) throws IOException {
        Path indexPath = tempDir.resolve(name + "-index");

        try (SchemaBuilder builder = new SchemaBuilder()) {
            builder.addDateField("timestamp", true, true, true); // stored, indexed, fast
            builder.addIntegerField("id", true, true, true);

            try (Schema schema = builder.build();
                 Index index = new Index(schema, indexPath.toString());
                 IndexWriter writer = index.writer(Index.Memory.DEFAULT_HEAP_SIZE, 1)) {

                int id = 0;
                for (long micros : timestampsMicros) {
                    try (Document doc = new Document()) {
                        // Convert microseconds to LocalDateTime
                        long millis = micros / 1000;
                        LocalDateTime dateTime = LocalDateTime.ofInstant(
                            Instant.ofEpochMilli(millis), ZoneOffset.UTC);
                        doc.addDate("timestamp", dateTime);
                        doc.addInteger("id", id++);
                        writer.addDocument(doc);
                    }
                }
                writer.commit();
            }
        }

        Path splitPath = tempDir.resolve(name + ".split");
        QuickwitSplit.SplitConfig config = new QuickwitSplit.SplitConfig(
            "xref-test-" + name, "test-source", "test-node"
        );
        QuickwitSplit.SplitMetadata metadata = QuickwitSplit.convertIndexFromPath(
            indexPath.toString(), splitPath.toString(), config);

        return new SplitInfo(splitPath, metadata);
    }

    private SplitInfo createJsonSplit(String name, List<Map<String, Object>> jsonDocs) throws IOException {
        Path indexPath = tempDir.resolve(name + "-index");

        try (SchemaBuilder builder = new SchemaBuilder()) {
            Field dataField = builder.addJsonField("data", JsonObjectOptions.storedAndIndexed());
            builder.addIntegerField("id", true, true, true);

            try (Schema schema = builder.build();
                 Index index = new Index(schema, indexPath.toString());
                 IndexWriter writer = index.writer(Index.Memory.DEFAULT_HEAP_SIZE, 1)) {

                int id = 0;
                for (Map<String, Object> jsonData : jsonDocs) {
                    try (Document doc = new Document()) {
                        doc.addJson(dataField, jsonData);
                        doc.addInteger("id", id++);
                        writer.addDocument(doc);
                    }
                }
                writer.commit();
            }
        }

        Path splitPath = tempDir.resolve(name + ".split");
        QuickwitSplit.SplitConfig config = new QuickwitSplit.SplitConfig(
            "xref-test-" + name, "test-source", "test-node"
        );
        QuickwitSplit.SplitMetadata metadata = QuickwitSplit.convertIndexFromPath(
            indexPath.toString(), splitPath.toString(), config);

        return new SplitInfo(splitPath, metadata);
    }

    private SplitInfo createMixedFieldSplit(String name, int variant) throws IOException {
        Path indexPath = tempDir.resolve(name + "-index");

        try (SchemaBuilder builder = new SchemaBuilder()) {
            builder.addTextField("text_content", true, false, "default", "position");
            builder.addIntegerField("int_value", true, true, true);
            builder.addFloatField("float_value", true, true, true);
            builder.addBooleanField("bool_flag", true, true, false); // stored, indexed, not fast
            builder.addDateField("created_at", true, true, true);
            builder.addIntegerField("id", true, true, true);

            try (Schema schema = builder.build();
                 Index index = new Index(schema, indexPath.toString());
                 IndexWriter writer = index.writer(Index.Memory.DEFAULT_HEAP_SIZE, 1)) {

                for (int i = 0; i < 5; i++) {
                    try (Document doc = new Document()) {
                        doc.addText("text_content", "Mixed document " + (variant * 100 + i) + " with unique content");
                        doc.addInteger("int_value", variant * 1000 + i * 100);
                        doc.addFloat("float_value", variant * 10.5 + i * 0.1);
                        doc.addBoolean("bool_flag", i % 2 == 0);
                        // Convert timestamp micros to LocalDateTime
                        long micros = (1704067200000L + variant * 86400000L + i * 3600000L) * 1000;
                        long millis = micros / 1000;
                        LocalDateTime dateTime = LocalDateTime.ofInstant(
                            Instant.ofEpochMilli(millis), ZoneOffset.UTC);
                        doc.addDate("created_at", dateTime);
                        doc.addInteger("id", variant * 100 + i);
                        writer.addDocument(doc);
                    }
                }
                writer.commit();
            }
        }

        Path splitPath = tempDir.resolve(name + ".split");
        QuickwitSplit.SplitConfig config = new QuickwitSplit.SplitConfig(
            "xref-test-" + name, "test-source", "test-node"
        );
        QuickwitSplit.SplitMetadata metadata = QuickwitSplit.convertIndexFromPath(
            indexPath.toString(), splitPath.toString(), config);

        return new SplitInfo(splitPath, metadata);
    }

    private XRefMetadata buildXRef(List<SplitInfo> splitInfos, String xrefId) throws IOException {
        List<XRefSourceSplit> sourceSplits = new ArrayList<>();

        for (SplitInfo info : splitInfos) {
            XRefSourceSplit source = XRefSourceSplit.fromSplitMetadata(
                "file://" + info.path.toAbsolutePath().toString(),
                info.metadata
            );
            sourceSplits.add(source);
        }

        XRefBuildConfig config = XRefBuildConfig.builder()
            .xrefId(xrefId)
            .indexUid("test-index")
            .sourceSplits(sourceSplits)
            .includePositions(false)
            .build();

        Path xrefPath = tempDir.resolve(xrefId + ".xref.split");
        return XRefSplit.build(config, xrefPath.toString());
    }

    /**
     * Helper class to hold split path and metadata together.
     */
    private static class SplitInfo {
        final Path path;
        final QuickwitSplit.SplitMetadata metadata;

        SplitInfo(Path path, QuickwitSplit.SplitMetadata metadata) {
            this.path = path;
            this.metadata = metadata;
        }
    }
}

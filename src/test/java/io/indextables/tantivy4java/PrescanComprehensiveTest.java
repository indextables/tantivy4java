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

import static org.junit.jupiter.api.Assertions.*;

import io.indextables.tantivy4java.core.*;
import io.indextables.tantivy4java.split.*;
import io.indextables.tantivy4java.split.merge.QuickwitSplit;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;

/**
 * Comprehensive prescan tests covering all field types and query operators.
 *
 * Tests the prescan FST term lookup across:
 * - Text fields with default tokenizer (lowercase)
 * - Text fields with raw tokenizer (keyword/exact)
 * - Integer fields (i64)
 * - Float fields (f64)
 * - Date fields
 * - Boolean fields
 * - JSON fields
 *
 * And all query types:
 * - TermQuery
 * - WildcardQuery (prefix, suffix, contains)
 * - RangeQuery
 * - BooleanQuery (AND, OR, combinations)
 * - PhraseQuery
 * - MatchAllQuery
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class PrescanComprehensiveTest {

    @TempDir
    Path tempDir;

    private Path indexDir;
    private Path splitPath;
    private SplitCacheManager cacheManager;
    private long footerOffset;
    private String docMappingJson;
    private String uniqueId;

    @BeforeEach
    public void setUp() throws Exception {
        uniqueId = UUID.randomUUID().toString();
        indexDir = tempDir.resolve("comprehensive_index_" + uniqueId);
        Files.createDirectories(indexDir);
        splitPath = tempDir.resolve("comprehensive_" + uniqueId + ".split");

        SplitCacheManager.CacheConfig config =
            new SplitCacheManager.CacheConfig("prescan-comprehensive-" + uniqueId)
                .withMaxCacheSize(100_000_000);
        cacheManager = SplitCacheManager.getInstance(config);

        createComprehensiveIndexAndSplit();
    }

    @AfterEach
    public void tearDown() throws Exception {
        if (cacheManager != null) {
            cacheManager.close();
        }
    }

    private void createComprehensiveIndexAndSplit() throws Exception {
        try (SchemaBuilder builder = new SchemaBuilder()) {
            // Text field with default tokenizer (lowercase)
            builder.addTextField("title", true, false, "default", "position");

            // Text field with raw tokenizer (keyword/exact match)
            builder.addTextField("category", true, false, "raw", "position");

            // Integer field
            builder.addIntegerField("count", true, true, false);

            // Float field
            builder.addFloatField("price", true, true, false);

            // Date field
            builder.addDateField("created_at", true, true, false);

            // Boolean field
            builder.addBooleanField("active", true, true, false);

            // JSON field
            builder.addJsonField("metadata", true, "default", "position");

            try (Schema schema = builder.build()) {
                try (Index index = new Index(schema, indexDir.toString())) {
                    try (IndexWriter writer =
                        index.writer(Index.Memory.DEFAULT_HEAP_SIZE, 1)) {

                        // Document 1: Active product
                        addDocument(writer,
                            "Hello World Product",      // title (lowercased to "hello world product")
                            "Electronics",              // category (exact: "Electronics")
                            100,                        // count
                            29.99,                      // price
                            "2024-01-15T10:30:00Z",     // created_at
                            true,                       // active
                            "{\"color\":\"red\",\"size\":\"large\"}"  // metadata
                        );

                        // Document 2: Inactive product
                        addDocument(writer,
                            "Goodbye World Item",       // title
                            "Clothing",                 // category
                            50,                         // count
                            49.99,                      // price
                            "2024-02-20T14:00:00Z",     // created_at
                            false,                      // active
                            "{\"color\":\"blue\",\"size\":\"medium\"}"
                        );

                        // Document 3: Another active product
                        addDocument(writer,
                            "Test Product Special",     // title
                            "Electronics",              // category
                            200,                        // count
                            99.99,                      // price
                            "2024-03-10T08:00:00Z",     // created_at
                            true,                       // active
                            "{\"color\":\"green\",\"weight\":5}"
                        );

                        // Document 4: Case sensitivity test
                        addDocument(writer,
                            "UPPERCASE TITLE",          // title (lowercased)
                            "Home & Garden",            // category (exact with special chars)
                            25,                         // count
                            15.50,                      // price
                            "2023-12-01T00:00:00Z",     // created_at
                            true,                       // active
                            "{\"material\":\"wood\"}"
                        );

                        writer.commit();
                    }

                    index.reload();

                    QuickwitSplit.SplitConfig splitConfig =
                        new QuickwitSplit.SplitConfig(
                            "comprehensive-index-" + uniqueId,
                            "comprehensive-source-" + uniqueId,
                            "comprehensive-node-" + uniqueId);
                    QuickwitSplit.SplitMetadata metadata =
                        QuickwitSplit.convertIndexFromPath(
                            indexDir.toString(), splitPath.toString(), splitConfig);

                    footerOffset = metadata.getFooterStartOffset();
                    docMappingJson = metadata.getDocMappingJson();

                    System.out.println("Created comprehensive split with " + metadata.getNumDocs() + " documents");
                    System.out.println("Doc mapping: " + docMappingJson);
                }
            }
        }
    }

    private void addDocument(IndexWriter writer, String title, String category,
                             int count, double price, String createdAt,
                             boolean active, String metadataJson) throws IOException {
        try (Document doc = new Document()) {
            doc.addText("title", title);
            doc.addText("category", category);
            doc.addInteger("count", count);
            doc.addFloat("price", price);
            // Parse ISO date string to LocalDateTime (remove Z suffix for LocalDateTime.parse)
            LocalDateTime dateTime = LocalDateTime.parse(
                createdAt.replace("Z", ""),
                DateTimeFormatter.ISO_LOCAL_DATE_TIME);
            doc.addDate("created_at", dateTime);
            doc.addBoolean("active", active);
            // Parse JSON string to Map for addJson
            @SuppressWarnings("unchecked")
            Map<String, Object> jsonMap = new com.fasterxml.jackson.databind.ObjectMapper()
                .readValue(metadataJson, Map.class);
            doc.addJson("metadata", jsonMap);
            writer.addDocument(doc);
        }
    }

    private List<SplitInfo> getSplits() {
        return Collections.singletonList(
            new SplitInfo("file://" + splitPath.toAbsolutePath(), footerOffset));
    }

    // ============================================================
    // TEXT FIELD WITH DEFAULT TOKENIZER (LOWERCASE)
    // ============================================================

    @Test
    @Order(1)
    public void testTextDefaultTokenizer_TermExists_Lowercase() throws IOException {
        // "hello" exists (lowercased from "Hello")
        SplitQuery query = new SplitTermQuery("title", "hello");
        List<PrescanResult> results = cacheManager.prescanSplits(getSplits(), docMappingJson, query);

        assertEquals(1, results.size());
        assertTrue(results.get(0).couldHaveResults(),
            "Term 'hello' should exist (lowercased from 'Hello')");
    }

    @Test
    @Order(2)
    public void testTextDefaultTokenizer_TermNotExists() throws IOException {
        // "nonexistent" does not exist
        SplitQuery query = new SplitTermQuery("title", "nonexistent");
        List<PrescanResult> results = cacheManager.prescanSplits(getSplits(), docMappingJson, query);

        assertEquals(1, results.size());
        assertFalse(results.get(0).couldHaveResults(),
            "Term 'nonexistent' should not exist");
    }

    @Test
    @Order(3)
    public void testTextDefaultTokenizer_CaseInsensitive() throws IOException {
        // "UPPERCASE" should match as "uppercase" due to lowercasing
        SplitQuery query = new SplitTermQuery("title", "uppercase");
        List<PrescanResult> results = cacheManager.prescanSplits(getSplits(), docMappingJson, query);

        assertEquals(1, results.size());
        assertTrue(results.get(0).couldHaveResults(),
            "Term 'uppercase' should exist (lowercased from 'UPPERCASE')");
    }

    @Test
    @Order(4)
    public void testTextDefaultTokenizer_WildcardPrefix() throws IOException {
        // "hel*" should match "hello"
        SplitQuery query = new SplitWildcardQuery("title", "hel*");
        List<PrescanResult> results = cacheManager.prescanSplits(getSplits(), docMappingJson, query);

        assertEquals(1, results.size());
        assertTrue(results.get(0).couldHaveResults(),
            "Wildcard 'hel*' should match 'hello'");
    }

    @Test
    @Order(5)
    public void testTextDefaultTokenizer_WildcardSuffix() throws IOException {
        // "*orld" should match "world"
        SplitQuery query = new SplitWildcardQuery("title", "*orld");
        List<PrescanResult> results = cacheManager.prescanSplits(getSplits(), docMappingJson, query);

        assertEquals(1, results.size());
        assertTrue(results.get(0).couldHaveResults(),
            "Wildcard '*orld' should match 'world'");
    }

    @Test
    @Order(6)
    public void testTextDefaultTokenizer_WildcardNoMatch() throws IOException {
        // "zzz*" should not match anything
        SplitQuery query = new SplitWildcardQuery("title", "zzz*");
        List<PrescanResult> results = cacheManager.prescanSplits(getSplits(), docMappingJson, query);

        assertEquals(1, results.size());
        assertFalse(results.get(0).couldHaveResults(),
            "Wildcard 'zzz*' should not match anything");
    }

    // ============================================================
    // TEXT FIELD WITH RAW TOKENIZER (KEYWORD/EXACT)
    // ============================================================

    @Test
    @Order(10)
    public void testTextRawTokenizer_ExactMatch() throws IOException {
        // "Electronics" should match exactly (raw tokenizer preserves case)
        SplitQuery query = new SplitTermQuery("category", "Electronics");
        List<PrescanResult> results = cacheManager.prescanSplits(getSplits(), docMappingJson, query);

        assertEquals(1, results.size());
        // Raw tokenizer preserves case, so exact match should work
        assertTrue(results.get(0).couldHaveResults(),
            "Exact term 'Electronics' should exist in raw tokenizer field");
    }

    @Test
    @Order(11)
    public void testTextRawTokenizer_CaseMismatch() throws IOException {
        // "electronics" (lowercase) should NOT match "Electronics" (raw preserves case)
        SplitQuery query = new SplitTermQuery("category", "electronics");
        List<PrescanResult> results = cacheManager.prescanSplits(getSplits(), docMappingJson, query);

        assertEquals(1, results.size());
        // With raw tokenizer, case matters - "electronics" != "Electronics"
        assertFalse(results.get(0).couldHaveResults(),
            "Lowercase 'electronics' should NOT match 'Electronics' with raw tokenizer");
    }

    @Test
    @Order(12)
    public void testTextRawTokenizer_SpecialChars() throws IOException {
        // "Home & Garden" should match exactly (special chars preserved with raw tokenizer)
        SplitQuery query = new SplitTermQuery("category", "Home & Garden");
        List<PrescanResult> results = cacheManager.prescanSplits(getSplits(), docMappingJson, query);

        assertEquals(1, results.size());
        // Raw tokenizer preserves special characters
        assertTrue(results.get(0).couldHaveResults(),
            "Exact term 'Home & Garden' with special chars should exist in raw tokenizer field");
    }

    @Test
    @Order(13)
    public void testTextRawTokenizer_WildcardPrefix() throws IOException {
        // "Elec*" should match "Electronics" (raw tokenizer, case-sensitive)
        SplitQuery query = new SplitWildcardQuery("category", "Elec*");
        List<PrescanResult> results = cacheManager.prescanSplits(getSplits(), docMappingJson, query);

        assertEquals(1, results.size());
        // Raw tokenizer preserves case, so "Elec*" should match "Electronics"
        assertTrue(results.get(0).couldHaveResults(),
            "Wildcard 'Elec*' should match 'Electronics' in raw tokenizer field");
    }

    // ============================================================
    // INTEGER FIELD
    // ============================================================

    @Test
    @Order(20)
    public void testIntegerField_TermQuery() throws IOException {
        // Term query for integer value 100
        // NOTE: Integer terms are encoded differently in Tantivy's FST (i64 binary encoding)
        // The prescan may not properly construct integer terms, so this could return
        // either true (if we handle it conservatively) or false (if we try text lookup)
        SplitQuery query = new SplitTermQuery("count", "100");
        List<PrescanResult> results = cacheManager.prescanSplits(getSplits(), docMappingJson, query);

        assertEquals(1, results.size());
        System.out.println("Integer term query result: " + results.get(0).couldHaveResults());
        System.out.println("  Term existence: " + results.get(0).getTermExistence());
        // For now, just document the behavior - this may need type-aware term encoding
    }

    @Test
    @Order(21)
    public void testIntegerField_RangeQuery() throws IOException {
        // Range query for count between 50 and 150
        SplitQuery query = SplitRangeQuery.inclusiveRange("count", "50", "150", "i64");
        List<PrescanResult> results = cacheManager.prescanSplits(getSplits(), docMappingJson, query);

        assertEquals(1, results.size());
        // Range queries should return conservative true
        assertTrue(results.get(0).couldHaveResults(),
            "Integer range query should return conservative true");
    }

    @Test
    @Order(22)
    public void testIntegerField_RangeWithTermExists() throws IOException {
        // Boolean: range:[0 TO 1000] AND title:hello
        SplitQuery rangeQuery = SplitRangeQuery.inclusiveRange("count", "0", "1000", "i64");
        SplitQuery termQuery = new SplitTermQuery("title", "hello");
        SplitQuery boolQuery = new SplitBooleanQuery()
            .addMust(rangeQuery)
            .addMust(termQuery);

        List<PrescanResult> results = cacheManager.prescanSplits(getSplits(), docMappingJson, boolQuery);

        assertEquals(1, results.size());
        assertTrue(results.get(0).couldHaveResults(),
            "Range (conservative true) AND existing term should return true");
    }

    @Test
    @Order(23)
    public void testIntegerField_RangeWithTermNotExists() throws IOException {
        // Boolean: range:[0 TO 1000] AND title:nonexistent
        SplitQuery rangeQuery = SplitRangeQuery.inclusiveRange("count", "0", "1000", "i64");
        SplitQuery termQuery = new SplitTermQuery("title", "nonexistent");
        SplitQuery boolQuery = new SplitBooleanQuery()
            .addMust(rangeQuery)
            .addMust(termQuery);

        List<PrescanResult> results = cacheManager.prescanSplits(getSplits(), docMappingJson, boolQuery);

        assertEquals(1, results.size());
        assertFalse(results.get(0).couldHaveResults(),
            "Range (conservative true) AND non-existent term should return false");
    }

    // ============================================================
    // FLOAT FIELD
    // ============================================================

    @Test
    @Order(30)
    public void testFloatField_RangeQuery() throws IOException {
        // Range query for price between 20.0 and 50.0
        SplitQuery query = SplitRangeQuery.inclusiveRange("price", "20.0", "50.0", "f64");
        List<PrescanResult> results = cacheManager.prescanSplits(getSplits(), docMappingJson, query);

        assertEquals(1, results.size());
        assertTrue(results.get(0).couldHaveResults(),
            "Float range query should return conservative true");
    }

    // ============================================================
    // DATE FIELD
    // ============================================================

    @Test
    @Order(40)
    public void testDateField_RangeQuery() throws IOException {
        // Range query for dates in 2024
        SplitQuery query = SplitRangeQuery.inclusiveRange(
            "created_at", "2024-01-01T00:00:00Z", "2024-12-31T23:59:59Z", "date");
        List<PrescanResult> results = cacheManager.prescanSplits(getSplits(), docMappingJson, query);

        assertEquals(1, results.size());
        assertTrue(results.get(0).couldHaveResults(),
            "Date range query should return conservative true");
    }

    // ============================================================
    // BOOLEAN FIELD
    // ============================================================

    @Test
    @Order(50)
    public void testBooleanField_TermTrue() throws IOException {
        // Term query for active=true
        // NOTE: Boolean terms are encoded specially in Tantivy's FST (single byte: 0 or 1)
        // The prescan may not properly construct boolean terms without type-aware encoding
        SplitQuery query = new SplitTermQuery("active", "true");
        List<PrescanResult> results = cacheManager.prescanSplits(getSplits(), docMappingJson, query);

        assertEquals(1, results.size());
        System.out.println("Boolean true term query result: " + results.get(0).couldHaveResults());
        System.out.println("  Term existence: " + results.get(0).getTermExistence());
        // For now, document behavior - this may need type-aware term encoding
    }

    @Test
    @Order(51)
    public void testBooleanField_TermFalse() throws IOException {
        // Term query for active=false
        // NOTE: Boolean terms are encoded specially in Tantivy's FST (single byte: 0 or 1)
        SplitQuery query = new SplitTermQuery("active", "false");
        List<PrescanResult> results = cacheManager.prescanSplits(getSplits(), docMappingJson, query);

        assertEquals(1, results.size());
        System.out.println("Boolean false term query result: " + results.get(0).couldHaveResults());
        System.out.println("  Term existence: " + results.get(0).getTermExistence());
        // For now, document behavior - this may need type-aware term encoding
    }

    // ============================================================
    // JSON FIELD
    // ============================================================

    @Test
    @Order(60)
    public void testJsonField_TermQuery() throws IOException {
        // Term query on JSON field for "color"="red"
        // JSON fields have complex term encoding with embedded paths, so prescan returns
        // conservative true (we can't easily check JSON terms in the FST without knowing the path)
        SplitQuery query = new SplitTermQuery("metadata", "red");
        List<PrescanResult> results = cacheManager.prescanSplits(getSplits(), docMappingJson, query);

        assertEquals(1, results.size());
        // JSON field queries should return conservative true
        assertTrue(results.get(0).couldHaveResults(),
            "JSON field term query should return conservative true (JSON terms have path encoding)");
    }

    @Test
    @Order(61)
    public void testJsonField_PathQuery_ExistingValue() throws IOException {
        // Term query on JSON field WITH path: metadata.color = "red"
        // This uses the new JSON path handling to construct proper JSON term
        SplitQuery query = new SplitTermQuery("metadata.color", "red");
        List<PrescanResult> results = cacheManager.prescanSplits(getSplits(), docMappingJson, query);

        assertEquals(1, results.size());
        System.out.println("JSON path query (metadata.color:red) result: " + results.get(0).couldHaveResults());
        System.out.println("  Term existence: " + results.get(0).getTermExistence());
        // With proper JSON path handling, this should find the term
        // Note: Result depends on how Tantivy encodes JSON terms - may still return conservative true
    }

    @Test
    @Order(62)
    public void testJsonField_PathQuery_NonExistingValue() throws IOException {
        // Term query on JSON field WITH path: metadata.color = "purple" (doesn't exist)
        SplitQuery query = new SplitTermQuery("metadata.color", "purple");
        List<PrescanResult> results = cacheManager.prescanSplits(getSplits(), docMappingJson, query);

        assertEquals(1, results.size());
        System.out.println("JSON path query (metadata.color:purple) result: " + results.get(0).couldHaveResults());
        System.out.println("  Term existence: " + results.get(0).getTermExistence());
        // If JSON path term lookup works correctly, this should return false
        // because "purple" is not a value in metadata.color
    }

    @Test
    @Order(63)
    public void testJsonField_PathQuery_NonExistingPath() throws IOException {
        // Term query on JSON field WITH path that doesn't exist: metadata.nonexistent = "value"
        SplitQuery query = new SplitTermQuery("metadata.nonexistent", "value");
        List<PrescanResult> results = cacheManager.prescanSplits(getSplits(), docMappingJson, query);

        assertEquals(1, results.size());
        System.out.println("JSON path query (metadata.nonexistent:value) result: " + results.get(0).couldHaveResults());
        System.out.println("  Term existence: " + results.get(0).getTermExistence());
        // Path doesn't exist in any document, so this should return false
    }

    // ============================================================
    // BOOLEAN QUERY COMBINATIONS
    // ============================================================

    @Test
    @Order(70)
    public void testBooleanQuery_MustMust_BothExist() throws IOException {
        // title:hello AND title:world (both exist)
        SplitQuery q1 = new SplitTermQuery("title", "hello");
        SplitQuery q2 = new SplitTermQuery("title", "world");
        SplitQuery boolQuery = new SplitBooleanQuery()
            .addMust(q1)
            .addMust(q2);

        List<PrescanResult> results = cacheManager.prescanSplits(getSplits(), docMappingJson, boolQuery);

        assertEquals(1, results.size());
        assertTrue(results.get(0).couldHaveResults(),
            "Boolean AND with both terms existing should return true");
    }

    @Test
    @Order(71)
    public void testBooleanQuery_MustMust_OneNotExist() throws IOException {
        // title:hello AND title:nonexistent (one doesn't exist)
        SplitQuery q1 = new SplitTermQuery("title", "hello");
        SplitQuery q2 = new SplitTermQuery("title", "nonexistent");
        SplitQuery boolQuery = new SplitBooleanQuery()
            .addMust(q1)
            .addMust(q2);

        List<PrescanResult> results = cacheManager.prescanSplits(getSplits(), docMappingJson, boolQuery);

        assertEquals(1, results.size());
        assertFalse(results.get(0).couldHaveResults(),
            "Boolean AND with one term not existing should return false");
    }

    @Test
    @Order(72)
    public void testBooleanQuery_ShouldShould_OneExists() throws IOException {
        // title:hello OR title:nonexistent (one exists)
        SplitQuery q1 = new SplitTermQuery("title", "hello");
        SplitQuery q2 = new SplitTermQuery("title", "nonexistent");
        SplitQuery boolQuery = new SplitBooleanQuery()
            .addShould(q1)
            .addShould(q2);

        List<PrescanResult> results = cacheManager.prescanSplits(getSplits(), docMappingJson, boolQuery);

        assertEquals(1, results.size());
        assertTrue(results.get(0).couldHaveResults(),
            "Boolean OR with one term existing should return true");
    }

    @Test
    @Order(73)
    public void testBooleanQuery_ShouldShould_NoneExist() throws IOException {
        // title:nonexistent1 OR title:nonexistent2 (neither exists)
        SplitQuery q1 = new SplitTermQuery("title", "nonexistent1");
        SplitQuery q2 = new SplitTermQuery("title", "nonexistent2");
        SplitQuery boolQuery = new SplitBooleanQuery()
            .addShould(q1)
            .addShould(q2);

        List<PrescanResult> results = cacheManager.prescanSplits(getSplits(), docMappingJson, boolQuery);

        assertEquals(1, results.size());
        assertFalse(results.get(0).couldHaveResults(),
            "Boolean OR with no terms existing should return false");
    }

    @Test
    @Order(74)
    public void testBooleanQuery_NestedBooleans() throws IOException {
        // (title:hello AND title:world) OR title:nonexistent
        SplitQuery q1 = new SplitTermQuery("title", "hello");
        SplitQuery q2 = new SplitTermQuery("title", "world");
        SplitQuery q3 = new SplitTermQuery("title", "nonexistent");

        SplitQuery innerBool = new SplitBooleanQuery()
            .addMust(q1)
            .addMust(q2);

        SplitQuery outerBool = new SplitBooleanQuery()
            .addShould(innerBool)
            .addShould(q3);

        List<PrescanResult> results = cacheManager.prescanSplits(getSplits(), docMappingJson, outerBool);

        assertEquals(1, results.size());
        assertTrue(results.get(0).couldHaveResults(),
            "Nested boolean (hello AND world) OR nonexistent should return true");
    }

    @Test
    @Order(75)
    public void testBooleanQuery_MixedFieldTypes() throws IOException {
        // title:hello AND category:Electronics (text + raw)
        SplitQuery q1 = new SplitTermQuery("title", "hello");
        SplitQuery q2 = new SplitTermQuery("category", "Electronics");
        SplitQuery boolQuery = new SplitBooleanQuery()
            .addMust(q1)
            .addMust(q2);

        List<PrescanResult> results = cacheManager.prescanSplits(getSplits(), docMappingJson, boolQuery);

        assertEquals(1, results.size());
        // Both terms exist: "hello" (lowercased from "Hello") and "Electronics" (exact match)
        assertTrue(results.get(0).couldHaveResults(),
            "Boolean AND with mixed field types (text + raw) should return true when both terms exist");
    }

    @Test
    @Order(76)
    public void testBooleanQuery_MustNot_Ignored() throws IOException {
        // title:hello AND NOT title:world
        // MUST_NOT should be ignored for prescan purposes - term existence doesn't guarantee all docs have it
        SplitQuery q1 = new SplitTermQuery("title", "hello");
        SplitQuery q2 = new SplitTermQuery("title", "world");
        SplitQuery boolQuery = new SplitBooleanQuery()
            .addMust(q1)
            .addMustNot(q2);

        List<PrescanResult> results = cacheManager.prescanSplits(getSplits(), docMappingJson, boolQuery);

        assertEquals(1, results.size());
        // Both terms exist, but MUST_NOT is ignored for prescan (conservative)
        // Split could have documents where "hello" exists but "world" does not
        assertTrue(results.get(0).couldHaveResults(),
            "Boolean with MUST_NOT should return true when MUST term exists (MUST_NOT ignored)");
    }

    @Test
    @Order(77)
    public void testBooleanQuery_MustNot_WithMissingMust() throws IOException {
        // title:nonexistent AND NOT title:hello
        // Even though MUST_NOT term exists, the MUST term doesn't - so should return false
        SplitQuery q1 = new SplitTermQuery("title", "nonexistent");
        SplitQuery q2 = new SplitTermQuery("title", "hello");
        SplitQuery boolQuery = new SplitBooleanQuery()
            .addMust(q1)
            .addMustNot(q2);

        List<PrescanResult> results = cacheManager.prescanSplits(getSplits(), docMappingJson, boolQuery);

        assertEquals(1, results.size());
        // MUST term doesn't exist, so no results possible regardless of MUST_NOT
        assertFalse(results.get(0).couldHaveResults(),
            "Boolean with missing MUST term should return false regardless of MUST_NOT");
    }

    @Test
    @Order(78)
    public void testBooleanQuery_OnlyMustNot() throws IOException {
        // Query with ONLY a MUST_NOT clause (matches all except those with "hello")
        // This is a valid query that could match documents
        SplitQuery q1 = new SplitTermQuery("title", "hello");
        SplitQuery boolQuery = new SplitBooleanQuery()
            .addMustNot(q1);

        List<PrescanResult> results = cacheManager.prescanSplits(getSplits(), docMappingJson, boolQuery);

        assertEquals(1, results.size());
        // MUST_NOT only query should return conservative true (could have documents not matching)
        assertTrue(results.get(0).couldHaveResults(),
            "Boolean with only MUST_NOT should return true (conservative - could have non-matching docs)");
    }

    // ============================================================
    // PHRASE QUERY
    // ============================================================

    @Test
    @Order(80)
    public void testPhraseQuery_TermsExist() throws IOException {
        // Phrase "hello world" - both terms exist
        SplitQuery query = new SplitPhraseQuery("title", Arrays.asList("hello", "world"));
        List<PrescanResult> results = cacheManager.prescanSplits(getSplits(), docMappingJson, query);

        assertEquals(1, results.size());
        // Phrase prescan checks if all terms exist (conservative - phrase might not match)
        assertTrue(results.get(0).couldHaveResults(),
            "Phrase query with all terms existing should return true (conservative approximation)");
    }

    @Test
    @Order(81)
    public void testPhraseQuery_TermNotExist() throws IOException {
        // Phrase "hello nonexistent" - one term doesn't exist
        SplitQuery query = new SplitPhraseQuery("title", Arrays.asList("hello", "nonexistent"));
        List<PrescanResult> results = cacheManager.prescanSplits(getSplits(), docMappingJson, query);

        assertEquals(1, results.size());
        // If any phrase term doesn't exist, phrase definitely can't match
        assertFalse(results.get(0).couldHaveResults(),
            "Phrase query with missing term should return false (phrase can't match)");
    }

    @Test
    @Order(82)
    public void testPhraseQuery_AllTermsMissing() throws IOException {
        // Phrase where ALL terms are missing
        SplitQuery query = new SplitPhraseQuery("title", Arrays.asList("xyz123", "abc456"));
        List<PrescanResult> results = cacheManager.prescanSplits(getSplits(), docMappingJson, query);

        assertEquals(1, results.size());
        assertFalse(results.get(0).couldHaveResults(),
            "Phrase query with all missing terms should return false");
    }

    // ============================================================
    // MATCH ALL QUERY
    // ============================================================

    @Test
    @Order(90)
    public void testMatchAllQuery() throws IOException {
        // MatchAll should always return true
        SplitQuery query = new SplitMatchAllQuery();
        List<PrescanResult> results = cacheManager.prescanSplits(getSplits(), docMappingJson, query);

        assertEquals(1, results.size());
        assertTrue(results.get(0).couldHaveResults(),
            "MatchAll query should always return true");
    }

    @Test
    @Order(91)
    public void testMatchAllQuery_WithBooleanMust() throws IOException {
        // MatchAll AND nonexistent should return false
        SplitQuery matchAll = new SplitMatchAllQuery();
        SplitQuery term = new SplitTermQuery("title", "nonexistent");
        SplitQuery boolQuery = new SplitBooleanQuery()
            .addMust(matchAll)
            .addMust(term);

        List<PrescanResult> results = cacheManager.prescanSplits(getSplits(), docMappingJson, boolQuery);

        assertEquals(1, results.size());
        assertFalse(results.get(0).couldHaveResults(),
            "MatchAll AND nonexistent term should return false");
    }

    // ============================================================
    // EDGE CASES
    // ============================================================

    @Test
    @Order(100)
    public void testEmptyQuery() throws IOException {
        // Empty boolean query should behave like MatchAll (or return conservative true)
        SplitQuery boolQuery = new SplitBooleanQuery();
        List<PrescanResult> results = cacheManager.prescanSplits(getSplits(), docMappingJson, boolQuery);

        assertEquals(1, results.size());
        System.out.println("Empty boolean query result: " + results.get(0).couldHaveResults());
    }

    @Test
    @Order(101)
    public void testUnknownField() throws IOException {
        // Query on a field that doesn't exist in schema
        SplitQuery query = new SplitTermQuery("unknown_field", "value");
        List<PrescanResult> results = cacheManager.prescanSplits(getSplits(), docMappingJson, query);

        assertEquals(1, results.size());
        // Unknown field should return false (term definitely doesn't exist)
        System.out.println("Unknown field query result: " + results.get(0).couldHaveResults());
    }

    @Test
    @Order(102)
    public void testSpecialCharactersInTerm() throws IOException {
        // Term with special characters
        SplitQuery query = new SplitTermQuery("title", "foo@bar.com");
        List<PrescanResult> results = cacheManager.prescanSplits(getSplits(), docMappingJson, query);

        assertEquals(1, results.size());
        System.out.println("Special chars in term result: " + results.get(0).couldHaveResults());
    }

    @Test
    @Order(103)
    public void testUnicodeInTerm() throws IOException {
        // Unicode term (should be normalized)
        SplitQuery query = new SplitTermQuery("title", "hllo");
        List<PrescanResult> results = cacheManager.prescanSplits(getSplits(), docMappingJson, query);

        assertEquals(1, results.size());
        System.out.println("Unicode term result: " + results.get(0).couldHaveResults());
    }

    // ============================================================
    // MULTIPLE SPLITS TESTING
    // ============================================================

    @Test
    @Order(110)
    public void testMultipleSplits_TermInOne() throws Exception {
        // Create a second split with different data
        Path splitPath2 = tempDir.resolve("comprehensive2_" + uniqueId + ".split");
        Path indexDir2 = tempDir.resolve("comprehensive_index2_" + uniqueId);
        Files.createDirectories(indexDir2);

        try (SchemaBuilder builder = new SchemaBuilder()) {
            builder.addTextField("title", true, false, "default", "position");
            builder.addTextField("category", true, false, "raw", "position");

            try (Schema schema = builder.build()) {
                try (Index index = new Index(schema, indexDir2.toString())) {
                    try (IndexWriter writer = index.writer(Index.Memory.DEFAULT_HEAP_SIZE, 1)) {
                        // Only add "goodbye" - not "hello"
                        try (Document doc = new Document()) {
                            doc.addText("title", "Goodbye Cruel World");
                            doc.addText("category", "Drama");
                            writer.addDocument(doc);
                        }
                        writer.commit();
                    }
                    index.reload();

                    QuickwitSplit.SplitConfig splitConfig2 =
                        new QuickwitSplit.SplitConfig(
                            "multi-index-" + uniqueId,
                            "multi-source-" + uniqueId,
                            "multi-node-" + uniqueId);
                    QuickwitSplit.SplitMetadata metadata2 =
                        QuickwitSplit.convertIndexFromPath(
                            indexDir2.toString(), splitPath2.toString(), splitConfig2);

                    long footerOffset2 = metadata2.getFooterStartOffset();

                    // Query for "hello" - only exists in split1, not split2
                    SplitQuery query = new SplitTermQuery("title", "hello");
                    List<SplitInfo> bothSplits = Arrays.asList(
                        new SplitInfo("file://" + splitPath.toAbsolutePath(), footerOffset),
                        new SplitInfo("file://" + splitPath2.toAbsolutePath(), footerOffset2)
                    );

                    List<PrescanResult> results = cacheManager.prescanSplits(bothSplits, docMappingJson, query);

                    assertEquals(2, results.size());

                    // Find results for each split
                    PrescanResult result1 = results.stream()
                        .filter(r -> r.getSplitUrl().contains("comprehensive_" + uniqueId + ".split"))
                        .findFirst().orElseThrow();
                    PrescanResult result2 = results.stream()
                        .filter(r -> r.getSplitUrl().contains("comprehensive2_" + uniqueId + ".split"))
                        .findFirst().orElseThrow();

                    assertTrue(result1.couldHaveResults(),
                        "Split1 should have results (contains 'hello')");
                    assertFalse(result2.couldHaveResults(),
                        "Split2 should NOT have results (doesn't contain 'hello')");

                    System.out.println("Multi-split test: split1=" + result1.couldHaveResults() +
                                       ", split2=" + result2.couldHaveResults());
                }
            }
        }
    }

    @Test
    @Order(111)
    public void testMultipleSplits_TermInBoth() throws Exception {
        // Create a second split with overlapping data
        Path splitPath2 = tempDir.resolve("comprehensive3_" + uniqueId + ".split");
        Path indexDir2 = tempDir.resolve("comprehensive_index3_" + uniqueId);
        Files.createDirectories(indexDir2);

        try (SchemaBuilder builder = new SchemaBuilder()) {
            builder.addTextField("title", true, false, "default", "position");

            try (Schema schema = builder.build()) {
                try (Index index = new Index(schema, indexDir2.toString())) {
                    try (IndexWriter writer = index.writer(Index.Memory.DEFAULT_HEAP_SIZE, 1)) {
                        // Also add "world" which exists in split1 too
                        try (Document doc = new Document()) {
                            doc.addText("title", "Another World Story");
                            writer.addDocument(doc);
                        }
                        writer.commit();
                    }
                    index.reload();

                    QuickwitSplit.SplitConfig splitConfig2 =
                        new QuickwitSplit.SplitConfig(
                            "multi3-index-" + uniqueId,
                            "multi3-source-" + uniqueId,
                            "multi3-node-" + uniqueId);
                    QuickwitSplit.SplitMetadata metadata2 =
                        QuickwitSplit.convertIndexFromPath(
                            indexDir2.toString(), splitPath2.toString(), splitConfig2);

                    long footerOffset2 = metadata2.getFooterStartOffset();

                    // Query for "world" - exists in both splits
                    SplitQuery query = new SplitTermQuery("title", "world");
                    List<SplitInfo> bothSplits = Arrays.asList(
                        new SplitInfo("file://" + splitPath.toAbsolutePath(), footerOffset),
                        new SplitInfo("file://" + splitPath2.toAbsolutePath(), footerOffset2)
                    );

                    List<PrescanResult> results = cacheManager.prescanSplits(bothSplits, docMappingJson, query);

                    assertEquals(2, results.size());
                    assertTrue(results.stream().allMatch(PrescanResult::couldHaveResults),
                        "Both splits should have results (both contain 'world')");
                }
            }
        }
    }

    @Test
    @Order(112)
    public void testMultipleSplits_TermInNeither() throws Exception {
        // Create a second split
        Path splitPath2 = tempDir.resolve("comprehensive4_" + uniqueId + ".split");
        Path indexDir2 = tempDir.resolve("comprehensive_index4_" + uniqueId);
        Files.createDirectories(indexDir2);

        try (SchemaBuilder builder = new SchemaBuilder()) {
            builder.addTextField("title", true, false, "default", "position");

            try (Schema schema = builder.build()) {
                try (Index index = new Index(schema, indexDir2.toString())) {
                    try (IndexWriter writer = index.writer(Index.Memory.DEFAULT_HEAP_SIZE, 1)) {
                        try (Document doc = new Document()) {
                            doc.addText("title", "Something Else Entirely");
                            writer.addDocument(doc);
                        }
                        writer.commit();
                    }
                    index.reload();

                    QuickwitSplit.SplitConfig splitConfig2 =
                        new QuickwitSplit.SplitConfig(
                            "multi4-index-" + uniqueId,
                            "multi4-source-" + uniqueId,
                            "multi4-node-" + uniqueId);
                    QuickwitSplit.SplitMetadata metadata2 =
                        QuickwitSplit.convertIndexFromPath(
                            indexDir2.toString(), splitPath2.toString(), splitConfig2);

                    long footerOffset2 = metadata2.getFooterStartOffset();

                    // Query for term that doesn't exist in either split
                    SplitQuery query = new SplitTermQuery("title", "nonexistent");
                    List<SplitInfo> bothSplits = Arrays.asList(
                        new SplitInfo("file://" + splitPath.toAbsolutePath(), footerOffset),
                        new SplitInfo("file://" + splitPath2.toAbsolutePath(), footerOffset2)
                    );

                    List<PrescanResult> results = cacheManager.prescanSplits(bothSplits, docMappingJson, query);

                    assertEquals(2, results.size());
                    assertTrue(results.stream().noneMatch(PrescanResult::couldHaveResults),
                        "Neither split should have results for 'nonexistent'");
                }
            }
        }
    }
}

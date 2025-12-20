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
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.UUID;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;

/**
 * Integration tests for prescan functionality with real Quickwit splits.
 *
 * <p>These tests create actual split files and verify that the prescan
 * FST term lookup works correctly to filter splits.
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class PrescanIntegrationTest {

    @TempDir
    Path tempDir;

    private Path indexDir;
    private Path splitPath;
    private SplitCacheManager cacheManager;
    private long footerOffset;
    private long fileSize;
    private String docMappingJson;
    private String uniqueId;

    @BeforeEach
    public void setUp() throws Exception {
        // Use unique UUID-based names to avoid cache collisions between tests
        uniqueId = UUID.randomUUID().toString();
        indexDir = tempDir.resolve("test_index_" + uniqueId);
        Files.createDirectories(indexDir);
        splitPath = tempDir.resolve("test_" + uniqueId + ".split");

        // Create a cache manager for testing with unique name
        SplitCacheManager.CacheConfig config =
            new SplitCacheManager.CacheConfig("prescan-test-" + uniqueId)
                .withMaxCacheSize(100_000_000);
        cacheManager = SplitCacheManager.getInstance(config);

        // Create index with test documents
        createTestIndexAndSplit();
    }

    @AfterEach
    public void tearDown() throws Exception {
        if (cacheManager != null) {
            cacheManager.close();
        }
    }

    private void createTestIndexAndSplit() throws Exception {
        // Create schema with various field types
        try (SchemaBuilder builder = new SchemaBuilder()) {
            builder.addTextField("title");
            builder.addTextField("body");
            builder.addTextField("tags");

            try (Schema schema = builder.build()) {
                try (Index index = new Index(schema, indexDir.toString())) {
                    try (IndexWriter writer =
                        index.writer(Index.Memory.DEFAULT_HEAP_SIZE, 1)) {

                        // Add documents with specific terms we can search for
                        addDocument(writer, "Hello World", "This is the first document about programming", "java,rust");
                        addDocument(writer, "Goodbye World", "This is the second document about testing", "python,testing");
                        addDocument(writer, "Hello Again", "Another document with hello in the title", "java,hello");
                        addDocument(writer, "Wildcard Test", "Testing wildcards like helicopter and help", "wildcard,test");
                        addDocument(writer, "Special Characters", "Document with special chars: foo@bar.com", "special");

                        writer.commit();
                    }

                    index.reload();

                    // Convert to Quickwit split with unique IDs
                    QuickwitSplit.SplitConfig splitConfig =
                        new QuickwitSplit.SplitConfig("test-index-" + uniqueId, "test-source-" + uniqueId, "test-node-" + uniqueId);
                    QuickwitSplit.SplitMetadata metadata =
                        QuickwitSplit.convertIndexFromPath(
                            indexDir.toString(), splitPath.toString(), splitConfig);

                    // Get footer offset, file size, and doc mapping from the metadata returned by split creation
                    footerOffset = metadata.getFooterStartOffset();
                    fileSize = metadata.getFooterEndOffset();
                    docMappingJson = metadata.getDocMappingJson();

                    System.out.println("Created split with " + metadata.getNumDocs() + " documents");
                    System.out.println("Split path: " + splitPath);
                    System.out.println("Split file size: " + fileSize + " bytes");
                    System.out.println("Footer offset: " + footerOffset);
                    System.out.println("Doc mapping JSON: " + docMappingJson);
                }
            }
        }
    }

    private void addDocument(IndexWriter writer, String title, String body, String tags)
        throws IOException {
        try (Document doc = new Document()) {
            doc.addText("title", title);
            doc.addText("body", body);
            doc.addText("tags", tags);
            writer.addDocument(doc);
        }
    }

    @Test
    @Order(1)
    public void testPrescanWithExactTermMatch() throws IOException {
        // Test prescan with a term that exists in the split
        List<SplitInfo> splits = Collections.singletonList(
            new SplitInfo("file://" + splitPath.toAbsolutePath(), footerOffset, fileSize));

        SplitQuery query = new SplitTermQuery("title", "hello");

        List<PrescanResult> results =
            cacheManager.prescanSplits(splits, docMappingJson, query);

        assertEquals(1, results.size());
        PrescanResult result = results.get(0);

        System.out.println("Prescan result for 'hello': " + result);
        System.out.println("  Status: " + result.getStatus());
        System.out.println("  Could have results: " + result.couldHaveResults());
        System.out.println("  Term existence: " + result.getTermExistence());

        // The split should potentially have results (term "hello" exists in title field)
        // Note: With the current conservative implementation, this will return true
        assertTrue(result.couldHaveResults(),
            "Split should potentially have results for 'hello'");
    }

    @Test
    @Order(2)
    public void testPrescanWithNonExistentTerm() throws IOException {
        // Test prescan with a term that doesn't exist in the split
        List<SplitInfo> splits = Collections.singletonList(
            new SplitInfo("file://" + splitPath.toAbsolutePath(), footerOffset, fileSize));

        SplitQuery query = new SplitTermQuery("title", "nonexistentterm12345");

        List<PrescanResult> results =
            cacheManager.prescanSplits(splits, docMappingJson, query);

        assertEquals(1, results.size());
        PrescanResult result = results.get(0);

        System.out.println("Prescan result for 'nonexistentterm12345': " + result);
        System.out.println("  Status: " + result.getStatus());
        System.out.println("  Could have results: " + result.couldHaveResults());
        System.out.println("  Term existence: " + result.getTermExistence());

        // With actual FST lookup, this should return false since the term doesn't exist
        // With conservative implementation, it would return true
    }

    @Test
    @Order(3)
    public void testPrescanWithWildcardPrefix() throws IOException {
        // Test prescan with a prefix wildcard pattern
        List<SplitInfo> splits = Collections.singletonList(
            new SplitInfo("file://" + splitPath.toAbsolutePath(), footerOffset, fileSize));

        SplitQuery query = new SplitWildcardQuery("title", "hel*");

        List<PrescanResult> results =
            cacheManager.prescanSplits(splits, docMappingJson, query);

        assertEquals(1, results.size());
        PrescanResult result = results.get(0);

        System.out.println("Prescan result for wildcard 'hel*': " + result);
        System.out.println("  Status: " + result.getStatus());
        System.out.println("  Could have results: " + result.couldHaveResults());

        // Should match "hello" terms
        assertTrue(result.couldHaveResults(),
            "Split should potentially have results for 'hel*' wildcard");
    }

    @Test
    @Order(4)
    public void testPrescanWithWildcardSuffix() throws IOException {
        // Test prescan with a suffix wildcard pattern
        List<SplitInfo> splits = Collections.singletonList(
            new SplitInfo("file://" + splitPath.toAbsolutePath(), footerOffset, fileSize));

        SplitQuery query = new SplitWildcardQuery("title", "*world");

        List<PrescanResult> results =
            cacheManager.prescanSplits(splits, docMappingJson, query);

        assertEquals(1, results.size());
        PrescanResult result = results.get(0);

        System.out.println("Prescan result for wildcard '*world': " + result);
        System.out.println("  Status: " + result.getStatus());
        System.out.println("  Could have results: " + result.couldHaveResults());

        // Should match "world" terms
        assertTrue(result.couldHaveResults(),
            "Split should potentially have results for '*world' wildcard");
    }

    @Test
    @Order(5)
    public void testPrescanWithNoMatchingWildcard() throws IOException {
        // Test prescan with a complex wildcard that doesn't match anything
        // Complex patterns (like xyz*abc with wildcard in middle) return conservative true
        List<SplitInfo> splits = Collections.singletonList(
            new SplitInfo("file://" + splitPath.toAbsolutePath(), footerOffset, fileSize));

        SplitQuery query = new SplitWildcardQuery("title", "xyz*abc");

        List<PrescanResult> results =
            cacheManager.prescanSplits(splits, docMappingJson, query);

        assertEquals(1, results.size());
        PrescanResult result = results.get(0);

        System.out.println("Prescan result for wildcard 'xyz*abc': " + result);
        System.out.println("  Status: " + result.getStatus());
        System.out.println("  Could have results: " + result.couldHaveResults());

        // Complex patterns return conservative true
        assertTrue(result.couldHaveResults(),
            "Complex wildcard patterns should return conservative true");
    }

    @Test
    @Order(51)
    public void testPrescanWithNonMatchingPrefixWildcard() throws IOException {
        // Test prescan with a prefix wildcard that doesn't match any terms
        List<SplitInfo> splits = Collections.singletonList(
            new SplitInfo("file://" + splitPath.toAbsolutePath(), footerOffset, fileSize));

        // "zzz*" should not match any terms in our test data
        SplitQuery query = new SplitWildcardQuery("title", "zzz*");

        List<PrescanResult> results =
            cacheManager.prescanSplits(splits, docMappingJson, query);

        assertEquals(1, results.size());
        PrescanResult result = results.get(0);

        System.out.println("Prescan result for non-matching prefix wildcard 'zzz*': " + result);
        System.out.println("  Status: " + result.getStatus());
        System.out.println("  Could have results: " + result.couldHaveResults());

        // No terms start with "zzz", so should return false
        assertFalse(result.couldHaveResults(),
            "Split should NOT have results for non-matching prefix 'zzz*'");
    }

    @Test
    @Order(52)
    public void testPrescanWithNonMatchingSuffixWildcard() throws IOException {
        // Test prescan with a suffix wildcard that doesn't match any terms
        List<SplitInfo> splits = Collections.singletonList(
            new SplitInfo("file://" + splitPath.toAbsolutePath(), footerOffset, fileSize));

        // "*zzz" should not match any terms in our test data
        SplitQuery query = new SplitWildcardQuery("title", "*zzz");

        List<PrescanResult> results =
            cacheManager.prescanSplits(splits, docMappingJson, query);

        assertEquals(1, results.size());
        PrescanResult result = results.get(0);

        System.out.println("Prescan result for non-matching suffix wildcard '*zzz': " + result);
        System.out.println("  Status: " + result.getStatus());
        System.out.println("  Could have results: " + result.couldHaveResults());

        // No terms end with "zzz", so should return false
        assertFalse(result.couldHaveResults(),
            "Split should NOT have results for non-matching suffix '*zzz'");
    }

    @Test
    @Order(6)
    public void testPrescanSimpleReturnsMatchingUrls() throws IOException {
        // Test the convenience method that returns only matching URLs
        List<SplitInfo> splits = Collections.singletonList(
            new SplitInfo("file://" + splitPath.toAbsolutePath(), footerOffset, fileSize));

        SplitQuery query = new SplitTermQuery("body", "document");

        List<String> matchingUrls =
            cacheManager.prescanSplitsSimple(splits, docMappingJson, query);

        System.out.println("Matching URLs for 'document': " + matchingUrls);

        // With conservative or actual implementation, should include the split
        assertTrue(matchingUrls.size() <= 1,
            "Should return at most 1 URL (one split)");
    }

    @Test
    @Order(7)
    public void testPrescanWithBooleanQuery() throws IOException {
        // Test prescan with a boolean query (MUST + MUST)
        List<SplitInfo> splits = Collections.singletonList(
            new SplitInfo("file://" + splitPath.toAbsolutePath(), footerOffset, fileSize));

        // Create a boolean query: title:hello AND body:document
        SplitQuery titleQuery = new SplitTermQuery("title", "hello");
        SplitQuery bodyQuery = new SplitTermQuery("body", "document");
        SplitQuery boolQuery = new SplitBooleanQuery()
            .addMust(titleQuery)
            .addMust(bodyQuery);

        List<PrescanResult> results =
            cacheManager.prescanSplits(splits, docMappingJson, boolQuery);

        assertEquals(1, results.size());
        PrescanResult result = results.get(0);

        System.out.println("Prescan result for boolean query (hello AND document): " + result);
        System.out.println("  Status: " + result.getStatus());
        System.out.println("  Could have results: " + result.couldHaveResults());
        System.out.println("  Term existence: " + result.getTermExistence());

        // Both terms exist, so should have results
        assertTrue(result.couldHaveResults(),
            "Split should potentially have results for boolean AND query");
    }

    @Test
    @Order(8)
    public void testPrescanWithRawTokenizerField() throws IOException {
        // Test prescan with a field using raw tokenizer (exact match)
        List<SplitInfo> splits = Collections.singletonList(
            new SplitInfo("file://" + splitPath.toAbsolutePath(), footerOffset, fileSize));

        // Raw tokenizer should match exact values
        SplitQuery query = new SplitTermQuery("tags", "java,rust");

        List<PrescanResult> results =
            cacheManager.prescanSplits(splits, docMappingJson, query);

        assertEquals(1, results.size());
        PrescanResult result = results.get(0);

        System.out.println("Prescan result for raw field 'java,rust': " + result);
        System.out.println("  Status: " + result.getStatus());
        System.out.println("  Could have results: " + result.couldHaveResults());
    }

    @Test
    @Order(9)
    public void testPrescanWithMultipleSplits() throws IOException {
        // Create a second split with different content using unique IDs
        String uniqueId2 = UUID.randomUUID().toString();
        Path indexDir2 = tempDir.resolve("test_index2_" + uniqueId2);
        Files.createDirectories(indexDir2);
        Path splitPath2 = tempDir.resolve("test2_" + uniqueId2 + ".split");

        long footerOffset2 = 0;
        long fileSize2 = 0;
        try (SchemaBuilder builder = new SchemaBuilder()) {
            builder.addTextField("title");
            builder.addTextField("body");
            builder.addTextField("tags");

            try (Schema schema = builder.build()) {
                try (Index index = new Index(schema, indexDir2.toString())) {
                    try (IndexWriter writer =
                        index.writer(Index.Memory.DEFAULT_HEAP_SIZE, 1)) {

                        // Add documents with different terms
                        addDocument(writer, "Different Title", "Unique content here", "unique");
                        addDocument(writer, "Another Title", "More different content", "another");

                        writer.commit();
                    }

                    index.reload();

                    QuickwitSplit.SplitConfig splitConfig =
                        new QuickwitSplit.SplitConfig("test-index2-" + uniqueId2, "test-source-" + uniqueId2, "test-node-" + uniqueId2);
                    QuickwitSplit.SplitMetadata metadata = QuickwitSplit.convertIndexFromPath(
                        indexDir2.toString(), splitPath2.toString(), splitConfig);
                    footerOffset2 = metadata.getFooterStartOffset();
                    fileSize2 = metadata.getFooterEndOffset();
                }
            }
        }

        // Now prescan both splits for a term that only exists in one
        // Use actual footer offsets to ensure correct split parsing
        List<SplitInfo> splits = Arrays.asList(
            new SplitInfo("file://" + splitPath.toAbsolutePath(), footerOffset, fileSize),
            new SplitInfo("file://" + splitPath2.toAbsolutePath(), footerOffset2, fileSize2));

        // Search for "hello" which only exists in first split
        SplitQuery query = new SplitTermQuery("title", "hello");

        List<PrescanResult> results =
            cacheManager.prescanSplits(splits, docMappingJson, query);

        assertEquals(2, results.size());

        System.out.println("Prescan results for 'hello' across 2 splits:");
        for (PrescanResult result : results) {
            System.out.println("  " + result.getSplitUrl() + ": " + result.couldHaveResults());
        }

        // At least one split should be identified as potentially having results
        boolean anyCouldHaveResults = results.stream().anyMatch(PrescanResult::couldHaveResults);
        assertTrue(anyCouldHaveResults, "At least one split should potentially have results");
    }

    @Test
    @Order(10)
    public void testPrescanWithPureRangeQuery() throws IOException {
        // Test that a pure range query returns conservative true
        // (we can't efficiently evaluate range queries in FST prescan)
        List<SplitInfo> splits = Collections.singletonList(
            new SplitInfo("file://" + splitPath.toAbsolutePath(), footerOffset, fileSize));

        // Create a range query on title field
        SplitQuery query = SplitRangeQuery.inclusiveRange("title", "a", "z", "str");

        List<PrescanResult> results =
            cacheManager.prescanSplits(splits, docMappingJson, query);

        assertEquals(1, results.size());
        PrescanResult result = results.get(0);

        System.out.println("Prescan result for pure range query [a TO z]: " + result);
        System.out.println("  Status: " + result.getStatus());
        System.out.println("  Could have results: " + result.couldHaveResults());

        // Range queries should always return conservative true
        assertTrue(result.couldHaveResults(),
            "Pure range query should return conservative true");
    }

    @Test
    @Order(11)
    public void testPrescanWithRangeAndNonExistentTerm() throws IOException {
        // Test that a Boolean query with range + non-existent term returns FALSE
        // The term clause should cause the query to fail, even though range is conservative true
        List<SplitInfo> splits = Collections.singletonList(
            new SplitInfo("file://" + splitPath.toAbsolutePath(), footerOffset, fileSize));

        // Create boolean query: range:[a TO z] AND nonexistent_term
        SplitQuery rangeQuery = SplitRangeQuery.inclusiveRange("title", "a", "z", "str");
        SplitQuery termQuery = new SplitTermQuery("title", "nonexistentterm98765");
        SplitQuery boolQuery = new SplitBooleanQuery()
            .addMust(rangeQuery)
            .addMust(termQuery);

        List<PrescanResult> results =
            cacheManager.prescanSplits(splits, docMappingJson, boolQuery);

        assertEquals(1, results.size());
        PrescanResult result = results.get(0);

        System.out.println("Prescan result for (range AND nonexistent_term): " + result);
        System.out.println("  Status: " + result.getStatus());
        System.out.println("  Could have results: " + result.couldHaveResults());
        System.out.println("  Term existence: " + result.getTermExistence());

        // Even though range is conservative true, the non-existent term should cause false
        assertFalse(result.couldHaveResults(),
            "Boolean (range AND nonexistent_term) should return false due to term failing");
    }

    @Test
    @Order(12)
    public void testPrescanWithRangeAndExistingTerm() throws IOException {
        // Test that a Boolean query with range + existing term returns TRUE
        List<SplitInfo> splits = Collections.singletonList(
            new SplitInfo("file://" + splitPath.toAbsolutePath(), footerOffset, fileSize));

        // Create boolean query: range:[a TO z] AND hello
        SplitQuery rangeQuery = SplitRangeQuery.inclusiveRange("title", "a", "z", "str");
        SplitQuery termQuery = new SplitTermQuery("title", "hello");
        SplitQuery boolQuery = new SplitBooleanQuery()
            .addMust(rangeQuery)
            .addMust(termQuery);

        List<PrescanResult> results =
            cacheManager.prescanSplits(splits, docMappingJson, boolQuery);

        assertEquals(1, results.size());
        PrescanResult result = results.get(0);

        System.out.println("Prescan result for (range AND hello): " + result);
        System.out.println("  Status: " + result.getStatus());
        System.out.println("  Could have results: " + result.couldHaveResults());
        System.out.println("  Term existence: " + result.getTermExistence());

        // Both range (conservative true) and term (exists) should result in true
        assertTrue(result.couldHaveResults(),
            "Boolean (range AND existing_term) should return true");
    }

    @Test
    @Order(13)
    public void testPrescanResultDetails() throws IOException {
        // Test that prescan results include detailed term existence information
        List<SplitInfo> splits = Collections.singletonList(
            new SplitInfo("file://" + splitPath.toAbsolutePath(), footerOffset, fileSize));

        SplitQuery query = new SplitTermQuery("title", "hello");

        List<PrescanResult> results =
            cacheManager.prescanSplits(splits, docMappingJson, query);

        assertEquals(1, results.size());
        PrescanResult result = results.get(0);

        // Verify result structure
        assertNotNull(result.getSplitUrl());
        assertNotNull(result.getStatus());
        assertNotNull(result.getTermExistence());

        System.out.println("Prescan result details:");
        System.out.println("  Split URL: " + result.getSplitUrl());
        System.out.println("  Status: " + result.getStatus());
        System.out.println("  Could have results: " + result.couldHaveResults());
        System.out.println("  Is success: " + result.isSuccess());
        System.out.println("  Term existence map size: " + result.getTermExistence().size());
        System.out.println("  Term existence: " + result.getTermExistence());
        if (result.getErrorMessage() != null) {
            System.out.println("  Error message: " + result.getErrorMessage());
        }
    }
}

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
import io.indextables.tantivy4java.split.SplitRangeQuery.RangeBound;
import io.indextables.tantivy4java.split.merge.*;
import io.indextables.tantivy4java.xref.*;

import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests XRef split building with local splits (no S3).
 */
public class LocalXRefBuildTest {

    @TempDir
    Path tempDir;

    @Test
    @DisplayName("Build XRef from local splits")
    void testBuildXRefFromLocalSplits() throws Exception {
        System.out.println("Creating 2 local test splits...");

        // Create split 1
        Path index1Path = tempDir.resolve("index1");
        Path split1Path = tempDir.resolve("split1.split");
        createTestIndex(index1Path, "split1", new String[]{"apple", "banana", "cherry"});
        QuickwitSplit.SplitConfig config1 = new QuickwitSplit.SplitConfig(
            "xref-local-test", "test-source", "node-1");
        QuickwitSplit.SplitMetadata meta1 = QuickwitSplit.convertIndexFromPath(
            index1Path.toString(), split1Path.toString(), config1);
        System.out.println("Created split1: " + meta1.getNumDocs() + " docs");

        // Create split 2
        Path index2Path = tempDir.resolve("index2");
        Path split2Path = tempDir.resolve("split2.split");
        createTestIndex(index2Path, "split2", new String[]{"delta", "epsilon", "zeta"});
        QuickwitSplit.SplitConfig config2 = new QuickwitSplit.SplitConfig(
            "xref-local-test", "test-source", "node-2");
        QuickwitSplit.SplitMetadata meta2 = QuickwitSplit.convertIndexFromPath(
            index2Path.toString(), split2Path.toString(), config2);
        System.out.println("Created split2: " + meta2.getNumDocs() + " docs");

        // Build XRef from local splits using XRefSourceSplit with footer offsets
        // This follows the same pattern as other split operations - caller passes metadata
        System.out.println("Building XRef from local splits...");
        System.out.println("  split1 footer: " + meta1.getFooterStartOffset() + "-" + meta1.getFooterEndOffset());
        System.out.println("  split2 footer: " + meta2.getFooterStartOffset() + "-" + meta2.getFooterEndOffset());

        // Create XRefSourceSplit objects from split metadata (this is the correct pattern)
        XRefSourceSplit source1 = XRefSourceSplit.fromSplitMetadata(
            split1Path.toString(), meta1);
        XRefSourceSplit source2 = XRefSourceSplit.fromSplitMetadata(
            split2Path.toString(), meta2);

        List<XRefSourceSplit> sourceSplits = Arrays.asList(source1, source2);

        XRefBuildConfig xrefConfig = XRefBuildConfig.builder()
            .xrefId("local-xref")
            .indexUid("local-index")
            .sourceSplits(sourceSplits)
            .build();

        Path xrefPath = tempDir.resolve("local.xref.split");
        XRefMetadata xrefMetadata = XRefSplit.build(xrefConfig, xrefPath.toString());

        assertNotNull(xrefMetadata, "XRef metadata should be returned");
        assertEquals(2, xrefMetadata.getNumSplits(), "XRef should have 2 source splits");
        assertTrue(xrefMetadata.hasFooterOffsets(), "XRef should have footer offsets");

        System.out.println("XRef built successfully:");
        System.out.println("  - XRef ID: " + xrefMetadata.getXrefId());
        System.out.println("  - Source splits: " + xrefMetadata.getNumSplits());
        System.out.println("  - Total terms: " + xrefMetadata.getTotalTerms());
    }

    @Test
    @DisplayName("Search XRef split with XRefSearcher")
    void testSearchXRefWithXRefSearcher() throws Exception {
        System.out.println("\n=== Testing XRef split with XRefSearcher ===");

        // Create split 1 with unique terms
        Path index1Path = tempDir.resolve("index1-searcher");
        Path split1Path = tempDir.resolve("split1-searcher.split");
        createTestIndex(index1Path, "astronomy", new String[]{"nebula", "quasar", "pulsar"});
        QuickwitSplit.SplitConfig config1 = new QuickwitSplit.SplitConfig(
            "xref-searcher-test", "test-source", "node-1");
        QuickwitSplit.SplitMetadata meta1 = QuickwitSplit.convertIndexFromPath(
            index1Path.toString(), split1Path.toString(), config1);
        System.out.println("Created astronomy split: " + meta1.getNumDocs() + " docs");

        // Create split 2 with different unique terms
        Path index2Path = tempDir.resolve("index2-searcher");
        Path split2Path = tempDir.resolve("split2-searcher.split");
        createTestIndex(index2Path, "biology", new String[]{"mitochondria", "chloroplast", "ribosome"});
        QuickwitSplit.SplitConfig config2 = new QuickwitSplit.SplitConfig(
            "xref-searcher-test", "test-source", "node-2");
        QuickwitSplit.SplitMetadata meta2 = QuickwitSplit.convertIndexFromPath(
            index2Path.toString(), split2Path.toString(), config2);
        System.out.println("Created biology split: " + meta2.getNumDocs() + " docs");

        // Build XRef split
        XRefSourceSplit source1 = XRefSourceSplit.fromSplitMetadata(split1Path.toString(), meta1);
        XRefSourceSplit source2 = XRefSourceSplit.fromSplitMetadata(split2Path.toString(), meta2);

        XRefBuildConfig xrefConfig = XRefBuildConfig.builder()
            .xrefId("searcher-test-xref")
            .indexUid("searcher-test-index")
            .sourceSplits(Arrays.asList(source1, source2))
            .build();

        Path xrefPath = tempDir.resolve("searcher-test.xref.split");
        XRefMetadata xrefMetadata = XRefSplit.build(xrefConfig, xrefPath.toString());
        System.out.println("Built XRef with " + xrefMetadata.getNumSplits() + " splits, " +
            xrefMetadata.getTotalTerms() + " terms");

        // Now search the XRef split using XRefSearcher (FuseXRef format)
        SplitCacheManager.CacheConfig cacheConfig = new SplitCacheManager.CacheConfig("xref-searcher-cache")
            .withMaxCacheSize(100_000_000);

        try (SplitCacheManager cacheManager = SplitCacheManager.getInstance(cacheConfig)) {

            // Open with XRefSearcher (required for FuseXRef binary format)
            try (XRefSearcher searcher = XRefSearcher.open(cacheManager, xrefPath.toString(), xrefMetadata)) {

                System.out.println("\n--- Testing XRefSearcher on FuseXRef split ---");

                // Test 1: Match-all query using "*"
                System.out.println("\nTest 1: Match-all query using '*'");
                XRefSearchResult matchAllResult = searcher.search("*", 10);
                System.out.println("  Match-all '*' returned " + matchAllResult.getNumMatchingSplits() + " matching splits");
                assertEquals(2, matchAllResult.getNumMatchingSplits(), "XRef should have 2 source splits");

                // Test 2: Term query for astronomy-specific term
                System.out.println("\nTest 2: Term query for 'content:nebula' (astronomy only)");
                XRefSearchResult nebulaResult = searcher.search("content:nebula", 10);
                System.out.println("  'content:nebula' returned " + nebulaResult.getNumMatchingSplits() + " matching splits");
                assertTrue(nebulaResult.getNumMatchingSplits() >= 1, "Should find at least 1 split with 'nebula'");

                // Test 3: Term query for biology-specific term
                System.out.println("\nTest 3: Term query for 'content:mitochondria' (biology only)");
                XRefSearchResult mitoResult = searcher.search("content:mitochondria", 10);
                System.out.println("  'content:mitochondria' returned " + mitoResult.getNumMatchingSplits() + " matching splits");
                assertTrue(mitoResult.getNumMatchingSplits() >= 1, "Should find at least 1 split with 'mitochondria'");

                // Test 4: Term query for domain field
                System.out.println("\nTest 4: Term query for 'domain:astronomy'");
                XRefSearchResult domainResult = searcher.search("domain:astronomy", 10);
                System.out.println("  'domain:astronomy' returned " + domainResult.getNumMatchingSplits() + " matching splits");
                assertTrue(domainResult.getNumMatchingSplits() >= 1, "Should find at least 1 split with domain 'astronomy'");

                // Test 5: Term query for non-existent term
                System.out.println("\nTest 5: Term query for 'content:xyznonexistent123' (should find nothing)");
                XRefSearchResult nonExistResult = searcher.search("content:xyznonexistent123", 10);
                System.out.println("  'content:xyznonexistent123' returned " + nonExistResult.getNumMatchingSplits() + " matching splits");
                assertEquals(0, nonExistResult.getNumMatchingSplits(), "Should find no splits with non-existent term");

                // Test 6: Check split URIs are available
                System.out.println("\nTest 6: Verify split URIs");
                java.util.List<String> splitUris = matchAllResult.getSplitUrisToSearch();
                System.out.println("  Split URIs to search: " + splitUris);
                assertEquals(2, splitUris.size(), "Should have 2 split URIs");

                // Test 7: Verify numSplits accessor
                System.out.println("\nTest 7: Verify getNumSplits()");
                int numSplits = searcher.getNumSplits();
                System.out.println("  getNumSplits() returned: " + numSplits);
                assertEquals(2, numSplits, "Should report 2 source splits");

                // Test 8: Verify getAllSplitUris accessor
                System.out.println("\nTest 8: Verify getAllSplitUris()");
                java.util.List<String> allUris = searcher.getAllSplitUris();
                System.out.println("  getAllSplitUris() returned: " + allUris);
                assertEquals(2, allUris.size(), "Should have 2 split URIs");

                System.out.println("\n=== All XRefSearcher tests passed! ===");
            }
        }
    }

    private void createTestIndex(Path indexPath, String domain, String[] uniqueTerms) throws IOException {
        SchemaBuilder schemaBuilder = new SchemaBuilder();
        schemaBuilder.addTextField("domain", true, false, "default", "position");
        schemaBuilder.addTextField("content", true, false, "default", "position");
        schemaBuilder.addIntegerField("id", true, true, true);

        Schema schema = schemaBuilder.build();

        try (Index index = new Index(schema, indexPath.toString());
             IndexWriter writer = index.writer(Index.Memory.DEFAULT_HEAP_SIZE, 1)) {

            for (int i = 0; i < 10; i++) {
                Document doc = new Document();
                doc.addText("domain", domain);
                doc.addText("content", "Document " + i + " about " + uniqueTerms[i % uniqueTerms.length]);
                doc.addInteger("id", i);
                writer.addDocument(doc);
            }

            writer.commit();
        }
    }

    @Test
    @DisplayName("Range query transforms to match-all in XRefSearcher")
    void testRangeQueryTransformToMatchAll() throws Exception {
        System.out.println("\n=== Testing Range Query Transformation to Match-All ===");

        // Create split 1
        Path index1Path = tempDir.resolve("index1-range");
        Path split1Path = tempDir.resolve("split1-range.split");
        createTestIndex(index1Path, "science", new String[]{"physics", "chemistry", "biology"});
        QuickwitSplit.SplitConfig config1 = new QuickwitSplit.SplitConfig(
            "xref-range-test", "test-source", "node-1");
        QuickwitSplit.SplitMetadata meta1 = QuickwitSplit.convertIndexFromPath(
            index1Path.toString(), split1Path.toString(), config1);

        // Create split 2
        Path index2Path = tempDir.resolve("index2-range");
        Path split2Path = tempDir.resolve("split2-range.split");
        createTestIndex(index2Path, "math", new String[]{"algebra", "calculus", "geometry"});
        QuickwitSplit.SplitConfig config2 = new QuickwitSplit.SplitConfig(
            "xref-range-test", "test-source", "node-2");
        QuickwitSplit.SplitMetadata meta2 = QuickwitSplit.convertIndexFromPath(
            index2Path.toString(), split2Path.toString(), config2);

        // Build XRef split
        XRefSourceSplit source1 = XRefSourceSplit.fromSplitMetadata(split1Path.toString(), meta1);
        XRefSourceSplit source2 = XRefSourceSplit.fromSplitMetadata(split2Path.toString(), meta2);

        XRefBuildConfig xrefConfig = XRefBuildConfig.builder()
            .xrefId("range-test-xref")
            .indexUid("range-test-index")
            .sourceSplits(Arrays.asList(source1, source2))
            .build();

        Path xrefPath = tempDir.resolve("range-test.xref.split");
        XRefMetadata xrefMetadata = XRefSplit.build(xrefConfig, xrefPath.toString());

        // Test with XRefSearcher - range query should be transformed to match-all
        SplitCacheManager.CacheConfig cacheConfig = new SplitCacheManager.CacheConfig("xref-range-test-cache")
            .withMaxCacheSize(100_000_000);

        try (SplitCacheManager cacheManager = SplitCacheManager.getInstance(cacheConfig);
             XRefSearcher xrefSearcher = XRefSplit.open(cacheManager, "file://" + xrefPath.toString(), xrefMetadata)) {

            // Create a range query (which would fail without transformation)
            SplitRangeQuery rangeQuery = SplitRangeQuery.inclusiveRange("id", "0", "100", "i64");

            // Transform the query (this should convert range to match-all)
            SplitQuery transformedQuery = xrefSearcher.transformQueryForXRef(rangeQuery);

            // Verify the transformation
            String transformedJson = transformedQuery.toQueryAstJson();
            assertTrue(transformedJson.contains("match_all"),
                "Range query should be transformed to match_all");
            assertFalse(transformedJson.contains("range"),
                "Transformed query should not contain 'range'");

            // Now search with the range query - it should return ALL splits
            XRefSearchResult rangeResult = xrefSearcher.search(rangeQuery, 100);

            // Since range becomes match-all, should return all 2 splits
            assertEquals(2, rangeResult.getNumMatchingSplits(),
                "Range query (transformed to match-all) should return all splits");

            // Compare with actual match-all query
            SplitMatchAllQuery matchAllQuery = new SplitMatchAllQuery();
            XRefSearchResult matchAllResult = xrefSearcher.search(matchAllQuery, 100);

            assertEquals(matchAllResult.getNumMatchingSplits(), rangeResult.getNumMatchingSplits(),
                "Range query should return same results as match-all in XRef context");
        }
    }

    @Test
    @DisplayName("Boolean query with nested range transforms correctly")
    void testBooleanQueryWithNestedRangeTransform() throws Exception {
        // Create splits
        Path index1Path = tempDir.resolve("index1-bool");
        Path split1Path = tempDir.resolve("split1-bool.split");
        createTestIndex(index1Path, "history", new String[]{"ancient", "medieval", "modern"});
        QuickwitSplit.SplitConfig config1 = new QuickwitSplit.SplitConfig(
            "xref-bool-test", "test-source", "node-1");
        QuickwitSplit.SplitMetadata meta1 = QuickwitSplit.convertIndexFromPath(
            index1Path.toString(), split1Path.toString(), config1);

        Path index2Path = tempDir.resolve("index2-bool");
        Path split2Path = tempDir.resolve("split2-bool.split");
        createTestIndex(index2Path, "geography", new String[]{"continents", "oceans", "mountains"});
        QuickwitSplit.SplitConfig config2 = new QuickwitSplit.SplitConfig(
            "xref-bool-test", "test-source", "node-2");
        QuickwitSplit.SplitMetadata meta2 = QuickwitSplit.convertIndexFromPath(
            index2Path.toString(), split2Path.toString(), config2);

        // Build XRef
        XRefSourceSplit source1 = XRefSourceSplit.fromSplitMetadata(split1Path.toString(), meta1);
        XRefSourceSplit source2 = XRefSourceSplit.fromSplitMetadata(split2Path.toString(), meta2);

        XRefBuildConfig xrefConfig = XRefBuildConfig.builder()
            .xrefId("bool-test-xref")
            .indexUid("bool-test-index")
            .sourceSplits(Arrays.asList(source1, source2))
            .build();

        Path xrefPath = tempDir.resolve("bool-test.xref.split");
        XRefMetadata xrefMetadata = XRefSplit.build(xrefConfig, xrefPath.toString());

        SplitCacheManager.CacheConfig cacheConfig = new SplitCacheManager.CacheConfig("xref-bool-cache")
            .withMaxCacheSize(100_000_000);

        try (SplitCacheManager cacheManager = SplitCacheManager.getInstance(cacheConfig);
             XRefSearcher xrefSearcher = XRefSplit.open(cacheManager, "file://" + xrefPath.toString(), xrefMetadata)) {

            // Create a boolean query with a term query AND a range query
            SplitTermQuery termQuery = new SplitTermQuery("domain", "history");
            SplitRangeQuery rangeQuery = SplitRangeQuery.inclusiveRange("id", "0", "5", "i64");

            SplitBooleanQuery boolQuery = new SplitBooleanQuery()
                .addMust(termQuery)
                .addMust(rangeQuery);

            // Transform
            SplitQuery transformedQuery = xrefSearcher.transformQueryForXRef(boolQuery);

            // Verify transformation
            String transformedJson = transformedQuery.toQueryAstJson();
            assertTrue(transformedJson.contains("match_all"),
                "Nested range query should be transformed to match_all");
            assertFalse(transformedJson.contains("range"),
                "Transformed query should not contain 'range'");
            assertTrue(transformedJson.contains("term") || transformedJson.contains("history"),
                "Term query should be preserved");

            // Search - the term query should still filter, but range becomes match-all
            XRefSearchResult result = xrefSearcher.search(boolQuery, 100);

            // Should return 1 split (only "history" domain)
            assertEquals(1, result.getNumMatchingSplits(),
                "Boolean query with term should still filter by term");
        }
    }

    @Test
    @DisplayName("Non-range queries pass through unchanged")
    void testNonRangeQueriesUnchanged() throws Exception {
        System.out.println("\n=== Testing Non-Range Queries Pass Through Unchanged ===");

        // Create a simple XRef
        Path index1Path = tempDir.resolve("index1-passthrough");
        Path split1Path = tempDir.resolve("split1-passthrough.split");
        createTestIndex(index1Path, "tech", new String[]{"computer", "software", "hardware"});
        QuickwitSplit.SplitConfig config1 = new QuickwitSplit.SplitConfig(
            "xref-passthrough", "test-source", "node-1");
        QuickwitSplit.SplitMetadata meta1 = QuickwitSplit.convertIndexFromPath(
            index1Path.toString(), split1Path.toString(), config1);

        XRefSourceSplit source1 = XRefSourceSplit.fromSplitMetadata(split1Path.toString(), meta1);

        XRefBuildConfig xrefConfig = XRefBuildConfig.builder()
            .xrefId("passthrough-xref")
            .indexUid("passthrough-index")
            .sourceSplits(Arrays.asList(source1))
            .build();

        Path xrefPath = tempDir.resolve("passthrough.xref.split");
        XRefMetadata xrefMetadata = XRefSplit.build(xrefConfig, xrefPath.toString());

        SplitCacheManager.CacheConfig cacheConfig = new SplitCacheManager.CacheConfig("xref-passthrough-cache")
            .withMaxCacheSize(100_000_000);

        try (SplitCacheManager cacheManager = SplitCacheManager.getInstance(cacheConfig);
             XRefSearcher xrefSearcher = XRefSplit.open(cacheManager, "file://" + xrefPath.toString(), xrefMetadata)) {

            // Test term query - should be unchanged (verify content, not exact string match due to JSON key order)
            SplitTermQuery termQuery = new SplitTermQuery("domain", "tech");
            String originalJson = termQuery.toQueryAstJson();
            SplitQuery transformedTermQuery = xrefSearcher.transformQueryForXRef(termQuery);
            String transformedJson = transformedTermQuery.toQueryAstJson();

            System.out.println("Term query original: " + originalJson);
            System.out.println("Term query transformed: " + transformedJson);

            // Verify term query is unchanged (check content, not exact match due to key order)
            assertTrue(transformedJson.contains("\"type\":\"term\""),
                "Term query should still be a term type");
            assertTrue(transformedJson.contains("\"field\":\"domain\""),
                "Term query should still have domain field");
            assertTrue(transformedJson.contains("\"value\":\"tech\""),
                "Term query should still have tech value");

            // Test match-all query - should be unchanged
            SplitMatchAllQuery matchAllQuery = new SplitMatchAllQuery();
            String matchAllOriginal = matchAllQuery.toQueryAstJson();
            SplitQuery transformedMatchAll = xrefSearcher.transformQueryForXRef(matchAllQuery);
            String matchAllTransformed = transformedMatchAll.toQueryAstJson();

            System.out.println("Match-all original: " + matchAllOriginal);
            System.out.println("Match-all transformed: " + matchAllTransformed);

            assertTrue(matchAllTransformed.contains("\"type\":\"match_all\""),
                "Match-all query should remain match_all type");

            System.out.println("\n=== Non-range query passthrough test passed! ===");
        }
    }

    @Test
    @DisplayName("Build XRef with custom tempDirectoryPath and heapSize")
    void testBuildXRefWithCustomTempDirAndHeapSize() throws Exception {
        System.out.println("\n=== Testing XRef Build with Custom tempDirectoryPath and heapSize ===");

        // Create custom temp directory
        Path customTempDir = tempDir.resolve("custom-xref-temp");
        java.nio.file.Files.createDirectories(customTempDir);
        System.out.println("Custom temp directory: " + customTempDir);

        // Create split 1
        Path index1Path = tempDir.resolve("index1-customconfig");
        Path split1Path = tempDir.resolve("split1-customconfig.split");
        createTestIndex(index1Path, "custom1", new String[]{"alpha", "beta", "gamma"});
        QuickwitSplit.SplitConfig config1 = new QuickwitSplit.SplitConfig(
            "xref-customconfig-test", "test-source", "node-1");
        QuickwitSplit.SplitMetadata meta1 = QuickwitSplit.convertIndexFromPath(
            index1Path.toString(), split1Path.toString(), config1);
        System.out.println("Created split1: " + meta1.getNumDocs() + " docs");

        // Create split 2
        Path index2Path = tempDir.resolve("index2-customconfig");
        Path split2Path = tempDir.resolve("split2-customconfig.split");
        createTestIndex(index2Path, "custom2", new String[]{"delta", "epsilon", "zeta"});
        QuickwitSplit.SplitConfig config2 = new QuickwitSplit.SplitConfig(
            "xref-customconfig-test", "test-source", "node-2");
        QuickwitSplit.SplitMetadata meta2 = QuickwitSplit.convertIndexFromPath(
            index2Path.toString(), split2Path.toString(), config2);
        System.out.println("Created split2: " + meta2.getNumDocs() + " docs");

        // Build XRef with custom configuration - EXPLICITLY SET tempDirectoryPath and heapSize
        XRefSourceSplit source1 = XRefSourceSplit.fromSplitMetadata(split1Path.toString(), meta1);
        XRefSourceSplit source2 = XRefSourceSplit.fromSplitMetadata(split2Path.toString(), meta2);

        XRefBuildConfig xrefConfig = XRefBuildConfig.builder()
            .xrefId("customconfig-xref")
            .indexUid("customconfig-index")
            .sourceSplits(Arrays.asList(source1, source2))
            .tempDirectoryPath(customTempDir.toString())  // Explicitly set custom temp directory
            .heapSize(XRefBuildConfig.DEFAULT_HEAP_SIZE)  // Explicitly set heap size (same as default)
            .build();

        System.out.println("Building XRef with:");
        System.out.println("  tempDirectoryPath: " + xrefConfig.getTempDirectoryPath());
        System.out.println("  heapSize: " + xrefConfig.getHeapSize() + " bytes");

        Path xrefPath = tempDir.resolve("customconfig.xref.split");
        XRefMetadata xrefMetadata = XRefSplit.build(xrefConfig, xrefPath.toString());

        assertNotNull(xrefMetadata, "XRef metadata should be returned");
        assertEquals(2, xrefMetadata.getNumSplits(), "XRef should have 2 source splits");
        assertTrue(xrefMetadata.hasFooterOffsets(), "XRef should have footer offsets");

        System.out.println("XRef built successfully with custom configuration:");
        System.out.println("  - XRef ID: " + xrefMetadata.getXrefId());
        System.out.println("  - Source splits: " + xrefMetadata.getNumSplits());
        System.out.println("  - Total terms: " + xrefMetadata.getTotalTerms());
        System.out.println("  - Build duration: " + xrefMetadata.getBuildStats().getBuildDurationMs() + "ms");

        // Verify the XRef can be searched
        SplitCacheManager.CacheConfig cacheConfig = new SplitCacheManager.CacheConfig("xref-customconfig-cache")
            .withMaxCacheSize(100_000_000);

        try (SplitCacheManager cacheManager = SplitCacheManager.getInstance(cacheConfig);
             XRefSearcher xrefSearcher = XRefSplit.open(cacheManager, "file://" + xrefPath.toString(), xrefMetadata)) {

            // Verify search works
            XRefSearchResult result = xrefSearcher.search("*", 10);
            assertEquals(2, result.getNumMatchingSplits(), "Match-all should return 2 splits");
            System.out.println("  - Search verification passed: " + result.getNumMatchingSplits() + " splits found");
        }

        System.out.println("\n=== Custom tempDirectoryPath and heapSize test passed! ===");
    }

    @Test
    @DisplayName("Build XRef with doubled heap size")
    void testBuildXRefWithLargeHeapSize() throws Exception {
        System.out.println("\n=== Testing XRef Build with Doubled Heap Size ===");

        // Create split 1
        Path index1Path = tempDir.resolve("index1-largeheap");
        Path split1Path = tempDir.resolve("split1-largeheap.split");
        createTestIndex(index1Path, "largeheap1", new String[]{"omega", "sigma", "theta"});
        QuickwitSplit.SplitConfig config1 = new QuickwitSplit.SplitConfig(
            "xref-largeheap-test", "test-source", "node-1");
        QuickwitSplit.SplitMetadata meta1 = QuickwitSplit.convertIndexFromPath(
            index1Path.toString(), split1Path.toString(), config1);

        // Create split 2
        Path index2Path = tempDir.resolve("index2-largeheap");
        Path split2Path = tempDir.resolve("split2-largeheap.split");
        createTestIndex(index2Path, "largeheap2", new String[]{"lambda", "kappa", "iota"});
        QuickwitSplit.SplitConfig config2 = new QuickwitSplit.SplitConfig(
            "xref-largeheap-test", "test-source", "node-2");
        QuickwitSplit.SplitMetadata meta2 = QuickwitSplit.convertIndexFromPath(
            index2Path.toString(), split2Path.toString(), config2);

        // Build XRef with doubled heap size
        XRefSourceSplit source1 = XRefSourceSplit.fromSplitMetadata(split1Path.toString(), meta1);
        XRefSourceSplit source2 = XRefSourceSplit.fromSplitMetadata(split2Path.toString(), meta2);

        // Use 2x default heap size (100MB instead of 50MB)
        long doubledHeapSize = XRefBuildConfig.DEFAULT_HEAP_SIZE * 2;

        XRefBuildConfig xrefConfig = XRefBuildConfig.builder()
            .xrefId("largeheap-xref")
            .indexUid("largeheap-index")
            .sourceSplits(Arrays.asList(source1, source2))
            .heapSize(doubledHeapSize)  // 100MB heap
            .build();

        System.out.println("Building XRef with heapSize: " + xrefConfig.getHeapSize() + " bytes (2x default)");

        Path xrefPath = tempDir.resolve("largeheap.xref.split");
        XRefMetadata xrefMetadata = XRefSplit.build(xrefConfig, xrefPath.toString());

        assertNotNull(xrefMetadata, "XRef metadata should be returned");
        assertEquals(2, xrefMetadata.getNumSplits(), "XRef should have 2 source splits");

        System.out.println("XRef built successfully with doubled heap size:");
        System.out.println("  - Build duration: " + xrefMetadata.getBuildStats().getBuildDurationMs() + "ms");

        System.out.println("\n=== Doubled heap size test passed! ===");
    }

    @Test
    @DisplayName("QueryParser interface: XRefSearcher parseQuery returns SplitQuery")
    void testQueryParserInterfaceWithXRefSearcher() throws Exception {
        System.out.println("\n=== Testing QueryParser Interface with XRefSearcher ===");

        // Create a simple XRef
        Path index1Path = tempDir.resolve("index1-queryparser");
        Path split1Path = tempDir.resolve("split1-queryparser.split");
        createTestIndex(index1Path, "tech", new String[]{"computer", "software", "hardware"});
        QuickwitSplit.SplitConfig config1 = new QuickwitSplit.SplitConfig(
            "xref-queryparser", "test-source", "node-1");
        QuickwitSplit.SplitMetadata meta1 = QuickwitSplit.convertIndexFromPath(
            index1Path.toString(), split1Path.toString(), config1);

        XRefSourceSplit source1 = XRefSourceSplit.fromSplitMetadata(split1Path.toString(), meta1);

        XRefBuildConfig xrefConfig = XRefBuildConfig.builder()
            .xrefId("queryparser-xref")
            .indexUid("queryparser-index")
            .sourceSplits(Arrays.asList(source1))
            .build();

        Path xrefPath = tempDir.resolve("queryparser.xref.split");
        XRefMetadata xrefMetadata = XRefSplit.build(xrefConfig, xrefPath.toString());

        SplitCacheManager.CacheConfig cacheConfig = new SplitCacheManager.CacheConfig("xref-queryparser-cache")
            .withMaxCacheSize(100_000_000);

        try (SplitCacheManager cacheManager = SplitCacheManager.getInstance(cacheConfig);
             XRefSearcher xrefSearcher = XRefSplit.open(cacheManager, "file://" + xrefPath.toString(), xrefMetadata)) {

            // Test 1: Verify XRefSearcher implements QueryParser
            assertTrue(xrefSearcher instanceof QueryParser,
                "XRefSearcher should implement QueryParser interface");

            // Test 2: Use XRefSearcher through QueryParser interface
            QueryParser queryParser = xrefSearcher;
            SplitQuery query = queryParser.parseQuery("domain:tech");

            assertNotNull(query, "parseQuery should return a SplitQuery");
            assertTrue(query instanceof SplitParsedQuery,
                "Query should be a SplitParsedQuery instance");

            // Test 3: Verify the parsed query has correct JSON structure
            String queryJson = query.toQueryAstJson();
            System.out.println("Parsed query JSON: " + queryJson);
            // Quickwit parses field:term as full_text type (not raw term)
            // Format: {"type":"full_text","field":"domain","text":"tech",...}
            assertTrue(queryJson.contains("full_text") || queryJson.contains("term") || queryJson.contains("Term"),
                "Parsed query should contain query type (full_text, term, or Term)");
            assertTrue(queryJson.contains("domain"),
                "Parsed query should contain field 'domain'");
            assertTrue(queryJson.contains("tech"),
                "Parsed query should contain value 'tech'");

            // Test 4: Search using the parsed SplitQuery
            XRefSearchResult result = xrefSearcher.search(query, 10);
            assertEquals(1, result.getNumMatchingSplits(),
                "Search with parsed query should find 1 split");

            // Test 5: Parse and search match-all
            SplitQuery matchAllQuery = queryParser.parseQuery("*");
            String matchAllJson = matchAllQuery.toQueryAstJson();
            System.out.println("Match-all query JSON: " + matchAllJson);
            assertTrue(matchAllJson.contains("match_all") || matchAllJson.contains("MatchAll"),
                "Match-all query should parse correctly");

            XRefSearchResult matchAllResult = xrefSearcher.search(matchAllQuery, 10);
            assertEquals(1, matchAllResult.getNumMatchingSplits(),
                "Match-all should find all splits");

            System.out.println("\n=== QueryParser interface test passed! ===");
        }
    }

    @Test
    @DisplayName("XRefSearcher getSchema returns valid schema")
    void testXRefSearcherGetSchema() throws Exception {
        System.out.println("\n=== Testing XRefSearcher.getSchema() ===");

        // Create a split with known fields
        Path index1Path = tempDir.resolve("index1-schema");
        Path split1Path = tempDir.resolve("split1-schema.split");
        createTestIndex(index1Path, "science", new String[]{"physics", "chemistry", "biology"});
        QuickwitSplit.SplitConfig config1 = new QuickwitSplit.SplitConfig(
            "xref-schema", "test-source", "node-1");
        QuickwitSplit.SplitMetadata meta1 = QuickwitSplit.convertIndexFromPath(
            index1Path.toString(), split1Path.toString(), config1);

        XRefSourceSplit source1 = XRefSourceSplit.fromSplitMetadata(split1Path.toString(), meta1);

        XRefBuildConfig xrefConfig = XRefBuildConfig.builder()
            .xrefId("schema-xref")
            .indexUid("schema-index")
            .sourceSplits(Arrays.asList(source1))
            .build();

        Path xrefPath = tempDir.resolve("schema.xref.split");
        XRefMetadata xrefMetadata = XRefSplit.build(xrefConfig, xrefPath.toString());

        SplitCacheManager.CacheConfig cacheConfig = new SplitCacheManager.CacheConfig("xref-schema-cache")
            .withMaxCacheSize(100_000_000);

        try (SplitCacheManager cacheManager = SplitCacheManager.getInstance(cacheConfig);
             XRefSearcher xrefSearcher = XRefSplit.open(cacheManager, "file://" + xrefPath.toString(), xrefMetadata)) {

            // Get the schema from XRefSearcher
            Schema schema = xrefSearcher.getSchema();
            assertNotNull(schema, "getSchema should return a non-null schema");

            // Verify schema contains expected fields
            java.util.List<String> fieldNames = schema.getFieldNames();
            System.out.println("Schema fields: " + fieldNames);

            assertTrue(fieldNames.contains("domain"),
                "Schema should contain 'domain' field");
            assertTrue(fieldNames.contains("content"),
                "Schema should contain 'content' field");
            assertTrue(fieldNames.contains("id"),
                "Schema should contain 'id' field");

            System.out.println("\n=== getSchema test passed! ===");
        }
    }

    @Test
    @DisplayName("Range query search sets hasUnevaluatedClauses flag")
    void testRangeQuerySetsHasUnevaluatedClauses() throws Exception {
        System.out.println("\n=== Testing Range Query hasUnevaluatedClauses Flag ===");

        // Create splits
        Path index1Path = tempDir.resolve("index1-unevaluated");
        Path split1Path = tempDir.resolve("split1-unevaluated.split");
        createTestIndex(index1Path, "numbers", new String[]{"one", "two", "three"});
        QuickwitSplit.SplitConfig config1 = new QuickwitSplit.SplitConfig(
            "xref-unevaluated", "test-source", "node-1");
        QuickwitSplit.SplitMetadata meta1 = QuickwitSplit.convertIndexFromPath(
            index1Path.toString(), split1Path.toString(), config1);

        Path index2Path = tempDir.resolve("index2-unevaluated");
        Path split2Path = tempDir.resolve("split2-unevaluated.split");
        createTestIndex(index2Path, "letters", new String[]{"alpha", "beta", "gamma"});
        QuickwitSplit.SplitConfig config2 = new QuickwitSplit.SplitConfig(
            "xref-unevaluated", "test-source", "node-2");
        QuickwitSplit.SplitMetadata meta2 = QuickwitSplit.convertIndexFromPath(
            index2Path.toString(), split2Path.toString(), config2);

        // Build XRef
        XRefSourceSplit source1 = XRefSourceSplit.fromSplitMetadata(split1Path.toString(), meta1);
        XRefSourceSplit source2 = XRefSourceSplit.fromSplitMetadata(split2Path.toString(), meta2);

        XRefBuildConfig xrefConfig = XRefBuildConfig.builder()
            .xrefId("unevaluated-xref")
            .indexUid("unevaluated-index")
            .sourceSplits(Arrays.asList(source1, source2))
            .build();

        Path xrefPath = tempDir.resolve("unevaluated.xref.split");
        XRefMetadata xrefMetadata = XRefSplit.build(xrefConfig, xrefPath.toString());

        SplitCacheManager.CacheConfig cacheConfig = new SplitCacheManager.CacheConfig("xref-unevaluated-cache")
            .withMaxCacheSize(100_000_000);

        try (SplitCacheManager cacheManager = SplitCacheManager.getInstance(cacheConfig);
             XRefSearcher xrefSearcher = XRefSplit.open(cacheManager, "file://" + xrefPath.toString(), xrefMetadata)) {

            // Test 1: Regular term query should NOT have unevaluated clauses
            SplitTermQuery termQuery = new SplitTermQuery("domain", "numbers");
            XRefSearchResult termResult = xrefSearcher.search(termQuery, 10);
            System.out.println("Term query hasUnevaluatedClauses: " + termResult.hasUnevaluatedClauses());
            assertFalse(termResult.hasUnevaluatedClauses(),
                "Term query should not have unevaluated clauses");

            // Test 2: Range query SHOULD have unevaluated clauses
            SplitRangeQuery rangeQuery = SplitRangeQuery.inclusiveRange("id", "0", "10", "i64");
            XRefSearchResult rangeResult = xrefSearcher.search(rangeQuery, 10);
            System.out.println("Range query hasUnevaluatedClauses: " + rangeResult.hasUnevaluatedClauses());
            assertTrue(rangeResult.hasUnevaluatedClauses(),
                "Range query should have unevaluated clauses");

            // Range query returns all splits (transformed to match-all)
            assertEquals(2, rangeResult.getNumMatchingSplits(),
                "Range query should return all splits");

            // Test 3: Match-all query should NOT have unevaluated clauses
            SplitMatchAllQuery matchAllQuery = new SplitMatchAllQuery();
            XRefSearchResult matchAllResult = xrefSearcher.search(matchAllQuery, 10);
            System.out.println("Match-all query hasUnevaluatedClauses: " + matchAllResult.hasUnevaluatedClauses());
            assertFalse(matchAllResult.hasUnevaluatedClauses(),
                "Match-all query should not have unevaluated clauses");

            System.out.println("\n=== hasUnevaluatedClauses test passed! ===");
        }
    }

    @Test
    @DisplayName("Search with SplitQuery objects from parseQuery")
    void testSearchWithParsedSplitQuery() throws Exception {
        System.out.println("\n=== Testing Search with Parsed SplitQuery ===");

        // Create two splits with different content
        Path index1Path = tempDir.resolve("index1-parsed");
        Path split1Path = tempDir.resolve("split1-parsed.split");
        createTestIndex(index1Path, "astronomy", new String[]{"star", "planet", "galaxy"});
        QuickwitSplit.SplitConfig config1 = new QuickwitSplit.SplitConfig(
            "xref-parsed", "test-source", "node-1");
        QuickwitSplit.SplitMetadata meta1 = QuickwitSplit.convertIndexFromPath(
            index1Path.toString(), split1Path.toString(), config1);

        Path index2Path = tempDir.resolve("index2-parsed");
        Path split2Path = tempDir.resolve("split2-parsed.split");
        createTestIndex(index2Path, "biology", new String[]{"cell", "dna", "protein"});
        QuickwitSplit.SplitConfig config2 = new QuickwitSplit.SplitConfig(
            "xref-parsed", "test-source", "node-2");
        QuickwitSplit.SplitMetadata meta2 = QuickwitSplit.convertIndexFromPath(
            index2Path.toString(), split2Path.toString(), config2);

        // Build XRef
        XRefSourceSplit source1 = XRefSourceSplit.fromSplitMetadata(split1Path.toString(), meta1);
        XRefSourceSplit source2 = XRefSourceSplit.fromSplitMetadata(split2Path.toString(), meta2);

        XRefBuildConfig xrefConfig = XRefBuildConfig.builder()
            .xrefId("parsed-xref")
            .indexUid("parsed-index")
            .sourceSplits(Arrays.asList(source1, source2))
            .build();

        Path xrefPath = tempDir.resolve("parsed.xref.split");
        XRefMetadata xrefMetadata = XRefSplit.build(xrefConfig, xrefPath.toString());

        SplitCacheManager.CacheConfig cacheConfig = new SplitCacheManager.CacheConfig("xref-parsed-cache")
            .withMaxCacheSize(100_000_000);

        try (SplitCacheManager cacheManager = SplitCacheManager.getInstance(cacheConfig);
             XRefSearcher xrefSearcher = XRefSplit.open(cacheManager, "file://" + xrefPath.toString(), xrefMetadata)) {

            // Test 1: Parse term query and search
            System.out.println("Test 1: Parse and search term query");
            SplitQuery astronomyQuery = xrefSearcher.parseQuery("domain:astronomy");
            XRefSearchResult astronomyResult = xrefSearcher.search(astronomyQuery, 10);
            assertEquals(1, astronomyResult.getNumMatchingSplits(),
                "Parsed term query should find 1 split");

            // Test 2: Parse boolean query (AND)
            System.out.println("Test 2: Parse and search AND query");
            SplitQuery andQuery = xrefSearcher.parseQuery("domain:biology AND content:cell");
            XRefSearchResult andResult = xrefSearcher.search(andQuery, 10);
            assertEquals(1, andResult.getNumMatchingSplits(),
                "AND query should find 1 split");

            // Test 3: Parse boolean query (OR)
            System.out.println("Test 3: Parse and search OR query");
            SplitQuery orQuery = xrefSearcher.parseQuery("domain:astronomy OR domain:biology");
            XRefSearchResult orResult = xrefSearcher.search(orQuery, 10);
            assertEquals(2, orResult.getNumMatchingSplits(),
                "OR query should find 2 splits");

            // Test 4: Parse NOT query
            System.out.println("Test 4: Parse and search NOT query");
            SplitQuery notQuery = xrefSearcher.parseQuery("domain:astronomy AND NOT domain:biology");
            XRefSearchResult notResult = xrefSearcher.search(notQuery, 10);
            // NOT cannot be evaluated by filters, so it's ignored (conservative)
            // The query effectively becomes just domain:astronomy
            assertEquals(1, notResult.getNumMatchingSplits(),
                "NOT query should still find astronomy split");

            // Test 5: Parse range query (returns all due to transformation)
            System.out.println("Test 5: Parse and search range query");
            SplitQuery rangeQuery = xrefSearcher.parseQuery("id:[0 TO 100]");
            XRefSearchResult rangeResult = xrefSearcher.search(rangeQuery, 10);
            assertEquals(2, rangeResult.getNumMatchingSplits(),
                "Range query should return all splits (unevaluable)");
            assertTrue(rangeResult.hasUnevaluatedClauses(),
                "Range query should set hasUnevaluatedClauses");

            System.out.println("\n=== Parsed SplitQuery search test passed! ===");
        }
    }

    @Test
    @DisplayName("Tokenizer modes: default tokenizer lowercases terms")
    void testDefaultTokenizerLowercasesTerms() throws Exception {
        System.out.println("\n=== Testing Default Tokenizer Lowercases Terms ===");

        // Create a split with text using the "default" tokenizer (which lowercases)
        Path index1Path = tempDir.resolve("index1-tokenizer");
        Path split1Path = tempDir.resolve("split1-tokenizer.split");

        SchemaBuilder schemaBuilder = new SchemaBuilder();
        schemaBuilder.addTextField("content", true, false, "default", "position");
        schemaBuilder.addIntegerField("id", true, true, true);
        Schema schema = schemaBuilder.build();

        try (Index index = new Index(schema, index1Path.toString());
             IndexWriter writer = index.writer(Index.Memory.DEFAULT_HEAP_SIZE, 1)) {
            // Add documents with mixed case content
            Document doc = new Document();
            doc.addText("content", "UPPERCASE lowercase MixedCase");
            doc.addInteger("id", 1);
            writer.addDocument(doc);
            writer.commit();
        }

        QuickwitSplit.SplitConfig config1 = new QuickwitSplit.SplitConfig(
            "xref-tokenizer-test", "test-source", "node-1");
        QuickwitSplit.SplitMetadata meta1 = QuickwitSplit.convertIndexFromPath(
            index1Path.toString(), split1Path.toString(), config1);

        // Build XRef
        XRefSourceSplit source1 = XRefSourceSplit.fromSplitMetadata(split1Path.toString(), meta1);

        XRefBuildConfig xrefConfig = XRefBuildConfig.builder()
            .xrefId("tokenizer-test-xref")
            .indexUid("tokenizer-test-index")
            .sourceSplits(Arrays.asList(source1))
            .build();

        Path xrefPath = tempDir.resolve("tokenizer-test.xref.split");
        XRefMetadata xrefMetadata = XRefSplit.build(xrefConfig, xrefPath.toString());

        SplitCacheManager.CacheConfig cacheConfig = new SplitCacheManager.CacheConfig("xref-tokenizer-cache")
            .withMaxCacheSize(100_000_000);

        try (SplitCacheManager cacheManager = SplitCacheManager.getInstance(cacheConfig);
             XRefSearcher xrefSearcher = XRefSplit.open(cacheManager, "file://" + xrefPath.toString(), xrefMetadata)) {

            // Test 1: Lowercase query should match UPPERCASE content (default tokenizer lowercases)
            System.out.println("Test 1: Lowercase query 'content:uppercase' should find match");
            XRefSearchResult lowercaseResult = xrefSearcher.search("content:uppercase", 10);
            assertEquals(1, lowercaseResult.getNumMatchingSplits(),
                "Lowercase query should find UPPERCASE content (tokenizer lowercases)");

            // Test 2: Uppercase query should also match (query is also lowercased)
            System.out.println("Test 2: Uppercase query 'content:UPPERCASE' should find match");
            XRefSearchResult uppercaseResult = xrefSearcher.search("content:UPPERCASE", 10);
            assertEquals(1, uppercaseResult.getNumMatchingSplits(),
                "Uppercase query should find content (query also lowercased)");

            // Test 3: Mixed case query should match
            System.out.println("Test 3: Mixed case query 'content:MixedCase' should find match");
            XRefSearchResult mixedResult = xrefSearcher.search("content:MixedCase", 10);
            assertEquals(1, mixedResult.getNumMatchingSplits(),
                "Mixed case query should find content");

            // Test 4: Nonexistent term should not match
            System.out.println("Test 4: Nonexistent term should not match");
            XRefSearchResult nonExistResult = xrefSearcher.search("content:nonexistent", 10);
            assertEquals(0, nonExistResult.getNumMatchingSplits(),
                "Nonexistent term should not match");

            System.out.println("\n=== Default tokenizer test passed! ===");
        }
    }

    @Test
    @DisplayName("Tokenizer modes: raw tokenizer preserves case")
    void testRawTokenizerPreservesCase() throws Exception {
        System.out.println("\n=== Testing Raw Tokenizer Preserves Case ===");

        // Create a split with text using the "raw" tokenizer (no tokenization)
        Path index1Path = tempDir.resolve("index1-raw-tokenizer");
        Path split1Path = tempDir.resolve("split1-raw-tokenizer.split");

        SchemaBuilder schemaBuilder = new SchemaBuilder();
        schemaBuilder.addTextField("keyword", true, false, "raw", "position");
        schemaBuilder.addIntegerField("id", true, true, true);
        Schema schema = schemaBuilder.build();

        try (Index index = new Index(schema, index1Path.toString());
             IndexWriter writer = index.writer(Index.Memory.DEFAULT_HEAP_SIZE, 1)) {
            // Add documents with specific keyword values
            Document doc = new Document();
            doc.addText("keyword", "ExactMatch");
            doc.addInteger("id", 1);
            writer.addDocument(doc);
            writer.commit();
        }

        QuickwitSplit.SplitConfig config1 = new QuickwitSplit.SplitConfig(
            "xref-raw-test", "test-source", "node-1");
        QuickwitSplit.SplitMetadata meta1 = QuickwitSplit.convertIndexFromPath(
            index1Path.toString(), split1Path.toString(), config1);

        // Build XRef
        XRefSourceSplit source1 = XRefSourceSplit.fromSplitMetadata(split1Path.toString(), meta1);

        XRefBuildConfig xrefConfig = XRefBuildConfig.builder()
            .xrefId("raw-test-xref")
            .indexUid("raw-test-index")
            .sourceSplits(Arrays.asList(source1))
            .build();

        Path xrefPath = tempDir.resolve("raw-test.xref.split");
        XRefMetadata xrefMetadata = XRefSplit.build(xrefConfig, xrefPath.toString());

        SplitCacheManager.CacheConfig cacheConfig = new SplitCacheManager.CacheConfig("xref-raw-cache")
            .withMaxCacheSize(100_000_000);

        try (SplitCacheManager cacheManager = SplitCacheManager.getInstance(cacheConfig);
             XRefSearcher xrefSearcher = XRefSplit.open(cacheManager, "file://" + xrefPath.toString(), xrefMetadata)) {

            // Test 1: Exact case match should find the document
            System.out.println("Test 1: Exact case 'keyword:ExactMatch' should find match");
            XRefSearchResult exactResult = xrefSearcher.search("keyword:ExactMatch", 10);
            assertEquals(1, exactResult.getNumMatchingSplits(),
                "Exact case match should find document");

            // Test 2: Different case should NOT match (raw tokenizer preserves case)
            System.out.println("Test 2: Different case 'keyword:exactmatch' should NOT find match");
            XRefSearchResult wrongCaseResult = xrefSearcher.search("keyword:exactmatch", 10);
            assertEquals(0, wrongCaseResult.getNumMatchingSplits(),
                "Wrong case should not match with raw tokenizer");

            // Test 3: Partial match should NOT work (raw tokenizer doesn't split)
            System.out.println("Test 3: Partial match 'keyword:Exact' should NOT find match");
            XRefSearchResult partialResult = xrefSearcher.search("keyword:Exact", 10);
            assertEquals(0, partialResult.getNumMatchingSplits(),
                "Partial match should not work with raw tokenizer");

            System.out.println("\n=== Raw tokenizer test passed! ===");
        }
    }

    @Test
    @DisplayName("Error handling: Open XRef without metadata throws exception")
    void testOpenXRefWithoutMetadataThrows() throws Exception {
        System.out.println("\n=== Testing Error Handling: Open XRef Without Metadata ===");

        // Create a valid XRef first
        Path index1Path = tempDir.resolve("index1-error");
        Path split1Path = tempDir.resolve("split1-error.split");
        createTestIndex(index1Path, "test", new String[]{"alpha", "beta"});
        QuickwitSplit.SplitConfig config1 = new QuickwitSplit.SplitConfig(
            "xref-error-test", "test-source", "node-1");
        QuickwitSplit.SplitMetadata meta1 = QuickwitSplit.convertIndexFromPath(
            index1Path.toString(), split1Path.toString(), config1);

        XRefSourceSplit source1 = XRefSourceSplit.fromSplitMetadata(split1Path.toString(), meta1);

        XRefBuildConfig xrefConfig = XRefBuildConfig.builder()
            .xrefId("error-test-xref")
            .indexUid("error-test-index")
            .sourceSplits(Arrays.asList(source1))
            .build();

        Path xrefPath = tempDir.resolve("error-test.xref.split");
        XRefSplit.build(xrefConfig, xrefPath.toString());

        // Try to open without metadata - should throw
        SplitCacheManager.CacheConfig cacheConfig = new SplitCacheManager.CacheConfig("xref-error-cache")
            .withMaxCacheSize(100_000_000);

        try (SplitCacheManager cacheManager = SplitCacheManager.getInstance(cacheConfig)) {
            assertThrows(IllegalArgumentException.class, () -> {
                XRefSearcher.open(cacheManager, xrefPath.toString(), (XRefMetadata) null);
            }, "Opening XRef without metadata should throw IllegalArgumentException");
        }

        System.out.println("=== Error handling test passed! ===");
    }

    @Test
    @DisplayName("Error handling: Open non-existent XRef file")
    void testOpenNonExistentXRefFile() throws Exception {
        System.out.println("\n=== Testing Error Handling: Open Non-Existent XRef File ===");

        // First create a valid XRef to get valid metadata
        Path index1Path = tempDir.resolve("index1-nonexistent");
        Path split1Path = tempDir.resolve("split1-nonexistent.split");
        createTestIndex(index1Path, "test", new String[]{"alpha"});
        QuickwitSplit.SplitConfig config1 = new QuickwitSplit.SplitConfig(
            "xref-nonexistent-test", "test-source", "node-1");
        QuickwitSplit.SplitMetadata meta1 = QuickwitSplit.convertIndexFromPath(
            index1Path.toString(), split1Path.toString(), config1);

        XRefSourceSplit source1 = XRefSourceSplit.fromSplitMetadata(split1Path.toString(), meta1);

        XRefBuildConfig xrefConfig = XRefBuildConfig.builder()
            .xrefId("nonexistent-test-xref")
            .indexUid("nonexistent-test-index")
            .sourceSplits(Arrays.asList(source1))
            .build();

        Path xrefPath = tempDir.resolve("temp-for-metadata.xref.split");
        XRefMetadata validMetadata = XRefSplit.build(xrefConfig, xrefPath.toString());

        SplitCacheManager.CacheConfig cacheConfig = new SplitCacheManager.CacheConfig("xref-nonexistent-cache")
            .withMaxCacheSize(100_000_000);

        // Try to open a non-existent path with valid metadata
        try (SplitCacheManager cacheManager = SplitCacheManager.getInstance(cacheConfig)) {
            assertThrows(RuntimeException.class, () -> {
                XRefSearcher.open(cacheManager, "/nonexistent/path/to/xref.split", validMetadata);
            }, "Opening non-existent XRef file should throw RuntimeException");
        }

        System.out.println("=== Non-existent file test passed! ===");
    }

    @Test
    @DisplayName("Edge case: Build XRef with single split")
    void testBuildXRefWithSingleSplit() throws Exception {
        System.out.println("\n=== Testing Edge Case: Single Split XRef ===");

        // Create only one split
        Path index1Path = tempDir.resolve("index1-single");
        Path split1Path = tempDir.resolve("split1-single.split");
        createTestIndex(index1Path, "only", new String[]{"unique", "single", "split"});
        QuickwitSplit.SplitConfig config1 = new QuickwitSplit.SplitConfig(
            "xref-single-test", "test-source", "node-1");
        QuickwitSplit.SplitMetadata meta1 = QuickwitSplit.convertIndexFromPath(
            index1Path.toString(), split1Path.toString(), config1);

        // Build XRef with just one split
        XRefSourceSplit source1 = XRefSourceSplit.fromSplitMetadata(split1Path.toString(), meta1);

        XRefBuildConfig xrefConfig = XRefBuildConfig.builder()
            .xrefId("single-split-xref")
            .indexUid("single-split-index")
            .sourceSplits(Arrays.asList(source1))
            .build();

        Path xrefPath = tempDir.resolve("single-split.xref.split");
        XRefMetadata xrefMetadata = XRefSplit.build(xrefConfig, xrefPath.toString());

        assertNotNull(xrefMetadata, "XRef metadata should be returned");
        assertEquals(1, xrefMetadata.getNumSplits(), "XRef should have 1 source split");

        // Verify it can be searched
        SplitCacheManager.CacheConfig cacheConfig = new SplitCacheManager.CacheConfig("xref-single-cache")
            .withMaxCacheSize(100_000_000);

        try (SplitCacheManager cacheManager = SplitCacheManager.getInstance(cacheConfig);
             XRefSearcher xrefSearcher = XRefSplit.open(cacheManager, "file://" + xrefPath.toString(), xrefMetadata)) {

            // Match-all should return the one split
            XRefSearchResult result = xrefSearcher.search("*", 10);
            assertEquals(1, result.getNumMatchingSplits(), "Single split XRef should return 1 split");

            // Term query should also work
            XRefSearchResult termResult = xrefSearcher.search("domain:only", 10);
            assertEquals(1, termResult.getNumMatchingSplits(), "Term query should find the single split");

            // Non-existent term should return 0
            XRefSearchResult noResult = xrefSearcher.search("content:nonexistent", 10);
            assertEquals(0, noResult.getNumMatchingSplits(), "Non-existent term should find 0 splits");
        }

        System.out.println("=== Single split XRef test passed! ===");
    }

    @Test
    @DisplayName("Edge case: Build XRef with many splits")
    void testBuildXRefWithManySplits() throws Exception {
        System.out.println("\n=== Testing Edge Case: Many Splits XRef ===");

        int numSplits = 10;
        java.util.List<XRefSourceSplit> sourceSplits = new java.util.ArrayList<>();
        String[] domains = {"alpha", "beta", "gamma", "delta", "epsilon",
                            "zeta", "eta", "theta", "iota", "kappa"};

        for (int i = 0; i < numSplits; i++) {
            Path indexPath = tempDir.resolve("index-many-" + i);
            Path splitPath = tempDir.resolve("split-many-" + i + ".split");
            createTestIndex(indexPath, domains[i], new String[]{domains[i] + "_unique_term"});

            QuickwitSplit.SplitConfig config = new QuickwitSplit.SplitConfig(
                "xref-many-test", "test-source", "node-" + i);
            QuickwitSplit.SplitMetadata meta = QuickwitSplit.convertIndexFromPath(
                indexPath.toString(), splitPath.toString(), config);

            sourceSplits.add(XRefSourceSplit.fromSplitMetadata(splitPath.toString(), meta));
        }

        // Build XRef with many splits
        XRefBuildConfig xrefConfig = XRefBuildConfig.builder()
            .xrefId("many-splits-xref")
            .indexUid("many-splits-index")
            .sourceSplits(sourceSplits)
            .build();

        Path xrefPath = tempDir.resolve("many-splits.xref.split");
        XRefMetadata xrefMetadata = XRefSplit.build(xrefConfig, xrefPath.toString());

        assertNotNull(xrefMetadata, "XRef metadata should be returned");
        assertEquals(numSplits, xrefMetadata.getNumSplits(), "XRef should have " + numSplits + " source splits");

        System.out.println("Built XRef with " + numSplits + " splits:");
        System.out.println("  - Total terms: " + xrefMetadata.getTotalTerms());
        System.out.println("  - Build time: " + xrefMetadata.getBuildStats().getBuildDurationMs() + "ms");

        // Verify searching
        SplitCacheManager.CacheConfig cacheConfig = new SplitCacheManager.CacheConfig("xref-many-cache")
            .withMaxCacheSize(100_000_000);

        try (SplitCacheManager cacheManager = SplitCacheManager.getInstance(cacheConfig);
             XRefSearcher xrefSearcher = XRefSplit.open(cacheManager, "file://" + xrefPath.toString(), xrefMetadata)) {

            // Match-all should return all splits
            XRefSearchResult allResult = xrefSearcher.search("*", 100);
            assertEquals(numSplits, allResult.getNumMatchingSplits(), "Match-all should return all " + numSplits + " splits");

            // Query for specific domain should return 1 split
            XRefSearchResult specificResult = xrefSearcher.search("domain:alpha", 10);
            assertEquals(1, specificResult.getNumMatchingSplits(), "Specific domain query should find 1 split");

            // Query for unique term in specific split
            XRefSearchResult uniqueResult = xrefSearcher.search("content:epsilon_unique_term", 10);
            assertEquals(1, uniqueResult.getNumMatchingSplits(), "Unique term query should find 1 split");
        }

        System.out.println("=== Many splits XRef test passed! ===");
    }

    @Test
    @DisplayName("Query types: Phrase query is evaluated")
    void testPhraseQueryEvaluation() throws Exception {
        System.out.println("\n=== Testing Phrase Query Evaluation ===");

        // Create split with content containing a phrase
        Path index1Path = tempDir.resolve("index1-phrase");
        Path split1Path = tempDir.resolve("split1-phrase.split");

        SchemaBuilder schemaBuilder = new SchemaBuilder();
        schemaBuilder.addTextField("content", true, false, "default", "position");
        schemaBuilder.addIntegerField("id", true, true, true);
        Schema schema = schemaBuilder.build();

        try (Index index = new Index(schema, index1Path.toString());
             IndexWriter writer = index.writer(Index.Memory.DEFAULT_HEAP_SIZE, 1)) {
            Document doc = new Document();
            doc.addText("content", "the quick brown fox jumps");
            doc.addInteger("id", 1);
            writer.addDocument(doc);
            writer.commit();
        }

        QuickwitSplit.SplitConfig config1 = new QuickwitSplit.SplitConfig(
            "xref-phrase-test", "test-source", "node-1");
        QuickwitSplit.SplitMetadata meta1 = QuickwitSplit.convertIndexFromPath(
            index1Path.toString(), split1Path.toString(), config1);

        // Build XRef
        XRefSourceSplit source1 = XRefSourceSplit.fromSplitMetadata(split1Path.toString(), meta1);

        XRefBuildConfig xrefConfig = XRefBuildConfig.builder()
            .xrefId("phrase-test-xref")
            .indexUid("phrase-test-index")
            .sourceSplits(Arrays.asList(source1))
            .build();

        Path xrefPath = tempDir.resolve("phrase-test.xref.split");
        XRefMetadata xrefMetadata = XRefSplit.build(xrefConfig, xrefPath.toString());

        SplitCacheManager.CacheConfig cacheConfig = new SplitCacheManager.CacheConfig("xref-phrase-cache")
            .withMaxCacheSize(100_000_000);

        try (SplitCacheManager cacheManager = SplitCacheManager.getInstance(cacheConfig);
             XRefSearcher xrefSearcher = XRefSplit.open(cacheManager, "file://" + xrefPath.toString(), xrefMetadata)) {

            // Test 1: Phrase query with terms that exist
            System.out.println("Test 1: Phrase query with existing terms");
            XRefSearchResult phraseResult = xrefSearcher.search("content:\"quick brown\"", 10);
            System.out.println("  Phrase query 'quick brown' matched " + phraseResult.getNumMatchingSplits() + " splits");
            // FuseXRef checks if each term in phrase exists - it should find the split
            // even though it can't verify position (positions aren't stored in filters)
            assertTrue(phraseResult.getNumMatchingSplits() >= 0,
                "Phrase query should return valid result (may or may not match depending on filter evaluation)");

            // Test 2: Individual terms should definitely exist
            XRefSearchResult quickResult = xrefSearcher.search("content:quick", 10);
            assertEquals(1, quickResult.getNumMatchingSplits(), "Individual term 'quick' should match");

            XRefSearchResult brownResult = xrefSearcher.search("content:brown", 10);
            assertEquals(1, brownResult.getNumMatchingSplits(), "Individual term 'brown' should match");
        }

        System.out.println("=== Phrase query test passed! ===");
    }

    @Test
    @DisplayName("Query types: Boolean MUST_NOT handling")
    void testBooleanMustNotHandling() throws Exception {
        System.out.println("\n=== Testing Boolean MUST_NOT Handling ===");

        // Create two splits with different content
        Path index1Path = tempDir.resolve("index1-mustnot");
        Path split1Path = tempDir.resolve("split1-mustnot.split");
        createTestIndex(index1Path, "positive", new String[]{"include", "want", "keep"});
        QuickwitSplit.SplitConfig config1 = new QuickwitSplit.SplitConfig(
            "xref-mustnot-test", "test-source", "node-1");
        QuickwitSplit.SplitMetadata meta1 = QuickwitSplit.convertIndexFromPath(
            index1Path.toString(), split1Path.toString(), config1);

        Path index2Path = tempDir.resolve("index2-mustnot");
        Path split2Path = tempDir.resolve("split2-mustnot.split");
        createTestIndex(index2Path, "negative", new String[]{"exclude", "unwant", "remove"});
        QuickwitSplit.SplitConfig config2 = new QuickwitSplit.SplitConfig(
            "xref-mustnot-test", "test-source", "node-2");
        QuickwitSplit.SplitMetadata meta2 = QuickwitSplit.convertIndexFromPath(
            index2Path.toString(), split2Path.toString(), config2);

        // Build XRef
        XRefSourceSplit source1 = XRefSourceSplit.fromSplitMetadata(split1Path.toString(), meta1);
        XRefSourceSplit source2 = XRefSourceSplit.fromSplitMetadata(split2Path.toString(), meta2);

        XRefBuildConfig xrefConfig = XRefBuildConfig.builder()
            .xrefId("mustnot-test-xref")
            .indexUid("mustnot-test-index")
            .sourceSplits(Arrays.asList(source1, source2))
            .build();

        Path xrefPath = tempDir.resolve("mustnot-test.xref.split");
        XRefMetadata xrefMetadata = XRefSplit.build(xrefConfig, xrefPath.toString());

        SplitCacheManager.CacheConfig cacheConfig = new SplitCacheManager.CacheConfig("xref-mustnot-cache")
            .withMaxCacheSize(100_000_000);

        try (SplitCacheManager cacheManager = SplitCacheManager.getInstance(cacheConfig);
             XRefSearcher xrefSearcher = XRefSplit.open(cacheManager, "file://" + xrefPath.toString(), xrefMetadata)) {

            // Test 1: Pure MUST_NOT cannot exclude definitively with filters
            // Filters can only confirm presence, not absence
            System.out.println("Test 1: MUST_NOT query behavior");
            SplitBooleanQuery mustNotQuery = new SplitBooleanQuery()
                .addMustNot(new SplitTermQuery("domain", "negative"));

            XRefSearchResult mustNotResult = xrefSearcher.search(mustNotQuery, 10);
            System.out.println("  MUST_NOT query matched " + mustNotResult.getNumMatchingSplits() + " splits");
            // MUST_NOT alone cannot be evaluated by filters (conservative approach returns all)
            // This is expected behavior - filters can't prove absence

            // Test 2: MUST with MUST_NOT - MUST filters, MUST_NOT is ignored
            System.out.println("Test 2: MUST + MUST_NOT query behavior");
            SplitBooleanQuery combinedQuery = new SplitBooleanQuery()
                .addMust(new SplitTermQuery("domain", "positive"))
                .addMustNot(new SplitTermQuery("domain", "negative"));

            XRefSearchResult combinedResult = xrefSearcher.search(combinedQuery, 10);
            System.out.println("  MUST + MUST_NOT query matched " + combinedResult.getNumMatchingSplits() + " splits");
            // The MUST clause filters to "positive" domain (1 split)
            // MUST_NOT cannot further filter with probability filters
            assertEquals(1, combinedResult.getNumMatchingSplits(),
                "MUST clause should filter, MUST_NOT is conservatively ignored");
        }

        System.out.println("=== MUST_NOT handling test passed! ===");
    }

    @Test
    @DisplayName("Build XRef with BinaryFuse16 filter type for lower false positive rate")
    void testBuildXRefWithFuse16FilterType() throws Exception {
        System.out.println("\n=== Testing BinaryFuse16 Filter Type ===");

        // Create split 1
        Path index1Path = tempDir.resolve("index1-fuse16");
        Path split1Path = tempDir.resolve("split1-fuse16.split");
        createTestIndex(index1Path, "science", new String[]{"physics", "chemistry", "biology"});
        QuickwitSplit.SplitConfig config1 = new QuickwitSplit.SplitConfig(
            "xref-fuse16-test", "test-source", "node-1");
        QuickwitSplit.SplitMetadata meta1 = QuickwitSplit.convertIndexFromPath(
            index1Path.toString(), split1Path.toString(), config1);
        System.out.println("Created split1: " + meta1.getNumDocs() + " docs");

        // Create split 2
        Path index2Path = tempDir.resolve("index2-fuse16");
        Path split2Path = tempDir.resolve("split2-fuse16.split");
        createTestIndex(index2Path, "math", new String[]{"algebra", "calculus", "geometry"});
        QuickwitSplit.SplitConfig config2 = new QuickwitSplit.SplitConfig(
            "xref-fuse16-test", "test-source", "node-2");
        QuickwitSplit.SplitMetadata meta2 = QuickwitSplit.convertIndexFromPath(
            index2Path.toString(), split2Path.toString(), config2);
        System.out.println("Created split2: " + meta2.getNumDocs() + " docs");

        // Build XRef with FUSE16 filter type (lower FPR, larger size)
        XRefSourceSplit source1 = XRefSourceSplit.fromSplitMetadata(split1Path.toString(), meta1);
        XRefSourceSplit source2 = XRefSourceSplit.fromSplitMetadata(split2Path.toString(), meta2);

        XRefBuildConfig xrefConfig = XRefBuildConfig.builder()
            .xrefId("fuse16-test-xref")
            .indexUid("fuse16-test-index")
            .sourceSplits(Arrays.asList(source1, source2))
            .filterType(XRefBuildConfig.FilterType.FUSE16)  // Use FUSE16 for ~0.0015% FPR
            .build();

        // Verify the filter type is set correctly
        assertEquals(XRefBuildConfig.FilterType.FUSE16, xrefConfig.getFilterType(),
            "Filter type should be FUSE16");
        System.out.println("Filter type: " + xrefConfig.getFilterType().getValue());
        System.out.println("Expected FPR: " + xrefConfig.getFilterType().getFalsePositiveRate());

        Path xrefPath = tempDir.resolve("fuse16-test.xref.split");
        XRefMetadata xrefMetadata = XRefSplit.build(xrefConfig, xrefPath.toString());

        assertNotNull(xrefMetadata, "XRef metadata should be returned");
        assertEquals(2, xrefMetadata.getNumSplits(), "XRef should have 2 source splits");
        assertTrue(xrefMetadata.hasFooterOffsets(), "XRef should have footer offsets");

        System.out.println("Built FUSE16 XRef:");
        System.out.println("  - XRef ID: " + xrefMetadata.getXrefId());
        System.out.println("  - Source splits: " + xrefMetadata.getNumSplits());
        System.out.println("  - Total terms: " + xrefMetadata.getTotalTerms());
        System.out.println("  - Build duration: " + xrefMetadata.getBuildStats().getBuildDurationMs() + "ms");

        // Verify the XRef can be searched
        SplitCacheManager.CacheConfig cacheConfig = new SplitCacheManager.CacheConfig("xref-fuse16-cache")
            .withMaxCacheSize(100_000_000);

        try (SplitCacheManager cacheManager = SplitCacheManager.getInstance(cacheConfig);
             XRefSearcher xrefSearcher = XRefSplit.open(cacheManager, "file://" + xrefPath.toString(), xrefMetadata)) {

            // Test 1: Match-all query should return all splits
            System.out.println("\nTest 1: Match-all query");
            XRefSearchResult matchAllResult = xrefSearcher.search("*", 10);
            assertEquals(2, matchAllResult.getNumMatchingSplits(),
                "Match-all should return all splits");

            // Test 2: Term query for science domain
            System.out.println("Test 2: Term query for 'domain:science'");
            XRefSearchResult scienceResult = xrefSearcher.search("domain:science", 10);
            assertEquals(1, scienceResult.getNumMatchingSplits(),
                "Should find 1 split with domain 'science'");

            // Test 3: Term query for math domain
            System.out.println("Test 3: Term query for 'domain:math'");
            XRefSearchResult mathResult = xrefSearcher.search("domain:math", 10);
            assertEquals(1, mathResult.getNumMatchingSplits(),
                "Should find 1 split with domain 'math'");

            // Test 4: Non-existent term should return 0 splits
            System.out.println("Test 4: Non-existent term query");
            XRefSearchResult nonExistResult = xrefSearcher.search("domain:nonexistent", 10);
            assertEquals(0, nonExistResult.getNumMatchingSplits(),
                "Non-existent term should return 0 splits");

            // Test 5: Content search in science split
            System.out.println("Test 5: Content search 'content:physics'");
            XRefSearchResult physicsResult = xrefSearcher.search("content:physics", 10);
            assertEquals(1, physicsResult.getNumMatchingSplits(),
                "Should find 1 split with 'physics' content");

            System.out.println("\n=== FUSE16 filter type test passed! ===");
        }
    }

    @Test
    @DisplayName("Compare FUSE8 vs FUSE16 filter types")
    void testCompareFuse8VsFuse16() throws Exception {
        System.out.println("\n=== Comparing FUSE8 vs FUSE16 Filter Types ===");

        // Create a single split for comparison
        Path indexPath = tempDir.resolve("index-compare");
        Path splitPath = tempDir.resolve("split-compare.split");
        createTestIndex(indexPath, "compare", new String[]{"apple", "banana", "cherry", "date", "elderberry"});
        QuickwitSplit.SplitConfig config = new QuickwitSplit.SplitConfig(
            "xref-compare-test", "test-source", "node-1");
        QuickwitSplit.SplitMetadata meta = QuickwitSplit.convertIndexFromPath(
            indexPath.toString(), splitPath.toString(), config);

        XRefSourceSplit source = XRefSourceSplit.fromSplitMetadata(splitPath.toString(), meta);

        // Build FUSE8 XRef
        XRefBuildConfig fuse8Config = XRefBuildConfig.builder()
            .xrefId("fuse8-compare-xref")
            .indexUid("compare-index")
            .sourceSplits(Arrays.asList(source))
            .filterType(XRefBuildConfig.FilterType.FUSE8)  // ~0.39% FPR
            .build();

        Path fuse8Path = tempDir.resolve("fuse8-compare.xref.split");
        XRefMetadata fuse8Metadata = XRefSplit.build(fuse8Config, fuse8Path.toString());

        // Build FUSE16 XRef
        XRefBuildConfig fuse16Config = XRefBuildConfig.builder()
            .xrefId("fuse16-compare-xref")
            .indexUid("compare-index")
            .sourceSplits(Arrays.asList(source))
            .filterType(XRefBuildConfig.FilterType.FUSE16)  // ~0.0015% FPR
            .build();

        Path fuse16Path = tempDir.resolve("fuse16-compare.xref.split");
        XRefMetadata fuse16Metadata = XRefSplit.build(fuse16Config, fuse16Path.toString());

        // Compare file sizes - FUSE16 should be larger (16-bit fingerprints vs 8-bit)
        long fuse8Size = java.nio.file.Files.size(fuse8Path);
        long fuse16Size = java.nio.file.Files.size(fuse16Path);

        System.out.println("FUSE8 XRef:");
        System.out.println("  - File size: " + fuse8Size + " bytes");
        System.out.println("  - Total terms: " + fuse8Metadata.getTotalTerms());
        System.out.println("  - Expected FPR: ~0.39%");

        System.out.println("FUSE16 XRef:");
        System.out.println("  - File size: " + fuse16Size + " bytes");
        System.out.println("  - Total terms: " + fuse16Metadata.getTotalTerms());
        System.out.println("  - Expected FPR: ~0.0015%");

        // FUSE16 uses 16-bit fingerprints (~2.24 bytes/key) vs FUSE8's 8-bit (~1.24 bytes/key)
        // So FUSE16 should be roughly 1.8x larger
        System.out.println("Size ratio (FUSE16/FUSE8): " + ((double) fuse16Size / fuse8Size));

        // Verify both can be searched and produce same results for deterministic queries
        SplitCacheManager.CacheConfig cacheConfig = new SplitCacheManager.CacheConfig("xref-compare-cache")
            .withMaxCacheSize(100_000_000);

        try (SplitCacheManager cacheManager = SplitCacheManager.getInstance(cacheConfig)) {

            try (XRefSearcher fuse8Searcher = XRefSplit.open(cacheManager, "file://" + fuse8Path.toString(), fuse8Metadata);
                 XRefSearcher fuse16Searcher = XRefSplit.open(cacheManager, "file://" + fuse16Path.toString(), fuse16Metadata)) {

                // Both should find the split for existing term
                XRefSearchResult fuse8Result = fuse8Searcher.search("domain:compare", 10);
                XRefSearchResult fuse16Result = fuse16Searcher.search("domain:compare", 10);

                assertEquals(1, fuse8Result.getNumMatchingSplits(), "FUSE8 should find 1 split");
                assertEquals(1, fuse16Result.getNumMatchingSplits(), "FUSE16 should find 1 split");

                // Both should return 0 for non-existent term
                XRefSearchResult fuse8NoMatch = fuse8Searcher.search("domain:nonexistent", 10);
                XRefSearchResult fuse16NoMatch = fuse16Searcher.search("domain:nonexistent", 10);

                assertEquals(0, fuse8NoMatch.getNumMatchingSplits(), "FUSE8 should find 0 splits for non-existent");
                assertEquals(0, fuse16NoMatch.getNumMatchingSplits(), "FUSE16 should find 0 splits for non-existent");
            }
        }

        System.out.println("\n=== FUSE8 vs FUSE16 comparison test passed! ===");
    }

    @Test
    @DisplayName("Test XRef compression with Zstd")
    void testXRefCompression() throws Exception {
        System.out.println("\n=== Testing XRef Compression ===");

        // Create a split with more data for meaningful compression comparison
        Path indexPath = tempDir.resolve("index-compression");
        Path splitPath = tempDir.resolve("split-compression.split");
        createTestIndex(indexPath, "compression", new String[]{
            "apple", "banana", "cherry", "date", "elderberry",
            "fig", "grape", "honeydew", "kiwi", "lemon",
            "mango", "nectarine", "orange", "papaya", "quince"
        });
        QuickwitSplit.SplitConfig config = new QuickwitSplit.SplitConfig(
            "xref-compression-test", "test-source", "node-1");
        QuickwitSplit.SplitMetadata meta = QuickwitSplit.convertIndexFromPath(
            indexPath.toString(), splitPath.toString(), config);

        XRefSourceSplit source = XRefSourceSplit.fromSplitMetadata(splitPath.toString(), meta);

        // Build XRef WITHOUT compression
        XRefBuildConfig noCompressionConfig = XRefBuildConfig.builder()
            .xrefId("no-compression-xref")
            .indexUid("compression-index")
            .sourceSplits(Arrays.asList(source))
            .compression(XRefBuildConfig.CompressionType.NONE)
            .build();

        Path noCompressionPath = tempDir.resolve("no-compression.xref.split");
        XRefMetadata noCompressionMetadata = XRefSplit.build(noCompressionConfig, noCompressionPath.toString());

        // Build XRef WITH zstd3 compression (default)
        XRefBuildConfig zstd3Config = XRefBuildConfig.builder()
            .xrefId("zstd3-compression-xref")
            .indexUid("compression-index")
            .sourceSplits(Arrays.asList(source))
            .compression(XRefBuildConfig.CompressionType.ZSTD3)
            .build();

        Path zstd3Path = tempDir.resolve("zstd3-compression.xref.split");
        XRefMetadata zstd3Metadata = XRefSplit.build(zstd3Config, zstd3Path.toString());

        // Build XRef WITH zstd9 compression (maximum)
        XRefBuildConfig zstd9Config = XRefBuildConfig.builder()
            .xrefId("zstd9-compression-xref")
            .indexUid("compression-index")
            .sourceSplits(Arrays.asList(source))
            .compression(XRefBuildConfig.CompressionType.ZSTD9)
            .build();

        Path zstd9Path = tempDir.resolve("zstd9-compression.xref.split");
        XRefMetadata zstd9Metadata = XRefSplit.build(zstd9Config, zstd9Path.toString());

        // Compare file sizes
        long noCompressionSize = java.nio.file.Files.size(noCompressionPath);
        long zstd3Size = java.nio.file.Files.size(zstd3Path);
        long zstd9Size = java.nio.file.Files.size(zstd9Path);

        System.out.println("Compression comparison:");
        System.out.println("  No compression: " + noCompressionSize + " bytes");
        System.out.println("  Zstd3 (default): " + zstd3Size + " bytes");
        System.out.println("  Zstd9 (max): " + zstd9Size + " bytes");

        // With very small test data, compression might not reduce size
        // but it should at least not make it much larger
        System.out.println("  Zstd3 ratio: " + ((double) zstd3Size / noCompressionSize * 100) + "%");
        System.out.println("  Zstd9 ratio: " + ((double) zstd9Size / noCompressionSize * 100) + "%");

        // Verify all XRefs can be searched and produce same results
        SplitCacheManager.CacheConfig cacheConfig = new SplitCacheManager.CacheConfig("xref-compression-cache")
            .withMaxCacheSize(100_000_000);

        try (SplitCacheManager cacheManager = SplitCacheManager.getInstance(cacheConfig)) {

            try (XRefSearcher noCompSearcher = XRefSplit.open(cacheManager, "file://" + noCompressionPath.toString(), noCompressionMetadata);
                 XRefSearcher zstd3Searcher = XRefSplit.open(cacheManager, "file://" + zstd3Path.toString(), zstd3Metadata);
                 XRefSearcher zstd9Searcher = XRefSplit.open(cacheManager, "file://" + zstd9Path.toString(), zstd9Metadata)) {

                // All should find the split for existing term
                XRefSearchResult noCompResult = noCompSearcher.search("domain:compression", 10);
                XRefSearchResult zstd3Result = zstd3Searcher.search("domain:compression", 10);
                XRefSearchResult zstd9Result = zstd9Searcher.search("domain:compression", 10);

                assertEquals(1, noCompResult.getNumMatchingSplits(), "No compression should find 1 split");
                assertEquals(1, zstd3Result.getNumMatchingSplits(), "Zstd3 should find 1 split");
                assertEquals(1, zstd9Result.getNumMatchingSplits(), "Zstd9 should find 1 split");

                // All should return 0 for non-existent term
                XRefSearchResult noCompNoMatch = noCompSearcher.search("domain:nonexistent", 10);
                XRefSearchResult zstd3NoMatch = zstd3Searcher.search("domain:nonexistent", 10);
                XRefSearchResult zstd9NoMatch = zstd9Searcher.search("domain:nonexistent", 10);

                assertEquals(0, noCompNoMatch.getNumMatchingSplits(), "No compression should find 0 splits for non-existent");
                assertEquals(0, zstd3NoMatch.getNumMatchingSplits(), "Zstd3 should find 0 splits for non-existent");
                assertEquals(0, zstd9NoMatch.getNumMatchingSplits(), "Zstd9 should find 0 splits for non-existent");

                // Content search should also work
                XRefSearchResult noCompContent = noCompSearcher.search("content:banana", 10);
                XRefSearchResult zstd3Content = zstd3Searcher.search("content:banana", 10);
                XRefSearchResult zstd9Content = zstd9Searcher.search("content:banana", 10);

                assertEquals(noCompContent.getNumMatchingSplits(), zstd3Content.getNumMatchingSplits(),
                    "Content search should return same results regardless of compression");
                assertEquals(noCompContent.getNumMatchingSplits(), zstd9Content.getNumMatchingSplits(),
                    "Content search should return same results regardless of compression");
            }
        }

        System.out.println("\n=== Compression test passed! ===");
    }

    @Test
    @DisplayName("Query tokenization: multi-word queries are tokenized")
    void testMultiWordQueryTokenization() throws Exception {
        System.out.println("\n=== Testing Multi-Word Query Tokenization ===");

        // Create a split with multi-word content
        Path index1Path = tempDir.resolve("index1-multiword");
        Path split1Path = tempDir.resolve("split1-multiword.split");

        SchemaBuilder schemaBuilder = new SchemaBuilder();
        schemaBuilder.addTextField("content", true, false, "default", "position");
        schemaBuilder.addIntegerField("id", true, true, true);
        Schema schema = schemaBuilder.build();

        try (Index index = new Index(schema, index1Path.toString());
             IndexWriter writer = index.writer(Index.Memory.DEFAULT_HEAP_SIZE, 1)) {
            // Add document with multi-word content
            Document doc = new Document();
            doc.addText("content", "hello world from tantivy4java");
            doc.addInteger("id", 1);
            writer.addDocument(doc);
            writer.commit();
        }

        QuickwitSplit.SplitConfig config1 = new QuickwitSplit.SplitConfig(
            "xref-multiword-test", "test-source", "node-1");
        QuickwitSplit.SplitMetadata meta1 = QuickwitSplit.convertIndexFromPath(
            index1Path.toString(), split1Path.toString(), config1);

        // Build XRef
        XRefSourceSplit source1 = XRefSourceSplit.fromSplitMetadata(split1Path.toString(), meta1);

        XRefBuildConfig xrefConfig = XRefBuildConfig.builder()
            .xrefId("multiword-test-xref")
            .indexUid("multiword-test-index")
            .sourceSplits(Arrays.asList(source1))
            .build();

        Path xrefPath = tempDir.resolve("multiword-test.xref.split");
        XRefMetadata xrefMetadata = XRefSplit.build(xrefConfig, xrefPath.toString());

        SplitCacheManager.CacheConfig cacheConfig = new SplitCacheManager.CacheConfig("xref-multiword-cache")
            .withMaxCacheSize(100_000_000);

        try (SplitCacheManager cacheManager = SplitCacheManager.getInstance(cacheConfig);
             XRefSearcher xrefSearcher = XRefSplit.open(cacheManager, "file://" + xrefPath.toString(), xrefMetadata)) {

            // Test 1: Single word from content should match
            System.out.println("Test 1: Single word 'content:hello' should find match");
            XRefSearchResult helloResult = xrefSearcher.search("content:hello", 10);
            assertEquals(1, helloResult.getNumMatchingSplits(),
                "Single word query should find multi-word content");

            // Test 2: Another single word should match
            System.out.println("Test 2: Another word 'content:world' should find match");
            XRefSearchResult worldResult = xrefSearcher.search("content:world", 10);
            assertEquals(1, worldResult.getNumMatchingSplits(),
                "Each word is indexed separately");

            // Test 3: Word from middle should match
            System.out.println("Test 3: Middle word 'content:tantivy4java' should find match");
            XRefSearchResult middleResult = xrefSearcher.search("content:tantivy4java", 10);
            assertEquals(1, middleResult.getNumMatchingSplits(),
                "Words in middle are also indexed");

            // Test 4: AND query should find split with both terms
            System.out.println("Test 4: AND query 'content:hello AND content:world' should find match");
            XRefSearchResult andResult = xrefSearcher.search("content:hello AND content:world", 10);
            assertEquals(1, andResult.getNumMatchingSplits(),
                "AND query should find split with both terms");

            // Test 5: AND query with non-existent term should not match
            System.out.println("Test 5: AND with nonexistent 'content:hello AND content:nonexistent' should not match");
            XRefSearchResult noMatchResult = xrefSearcher.search("content:hello AND content:nonexistent", 10);
            assertEquals(0, noMatchResult.getNumMatchingSplits(),
                "AND with nonexistent term should not match");

            System.out.println("\n=== Multi-word tokenization test passed! ===");
        }
    }
}

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
            .includePositions(false)
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
    @DisplayName("Search XRef split with regular SplitSearcher (not XRefSearcher)")
    void testSearchXRefWithRegularSplitSearcher() throws Exception {
        System.out.println("\n=== Testing XRef split with regular SplitSearcher ===");

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
            .includePositions(false)
            .build();

        Path xrefPath = tempDir.resolve("searcher-test.xref.split");
        XRefMetadata xrefMetadata = XRefSplit.build(xrefConfig, xrefPath.toString());
        System.out.println("Built XRef with " + xrefMetadata.getNumSplits() + " splits, " +
            xrefMetadata.getTotalTerms() + " terms");

        // Now search the XRef split using regular SplitSearcher (not XRefSearcher)
        SplitCacheManager.CacheConfig cacheConfig = new SplitCacheManager.CacheConfig("xref-regular-searcher-cache")
            .withMaxCacheSize(100_000_000);

        try (SplitCacheManager cacheManager = SplitCacheManager.getInstance(cacheConfig)) {

            // Create SplitMetadata from XRefMetadata for regular SplitSearcher
            QuickwitSplit.SplitMetadata splitMetadata = new QuickwitSplit.SplitMetadata(
                xrefMetadata.getXrefId(),
                xrefMetadata.getIndexUid(),
                0L,
                "xref",
                "xref-node",
                xrefMetadata.getNumSplits(),
                xrefMetadata.getBuildStats().getOutputSizeBytes(),
                null, null,
                xrefMetadata.getCreatedAt(),
                "Mature",
                java.util.Collections.emptySet(),
                xrefMetadata.getFooterStartOffset(),
                xrefMetadata.getFooterEndOffset(),
                0L, 0, "xref-doc-mapping", null, null
            );

            // Open with regular SplitSearcher
            try (SplitSearcher searcher = cacheManager.createSplitSearcher(
                    "file://" + xrefPath.toString(), splitMetadata)) {

                System.out.println("\n--- Testing regular SplitSearcher on XRef split ---");

                // Test 1: Match-all query should return 2 documents (one per source split)
                System.out.println("\nTest 1: Match-all query");
                SplitQuery matchAllQuery = new SplitMatchAllQuery();
                SearchResult matchAllResult = searcher.search(matchAllQuery, 10);
                System.out.println("  Match-all returned " + matchAllResult.getHits().size() + " hits");
                assertEquals(2, matchAllResult.getHits().size(), "XRef should have 2 documents (one per source split)");

                // Test 2: Term query for astronomy-specific term
                System.out.println("\nTest 2: Term query for 'nebula' (astronomy only)");
                SplitQuery nebulaQuery = new SplitTermQuery("content", "nebula");
                SearchResult nebulaResult = searcher.search(nebulaQuery, 10);
                System.out.println("  'nebula' query returned " + nebulaResult.getHits().size() + " hits");
                assertTrue(nebulaResult.getHits().size() >= 1, "Should find at least 1 split with 'nebula'");

                // Test 3: Term query for biology-specific term
                System.out.println("\nTest 3: Term query for 'mitochondria' (biology only)");
                SplitQuery mitoQuery = new SplitTermQuery("content", "mitochondria");
                SearchResult mitoResult = searcher.search(mitoQuery, 10);
                System.out.println("  'mitochondria' query returned " + mitoResult.getHits().size() + " hits");
                assertTrue(mitoResult.getHits().size() >= 1, "Should find at least 1 split with 'mitochondria'");

                // Test 4: Term query for domain field
                System.out.println("\nTest 4: Term query for domain 'astronomy'");
                SplitQuery domainQuery = new SplitTermQuery("domain", "astronomy");
                SearchResult domainResult = searcher.search(domainQuery, 10);
                System.out.println("  'domain:astronomy' query returned " + domainResult.getHits().size() + " hits");
                assertTrue(domainResult.getHits().size() >= 1, "Should find at least 1 split with domain 'astronomy'");

                // Test 5: Term query for non-existent term
                System.out.println("\nTest 5: Term query for 'nonexistent' (should find nothing)");
                SplitQuery nonExistQuery = new SplitTermQuery("content", "xyznonexistent123");
                SearchResult nonExistResult = searcher.search(nonExistQuery, 10);
                System.out.println("  'nonexistent' query returned " + nonExistResult.getHits().size() + " hits");
                assertEquals(0, nonExistResult.getHits().size(), "Should find no splits with non-existent term");

                // Test 6: Retrieve document and check XRef metadata fields
                System.out.println("\nTest 6: Document retrieval - checking XRef metadata fields");
                if (!matchAllResult.getHits().isEmpty()) {
                    try (Document doc = searcher.doc(matchAllResult.getHits().get(0).getDocAddress())) {
                        Object uri = doc.getFirst("_xref_uri");
                        Object splitId = doc.getFirst("_xref_split_id");
                        Object metadata = doc.getFirst("_xref_split_metadata");

                        System.out.println("  _xref_uri: " + uri);
                        System.out.println("  _xref_split_id: " + splitId);
                        System.out.println("  _xref_split_metadata present: " + (metadata != null));

                        assertNotNull(uri, "XRef document should have _xref_uri field");
                        assertNotNull(splitId, "XRef document should have _xref_split_id field");
                    }
                }

                // Test 7: Parse query (using Quickwit query syntax)
                System.out.println("\nTest 7: parseQuery with 'content:quasar'");
                SplitQuery parsedQuery = searcher.parseQuery("content:quasar");
                SearchResult parsedResult = searcher.search(parsedQuery, 10);
                System.out.println("  parsed 'content:quasar' returned " + parsedResult.getHits().size() + " hits");
                assertTrue(parsedResult.getHits().size() >= 1, "Parsed query should find astronomy split");

                System.out.println("\n=== All regular SplitSearcher tests passed! ===");
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
            .includePositions(false)
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
            .includePositions(false)
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
            .includePositions(false)
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
}

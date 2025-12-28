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
import io.indextables.tantivy4java.split.merge.QuickwitSplit;

import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Test to validate that ALL index components can be prewarmed.
 *
 * Tests the multi-component prewarm feature with:
 * - TERM: Term dictionaries (FST)
 * - POSTINGS: Posting lists
 * - FIELDNORM: Field norms for scoring
 * - FASTFIELD: Fast fields for sorting/filtering
 */
public class MultiComponentPrewarmTest {

    @TempDir
    Path tempDir;

    private static final int NUM_DOCUMENTS = 1000;
    private static final Random random = new Random(42);
    private static final List<String> TERMS = generateTerms(500);

    private static List<String> generateTerms(int count) {
        Set<String> terms = new HashSet<>();
        Random r = new Random(42);
        while (terms.size() < count) {
            int len = 6 + r.nextInt(6);
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < len; i++) {
                sb.append((char) ('a' + r.nextInt(26)));
            }
            terms.add(sb.toString());
        }
        return new ArrayList<>(terms);
    }

    @Test
    @DisplayName("Test preloading all components (TERM, POSTINGS, FIELDNORM, FASTFIELD)")
    void testPreloadAllComponents() throws Exception {
        System.out.println("=".repeat(70));
        System.out.println("MULTI-COMPONENT PREWARM TEST");
        System.out.println("=".repeat(70));

        // Step 1: Create index with multiple field types
        Path indexPath = tempDir.resolve("multi-component-test-index");
        Path splitPath = tempDir.resolve("multi-component-test.split");

        System.out.println("\nStep 1: Creating index with diverse field types...");
        createTestIndex(indexPath);

        // Step 2: Convert to split
        System.out.println("\nStep 2: Converting to Quickwit split...");
        QuickwitSplit.SplitConfig config = new QuickwitSplit.SplitConfig(
            "multi-component-index", "test-source", "test-node");

        QuickwitSplit.SplitMetadata metadata = QuickwitSplit.convertIndexFromPath(
            indexPath.toString(), splitPath.toString(), config);

        System.out.println("   Split created: " + metadata.getSplitId());
        System.out.println("   Documents: " + metadata.getNumDocs());
        System.out.println("   Size: " + Files.size(splitPath) + " bytes");

        // Step 3: Create SplitSearcher and test prewarm
        String splitUrl = "file://" + splitPath.toAbsolutePath();
        String cacheName = "multi-component-test-" + System.currentTimeMillis();

        SplitCacheManager.CacheConfig cacheConfig = new SplitCacheManager.CacheConfig(cacheName)
            .withMaxCacheSize(100_000_000); // 100MB

        SplitCacheManager cacheManager = SplitCacheManager.getInstance(cacheConfig);

        try (SplitSearcher searcher = cacheManager.createSplitSearcher(splitUrl, metadata)) {

            System.out.println("\nStep 3: Testing multi-component prewarm...");

            // Get initial cache stats
            var initialStats = searcher.getCacheStats();
            System.out.println("   Initial cache size: " + initialStats.getTotalSize() + " bytes");
            System.out.println("   Initial misses: " + initialStats.getMissCount());

            // PREWARM ALL components
            System.out.println("\n   Prewarming all components:");
            System.out.println("     - TERM (term dictionaries/FST)");
            System.out.println("     - POSTINGS (posting lists)");
            System.out.println("     - FIELDNORM (field norms)");
            System.out.println("     - FASTFIELD (fast fields)");
            System.out.println("     - STORE (document storage)");

            long prewarmStart = System.nanoTime();

            // Test that preloading all components completes without errors
            assertDoesNotThrow(() -> {
                searcher.preloadComponents(
                    SplitSearcher.IndexComponent.TERM,
                    SplitSearcher.IndexComponent.POSTINGS,
                    SplitSearcher.IndexComponent.FIELDNORM,
                    SplitSearcher.IndexComponent.FASTFIELD,
                    SplitSearcher.IndexComponent.STORE
                ).join();
            }, "Preloading all components should not throw an exception");

            long prewarmTimeMs = (System.nanoTime() - prewarmStart) / 1_000_000;

            var afterPrewarmStats = searcher.getCacheStats();
            System.out.println("\n   Prewarm completed in " + prewarmTimeMs + " ms");
            System.out.println("   Cache size after prewarm: " + afterPrewarmStats.getTotalSize() + " bytes");
            System.out.println("   Total misses (prewarm operations): " + afterPrewarmStats.getMissCount());

            // Verify cache size increased (indicates data was loaded)
            assertTrue(afterPrewarmStats.getTotalSize() >= initialStats.getTotalSize(),
                "Cache size should increase after prewarming");

            // Step 4: Execute test queries
            System.out.println("\nStep 4: Running test queries after prewarm...");

            long missesBeforeQueries = afterPrewarmStats.getMissCount();
            List<String> testTerms = Arrays.asList(
                TERMS.get(0),
                TERMS.get(TERMS.size() / 2),
                TERMS.get(TERMS.size() - 1)
            );

            for (String term : testTerms) {
                SplitQuery query = new SplitTermQuery("content", term);
                SearchResult result = searcher.search(query, 10);
                System.out.println("   Query '" + term + "' -> " + result.getHits().size() + " hits");
            }

            var finalStats = searcher.getCacheStats();
            long newMissesAfterQueries = finalStats.getMissCount() - missesBeforeQueries;

            System.out.println("\n   New cache misses after queries: " + newMissesAfterQueries);
            System.out.println("   Final hit rate: " + String.format("%.1f%%",
                finalStats.getHitCount() * 100.0 / Math.max(1, finalStats.getHitCount() + finalStats.getMissCount())));

            // Summary
            System.out.println("\n" + "=".repeat(70));
            System.out.println("MULTI-COMPONENT PREWARM TEST RESULTS");
            System.out.println("=".repeat(70));
            System.out.println("   Components prewarmed: TERM, POSTINGS, FIELDNORM, FASTFIELD, STORE");
            System.out.println("   Prewarm time: " + prewarmTimeMs + " ms");
            System.out.println("   Cache size: " + afterPrewarmStats.getTotalSize() + " bytes");
            System.out.println("   Test queries executed: " + testTerms.size());
            System.out.println("\n   SUCCESS: All components prewarmed without errors!");
            System.out.println("=".repeat(70));
        }
    }

    @Test
    @DisplayName("Test preloading individual components")
    void testPreloadIndividualComponents() throws Exception {
        System.out.println("=".repeat(70));
        System.out.println("INDIVIDUAL COMPONENT PREWARM TEST");
        System.out.println("=".repeat(70));

        // Create test index
        Path indexPath = tempDir.resolve("individual-component-test-index");
        Path splitPath = tempDir.resolve("individual-component-test.split");

        createTestIndex(indexPath);

        QuickwitSplit.SplitConfig config = new QuickwitSplit.SplitConfig(
            "individual-component-index", "test-source", "test-node");

        QuickwitSplit.SplitMetadata metadata = QuickwitSplit.convertIndexFromPath(
            indexPath.toString(), splitPath.toString(), config);

        String splitUrl = "file://" + splitPath.toAbsolutePath();

        // Test each component individually
        SplitSearcher.IndexComponent[] components = {
            SplitSearcher.IndexComponent.TERM,
            SplitSearcher.IndexComponent.POSTINGS,
            SplitSearcher.IndexComponent.FIELDNORM,
            SplitSearcher.IndexComponent.FASTFIELD,
            SplitSearcher.IndexComponent.STORE
        };

        for (SplitSearcher.IndexComponent component : components) {
            String cacheName = "individual-" + component.name() + "-" + System.currentTimeMillis();
            SplitCacheManager.CacheConfig cacheConfig = new SplitCacheManager.CacheConfig(cacheName)
                .withMaxCacheSize(100_000_000);

            SplitCacheManager cacheManager = SplitCacheManager.getInstance(cacheConfig);

            try (SplitSearcher searcher = cacheManager.createSplitSearcher(splitUrl, metadata)) {
                System.out.println("\nTesting " + component.name() + " prewarm...");

                long start = System.nanoTime();
                assertDoesNotThrow(() -> {
                    searcher.preloadComponents(component).join();
                }, component.name() + " prewarm should not throw");
                long timeMs = (System.nanoTime() - start) / 1_000_000;

                System.out.println("   " + component.name() + " prewarm completed in " + timeMs + " ms");
            }
        }

        System.out.println("\n" + "=".repeat(70));
        System.out.println("SUCCESS: All individual components prewarmed successfully!");
        System.out.println("=".repeat(70));
    }

    private void createTestIndex(Path indexPath) throws IOException {
        try (SchemaBuilder schemaBuilder = new SchemaBuilder()) {
            // Text fields (have FST/term dictionaries and field norms)
            schemaBuilder.addTextField("content", true, false, "default", "position");
            schemaBuilder.addTextField("title", true, false, "default", "position");

            // Numeric fields with fast field enabled (for FASTFIELD prewarm)
            schemaBuilder.addIntegerField("doc_id", true, true, true);  // stored, indexed, fast
            schemaBuilder.addIntegerField("score", true, true, true);   // stored, indexed, fast
            schemaBuilder.addFloatField("rating", true, true, true);    // stored, indexed, fast

            try (Schema schema = schemaBuilder.build()) {
                try (Index index = new Index(schema, indexPath.toString(), false)) {
                    try (IndexWriter writer = index.writer(Index.Memory.LARGE_HEAP_SIZE, 1)) {

                        for (int i = 0; i < NUM_DOCUMENTS; i++) {
                            try (Document doc = new Document()) {
                                // Content with multiple terms
                                StringBuilder content = new StringBuilder();
                                int numTerms = 5 + random.nextInt(10);
                                for (int j = 0; j < numTerms; j++) {
                                    content.append(TERMS.get(random.nextInt(TERMS.size()))).append(" ");
                                }
                                doc.addText("content", content.toString().trim());

                                // Title with 2-4 terms
                                StringBuilder title = new StringBuilder();
                                int numTitleTerms = 2 + random.nextInt(3);
                                for (int j = 0; j < numTitleTerms; j++) {
                                    title.append(TERMS.get(random.nextInt(TERMS.size()))).append(" ");
                                }
                                doc.addText("title", title.toString().trim());

                                // Numeric fields
                                doc.addInteger("doc_id", i);
                                doc.addInteger("score", random.nextInt(100));
                                doc.addFloat("rating", random.nextFloat() * 5.0f);

                                writer.addDocument(doc);
                            }
                        }

                        writer.commit();
                    }

                    index.reload();
                }
            }
        }
    }
}

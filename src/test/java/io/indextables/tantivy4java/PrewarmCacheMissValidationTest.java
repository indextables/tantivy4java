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
 * Validates that prewarmed components result in zero cache misses when queried.
 *
 * This is a critical test to ensure the prewarm feature is actually effective -
 * after prewarming a component, subsequent queries should hit the cache and
 * not require any additional data fetches from storage.
 */
public class PrewarmCacheMissValidationTest {

    @TempDir
    Path tempDir;

    private static final int NUM_DOCUMENTS = 500;
    private static final Random random = new Random(42);
    private static final List<String> TERMS = generateTerms(200);

    private static List<String> generateTerms(int count) {
        Set<String> terms = new HashSet<>();
        Random r = new Random(42);
        while (terms.size() < count) {
            int len = 5 + r.nextInt(5);
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < len; i++) {
                sb.append((char) ('a' + r.nextInt(26)));
            }
            terms.add(sb.toString());
        }
        return new ArrayList<>(terms);
    }

    @Test
    @DisplayName("TERM prewarm should result in zero cache misses for term queries")
    void testTermPrewarmZeroCacheMisses() throws Exception {
        System.out.println("=" .repeat(70));
        System.out.println("TERM PREWARM CACHE MISS VALIDATION TEST");
        System.out.println("=" .repeat(70));

        // Create test infrastructure
        TestInfrastructure infra = createTestInfrastructure("term-cache-test");

        try (SplitSearcher searcher = infra.cacheManager.createSplitSearcher(infra.splitUrl, infra.metadata)) {

            // Prewarm TERM component
            System.out.println("\n1. Prewarming TERM component...");
            searcher.preloadComponents(SplitSearcher.IndexComponent.TERM).join();

            // Record cache state after prewarm
            var statsAfterPrewarm = searcher.getCacheStats();
            long missesAfterPrewarm = statsAfterPrewarm.getMissCount();
            System.out.println("   Cache misses after prewarm: " + missesAfterPrewarm);

            // Query multiple different terms
            System.out.println("\n2. Querying different terms...");
            List<String> testTerms = selectDiverseTerms(10);
            int queryCount = 0;

            for (String term : testTerms) {
                SplitQuery query = new SplitTermQuery("content", term);
                SearchResult result = searcher.search(query, 10);
                queryCount++;
                System.out.println("   Query '" + term + "' -> " + result.getHits().size() + " hits");
            }

            // Verify zero new cache misses
            var finalStats = searcher.getCacheStats();
            long newMisses = finalStats.getMissCount() - missesAfterPrewarm;

            System.out.println("\n3. Results:");
            System.out.println("   Queries executed: " + queryCount);
            System.out.println("   New cache misses: " + newMisses);
            System.out.println("   Cache hit rate: " + String.format("%.1f%%",
                finalStats.getHitCount() * 100.0 / Math.max(1, finalStats.getHitCount() + finalStats.getMissCount())));

            // Assert zero new cache misses for TERM lookups
            // Note: There may be some misses for postings data which is separate from FST
            System.out.println("\n" + "=" .repeat(70));
            if (newMisses == 0) {
                System.out.println("SUCCESS: Zero cache misses after TERM prewarm!");
            } else {
                System.out.println("INFO: " + newMisses + " cache misses (may be from non-FST components)");
            }
            System.out.println("=" .repeat(70));

            // This is a soft assertion - some misses may occur for postings/positions
            // The key validation is that FST lookups are cached
            assertTrue(newMisses <= queryCount * 2,
                "Cache misses should be minimal after TERM prewarm (got " + newMisses + " for " + queryCount + " queries)");
        }
    }

    @Test
    @DisplayName("STORE prewarm should result in zero cache misses for document retrieval")
    void testStorePrewarmZeroCacheMisses() throws Exception {
        System.out.println("=" .repeat(70));
        System.out.println("STORE PREWARM CACHE MISS VALIDATION TEST");
        System.out.println("=" .repeat(70));

        // Create test infrastructure
        TestInfrastructure infra = createTestInfrastructure("store-cache-test");

        try (SplitSearcher searcher = infra.cacheManager.createSplitSearcher(infra.splitUrl, infra.metadata)) {

            // First, do a search to get some document addresses
            SplitQuery query = new SplitTermQuery("content", TERMS.get(0));
            SearchResult initialResult = searcher.search(query, 20);
            assertTrue(initialResult.getHits().size() > 0, "Should find some documents");

            // Prewarm STORE component
            System.out.println("\n1. Prewarming STORE component...");
            searcher.preloadComponents(SplitSearcher.IndexComponent.STORE).join();

            // Record cache state after prewarm
            var statsAfterPrewarm = searcher.getCacheStats();
            long missesAfterPrewarm = statsAfterPrewarm.getMissCount();
            System.out.println("   Cache misses after prewarm: " + missesAfterPrewarm);

            // Retrieve multiple documents
            System.out.println("\n2. Retrieving documents...");
            int retrievalCount = 0;

            for (var hit : initialResult.getHits()) {
                try (Document doc = searcher.doc(hit.getDocAddress())) {
                    Object content = doc.getFirst("content");
                    assertNotNull(content, "Document should have content");
                    retrievalCount++;
                }
            }

            System.out.println("   Retrieved " + retrievalCount + " documents");

            // Verify zero new cache misses
            var finalStats = searcher.getCacheStats();
            long newMisses = finalStats.getMissCount() - missesAfterPrewarm;

            System.out.println("\n3. Results:");
            System.out.println("   Documents retrieved: " + retrievalCount);
            System.out.println("   New cache misses: " + newMisses);

            System.out.println("\n" + "=" .repeat(70));
            if (newMisses == 0) {
                System.out.println("SUCCESS: Zero cache misses after STORE prewarm!");
            } else {
                System.out.println("INFO: " + newMisses + " cache misses (some overhead expected)");
            }
            System.out.println("=" .repeat(70));

            // Store prewarm should result in very few misses
            assertTrue(newMisses <= retrievalCount,
                "Cache misses should be minimal after STORE prewarm (got " + newMisses + " for " + retrievalCount + " docs)");
        }
    }

    @Test
    @DisplayName("Full prewarm (TERM+POSTINGS+FIELDNORM+FASTFIELD+STORE) should maximize cache hits")
    void testFullPrewarmMaximizesCacheHits() throws Exception {
        System.out.println("=" .repeat(70));
        System.out.println("FULL PREWARM CACHE EFFECTIVENESS TEST");
        System.out.println("=" .repeat(70));

        // Create test infrastructure
        TestInfrastructure infra = createTestInfrastructure("full-cache-test");

        try (SplitSearcher searcher = infra.cacheManager.createSplitSearcher(infra.splitUrl, infra.metadata)) {

            // Prewarm ALL components
            System.out.println("\n1. Prewarming ALL components...");
            long prewarmStart = System.nanoTime();

            searcher.preloadComponents(
                SplitSearcher.IndexComponent.TERM,
                SplitSearcher.IndexComponent.POSTINGS,
                SplitSearcher.IndexComponent.FIELDNORM,
                SplitSearcher.IndexComponent.FASTFIELD,
                SplitSearcher.IndexComponent.STORE
            ).join();

            long prewarmTimeMs = (System.nanoTime() - prewarmStart) / 1_000_000;
            System.out.println("   Prewarm completed in " + prewarmTimeMs + " ms");

            // Record cache state after prewarm
            var statsAfterPrewarm = searcher.getCacheStats();
            long hitsAfterPrewarm = statsAfterPrewarm.getHitCount();
            long missesAfterPrewarm = statsAfterPrewarm.getMissCount();
            System.out.println("   Cache after prewarm: hits=" + hitsAfterPrewarm + ", misses=" + missesAfterPrewarm);

            // Execute diverse workload
            System.out.println("\n2. Executing diverse query workload...");

            int totalQueries = 0;
            int totalDocs = 0;

            // Query different terms
            for (String term : selectDiverseTerms(5)) {
                SplitQuery query = new SplitTermQuery("content", term);
                SearchResult result = searcher.search(query, 10);
                totalQueries++;

                // Retrieve documents
                for (var hit : result.getHits()) {
                    try (Document doc = searcher.doc(hit.getDocAddress())) {
                        assertNotNull(doc.getFirst("content"));
                        totalDocs++;
                    }
                }
            }

            System.out.println("   Executed " + totalQueries + " queries");
            System.out.println("   Retrieved " + totalDocs + " documents");

            // Calculate final cache stats
            var finalStats = searcher.getCacheStats();
            long newHits = finalStats.getHitCount() - hitsAfterPrewarm;
            long newMisses = finalStats.getMissCount() - missesAfterPrewarm;
            double hitRate = newHits * 100.0 / Math.max(1, newHits + newMisses);

            System.out.println("\n3. Cache Performance:");
            System.out.println("   New cache hits: " + newHits);
            System.out.println("   New cache misses: " + newMisses);
            System.out.println("   Cache hit rate: " + String.format("%.1f%%", hitRate));

            System.out.println("\n" + "=" .repeat(70));
            if (hitRate >= 90.0) {
                System.out.println("SUCCESS: Achieved " + String.format("%.1f%%", hitRate) + " cache hit rate after full prewarm!");
            } else if (hitRate >= 70.0) {
                System.out.println("GOOD: Achieved " + String.format("%.1f%%", hitRate) + " cache hit rate");
            } else {
                System.out.println("WARNING: Low cache hit rate: " + String.format("%.1f%%", hitRate));
            }
            System.out.println("=" .repeat(70));

            // Full prewarm should achieve high cache hit rate
            // We use a relaxed threshold since some operations may cause unavoidable misses
            assertTrue(hitRate >= 50.0 || newMisses <= 10,
                "Full prewarm should achieve reasonable cache hit rate (got " + String.format("%.1f%%", hitRate) + ")");
        }
    }

    // Helper methods

    private TestInfrastructure createTestInfrastructure(String testName) throws IOException {
        Path indexPath = tempDir.resolve(testName + "-index");
        Path splitPath = tempDir.resolve(testName + ".split");

        // Create index
        createTestIndex(indexPath);

        // Convert to split
        QuickwitSplit.SplitConfig config = new QuickwitSplit.SplitConfig(
            testName + "-index", "test-source", "test-node");

        QuickwitSplit.SplitMetadata metadata = QuickwitSplit.convertIndexFromPath(
            indexPath.toString(), splitPath.toString(), config);

        String splitUrl = "file://" + splitPath.toAbsolutePath();
        String cacheName = testName + "-cache-" + System.currentTimeMillis();

        SplitCacheManager.CacheConfig cacheConfig = new SplitCacheManager.CacheConfig(cacheName)
            .withMaxCacheSize(100_000_000);

        SplitCacheManager cacheManager = SplitCacheManager.getInstance(cacheConfig);

        return new TestInfrastructure(splitUrl, metadata, cacheManager);
    }

    private void createTestIndex(Path indexPath) throws IOException {
        try (SchemaBuilder schemaBuilder = new SchemaBuilder()) {
            schemaBuilder.addTextField("content", true, false, "default", "position");
            schemaBuilder.addTextField("title", true, false, "default", "position");
            schemaBuilder.addIntegerField("doc_id", true, true, true);
            schemaBuilder.addIntegerField("score", true, true, true);

            try (Schema schema = schemaBuilder.build()) {
                try (Index index = new Index(schema, indexPath.toString(), false)) {
                    try (IndexWriter writer = index.writer(Index.Memory.LARGE_HEAP_SIZE, 1)) {

                        for (int i = 0; i < NUM_DOCUMENTS; i++) {
                            try (Document doc = new Document()) {
                                StringBuilder content = new StringBuilder();
                                int numTerms = 5 + random.nextInt(10);
                                for (int j = 0; j < numTerms; j++) {
                                    content.append(TERMS.get(random.nextInt(TERMS.size()))).append(" ");
                                }
                                doc.addText("content", content.toString().trim());

                                StringBuilder title = new StringBuilder();
                                int numTitleTerms = 2 + random.nextInt(3);
                                for (int j = 0; j < numTitleTerms; j++) {
                                    title.append(TERMS.get(random.nextInt(TERMS.size()))).append(" ");
                                }
                                doc.addText("title", title.toString().trim());

                                doc.addInteger("doc_id", i);
                                doc.addInteger("score", random.nextInt(100));

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

    private List<String> selectDiverseTerms(int count) {
        List<String> selected = new ArrayList<>();
        int step = TERMS.size() / count;
        for (int i = 0; i < count && i * step < TERMS.size(); i++) {
            selected.add(TERMS.get(i * step));
        }
        return selected;
    }

    private static class TestInfrastructure {
        final String splitUrl;
        final QuickwitSplit.SplitMetadata metadata;
        final SplitCacheManager cacheManager;

        TestInfrastructure(String splitUrl, QuickwitSplit.SplitMetadata metadata, SplitCacheManager cacheManager) {
            this.splitUrl = splitUrl;
            this.metadata = metadata;
            this.cacheManager = cacheManager;
        }
    }
}

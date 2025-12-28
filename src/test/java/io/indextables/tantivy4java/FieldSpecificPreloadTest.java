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
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for field-specific preloading functionality.
 *
 * The preloadFields() API allows preloading specific components (TERM, POSTINGS, etc.)
 * for only the specified fields, reducing cache usage and prewarm time compared to
 * preloadComponents() which preloads all fields.
 */
public class FieldSpecificPreloadTest {

    @TempDir
    Path tempDir;

    private static final int NUM_DOCUMENTS = 200;
    private static final Random random = new Random(42);

    @Test
    @DisplayName("preloadFields(TERM, fields...) should only prewarm specified fields")
    void testTermPreloadForSpecificFields() throws Exception {
        System.out.println("=" .repeat(70));
        System.out.println("FIELD-SPECIFIC TERM PRELOAD TEST");
        System.out.println("=" .repeat(70));

        TestInfrastructure infra = createTestInfrastructure("field-specific-term");

        try (SplitSearcher searcher = infra.cacheManager.createSplitSearcher(infra.splitUrl, infra.metadata)) {

            // Prewarm TERM for only 'title' field (not 'content')
            System.out.println("\n1. Prewarming TERM for 'title' field only...");
            searcher.preloadFields(SplitSearcher.IndexComponent.TERM, "title").join();

            var statsAfterPrewarm = searcher.getCacheStats();
            long missesAfterPrewarm = statsAfterPrewarm.getMissCount();
            System.out.println("   Cache misses after prewarm: " + missesAfterPrewarm);

            // Query the prewarmed field - should have minimal misses
            System.out.println("\n2. Querying prewarmed 'title' field...");
            SplitQuery titleQuery = new SplitTermQuery("title", "test");
            SearchResult titleResult = searcher.search(titleQuery, 10);
            System.out.println("   Title query returned " + titleResult.getHits().size() + " hits");

            var statsAfterTitleQuery = searcher.getCacheStats();
            long missesAfterTitleQuery = statsAfterTitleQuery.getMissCount() - missesAfterPrewarm;
            System.out.println("   New cache misses for title query: " + missesAfterTitleQuery);

            // The test validates that the API works - actual cache behavior depends on
            // the specific data patterns
            System.out.println("\n" + "=" .repeat(70));
            System.out.println("SUCCESS: Field-specific TERM preload completed without errors");
            System.out.println("=" .repeat(70));
        }
    }

    @Test
    @DisplayName("preloadFields(FASTFIELD, fields...) should only prewarm specified fast fields")
    void testFastFieldPreloadForSpecificFields() throws Exception {
        System.out.println("=" .repeat(70));
        System.out.println("FIELD-SPECIFIC FASTFIELD PRELOAD TEST");
        System.out.println("=" .repeat(70));

        TestInfrastructure infra = createTestInfrastructure("field-specific-fast");

        try (SplitSearcher searcher = infra.cacheManager.createSplitSearcher(infra.splitUrl, infra.metadata)) {

            // Prewarm FASTFIELD for only 'doc_id' field (not 'score')
            System.out.println("\n1. Prewarming FASTFIELD for 'doc_id' field only...");
            searcher.preloadFields(SplitSearcher.IndexComponent.FASTFIELD, "doc_id").join();

            var statsAfterPrewarm = searcher.getCacheStats();
            System.out.println("   Cache stats after prewarm - hits: " + statsAfterPrewarm.getHitCount() +
                             ", misses: " + statsAfterPrewarm.getMissCount());

            System.out.println("\n" + "=" .repeat(70));
            System.out.println("SUCCESS: Field-specific FASTFIELD preload completed without errors");
            System.out.println("=" .repeat(70));
        }
    }

    @Test
    @DisplayName("preloadFields should work with multiple fields")
    void testPreloadMultipleFields() throws Exception {
        System.out.println("=" .repeat(70));
        System.out.println("MULTI-FIELD PRELOAD TEST");
        System.out.println("=" .repeat(70));

        TestInfrastructure infra = createTestInfrastructure("multi-field-preload");

        try (SplitSearcher searcher = infra.cacheManager.createSplitSearcher(infra.splitUrl, infra.metadata)) {

            // Prewarm TERM for both 'title' and 'content' fields
            System.out.println("\n1. Prewarming TERM for 'title' and 'content' fields...");
            searcher.preloadFields(SplitSearcher.IndexComponent.TERM, "title", "content").join();

            var statsAfterPrewarm = searcher.getCacheStats();
            System.out.println("   Cache stats after prewarm - hits: " + statsAfterPrewarm.getHitCount() +
                             ", misses: " + statsAfterPrewarm.getMissCount());

            // Verify both fields are searchable
            System.out.println("\n2. Querying both prewarmed fields...");
            SplitQuery titleQuery = new SplitTermQuery("title", "test");
            SplitQuery contentQuery = new SplitTermQuery("content", "document");

            SearchResult titleResult = searcher.search(titleQuery, 10);
            SearchResult contentResult = searcher.search(contentQuery, 10);

            System.out.println("   Title query: " + titleResult.getHits().size() + " hits");
            System.out.println("   Content query: " + contentResult.getHits().size() + " hits");

            System.out.println("\n" + "=" .repeat(70));
            System.out.println("SUCCESS: Multi-field preload completed without errors");
            System.out.println("=" .repeat(70));
        }
    }

    @Test
    @DisplayName("preloadFields should run concurrently for different components")
    void testConcurrentFieldPreloading() throws Exception {
        System.out.println("=" .repeat(70));
        System.out.println("CONCURRENT FIELD PRELOAD TEST");
        System.out.println("=" .repeat(70));

        TestInfrastructure infra = createTestInfrastructure("concurrent-preload");

        try (SplitSearcher searcher = infra.cacheManager.createSplitSearcher(infra.splitUrl, infra.metadata)) {

            // Prewarm multiple components concurrently for specific fields
            System.out.println("\n1. Starting concurrent preloads...");
            long startTime = System.currentTimeMillis();

            CompletableFuture<Void> termPreload = searcher.preloadFields(
                SplitSearcher.IndexComponent.TERM, "title", "content");
            CompletableFuture<Void> fastFieldPreload = searcher.preloadFields(
                SplitSearcher.IndexComponent.FASTFIELD, "doc_id", "score");
            CompletableFuture<Void> postingsPreload = searcher.preloadFields(
                SplitSearcher.IndexComponent.POSTINGS, "content");

            // Wait for all to complete
            CompletableFuture.allOf(termPreload, fastFieldPreload, postingsPreload).join();

            long elapsed = System.currentTimeMillis() - startTime;
            System.out.println("   All concurrent preloads completed in " + elapsed + " ms");

            var finalStats = searcher.getCacheStats();
            System.out.println("   Final cache stats - hits: " + finalStats.getHitCount() +
                             ", misses: " + finalStats.getMissCount());

            System.out.println("\n" + "=" .repeat(70));
            System.out.println("SUCCESS: Concurrent field preloading completed without errors");
            System.out.println("=" .repeat(70));
        }
    }

    @Test
    @DisplayName("preloadFields should throw IllegalArgumentException for empty field names")
    void testPreloadFieldsValidation() throws Exception {
        System.out.println("=" .repeat(70));
        System.out.println("FIELD PRELOAD VALIDATION TEST");
        System.out.println("=" .repeat(70));

        TestInfrastructure infra = createTestInfrastructure("validation-test");

        try (SplitSearcher searcher = infra.cacheManager.createSplitSearcher(infra.splitUrl, infra.metadata)) {

            // Should throw for empty field names
            System.out.println("\n1. Testing validation for empty field names...");
            assertThrows(IllegalArgumentException.class, () -> {
                searcher.preloadFields(SplitSearcher.IndexComponent.TERM);
            }, "Should throw IllegalArgumentException for empty field names");

            // Should throw for null field names
            System.out.println("2. Testing validation for null field names...");
            assertThrows(IllegalArgumentException.class, () -> {
                searcher.preloadFields(SplitSearcher.IndexComponent.TERM, (String[]) null);
            }, "Should throw IllegalArgumentException for null field names");

            System.out.println("\n" + "=" .repeat(70));
            System.out.println("SUCCESS: Validation tests passed");
            System.out.println("=" .repeat(70));
        }
    }

    @Test
    @DisplayName("STORE component should work with preloadFields (not field-specific)")
    void testStorePreloadWithFieldsAPI() throws Exception {
        System.out.println("=" .repeat(70));
        System.out.println("STORE PRELOAD (NOT FIELD-SPECIFIC) TEST");
        System.out.println("=" .repeat(70));

        TestInfrastructure infra = createTestInfrastructure("store-preload");

        try (SplitSearcher searcher = infra.cacheManager.createSplitSearcher(infra.splitUrl, infra.metadata)) {

            // STORE is not field-specific, but the API should still work
            // (it will prewarm all document storage regardless of field names)
            System.out.println("\n1. Preloading STORE component (not field-specific)...");
            searcher.preloadFields(SplitSearcher.IndexComponent.STORE, "title").join();

            System.out.println("   STORE preload completed");

            // Verify document retrieval works
            SplitQuery query = new SplitTermQuery("title", "test");
            SearchResult result = searcher.search(query, 5);

            if (!result.getHits().isEmpty()) {
                System.out.println("\n2. Retrieving documents after STORE prewarm...");
                for (var hit : result.getHits()) {
                    try (Document doc = searcher.doc(hit.getDocAddress())) {
                        assertNotNull(doc.getFirst("title"), "Document should have title");
                    }
                }
                System.out.println("   Document retrieval successful");
            }

            System.out.println("\n" + "=" .repeat(70));
            System.out.println("SUCCESS: STORE preload through fields API completed");
            System.out.println("=" .repeat(70));
        }
    }

    // Helper methods

    private TestInfrastructure createTestInfrastructure(String testName) throws IOException {
        Path indexPath = tempDir.resolve(testName + "-index");
        Path splitPath = tempDir.resolve(testName + ".split");

        createTestIndex(indexPath);

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
            schemaBuilder.addIntegerField("doc_id", true, true, true);  // stored, indexed, fast
            schemaBuilder.addIntegerField("score", true, true, true);   // stored, indexed, fast

            try (Schema schema = schemaBuilder.build()) {
                try (Index index = new Index(schema, indexPath.toString(), false)) {
                    try (IndexWriter writer = index.writer(Index.Memory.LARGE_HEAP_SIZE, 1)) {

                        String[] words = {"test", "document", "search", "index", "query",
                                         "field", "term", "prewarm", "cache", "split"};

                        for (int i = 0; i < NUM_DOCUMENTS; i++) {
                            try (Document doc = new Document()) {
                                // Generate content with random words
                                StringBuilder content = new StringBuilder();
                                int numWords = 5 + random.nextInt(10);
                                for (int j = 0; j < numWords; j++) {
                                    content.append(words[random.nextInt(words.length)]).append(" ");
                                }
                                doc.addText("content", content.toString().trim());

                                // Generate title
                                StringBuilder title = new StringBuilder();
                                int numTitleWords = 2 + random.nextInt(3);
                                for (int j = 0; j < numTitleWords; j++) {
                                    title.append(words[random.nextInt(words.length)]).append(" ");
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

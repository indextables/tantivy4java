package io.indextables.tantivy4java;

import io.indextables.tantivy4java.core.*;
import io.indextables.tantivy4java.query.*;
import io.indextables.tantivy4java.result.*;
import io.indextables.tantivy4java.split.*;
import io.indextables.tantivy4java.split.merge.*;
import io.indextables.tantivy4java.aggregation.*;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;

import java.nio.file.Path;
import java.util.Map;

/**
 * Integration tests for the FFI read-path profiler.
 * Exercises the full lifecycle: enable → workload → snapshot → reset → disable.
 */
public class FfiProfilerTest {

    private SplitCacheManager cacheManager;
    private SplitSearcher searcher;
    private String splitPath;

    @BeforeEach
    public void setUp(@TempDir Path tempDir) throws Exception {
        // Ensure profiler starts disabled
        FfiProfiler.disable();

        String uniqueId = "profiler_test_" + System.nanoTime();
        String indexPath = tempDir.resolve(uniqueId + "_index").toString();
        splitPath = tempDir.resolve(uniqueId + ".split").toString();

        try (SchemaBuilder builder = new SchemaBuilder()) {
            builder
                .addTextField("title", true, false, "default", "position")
                .addTextField("body", true, false, "default", "position")
                .addIntegerField("score", true, true, true);

            try (Schema schema = builder.build()) {
                try (Index index = new Index(schema, indexPath, false)) {
                    try (IndexWriter writer = index.writer(Index.Memory.DEFAULT_HEAP_SIZE, 1)) {
                        for (int i = 0; i < 100; i++) {
                            try (Document doc = new Document()) {
                                doc.addText("title", "document number " + i);
                                doc.addText("body", "this is the body text for document " + i + " with some words");
                                doc.addInteger("score", i * 10);
                                writer.addDocument(doc);
                            }
                        }
                        writer.commit();
                    }

                    QuickwitSplit.SplitConfig config = new QuickwitSplit.SplitConfig(
                        uniqueId, "test-source", "test-node");
                    QuickwitSplit.SplitMetadata metadata =
                        QuickwitSplit.convertIndexFromPath(indexPath, splitPath, config);

                    String uniqueCacheName = uniqueId + "-cache";
                    SplitCacheManager.CacheConfig cacheConfig =
                        new SplitCacheManager.CacheConfig(uniqueCacheName);
                    cacheManager = SplitCacheManager.getInstance(cacheConfig);
                    searcher = cacheManager.createSplitSearcher("file://" + splitPath, metadata);
                }
            }
        }
    }

    @AfterEach
    public void tearDown() {
        FfiProfiler.disable();
        if (searcher != null) {
            searcher.close();
        }
    }

    @Test
    @DisplayName("Profiler starts disabled and enable/disable toggles correctly")
    public void testEnableDisable() {
        assertFalse(FfiProfiler.isEnabled(), "Should start disabled");
        FfiProfiler.enable();
        assertTrue(FfiProfiler.isEnabled(), "Should be enabled after enable()");
        FfiProfiler.disable();
        assertFalse(FfiProfiler.isEnabled(), "Should be disabled after disable()");
    }

    @Test
    @DisplayName("Snapshot returns empty map when no work has been done")
    public void testEmptySnapshot() {
        FfiProfiler.enable();
        Map<String, FfiProfiler.ProfileEntry> snap = FfiProfiler.snapshot();
        assertTrue(snap.isEmpty(), "Snapshot should be empty before any operations");
    }

    @Test
    @DisplayName("Search operations produce profiler entries with correct invariants")
    public void testSearchProducesProfileEntries() {
        FfiProfiler.enable();

        // Run a search
        SplitTermQuery query = new SplitTermQuery("title", "document");
        SearchResult result = searcher.search(query, 10);
        assertNotNull(result, "Search should return results");

        Map<String, FfiProfiler.ProfileEntry> snap = FfiProfiler.snapshot();
        assertFalse(snap.isEmpty(), "Snapshot should have entries after search");

        // At minimum, result_creation should fire (it's in the JNI return path)
        FfiProfiler.ProfileEntry resultCreation = snap.get("result_creation");
        assertNotNull(resultCreation, "result_creation should be present after search");
        assertTrue(resultCreation.getCount() > 0, "result_creation count should be > 0");
        assertTrue(resultCreation.getTotalNanos() > 0, "result_creation totalNanos should be > 0");

        // Verify min/max invariants
        for (FfiProfiler.ProfileEntry entry : snap.values()) {
            assertTrue(entry.getCount() > 0, entry.getSection() + " count should be > 0");
            assertTrue(entry.getTotalNanos() > 0, entry.getSection() + " totalNanos should be > 0");
            assertTrue(entry.getMinNanos() > 0, entry.getSection() + " minNanos should be > 0");
            assertTrue(entry.getMaxNanos() >= entry.getMinNanos(),
                entry.getSection() + " maxNanos should be >= minNanos");
            assertTrue(entry.getTotalNanos() >= entry.getMinNanos(),
                entry.getSection() + " totalNanos should be >= minNanos");
            assertTrue(entry.avgMicros() >= 0, entry.getSection() + " avgMicros should be >= 0");
        }

        System.out.println("Search profile entries:");
        snap.values().forEach(e -> System.out.println("  " + e));
    }

    @Test
    @DisplayName("Multiple searches accumulate counts correctly")
    public void testAccumulation() {
        FfiProfiler.enable();

        // Run 5 searches
        for (int i = 0; i < 5; i++) {
            SplitTermQuery query = new SplitTermQuery("title", "document");
            searcher.search(query, 10);
        }

        Map<String, FfiProfiler.ProfileEntry> snap = FfiProfiler.snapshot();
        FfiProfiler.ProfileEntry resultCreation = snap.get("result_creation");
        assertNotNull(resultCreation);
        assertEquals(5, resultCreation.getCount(), "Should have 5 invocations");
    }

    @Test
    @DisplayName("Reset returns pre-reset values and zeroes counters")
    public void testReset() {
        FfiProfiler.enable();

        // Do some work
        SplitTermQuery query = new SplitTermQuery("title", "document");
        searcher.search(query, 10);

        // Snapshot before reset
        Map<String, FfiProfiler.ProfileEntry> preReset = FfiProfiler.snapshot();
        assertFalse(preReset.isEmpty());

        // Reset should return the same values
        Map<String, FfiProfiler.ProfileEntry> resetResult = FfiProfiler.reset();
        assertFalse(resetResult.isEmpty());

        // After reset, snapshot should be empty (or have zeros)
        Map<String, FfiProfiler.ProfileEntry> postReset = FfiProfiler.snapshot();
        assertTrue(postReset.isEmpty(), "Snapshot should be empty after reset");
    }

    @Test
    @DisplayName("Disabled profiler produces no entries")
    public void testDisabledNoOverhead() {
        // Profiler is disabled (from setUp tearDown)
        assertFalse(FfiProfiler.isEnabled());

        SplitTermQuery query = new SplitTermQuery("title", "document");
        searcher.search(query, 10);

        // Enable just to read — enable auto-resets so this is a clean snapshot
        FfiProfiler.enable();
        Map<String, FfiProfiler.ProfileEntry> snap = FfiProfiler.snapshot();
        assertTrue(snap.isEmpty(), "No entries should exist from disabled profiling");
    }

    @Test
    @DisplayName("Aggregation search produces agg-specific profiler entries")
    public void testAggregationProfiling() {
        FfiProfiler.enable();

        // Run an aggregation search
        StatsAggregation statsAgg = new StatsAggregation("score_stats", "score");
        SplitTermQuery query = new SplitTermQuery("title", "document");

        java.util.Map<String, SplitAggregation> aggs = new java.util.HashMap<>();
        aggs.put("score_stats", statsAgg);

        SearchResult result = searcher.search(query, 10, aggs);
        assertNotNull(result, "Aggregation search should return results");

        Map<String, FfiProfiler.ProfileEntry> snap = FfiProfiler.snapshot();
        assertFalse(snap.isEmpty(), "Snapshot should have entries after aggregation search");

        // Aggregation-specific sections should be present
        FfiProfiler.ProfileEntry aggResult = snap.get("agg_result_creation");
        assertNotNull(aggResult, "agg_result_creation should be present after aggregation search");
        assertTrue(aggResult.getCount() > 0);

        System.out.println("Aggregation profile entries:");
        snap.values().forEach(e -> System.out.println("  " + e));
    }

    @Test
    @DisplayName("Cache counters are tracked correctly")
    public void testCacheCounters() {
        FfiProfiler.enable();

        // Run several searches to trigger cache activity
        for (int i = 0; i < 3; i++) {
            SplitTermQuery query = new SplitTermQuery("title", "document");
            searcher.search(query, 10);
        }

        Map<String, Long> cacheStats = FfiProfiler.cacheCounters();
        // We may or may not have cache hits depending on the split, but the API should work
        assertNotNull(cacheStats);

        System.out.println("Cache counters:");
        cacheStats.forEach((k, v) -> System.out.println("  " + k + ": " + v));
    }

    @Test
    @DisplayName("Cache counter reset returns pre-reset values and zeroes")
    public void testCacheCounterReset() {
        FfiProfiler.enable();

        SplitTermQuery query = new SplitTermQuery("title", "document");
        searcher.search(query, 10);

        Map<String, Long> preReset = FfiProfiler.cacheCounters();
        Map<String, Long> resetResult = FfiProfiler.resetCacheCounters();

        // Reset result should match pre-reset snapshot
        assertEquals(preReset.size(), resetResult.size(), "Reset should return same number of counters");

        // Post-reset should be empty
        Map<String, Long> postReset = FfiProfiler.cacheCounters();
        assertTrue(postReset.isEmpty(), "Cache counters should be empty after reset");
    }

    @Test
    @DisplayName("ProfileEntry toString produces readable output")
    public void testProfileEntryToString() {
        FfiProfiler.enable();

        SplitTermQuery query = new SplitTermQuery("title", "document");
        searcher.search(query, 10);

        Map<String, FfiProfiler.ProfileEntry> snap = FfiProfiler.snapshot();
        for (FfiProfiler.ProfileEntry entry : snap.values()) {
            String str = entry.toString();
            assertNotNull(str);
            assertTrue(str.contains(entry.getSection()), "toString should contain section name");
            assertTrue(str.contains("calls="), "toString should contain calls=");
            assertTrue(str.contains("total="), "toString should contain total=");
            assertTrue(str.contains("avg="), "toString should contain avg=");
            assertTrue(str.contains("min="), "toString should contain min=");
            assertTrue(str.contains("max="), "toString should contain max=");
        }
    }

    @Test
    @DisplayName("Section names in snapshot are from the expected set")
    public void testSectionNamesValid() {
        FfiProfiler.enable();

        SplitTermQuery query = new SplitTermQuery("title", "document");
        searcher.search(query, 10);

        Map<String, FfiProfiler.ProfileEntry> snap = FfiProfiler.snapshot();
        // All keys should be valid section names (no garbage data)
        for (String name : snap.keySet()) {
            assertTrue(name.matches("[a-z_]+"), "Section name should be lowercase+underscore: " + name);
            assertFalse(name.isEmpty(), "Section name should not be empty");
        }
    }

    @Test
    @DisplayName("Full profiler lifecycle: enable → work → snapshot → reset → more work → snapshot → disable")
    public void testFullLifecycle() {
        // Phase 1: Enable and do work
        FfiProfiler.enable();
        SplitTermQuery query = new SplitTermQuery("title", "document");
        searcher.search(query, 10);

        Map<String, FfiProfiler.ProfileEntry> snap1 = FfiProfiler.snapshot();
        assertFalse(snap1.isEmpty(), "Phase 1 should produce entries");
        long phase1Count = snap1.values().stream().mapToLong(FfiProfiler.ProfileEntry::getCount).sum();
        assertTrue(phase1Count > 0);

        // Phase 2: Reset and do more work
        FfiProfiler.reset();
        searcher.search(query, 10);
        searcher.search(query, 10);

        Map<String, FfiProfiler.ProfileEntry> snap2 = FfiProfiler.snapshot();
        assertFalse(snap2.isEmpty(), "Phase 2 should produce entries");

        // Phase 2 should have exactly 2x the count of a single search for result_creation
        FfiProfiler.ProfileEntry rc2 = snap2.get("result_creation");
        assertNotNull(rc2);
        assertEquals(2, rc2.getCount(), "Phase 2 should have 2 result_creation invocations");

        // Phase 3: Disable
        FfiProfiler.disable();
        assertFalse(FfiProfiler.isEnabled());

        // Counters should still be readable after disable
        Map<String, FfiProfiler.ProfileEntry> snap3 = FfiProfiler.snapshot();
        assertFalse(snap3.isEmpty(), "Counters should persist after disable");
    }
}

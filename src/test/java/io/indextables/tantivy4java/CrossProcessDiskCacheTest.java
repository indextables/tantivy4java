package io.indextables.tantivy4java;

import static org.junit.jupiter.api.Assertions.*;

import io.indextables.tantivy4java.core.*;
import io.indextables.tantivy4java.result.*;
import io.indextables.tantivy4java.split.*;
import io.indextables.tantivy4java.split.merge.QuickwitSplit;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.UUID;

/**
 * Validates that disk cache persists across "processes" (simulated by completely
 * tearing down and recreating all Java-side state).
 *
 * This test proves that:
 * 1. Prewarm downloads files from S3 and writes them to disk cache
 * 2. A fresh SplitCacheManager with the same disk cache path can serve queries
 *    with ZERO S3 downloads (all data served from disk cache)
 */
public class CrossProcessDiskCacheTest {

    @TempDir
    Path tempDir;

    @Test
    public void testDiskCachePersistsAcrossProcesses() throws Exception {
        // Shared paths
        Path diskCachePath = tempDir.resolve("shared_disk_cache");
        Path splitPath = tempDir.resolve("test_split_" + UUID.randomUUID() + ".split");
        String splitUri = "file://" + splitPath.toAbsolutePath();

        Files.createDirectories(diskCachePath);

        System.out.println("╔══════════════════════════════════════════════════════════════╗");
        System.out.println("║  CROSS-PROCESS DISK CACHE VALIDATION TEST                    ║");
        System.out.println("╠══════════════════════════════════════════════════════════════╣");
        System.out.println("║  Disk Cache: " + diskCachePath);
        System.out.println("║  Split URI:  " + splitUri);
        System.out.println("╚══════════════════════════════════════════════════════════════╝");

        // ═══════════════════════════════════════════════════════════════════
        // PHASE 1: "First Process" - Create index, convert to split, prewarm
        // ═══════════════════════════════════════════════════════════════════
        System.out.println("\n[PHASE 1] First 'process' - Create index, split, and prewarm...");

        // Create a test index with some documents
        Path indexPath = tempDir.resolve("test_index_" + UUID.randomUUID());
        QuickwitSplit.SplitMetadata splitMetadata;

        try (SchemaBuilder builder = new SchemaBuilder()) {
            builder.addTextField("title", true, true, "default", "position");
            builder.addTextField("body", true, false, "default", "position");
            builder.addIntegerField("doc_id", true, true, true);

            try (Schema schema = builder.build();
                 Index index = new Index(schema, indexPath.toString(), false)) {

                try (IndexWriter writer = index.writer(Index.Memory.DEFAULT_HEAP_SIZE, 1)) {
                    // Add test documents
                    for (int i = 0; i < 50; i++) {
                        try (Document doc = new Document()) {
                            doc.addText("title", "Document Title " + i);
                            doc.addText("body", "This is the body content for document number " + i);
                            doc.addInteger("doc_id", i);
                            writer.addDocument(doc);
                        }
                    }
                    writer.commit();
                }

                index.reload();
                System.out.println("   ✓ Created index with 50 documents");

                // Convert to Quickwit split
                QuickwitSplit.SplitConfig splitConfig = new QuickwitSplit.SplitConfig(
                    "cross-process-test", "test-source", "test-node");
                splitMetadata = QuickwitSplit.convertIndexFromPath(
                    indexPath.toString(), splitPath.toString(), splitConfig);
                System.out.println("   ✓ Converted to Quickwit split: " + splitPath.getFileName());
            }
        }

        // Create cache manager with disk cache and prewarm
        long phase1Downloads;
        long phase1Bytes;
        {
            SplitCacheManager.CacheConfig cacheConfig = new SplitCacheManager.CacheConfig("phase1-cache")
                .withMaxCacheSize(100_000_000);

            // Enable tiered disk cache
            SplitCacheManager.TieredCacheConfig tieredConfig = new SplitCacheManager.TieredCacheConfig()
                .withDiskCachePath(diskCachePath.toString())
                .withMaxDiskSize(1_000_000_000L);
            cacheConfig.withTieredCache(tieredConfig);

            try (SplitCacheManager cacheManager = SplitCacheManager.getInstance(cacheConfig)) {
                // Reset metrics before prewarm
                SplitCacheManager.resetStorageDownloadMetrics();

                try (SplitSearcher searcher = cacheManager.createSplitSearcher(splitUri, splitMetadata)) {
                    // Prewarm all components
                    searcher.preloadComponents(
                        SplitSearcher.IndexComponent.TERM,
                        SplitSearcher.IndexComponent.POSTINGS,
                        SplitSearcher.IndexComponent.POSITIONS,
                        SplitSearcher.IndexComponent.FASTFIELD,
                        SplitSearcher.IndexComponent.FIELDNORM,
                        SplitSearcher.IndexComponent.STORE
                    ).join();

                    // Get download metrics
                    SplitCacheManager.StorageDownloadMetrics metrics = SplitCacheManager.getStorageDownloadMetrics();
                    phase1Downloads = metrics.getTotalDownloads();
                    phase1Bytes = metrics.getTotalBytes();

                    System.out.println("   ✓ Prewarm complete: " + phase1Downloads + " downloads (" + phase1Bytes + " bytes)");

                    // Run a quick query to verify the split works
                    SplitQuery query = searcher.parseQuery("title:Document");
                    SearchResult result = searcher.search(query, 10);
                    System.out.println("   ✓ Verification query returned " + result.getHits().size() + " hits");
                }
            }
            // Cache manager is now closed - simulating process exit
        }

        assertTrue(phase1Downloads > 0, "Phase 1 should have downloaded data from storage");
        System.out.println("\n   [PHASE 1 COMPLETE] All Java state destroyed, disk cache remains at: " + diskCachePath);

        // Verify disk cache files exist
        Path sliceCacheDir = diskCachePath.resolve("tantivy4java_slicecache");
        assertTrue(Files.exists(sliceCacheDir), "Disk cache directory should exist");
        long cachedFileCount = Files.walk(sliceCacheDir)
            .filter(Files::isRegularFile)
            .count();
        System.out.println("   ✓ Disk cache contains " + cachedFileCount + " files");
        assertTrue(cachedFileCount > 0, "Disk cache should contain files after prewarm");

        // ═══════════════════════════════════════════════════════════════════
        // PHASE 2: "Second Process" - Fresh state, queries should use disk cache
        // ═══════════════════════════════════════════════════════════════════
        System.out.println("\n[PHASE 2] Second 'process' - Fresh cache manager, queries should use disk cache...");

        long phase2Downloads;
        long phase2Bytes;
        int phase2Hits;
        {
            // Create a completely fresh cache manager with same disk cache path
            SplitCacheManager.CacheConfig cacheConfig2 = new SplitCacheManager.CacheConfig("phase2-cache")
                .withMaxCacheSize(100_000_000);

            // Same disk cache path - queries should read from disk cache, not S3
            SplitCacheManager.TieredCacheConfig tieredConfig2 = new SplitCacheManager.TieredCacheConfig()
                .withDiskCachePath(diskCachePath.toString())
                .withMaxDiskSize(1_000_000_000L);
            cacheConfig2.withTieredCache(tieredConfig2);

            try (SplitCacheManager cacheManager2 = SplitCacheManager.getInstance(cacheConfig2)) {
                // Reset metrics for clean measurement
                SplitCacheManager.resetStorageDownloadMetrics();

                try (SplitSearcher searcher2 = cacheManager2.createSplitSearcher(splitUri, splitMetadata)) {
                    // Run queries directly - NO preload
                    // Expected: ZERO S3 downloads because disk cache should serve data
                    System.out.println("   Running queries (should use disk cache, zero S3 downloads)...");

                    // Query 1: Term query
                    SplitQuery query1 = searcher2.parseQuery("title:Document");
                    SearchResult result1 = searcher2.search(query1, 10);
                    System.out.println("   ✓ Query 1 (title:Document): " + result1.getHits().size() + " hits");

                    // Query 2: Different term
                    SplitQuery query2 = searcher2.parseQuery("body:content");
                    SearchResult result2 = searcher2.search(query2, 10);
                    System.out.println("   ✓ Query 2 (body:content): " + result2.getHits().size() + " hits");

                    // Query 3: Numeric range (uses fast fields)
                    SplitQuery query3 = searcher2.parseQuery("doc_id:[10 TO 20]");
                    SearchResult result3 = searcher2.search(query3, 20);
                    System.out.println("   ✓ Query 3 (doc_id:[10 TO 20]): " + result3.getHits().size() + " hits");

                    // Retrieve some documents (uses store)
                    if (!result1.getHits().isEmpty()) {
                        try (Document doc = searcher2.doc(result1.getHits().get(0).getDocAddress())) {
                            Object title = doc.getFirst("title");
                            System.out.println("   ✓ Document retrieval: " + title);
                        }
                    }

                    phase2Hits = result1.getHits().size();

                    // Get final metrics
                    SplitCacheManager.StorageDownloadMetrics metrics2 = SplitCacheManager.getStorageDownloadMetrics();
                    phase2Downloads = metrics2.getTotalDownloads();
                    phase2Bytes = metrics2.getTotalBytes();
                }
            }
        }

        // ═══════════════════════════════════════════════════════════════════
        // VALIDATION
        // ═══════════════════════════════════════════════════════════════════
        System.out.println("\n╔══════════════════════════════════════════════════════════════╗");
        System.out.println("║  RESULTS                                                     ║");
        System.out.println("╠══════════════════════════════════════════════════════════════╣");
        System.out.println("║  Phase 1 (prewarm):  " + phase1Downloads + " downloads (" + phase1Bytes + " bytes)");
        System.out.println("║  Phase 2 (queries):  " + phase2Downloads + " downloads (" + phase2Bytes + " bytes)");
        System.out.println("║  Query hits:         " + phase2Hits);
        System.out.println("╚══════════════════════════════════════════════════════════════╝");

        // The critical assertion: Phase 2 should have ZERO downloads
        assertEquals(0, phase2Downloads,
            "Phase 2 should have ZERO storage downloads - all data should come from disk cache");
        assertEquals(0, phase2Bytes,
            "Phase 2 should have ZERO bytes downloaded");
        assertTrue(phase2Hits > 0, "Queries should return results");

        System.out.println("\n✅ CROSS-PROCESS DISK CACHE VALIDATION PASSED!");
        System.out.println("   Disk cache successfully served all queries without any storage downloads.");
    }
}

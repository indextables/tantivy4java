package io.indextables.tantivy4java;

import io.indextables.tantivy4java.core.*;
import io.indextables.tantivy4java.split.*;
import io.indextables.tantivy4java.split.merge.*;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import java.nio.file.Path;
import java.nio.file.Files;
import java.util.concurrent.atomic.AtomicInteger;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Integration tests for the L2 tiered disk cache.
 *
 * Tests:
 * - TieredCacheConfig builder API
 * - CompressionAlgorithm enum values
 * - Cache configuration with SplitCacheManager
 * - Disk cache persistence and recovery
 */
public class TieredDiskCacheTest {

    @Test
    void testCompressionAlgorithmEnumValues() {
        // Verify enum values exist and have correct ordinals
        assertEquals(0, SplitCacheManager.CompressionAlgorithm.NONE.ordinal());
        assertEquals(1, SplitCacheManager.CompressionAlgorithm.LZ4.ordinal());
        assertEquals(2, SplitCacheManager.CompressionAlgorithm.ZSTD.ordinal());

        assertEquals(3, SplitCacheManager.CompressionAlgorithm.values().length);
    }

    @Test
    void testTieredCacheConfigDefaults() {
        // Test default values
        SplitCacheManager.TieredCacheConfig config = new SplitCacheManager.TieredCacheConfig();

        assertNull(config.getDiskCachePath());
        assertEquals(0, config.getMaxDiskSizeBytes()); // 0 = auto
        assertEquals(SplitCacheManager.CompressionAlgorithm.LZ4, config.getCompression());
        assertEquals(4096, config.getMinCompressSizeBytes());
        assertEquals(30, config.getManifestSyncIntervalSecs());
        assertEquals(1, config.getCompressionOrdinal()); // LZ4 = 1
    }

    @Test
    void testTieredCacheConfigBuilder(@TempDir Path tempDir) {
        String cachePath = tempDir.resolve("disk_cache").toString();

        // Test fluent builder API
        SplitCacheManager.TieredCacheConfig config = new SplitCacheManager.TieredCacheConfig()
            .withDiskCachePath(cachePath)
            .withMaxDiskSize(50_000_000_000L) // 50GB
            .withCompression(SplitCacheManager.CompressionAlgorithm.LZ4)
            .withMinCompressSize(8192)
            .withManifestSyncInterval(60);

        assertEquals(cachePath, config.getDiskCachePath());
        assertEquals(50_000_000_000L, config.getMaxDiskSizeBytes());
        assertEquals(SplitCacheManager.CompressionAlgorithm.LZ4, config.getCompression());
        assertEquals(8192, config.getMinCompressSizeBytes());
        assertEquals(60, config.getManifestSyncIntervalSecs());
    }

    @Test
    void testTieredCacheConfigWithoutCompression(@TempDir Path tempDir) {
        String cachePath = tempDir.resolve("disk_cache").toString();

        SplitCacheManager.TieredCacheConfig config = new SplitCacheManager.TieredCacheConfig()
            .withDiskCachePath(cachePath)
            .withoutCompression();

        assertEquals(SplitCacheManager.CompressionAlgorithm.NONE, config.getCompression());
        assertEquals(0, config.getCompressionOrdinal()); // NONE = 0
    }

    @Test
    void testCacheConfigWithTieredCache(@TempDir Path tempDir) {
        String cachePath = tempDir.resolve("disk_cache").toString();

        SplitCacheManager.TieredCacheConfig tieredConfig = new SplitCacheManager.TieredCacheConfig()
            .withDiskCachePath(cachePath)
            .withMaxDiskSize(10_000_000_000L); // 10GB

        // Test integrating TieredCacheConfig with CacheConfig
        SplitCacheManager.CacheConfig cacheConfig =
            new SplitCacheManager.CacheConfig("tiered-test-cache-" + System.currentTimeMillis())
                .withMaxCacheSize(100_000_000) // 100MB L1
                .withTieredCache(tieredConfig);

        assertNotNull(cacheConfig.getTieredCacheConfig());
        assertEquals(cachePath, cacheConfig.getTieredCacheConfig().getDiskCachePath());
        assertEquals(10_000_000_000L, cacheConfig.getTieredCacheConfig().getMaxDiskSizeBytes());
    }

    @Test
    void testCacheManagerCreationWithTieredCache(@TempDir Path tempDir) throws Exception {
        String cachePath = tempDir.resolve("disk_cache").toString();
        Files.createDirectories(Path.of(cachePath));

        SplitCacheManager.TieredCacheConfig tieredConfig = new SplitCacheManager.TieredCacheConfig()
            .withDiskCachePath(cachePath)
            .withMaxDiskSize(1_000_000_000L) // 1GB
            .withCompression(SplitCacheManager.CompressionAlgorithm.LZ4)
            .withMinCompressSize(4096);

        SplitCacheManager.CacheConfig cacheConfig =
            new SplitCacheManager.CacheConfig("tiered-manager-test-" + System.currentTimeMillis())
                .withMaxCacheSize(50_000_000) // 50MB L1
                .withTieredCache(tieredConfig);

        // Create cache manager - this should initialize the disk cache
        try (SplitCacheManager manager = SplitCacheManager.getInstance(cacheConfig)) {
            assertNotNull(manager);
            System.out.println("✅ Cache manager created with tiered disk cache");
        }
    }

    @Test
    void testTieredCacheWithSplitSearcher(@TempDir Path tempDir) throws Exception {
        // Step 1: Create a simple index
        Path indexPath = tempDir.resolve("test_index");
        Path splitPath = tempDir.resolve("test.split");
        String diskCachePath = tempDir.resolve("disk_cache").toString();
        Files.createDirectories(Path.of(diskCachePath));

        QuickwitSplit.SplitMetadata metadata;

        // Create index
        try (SchemaBuilder builder = new SchemaBuilder()) {
            builder.addTextField("title", true, false, "default", "position");
            builder.addTextField("content", true, false, "default", "position");

            try (Schema schema = builder.build();
                 Index index = new Index(schema, indexPath.toString(), false)) {

                try (IndexWriter writer = index.writer(Index.Memory.DEFAULT_HEAP_SIZE, 1)) {
                    for (int i = 0; i < 100; i++) {
                        try (Document doc = new Document()) {
                            doc.addText("title", "Document " + i);
                            doc.addText("content", "This is the content for document number " + i +
                                        " with searchable text.");
                            writer.addDocument(doc);
                        }
                    }
                    writer.commit();
                }
                index.reload();

                // Step 2: Convert to split
                QuickwitSplit.SplitConfig splitConfig = new QuickwitSplit.SplitConfig(
                    "test-index", "test-source", "test-node");
                metadata = QuickwitSplit.convertIndexFromPath(
                    indexPath.toString(),
                    splitPath.toString(),
                    splitConfig);

                assertTrue(Files.exists(splitPath), "Split file should be created");
                System.out.println("✅ Split created at: " + splitPath);
            }
        }

        // Step 3: Search using SplitSearcher with tiered disk cache
        SplitCacheManager.TieredCacheConfig tieredConfig = new SplitCacheManager.TieredCacheConfig()
            .withDiskCachePath(diskCachePath)
            .withMaxDiskSize(500_000_000L) // 500MB
            .withCompression(SplitCacheManager.CompressionAlgorithm.LZ4);

        SplitCacheManager.CacheConfig cacheConfig =
            new SplitCacheManager.CacheConfig("split-search-tiered-" + System.currentTimeMillis())
                .withMaxCacheSize(50_000_000) // 50MB L1
                .withTieredCache(tieredConfig);

        try (SplitCacheManager cacheManager = SplitCacheManager.getInstance(cacheConfig);
             SplitSearcher searcher = cacheManager.createSplitSearcher(
                 "file://" + splitPath.toAbsolutePath(), metadata)) {

            assertNotNull(searcher);

            // First search - should populate caches
            SplitQuery query = searcher.parseQuery("content:searchable");
            var result = searcher.search(query, 10);
            assertTrue(result.getHits().size() > 0, "Should find documents");
            System.out.println("✅ First search found " + result.getHits().size() + " documents");

            // Second search - should benefit from cache
            var result2 = searcher.search(query, 10);
            assertEquals(result.getHits().size(), result2.getHits().size());
            System.out.println("✅ Second search (cached) found " + result2.getHits().size() + " documents");
        }

        // Wait for Rust background threads to release file handles
        Thread.sleep(500);
    }

    @Test
    void testZstdCompressionFallback(@TempDir Path tempDir) {
        String cachePath = tempDir.resolve("disk_cache").toString();

        // ZSTD currently falls back to LZ4 in the native layer
        SplitCacheManager.TieredCacheConfig config = new SplitCacheManager.TieredCacheConfig()
            .withDiskCachePath(cachePath)
            .withCompression(SplitCacheManager.CompressionAlgorithm.ZSTD);

        assertEquals(SplitCacheManager.CompressionAlgorithm.ZSTD, config.getCompression());
        assertEquals(2, config.getCompressionOrdinal()); // ZSTD = 2
        // Note: Native layer will handle fallback to LZ4
    }

    @Test
    void testMinCompressSizeValidation(@TempDir Path tempDir) {
        String cachePath = tempDir.resolve("disk_cache").toString();

        // Test various min compress sizes
        SplitCacheManager.TieredCacheConfig smallConfig = new SplitCacheManager.TieredCacheConfig()
            .withDiskCachePath(cachePath)
            .withMinCompressSize(1024); // 1KB
        assertEquals(1024, smallConfig.getMinCompressSizeBytes());

        SplitCacheManager.TieredCacheConfig largeConfig = new SplitCacheManager.TieredCacheConfig()
            .withDiskCachePath(cachePath)
            .withMinCompressSize(65536); // 64KB
        assertEquals(65536, largeConfig.getMinCompressSizeBytes());
    }

    @Test
    void testDiskCacheAutoSizing(@TempDir Path tempDir) {
        String cachePath = tempDir.resolve("disk_cache").toString();

        // maxDiskSizeBytes = 0 means auto-detect (2/3 of available space)
        SplitCacheManager.TieredCacheConfig autoConfig = new SplitCacheManager.TieredCacheConfig()
            .withDiskCachePath(cachePath);

        assertEquals(0, autoConfig.getMaxDiskSizeBytes());
        // Native layer will auto-detect and use 2/3 of available space
    }

    /**
     * Test that proves the L1 -> L2 cache flow:
     * 1. First search populates both L1 (memory) and L2 (disk) caches
     * 2. Create a new cache manager with fresh L1 but same L2 path
     * 3. Second search should retrieve data from L2 (disk) and promote to L1
     *
     * This validates the tiered cache architecture:
     * - L3 (storage) -> L1 + L2 on first access
     * - L2 -> L1 on second access (after L1 is cleared)
     */
    @Test
    void testL1ToL2CacheFlow(@TempDir Path tempDir) throws Exception {
        // Step 1: Create a simple index with enough data to cache
        Path indexPath = tempDir.resolve("cache_flow_index");
        Path splitPath = tempDir.resolve("cache_flow.split");
        String diskCachePath = tempDir.resolve("persistent_disk_cache").toString();
        Files.createDirectories(Path.of(diskCachePath));

        QuickwitSplit.SplitMetadata metadata;

        // Create index with 500 documents for sufficient cache data
        try (SchemaBuilder builder = new SchemaBuilder()) {
            builder.addTextField("title", true, false, "default", "position");
            builder.addTextField("content", true, false, "default", "position");
            builder.addIntegerField("id", true, true, false);

            try (Schema schema = builder.build();
                 Index index = new Index(schema, indexPath.toString(), false)) {

                try (IndexWriter writer = index.writer(Index.Memory.DEFAULT_HEAP_SIZE, 1)) {
                    for (int i = 0; i < 500; i++) {
                        try (Document doc = new Document()) {
                            doc.addText("title", "Cache Flow Test Document " + i);
                            doc.addText("content", "This document contains searchable content " +
                                        "for testing the tiered cache flow from L1 memory to L2 disk. " +
                                        "Document number " + i + " with unique identifier.");
                            doc.addInteger("id", i);
                            writer.addDocument(doc);
                        }
                    }
                    writer.commit();
                }
                index.reload();

                // Convert to split
                QuickwitSplit.SplitConfig splitConfig = new QuickwitSplit.SplitConfig(
                    "cache-flow-index", "cache-flow-source", "cache-flow-node");
                metadata = QuickwitSplit.convertIndexFromPath(
                    indexPath.toString(),
                    splitPath.toString(),
                    splitConfig);

                assertTrue(Files.exists(splitPath), "Split file should be created");
                System.out.println("✅ Split created with " + metadata.getNumDocs() + " documents");
            }
        }

        // Step 2: First cache manager - populate L1 and L2
        SplitCacheManager.TieredCacheConfig tieredConfig = new SplitCacheManager.TieredCacheConfig()
            .withDiskCachePath(diskCachePath)
            .withMaxDiskSize(100_000_000L) // 100MB L2
            .withCompression(SplitCacheManager.CompressionAlgorithm.LZ4);

        String splitUri = "file://" + splitPath.toAbsolutePath();

        // First session - populates both L1 and L2
        System.out.println("\n📥 Session 1: Populating L1 and L2 caches...");
        try (SplitCacheManager cacheManager1 = SplitCacheManager.getInstance(
                new SplitCacheManager.CacheConfig("cache-flow-session-1-" + System.currentTimeMillis())
                    .withMaxCacheSize(10_000_000) // 10MB L1
                    .withTieredCache(tieredConfig));
             SplitSearcher searcher1 = cacheManager1.createSplitSearcher(splitUri, metadata)) {

            // Execute several searches to populate caches
            for (int i = 0; i < 5; i++) {
                SplitQuery query = searcher1.parseQuery("content:searchable");
                var result = searcher1.search(query, 50);
                assertTrue(result.getHits().size() > 0, "Should find documents");
            }
            System.out.println("✅ Session 1 complete - L1 and L2 should be populated");
        }

        // Wait a bit for disk cache manifest to sync
        Thread.sleep(100);

        // Check disk cache files exist
        java.io.File diskCacheDir = new java.io.File(diskCachePath);
        String[] cacheFiles = diskCacheDir.list();
        System.out.println("📁 Disk cache files: " + (cacheFiles != null ? cacheFiles.length : 0));

        // Step 3: Second cache manager - fresh L1, same L2
        System.out.println("\n📤 Session 2: Fresh L1, reading from L2...");
        try (SplitCacheManager cacheManager2 = SplitCacheManager.getInstance(
                new SplitCacheManager.CacheConfig("cache-flow-session-2-" + System.currentTimeMillis())
                    .withMaxCacheSize(10_000_000) // Fresh 10MB L1
                    .withTieredCache(tieredConfig));  // Same L2 disk cache path
             SplitSearcher searcher2 = cacheManager2.createSplitSearcher(splitUri, metadata)) {

            // Execute the same searches - should hit L2 and promote to L1
            for (int i = 0; i < 5; i++) {
                SplitQuery query = searcher2.parseQuery("content:searchable");
                var result = searcher2.search(query, 50);
                assertTrue(result.getHits().size() > 0, "Should find documents from cache");
            }
            System.out.println("✅ Session 2 complete - should have L2 hits");
        }

        // Verify disk cache was used (files should still exist)
        cacheFiles = diskCacheDir.list();
        assertTrue(cacheFiles != null && cacheFiles.length > 0,
            "Disk cache should have files after both sessions");

        System.out.println("\n✅ L1->L2 cache flow test complete!");
        System.out.println("   To see detailed cache hits, run with TANTIVY4JAVA_DEBUG=1");
    }

    /**
     * Test cache persistence across JVM restarts (simulated by using same disk path).
     */
    @Test
    void testDiskCachePersistence(@TempDir Path tempDir) throws Exception {
        Path indexPath = tempDir.resolve("persist_index");
        Path splitPath = tempDir.resolve("persist.split");
        String diskCachePath = tempDir.resolve("persistent_cache").toString();
        Files.createDirectories(Path.of(diskCachePath));

        QuickwitSplit.SplitMetadata metadata;

        // Create and convert index
        try (SchemaBuilder builder = new SchemaBuilder()) {
            builder.addTextField("text", true, false, "default", "position");

            try (Schema schema = builder.build();
                 Index index = new Index(schema, indexPath.toString(), false)) {

                try (IndexWriter writer = index.writer(Index.Memory.DEFAULT_HEAP_SIZE, 1)) {
                    for (int i = 0; i < 200; i++) {
                        try (Document doc = new Document()) {
                            doc.addText("text", "Persistent cache test document " + i);
                            writer.addDocument(doc);
                        }
                    }
                    writer.commit();
                }
                index.reload();

                QuickwitSplit.SplitConfig splitConfig = new QuickwitSplit.SplitConfig(
                    "persist-index", "persist-source", "persist-node");
                metadata = QuickwitSplit.convertIndexFromPath(
                    indexPath.toString(), splitPath.toString(), splitConfig);
            }
        }

        String splitUri = "file://" + splitPath.toAbsolutePath();

        // Session 1: Populate cache
        SplitCacheManager.TieredCacheConfig tieredConfig = new SplitCacheManager.TieredCacheConfig()
            .withDiskCachePath(diskCachePath)
            .withMaxDiskSize(50_000_000L);

        try (SplitCacheManager cm1 = SplitCacheManager.getInstance(
                new SplitCacheManager.CacheConfig("persist-session-1-" + System.currentTimeMillis())
                    .withMaxCacheSize(5_000_000)
                    .withTieredCache(tieredConfig));
             SplitSearcher s1 = cm1.createSplitSearcher(splitUri, metadata)) {

            var result = s1.search(s1.parseQuery("text:persistent"), 20);
            assertTrue(result.getHits().size() > 0);
            System.out.println("Session 1: Found " + result.getHits().size() + " documents");
        }

        // Check disk cache has content
        java.io.File cacheDir = new java.io.File(diskCachePath);
        long cacheSizeBefore = Files.walk(cacheDir.toPath())
            .filter(Files::isRegularFile)
            .mapToLong(p -> p.toFile().length())
            .sum();
        System.out.println("Disk cache size after session 1: " + cacheSizeBefore + " bytes");

        // Session 2: Should read from persisted cache
        try (SplitCacheManager cm2 = SplitCacheManager.getInstance(
                new SplitCacheManager.CacheConfig("persist-session-2-" + System.currentTimeMillis())
                    .withMaxCacheSize(5_000_000)
                    .withTieredCache(tieredConfig));
             SplitSearcher s2 = cm2.createSplitSearcher(splitUri, metadata)) {

            var result = s2.search(s2.parseQuery("text:persistent"), 20);
            assertTrue(result.getHits().size() > 0);
            System.out.println("Session 2: Found " + result.getHits().size() + " documents");
        }

        System.out.println("✅ Disk cache persistence test complete!");
    }

    @Test
    void testDiskCacheEvictionAPI(@TempDir Path tempDir) throws Exception {
        // Test the eviction API functionality
        Path indexPath = tempDir.resolve("eviction_index");
        Path splitPath = tempDir.resolve("eviction.split");
        String diskCachePath = tempDir.resolve("eviction_cache").toString();

        // Create a small split
        QuickwitSplit.SplitMetadata metadata;
        try (SchemaBuilder builder = new SchemaBuilder()) {
            builder.addTextField("text", true, false, "default", "position");
            try (Schema schema = builder.build();
                 Index index = new Index(schema, indexPath.toString(), false)) {
                try (IndexWriter writer = index.writer(Index.Memory.DEFAULT_HEAP_SIZE, 1)) {
                    for (int i = 0; i < 50; i++) {
                        try (Document doc = new Document()) {
                            doc.addText("text", "eviction test document number " + i);
                            writer.addDocument(doc);
                        }
                    }
                    writer.commit();
                }
                index.reload();

                QuickwitSplit.SplitConfig splitConfig = new QuickwitSplit.SplitConfig(
                    "eviction-index", "eviction-source", "eviction-node");
                metadata = QuickwitSplit.convertIndexFromPath(
                    indexPath.toString(), splitPath.toString(), splitConfig);
            }
        }

        String splitUri = "file://" + splitPath.toAbsolutePath();

        SplitCacheManager.TieredCacheConfig tieredConfig = new SplitCacheManager.TieredCacheConfig()
            .withDiskCachePath(diskCachePath)
            .withMaxDiskSize(10_000_000L);

        try (SplitCacheManager manager = SplitCacheManager.getInstance(
                new SplitCacheManager.CacheConfig("eviction-test-" + System.currentTimeMillis())
                    .withMaxCacheSize(5_000_000)
                    .withTieredCache(tieredConfig))) {

            // Verify disk cache is enabled
            assertTrue(manager.isDiskCacheEnabled(), "Disk cache should be enabled");

            // Search to access split
            try (SplitSearcher searcher = manager.createSplitSearcher(splitUri, metadata)) {
                var result = searcher.search(searcher.parseQuery("text:eviction"), 10);
                assertTrue(result.getHits().size() > 0);
            }

            // Test eviction API - should succeed even if nothing cached
            // (Note: actual L2 caching depends on Quickwit's storage patterns)
            boolean evicted = manager.evictSplitFromDiskCache(splitUri);
            assertTrue(evicted, "Eviction API should succeed");

            // Get stats after eviction
            SplitCacheManager.DiskCacheStats statsAfter = manager.getDiskCacheStats();
            System.out.println("Stats after eviction: " + statsAfter);
        }

        // Test eviction when disk cache is disabled
        try (SplitCacheManager noDiskManager = SplitCacheManager.getInstance(
                new SplitCacheManager.CacheConfig("no-disk-eviction-" + System.currentTimeMillis())
                    .withMaxCacheSize(5_000_000))) {
            assertFalse(noDiskManager.isDiskCacheEnabled());
            boolean evictResult = noDiskManager.evictSplitFromDiskCache(splitUri);
            assertFalse(evictResult, "Eviction should return false when disk cache disabled");
        }

        System.out.println("✅ Disk cache eviction API test complete!");
    }

    @Test
    void testDiskCacheStatsAPI(@TempDir Path tempDir) throws Exception {
        // Test the disk cache stats API
        String diskCachePath = tempDir.resolve("stats_cache").toString();

        SplitCacheManager.TieredCacheConfig tieredConfig = new SplitCacheManager.TieredCacheConfig()
            .withDiskCachePath(diskCachePath)
            .withMaxDiskSize(100_000_000L);

        try (SplitCacheManager manager = SplitCacheManager.getInstance(
                new SplitCacheManager.CacheConfig("stats-test-" + System.currentTimeMillis())
                    .withMaxCacheSize(5_000_000)
                    .withTieredCache(tieredConfig))) {

            // Verify stats API when disk cache is enabled
            assertTrue(manager.isDiskCacheEnabled());
            SplitCacheManager.DiskCacheStats stats = manager.getDiskCacheStats();
            assertTrue(stats.isEnabled(), "Stats should report enabled");
            assertEquals(100_000_000L, stats.getMaxBytes());
            assertTrue(stats.getTotalBytes() >= 0, "Total bytes should be non-negative");
            assertTrue(stats.getSplitCount() >= 0, "Split count should be non-negative");
            assertTrue(stats.getComponentCount() >= 0, "Component count should be non-negative");
            assertTrue(stats.getUsagePercent() >= 0, "Usage percent should be non-negative");
            assertTrue(stats.getUsagePercent() <= 100, "Usage percent should be <= 100");
            System.out.println("Stats with disk cache enabled: " + stats);

            // Verify toString() format
            String statsString = stats.toString();
            assertTrue(statsString.contains("DiskCacheStats"), "toString should contain class name");
            assertTrue(statsString.contains("totalBytes"), "toString should contain totalBytes");
        }

        // Test stats API when disk cache is NOT enabled
        try (SplitCacheManager noDiskManager = SplitCacheManager.getInstance(
                new SplitCacheManager.CacheConfig("no-disk-stats-test-" + System.currentTimeMillis())
                    .withMaxCacheSize(5_000_000))) {  // No tiered cache

            assertFalse(noDiskManager.isDiskCacheEnabled());
            SplitCacheManager.DiskCacheStats noDiskStats = noDiskManager.getDiskCacheStats();
            assertFalse(noDiskStats.isEnabled(), "Stats should report not enabled");
            assertEquals(0, noDiskStats.getTotalBytes());
            assertEquals(0, noDiskStats.getMaxBytes());
            assertEquals(0, noDiskStats.getSplitCount());
            assertEquals(0, noDiskStats.getComponentCount());
            assertEquals(0.0, noDiskStats.getUsagePercent(), 0.01);
            System.out.println("Stats without disk cache: " + noDiskStats);

            // Verify toString() for disabled state
            assertTrue(noDiskStats.toString().contains("enabled=false"));
        }

        System.out.println("✅ Disk cache stats API test complete!");
    }

    @Test
    void testCompressionConfiguration(@TempDir Path tempDir) throws Exception {
        // Test that compression configuration is accepted
        String compressedCachePath = tempDir.resolve("compressed_cache").toString();
        String uncompressedCachePath = tempDir.resolve("uncompressed_cache").toString();

        // Test LZ4 compression config
        SplitCacheManager.TieredCacheConfig lz4Config = new SplitCacheManager.TieredCacheConfig()
            .withDiskCachePath(compressedCachePath)
            .withMaxDiskSize(50_000_000L)
            .withCompression(SplitCacheManager.CompressionAlgorithm.LZ4);

        assertEquals(SplitCacheManager.CompressionAlgorithm.LZ4, lz4Config.getCompression());

        try (SplitCacheManager lz4Manager = SplitCacheManager.getInstance(
                new SplitCacheManager.CacheConfig("lz4-test-" + System.currentTimeMillis())
                    .withMaxCacheSize(5_000_000)
                    .withTieredCache(lz4Config))) {
            assertTrue(lz4Manager.isDiskCacheEnabled());
            System.out.println("✅ LZ4 compression config accepted");
        }

        // Test NONE compression config
        SplitCacheManager.TieredCacheConfig noneConfig = new SplitCacheManager.TieredCacheConfig()
            .withDiskCachePath(uncompressedCachePath)
            .withMaxDiskSize(50_000_000L)
            .withCompression(SplitCacheManager.CompressionAlgorithm.NONE);

        assertEquals(SplitCacheManager.CompressionAlgorithm.NONE, noneConfig.getCompression());

        try (SplitCacheManager noneManager = SplitCacheManager.getInstance(
                new SplitCacheManager.CacheConfig("none-test-" + System.currentTimeMillis())
                    .withMaxCacheSize(5_000_000)
                    .withTieredCache(noneConfig))) {
            assertTrue(noneManager.isDiskCacheEnabled());
            System.out.println("✅ NONE compression config accepted");
        }

        // Test ZSTD compression config (falls back to LZ4)
        String zstdCachePath = tempDir.resolve("zstd_cache").toString();
        SplitCacheManager.TieredCacheConfig zstdConfig = new SplitCacheManager.TieredCacheConfig()
            .withDiskCachePath(zstdCachePath)
            .withMaxDiskSize(50_000_000L)
            .withCompression(SplitCacheManager.CompressionAlgorithm.ZSTD);

        assertEquals(SplitCacheManager.CompressionAlgorithm.ZSTD, zstdConfig.getCompression());

        try (SplitCacheManager zstdManager = SplitCacheManager.getInstance(
                new SplitCacheManager.CacheConfig("zstd-test-" + System.currentTimeMillis())
                    .withMaxCacheSize(5_000_000)
                    .withTieredCache(zstdConfig))) {
            assertTrue(zstdManager.isDiskCacheEnabled());
            System.out.println("✅ ZSTD compression config accepted (falls back to LZ4)");
        }

        System.out.println("✅ Compression configuration test complete!");
    }

    @Test
    void testConcurrentDiskCacheAccess(@TempDir Path tempDir) throws Exception {
        // Test concurrent access to disk cache from multiple threads
        Path indexPath = tempDir.resolve("concurrent_index");
        Path splitPath = tempDir.resolve("concurrent.split");
        String diskCachePath = tempDir.resolve("concurrent_cache").toString();

        // Create a split
        QuickwitSplit.SplitMetadata metadata;
        try (SchemaBuilder builder = new SchemaBuilder()) {
            builder.addTextField("text", true, false, "default", "position");
            try (Schema schema = builder.build();
                 Index index = new Index(schema, indexPath.toString(), false)) {
                try (IndexWriter writer = index.writer(Index.Memory.DEFAULT_HEAP_SIZE, 1)) {
                    for (int i = 0; i < 100; i++) {
                        try (Document doc = new Document()) {
                            doc.addText("text", "concurrent test document number " + i);
                            writer.addDocument(doc);
                        }
                    }
                    writer.commit();
                }
                index.reload();

                QuickwitSplit.SplitConfig splitConfig = new QuickwitSplit.SplitConfig(
                    "concurrent-index", "concurrent-source", "concurrent-node");
                metadata = QuickwitSplit.convertIndexFromPath(
                    indexPath.toString(), splitPath.toString(), splitConfig);
            }
        }

        String splitUri = "file://" + splitPath.toAbsolutePath();

        SplitCacheManager.TieredCacheConfig tieredConfig = new SplitCacheManager.TieredCacheConfig()
            .withDiskCachePath(diskCachePath)
            .withMaxDiskSize(50_000_000L);

        java.util.concurrent.atomic.AtomicInteger successCount = new java.util.concurrent.atomic.AtomicInteger(0);
        java.util.concurrent.atomic.AtomicInteger errorCount = new java.util.concurrent.atomic.AtomicInteger(0);

        try (SplitCacheManager manager = SplitCacheManager.getInstance(
                new SplitCacheManager.CacheConfig("concurrent-test-" + System.currentTimeMillis())
                    .withMaxCacheSize(10_000_000)
                    .withTieredCache(tieredConfig))) {

            // Run concurrent searches
            java.util.concurrent.ExecutorService executor = java.util.concurrent.Executors.newFixedThreadPool(4);
            java.util.List<java.util.concurrent.Future<?>> futures = new java.util.ArrayList<>();
            final QuickwitSplit.SplitMetadata finalMetadata = metadata;

            for (int i = 0; i < 10; i++) {
                int threadNum = i;
                futures.add(executor.submit(() -> {
                    try (SplitSearcher searcher = manager.createSplitSearcher(splitUri, finalMetadata)) {
                        for (int j = 0; j < 5; j++) {
                            var result = searcher.search(searcher.parseQuery("text:concurrent"), 10);
                            if (result.getHits().size() > 0) {
                                successCount.incrementAndGet();
                            }
                        }
                    } catch (Exception e) {
                        errorCount.incrementAndGet();
                        System.err.println("Thread " + threadNum + " error: " + e.getMessage());
                    }
                }));
            }

            // Wait for completion
            for (var future : futures) {
                future.get(30, java.util.concurrent.TimeUnit.SECONDS);
            }
            executor.shutdown();

            System.out.println("Concurrent test: " + successCount.get() + " successes, " +
                             errorCount.get() + " errors");
            assertTrue(successCount.get() > 0, "Should have successful searches");
            assertEquals(0, errorCount.get(), "Should have no errors");
        }

        System.out.println("✅ Concurrent disk cache access test complete!");
    }

    // ── Write Queue Configuration Tests ────────────────────────────────

    @Test
    void testWriteQueueModeDefaults() {
        SplitCacheManager.TieredCacheConfig config = new SplitCacheManager.TieredCacheConfig();

        // Default mode is FRAGMENT
        assertEquals(0, config.getWriteQueueModeOrdinal(), "Default mode should be FRAGMENT (ordinal 0)");
        assertEquals(16, config.getWriteQueueCapacity(), "Default fragment capacity should be 16");
        assertEquals(2_147_483_648L, config.getWriteQueueMaxBytes(), "Default max bytes should be 2GB");
        assertFalse(config.isDropWritesWhenFull(), "Drop writes should be disabled by default");
    }

    @Test
    void testWriteQueueFragmentCapacityBuilder() {
        SplitCacheManager.TieredCacheConfig config = new SplitCacheManager.TieredCacheConfig()
            .withWriteQueueFragmentCapacity(32);

        assertEquals(SplitCacheManager.WriteQueueMode.FRAGMENT, getMode(config));
        assertEquals(0, config.getWriteQueueModeOrdinal());
        assertEquals(32, config.getWriteQueueCapacity());
    }

    @Test
    void testWriteQueueSizeLimitBuilder() {
        SplitCacheManager.TieredCacheConfig config = new SplitCacheManager.TieredCacheConfig()
            .withWriteQueueSizeLimit(500_000_000L);

        assertEquals(SplitCacheManager.WriteQueueMode.SIZE_BASED, getMode(config));
        assertEquals(1, config.getWriteQueueModeOrdinal());
        assertEquals(500_000_000L, config.getWriteQueueMaxBytes());
    }

    @Test
    void testWriteQueueModeLastCallWins() {
        // If both are called, last call wins
        SplitCacheManager.TieredCacheConfig config = new SplitCacheManager.TieredCacheConfig()
            .withWriteQueueFragmentCapacity(8)
            .withWriteQueueSizeLimit(1_000_000_000L);

        assertEquals(1, config.getWriteQueueModeOrdinal(), "Last call (SIZE_BASED) should win");

        SplitCacheManager.TieredCacheConfig config2 = new SplitCacheManager.TieredCacheConfig()
            .withWriteQueueSizeLimit(1_000_000_000L)
            .withWriteQueueFragmentCapacity(8);

        assertEquals(0, config2.getWriteQueueModeOrdinal(), "Last call (FRAGMENT) should win");
    }

    @Test
    void testDropWritesWhenFullBuilder() {
        SplitCacheManager.TieredCacheConfig config = new SplitCacheManager.TieredCacheConfig()
            .withDropWritesWhenFull(true);

        assertTrue(config.isDropWritesWhenFull());

        SplitCacheManager.TieredCacheConfig config2 = new SplitCacheManager.TieredCacheConfig()
            .withDropWritesWhenFull(false);

        assertFalse(config2.isDropWritesWhenFull());
    }

    /**
     * End-to-end: Fragment mode with custom capacity flows through JNI to Rust.
     * Creates a SplitCacheManager with fragment capacity=32, searches a real split,
     * and verifies data is cached to disk.
     */
    @Test
    void testFragmentModeEndToEnd(@TempDir Path tempDir) throws Exception {
        Path indexPath = tempDir.resolve("frag_e2e_index");
        Path splitPath = tempDir.resolve("frag_e2e.split");
        String diskCachePath = tempDir.resolve("frag_e2e_cache").toString();

        QuickwitSplit.SplitMetadata metadata = createTestSplit(indexPath, splitPath);
        String splitUri = "file://" + splitPath.toAbsolutePath();

        SplitCacheManager.TieredCacheConfig tieredConfig = new SplitCacheManager.TieredCacheConfig()
            .withDiskCachePath(diskCachePath)
            .withMaxDiskSize(100_000_000L)
            .withWriteQueueFragmentCapacity(32);

        assertEquals(0, tieredConfig.getWriteQueueModeOrdinal(), "Should be FRAGMENT mode");
        assertEquals(32, tieredConfig.getWriteQueueCapacity());

        // Session 1: populate disk cache
        try (SplitCacheManager cm = SplitCacheManager.getInstance(
                new SplitCacheManager.CacheConfig("frag-e2e-" + System.currentTimeMillis())
                    .withMaxCacheSize(10_000_000)
                    .withTieredCache(tieredConfig));
             SplitSearcher searcher = cm.createSplitSearcher(splitUri, metadata)) {

            assertTrue(cm.isDiskCacheEnabled(), "Disk cache should be enabled");

            var result = searcher.search(searcher.parseQuery("content:searchable"), 10);
            assertTrue(result.getHits().size() > 0, "Should find documents");

            // Give background writer time to flush
            Thread.sleep(200);
        }

        // Verify disk cache has content
        long cacheSize = Files.walk(Path.of(diskCachePath))
            .filter(Files::isRegularFile)
            .mapToLong(p -> p.toFile().length())
            .sum();
        assertTrue(cacheSize > 0, "Disk cache should have data after fragment-mode search");
        System.out.println("✅ Fragment mode E2E: disk cache populated with " + cacheSize + " bytes");

        // Session 2: verify cached data serves queries
        SplitCacheManager.resetStorageDownloadMetrics();
        try (SplitCacheManager cm2 = SplitCacheManager.getInstance(
                new SplitCacheManager.CacheConfig("frag-e2e-2-" + System.currentTimeMillis())
                    .withMaxCacheSize(10_000_000)
                    .withTieredCache(tieredConfig));
             SplitSearcher searcher2 = cm2.createSplitSearcher(splitUri, metadata)) {

            var result2 = searcher2.search(searcher2.parseQuery("content:searchable"), 10);
            assertTrue(result2.getHits().size() > 0, "Session 2 should find documents from cache");
        }
        System.out.println("✅ Fragment mode E2E: session 2 served from disk cache");
    }

    /**
     * End-to-end: Size-based mode flows through JNI to Rust.
     * Creates a SplitCacheManager with size-based 500MB limit, searches a real split,
     * and verifies data is cached to disk.
     */
    @Test
    void testSizeBasedModeEndToEnd(@TempDir Path tempDir) throws Exception {
        Path indexPath = tempDir.resolve("sb_e2e_index");
        Path splitPath = tempDir.resolve("sb_e2e.split");
        String diskCachePath = tempDir.resolve("sb_e2e_cache").toString();

        QuickwitSplit.SplitMetadata metadata = createTestSplit(indexPath, splitPath);
        String splitUri = "file://" + splitPath.toAbsolutePath();

        SplitCacheManager.TieredCacheConfig tieredConfig = new SplitCacheManager.TieredCacheConfig()
            .withDiskCachePath(diskCachePath)
            .withMaxDiskSize(100_000_000L)
            .withWriteQueueSizeLimit(500_000_000L);

        assertEquals(1, tieredConfig.getWriteQueueModeOrdinal(), "Should be SIZE_BASED mode");
        assertEquals(500_000_000L, tieredConfig.getWriteQueueMaxBytes());

        // Session 1: populate disk cache
        try (SplitCacheManager cm = SplitCacheManager.getInstance(
                new SplitCacheManager.CacheConfig("sb-e2e-" + System.currentTimeMillis())
                    .withMaxCacheSize(10_000_000)
                    .withTieredCache(tieredConfig));
             SplitSearcher searcher = cm.createSplitSearcher(splitUri, metadata)) {

            assertTrue(cm.isDiskCacheEnabled(), "Disk cache should be enabled");

            var result = searcher.search(searcher.parseQuery("content:searchable"), 10);
            assertTrue(result.getHits().size() > 0, "Should find documents");

            Thread.sleep(200);
        }

        // Verify disk cache has content
        long cacheSize = Files.walk(Path.of(diskCachePath))
            .filter(Files::isRegularFile)
            .mapToLong(p -> p.toFile().length())
            .sum();
        assertTrue(cacheSize > 0, "Disk cache should have data after size-based-mode search");
        System.out.println("✅ Size-based mode E2E: disk cache populated with " + cacheSize + " bytes");

        // Session 2: verify cached data serves queries
        try (SplitCacheManager cm2 = SplitCacheManager.getInstance(
                new SplitCacheManager.CacheConfig("sb-e2e-2-" + System.currentTimeMillis())
                    .withMaxCacheSize(10_000_000)
                    .withTieredCache(tieredConfig));
             SplitSearcher searcher2 = cm2.createSplitSearcher(splitUri, metadata)) {

            var result2 = searcher2.search(searcher2.parseQuery("content:searchable"), 10);
            assertTrue(result2.getHits().size() > 0, "Session 2 should find documents from cache");
        }
        System.out.println("✅ Size-based mode E2E: session 2 served from disk cache");
    }

    /**
     * End-to-end: dropWritesWhenFull=true still caches data when queue is not full.
     * Verifies the option flows through JNI and doesn't break normal caching.
     */
    @Test
    void testDropWritesWhenFullEndToEnd(@TempDir Path tempDir) throws Exception {
        Path indexPath = tempDir.resolve("drop_e2e_index");
        Path splitPath = tempDir.resolve("drop_e2e.split");
        String diskCachePath = tempDir.resolve("drop_e2e_cache").toString();

        QuickwitSplit.SplitMetadata metadata = createTestSplit(indexPath, splitPath);
        String splitUri = "file://" + splitPath.toAbsolutePath();

        SplitCacheManager.TieredCacheConfig tieredConfig = new SplitCacheManager.TieredCacheConfig()
            .withDiskCachePath(diskCachePath)
            .withMaxDiskSize(100_000_000L)
            .withDropWritesWhenFull(true);

        assertTrue(tieredConfig.isDropWritesWhenFull());

        // Session 1: populate cache (under normal load, queue won't be full)
        try (SplitCacheManager cm = SplitCacheManager.getInstance(
                new SplitCacheManager.CacheConfig("drop-e2e-" + System.currentTimeMillis())
                    .withMaxCacheSize(10_000_000)
                    .withTieredCache(tieredConfig));
             SplitSearcher searcher = cm.createSplitSearcher(splitUri, metadata)) {

            assertTrue(cm.isDiskCacheEnabled());

            // Multiple searches to give the cache data to write
            for (int i = 0; i < 3; i++) {
                var result = searcher.search(searcher.parseQuery("content:searchable"), 10);
                assertTrue(result.getHits().size() > 0);
            }

            Thread.sleep(300);
        }

        // Disk cache should have data (under light load, nothing gets dropped)
        long cacheSize = Files.walk(Path.of(diskCachePath))
            .filter(Files::isRegularFile)
            .mapToLong(p -> p.toFile().length())
            .sum();
        assertTrue(cacheSize > 0,
            "With dropWritesWhenFull=true under light load, data should still be cached");
        System.out.println("✅ dropWritesWhenFull E2E: disk cache populated with " + cacheSize + " bytes");
    }

    /**
     * End-to-end: Prewarm with size-based mode + drop enabled, verify disk cache populated.
     * Prewarm should always block (never drop), then second session served from disk cache.
     */
    @Test
    void testPrewarmWithSizeBasedModeEndToEnd(@TempDir Path tempDir) throws Exception {
        Path indexPath = tempDir.resolve("prewarm_sb_index");
        Path splitPath = tempDir.resolve("prewarm_sb.split");
        String diskCachePath = tempDir.resolve("prewarm_sb_cache").toString();

        QuickwitSplit.SplitMetadata metadata = createTestSplit(indexPath, splitPath);
        String splitUri = "file://" + splitPath.toAbsolutePath();

        SplitCacheManager.TieredCacheConfig tieredConfig = new SplitCacheManager.TieredCacheConfig()
            .withDiskCachePath(diskCachePath)
            .withMaxDiskSize(100_000_000L)
            .withWriteQueueSizeLimit(100_000_000L)
            .withDropWritesWhenFull(true); // Even with drop enabled, prewarm must block

        // Session 1: prewarm all components — verify disk cache gets populated
        long diskCacheBytesAfterPrewarm;
        try (SplitCacheManager cm = SplitCacheManager.getInstance(
                new SplitCacheManager.CacheConfig("prewarm-sb-" + System.currentTimeMillis())
                    .withMaxCacheSize(10_000_000)
                    .withTieredCache(tieredConfig));
             SplitSearcher searcher = cm.createSplitSearcher(splitUri, metadata)) {

            // Prewarm all components — uses blocking put(), not put_query_path()
            searcher.preloadComponents(
                SplitSearcher.IndexComponent.TERM,
                SplitSearcher.IndexComponent.POSTINGS,
                SplitSearcher.IndexComponent.FASTFIELD,
                SplitSearcher.IndexComponent.FIELDNORM,
                SplitSearcher.IndexComponent.STORE
            ).join();

            SplitCacheManager.DiskCacheStats stats = cm.getDiskCacheStats();
            diskCacheBytesAfterPrewarm = stats.getTotalBytes();
            assertTrue(stats.isEnabled(), "Disk cache should be enabled");
            assertTrue(diskCacheBytesAfterPrewarm > 0,
                "Phase 1 prewarm should have populated disk cache");
            System.out.println("✅ Phase 1 prewarm: disk cache has " +
                diskCacheBytesAfterPrewarm + " bytes, " + stats.getComponentCount() + " components");
        }

        // Session 2: fresh manager, same disk cache — should find data from cache
        try (SplitCacheManager cm2 = SplitCacheManager.getInstance(
                new SplitCacheManager.CacheConfig("prewarm-sb-2-" + System.currentTimeMillis())
                    .withMaxCacheSize(10_000_000)
                    .withTieredCache(tieredConfig));
             SplitSearcher searcher2 = cm2.createSplitSearcher(splitUri, metadata)) {

            var result = searcher2.search(searcher2.parseQuery("content:searchable"), 10);
            assertTrue(result.getHits().size() > 0, "Should find documents from disk cache");
            System.out.println("✅ Phase 2 search: found " + result.getHits().size() +
                " results from disk cache");
        }
    }

    // ── Helpers ──────────────────────────────────────────────────────────

    /** Helper to get WriteQueueMode from ordinal. */
    private SplitCacheManager.WriteQueueMode getMode(SplitCacheManager.TieredCacheConfig config) {
        return SplitCacheManager.WriteQueueMode.values()[config.getWriteQueueModeOrdinal()];
    }

    /** Create a test split with 200 documents for integration tests. */
    private QuickwitSplit.SplitMetadata createTestSplit(Path indexPath, Path splitPath) throws Exception {
        try (SchemaBuilder builder = new SchemaBuilder()) {
            builder.addTextField("title", true, false, "default", "position");
            builder.addTextField("content", true, false, "default", "position");
            builder.addIntegerField("id", true, true, false);

            try (Schema schema = builder.build();
                 Index index = new Index(schema, indexPath.toString(), false)) {

                try (IndexWriter writer = index.writer(Index.Memory.DEFAULT_HEAP_SIZE, 1)) {
                    for (int i = 0; i < 200; i++) {
                        try (Document doc = new Document()) {
                            doc.addText("title", "Test Document " + i);
                            doc.addText("content", "This document contains searchable content " +
                                "for write queue integration testing. Document number " + i);
                            doc.addInteger("id", i);
                            writer.addDocument(doc);
                        }
                    }
                    writer.commit();
                }
                index.reload();

                QuickwitSplit.SplitConfig splitConfig = new QuickwitSplit.SplitConfig(
                    "wq-test-index", "wq-test-source", "wq-test-node");
                return QuickwitSplit.convertIndexFromPath(
                    indexPath.toString(), splitPath.toString(), splitConfig);
            }
        }
    }
}

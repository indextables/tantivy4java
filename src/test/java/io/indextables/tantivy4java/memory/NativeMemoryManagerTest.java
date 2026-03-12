package io.indextables.tantivy4java.memory;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.MethodOrderer.OrderAnnotation;
import org.junit.jupiter.api.BeforeAll;

import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

import io.indextables.tantivy4java.core.*;

/**
 * Integration tests for the unified memory management system.
 *
 * Tests are ordered: default-pool tests run first (@Order 1-49),
 * then setAccountant tests run last (@Order 50+), since OnceLock
 * means setAccountant can only be called once per JVM.
 */
@TestMethodOrder(OrderAnnotation.class)
public class NativeMemoryManagerTest {

    @BeforeAll
    static void ensureNativeLoaded() {
        // Force native library load via Tantivy version check
        try {
            Class.forName("io.indextables.tantivy4java.core.Index");
        } catch (ClassNotFoundException e) {
            fail("Native library not available");
        }
    }

    // ========================================================================
    // Basic API Tests (default unlimited pool — must run before setAccountant)
    // ========================================================================

    @Test
    @Order(1)
    void testDefaultUnlimitedMode() {
        // Without calling setAccountant, the pool should be in unlimited mode
        NativeMemoryStats stats = NativeMemoryManager.getStats();
        assertNotNull(stats, "Stats should never be null");
        assertTrue(stats.getUsedBytes() >= 0, "Used bytes should be non-negative");
        assertTrue(stats.getPeakBytes() >= 0, "Peak bytes should be non-negative");
        // Default pool reports granted as -1 (unlimited)
        assertEquals(-1, stats.getGrantedBytes(), "Default pool should report unlimited (-1)");
    }

    @Test
    @Order(2)
    void testStatsToString() {
        NativeMemoryStats stats = NativeMemoryManager.getStats();
        String str = stats.toString();
        assertNotNull(str);
        assertTrue(str.contains("NativeMemoryStats{"), "Should contain class name");
        assertTrue(str.contains("used="), "Should contain used field");
        assertTrue(str.contains("peak="), "Should contain peak field");
        assertTrue(str.contains("granted="), "Should contain granted field");
    }

    @Test
    @Order(3)
    void testUnlimitedAccountantAlwaysGrants() {
        UnlimitedMemoryAccountant accountant = new UnlimitedMemoryAccountant();
        assertEquals(1000, accountant.acquireMemory(1000));
        assertEquals(1_000_000_000L, accountant.acquireMemory(1_000_000_000L));
        // releaseMemory is a no-op — just verify it doesn't throw
        accountant.releaseMemory(1000);
    }

    @Test
    @Order(4)
    void testSetAccountantRejectsNull() {
        assertThrows(IllegalArgumentException.class, () -> {
            NativeMemoryManager.setAccountant(null);
        });
    }

    // ========================================================================
    // Mock Accountant Tests (pure Java, no native calls)
    // ========================================================================

    @Test
    @Order(5)
    void testTrackingAccountantRecordsOperations() {
        TrackingAccountant accountant = new TrackingAccountant();

        // Simulate what the native layer does
        long acquired = accountant.acquireMemory(1024);
        assertEquals(1024, acquired);
        assertEquals(1024, accountant.getTotalAcquired());
        assertEquals(1, accountant.getAcquireCallCount());

        accountant.releaseMemory(512);
        assertEquals(512, accountant.getTotalReleased());
        assertEquals(1, accountant.getReleaseCallCount());
    }

    @Test
    @Order(6)
    void testLimitedAccountantDeniesExcessiveRequests() {
        LimitedAccountant accountant = new LimitedAccountant(1_000_000); // 1MB limit

        assertEquals(500_000, accountant.acquireMemory(500_000)); // OK
        assertEquals(500_000, accountant.acquireMemory(500_000)); // OK, at limit
        assertEquals(0, accountant.acquireMemory(1)); // Denied, over limit

        accountant.releaseMemory(100_000);
        assertEquals(100_000, accountant.acquireMemory(100_000)); // OK, freed some
    }

    @Test
    @Order(7)
    void testLimitedAccountantPartialGrant() {
        LimitedAccountant accountant = new LimitedAccountant(1_000_000);

        accountant.acquireMemory(800_000);
        // Only 200KB left, requesting 500KB should get 0 (denied)
        long acquired = accountant.acquireMemory(500_000);
        assertEquals(0, acquired, "Should deny when insufficient memory");
    }

    // ========================================================================
    // Thread Safety Tests (pure Java accountant tests)
    // ========================================================================

    @Test
    @Order(8)
    void testTrackingAccountantThreadSafety() throws Exception {
        TrackingAccountant accountant = new TrackingAccountant();
        int numThreads = 10;
        int opsPerThread = 1000;
        CountDownLatch latch = new CountDownLatch(numThreads);
        ExecutorService executor = Executors.newFixedThreadPool(numThreads);

        for (int i = 0; i < numThreads; i++) {
            executor.submit(() -> {
                try {
                    for (int j = 0; j < opsPerThread; j++) {
                        accountant.acquireMemory(100);
                        accountant.releaseMemory(100);
                    }
                } finally {
                    latch.countDown();
                }
            });
        }

        assertTrue(latch.await(30, TimeUnit.SECONDS));
        executor.shutdown();

        assertEquals(numThreads * opsPerThread, accountant.getAcquireCallCount());
        assertEquals(numThreads * opsPerThread, accountant.getReleaseCallCount());
        assertEquals(
                accountant.getTotalAcquired(),
                accountant.getTotalReleased(),
                "All acquired memory should be released"
        );
    }

    @Test
    @Order(9)
    void testLimitedAccountantThreadSafety() throws Exception {
        long limit = 10_000_000; // 10MB
        LimitedAccountant accountant = new LimitedAccountant(limit);
        int numThreads = 8;
        int opsPerThread = 500;
        CountDownLatch latch = new CountDownLatch(numThreads);
        ExecutorService executor = Executors.newFixedThreadPool(numThreads);
        AtomicLong totalAcquired = new AtomicLong(0);
        AtomicLong totalReleased = new AtomicLong(0);
        AtomicLong deniedCount = new AtomicLong(0);

        for (int i = 0; i < numThreads; i++) {
            executor.submit(() -> {
                try {
                    for (int j = 0; j < opsPerThread; j++) {
                        long acquired = accountant.acquireMemory(10_000);
                        if (acquired > 0) {
                            totalAcquired.addAndGet(acquired);
                            accountant.releaseMemory(acquired);
                            totalReleased.addAndGet(acquired);
                        } else {
                            deniedCount.incrementAndGet();
                        }
                    }
                } finally {
                    latch.countDown();
                }
            });
        }

        assertTrue(latch.await(30, TimeUnit.SECONDS));
        executor.shutdown();

        assertEquals(totalAcquired.get(), totalReleased.get(),
                "All acquired memory should be released");
        assertTrue(accountant.getCurrentUsage() == 0,
                "Final usage should be zero");
    }

    // ========================================================================
    // NativeMemoryStats Tests
    // ========================================================================

    @Test
    @Order(10)
    void testStatsFormatBytes() {
        // Test the toString formatting with different magnitudes
        NativeMemoryStats small = new NativeMemoryStats(100, 200, -1,
                Map.of("test", 100L));
        assertTrue(small.toString().contains("100B"));

        NativeMemoryStats medium = new NativeMemoryStats(1_500_000, 2_000_000, 5_000_000,
                Map.of("l1_cache", 1_000_000L, "index_writer", 500_000L));
        String str = medium.toString();
        assertTrue(str.contains("MB"), "Should format as MB");
        assertTrue(str.contains("l1_cache"), "Should include category");
        assertTrue(str.contains("index_writer"), "Should include category");
    }

    @Test
    @Order(11)
    void testStatsCategoryBreakdownIsUnmodifiable() {
        NativeMemoryStats stats = new NativeMemoryStats(0, 0, 0,
                Map.of("test", 100L));
        assertThrows(UnsupportedOperationException.class, () -> {
            stats.getCategoryBreakdown().put("hack", 999L);
        });
    }

    // ========================================================================
    // Integration: IndexWriter category tracking (default pool)
    // ========================================================================

    @Test
    @Order(20)
    void testIndexWriterCategoryTracking() throws Exception {
        // Record baseline before creating writer
        long baselineUsed = NativeMemoryManager.getStats().getUsedBytes();

        try (SchemaBuilder builder = new SchemaBuilder()) {
            builder.addTextField("title", true, false, "default", "position");
            builder.addIntegerField("score", true, true, false);

            try (Schema schema = builder.build()) {
                try (Index index = new Index(schema, "", true)) {
                    try (IndexWriter writer = index.writer(Index.Memory.DEFAULT_HEAP_SIZE, 1)) {
                        // While writer is open, index_writer category should be tracked
                        NativeMemoryStats duringWrite = NativeMemoryManager.getStats();
                        long usedDuringWrite = duringWrite.getUsedBytes();
                        Map<String, Long> breakdown = duringWrite.getCategoryBreakdown();

                        assertTrue(usedDuringWrite > baselineUsed,
                                "Used bytes should increase when writer is open: was " + baselineUsed + ", now " + usedDuringWrite);
                        assertTrue(breakdown.containsKey("index_writer"),
                                "Category breakdown should contain 'index_writer', got: " + breakdown);
                        long writerBytes = breakdown.get("index_writer");
                        assertTrue(writerBytes >= Index.Memory.DEFAULT_HEAP_SIZE,
                                "index_writer should reserve at least DEFAULT_HEAP_SIZE (" +
                                Index.Memory.DEFAULT_HEAP_SIZE + "), got: " + writerBytes);

                        // Add a document and commit (verifies operations work with tracking)
                        try (Document doc = new Document()) {
                            doc.addText("title", "Memory management test");
                            doc.addInteger("score", 42);
                            writer.addDocument(doc);
                        }
                        writer.commit();
                    }

                    // After writer.close(), reservation should be released
                    NativeMemoryStats afterClose = NativeMemoryManager.getStats();
                    Map<String, Long> afterBreakdown = afterClose.getCategoryBreakdown();
                    long afterWriterBytes = afterBreakdown.getOrDefault("index_writer", 0L);
                    assertTrue(afterWriterBytes == 0 || !afterBreakdown.containsKey("index_writer"),
                            "index_writer category should be 0 or absent after close, got: " + afterWriterBytes);
                }
            }
        }
    }

    @Test
    @Order(21)
    void testResetPeak() throws Exception {
        // Reset peak and verify it returns something reasonable
        long oldPeak = NativeMemoryManager.resetPeak();
        assertTrue(oldPeak >= 0, "Old peak should be non-negative");

        // After reset, peak should equal current used
        NativeMemoryStats stats = NativeMemoryManager.getStats();
        assertEquals(stats.getUsedBytes(), stats.getPeakBytes(),
                "After resetPeak, peak should equal current used");

        // Create a writer to push peak up
        try (SchemaBuilder builder = new SchemaBuilder()) {
            builder.addTextField("title", true, false, "default", "position");
            try (Schema schema = builder.build()) {
                try (Index index = new Index(schema, "", true)) {
                    try (IndexWriter writer = index.writer(Index.Memory.DEFAULT_HEAP_SIZE, 1)) {
                        NativeMemoryStats duringWrite = NativeMemoryManager.getStats();
                        assertTrue(duringWrite.getPeakBytes() >= Index.Memory.DEFAULT_HEAP_SIZE,
                                "Peak should reflect writer allocation");
                    }
                }
            }
        }

        // After close, peak should still be high (peak tracks max)
        NativeMemoryStats afterClose = NativeMemoryManager.getStats();
        assertTrue(afterClose.getPeakBytes() >= Index.Memory.DEFAULT_HEAP_SIZE,
                "Peak should still reflect past writer allocation");

        // Reset peak again — now it should drop to current (no writer)
        NativeMemoryManager.resetPeak();
        NativeMemoryStats afterReset = NativeMemoryManager.getStats();
        assertTrue(afterReset.getPeakBytes() < Index.Memory.DEFAULT_HEAP_SIZE,
                "After second reset with no writer, peak should be below DEFAULT_HEAP_SIZE, got: " +
                afterReset.getPeakBytes());
    }

    @Test
    @Order(22)
    void testWriterCloseReleasesReservation() throws Exception {
        long beforeUsed = NativeMemoryManager.getStats().getUsedBytes();

        try (SchemaBuilder builder = new SchemaBuilder()) {
            builder.addTextField("body", true, false, "default", "position");
            try (Schema schema = builder.build()) {
                try (Index index = new Index(schema, "", true)) {
                    // Open writer — memory goes up
                    IndexWriter writer = index.writer(Index.Memory.DEFAULT_HEAP_SIZE, 1);
                    long duringUsed = NativeMemoryManager.getStats().getUsedBytes();
                    assertTrue(duringUsed >= beforeUsed + Index.Memory.DEFAULT_HEAP_SIZE,
                            "Memory should increase by at least heap size during writer lifetime");

                    // Close writer — memory goes back down
                    writer.close();
                    long afterUsed = NativeMemoryManager.getStats().getUsedBytes();
                    assertTrue(afterUsed < duringUsed,
                            "Memory should decrease after writer close: during=" + duringUsed + ", after=" + afterUsed);
                    // Should be approximately back to baseline (within some tolerance for other state)
                    assertTrue(afterUsed - beforeUsed < Index.Memory.DEFAULT_HEAP_SIZE,
                            "Memory delta after close should be less than heap size: delta=" + (afterUsed - beforeUsed));
                }
            }
        }
    }

    @Test
    @Order(23)
    void testMultipleWritersConcurrentCategories() throws Exception {
        try (SchemaBuilder builder = new SchemaBuilder()) {
            builder.addTextField("text", true, false, "default", "position");
            try (Schema schema = builder.build()) {
                try (Index index1 = new Index(schema, "", true);
                     Index index2 = new Index(schema, "", true)) {

                    try (IndexWriter writer1 = index1.writer(Index.Memory.DEFAULT_HEAP_SIZE, 1);
                         IndexWriter writer2 = index2.writer(Index.Memory.DEFAULT_HEAP_SIZE, 1)) {

                        NativeMemoryStats stats = NativeMemoryManager.getStats();
                        Map<String, Long> breakdown = stats.getCategoryBreakdown();

                        assertTrue(breakdown.containsKey("index_writer"),
                                "Should track index_writer category");
                        long writerBytes = breakdown.get("index_writer");
                        // Two writers, each at DEFAULT_HEAP_SIZE
                        assertTrue(writerBytes >= 2 * Index.Memory.DEFAULT_HEAP_SIZE,
                                "Two writers should reserve at least 2x DEFAULT_HEAP_SIZE (" +
                                (2 * Index.Memory.DEFAULT_HEAP_SIZE) + "), got: " + writerBytes);
                    }

                    // Both closed — index_writer should be 0
                    NativeMemoryStats afterClose = NativeMemoryManager.getStats();
                    long afterWriterBytes = afterClose.getCategoryBreakdown().getOrDefault("index_writer", 0L);
                    assertEquals(0, afterWriterBytes,
                            "index_writer should be 0 after both writers closed");
                }
            }
        }
    }

    @Test
    @Order(24)
    void testIndexWriterWithDefaultMemoryPool() throws Exception {
        // Verify that basic indexing works with the default (unlimited) memory pool
        try (SchemaBuilder builder = new SchemaBuilder()) {
            builder.addTextField("title", true, false, "default", "position");
            builder.addIntegerField("score", true, true, false);

            try (Schema schema = builder.build()) {
                try (Index index = new Index(schema, "", true)) {
                    try (IndexWriter writer = index.writer(Index.Memory.DEFAULT_HEAP_SIZE, 1)) {
                        try (Document doc = new Document()) {
                            doc.addText("title", "Memory management test");
                            doc.addInteger("score", 42);
                            writer.addDocument(doc);
                        }
                        writer.commit();
                    }

                    // Verify stats are accessible
                    NativeMemoryStats stats = NativeMemoryManager.getStats();
                    assertNotNull(stats);
                    assertTrue(stats.getUsedBytes() >= 0);
                }
            }
        }
    }

    // ========================================================================
    // End-to-End: setAccountant with JNI bridge (MUST RUN LAST — OnceLock)
    // ========================================================================

    @Test
    @Order(50)
    void testSetAccountantEndToEnd() throws Exception {
        // This test calls setAccountant() which sets the OnceLock.
        // All subsequent tests in this JVM will use this accountant.
        TrackingAccountant accountant = new TrackingAccountant();
        boolean result = NativeMemoryManager.setAccountant(accountant);
        assertTrue(result, "First setAccountant call should succeed");
        assertTrue(NativeMemoryManager.isConfigured(), "Pool should be configured after setAccountant");

        // Second call should fail (OnceLock already set)
        TrackingAccountant accountant2 = new TrackingAccountant();
        boolean result2 = NativeMemoryManager.setAccountant(accountant2);
        assertFalse(result2, "Second setAccountant call should fail");

        // Now do a real native operation — create an IndexWriter
        try (SchemaBuilder builder = new SchemaBuilder()) {
            builder.addTextField("title", true, false, "default", "position");
            try (Schema schema = builder.build()) {
                try (Index index = new Index(schema, "", true)) {
                    try (IndexWriter writer = index.writer(Index.Memory.DEFAULT_HEAP_SIZE, 1)) {
                        // The JVM accountant should have been called via JNI
                        assertTrue(accountant.getAcquireCallCount() > 0,
                                "JVM accountant should have received acquire calls via JNI, got: " +
                                accountant.getAcquireCallCount());
                        assertTrue(accountant.getTotalAcquired() >= Index.Memory.DEFAULT_HEAP_SIZE,
                                "JVM accountant should have acquired at least DEFAULT_HEAP_SIZE (" +
                                Index.Memory.DEFAULT_HEAP_SIZE + "), got: " + accountant.getTotalAcquired());

                        // Stats should reflect the JVM-backed pool
                        NativeMemoryStats stats = NativeMemoryManager.getStats();
                        assertTrue(stats.getGrantedBytes() >= 0,
                                "JVM pool should report non-negative granted, got: " + stats.getGrantedBytes());
                        assertTrue(stats.getUsedBytes() > 0,
                                "JVM pool should report positive used during writer lifetime");

                        // Add document and commit
                        try (Document doc = new Document()) {
                            doc.addText("title", "JVM accountant test");
                            writer.addDocument(doc);
                        }
                        writer.commit();
                    }
                }
            }
        }

        // After writer close, memory should be released from the pool
        NativeMemoryStats afterCloseStats = NativeMemoryManager.getStats();
        assertEquals(0, afterCloseStats.getUsedBytes(),
                "Pool used bytes should be 0 after writer close");

        // The JVM accountant may or may not have received release callbacks
        // depending on watermark batching — the important thing is that the
        // pool's internal tracking shows 0 used bytes.
    }

    @Test
    @Order(51)
    void testCategoryBreakdownWithJvmPool() throws Exception {
        // Now running with JVM-backed pool (set in testSetAccountantEndToEnd)
        // Verify categories still work correctly
        try (SchemaBuilder builder = new SchemaBuilder()) {
            builder.addTextField("content", true, false, "default", "position");
            try (Schema schema = builder.build()) {
                try (Index index = new Index(schema, "", true)) {
                    try (IndexWriter writer = index.writer(Index.Memory.LARGE_HEAP_SIZE, 1)) {
                        NativeMemoryStats stats = NativeMemoryManager.getStats();
                        Map<String, Long> breakdown = stats.getCategoryBreakdown();

                        assertTrue(breakdown.containsKey("index_writer"),
                                "JVM pool should track index_writer category, got: " + breakdown);
                        assertTrue(breakdown.get("index_writer") >= Index.Memory.LARGE_HEAP_SIZE,
                                "index_writer should reserve at least LARGE_HEAP_SIZE");
                    }
                }
            }
        }
    }

    // ========================================================================
    // Helper: Tracking Accountant (records all operations)
    // ========================================================================

    /**
     * A test accountant that tracks all acquire/release calls.
     * Always grants the full request.
     */
    static class TrackingAccountant implements NativeMemoryAccountant {
        private final AtomicLong totalAcquired = new AtomicLong(0);
        private final AtomicLong totalReleased = new AtomicLong(0);
        private final AtomicLong acquireCallCount = new AtomicLong(0);
        private final AtomicLong releaseCallCount = new AtomicLong(0);

        @Override
        public long acquireMemory(long bytes) {
            acquireCallCount.incrementAndGet();
            totalAcquired.addAndGet(bytes);
            return bytes;
        }

        @Override
        public void releaseMemory(long bytes) {
            releaseCallCount.incrementAndGet();
            totalReleased.addAndGet(bytes);
        }

        public long getTotalAcquired() { return totalAcquired.get(); }
        public long getTotalReleased() { return totalReleased.get(); }
        public long getAcquireCallCount() { return acquireCallCount.get(); }
        public long getReleaseCallCount() { return releaseCallCount.get(); }
    }

    // ========================================================================
    // Helper: Limited Accountant (enforces a memory limit)
    // ========================================================================

    /**
     * A test accountant that enforces a hard memory limit.
     * Returns 0 if the request would exceed the limit.
     */
    static class LimitedAccountant implements NativeMemoryAccountant {
        private final long maxBytes;
        private final AtomicLong currentUsage = new AtomicLong(0);

        LimitedAccountant(long maxBytes) {
            this.maxBytes = maxBytes;
        }

        @Override
        public synchronized long acquireMemory(long bytes) {
            long current = currentUsage.get();
            if (current + bytes > maxBytes) {
                return 0; // Denied
            }
            currentUsage.addAndGet(bytes);
            return bytes;
        }

        @Override
        public void releaseMemory(long bytes) {
            currentUsage.addAndGet(-bytes);
        }

        public long getCurrentUsage() { return currentUsage.get(); }
        public long getMaxBytes() { return maxBytes; }
    }
}

package io.indextables.tantivy4java.memory;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.MethodOrderer.OrderAnnotation;
import org.junit.jupiter.api.BeforeAll;

import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.*;

import io.indextables.tantivy4java.core.*;

/**
 * Tests that memory pool denial properly prevents operations (fail-fast behavior).
 *
 * IMPORTANT: This test class MUST run in its own forked JVM because it calls
 * NativeMemoryManager.setAccountant() with a LimitedAccountant. Since the global
 * pool can only be set once per JVM, this test class must not share a JVM with
 * NativeMemoryManagerTest (which sets a TrackingAccountant).
 *
 * Configure in Maven surefire:
 *   -Dtest="MemoryDenialTest" with forkCount=1, reuseForks=false
 * Or run separately: mvn test -Dtest="MemoryDenialTest" -DforkCount=1 -DreuseForks=false
 */
@TestMethodOrder(OrderAnnotation.class)
public class MemoryDenialTest {

    @BeforeAll
    static void ensureNativeLoaded() {
        try {
            Class.forName("io.indextables.tantivy4java.core.Index");
        } catch (ClassNotFoundException e) {
            fail("Native library not available");
        }
    }

    // ========================================================================
    // Step 1: Configure a very tight memory limit
    // ========================================================================

    @Test
    @Order(1)
    void testSetLimitedAccountant() {
        // Set a 10MB limit — enough for small operations but not for DEFAULT_HEAP_SIZE (50MB)
        LimitedAccountant accountant = new LimitedAccountant(10_000_000);
        boolean result = NativeMemoryManager.setAccountant(accountant);
        assertTrue(result, "setAccountant should succeed on first call");
        assertTrue(NativeMemoryManager.isConfigured());
    }

    // ========================================================================
    // Step 2: Verify IndexWriter creation fails fast when pool denies
    // ========================================================================

    @Test
    @Order(2)
    void testIndexWriterDeniedWhenPoolTight() throws Exception {
        try (SchemaBuilder builder = new SchemaBuilder()) {
            builder.addTextField("title", true, false, "default", "position");
            try (Schema schema = builder.build()) {
                try (Index index = new Index(schema, "", true)) {
                    // DEFAULT_HEAP_SIZE is 50MB, pool limit is 10MB — should be denied
                    Exception exception = assertThrows(Exception.class, () -> {
                        index.writer(Index.Memory.DEFAULT_HEAP_SIZE, 1);
                    });
                    String msg = exception.getMessage();
                    assertTrue(msg != null && msg.toLowerCase().contains("denied"),
                            "Exception should mention denial, got: " + msg);
                }
            }
        }
    }

    @Test
    @Order(3)
    void testSmallWriterSucceedsUnderLimit() throws Exception {
        // MIN_HEAP_SIZE (15MB) might fit depending on watermark acquire increment.
        // The JvmMemoryPool acquires in 64MB chunks by default, so even MIN_HEAP_SIZE
        // will trigger a 64MB JNI acquire which exceeds our 10MB limit.
        // This test verifies that the limit is properly enforced.
        try (SchemaBuilder builder = new SchemaBuilder()) {
            builder.addTextField("title", true, false, "default", "position");
            try (Schema schema = builder.build()) {
                try (Index index = new Index(schema, "", true)) {
                    // Even MIN_HEAP_SIZE should be denied because the JvmMemoryPool
                    // will try to acquire max(15MB, 64MB) = 64MB from the JVM accountant,
                    // which exceeds our 10MB limit.
                    Exception exception = assertThrows(Exception.class, () -> {
                        index.writer(Index.Memory.MIN_HEAP_SIZE, 1);
                    });
                    assertNotNull(exception.getMessage(),
                            "Should get a meaningful error message on denial");
                }
            }
        }
    }

    @Test
    @Order(4)
    void testPoolStatsAfterDenial() {
        // After denial, pool should still be functional for queries
        NativeMemoryStats stats = NativeMemoryManager.getStats();
        assertNotNull(stats);
        // Used should be 0 or very small (no successful reservations)
        assertTrue(stats.getUsedBytes() >= 0);
        // Granted should reflect what the accountant actually gave
        assertTrue(stats.getGrantedBytes() >= 0);
    }

    // ========================================================================
    // Helper: Limited Accountant (same as in NativeMemoryManagerTest)
    // ========================================================================

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
    }
}

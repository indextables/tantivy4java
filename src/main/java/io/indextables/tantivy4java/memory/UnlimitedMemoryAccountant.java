package io.indextables.tantivy4java.memory;

/**
 * Default memory accountant that always grants the full requested amount.
 * Used when no external memory manager is configured (backward-compatible default).
 */
public class UnlimitedMemoryAccountant implements NativeMemoryAccountant {

    @Override
    public long acquireMemory(long bytes) {
        return bytes;
    }

    @Override
    public void releaseMemory(long bytes) {
        // No-op: unlimited pool doesn't track externally
    }
}

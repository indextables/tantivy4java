package io.indextables.tantivy4java.memory;

/**
 * Interface for external memory managers to coordinate with native Rust allocations.
 *
 * <p>Implementations must be thread-safe. The native layer may call these methods
 * from multiple threads concurrently.
 *
 * <p>Example: integrate with Spark's TaskMemoryManager:
 * <pre>{@code
 * public class SparkMemoryAccountant implements NativeMemoryAccountant {
 *     private final TaskMemoryManager taskMemoryManager;
 *     private final MemoryConsumer consumer;
 *
 *     public long acquireMemory(long bytes) {
 *         return taskMemoryManager.acquireExecutionMemory(bytes, consumer);
 *     }
 *
 *     public void releaseMemory(long bytes) {
 *         taskMemoryManager.releaseExecutionMemory(bytes, consumer);
 *     }
 * }
 * }</pre>
 */
public interface NativeMemoryAccountant {

    /**
     * Request memory from the external manager.
     *
     * @param bytes requested number of bytes
     * @return actual bytes granted (may be less than requested; 0 = denied)
     */
    long acquireMemory(long bytes);

    /**
     * Release previously acquired memory back to the external manager.
     *
     * @param bytes number of bytes to release
     */
    void releaseMemory(long bytes);
}

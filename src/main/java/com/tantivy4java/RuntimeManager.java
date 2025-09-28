package com.tantivy4java;

/**
 * Runtime Manager for controlling the native Tokio runtime lifecycle.
 *
 * This class provides methods to gracefully shutdown the native async runtime,
 * preventing "dispatch task is gone" errors when the JVM terminates while
 * async operations are still running.
 */
public class RuntimeManager {

    static {
        // Ensure native library is loaded
        System.loadLibrary("tantivy4java");
    }

    /**
     * Gracefully shutdown the native Tokio runtime with a timeout.
     *
     * This method should be called during application shutdown or test cleanup
     * to ensure all pending async operations complete before the runtime is dropped.
     *
     * @param timeoutSeconds Maximum time to wait for pending operations to complete
     */
    public static void shutdownGracefully(long timeoutSeconds) {
        shutdownGracefullyNative(timeoutSeconds);
    }

    /**
     * Gracefully shutdown the native Tokio runtime with a default 5-second timeout.
     */
    public static void shutdownGracefully() {
        shutdownGracefully(5L);
    }

    /**
     * Register a JVM shutdown hook to automatically shutdown the runtime
     * when the JVM terminates.
     */
    public static void registerShutdownHook() {
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.err.println("🛑 RuntimeManager: JVM shutdown detected, shutting down Tokio runtime...");
            shutdownGracefully(3L); // 3 second timeout for shutdown hooks
            System.err.println("✅ RuntimeManager: Tokio runtime shutdown completed");
        }, "tantivy4java-shutdown"));
    }

    // Native method declaration
    private static native void shutdownGracefullyNative(long timeoutSeconds);
}
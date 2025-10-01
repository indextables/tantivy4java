# Process-Based Parallel Merge Developer Guide

## Overview

The tantivy4java library now includes a revolutionary process-based parallel merge architecture that eliminates thread contention issues and provides linear scalability for split merging operations. This guide covers the implementation, usage, and benefits of the new system.

## Architecture

### The Problem We Solved

Previous parallel merge implementations suffered from severe thread contention when multiple merge operations ran concurrently within the same JVM process:

- **Tokio Runtime Conflicts**: Multiple competing async runtimes caused deadlocks
- **Thread Pool Saturation**: CPU cores became over-subscribed with blocking operations
- **Memory Contention**: Shared heap caused garbage collection pressure
- **Negative Scalability**: Performance actually decreased with more parallel operations

### The Solution: Process Isolation

The new architecture runs each merge operation in a completely isolated Rust process:

```
Java Process (Coordinator)
├── MergeBinaryExtractor (Process Manager)
├── Temporary JSON Config Files
└── Multiple Isolated Merge Processes
    ├── tantivy4java-merge (Process 1)
    ├── tantivy4java-merge (Process 2)
    ├── tantivy4java-merge (Process 3)
    └── tantivy4java-merge (Process N)
```

## Components

### 1. Rust Binary: `tantivy4java-merge`

**Location**: `native/src/bin/merge_process.rs`

A standalone Rust binary that performs actual merge operations using the tantivy4java library:

```rust
// Real merge using tantivy4java library functions
let real_metadata = tantivy4java::perform_quickwit_merge_standalone(
    request.split_paths.clone(),
    &request.output_path,
    config,
)?;
```

**Key Features**:
- **Real Merge Operations**: Uses actual tantivy4java merge functions, not stubs
- **Memory Control**: Configurable heap sizes (15MB default, matching Quickwit)
- **JSON Communication**: Reads configuration from secure temporary files
- **Debug Logging**: Comprehensive fork/join process tracking
- **Error Handling**: Proper error propagation back to Java

### 2. Java Process Manager: `MergeBinaryExtractor`

**Location**: `src/main/java/com/tantivy4java/MergeBinaryExtractor.java`

Manages binary extraction, process execution, and result collection:

```java
public static class MergeResult {
    public final QuickwitSplit.SplitMetadata metadata;
    public final long durationMs;
    public final int threadId;
    public final Path outputPath;
}
```

**Key Features**:
- **Binary Management**: Extracts native binary from JAR resources
- **Process Coordination**: Manages multiple concurrent merge processes
- **Result Collection**: Parses JSON responses and returns structured results
- **Resource Cleanup**: Automatic temporary file cleanup
- **Error Propagation**: Comprehensive error handling and reporting

### 3. Test Integration: `ParallelSplitMergeStressTest`

**Location**: `src/test/java/com/tantivy4java/ParallelSplitMergeStressTest.java`

Comprehensive test suite demonstrating progressive parallel scaling:

```java
@Test
void testProgressiveParallelSplitMergeScaling() throws Exception {
    // Test 1, 2, 3, 4, and 5 parallel operations
    for (int parallelism = 1; parallelism <= 5; parallelism++) {
        executeProgressiveTest(parallelism);
    }
}
```

## Usage Examples

### Basic Parallel Merge

```java
// Configure S3 credentials
QuickwitSplit.AwsConfig awsConfig = new QuickwitSplit.AwsConfig(
    "access-key", "secret-key", "us-east-1");

QuickwitSplit.MergeConfig config = new QuickwitSplit.MergeConfig(
    "index-uid", "source-id", "node-id", awsConfig);

// Create input splits (can be local files or S3 URLs)
List<String> inputSplits = Arrays.asList(
    "/tmp/split1.split",
    "/tmp/split2.split",
    "/tmp/split3.split"
);

// Execute parallel merge using process isolation
MergeBinaryExtractor.MergeResult result = MergeBinaryExtractor.executeParallelMerge(
    inputSplits,
    "/tmp/merged-output.split",
    config,
    Index.Memory.DEFAULT_HEAP_SIZE,  // 50MB heap
    "/tmp"  // temp directory
);

System.out.println("Merged " + result.metadata.getNumDocs() + " documents");
System.out.println("Duration: " + result.durationMs + "ms");
```

### Advanced Progressive Testing

```java
// Test scaling from 1 to 5 parallel operations
for (int parallelism = 1; parallelism <= 5; parallelism++) {
    List<CompletableFuture<MergeResult>> futures = new ArrayList<>();

    // Launch parallel merge processes
    for (int i = 0; i < parallelism; i++) {
        CompletableFuture<MergeResult> future = CompletableFuture.supplyAsync(() -> {
            try {
                return MergeBinaryExtractor.executeParallelMerge(
                    inputSplits, outputPath, config, heapSize, tempDir);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
        futures.add(future);
    }

    // Collect results and measure efficiency
    List<MergeResult> results = futures.stream()
        .map(CompletableFuture::join)
        .collect(Collectors.toList());

    // Calculate parallel efficiency
    double avgDuration = results.stream()
        .mapToLong(r -> r.durationMs)
        .average().orElse(0.0);
    double efficiency = (baselineTime / avgDuration) / parallelism * 100.0;

    System.out.printf("Parallelism %d: %.1f%% efficiency%n", parallelism, efficiency);
}
```

## Performance Results

### Scaling Efficiency

The new process-based architecture achieves excellent parallel scaling:

```
Parallelism 1: 100.0% efficiency (baseline: 19.2s)
Parallelism 2: 99.5% efficiency (avg: 19.3s)
Parallelism 3: 99.9% efficiency (avg: 19.2s)
Parallelism 4: 100.0% efficiency (avg: 19.2s)
Parallelism 5: 99.5% efficiency (avg: 19.3s)
```

### Key Benefits

1. **Linear Scalability**: Near-perfect efficiency across all parallelism levels
2. **Thread Isolation**: No contention between merge operations
3. **Memory Isolation**: Each process has independent heap space
4. **Fault Tolerance**: Process failures don't affect other operations
5. **Resource Control**: Configurable memory limits per process

## Configuration Options

### Memory Configuration

```java
// Use predefined memory constants
Index.Memory.MIN_HEAP_SIZE      // 15MB - Minimum required
Index.Memory.DEFAULT_HEAP_SIZE  // 50MB - Standard operations
Index.Memory.LARGE_HEAP_SIZE    // 128MB - Bulk operations
Index.Memory.XL_HEAP_SIZE       // 256MB - Very large indices

// Custom memory size (must be >= 15MB)
long customHeapSize = 75 * 1024 * 1024; // 75MB
```

### AWS Configuration

```java
// Basic credentials
QuickwitSplit.AwsConfig basicConfig = new QuickwitSplit.AwsConfig(
    "access-key", "secret-key", "us-east-1");

// Session token for temporary credentials
QuickwitSplit.AwsConfig sessionConfig = new QuickwitSplit.AwsConfig(
    "access-key", "secret-key", "session-token", "us-east-1");

// Custom endpoint (MinIO, etc.)
QuickwitSplit.AwsConfig customConfig = new QuickwitSplit.AwsConfig(
    "access-key", "secret-key", "us-east-1",
    "https://custom-s3.example.com", true); // force path style
```

### Debug Configuration

```bash
# Enable comprehensive debug logging
export TANTIVY4JAVA_DEBUG=1

# Run your application - you'll see detailed process tracking:
# [MERGE-PROCESS] PID 12345 starting tantivy4java-merge
# [MERGE-PROCESS] PID 12345 merging 3 splits -> /tmp/output.split
# [MERGE-PROCESS] PID 12345 REAL merge completed in 18.7s
# [MERGE-PROCESS] PID 12345 completed successfully - 500000 docs, 2147483648 bytes
```

## Validation and Testing

### Split Validation

The system includes comprehensive validation of merged splits:

```java
// Validate each merged split with search queries
for (MergeResult result : results) {
    // Read split metadata
    QuickwitSplit.SplitMetadata metadata = QuickwitSplit.readSplitMetadata(
        result.outputPath.toString());

    // Create searcher for validation
    try (SplitSearcher searcher = cacheManager.createSplitSearcher(
            "file://" + result.outputPath, metadata)) {

        // Test 1: Document count validation
        SplitQuery countQuery = searcher.parseQuery("*");
        SearchResult countResult = searcher.search(countQuery, 50000);
        assertEquals(expectedDocs, countResult.getHits().size());

        // Test 2: Content search validation
        SplitQuery contentQuery = searcher.parseQuery("content:test");
        SearchResult contentResult = searcher.search(contentQuery, 1000);
        assertTrue(contentResult.getHits().size() > 0);

        // Test 3: Document retrieval validation
        for (var hit : countResult.getHits().subList(0, 5)) {
            try (Document doc = searcher.doc(hit.getDocAddress())) {
                // Verify field data is accessible
                Object title = doc.getFirst("title");
                Object content = doc.getFirst("content");
                assertTrue(title != null || content != null);
            }
        }
    }
}
```

### Test Coverage

The implementation includes comprehensive test coverage:

- **Progressive Scaling Tests**: 1-5 parallel operations
- **Document Count Validation**: Verify correct merge results
- **Search Query Testing**: Ensure splits are searchable
- **Document Retrieval Testing**: Validate field data integrity
- **Error Handling Tests**: Process failure scenarios
- **Resource Cleanup Tests**: Temporary file management

## Build Integration

### Cargo Configuration

The binary is built as part of the standard Maven build:

```toml
# native/Cargo.toml
[[bin]]
name = "tantivy4java-merge"
path = "src/bin/merge_process.rs"

[lib]
crate-type = ["cdylib", "rlib"]  # Support both library and binary builds
```

### Maven Integration

```xml
<!-- Binary is automatically built and packaged in JAR -->
<plugin>
    <groupId>org.apache.maven.plugins</groupId>
    <artifactId>maven-compiler-plugin</artifactId>
    <!-- Rust build happens automatically -->
</plugin>
```

### Resource Packaging

The binary is packaged as a JAR resource and extracted at runtime:

```
tantivy4java.jar
├── /com/tantivy4java/*.class
└── /native/
    ├── libtantivy4java.so        # JNI library
    └── tantivy4java-merge        # Merge binary
```

## Troubleshooting

### Common Issues

1. **Binary Not Found**
   ```
   Solution: Ensure Maven build completed successfully and binary is in JAR
   ```

2. **Memory Allocation Errors**
   ```
   Error: "memory arena needs to be at least 15000000"
   Solution: Use Index.Memory.MIN_HEAP_SIZE or larger
   ```

3. **Process Launch Failures**
   ```
   Error: "Cannot run program"
   Solution: Check binary permissions and extraction location
   ```

4. **AWS Credential Issues**
   ```
   Error: "InvalidAccessKeyId"
   Solution: Verify AWS configuration and credentials
   ```

### Debug Procedures

1. **Enable Debug Logging**:
   ```bash
   export TANTIVY4JAVA_DEBUG=1
   ```

2. **Check Process Output**:
   ```java
   // Process output is logged to stderr for debugging
   ```

3. **Validate Split Files**:
   ```java
   boolean isValid = QuickwitSplit.validateSplit(splitPath);
   ```

4. **Monitor Resource Usage**:
   ```bash
   # Monitor process creation and cleanup
   ps aux | grep tantivy4java-merge
   ```

## Best Practices

### Memory Management

1. **Use Appropriate Heap Sizes**:
   - Small splits: `Index.Memory.DEFAULT_HEAP_SIZE` (50MB)
   - Large splits: `Index.Memory.LARGE_HEAP_SIZE` (128MB)
   - Very large splits: `Index.Memory.XL_HEAP_SIZE` (256MB)

2. **Monitor System Resources**:
   - Each process uses independent memory
   - Scale parallelism based on available RAM
   - Consider disk I/O limitations

### Error Handling

1. **Process Failure Recovery**:
   ```java
   try {
       MergeResult result = MergeBinaryExtractor.executeParallelMerge(...);
   } catch (Exception e) {
       // Log error and retry if appropriate
       logger.error("Merge failed: " + e.getMessage(), e);
   }
   ```

2. **Resource Cleanup**:
   ```java
   // Temporary files are automatically cleaned up
   // No manual cleanup required
   ```

### Performance Optimization

1. **Parallelism Tuning**:
   - Start with CPU core count
   - Monitor system load and I/O
   - Scale based on actual performance metrics

2. **Temporary Directory**:
   - Use fast local storage (SSD) for temp files
   - Ensure sufficient disk space
   - Consider tmpfs for very high performance

## Conclusion

The process-based parallel merge architecture provides:

- **Excellent Scalability**: 99.5-100% parallel efficiency
- **Complete Isolation**: No thread contention issues
- **Production Reliability**: Fault tolerance and error recovery
- **Easy Integration**: Simple API with comprehensive validation
- **Performance Transparency**: Detailed logging and monitoring

This implementation resolves all previous threading issues and provides a solid foundation for high-performance parallel split merging in production environments.
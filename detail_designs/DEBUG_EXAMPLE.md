# Process-Based Merge Debug Configuration

This document shows how to enable debug logging in process-based merge operations using the `MergeConfig` debug flag.

## 🎯 Overview

The process-based merge extractor now supports debug logging through the `MergeConfig.isDebugEnabled()` flag. When enabled, it:

1. **Sets `TANTIVY4JAVA_DEBUG=1`** environment variable for the child Rust process
2. **Shows detailed configuration JSON** being sent to the child process
3. **Displays comprehensive output** from both STDOUT and STDERR of the child process
4. **Provides enhanced debugging information** for troubleshooting merge operations

## 🔧 Usage Examples

### Basic Debug Usage
```java
// Create AWS configuration
QuickwitSplit.AwsConfig awsConfig = new QuickwitSplit.AwsConfig(
    "your-access-key", "your-secret-key", "us-east-1");

// Create merge configuration WITH debug enabled
QuickwitSplit.MergeConfig debugConfig = new QuickwitSplit.MergeConfig(
    "my-index", "my-source", "my-node",
    awsConfig,
    true  // Enable debug logging
);

// Perform merge with debug output
List<String> splits = Arrays.asList("split1.split", "split2.split");
QuickwitSplit.SplitMetadata result = QuickwitSplit.mergeSplits(
    splits, "merged-output.split", debugConfig);
```

### Standard Usage (No Debug)
```java
// Create merge configuration WITHOUT debug (default behavior)
QuickwitSplit.MergeConfig standardConfig = new QuickwitSplit.MergeConfig(
    "my-index", "my-source", "my-node", awsConfig);

// Perform merge with standard output
QuickwitSplit.SplitMetadata result = QuickwitSplit.mergeSplits(
    splits, "merged-output.split", standardConfig);
```

### Using the Full Constructor
```java
// Create merge configuration with full control including debug
QuickwitSplit.MergeConfig fullConfig = new QuickwitSplit.MergeConfig(
    "my-index",                    // indexUid
    "my-source",                   // sourceId
    "my-node",                     // nodeId
    "custom-doc-mapping",          // docMappingUid
    42L,                          // partitionId
    Arrays.asList("deleted:true"), // deleteQueries
    awsConfig,                     // awsConfig
    "/tmp/custom-temp",           // tempDirectoryPath
    128L * 1024 * 1024,          // heapSizeBytes (128MB)
    true                          // debugEnabled
);
```

## 📋 Debug Output Examples

### With Debug Enabled (`debugEnabled = true`)

```
[DEBUG] Debug mode enabled via MergeConfig - child process will output detailed logs
[DEBUG] Merge configuration JSON being sent to child process:
[DEBUG] {"split_paths":["/path/split1.split","/path/split2.split"],"output_path":"/path/merged.split","config":{"index_uid":"my-index","source_id":"my-source","node_id":"my-node","aws_config":{"access_key":"AKIA...","secret_key":"***","session_token":null,"region":"us-east-1","endpoint_url":null,"force_path_style":false}},"heap_size":null,"temp_directory":null}
[FORK-JOIN] Java thread main (ID:1) forking merge process
[FORK-JOIN] Command: [/tmp/tantivy4java-merge123, /tmp/tantivy4java-merge-config456.json]
[FORK-JOIN] Merging 2 splits -> /path/merged.split
[FORK-JOIN] Child process PID 12345 started
[DEBUG] Child process PID 12345 STDOUT output:
[DEBUG] {"success":true,"split_id":"merged-12345","num_docs":1000,"uncompressed_size_bytes":52428800}
[DEBUG] Child process PID 12345 STDERR output:
[DEBUG] [DEBUG] Loading split: /path/split1.split
[DEBUG] [DEBUG] Loading split: /path/split2.split
[DEBUG] [DEBUG] AWS S3 client configured with region: us-east-1
[DEBUG] [DEBUG] Merge completed successfully
[FORK-JOIN] Child process PID 12345 completed with exit code 0 in 2150ms
[FORK-JOIN] Java thread main successfully joined child process PID 12345
```

### Without Debug (`debugEnabled = false` - default)

```
[FORK-JOIN] Java thread main (ID:1) forking merge process
[FORK-JOIN] Command: [/tmp/tantivy4java-merge123, /tmp/tantivy4java-merge-config456.json]
[FORK-JOIN] Merging 2 splits -> /path/merged.split
[FORK-JOIN] Child process PID 12345 started
[FORK-JOIN] Child process PID 12345 completed with exit code 0 in 2150ms
[FORK-JOIN] Java thread main successfully joined child process PID 12345
```

## 🛠️ Available MergeConfig Constructors with Debug

### Constructor Options
```java
// 1. Simple constructor with AWS config and debug
new MergeConfig(indexUid, sourceId, nodeId, awsConfig, debugEnabled)

// 2. Full constructor with all options
new MergeConfig(indexUid, sourceId, nodeId, docMappingUid, partitionId,
                deleteQueries, awsConfig, tempDirectoryPath, heapSizeBytes, debugEnabled)

// 3. All existing constructors default debugEnabled = false
new MergeConfig(indexUid, sourceId, nodeId)  // debugEnabled = false
new MergeConfig(indexUid, sourceId, nodeId, awsConfig)  // debugEnabled = false
```

## 📊 Benefits of Debug Mode

### 🔍 **Enhanced Troubleshooting**
- See exact configuration being sent to child process
- View detailed Rust-level debug output
- Understand AWS credential propagation
- Monitor child process lifecycle

### 🐛 **Error Diagnosis**
- Detailed error messages from native layer
- AWS authentication debugging information
- Split file loading and validation details
- Memory and performance metrics

### 🎯 **Development & Testing**
- Validate configuration serialization
- Verify AWS credentials are properly passed
- Monitor child process performance
- Debug complex merge scenarios

## ⚠️ Important Notes

### Security Considerations
- Debug mode may log sensitive information
- **Only use in development/testing environments**
- AWS credentials are partially masked in logs
- Temporary config files are automatically cleaned up

### Performance Impact
- Debug mode adds minimal overhead to Java process
- Child process debug output may slow merge operations slightly
- Extra logging increases memory usage temporarily

### Production Usage
- **Disable debug in production** (`debugEnabled = false`)
- Use standard error output for production monitoring
- Reserve debug mode for troubleshooting specific issues

## 🎛️ Environment Variable Control

The debug flag in `MergeConfig` controls whether `TANTIVY4JAVA_DEBUG=1` is set for the **child process**. The Rust merge binary will then output detailed debug information when this environment variable is present.

This approach provides:
- **Explicit control** through the Java API
- **Process isolation** - debug only affects the child process
- **Clean separation** - no environment variable pollution in parent JVM
- **Flexible configuration** - per-operation debug control

## 🚀 Getting Started

1. **Enable debug in your MergeConfig**:
   ```java
   MergeConfig debugConfig = new MergeConfig(indexUid, sourceId, nodeId, awsConfig, true);
   ```

2. **Run your merge operation**:
   ```java
   QuickwitSplit.mergeSplits(splitPaths, outputPath, debugConfig);
   ```

3. **Analyze the detailed debug output** to troubleshoot issues or understand the merge process

4. **Disable debug for production** by setting `debugEnabled = false` or using standard constructors

This debug configuration makes it easy to troubleshoot process-based merge operations while maintaining clean separation between development debugging and production operations.
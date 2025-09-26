# Split Merge Concurrency Issues Analysis

## 🚨 **IDENTIFIED CONCURRENCY DEFECTS**

Based on analysis of the merge implementation in `quickwit_split.rs`, there are two critical concurrency issues affecting high-concurrency merge operations:

### **Issue 1: File Not Found Error for metadata.json (Race Condition)**

**Root Cause**: Temporary directory cleanup race condition during concurrent merges.

**Problem Location**: Lines 1210-1336 and 1479-1488 in `merge_splits_impl()`
```rust
// Each merge creates multiple temporary directories
let temp_extract_dir = temp::TempDir::new()?;           // Line 1210
let temp_extract_path = temp_extract_dir.path();

// Later cleanup races with ongoing operations
std::fs::remove_dir_all(&output_temp_dir).unwrap_or_else(|e| {  // Line 1480
    debug_log!("Warning: Could not clean up output temp directory: {}", e);
});
```

**Race Condition Details**:
1. **Directory Name Collision**: Multiple concurrent merges create temp directories with similar patterns
2. **Premature Cleanup**: One merge operation cleans up directories while another merge is still accessing them
3. **metadata.json Access**: When one merge creates shadowing meta.json directory, another concurrent merge may access a cleaned-up directory

**Specific Failure Mode**:
- Merge A creates `temp_extract_dir_12345` and extracts split, creates shadowing meta.json
- Merge B starts and attempts to access similar temp directory or shared shadowing directory
- Merge A completes and cleans up temp directories
- Merge B fails with "metadata.json not found" because its temporary directory was cleaned up

### **Issue 2: Non-Linear Scaling Due to Locking Bottlenecks**

**Root Cause**: Sequential operations and shared resource contention during merge phases.

**Bottleneck Locations**:

1. **Sequential Split Extraction** (Lines 1206-1337):
```rust
for (i, split_url) in split_urls.iter().enumerate() {
    // Each split extracted sequentially - no parallelization
    let temp_extract_dir = temp::TempDir::new()?;
    extract_split_to_directory_impl(&temp_split_path, temp_extract_path)?;
    // Full file I/O per split in sequence
}
```

2. **Single-Threaded Runtime** (Lines 1192-1195):
```rust
let runtime = tokio::runtime::Builder::new_current_thread()  // Single thread!
    .enable_all()
    .build()?;
```

3. **Unified Index Writer Lock** (Lines 1615-1620):
```rust
let mut index_writer: IndexWriter = union_index.writer_with_num_threads(1, 15_000_000)?;
// Single writer with num_threads=1 prevents parallel segment operations
```

4. **Directory Stack Contention** (Lines 1371-1383):
```rust
let union_directory = UnionDirectory::union_of(directory_stack);
// All merges create union directories with potential shared access patterns
```

## 🔧 **DETAILED TECHNICAL ISSUES**

### **Temporary Directory Management Problems**

**Current Implementation**:
- Creates unique temp directories per split but with predictable patterns
- Cleanup happens at function exit, potentially interfering with other operations
- No synchronization between concurrent merge operations

**Race Condition Timeline**:
```
Time 1: Merge A creates temp_dir_A, extracts split_1 → meta.json
Time 2: Merge B creates temp_dir_B, starts extracting split_1
Time 3: Merge A completes, cleans up temp_dir_A (including meta.json references)
Time 4: Merge B tries to access meta.json → FILE NOT FOUND ERROR
```

### **Serialization Bottlenecks**

**Sequential S3 Downloads**:
- Each split downloaded sequentially, even for different merge operations
- No connection pooling or parallel download capabilities
- Temporary file creation serialized

**Index Operations**:
- Single-threaded Tokio runtime limits async parallelism
- IndexWriter limited to 1 thread prevents concurrent segment operations
- UnionDirectory creation not parallelized

**Memory Competition**:
- All merges use 15MB heap limit from same memory pool
- No resource isolation between concurrent merge operations

## 🚀 **RECOMMENDED FIXES**

### **Fix 1: Eliminate Temporary Directory Race Conditions**

**Strategy**: Process-scoped temporary directories with proper cleanup coordination

```rust
// Use process-specific temp directory naming
fn create_merge_temp_dir(merge_id: &str, split_index: usize) -> Result<temp::TempDir> {
    let temp_dir_name = format!("tantivy4java_merge_{}_{}_split_{}",
                                std::process::id(), merge_id, split_index);
    temp::Builder::new()
        .prefix(&temp_dir_name)
        .tempdir()
        .map_err(|e| anyhow!("Failed to create temp directory: {}", e))
}

// Defer cleanup until all references released
struct MergeContext {
    merge_id: String,
    temp_dirs: Vec<temp::TempDir>,
    cleanup_guard: Arc<Mutex<()>>,
}

impl Drop for MergeContext {
    fn drop(&mut self) {
        let _guard = self.cleanup_guard.lock();
        // Cleanup only when all operations complete
    }
}
```

### **Fix 2: Implement Parallel Merge Architecture**

**Strategy**: Multi-threaded processing with resource isolation

```rust
// Use multi-threaded runtime for parallel operations
let runtime = tokio::runtime::Builder::new_multi_thread()
    .worker_threads(num_cpus::get().min(8))  // Limit to prevent resource exhaustion
    .enable_all()
    .build()?;

// Parallel split extraction
async fn extract_splits_parallel(split_urls: &[String], config: &MergeConfig) -> Result<Vec<ExtractedSplit>> {
    let tasks: Vec<_> = split_urls.iter().enumerate().map(|(i, url)| {
        let url = url.clone();
        let config = config.clone();
        tokio::spawn(async move {
            extract_single_split(i, &url, &config).await
        })
    }).collect();

    // Wait for all extractions to complete
    let mut results = Vec::new();
    for task in tasks {
        results.push(task.await??);
    }
    Ok(results)
}

// Resource-isolated IndexWriter per merge
fn create_isolated_index_writer(index: &Index, merge_id: &str) -> Result<IndexWriter> {
    // Use merge-specific thread count and memory limits
    let thread_count = (num_cpus::get() / 4).max(1);  // Share CPU resources
    let memory_limit = 50_000_000;  // 50MB per merge instead of 15MB

    let mut writer = index.writer_with_num_threads(thread_count, memory_limit)?;
    writer.set_merge_policy(Box::new(NoMergePolicy));
    Ok(writer)
}
```

### **Fix 3: Advanced Resource Management**

**Strategy**: Implement resource pools and coordination mechanisms

```rust
// Global merge coordinator to prevent resource conflicts
lazy_static! {
    static ref MERGE_COORDINATOR: Arc<Mutex<MergeCoordinator>> =
        Arc::new(Mutex::new(MergeCoordinator::new()));
}

struct MergeCoordinator {
    active_merges: HashMap<String, MergeOperation>,
    temp_dir_registry: HashSet<PathBuf>,
    resource_semaphore: Arc<Semaphore>,
}

impl MergeCoordinator {
    fn register_merge(&mut self, merge_id: String) -> Result<MergeToken> {
        // Acquire resource permit to limit concurrent merges
        let permit = self.resource_semaphore.try_acquire()
            .ok_or_else(|| anyhow!("Too many concurrent merge operations"))?;

        // Register unique temp directories
        let temp_dirs = self.allocate_unique_temp_dirs(&merge_id)?;

        let token = MergeToken {
            merge_id: merge_id.clone(),
            temp_dirs,
            _permit: permit,
        };

        self.active_merges.insert(merge_id, MergeOperation::new());
        Ok(token)
    }
}

// Connection pooling for S3 operations
struct S3DownloadPool {
    client_pool: Arc<Mutex<Vec<S3Client>>>,
    download_semaphore: Arc<Semaphore>,
}

impl S3DownloadPool {
    async fn download_parallel(&self, urls: &[String]) -> Result<Vec<Vec<u8>>> {
        let semaphore = Arc::clone(&self.download_semaphore);
        let tasks: Vec<_> = urls.iter().map(|url| {
            let url = url.clone();
            let semaphore = Arc::clone(&semaphore);
            tokio::spawn(async move {
                let _permit = semaphore.acquire().await?;
                download_split(&url).await
            })
        }).collect();

        futures::future::try_join_all(tasks).await
    }
}
```

### **Fix 4: Metadata Isolation and Caching**

**Strategy**: Per-merge metadata isolation with intelligent caching

```rust
// Isolated metadata directory per merge
fn create_merge_specific_metadata(merge_id: &str, index_meta: IndexMeta) -> Result<RamDirectory> {
    let ram_directory = RamDirectory::default();

    // Create merge-specific meta.json with unique namespace
    let metadata_json = serde_json::to_string_pretty(&index_meta)?;
    let meta_filename = format!("meta_{}.json", merge_id);

    ram_directory.atomic_write(Path::new(&meta_filename), metadata_json.as_bytes())?;
    ram_directory.atomic_write(Path::new("meta.json"), metadata_json.as_bytes())?;

    Ok(ram_directory)
}

// Cache frequently accessed split metadata
lazy_static! {
    static ref SPLIT_METADATA_CACHE: Arc<Mutex<LruCache<String, Arc<IndexMeta>>>> =
        Arc::new(Mutex::new(LruCache::new(100)));
}
```

## 📊 **PERFORMANCE OPTIMIZATION TARGETS**

### **Current Bottlenecks**:
- **Sequential Split Processing**: Linear time complexity O(n) for n splits
- **Single-threaded Runtime**: No async parallelism
- **Memory Competition**: All merges share 15MB heap
- **Temp Directory Races**: Random failures under concurrency

### **Target Performance Improvements**:
1. **Linear Scaling**: Merge time should scale with `max(split_size)` not `sum(split_sizes)`
2. **Parallel Efficiency**: N-core system should approach N× speedup for independent merges
3. **Memory Isolation**: Each merge gets dedicated memory allocation
4. **Zero Race Conditions**: Deterministic behavior under high concurrency

### **Benchmark Targets**:
```
Current: 4 concurrent merges = 4× single merge time (no scaling)
Target:  4 concurrent merges = 1.2× single merge time (near-linear scaling)

Current: 8 splits × 100MB each = 45 seconds sequential
Target:  8 splits × 100MB each = 12 seconds parallel (4 cores)
```

## 🎯 **IMPLEMENTATION PRIORITY**

### **Phase 1: Critical Race Condition Fix (High Priority)**
- [ ] Fix temporary directory naming and cleanup coordination
- [ ] Implement MergeCoordinator for resource management
- [ ] Add proper merge operation isolation
- [ ] **Target**: Zero metadata.json file not found errors

### **Phase 2: Parallel Processing Implementation (Medium Priority)**
- [ ] Convert to multi-threaded Tokio runtime
- [ ] Implement parallel split extraction
- [ ] Add S3 connection pooling
- [ ] **Target**: Linear scaling for independent merge operations

### **Phase 3: Advanced Optimization (Lower Priority)**
- [ ] Implement split metadata caching
- [ ] Add intelligent resource allocation
- [ ] Optimize memory usage patterns
- [ ] **Target**: Maximum throughput under sustained load

The fixes in Phase 1 should immediately resolve the metadata.json race condition, while Phase 2 will address the linear scaling issues. These changes will make the merge system robust under high concurrency while maintaining data integrity.
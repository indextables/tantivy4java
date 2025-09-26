# 🚀 Tantivy4Java Async-First Architecture Redesign

## 📋 Executive Summary

**Current Problem**: Persistent deadlocks in JNI layer due to mixing sync and async operations with tokio runtime conflicts.

**Root Cause**: Fundamental architectural mismatch between Java's synchronous JNI expectations and Quickwit's purely async design.

**Solution**: Complete redesign to async-first architecture with proper tokio runtime integration.

---

## 🎯 Current Architecture Problems

### 1. **Sync-in-Async Deadlock Pattern**
```rust
// ❌ CURRENT PROBLEMATIC PATTERN:
runtime.enter();                    // Enter tokio context
runtime.block_on(async { ... })     // Deadlock: block_on from within runtime
```

### 2. **Mixed Runtime Contexts**
- **JNI Layer**: Synchronous, expects immediate results
- **Quickwit Core**: Purely async with tokio runtime dependency
- **Bridge Layer**: Attempts to force sync-in-async with deadlocks

### 3. **Storage Resolver Lifecycle Issues**
- Lazy async creation inside sync contexts
- Multiple resolver instances instead of shared caching
- Runtime context confusion between creation and usage

---

## 🏗️ Proposed Async-First Architecture

### **Phase 1: Tokio Runtime Integration Foundation**

#### 1.1 Global Tokio Runtime Manager
```rust
// Global singleton runtime manager
pub struct TantivyRuntimeManager {
    runtime: Arc<Runtime>,
    thread_pool: Arc<ThreadPool>,
}

impl TantivyRuntimeManager {
    pub fn global() -> &'static Self { /* singleton */ }

    pub fn spawn_search<F>(&self, future: F) -> JoinHandle<Result<jobject, anyhow::Error>>
    where F: Future<Output = Result<jobject, anyhow::Error>> + Send + 'static

    pub fn block_on_search<F>(&self, future: F) -> Result<jobject, anyhow::Error>
    where F: Future<Output = Result<jobject, anyhow::Error>>
}
```

#### 1.2 Async JNI Bridge Pattern
```rust
// New async-compatible JNI pattern
#[no_mangle]
pub extern "system" fn Java_com_tantivy4java_SplitSearcher_searchWithQueryAst(
    env: JNIEnv,
    _class: JClass,
    searcher_ptr: jlong,
    query_ast_json: JString,
    limit: jint,
) -> jobject {
    let runtime = TantivyRuntimeManager::global();

    runtime.block_on_search(async move {
        // Pure async implementation - no more runtime.enter() or nested block_on()
        perform_async_search(env, searcher_ptr, query_ast_json, limit).await
    }).unwrap_or_else(|e| {
        to_java_exception(&mut env, &e);
        std::ptr::null_mut()
    })
}
```

### **Phase 2: Async-Native Operations**

#### 2.1 Pure Async Search Implementation
```rust
async fn perform_async_search(
    mut env: JNIEnv<'_>,
    searcher_ptr: jlong,
    query_ast_json: JString,
    limit: jint,
) -> Result<jobject, anyhow::Error> {
    // All operations are naturally async - no forced sync conversions
    let searcher_context = get_searcher_context(searcher_ptr)?;
    let query_ast = parse_query_ast_async(&env, query_ast_json).await?;
    let search_response = searcher_context.search_async(query_ast, limit).await?;
    create_java_result_async(&mut env, search_response).await
}
```

#### 2.2 Async-First Storage Operations
```rust
// Storage operations become naturally async
pub struct AsyncSplitSearcher {
    searcher: StandaloneSearcher,
    storage_resolver: StorageResolver,  // Pre-created, shared
    cache_manager: Arc<AsyncCacheManager>,
}

impl AsyncSplitSearcher {
    async fn search_async(&self, query: QueryAst, limit: usize) -> Result<LeafSearchResponse, Error> {
        // Natural async flow - no runtime conflicts
        let storage = self.storage_resolver.resolve(&self.split_uri).await?;
        let searcher_context = self.create_context_async().await?;
        self.perform_search_async(searcher_context, query, limit).await
    }
}
```

### **Phase 3: Shared Resource Management**

#### 3.1 Async Cache Manager
```rust
pub struct AsyncCacheManager {
    searcher_cache: Arc<TokioMutex<LruCache<String, Arc<AsyncSplitSearcher>>>>,
    storage_cache: Arc<TokioMutex<LruCache<String, StorageResolver>>>,
    schema_cache: Arc<TokioMutex<LruCache<String, Schema>>>,
}

impl AsyncCacheManager {
    async fn get_or_create_searcher(&self, config: &SearchConfig) -> Result<Arc<AsyncSplitSearcher>, Error> {
        let mut cache = self.searcher_cache.lock().await;
        // Async cache operations with proper tokio primitives
    }

    async fn get_or_create_storage_resolver(&self, s3_config: &S3Config) -> Result<StorageResolver, Error> {
        // Pre-create and share storage resolvers across all operations
        let mut cache = self.storage_cache.lock().await;
        // Async storage resolver creation and caching
    }
}
```

---

## 🚧 Implementation Roadmap

### **Phase 1: Foundation (Week 1-2)**

#### 1.1 Global Runtime Manager
- [ ] Create `TantivyRuntimeManager` singleton
- [ ] Implement `block_on_search()` with proper error handling
- [ ] Add runtime lifecycle management
- [ ] Create async JNI bridge utilities

#### 1.2 Core JNI Function Migration
- [ ] Convert `searchWithQueryAst` to async-first pattern
- [ ] Convert `docsNative` to async-first pattern
- [ ] Convert schema retrieval functions to async-first
- [ ] Test basic functionality with new pattern

### **Phase 2: Storage & Caching (Week 3-4)**

#### 2.1 Async Storage Management
- [ ] Redesign `AsyncSplitSearcher` with pre-created storage resolvers
- [ ] Implement async-first storage resolver creation and caching
- [ ] Convert all storage operations to pure async
- [ ] Eliminate all `runtime.enter()` and nested `block_on()` patterns

#### 2.2 Async Cache Architecture
- [ ] Implement `AsyncCacheManager` with tokio primitives
- [ ] Convert searcher caching to async-first
- [ ] Convert storage resolver caching to async-first
- [ ] Add comprehensive cache sharing validation

### **Phase 3: Advanced Features (Week 5-6)**

#### 3.1 Document Retrieval Redesign
- [ ] Convert batch document retrieval to async-first
- [ ] Implement async-compatible document caching
- [ ] Add async-first document validation and field access
- [ ] Optimize async memory management patterns

#### 3.2 S3 Integration Overhaul
- [ ] Redesign S3 operations for pure async
- [ ] Implement async-first credential management
- [ ] Add async S3 connection pooling and retry logic
- [ ] Validate async S3 performance characteristics

### **Phase 4: Testing & Validation (Week 7-8)**

#### 4.1 Comprehensive Testing
- [ ] Validate all existing tests pass with new architecture
- [ ] Add async-specific performance tests
- [ ] Test deadlock elimination under load
- [ ] Validate cache sharing and memory efficiency

#### 4.2 Performance Optimization
- [ ] Benchmark async vs sync performance
- [ ] Optimize async memory allocation patterns
- [ ] Add async-first monitoring and debugging tools
- [ ] Document performance characteristics

---

## 📊 Expected Benefits

### **1. Deadlock Elimination**
- ✅ **Complete elimination** of sync-in-async deadlocks
- ✅ **Natural async flow** matching Quickwit's design
- ✅ **Proper tokio runtime usage** without conflicts

### **2. Performance Improvements**
- 🚀 **Better concurrency** with natural async operations
- 🚀 **Improved cache sharing** with async-first design
- 🚀 **Reduced memory overhead** from eliminating runtime conflicts

### **3. Architectural Benefits**
- 🏗️ **Cleaner separation** between sync JNI and async core
- 🏗️ **Better error handling** with async-first patterns
- 🏗️ **Easier maintenance** with consistent async patterns

---

## ⚠️ Migration Considerations

### **Breaking Changes**
- JNI function signatures remain the same (external API unchanged)
- Internal architecture completely redesigned
- All storage and cache operations become async-first

### **Risk Mitigation**
- **Phased implementation** with incremental testing
- **Parallel development** - keep current implementation until new one is validated
- **Comprehensive test coverage** throughout migration
- **Performance benchmarking** at each phase

### **Rollback Strategy**
- Keep current sync implementation as backup
- Feature flags for async vs sync implementations
- Gradual rollout with ability to revert per function

---

## 🔧 Technical Implementation Details

### **Key Pattern Changes**

#### Before (Problematic):
```rust
runtime.enter();
runtime.block_on(async {
    // Async operations forced into sync context - DEADLOCKS
});
```

#### After (Async-First):
```rust
TantivyRuntimeManager::global().block_on_search(async {
    // Pure async operations - natural flow
});
```

### **Storage Resolver Pattern**

#### Before (Deadlock-Prone):
```rust
// Lazy async creation in sync context
let storage_resolver = get_configured_storage_resolver_async(config).await; // DEADLOCK
```

#### After (Pre-Created):
```rust
// Pre-created during initialization, shared across operations
let cache_manager = AsyncCacheManager::global();
let storage_resolver = cache_manager.get_storage_resolver(config).await; // SAFE
```

---

## 🎯 Success Metrics

1. **Zero Deadlocks**: Complete elimination of all hanging issues
2. **Performance Parity**: Async implementation matches or exceeds current performance
3. **Test Coverage**: 100% of existing tests pass with new architecture
4. **Memory Efficiency**: Improved memory usage through better cache sharing
5. **Maintainability**: Cleaner, more maintainable async-first codebase

---

## 📚 References

- [Tokio Runtime Documentation](https://docs.rs/tokio/latest/tokio/runtime/)
- [JNI Async Patterns](https://github.com/jni-rs/jni-rs/discussions)
- [Quickwit Async Architecture](https://github.com/quickwit-oss/quickwit)
- [Rust Async Best Practices](https://rust-lang.github.io/async-book/)

---

**Status**: 📋 Planning Complete - Ready for Implementation
**Next Step**: Begin Phase 1 with Global Runtime Manager implementation
// standalone_searcher.rs - Clean implementation based on standalone_searcher_design.md

use std::sync::{Arc, RwLock};
use std::time::Duration;

use bytesize::ByteSize;
use anyhow::{Result, Context as AnyhowContext};

use quickwit_storage::{Storage, StorageResolver};
use quickwit_config::S3StorageConfig;
use crate::global_cache::{get_configured_storage_resolver, get_global_searcher_context};
use quickwit_proto::search::{SearchRequest, LeafSearchResponse, SplitIdAndFooterOffsets};
use quickwit_doc_mapper::DocMapper;
use quickwit_search::{leaf_search_single_split, SearcherContext, CanSplitDoBetter};
use quickwit_search::search_permit_provider::{SearchPermit, compute_initial_memory_allocation};
use quickwit_common::uri::Uri;
use crate::debug_println;

/// Result of async search operation for JNI bridge
pub struct SearchResult {
    pub response: LeafSearchResponse,
    pub error: Option<anyhow::Error>,
}

/// Configuration for the standalone searcher
#[derive(Clone, Debug)]
pub struct StandaloneSearchConfig {
    /// Cache configuration
    pub cache: CacheConfig,
    /// Resource limits
    pub resources: ResourceConfig,
    /// Timeout and retry policies
    pub timeouts: TimeoutConfig,
    /// Warmup configuration
    pub warmup: WarmupConfig,
}

#[derive(Clone, Debug)]
pub struct CacheConfig {
    /// Fast field cache capacity (default: 1GB)
    pub fast_field_cache_capacity: ByteSize,
    /// Split footer cache capacity (default: 500MB)
    pub split_footer_cache_capacity: ByteSize,
    /// Partial request cache capacity (default: 64MB)
    pub partial_request_cache_capacity: ByteSize,
}

#[derive(Clone, Debug)]
pub struct ResourceConfig {
    /// Maximum concurrent split searches (default: 100)
    pub max_concurrent_splits: usize,
    /// Aggregation memory limit (default: 500MB)
    pub aggregation_memory_limit: ByteSize,
    /// Maximum aggregation buckets (default: 65000)
    pub aggregation_bucket_limit: u32,
}

#[derive(Clone, Debug)]
pub struct TimeoutConfig {
    /// Request timeout (default: 30s)
    pub request_timeout: Duration,
}

#[derive(Clone, Debug)]
pub struct WarmupConfig {
    /// Total warmup memory budget (default: 100GB)
    pub memory_budget: ByteSize,
    /// Initial allocation per split (default: 1GB)
    pub split_initial_allocation: ByteSize,
}

impl Default for StandaloneSearchConfig {
    fn default() -> Self {
        Self {
            cache: CacheConfig {
                fast_field_cache_capacity: ByteSize::gb(1),
                split_footer_cache_capacity: ByteSize::mb(500),
                partial_request_cache_capacity: ByteSize::mb(64),
            },
            resources: ResourceConfig {
                max_concurrent_splits: 100,
                aggregation_memory_limit: ByteSize::mb(500),
                aggregation_bucket_limit: 65000,
            },
            timeouts: TimeoutConfig {
                request_timeout: Duration::from_secs(30),
            },
            warmup: WarmupConfig {
                memory_budget: ByteSize::gb(100),
                split_initial_allocation: ByteSize::gb(1),
            },
        }
    }
}

/// Metadata required to search a split
#[derive(Clone, Debug)]
pub struct SplitSearchMetadata {
    /// Split identifier
    pub split_id: String,
    /// Footer offsets in the split file
    pub split_footer_start: u64,
    pub split_footer_end: u64,
    /// Actual file size from storage.file_num_bytes()
    pub file_size: u64,
    /// Number of documents in the split
    pub num_docs: u64,
    /// Optional timestamp range for optimization
    pub time_range: Option<(i64, i64)>,
    /// Delete opstamp for filtering deleted docs
    pub delete_opstamp: u64,
}

impl From<SplitSearchMetadata> for SplitIdAndFooterOffsets {
    fn from(meta: SplitSearchMetadata) -> Self {
        SplitIdAndFooterOffsets {
            split_id: meta.split_id,
            split_footer_start: meta.split_footer_start,
            split_footer_end: meta.split_footer_end,
            timestamp_start: meta.time_range.map(|(start, _)| start),
            timestamp_end: meta.time_range.map(|(_, end)| end),
            num_docs: meta.num_docs,
        }
    }
}

/// Request to search a single split
#[derive(Clone)]
pub struct SplitSearchRequest {
    /// Storage URI for the split (s3://, file://, etc.)
    pub split_uri: String,
    /// Split metadata
    pub metadata: SplitSearchMetadata,
    /// Search request
    pub search_request: SearchRequest,
    /// Document mapper for the index
    pub doc_mapper: Arc<DocMapper>,
}

/// Cache statistics
#[derive(Debug, Clone)]
pub struct CacheStats {
    pub fast_field_bytes: usize,
    pub split_footer_bytes: usize,
    pub partial_request_count: usize,
}

/// Core context maintaining caches and resource controls
/// This now uses global caches following Quickwit's pattern
pub struct StandaloneSearchContext {
    /// Configuration
    config: StandaloneSearchConfig,
    /// Quickwit SearcherContext with global caches
    searcher_context: Arc<SearcherContext>,
}

impl StandaloneSearchContext {
    pub fn new(config: StandaloneSearchConfig) -> Result<Self> {
        debug_println!("RUST DEBUG: Creating StandaloneSearchContext with global caches");
        
        // Use the global searcher context which contains all the shared caches
        // This follows Quickwit's pattern of sharing caches across all searcher instances
        let searcher_context = get_global_searcher_context();
        
        Ok(Self {
            config,
            searcher_context,
        })
    }
    
    pub fn cache_stats(&self) -> CacheStats {
        CacheStats {
            fast_field_bytes: 0, // QuickwitCache doesn't expose byte size directly
            split_footer_bytes: 0, // MemorySizedCache doesn't expose byte size consistently
            partial_request_count: 0, // We'll track this separately if needed
        }
    }
    
    pub fn clear_caches(&self) {
        // Global caches are not cleared per-instance
        // This is intentional to maintain cache efficiency across all searchers
        debug_println!("RUST DEBUG: Clear caches called but using global caches - no-op");
    }
    
    /// Get the underlying SearcherContext for direct access
    pub fn searcher_context(&self) -> &Arc<SearcherContext> {
        &self.searcher_context
    }
}

/// Main entry point for standalone searching
pub struct StandaloneSearcher {
    context: Arc<StandaloneSearchContext>,
    storage_resolver: StorageResolver,
}

impl StandaloneSearcher {
    /// Create a new standalone searcher with default configuration
    /// Uses the global storage resolver following Quickwit's pattern
    pub fn new(config: StandaloneSearchConfig) -> Result<Self> {
        debug_println!("RUST DEBUG: Creating StandaloneSearcher with global storage resolver");
        let context = Arc::new(StandaloneSearchContext::new(config)?);
        // Use the global storage resolver instead of creating a new one
        let storage_resolver = get_configured_storage_resolver(None);
        Ok(Self {
            context,
            storage_resolver,
        })
    }
    
    /// Create with default configuration
    pub fn default() -> Result<Self> {
        Self::new(StandaloneSearchConfig::default())
    }
    
    /// Create with a specific S3 configuration
    /// This allows for dynamic credentials while still using global caches
    pub fn with_s3_config(config: StandaloneSearchConfig, s3_config: S3StorageConfig) -> Result<Self> {
        debug_println!("RUST DEBUG: Creating StandaloneSearcher with custom S3 config");
        let context = Arc::new(StandaloneSearchContext::new(config)?);
        // Get a configured storage resolver with the specific S3 config
        let storage_resolver = get_configured_storage_resolver(Some(s3_config));
        Ok(Self {
            context,
            storage_resolver,
        })
    }
    
    /// Create with a specific storage resolver (for backwards compatibility)
    pub fn with_storage_resolver(config: StandaloneSearchConfig, storage_resolver: StorageResolver) -> Result<Self> {
        let context = Arc::new(StandaloneSearchContext::new(config)?);
        Ok(Self {
            context,
            storage_resolver,
        })
    }
    
    /// Search a single split using Quickwit's exact async pattern
    /// This follows the same pattern as single_doc_mapping_leaf_search
    pub async fn search_split(
        &self,
        split_uri: &str,
        metadata: SplitSearchMetadata,
        search_request: SearchRequest,
        doc_mapper: Arc<DocMapper>,
    ) -> Result<LeafSearchResponse> {
        // Resolve storage using the helper that handles S3 URIs correctly
        // println!("SCOTTWIT: search_split : before resolve");
        let storage = resolve_storage_for_split(&self.storage_resolver, split_uri).await?;
        // println!("SCOTTWIT: search_split : after resolve");
        
        // Convert metadata to internal format
        let split_offsets = SplitIdAndFooterOffsets::from(metadata.clone());
        
        // println!("SCOTTWIT: search_split : after resolve2");
        // FOLLOW QUICKWIT'S EXACT PATTERN: Get permits first, then await individual permit
        let memory_allocation = compute_initial_memory_allocation(
            &split_offsets,
            self.context.config.warmup.split_initial_allocation
        );
        // println!("SCOTTWIT: search_split : after resolve3");
        
        // Use the search permit provider from the global searcher context
        let permit_futures = self.context.searcher_context.search_permit_provider.get_permits(vec![memory_allocation]).await;
        let permit_future = permit_futures.into_iter().next()
            .expect("Expected one permit future");
        let mut search_permit = permit_future.await;
        
        // println!("SCOTTWIT: search_split : after resolve4");
        // Execute search using Quickwit's proven search implementation
        self.search_single_split_with_permit(
            storage,
            split_offsets,
            search_request,
            doc_mapper,
            &mut search_permit,
        ).await
    }
    
    async fn search_single_split_with_permit(
        &self,
        storage: Arc<dyn Storage>,
        split: SplitIdAndFooterOffsets,
        search_request: SearchRequest,
        doc_mapper: Arc<DocMapper>,
        search_permit: &mut SearchPermit,
    ) -> Result<LeafSearchResponse> {
        // Use the global searcher context instead of creating a new one
        // This ensures all searches share the same caches
        let searcher_context = &self.context.searcher_context;
        
        // Create required dependencies for leaf_search_single_split
        let split_filter = Arc::new(RwLock::new(CanSplitDoBetter::Uninformative));
        let aggregations_limits = searcher_context.aggregation_limit.clone();

        // Save split_id before move
        let split_id = split.split_id.clone();

        // Debug: Dump the QueryAst before calling leaf_search_single_split
        println!("TANTIVY4JAVA DEBUG: Split ID: {} QueryAst: {}", split_id, search_request.query_ast);
        
        // Parse and print the QueryAst structure
        if let Ok(parsed_query_ast) = serde_json::from_str::<quickwit_query::query_ast::QueryAst>(&search_request.query_ast) {
            println!("RUST DEBUG: Successfully parsed QueryAst: {:?}", parsed_query_ast);
        }

        // Use Quickwit's proven leaf_search_single_split function directly
        // This eliminates all the complexity we had in the previous implementation
        let result = leaf_search_single_split(
            &searcher_context,
            search_request,
            storage,
            split,  // split is moved here
            doc_mapper,
            split_filter,
            aggregations_limits,
            search_permit,
        ).await
        .with_context(|| format!("Failed to search split: {}", split_id))?;
        
        Ok(result)
    }
    
    /// Synchronous search method for JNI bridge that avoids block_on deadlocks
    /// This creates its own runtime to handle the async operations properly
    pub fn search_split_sync(
        &self,
        split_uri: &str,
        metadata: SplitSearchMetadata,
        search_request: SearchRequest,
        doc_mapper: Arc<DocMapper>,
    ) -> Result<LeafSearchResponse> {
        // Check if we're already inside a runtime
        match tokio::runtime::Handle::try_current() {
            Ok(_) => {
                // We're already in a runtime context, so we can't use block_on
                // Instead, we need to spawn a task or use a different approach
                return Err(anyhow::anyhow!("Cannot create nested runtime - use async search_split method instead"));
            }
            Err(_) => {
                // Not in a runtime, safe to create one
                let rt = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .with_context(|| "Failed to create runtime for sync search")?;
                    
                // Run the async search in the dedicated runtime
                // println!("SCOTTWIT: BEFORE block on");
                rt.block_on(async {
                    self.search_split(split_uri, metadata, search_request, doc_mapper).await
                })
            }
        }
    }

    /// Get cache statistics
    pub fn cache_stats(&self) -> CacheStats {
        self.context.cache_stats()
    }
    
    /// Clear all caches
    pub fn clear_caches(&self) {
        self.context.clear_caches();
    }
}

/// Helper function to resolve storage for a split URI or file path
/// For S3 URIs, this resolves the parent directory instead of the full file path
/// For direct file paths, this treats them as local file paths and resolves the directory
/// This is necessary because Quickwit's S3 storage uses the prefix for the directory
pub async fn resolve_storage_for_split(
    storage_resolver: &StorageResolver,
    split_uri: &str,
) -> Result<Arc<dyn Storage>> {
    // Handle direct file paths (no protocol)
    if !split_uri.contains("://") {
        let path = std::path::Path::new(split_uri);
        
        // Check if split_uri is already a directory path (ends with slash)
        if split_uri.ends_with('/') {
            // It's already a directory path, use it directly
            let directory_uri_str = format!("file://{}", split_uri);
            let directory_uri: Uri = directory_uri_str.parse()
                .with_context(|| format!("Failed to parse local directory URI: {}", directory_uri_str))?;
            
            debug_println!("RUST DEBUG: Resolving local file storage for directory: '{}'", directory_uri_str);
            return storage_resolver.resolve(&directory_uri).await
                .with_context(|| format!("Failed to resolve storage for local directory: {}", directory_uri_str));
        } else {
            // This is a file path, convert to file:// URI for the directory containing the file
            if let Some(parent_dir) = path.parent() {
                let directory_uri_str = format!("file://{}/", parent_dir.display());
                let directory_uri: Uri = directory_uri_str.parse()
                    .with_context(|| format!("Failed to parse local file directory URI: {}", directory_uri_str))?;
                
                debug_println!("RUST DEBUG: Resolving local file storage for directory: '{}'", directory_uri_str);
                return storage_resolver.resolve(&directory_uri).await
                    .with_context(|| format!("Failed to resolve storage for local file directory: {}", directory_uri_str));
            } else {
                // Use current directory if no parent
                let directory_uri: Uri = "file://./".parse()
                    .with_context(|| "Failed to parse current directory URI")?;
                return storage_resolver.resolve(&directory_uri).await
                    .with_context(|| "Failed to resolve storage for current directory");
            }
        }
    }

    let uri: Uri = split_uri.parse()
        .with_context(|| format!("Failed to parse URI: {}", split_uri))?;
    
    match uri.protocol() {
        quickwit_common::uri::Protocol::S3 => {
            // For S3, we need to resolve the directory, not the file
            let uri_str = uri.as_str();
            if let Some(last_slash_pos) = uri_str.rfind('/') {
                let directory_uri_str = &uri_str[..last_slash_pos + 1];
                let directory_uri: Uri = directory_uri_str.parse()
                    .with_context(|| format!("Failed to parse S3 directory URI: {}", directory_uri_str))?;
                
                println!("QUICKWIT DEBUG: Resolving S3 storage for directory: '{}'", directory_uri_str);
                storage_resolver.resolve(&directory_uri).await
                    .with_context(|| format!("Failed to resolve storage for directory URI: {}", directory_uri_str))
            } else {
                // Fallback if no slash found (shouldn't happen with valid S3 URIs)
                storage_resolver.resolve(&uri).await
                    .with_context(|| format!("Failed to resolve storage for URI: {}", split_uri))
            }
        },
        quickwit_common::uri::Protocol::File => {
            // For file://, we also need to resolve the directory, not the file
            let uri_str = uri.as_str();
            if let Some(last_slash_pos) = uri_str.rfind('/') {
                let directory_uri_str = &uri_str[..last_slash_pos + 1];
                let directory_uri: Uri = directory_uri_str.parse()
                    .with_context(|| format!("Failed to parse file directory URI: {}", directory_uri_str))?;
                
                debug_println!("RUST DEBUG: Resolving file:// storage for directory: '{}'", directory_uri_str);
                storage_resolver.resolve(&directory_uri).await
                    .with_context(|| format!("Failed to resolve storage for file directory URI: {}", directory_uri_str))
            } else {
                // Fallback if no slash found
                storage_resolver.resolve(&uri).await
                    .with_context(|| format!("Failed to resolve storage for file URI: {}", split_uri))
            }
        },
        _ => {
            // For other protocols, use the full URI
            storage_resolver.resolve(&uri).await
                .with_context(|| format!("Failed to resolve storage for URI: {}", split_uri))
        }
    }
}

/// Builder pattern for convenience
pub struct StandaloneSearcherBuilder {
    config: StandaloneSearchConfig,
}

impl StandaloneSearcherBuilder {
    pub fn new() -> Self {
        Self {
            config: StandaloneSearchConfig::default(),
        }
    }
    
    pub fn fast_field_cache_size(mut self, size: ByteSize) -> Self {
        self.config.cache.fast_field_cache_capacity = size;
        self
    }
    
    pub fn split_footer_cache_size(mut self, size: ByteSize) -> Self {
        self.config.cache.split_footer_cache_capacity = size;
        self
    }
    
    pub fn max_concurrent_splits(mut self, max: usize) -> Self {
        self.config.resources.max_concurrent_splits = max;
        self
    }
    
    pub fn request_timeout(mut self, timeout: Duration) -> Self {
        self.config.timeouts.request_timeout = timeout;
        self
    }
    
    pub fn build(self) -> Result<StandaloneSearcher> {
        StandaloneSearcher::new(self.config)
    }
}

impl Default for StandaloneSearcherBuilder {
    fn default() -> Self {
        Self::new()
    }
}

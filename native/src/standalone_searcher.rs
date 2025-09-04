// standalone_searcher.rs - Clean implementation based on standalone_searcher_design.md

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{Arc, RwLock};
use std::time::Duration;

use bytesize::ByteSize;
use tokio::sync::Semaphore;
use anyhow::{Result, Context as AnyhowContext};

use quickwit_storage::{
    Storage, StorageResolver, MemorySizedCache, QuickwitCache, ByteRangeCache,
    wrap_storage_with_cache, STORAGE_METRICS, LocalFileStorageFactory, 
    S3CompatibleObjectStorageFactory
};
use quickwit_config::S3StorageConfig;
use quickwit_proto::search::{SearchRequest, LeafSearchResponse, SplitIdAndFooterOffsets};
use quickwit_doc_mapper::DocMapper;
use quickwit_search::{leaf_search_single_split, SearcherContext, CanSplitDoBetter};
use quickwit_search::list_fields_cache::ListFieldsCache;
use quickwit_search::leaf_cache::LeafSearchCache;
use quickwit_search::search_permit_provider::{SearchPermitProvider, SearchPermit};
use quickwit_config::SearcherConfig;
use quickwit_common::uri::Uri;
use tantivy::aggregation::AggregationLimitsGuard;

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
pub struct StandaloneSearchContext {
    /// Configuration
    config: StandaloneSearchConfig,
    /// Fast field cache shared across searches
    pub fast_fields_cache: Arc<QuickwitCache>,
    /// Split footer cache
    pub split_footer_cache: Arc<MemorySizedCache<String>>,
    /// Resource permit provider
    pub permit_provider: Arc<SearchPermitProvider>,
    /// Aggregation limits guard
    aggregation_limits: Arc<AggregationLimitsGuard>,
}

impl StandaloneSearchContext {
    pub fn new(config: StandaloneSearchConfig) -> Result<Self> {
        // Initialize fast field cache
        let fast_fields_cache = Arc::new(QuickwitCache::new(
            config.cache.fast_field_cache_capacity.as_u64() as usize
        ));
        
        // Initialize split footer cache
        let split_footer_cache = Arc::new(MemorySizedCache::with_capacity_in_bytes(
            config.cache.split_footer_cache_capacity.as_u64() as usize,
            &STORAGE_METRICS.split_footer_cache,
        ));
        
        // Initialize permit provider
        let permit_provider = Arc::new(SearchPermitProvider::new(
            config.resources.max_concurrent_splits,
            config.warmup.memory_budget,
        ));
        
        // Initialize aggregation limits
        let aggregation_limits = Arc::new(AggregationLimitsGuard::new(
            Some(config.resources.aggregation_memory_limit.as_u64()),
            Some(config.resources.aggregation_bucket_limit),
        ));
        
        Ok(Self {
            config,
            fast_fields_cache,
            split_footer_cache,
            permit_provider,
            aggregation_limits,
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
        // QuickwitCache doesn't expose clear method, would need to be added
        // MemorySizedCache doesn't expose clear method publicly
        // These would need to be added to the Quickwit API
    }
}

/// Main entry point for standalone searching
pub struct StandaloneSearcher {
    context: Arc<StandaloneSearchContext>,
    storage_resolver: StorageResolver,
}

impl StandaloneSearcher {
    /// Create a new standalone searcher with default configuration
    pub fn new(config: StandaloneSearchConfig) -> Result<Self> {
        let context = Arc::new(StandaloneSearchContext::new(config)?);
        let storage_resolver = StorageResolver::unconfigured();
        Ok(Self {
            context,
            storage_resolver,
        })
    }
    
    /// Create with default configuration
    pub fn default() -> Result<Self> {
        Self::new(StandaloneSearchConfig::default())
    }
    
    /// Search a single split
    pub async fn search_split(
        &self,
        split_uri: &str,
        metadata: SplitSearchMetadata,
        search_request: SearchRequest,
        doc_mapper: Arc<DocMapper>,
    ) -> Result<LeafSearchResponse> {
        // Parse URI string to Uri type
        let uri: Uri = split_uri.parse()
            .with_context(|| format!("Failed to parse URI: {}", split_uri))?;
        
        // Resolve storage from URI
        let storage = self.storage_resolver.resolve(&uri).await
            .with_context(|| format!("Failed to resolve storage for URI: {}", split_uri))?;
        
        // Convert metadata to internal format
        let split_offsets = SplitIdAndFooterOffsets::from(metadata.clone());
        
        // Execute search using Quickwit's proven search implementation
        self.search_single_split_internal(
            storage,
            split_offsets,
            search_request,
            doc_mapper,
        ).await
    }
    
    async fn search_single_split_internal(
        &self,
        storage: Arc<dyn Storage>,
        split: SplitIdAndFooterOffsets,
        search_request: SearchRequest,
        doc_mapper: Arc<DocMapper>,
    ) -> Result<LeafSearchResponse> {
        // Create SearcherContext using the proven approach from open_split_bundle
        let searcher_context = SearcherContext {
            searcher_config: SearcherConfig::default(),
            fast_fields_cache: self.context.fast_fields_cache.clone(),
            search_permit_provider: (*self.context.permit_provider).clone(),
            split_footer_cache: MemorySizedCache::with_capacity_in_bytes(
                500_000_000, // 500MB
                &STORAGE_METRICS.split_footer_cache,
            ),
            split_stream_semaphore: Semaphore::new(100),
            split_cache_opt: None,
            list_fields_cache: ListFieldsCache::new(1000),
            leaf_search_cache: LeafSearchCache::new(1000),
            aggregation_limit: (*self.context.aggregation_limits).clone(),
        };
        
        // Create required dependencies for leaf_search_single_split
        let split_filter = Arc::new(RwLock::new(CanSplitDoBetter::Uninformative));
        let aggregations_limits = (*self.context.aggregation_limits).clone();
        
        // Get search permit - we need to provide the memory size for the split
        let permit_futures = self.context.permit_provider.get_permits(vec![
            self.context.config.warmup.split_initial_allocation
        ]).await;
        let permit_future = permit_futures.into_iter().next()
            .expect("Expected one permit future");
        let mut search_permit = permit_future.await;

        // Save split_id before move
        let split_id = split.split_id.clone();

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
            &mut search_permit,
        ).await
        .with_context(|| format!("Failed to search split: {}", split_id))?;
        
        Ok(result)
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

/// Create storage resolver with default S3 config - moved from split_searcher.rs
pub fn create_storage_resolver() -> StorageResolver {
    let s3_config = S3StorageConfig::default();
    create_storage_resolver_with_config(s3_config)
}

/// Create storage resolver with specific S3 config - moved from split_searcher.rs
pub fn create_storage_resolver_with_config(s3_config: S3StorageConfig) -> StorageResolver {
    StorageResolver::builder()
        .register(LocalFileStorageFactory::default())
        .register(S3CompatibleObjectStorageFactory::new(s3_config))
        .build()
        .expect("Failed to create storage resolver")
}
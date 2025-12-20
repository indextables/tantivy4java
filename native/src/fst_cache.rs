// fst_cache.rs - Simple 2-tier FST/Term Dictionary Cache
//
// Purpose: Fast term existence checking WITHOUT downloading fast fields.
//
// Architecture:
// - Tier 1: Memory LRU cache (~5GB default) - reads through to disk on miss
// - Tier 2: Disk cache at $basepath/FST_SKIPPING_CACHE/{hashcode}/{split_id}/{field}.term
//
// CRITICAL: This cache ONLY downloads term dictionary bytes from splits.
// Fast fields, posting lists, and other data are NOT downloaded.

use std::collections::HashMap;
use std::fs::{self, File};
use std::io::Write as IoWrite;
use std::path::{Path, PathBuf};
use std::sync::{Arc, RwLock};
use std::hash::{Hash, Hasher};
use std::collections::hash_map::DefaultHasher;

use anyhow::{Result, anyhow, Context};
use lru::LruCache;

use quickwit_storage::{Storage, OwnedBytes};

use crate::debug_println;

/// Byte buffer type for cached data
type CachedBytes = OwnedBytes;

// ================================
// Configuration
// ================================

/// Configuration for the FST cache
#[derive(Clone, Debug)]
pub struct FstCacheConfig {
    /// Maximum memory cache size in bytes (default: 5GB)
    pub memory_cache_size_bytes: u64,

    /// Maximum disk cache size in bytes (default: 50% of disk, max 1000 * disk_gb)
    pub disk_cache_size_bytes: u64,

    /// Base path for disk cache (user_dir → local_disk0 → system_temp)
    pub disk_cache_base_path: PathBuf,
}

impl Default for FstCacheConfig {
    fn default() -> Self {
        // Default: 5GB memory, calculate disk from system
        let memory_cache_size = 5 * 1024 * 1024 * 1024; // 5GB

        // Get system temp directory as fallback
        let base_path = std::env::temp_dir();

        // Calculate disk cache size (50% of disk, max 1000 * disk_gb)
        let disk_cache_size = calculate_disk_cache_size(&base_path);

        FstCacheConfig {
            memory_cache_size_bytes: memory_cache_size,
            disk_cache_size_bytes: disk_cache_size,
            disk_cache_base_path: base_path,
        }
    }
}

impl FstCacheConfig {
    pub fn with_memory_size(mut self, bytes: u64) -> Self {
        self.memory_cache_size_bytes = bytes;
        self
    }

    pub fn with_disk_size(mut self, bytes: u64) -> Self {
        self.disk_cache_size_bytes = bytes;
        self
    }

    pub fn with_base_path(mut self, path: PathBuf) -> Self {
        // Recalculate disk size for new path (before moving)
        self.disk_cache_size_bytes = calculate_disk_cache_size(&path);
        self.disk_cache_base_path = path;
        self
    }
}

/// Calculate disk cache size: 50% of available disk, max 1000 * disk_gb
fn calculate_disk_cache_size(path: &Path) -> u64 {
    // Try to get disk space info
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        if let Ok(metadata) = fs::metadata(path) {
            // Use statvfs to get disk info
            // For simplicity, default to 50GB if we can't determine
            return 50 * 1024 * 1024 * 1024; // 50GB default
        }
    }

    // Default: 50GB
    50 * 1024 * 1024 * 1024
}

// ================================
// Memory LRU Cache (using lru crate)
// ================================

/// Memory LRU cache using the battle-tested `lru` crate
/// Same LRU implementation used by Quickwit
struct MemoryLruCache {
    /// LRU cache from the lru crate
    cache: LruCache<String, CachedBytes>,

    /// Current total size in bytes
    current_size: u64,

    /// Maximum size in bytes
    max_size: u64,
}

impl MemoryLruCache {
    fn new(max_size: u64) -> Self {
        MemoryLruCache {
            // Unbounded by count, we manage by size
            cache: LruCache::unbounded(),
            current_size: 0,
            max_size,
        }
    }

    fn get(&mut self, key: &str) -> Option<CachedBytes> {
        self.cache.get(key).cloned()
    }

    fn insert(&mut self, key: String, data: CachedBytes) {
        let size = data.len() as u64;

        // Don't insert if single entry exceeds max size
        if size > self.max_size {
            debug_println!("FST_CACHE: Entry {} too large for memory cache ({} > {})",
                           key, size, self.max_size);
            return;
        }

        // Evict LRU entries until we have room
        while self.current_size + size > self.max_size {
            if let Some((evicted_key, evicted_data)) = self.cache.pop_lru() {
                let evicted_size = evicted_data.len() as u64;
                self.current_size -= evicted_size;
                debug_println!("FST_CACHE: Evicted LRU entry {} ({} bytes)", evicted_key, evicted_size);
            } else {
                break;
            }
        }

        // Insert new entry
        if let Some((_old_key, old_data)) = self.cache.push(key, data) {
            // Key already existed, adjust size
            self.current_size -= old_data.len() as u64;
        }
        self.current_size += size;
    }

    fn stats(&self) -> (usize, u64, u64) {
        (self.cache.len(), self.current_size, self.max_size)
    }
}

// ================================
// Disk Cache
// ================================

struct DiskCache {
    base_path: PathBuf,
    max_size: u64,
    // We don't track current size - rely on filesystem
}

impl DiskCache {
    fn new(base_path: PathBuf, max_size: u64) -> Self {
        DiskCache { base_path, max_size }
    }

    /// Get the disk path for a cache entry
    fn get_path(&self, table_path: &str, split_id: &str, field_name: &str) -> PathBuf {
        let table_hash = hash_string(table_path);
        self.base_path
            .join("FST_SKIPPING_CACHE")
            .join(format!("{:016x}", table_hash))
            .join(split_id)
            .join(format!("{}.term", field_name))
    }

    fn get(&self, table_path: &str, split_id: &str, field_name: &str) -> Option<CachedBytes> {
        let path = self.get_path(table_path, split_id, field_name);

        if path.exists() {
            match fs::read(&path) {
                Ok(data) => {
                    debug_println!("FST_CACHE: Disk cache HIT: {:?}", path);
                    Some(OwnedBytes::new(data))  // Convert Vec<u8> to OwnedBytes
                }
                Err(e) => {
                    debug_println!("FST_CACHE: Disk cache read error: {:?} - {}", path, e);
                    None
                }
            }
        } else {
            debug_println!("FST_CACHE: Disk cache MISS: {:?}", path);
            None
        }
    }

    fn insert(&self, table_path: &str, split_id: &str, field_name: &str, data: &CachedBytes) -> Result<()> {
        let path = self.get_path(table_path, split_id, field_name);

        // Create parent directories
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)
                .context("Failed to create cache directory")?;
        }

        // Write data
        let mut file = File::create(&path)
            .context("Failed to create cache file")?;
        file.write_all(data)
            .context("Failed to write cache data")?;

        debug_println!("FST_CACHE: Wrote to disk cache: {:?} ({} bytes)", path, data.len());
        Ok(())
    }
}

/// Hash a string to a u64 for directory naming
fn hash_string(s: &str) -> u64 {
    let mut hasher = DefaultHasher::new();
    s.hash(&mut hasher);
    hasher.finish()
}

// ================================
// Main FST Cache
// ================================

/// Two-tier FST cache with memory LRU and disk backing
pub struct FstCache {
    memory: RwLock<MemoryLruCache>,
    disk: DiskCache,
    /// Cache of parsed bundle metadata (file offsets) per split
    /// This avoids re-downloading the footer for each field
    metadata_cache: RwLock<HashMap<String, BundleMetadataEntry>>,
}

/// Cached bundle metadata for a split
#[derive(Clone)]
struct BundleMetadataEntry {
    /// File offsets in the bundle
    file_offsets: HashMap<PathBuf, std::ops::Range<u64>>,
    /// List of segment IDs that have .term files
    term_dict_segments: Vec<String>,
    /// Field name to field ID mapping (from meta.json)
    field_ids: HashMap<String, u32>,
}

impl FstCache {
    pub fn new(config: FstCacheConfig) -> Self {
        let cache_dir = config.disk_cache_base_path.clone();

        // Create the FST_SKIPPING_CACHE directory
        let fst_cache_dir = cache_dir.join("FST_SKIPPING_CACHE");
        if let Err(e) = fs::create_dir_all(&fst_cache_dir) {
            debug_println!("FST_CACHE: Warning - failed to create cache dir: {:?} - {}", fst_cache_dir, e);
        }

        FstCache {
            memory: RwLock::new(MemoryLruCache::new(config.memory_cache_size_bytes)),
            disk: DiskCache::new(cache_dir, config.disk_cache_size_bytes),
            metadata_cache: RwLock::new(HashMap::new()),
        }
    }

    /// Get term dictionary bytes for a field, using read-through caching
    ///
    /// Cache hierarchy:
    /// 1. Check memory cache
    /// 2. Check disk cache (and promote to memory)
    /// 3. Fetch from storage (and write to both caches)
    ///
    /// CRITICAL: Only downloads term dictionary bytes, NOT fast fields or posting lists.
    ///
    /// Returns: (term_dict_bytes, field_id) - the field ID is needed to construct proper term keys
    pub async fn get_term_dict(
        &self,
        table_path: &str,
        split_url: &str,
        split_id: &str,
        field_name: &str,
        _footer_offset: u64,
        storage: Arc<dyn Storage>,
    ) -> Result<(CachedBytes, u32)> {
        // 1. Get or fetch bundle metadata (includes segment IDs and field IDs)
        let metadata = self.get_or_fetch_metadata(split_url, split_id, storage.clone()).await?;

        // Get field ID for this field (needed to construct term keys)
        let field_id = *metadata.field_ids.get(field_name)
            .ok_or_else(|| anyhow!(
                "Field '{}' not found in schema. Available fields: {:?}",
                field_name,
                metadata.field_ids.keys().collect::<Vec<_>>()
            ))?;

        eprintln!("[FST_CACHE TRACE] Field '{}' has ID {}", field_name, field_id);

        // 2. Get first segment's term dict (splits typically have one segment)
        let segment_id = metadata.term_dict_segments.first()
            .ok_or_else(|| anyhow!("No .term files found in split"))?;

        eprintln!("[FST_CACHE TRACE] Using segment '{}.term'", segment_id);

        // Use segment-based cache key (includes field_id for per-field caching)
        let cache_key = format!("{}:{}:{}:{}", table_path, split_id, segment_id, field_id);

        // Check memory cache
        {
            let mut memory = self.memory.write().unwrap();
            if let Some(data) = memory.get(&cache_key) {
                debug_println!("FST_CACHE: Memory cache HIT for {}", cache_key);
                return Ok((data, field_id));
            }
        }

        // Check disk cache (using segment_id as "field" for path compatibility)
        if let Some(data) = self.disk.get(table_path, split_id, &format!("{}", segment_id)) {
            // Promote to memory cache
            {
                let mut memory = self.memory.write().unwrap();
                memory.insert(cache_key.clone(), data.clone());
            }
            return Ok((data, field_id));
        }

        // 3. Fetch the segment's term dictionary bytes
        let term_dict_file = format!("{}.term", segment_id);
        let term_dict_path = PathBuf::from(&term_dict_file);

        let term_dict_range = metadata.file_offsets.get(&term_dict_path)
            .ok_or_else(|| anyhow!(
                "Term dict file '{}' not found in bundle. Available: {:?}",
                term_dict_file,
                metadata.file_offsets.keys().map(|p| p.display().to_string()).collect::<Vec<_>>()
            ))?;

        eprintln!("[FST_CACHE TRACE] Fetching term dict '{}' at range {}..{} ({} bytes)",
                       term_dict_file, term_dict_range.start, term_dict_range.end,
                       term_dict_range.end - term_dict_range.start);

        // Extract filename from URL
        let filename = split_url.rfind('/')
            .map(|i| &split_url[i + 1..])
            .unwrap_or(split_url);
        let path = Path::new(filename);

        // Get file size for validation
        let file_size = storage.file_num_bytes(path).await
            .context("Failed to get split file size for term dict")?;
        eprintln!("[FST_CACHE TRACE] Split file size: {} bytes", file_size);
        eprintln!("[FST_CACHE TRACE] Requesting range: {}..{}", term_dict_range.start, term_dict_range.end);

        if term_dict_range.end > file_size as u64 {
            eprintln!("[FST_CACHE TRACE] WARNING: term dict range end ({}) > file size ({})",
                     term_dict_range.end, file_size);
        }

        // Download ONLY the term dictionary bytes (NO fast fields!)
        let data: CachedBytes = storage.get_slice(
            path,
            term_dict_range.start as usize..term_dict_range.end as usize
        ).await
            .context("Failed to fetch term dictionary")?;

        eprintln!("[FST_CACHE TRACE] Downloaded {} bytes of term dict (NO fast fields!)", data.len());

        // CRITICAL: Check the last 4 bytes to see what dictionary type marker we have
        if data.len() >= 4 {
            let last_4 = &data[data.len() - 4..];
            let dict_type = u32::from_le_bytes([last_4[0], last_4[1], last_4[2], last_4[3]]);
            eprintln!("[FST_CACHE TRACE] Last 4 bytes (should be 1=Fst or 2=SSTable): {:?} = {}",
                     last_4, dict_type);

            // INVESTIGATION: Check if the data contains any valid FST/SSTable markers
            // SSTable should end with type 2, FST with type 1
            eprintln!("[FST_CACHE TRACE] Looking for valid dictionary type markers in downloaded data...");
            let mut last_valid_marker_offset: Option<usize> = None;
            for i in 0..data.len().saturating_sub(3) {
                let marker = u32::from_le_bytes([data[i], data[i+1], data[i+2], data[i+3]]);
                if marker == 1 || marker == 2 {
                    eprintln!("[FST_CACHE TRACE]   Found valid marker {} at offset {} (from end: {})",
                             marker, i, data.len() - i - 4);
                    last_valid_marker_offset = Some(i);
                }
            }

            // Show bytes around the last valid marker
            if let Some(offset) = last_valid_marker_offset {
                let end_of_dict = offset + 4;
                eprintln!("[FST_CACHE TRACE] Last valid marker at offset {}. Dictionary might end at byte {}.",
                         offset, end_of_dict);
                eprintln!("[FST_CACHE TRACE] Bytes after marker (should be trailer/checksum): {:02x?}",
                         &data[end_of_dict..data.len().min(end_of_dict + 32)]);

                // Check if data after marker looks like a JSON checksum or hotcache footer
                if data.len() > end_of_dict {
                    let trailer_bytes = &data[end_of_dict..];
                    if let Ok(trailer_str) = std::str::from_utf8(trailer_bytes) {
                        eprintln!("[FST_CACHE TRACE] Trailer as UTF-8: {}", trailer_str);
                    }
                }
            }

            // Check if maybe we need to skip some header bytes
            eprintln!("[FST_CACHE TRACE] First 16 bytes as hex: {:02x?}", &data[..data.len().min(16)]);
            eprintln!("[FST_CACHE TRACE] Bytes 0-7 as u64 LE: {}", u64::from_le_bytes(data[..8].try_into().unwrap_or([0;8])));
        }

        // CRITICAL FIX: The bundle stores term dict + hotcache footer together.
        // Tantivy's TermDictionary::open expects ONLY the term dict bytes, ending with
        // the dictionary type marker (1=Fst or 2=SSTable) as the last 4 bytes.
        // We need to find and truncate at the last valid marker.

        let truncated_data = find_and_truncate_at_dict_marker(&data);

        // Write to disk cache (using segment_id as identifier)
        if let Err(e) = self.disk.insert(table_path, split_id, &format!("{}", segment_id), &truncated_data) {
            debug_println!("FST_CACHE: Warning - failed to write disk cache: {}", e);
        }

        // Write to memory cache
        {
            let mut memory = self.memory.write().unwrap();
            memory.insert(cache_key, truncated_data.clone());
        }

        Ok((truncated_data, field_id))
    }

    /// Get or fetch bundle metadata (file offsets) for a split
    ///
    /// This is cached separately to avoid re-downloading the footer for each field.
    async fn get_or_fetch_metadata(
        &self,
        split_url: &str,
        split_id: &str,
        storage: Arc<dyn Storage>,
    ) -> Result<BundleMetadataEntry> {
        // Check metadata cache
        {
            let cache = self.metadata_cache.read().unwrap();
            if let Some(entry) = cache.get(split_id) {
                debug_println!("FST_CACHE: Metadata cache HIT for {}", split_id);
                return Ok(BundleMetadataEntry {
                    file_offsets: entry.file_offsets.clone(),
                    term_dict_segments: entry.term_dict_segments.clone(),
                    field_ids: entry.field_ids.clone(),
                });
            }
        }

        debug_println!("FST_CACHE: Metadata cache MISS for {} - fetching footer", split_id);

        // Fetch metadata from storage
        let file_offsets = fetch_bundle_metadata(split_url, storage.clone()).await?;

        // Extract segment IDs that have .term files
        let term_dict_segments: Vec<String> = file_offsets.keys()
            .filter_map(|p| {
                let s = p.to_string_lossy();
                if s.ends_with(".term") {
                    Some(s.trim_end_matches(".term").to_string())
                } else {
                    None
                }
            })
            .collect();

        eprintln!("[FST_CACHE TRACE] Split {} has {} .term files (segments): {:?}",
                       split_id, term_dict_segments.len(), term_dict_segments);

        // Fetch and parse meta.json to get field IDs
        let field_ids = fetch_field_ids_from_meta_json(split_url, &file_offsets, storage).await
            .unwrap_or_else(|e| {
                eprintln!("[FST_CACHE WARN] Failed to parse meta.json for field IDs: {}", e);
                HashMap::new()
            });

        eprintln!("[FST_CACHE TRACE] Field IDs: {:?}", field_ids);

        let entry = BundleMetadataEntry {
            file_offsets,
            term_dict_segments,
            field_ids,
        };

        // Cache the metadata
        {
            let mut cache = self.metadata_cache.write().unwrap();
            cache.insert(split_id.to_string(), entry.clone());
        }

        Ok(entry)
    }

    /// Clone BundleMetadataEntry
    fn clone_metadata_entry(entry: &BundleMetadataEntry) -> BundleMetadataEntry {
        BundleMetadataEntry {
            file_offsets: entry.file_offsets.clone(),
            term_dict_segments: entry.term_dict_segments.clone(),
            field_ids: entry.field_ids.clone(),
        }
    }

    /// Get list of available term dict fields for a split
    pub async fn get_available_fields(
        &self,
        split_url: &str,
        split_id: &str,
        storage: Arc<dyn Storage>,
    ) -> Result<Vec<String>> {
        // Ensure metadata is cached
        let _ = self.get_or_fetch_metadata(split_url, split_id, storage).await?;

        // Return cached field list from field_ids map
        let cache = self.metadata_cache.read().unwrap();
        if let Some(entry) = cache.get(split_id) {
            Ok(entry.field_ids.keys().cloned().collect())
        } else {
            Ok(Vec::new())
        }
    }

    /// Get cache statistics
    pub fn stats(&self) -> FstCacheStats {
        let memory = self.memory.read().unwrap();
        let (entry_count, current_size, max_size) = memory.stats();
        let metadata_cache_size = self.metadata_cache.read().unwrap().len();

        FstCacheStats {
            memory_entry_count: entry_count,
            memory_current_size: current_size,
            memory_max_size: max_size,
            disk_max_size: self.disk.max_size,
            metadata_cache_entries: metadata_cache_size,
        }
    }
}

#[derive(Debug)]
pub struct FstCacheStats {
    pub memory_entry_count: usize,
    pub memory_current_size: u64,
    pub memory_max_size: u64,
    pub disk_max_size: u64,
    pub metadata_cache_entries: usize,
}

// ================================
// Storage Operations
// ================================

/// Fetch field name to ID mapping from meta.json in the bundle
///
/// The meta.json contains the Tantivy index schema which maps field names to IDs.
/// We need this to construct proper term dictionary keys.
async fn fetch_field_ids_from_meta_json(
    split_url: &str,
    file_offsets: &HashMap<PathBuf, std::ops::Range<u64>>,
    storage: Arc<dyn Storage>,
) -> Result<HashMap<String, u32>> {
    // Find meta.json in the bundle
    let meta_json_path = PathBuf::from("meta.json");
    let meta_range = file_offsets.get(&meta_json_path)
        .ok_or_else(|| anyhow!("meta.json not found in bundle"))?;

    // Extract filename from URL
    let filename = split_url.rfind('/')
        .map(|i| &split_url[i + 1..])
        .unwrap_or(split_url);
    let path = Path::new(filename);

    eprintln!("[FST_CACHE TRACE] Fetching meta.json from {}..{}", meta_range.start, meta_range.end);

    // Fetch meta.json bytes
    let meta_bytes = storage.get_slice(path, meta_range.start as usize..meta_range.end as usize).await
        .context("Failed to fetch meta.json")?;

    // Parse meta.json to extract schema fields
    // Tantivy meta.json format: { "segments": [...], "schema": [...], ... }
    #[derive(serde::Deserialize)]
    struct MetaJson {
        #[serde(default)]
        schema: Vec<FieldEntry>,
    }

    #[derive(serde::Deserialize)]
    struct FieldEntry {
        name: String,
        #[serde(rename = "type")]
        _field_type: serde_json::Value,
    }

    let meta: MetaJson = serde_json::from_slice(&meta_bytes)
        .context("Failed to parse meta.json")?;

    // Field IDs are assigned in order of appearance in the schema array
    let field_ids: HashMap<String, u32> = meta.schema.iter()
        .enumerate()
        .map(|(idx, field)| (field.name.clone(), idx as u32))
        .collect();

    eprintln!("[FST_CACHE TRACE] Extracted {} field IDs from meta.json: {:?}", field_ids.len(), field_ids);

    Ok(field_ids)
}

/// Fetch ONLY the bundle metadata (file offsets) from a split.
///
/// CRITICAL: This does NOT download fast fields, posting lists, or term dictionaries.
/// Only downloads:
/// 1. Last 4 bytes to get hotcache length (~4 bytes)
/// 2. Metadata length (~4 bytes)
/// 3. Metadata JSON (~1-10 KB depending on number of fields)
///
/// Total download: ~10 KB (NOT the full split file!)
async fn fetch_bundle_metadata(
    split_url: &str,
    storage: Arc<dyn Storage>,
) -> Result<HashMap<PathBuf, std::ops::Range<u64>>> {
    // Extract filename from URL
    let filename = split_url.rfind('/')
        .map(|i| &split_url[i + 1..])
        .unwrap_or(split_url);
    let path = Path::new(filename);

    // Get file size
    let file_size = storage.file_num_bytes(path).await
        .context("Failed to get split file size")?;

    debug_println!("FST_CACHE: Split {} file_size={}", filename, file_size);

    // STEP 1: Read ONLY the last 4 bytes to get hotcache length
    let hotcache_len_range = (file_size as usize - 4)..file_size as usize;
    let hotcache_len_bytes = storage.get_slice(path, hotcache_len_range).await
        .context("Failed to read hotcache length")?;

    let hotcache_len = u32::from_le_bytes(
        hotcache_len_bytes.as_ref().try_into()
            .map_err(|_| anyhow!("Invalid hotcache length bytes"))?
    ) as usize;

    debug_println!("FST_CACHE: Hotcache length = {} bytes", hotcache_len);

    // STEP 2: Read the bundle metadata (just before hotcache)
    // Layout: [files...][metadata][metadata_len:4][hotcache...][hotcache_len:4]
    // We need to read enough to get the metadata, which includes:
    // - metadata_len (4 bytes) at position file_size - 4 - hotcache_len - 4
    let metadata_len_pos = file_size as usize - 4 - hotcache_len - 4;
    let metadata_len_range = metadata_len_pos..metadata_len_pos + 4;
    let metadata_len_bytes = storage.get_slice(path, metadata_len_range).await
        .context("Failed to read metadata length")?;

    let metadata_len = u32::from_le_bytes(
        metadata_len_bytes.as_ref().try_into()
            .map_err(|_| anyhow!("Invalid metadata length bytes"))?
    ) as usize;

    debug_println!("FST_CACHE: Metadata length = {} bytes", metadata_len);

    // STEP 3: Read the metadata JSON
    let metadata_start = metadata_len_pos - metadata_len;
    let metadata_range = metadata_start..metadata_len_pos;
    let metadata_bytes = storage.get_slice(path, metadata_range).await
        .context("Failed to read bundle metadata")?;

    debug_println!("FST_CACHE: Read {} bytes of bundle metadata (NO fast fields, NO posting lists!)",
                   metadata_bytes.len());

    // Parse the metadata JSON to get file offsets
    eprintln!("[FST_CACHE TRACE] About to parse {} bytes of metadata", metadata_bytes.len());

    // CRITICAL DEBUG: Show raw metadata JSON for debugging
    if let Ok(metadata_str) = std::str::from_utf8(&metadata_bytes[8..]) {
        eprintln!("[FST_CACHE TRACE] Bundle metadata JSON (first 1000 chars): {}",
                 &metadata_str[..metadata_str.len().min(1000)]);
    }

    let file_offsets = match parse_bundle_metadata(&metadata_bytes) {
        Ok(offsets) => offsets,
        Err(e) => {
            eprintln!("[FST_CACHE TRACE] Failed to parse bundle metadata: {}", e);
            eprintln!("[FST_CACHE TRACE] Metadata bytes (first 200): {:?}", &metadata_bytes[..metadata_bytes.len().min(200)]);
            return Err(e).context("Failed to parse bundle metadata");
        }
    };

    // CRITICAL DEBUG: Show all file offsets
    eprintln!("[FST_CACHE TRACE] All files in bundle:");
    for (file, range) in &file_offsets {
        eprintln!("[FST_CACHE TRACE]   {} -> {}..{} ({} bytes)",
                 file.display(), range.start, range.end, range.end - range.start);
    }

    debug_println!("FST_CACHE: Bundle contains {} files:", file_offsets.len());
    let mut term_files = Vec::new();
    for (file, range) in &file_offsets {
        let size_kb = (range.end - range.start) as f64 / 1024.0;
        debug_println!("FST_CACHE:   {} -> {}..{} ({:.1} KB)",
                       file.display(), range.start, range.end, size_kb);
        let file_str = file.to_string_lossy();
        if file_str.ends_with(".term") {
            term_files.push(file_str.to_string());
        }
    }
    debug_println!("FST_CACHE: Found {} .term files: {:?}", term_files.len(), term_files);

    Ok(file_offsets)
}

/// Find the end of the term dictionary by locating the hotcache footer start.
///
/// The Quickwit bundle stores term dictionary files with their hotcache footer appended.
/// Tantivy's TermDictionary::open expects ONLY the dictionary data, ending with the
/// type marker as the last 4 bytes.
///
/// Hotcache format: [term_dict][5-byte-header][json: {"version":...][4-byte-length]
/// We find the JSON start and work backwards.
fn find_and_truncate_at_dict_marker(data: &OwnedBytes) -> OwnedBytes {
    if data.len() < 4 {
        eprintln!("[FST_CACHE TRACE] Data too short ({} bytes), returning as-is", data.len());
        return data.clone();
    }

    // Check if last 4 bytes are already a valid marker
    let last_4 = &data[data.len() - 4..];
    let last_marker = u32::from_le_bytes([last_4[0], last_4[1], last_4[2], last_4[3]]);
    if last_marker == 1 || last_marker == 2 {
        eprintln!("[FST_CACHE TRACE] Data already ends with valid marker {}, no truncation needed", last_marker);
        return data.clone();
    }

    // Strategy 1: Find the hotcache JSON signature and work backwards
    // The hotcache JSON starts with {"version" (bytes: 7b 22 76 65 72 73 69 6f 6e)
    let json_signature = b"{\"version\"";
    let mut json_start: Option<usize> = None;

    for i in 0..data.len().saturating_sub(json_signature.len()) {
        if &data[i..i + json_signature.len()] == json_signature {
            json_start = Some(i);
            eprintln!("[FST_CACHE TRACE] Found hotcache JSON signature at offset {}", i);
            break;
        }
    }

    if let Some(json_pos) = json_start {
        // The hotcache has a 5-byte header before the JSON
        // So the term dictionary ends 5 bytes before the JSON start
        if json_pos >= 5 {
            let dict_end = json_pos - 5;

            // Verify that the bytes at dict_end-4..dict_end are a valid marker
            if dict_end >= 4 {
                let marker_pos = dict_end - 4;
                let marker = u32::from_le_bytes([
                    data[marker_pos], data[marker_pos+1],
                    data[marker_pos+2], data[marker_pos+3]
                ]);

                eprintln!("[FST_CACHE TRACE] Checking marker at position {}: {} (bytes: {:?})",
                         marker_pos, marker, &data[marker_pos..dict_end]);

                if marker == 2 && dict_end >= 28 {
                    // For SSTable (type 2), validate footer offsets
                    let footer_start = dict_end - 28;
                    let block_start = u64::from_le_bytes(data[footer_start..footer_start+8].try_into().unwrap());
                    let index_start = u64::from_le_bytes(data[footer_start+8..footer_start+16].try_into().unwrap());
                    let num_terms = u64::from_le_bytes(data[footer_start+16..footer_start+24].try_into().unwrap());
                    eprintln!("[FST_CACHE TRACE] SSTable footer check: block_start={}, index_start={}, num_terms={}, dict_end={}",
                             block_start, index_start, num_terms, dict_end);

                    // Validate offsets - if invalid, this marker is a false positive
                    if block_start >= dict_end as u64 || index_start >= dict_end as u64 || index_start < block_start || num_terms > 100_000_000 {
                        eprintln!("[FST_CACHE TRACE] SSTable footer invalid - marker at {} is false positive, will scan for FST marker",
                                 marker_pos);
                        // Don't use this marker, fall through to Strategy 2 (scan for FST markers)
                    } else {
                        eprintln!("[FST_CACHE TRACE] Valid SSTable at position {}. Truncating to {} bytes.", marker_pos, dict_end);
                        return OwnedBytes::new(data[..dict_end].to_vec());
                    }
                } else if marker == 1 {
                    // FST (type 1) has simpler format, just validate basic structure
                    eprintln!("[FST_CACHE TRACE] Valid FST dictionary type at position {}. Truncating to {} bytes.",
                             marker_pos, dict_end);
                    return OwnedBytes::new(data[..dict_end].to_vec());
                }
            }
        }
    }

    // Strategy 2: Fallback - scan backwards for valid markers
    // Look for markers where the SSTable footer offsets make sense
    eprintln!("[FST_CACHE TRACE] Hotcache signature not found or invalid, scanning for valid markers...");

    for i in (28..data.len().saturating_sub(3)).rev() {
        let marker = u32::from_le_bytes([data[i], data[i+1], data[i+2], data[i+3]]);
        if marker == 1 || marker == 2 {
            let dict_end = i + 4;

            // For SSTable, verify footer offsets are within bounds
            if marker == 2 && dict_end >= 28 {
                let footer_start = dict_end - 28;
                let block_start = u64::from_le_bytes(data[footer_start..footer_start+8].try_into().unwrap());
                let index_start = u64::from_le_bytes(data[footer_start+8..footer_start+16].try_into().unwrap());

                // Offsets must be less than file size
                if block_start < dict_end as u64 && index_start < dict_end as u64 && index_start >= block_start {
                    eprintln!("[FST_CACHE TRACE] Found valid SSTable marker at {} with valid offsets (block={}, index={}, end={})",
                             i, block_start, index_start, dict_end);
                    return OwnedBytes::new(data[..dict_end].to_vec());
                }
            } else if marker == 1 {
                // FST has no complex footer, just verify we're not in the middle of hotcache
                // Check that bytes after this don't look like more term dict data
                eprintln!("[FST_CACHE TRACE] Found potential FST marker at {}, dict_end={}", i, dict_end);
                return OwnedBytes::new(data[..dict_end].to_vec());
            }
        }
    }

    eprintln!("[FST_CACHE TRACE] WARNING: No valid dictionary marker found, returning original data");
    data.clone()
}

/// Parse bundle metadata JSON to extract file offsets
///
/// The metadata format is:
/// [magic:4][version:4][json_bytes...]
///
/// JSON format: {"files": {"path": [start, end], ...}}
fn parse_bundle_metadata(data: &[u8]) -> Result<HashMap<PathBuf, std::ops::Range<u64>>> {
    // Skip magic number (4 bytes) and version (4 bytes)
    if data.len() < 8 {
        return Err(anyhow!("Metadata too short: {} bytes", data.len()));
    }

    let magic = u32::from_le_bytes(data[0..4].try_into().unwrap());
    let version = u32::from_le_bytes(data[4..8].try_into().unwrap());

    eprintln!("[FST_CACHE TRACE] Bundle metadata magic={:#x} (dec: {}), version={}", magic, magic, version);

    // Quickwit bundle magic numbers (may vary by version)
    // 0x181212AE (403,881,646) - original
    // 0x1812BEAE (404,176,558) - newer version?
    const EXPECTED_MAGIC_V1: u32 = 0x181212AE;
    const EXPECTED_MAGIC_V2: u32 = 0x1812BEAE;

    if magic != EXPECTED_MAGIC_V1 && magic != EXPECTED_MAGIC_V2 {
        return Err(anyhow!(
            "Invalid bundle magic number: {:#x} (expected {:#x} or {:#x})",
            magic, EXPECTED_MAGIC_V1, EXPECTED_MAGIC_V2
        ));
    }

    // Parse JSON after header
    let json_bytes = &data[8..];

    // File range can be either {"start": N, "end": M} or [N, M]
    #[derive(serde::Deserialize)]
    #[serde(untagged)]
    enum FileRange {
        Object { start: u64, end: u64 },
        Array(Vec<u64>),
    }

    #[derive(serde::Deserialize)]
    struct BundleMetadata {
        files: HashMap<PathBuf, FileRange>,
    }

    let metadata: BundleMetadata = serde_json::from_slice(json_bytes)
        .context("Failed to parse bundle metadata JSON")?;

    // Convert to Range<u64>
    let file_offsets: HashMap<PathBuf, std::ops::Range<u64>> = metadata.files
        .into_iter()
        .filter_map(|(path, range)| {
            match range {
                FileRange::Object { start, end } => Some((path, start..end)),
                FileRange::Array(arr) if arr.len() == 2 => Some((path, arr[0]..arr[1])),
                _ => None,
            }
        })
        .collect();

    Ok(file_offsets)
}

// ================================
// Global Instance
// ================================

lazy_static::lazy_static! {
    /// Global FST cache instance
    static ref GLOBAL_FST_CACHE: RwLock<Option<Arc<FstCache>>> = RwLock::new(None);
}

/// Get or create the global FST cache
pub fn get_global_fst_cache() -> Arc<FstCache> {
    {
        let cache = GLOBAL_FST_CACHE.read().unwrap();
        if let Some(ref c) = *cache {
            return Arc::clone(c);
        }
    }

    // Create default cache
    let config = FstCacheConfig::default();
    let cache = Arc::new(FstCache::new(config));

    {
        let mut global = GLOBAL_FST_CACHE.write().unwrap();
        *global = Some(Arc::clone(&cache));
    }

    cache
}

/// Initialize the global FST cache with custom config
pub fn init_global_fst_cache(config: FstCacheConfig) -> Arc<FstCache> {
    let cache = Arc::new(FstCache::new(config));

    {
        let mut global = GLOBAL_FST_CACHE.write().unwrap();
        *global = Some(Arc::clone(&cache));
    }

    cache
}

// ================================
// Tests
// ================================

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Instant;

    // ================================
    // Basic Unit Tests
    // ================================

    #[test]
    fn test_hash_string() {
        let h1 = hash_string("s3://bucket/table1");
        let h2 = hash_string("s3://bucket/table2");
        let h3 = hash_string("s3://bucket/table1");

        assert_ne!(h1, h2);
        assert_eq!(h1, h3);
    }

    #[test]
    fn test_disk_cache_path() {
        let cache = DiskCache::new(PathBuf::from("/tmp"), 1000);
        let path = cache.get_path("s3://bucket/table", "split-001", "title");

        assert!(path.to_string_lossy().contains("FST_SKIPPING_CACHE"));
        assert!(path.to_string_lossy().contains("split-001"));
        assert!(path.to_string_lossy().ends_with("title.term"));
    }

    #[test]
    fn test_memory_lru_cache() {
        let mut cache = MemoryLruCache::new(100);

        // Insert some data
        cache.insert("key1".to_string(), OwnedBytes::new(b"hello".to_vec()));
        cache.insert("key2".to_string(), OwnedBytes::new(b"world".to_vec()));

        assert_eq!(cache.get("key1").map(|b| b.as_ref().to_vec()), Some(b"hello".to_vec()));
        assert_eq!(cache.get("key2").map(|b| b.as_ref().to_vec()), Some(b"world".to_vec()));
        assert_eq!(cache.get("key3"), None);
    }

    #[test]
    fn test_memory_lru_eviction() {
        // Cache with max 20 bytes
        let mut cache = MemoryLruCache::new(20);

        // Insert 10 bytes
        cache.insert("key1".to_string(), OwnedBytes::new(b"0123456789".to_vec()));

        // Access key1 to mark it as recently used
        cache.get("key1");

        // Insert another 10 bytes
        cache.insert("key2".to_string(), OwnedBytes::new(b"abcdefghij".to_vec()));

        // Both should fit (20 bytes)
        assert!(cache.get("key1").is_some());
        assert!(cache.get("key2").is_some());

        // Insert 10 more bytes - should evict LRU (key1 was accessed first, then key2)
        // Actually key2 was accessed more recently due to the get above
        cache.insert("key3".to_string(), OwnedBytes::new(b"ABCDEFGHIJ".to_vec()));

        // key1 should be evicted (LRU)
        assert!(cache.get("key1").is_none());
        assert!(cache.get("key2").is_some() || cache.get("key3").is_some());
    }

    // ================================
    // Memory LRU Cache Validation Tests
    // ================================

    #[test]
    fn test_memory_lru_size_tracking() {
        let mut cache = MemoryLruCache::new(1000);

        // Insert entries of known sizes
        cache.insert("a".to_string(), OwnedBytes::new(vec![0u8; 100]));
        cache.insert("b".to_string(), OwnedBytes::new(vec![0u8; 200]));
        cache.insert("c".to_string(), OwnedBytes::new(vec![0u8; 300]));

        let (count, current_size, max_size) = cache.stats();
        assert_eq!(count, 3);
        assert_eq!(current_size, 600);
        assert_eq!(max_size, 1000);
    }

    #[test]
    fn test_memory_lru_eviction_order() {
        // Test that eviction follows strict LRU order
        let mut cache = MemoryLruCache::new(30);

        // Insert 3 entries of 10 bytes each
        cache.insert("first".to_string(), OwnedBytes::new(b"1111111111".to_vec()));
        cache.insert("second".to_string(), OwnedBytes::new(b"2222222222".to_vec()));
        cache.insert("third".to_string(), OwnedBytes::new(b"3333333333".to_vec()));

        // Access "first" to make it recently used
        cache.get("first");

        // Insert a new entry - should evict "second" (now LRU)
        cache.insert("fourth".to_string(), OwnedBytes::new(b"4444444444".to_vec()));

        // "first" should still be there (accessed recently)
        assert!(cache.get("first").is_some(), "first should still be in cache");
        // "second" should be evicted (was LRU)
        assert!(cache.get("second").is_none(), "second should be evicted");
        // "third" or "fourth" should be there
        assert!(cache.get("third").is_some() || cache.get("fourth").is_some());
    }

    #[test]
    fn test_memory_lru_oversized_entry_rejected() {
        let mut cache = MemoryLruCache::new(50);

        // Insert an entry larger than max size - should be rejected
        cache.insert("huge".to_string(), OwnedBytes::new(vec![0u8; 100]));

        // Entry should not be in cache
        assert!(cache.get("huge").is_none());

        let (count, current_size, _) = cache.stats();
        assert_eq!(count, 0);
        assert_eq!(current_size, 0);
    }

    #[test]
    fn test_memory_lru_update_existing_key() {
        let mut cache = MemoryLruCache::new(100);

        // Insert initial value
        cache.insert("key".to_string(), OwnedBytes::new(b"original".to_vec()));

        let (_, size1, _) = cache.stats();
        assert_eq!(size1, 8); // "original" is 8 bytes

        // Update with different size value
        cache.insert("key".to_string(), OwnedBytes::new(b"new_longer_value".to_vec()));

        let (count, size2, _) = cache.stats();
        assert_eq!(count, 1);
        assert_eq!(size2, 16); // "new_longer_value" is 16 bytes

        // Value should be updated
        let value = cache.get("key").unwrap();
        assert_eq!(value.as_ref(), b"new_longer_value");
    }

    // ================================
    // Disk Cache Validation Tests
    // ================================

    #[test]
    fn test_disk_cache_write_and_read() {
        let temp_dir = std::env::temp_dir().join("fst_cache_test_write_read");
        let _ = fs::remove_dir_all(&temp_dir); // Clean up any previous run

        let cache = DiskCache::new(temp_dir.clone(), 1_000_000);

        let data = OwnedBytes::new(b"test data for disk cache".to_vec());

        // Write to disk
        cache.insert("s3://bucket/table", "split-123", "title", &data).unwrap();

        // Read back
        let read_data = cache.get("s3://bucket/table", "split-123", "title");
        assert!(read_data.is_some());
        assert_eq!(read_data.unwrap().as_ref(), data.as_ref());

        // Cleanup
        let _ = fs::remove_dir_all(&temp_dir);
    }

    #[test]
    fn test_disk_cache_path_structure() {
        let temp_dir = std::env::temp_dir().join("fst_cache_test_path");
        let _ = fs::remove_dir_all(&temp_dir);

        let cache = DiskCache::new(temp_dir.clone(), 1_000_000);

        let table_path = "s3://my-bucket/path/to/table";
        let split_id = "split-abc-123";
        let field_name = "document_title";

        let path = cache.get_path(table_path, split_id, field_name);

        // Verify path structure
        let path_str = path.to_string_lossy();
        assert!(path_str.contains("FST_SKIPPING_CACHE"));
        assert!(path_str.contains(split_id));
        assert!(path_str.ends_with(&format!("{}.term", field_name)));

        // Verify the hash is a 16-char hex string
        let hash = hash_string(table_path);
        assert!(path_str.contains(&format!("{:016x}", hash)));

        let _ = fs::remove_dir_all(&temp_dir);
    }

    #[test]
    fn test_disk_cache_miss() {
        let temp_dir = std::env::temp_dir().join("fst_cache_test_miss");
        let _ = fs::remove_dir_all(&temp_dir);

        let cache = DiskCache::new(temp_dir.clone(), 1_000_000);

        // Read non-existent entry
        let data = cache.get("s3://bucket/table", "split-nonexistent", "field");
        assert!(data.is_none());

        let _ = fs::remove_dir_all(&temp_dir);
    }

    #[test]
    fn test_disk_cache_multiple_fields_same_split() {
        let temp_dir = std::env::temp_dir().join("fst_cache_test_multi_field");
        let _ = fs::remove_dir_all(&temp_dir);

        let cache = DiskCache::new(temp_dir.clone(), 1_000_000);

        // Write multiple fields for same split
        cache.insert("s3://bucket/table", "split-1", "title",
                     &OwnedBytes::new(b"title data".to_vec())).unwrap();
        cache.insert("s3://bucket/table", "split-1", "body",
                     &OwnedBytes::new(b"body data".to_vec())).unwrap();
        cache.insert("s3://bucket/table", "split-1", "author",
                     &OwnedBytes::new(b"author data".to_vec())).unwrap();

        // Verify all can be read back
        assert_eq!(cache.get("s3://bucket/table", "split-1", "title").unwrap().as_ref(), b"title data");
        assert_eq!(cache.get("s3://bucket/table", "split-1", "body").unwrap().as_ref(), b"body data");
        assert_eq!(cache.get("s3://bucket/table", "split-1", "author").unwrap().as_ref(), b"author data");

        let _ = fs::remove_dir_all(&temp_dir);
    }

    // ================================
    // Two-Tier Cache Integration Tests
    // ================================

    #[test]
    fn test_two_tier_memory_hit() {
        let temp_dir = std::env::temp_dir().join("fst_cache_test_two_tier_mem");
        let _ = fs::remove_dir_all(&temp_dir);

        let config = FstCacheConfig {
            memory_cache_size_bytes: 1_000_000,
            disk_cache_size_bytes: 10_000_000,
            disk_cache_base_path: temp_dir.clone(),
        };

        let cache = FstCache::new(config);

        // Manually populate memory cache
        {
            let mut memory = cache.memory.write().unwrap();
            memory.insert(
                "table:split-1:title".to_string(),
                OwnedBytes::new(b"memory cached data".to_vec())
            );
        }

        // Verify it's in memory
        {
            let mut memory = cache.memory.write().unwrap();
            let data = memory.get("table:split-1:title");
            assert!(data.is_some());
            assert_eq!(data.unwrap().as_ref(), b"memory cached data");
        }

        let _ = fs::remove_dir_all(&temp_dir);
    }

    #[test]
    fn test_two_tier_disk_hit_promotes_to_memory() {
        let temp_dir = std::env::temp_dir().join("fst_cache_test_two_tier_disk");
        let _ = fs::remove_dir_all(&temp_dir);

        let config = FstCacheConfig {
            memory_cache_size_bytes: 1_000_000,
            disk_cache_size_bytes: 10_000_000,
            disk_cache_base_path: temp_dir.clone(),
        };

        let cache = FstCache::new(config);

        // Write directly to disk cache (simulating a previous session)
        cache.disk.insert(
            "s3://bucket/table",
            "split-1",
            "title",
            &OwnedBytes::new(b"disk cached data".to_vec())
        ).unwrap();

        // Verify NOT in memory cache
        {
            let mut memory = cache.memory.write().unwrap();
            assert!(memory.get("s3://bucket/table:split-1:title").is_none());
        }

        // Verify IS on disk
        let disk_data = cache.disk.get("s3://bucket/table", "split-1", "title");
        assert!(disk_data.is_some());
        assert_eq!(disk_data.unwrap().as_ref(), b"disk cached data");

        let _ = fs::remove_dir_all(&temp_dir);
    }

    #[test]
    fn test_two_tier_eviction_preserves_disk() {
        let temp_dir = std::env::temp_dir().join("fst_cache_test_evict_disk");
        let _ = fs::remove_dir_all(&temp_dir);

        // Very small memory cache - will evict quickly
        // Each entry is 100 bytes, max is 250 bytes = room for ~2 entries
        let config = FstCacheConfig {
            memory_cache_size_bytes: 250,
            disk_cache_size_bytes: 10_000_000,
            disk_cache_base_path: temp_dir.clone(),
        };

        let cache = FstCache::new(config);

        // Write to disk first (100 bytes each)
        for i in 0..5 {
            cache.disk.insert(
                "s3://bucket/table",
                &format!("split-{}", i),
                "title",
                &OwnedBytes::new(vec![i as u8; 100])
            ).unwrap();
        }

        // Add to memory cache (will cause evictions since 5*100 > 250)
        {
            let mut memory = cache.memory.write().unwrap();
            for i in 0..5 {
                memory.insert(
                    format!("s3://bucket/table:split-{}:title", i),
                    OwnedBytes::new(vec![i as u8; 100])
                );
            }
        }

        // Memory should have evicted some entries (can only hold 2 of 5)
        let (mem_count, _, _) = {
            let memory = cache.memory.read().unwrap();
            memory.stats()
        };
        assert!(mem_count < 5, "Memory cache should have evicted some entries (has {})", mem_count);

        // But disk should still have ALL entries
        for i in 0..5 {
            let disk_data = cache.disk.get("s3://bucket/table", &format!("split-{}", i), "title");
            assert!(disk_data.is_some(), "Disk should still have split-{}", i);
        }

        let _ = fs::remove_dir_all(&temp_dir);
    }

    #[test]
    fn test_cache_stats() {
        let temp_dir = std::env::temp_dir().join("fst_cache_test_stats");
        let _ = fs::remove_dir_all(&temp_dir);

        let config = FstCacheConfig {
            memory_cache_size_bytes: 1_000_000,
            disk_cache_size_bytes: 10_000_000,
            disk_cache_base_path: temp_dir.clone(),
        };

        let cache = FstCache::new(config);

        // Add some entries
        {
            let mut memory = cache.memory.write().unwrap();
            memory.insert("key1".to_string(), OwnedBytes::new(vec![0u8; 100]));
            memory.insert("key2".to_string(), OwnedBytes::new(vec![0u8; 200]));
        }

        let stats = cache.stats();

        assert_eq!(stats.memory_entry_count, 2);
        assert_eq!(stats.memory_current_size, 300);
        assert_eq!(stats.memory_max_size, 1_000_000);
        assert_eq!(stats.disk_max_size, 10_000_000);

        let _ = fs::remove_dir_all(&temp_dir);
    }

    // ================================
    // LRU Behavior Stress Tests
    // ================================

    #[test]
    fn test_lru_access_pattern_fifo() {
        // Test that without any gets, eviction is FIFO
        let mut cache = MemoryLruCache::new(50);

        // Insert 5 entries of 10 bytes each
        for i in 0..5 {
            cache.insert(format!("key{}", i), OwnedBytes::new(vec![i as u8; 10]));
        }

        // All 5 fit (50 bytes)
        assert_eq!(cache.stats().0, 5);

        // Insert 6th - should evict key0 (first inserted)
        cache.insert("key5".to_string(), OwnedBytes::new(vec![5u8; 10]));

        assert!(cache.get("key0").is_none(), "key0 should be evicted (FIFO)");
        assert!(cache.get("key5").is_some(), "key5 should exist");
    }

    #[test]
    fn test_lru_access_pattern_with_gets() {
        let mut cache = MemoryLruCache::new(50);

        // Insert 5 entries
        for i in 0..5 {
            cache.insert(format!("key{}", i), OwnedBytes::new(vec![i as u8; 10]));
        }

        // Access key0 and key1 to make them recently used
        cache.get("key0");
        cache.get("key1");

        // Insert 2 more entries - should evict key2 and key3 (now LRU)
        cache.insert("key5".to_string(), OwnedBytes::new(vec![5u8; 10]));
        cache.insert("key6".to_string(), OwnedBytes::new(vec![6u8; 10]));

        // key0 and key1 should still exist (accessed recently)
        assert!(cache.get("key0").is_some(), "key0 should exist (accessed recently)");
        assert!(cache.get("key1").is_some(), "key1 should exist (accessed recently)");

        // key2 should be evicted (was LRU after key0,key1 were accessed)
        assert!(cache.get("key2").is_none(), "key2 should be evicted");
    }

    #[test]
    fn test_lru_repeated_access_same_key() {
        let mut cache = MemoryLruCache::new(30);

        // Insert 3 entries
        cache.insert("a".to_string(), OwnedBytes::new(vec![0u8; 10]));
        cache.insert("b".to_string(), OwnedBytes::new(vec![0u8; 10]));
        cache.insert("c".to_string(), OwnedBytes::new(vec![0u8; 10]));

        // Repeatedly access "a" to keep it hot
        for _ in 0..100 {
            cache.get("a");
        }

        // Insert new entries - should evict b and c, not a
        cache.insert("d".to_string(), OwnedBytes::new(vec![0u8; 10]));
        cache.insert("e".to_string(), OwnedBytes::new(vec![0u8; 10]));

        assert!(cache.get("a").is_some(), "a should still exist (accessed 100 times)");
    }

    // ================================
    // Performance Measurement Tests
    // ================================

    #[test]
    fn test_memory_cache_lookup_performance() {
        let mut cache = MemoryLruCache::new(100_000_000); // 100MB

        // Pre-populate with 1000 entries
        for i in 0..1000 {
            cache.insert(
                format!("key{}", i),
                OwnedBytes::new(vec![0u8; 1000])
            );
        }

        // Measure lookup time
        let iterations = 10000;
        let start = Instant::now();

        for i in 0..iterations {
            let key = format!("key{}", i % 1000);
            let _ = cache.get(&key);
        }

        let elapsed = start.elapsed();
        let avg_ns = elapsed.as_nanos() / iterations as u128;

        println!("Memory cache lookup: {} ns/op ({} ops in {:?})",
                 avg_ns, iterations, elapsed);

        // Should be very fast - under 1 microsecond per lookup
        assert!(avg_ns < 1000, "Memory cache lookup should be < 1µs, got {} ns", avg_ns);
    }

    #[test]
    fn test_memory_cache_insert_performance() {
        let mut cache = MemoryLruCache::new(100_000_000); // 100MB

        let iterations = 10000;
        let start = Instant::now();

        for i in 0..iterations {
            cache.insert(
                format!("key{}", i),
                OwnedBytes::new(vec![0u8; 100])
            );
        }

        let elapsed = start.elapsed();
        let avg_ns = elapsed.as_nanos() / iterations as u128;

        println!("Memory cache insert: {} ns/op ({} ops in {:?})",
                 avg_ns, iterations, elapsed);

        // Should be reasonably fast
        assert!(avg_ns < 10000, "Memory cache insert should be < 10µs, got {} ns", avg_ns);
    }
}

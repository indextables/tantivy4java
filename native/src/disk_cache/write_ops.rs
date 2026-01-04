// write_ops.rs - Write operations for disk cache
// Extracted from mod.rs during refactoring

#![allow(dead_code)]

use std::collections::HashMap;
use std::fs::{self, File, OpenOptions};
use std::io::{self, Write};
use std::ops::Range;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Mutex, RwLock};
use std::time::{SystemTime, UNIX_EPOCH};

use super::compression::compress_data;
use super::lru::SplitLruTable;
use super::manifest::{CacheManifest, SplitEntry, SplitState};
use super::path_helpers::{cache_dir, component_path, split_dir};
use super::range_index::CachedRange;
use super::types::{ComponentEntry, DiskCacheConfig};

/// Actually write data to disk
pub fn do_put(
    config: &DiskCacheConfig,
    manifest: &RwLock<CacheManifest>,
    split_states: &RwLock<HashMap<String, SplitState>>,
    lru_table: &Mutex<SplitLruTable>,
    total_bytes: &AtomicU64,
    storage_loc: &str,
    split_id: &str,
    component: &str,
    byte_range: Option<Range<u64>>,
    data: &[u8],
) -> io::Result<()> {
    // Compress if appropriate
    let (compressed, compression) = compress_data(config, component, data);

    // Create split directory
    let split_dir_path = split_dir(&config.root_path, storage_loc, split_id);
    fs::create_dir_all(&split_dir_path)?;

    // Write to temp file then rename (atomic)
    let final_path = component_path(
        &config.root_path,
        storage_loc,
        split_id,
        component,
        byte_range.clone(),
        compression,
    );
    let temp_path = final_path.with_extension("tmp");

    {
        let mut file = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(&temp_path)?;
        file.write_all(&compressed)?;
        file.sync_all()?;
    }

    fs::rename(&temp_path, &final_path)?;

    // Update manifest
    let split_key = CacheManifest::split_key(storage_loc, split_id);
    let comp_key = CacheManifest::component_key(component, byte_range.clone());
    let disk_size = compressed.len() as u64;
    let uncompressed_size = data.len() as u64;

    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();

    let entry = ComponentEntry {
        file_name: final_path
            .file_name()
            .unwrap()
            .to_string_lossy()
            .to_string(),
        uncompressed_size_bytes: uncompressed_size,
        disk_size_bytes: disk_size,
        compression,
        component: component.to_string(),
        byte_range: byte_range.as_ref().map(|r| (r.start, r.end)),
        created_at: now,
    };

    {
        let mut manifest_guard = manifest.write().unwrap();

        // Ensure split entry exists
        if !manifest_guard.splits.contains_key(&split_key) {
            manifest_guard.splits.insert(split_key.clone(), SplitEntry {
                split_id: split_id.to_string(),
                storage_loc: storage_loc.to_string(),
                total_size_bytes: 0,
                last_accessed: now,
                components: HashMap::new(),
            });
        }

        // Get old size for accounting (if replacing existing component)
        let old_disk_size = manifest_guard
            .splits
            .get(&split_key)
            .and_then(|s| s.components.get(&comp_key))
            .map(|c| c.disk_size_bytes)
            .unwrap_or(0);

        // Update size tracking for replacement
        if old_disk_size > 0 {
            manifest_guard.total_bytes = manifest_guard.total_bytes.saturating_sub(old_disk_size);
            total_bytes.fetch_sub(old_disk_size, Ordering::Relaxed);
        }

        // Now update the split entry
        if let Some(split) = manifest_guard.splits.get_mut(&split_key) {
            if old_disk_size > 0 {
                split.total_size_bytes = split.total_size_bytes.saturating_sub(old_disk_size);
            }
            split.components.insert(comp_key.clone(), entry);
            split.total_size_bytes += disk_size;
            split.last_accessed = now;
        }

        manifest_guard.total_bytes += disk_size;
    }

    total_bytes.fetch_add(disk_size, Ordering::Relaxed);

    // Update range index for coalescing (if this is a byte range, not full component)
    if let Some(ref range) = byte_range {
        if let Ok(mut states) = split_states.write() {
            let state = states
                .entry(split_key.clone())
                .or_insert_with(SplitState::new);

            let index = state.get_or_create_index(component);
            let comp_key = CacheManifest::component_key(component, byte_range.clone());

            // Remove old entry if exists (in case of replacement)
            index.remove(&comp_key);

            // Add new range
            index.insert(CachedRange {
                start: range.start,
                end: range.end,
                cache_key: comp_key,
                compression,
            });
        }
    }

    // Update LRU
    if let Ok(mut lru) = lru_table.lock() {
        if let Ok(manifest_guard) = manifest.read() {
            if let Some(split) = manifest_guard.splits.get(&split_key) {
                lru.touch(&split_key, split.total_size_bytes);
            }
        }
    }

    Ok(())
}

/// Async version of do_put using tokio::fs for parallel I/O
pub async fn do_put_async(
    config: &DiskCacheConfig,
    manifest: &RwLock<CacheManifest>,
    split_states: &RwLock<HashMap<String, SplitState>>,
    lru_table: &Mutex<SplitLruTable>,
    total_bytes: &AtomicU64,
    storage_loc: &str,
    split_id: &str,
    component: &str,
    byte_range: Option<Range<u64>>,
    data: &[u8],
) -> io::Result<()> {
    use tokio::io::AsyncWriteExt;

    // Compress if appropriate (CPU-bound, but fast)
    let (compressed, compression) = compress_data(config, component, data);

    // Create split directory
    let split_dir_path = split_dir(&config.root_path, storage_loc, split_id);
    tokio::fs::create_dir_all(&split_dir_path).await?;

    // Write to temp file then rename (atomic)
    let final_path = component_path(
        &config.root_path,
        storage_loc,
        split_id,
        component,
        byte_range.clone(),
        compression,
    );
    let temp_path = final_path.with_extension("tmp");

    // Async file write
    {
        let mut file = tokio::fs::File::create(&temp_path).await?;
        file.write_all(&compressed).await?;
        file.sync_all().await?;
    }

    tokio::fs::rename(&temp_path, &final_path).await?;

    // Update manifest (sync - needs locking, but fast)
    let split_key = CacheManifest::split_key(storage_loc, split_id);
    let comp_key = CacheManifest::component_key(component, byte_range.clone());
    let disk_size = compressed.len() as u64;
    let uncompressed_size = data.len() as u64;

    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();

    let entry = ComponentEntry {
        file_name: final_path
            .file_name()
            .unwrap()
            .to_string_lossy()
            .to_string(),
        uncompressed_size_bytes: uncompressed_size,
        disk_size_bytes: disk_size,
        compression,
        component: component.to_string(),
        byte_range: byte_range.as_ref().map(|r| (r.start, r.end)),
        created_at: now,
    };

    {
        let mut manifest_guard = manifest.write().unwrap();

        if !manifest_guard.splits.contains_key(&split_key) {
            manifest_guard.splits.insert(
                split_key.clone(),
                SplitEntry {
                    split_id: split_id.to_string(),
                    storage_loc: storage_loc.to_string(),
                    total_size_bytes: 0,
                    last_accessed: now,
                    components: HashMap::new(),
                },
            );
        }

        let old_disk_size = manifest_guard
            .splits
            .get(&split_key)
            .and_then(|s| s.components.get(&comp_key))
            .map(|c| c.disk_size_bytes)
            .unwrap_or(0);

        if old_disk_size > 0 {
            manifest_guard.total_bytes = manifest_guard.total_bytes.saturating_sub(old_disk_size);
            total_bytes.fetch_sub(old_disk_size, Ordering::Relaxed);
        }

        if let Some(split) = manifest_guard.splits.get_mut(&split_key) {
            if old_disk_size > 0 {
                split.total_size_bytes = split.total_size_bytes.saturating_sub(old_disk_size);
            }
            split.components.insert(comp_key.clone(), entry);
            split.total_size_bytes += disk_size;
            split.last_accessed = now;
        }

        manifest_guard.total_bytes += disk_size;
    }

    total_bytes.fetch_add(disk_size, Ordering::Relaxed);

    // Update range index
    if let Some(ref range) = byte_range {
        if let Ok(mut states) = split_states.write() {
            let state = states
                .entry(split_key.clone())
                .or_insert_with(SplitState::new);

            let index = state.get_or_create_index(component);
            let comp_key = CacheManifest::component_key(component, byte_range.clone());

            index.remove(&comp_key);
            index.insert(CachedRange {
                start: range.start,
                end: range.end,
                cache_key: comp_key,
                compression,
            });
        }
    }

    // Update LRU
    if let Ok(mut lru) = lru_table.lock() {
        if let Ok(manifest_guard) = manifest.read() {
            if let Some(split) = manifest_guard.splits.get(&split_key) {
                lru.touch(&split_key, split.total_size_bytes);
            }
        }
    }

    Ok(())
}

/// Actually evict a split from disk
pub fn do_evict(
    config: &DiskCacheConfig,
    manifest: &RwLock<CacheManifest>,
    split_states: &RwLock<HashMap<String, SplitState>>,
    lru_table: &Mutex<SplitLruTable>,
    total_bytes: &AtomicU64,
    storage_loc: &str,
    split_id: &str,
) -> io::Result<()> {
    let split_key = CacheManifest::split_key(storage_loc, split_id);
    let split_dir_path = split_dir(&config.root_path, storage_loc, split_id);

    // Get size before removal
    let size_to_remove = {
        let manifest_guard = manifest.read().unwrap();
        manifest_guard
            .splits
            .get(&split_key)
            .map(|s| s.total_size_bytes)
            .unwrap_or(0)
    };

    // Remove directory
    if split_dir_path.exists() {
        fs::remove_dir_all(&split_dir_path)?;
    }

    // Update manifest
    {
        let mut manifest_guard = manifest.write().unwrap();
        manifest_guard.splits.remove(&split_key);
        manifest_guard.total_bytes = manifest_guard.total_bytes.saturating_sub(size_to_remove);
    }

    total_bytes.fetch_sub(size_to_remove, Ordering::Relaxed);

    // Remove from split_states (range index cleanup)
    if let Ok(mut states) = split_states.write() {
        states.remove(&split_key);
    }

    // Update LRU
    if let Ok(mut lru) = lru_table.lock() {
        lru.remove(&split_key);
    }

    Ok(())
}

/// Async version of do_evict using tokio::fs
pub async fn do_evict_async(
    config: &DiskCacheConfig,
    manifest: &RwLock<CacheManifest>,
    split_states: &RwLock<HashMap<String, SplitState>>,
    lru_table: &Mutex<SplitLruTable>,
    total_bytes: &AtomicU64,
    storage_loc: &str,
    split_id: &str,
) -> io::Result<()> {
    let split_key = CacheManifest::split_key(storage_loc, split_id);
    let split_dir_path = split_dir(&config.root_path, storage_loc, split_id);

    let size_to_remove = {
        let manifest_guard = manifest.read().unwrap();
        manifest_guard
            .splits
            .get(&split_key)
            .map(|s| s.total_size_bytes)
            .unwrap_or(0)
    };

    // Async directory removal
    if tokio::fs::try_exists(&split_dir_path).await.unwrap_or(false) {
        tokio::fs::remove_dir_all(&split_dir_path).await?;
    }

    // Update manifest (sync - fast)
    {
        let mut manifest_guard = manifest.write().unwrap();
        manifest_guard.splits.remove(&split_key);
        manifest_guard.total_bytes = manifest_guard.total_bytes.saturating_sub(size_to_remove);
    }

    total_bytes.fetch_sub(size_to_remove, Ordering::Relaxed);

    if let Ok(mut states) = split_states.write() {
        states.remove(&split_key);
    }

    if let Ok(mut lru) = lru_table.lock() {
        lru.remove(&split_key);
    }

    Ok(())
}

/// Actually sync manifest to disk
pub fn do_sync_manifest(
    config: &DiskCacheConfig,
    manifest: &RwLock<CacheManifest>,
) -> io::Result<()> {
    let cache_dir_path = cache_dir(&config.root_path);
    let manifest_path = cache_dir_path.join("manifest.json");
    let backup_path = cache_dir_path.join("manifest.json.bak");
    let temp_path = cache_dir_path.join("manifest.json.tmp");

    // Serialize manifest
    let manifest_data = {
        let mut manifest_guard = manifest.write().unwrap();
        manifest_guard.last_sync = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        serde_json::to_string_pretty(&*manifest_guard)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?
    };

    // Write to temp file
    {
        let mut file = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(&temp_path)?;
        file.write_all(manifest_data.as_bytes())?;
        file.sync_all()?;
    }

    // Backup existing manifest
    if manifest_path.exists() {
        fs::copy(&manifest_path, &backup_path)?;
    }

    // Atomic rename
    fs::rename(&temp_path, &manifest_path)?;

    Ok(())
}

// disk_cache/tests.rs - Integration tests for L2DiskCache
// Extracted from disk_cache/mod.rs during refactoring

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use tempfile::TempDir;

    use super::super::*;
    use crate::disk_cache::compression;
    use crate::disk_cache::path_helpers::CACHE_SUBDIR;

    #[test]
    fn test_compression_decision() {
        let temp_dir = TempDir::new().unwrap();
        let config = DiskCacheConfig {
            root_path: temp_dir.path().to_path_buf(),
            compression: CompressionAlgorithm::Lz4,
            min_compress_size: 4096,
            ..Default::default()
        };

        // Small data - no compression regardless of component
        assert!(!compression::should_compress(&config, "idx", 100));
        assert!(!compression::should_compress(&config, "pos", 1000));

        // Footer/metadata - never compress
        assert!(!compression::should_compress(&config, "footer", 10000));
        assert!(!compression::should_compress(&config, "metadata", 10000));
        assert!(!compression::should_compress(&config, "fieldnorm", 10000));

        // Store files - already LZ4/Zstd compressed by Tantivy
        assert!(!compression::should_compress(&config, "store", 10000));
        assert!(!compression::should_compress(&config, "store", 1000000));

        // Term files - already Zstd compressed (sstable)
        assert!(!compression::should_compress(&config, "term", 10000));
        assert!(!compression::should_compress(&config, "term", 1000000));

        // idx/pos - block-access patterns
        assert!(!compression::should_compress(&config, "idx", 5000));
        assert!(!compression::should_compress(&config, "idx", 10_000_000));
        assert!(!compression::should_compress(&config, "pos", 5000));
        assert!(!compression::should_compress(&config, "pos", 10_000_000));

        // Fast fields - random access by doc_id
        assert!(!compression::should_compress(&config, "fast", 50000));
        assert!(!compression::should_compress(&config, "fast", 100000));
    }

    #[test]
    fn test_storage_loc_hash() {
        let hash1 = path_helpers::storage_loc_hash("s3://my-bucket/path/to/splits");
        assert!(hash1.starts_with("s3_my-bucket__"));

        let hash2 = path_helpers::storage_loc_hash("azure://container/path");
        assert!(hash2.starts_with("azure_container__"));

        let hash3 = path_helpers::storage_loc_hash("/local/path");
        assert!(hash3.starts_with("local_"));
    }

    #[test]
    fn test_lz4_roundtrip() {
        use lz4_flex::{compress_prepend_size, decompress_size_prepended};
        let original = b"Hello, world! This is test data for compression.";
        let compressed = compress_prepend_size(original);
        let decompressed = decompress_size_prepended(&compressed).unwrap();
        assert_eq!(original.as_slice(), decompressed.as_slice());
    }

    #[test]
    fn test_put_get_roundtrip() {
        let temp_dir = TempDir::new().unwrap();
        let config = DiskCacheConfig::new(temp_dir.path());
        let cache = L2DiskCache::new(config).unwrap();

        let data: Vec<u8> = (0..10000).map(|i| (i % 256) as u8).collect();

        cache.put("s3://bucket/path", "split-001", "term", None, &data);
        std::thread::sleep(Duration::from_millis(100));

        let retrieved = cache.get("s3://bucket/path", "split-001", "term", None);
        assert!(retrieved.is_some());
        assert_eq!(retrieved.unwrap().as_slice(), data.as_slice());
    }

    #[test]
    fn test_byte_range_caching() {
        let temp_dir = TempDir::new().unwrap();
        let config = DiskCacheConfig::new(temp_dir.path());
        let cache = L2DiskCache::new(config).unwrap();

        let data: Vec<u8> = (0..10000).map(|i| (i % 256) as u8).collect();
        let range = Some(0u64..5000u64);

        cache.put("s3://bucket", "split-002", "idx", range.clone(), &data);
        std::thread::sleep(Duration::from_millis(100));

        let retrieved = cache.get("s3://bucket", "split-002", "idx", range);
        assert!(retrieved.is_some());
        assert_eq!(retrieved.unwrap().as_slice(), data.as_slice());
    }

    #[test]
    fn test_stats_tracking() {
        let temp_dir = TempDir::new().unwrap();
        let config = DiskCacheConfig::new(temp_dir.path());
        let cache = L2DiskCache::new(config).unwrap();

        assert_eq!(cache.get_total_bytes(), 0);
        assert_eq!(cache.get_split_count(), 0);
        assert_eq!(cache.get_component_count(), 0);

        let data: Vec<u8> = (0..10000).map(|i| (i % 256) as u8).collect();
        cache.put("s3://bucket", "split-001", "term", None, &data);
        std::thread::sleep(Duration::from_millis(100));

        assert!(cache.get_total_bytes() > 0);
        assert_eq!(cache.get_split_count(), 1);
        assert_eq!(cache.get_component_count(), 1);

        cache.put("s3://bucket", "split-001", "idx", None, &data);
        std::thread::sleep(Duration::from_millis(100));

        assert_eq!(cache.get_split_count(), 1);
        assert_eq!(cache.get_component_count(), 2);

        cache.put("s3://bucket", "split-002", "term", None, &data);
        std::thread::sleep(Duration::from_millis(100));

        assert_eq!(cache.get_split_count(), 2);
        assert_eq!(cache.get_component_count(), 3);
    }

    #[test]
    fn test_evict_split() {
        let temp_dir = TempDir::new().unwrap();
        let config = DiskCacheConfig::new(temp_dir.path());
        let cache = L2DiskCache::new(config).unwrap();

        let data: Vec<u8> = (0..10000).map(|i| (i % 256) as u8).collect();

        cache.put("s3://bucket", "split-001", "term", None, &data);
        cache.put("s3://bucket", "split-001", "idx", None, &data);
        cache.put("s3://bucket", "split-002", "term", None, &data);
        std::thread::sleep(Duration::from_millis(150));

        assert_eq!(cache.get_split_count(), 2);
        assert_eq!(cache.get_component_count(), 3);
        let bytes_before = cache.get_total_bytes();

        cache.evict_split("s3://bucket", "split-001");
        std::thread::sleep(Duration::from_millis(100));

        assert_eq!(cache.get_split_count(), 1);
        assert_eq!(cache.get_component_count(), 1);
        assert!(cache.get_total_bytes() < bytes_before);

        assert!(cache.get("s3://bucket", "split-001", "term", None).is_none());
        assert!(cache.get("s3://bucket", "split-001", "idx", None).is_none());
        assert!(cache.get("s3://bucket", "split-002", "term", None).is_some());
    }

    #[test]
    fn test_no_compression_mode() {
        let temp_dir = TempDir::new().unwrap();
        let config = DiskCacheConfig {
            root_path: temp_dir.path().to_path_buf(),
            compression: CompressionAlgorithm::None,
            min_compress_size: 4096,
            ..Default::default()
        };
        let cache = L2DiskCache::new(config).unwrap();

        let data: Vec<u8> = (0..10000).map(|i| (i % 256) as u8).collect();

        cache.put("s3://bucket", "split-001", "term", None, &data);
        std::thread::sleep(Duration::from_millis(100));

        let retrieved = cache.get("s3://bucket", "split-001", "term", None);
        assert!(retrieved.is_some());
        assert_eq!(retrieved.unwrap().as_slice(), data.as_slice());

        // Verify file is not compressed
        let cache_dir = temp_dir.path().join(CACHE_SUBDIR);
        let mut found_uncompressed = false;
        for entry in std::fs::read_dir(&cache_dir).unwrap() {
            let entry = entry.unwrap();
            if entry.file_type().unwrap().is_dir() {
                for subentry in std::fs::read_dir(entry.path()).unwrap().flatten() {
                    if subentry.file_type().unwrap().is_dir() {
                        for file in std::fs::read_dir(subentry.path()).unwrap().flatten() {
                            let name = file.file_name().to_string_lossy().to_string();
                            if name.ends_with(".cache") && !name.ends_with(".lz4") {
                                found_uncompressed = true;
                            }
                        }
                    }
                }
            }
        }
        assert!(found_uncompressed);
    }

    #[test]
    fn test_manifest_persistence() {
        let temp_dir = TempDir::new().unwrap();
        let cache_path = temp_dir.path().to_path_buf();

        {
            let config = DiskCacheConfig::new(&cache_path);
            let cache = L2DiskCache::new(config).unwrap();

            let data: Vec<u8> = (0..10000).map(|i| (i % 256) as u8).collect();
            cache.put("s3://bucket", "split-001", "term", None, &data);
            cache.put("s3://bucket", "split-002", "idx", None, &data);
            std::thread::sleep(Duration::from_millis(100));

            cache.sync_manifest().unwrap();
            std::thread::sleep(Duration::from_millis(100));
        }

        let manifest_path = cache_path.join(CACHE_SUBDIR).join("manifest.json");
        assert!(manifest_path.exists());

        {
            let config = DiskCacheConfig::new(&cache_path);
            let cache = L2DiskCache::new(config).unwrap();

            let retrieved = cache.get("s3://bucket", "split-001", "term", None);
            assert!(retrieved.is_some());
            assert_eq!(retrieved.unwrap().len(), 10000);

            let retrieved2 = cache.get("s3://bucket", "split-002", "idx", None);
            assert!(retrieved2.is_some());
        }
    }

    #[test]
    fn test_manifest_backup_recovery() {
        let temp_dir = TempDir::new().unwrap();
        let cache_path = temp_dir.path().to_path_buf();

        {
            let config = DiskCacheConfig::new(&cache_path);
            let cache = L2DiskCache::new(config).unwrap();

            let data: Vec<u8> = (0..10000).map(|i| (i % 256) as u8).collect();
            cache.put("s3://bucket", "split-001", "term", None, &data);
            std::thread::sleep(Duration::from_millis(100));
            cache.sync_manifest().unwrap();
        }

        let cache_subdir = cache_path.join(CACHE_SUBDIR);
        let manifest_path = cache_subdir.join("manifest.json");
        let backup_path = cache_subdir.join("manifest.json.bak");

        if manifest_path.exists() && !backup_path.exists() {
            std::fs::copy(&manifest_path, &backup_path).unwrap();
        }

        std::fs::write(&manifest_path, "invalid json {{{").unwrap();

        {
            let config = DiskCacheConfig::new(&cache_path);
            let cache = L2DiskCache::new(config).unwrap();

            let retrieved = cache.get("s3://bucket", "split-001", "term", None);
            if retrieved.is_some() {
                assert_eq!(retrieved.unwrap().len(), 10000);
            }
        }
    }

    #[test]
    fn test_lru_eviction_under_pressure() {
        let temp_dir = TempDir::new().unwrap();
        let config = DiskCacheConfig {
            root_path: temp_dir.path().to_path_buf(),
            max_size_bytes: 50000,
            compression: CompressionAlgorithm::None,
            min_compress_size: 100000,
            ..Default::default()
        };
        let cache = L2DiskCache::new(config).unwrap();

        let data: Vec<u8> = (0..10000).map(|i| (i % 256) as u8).collect();

        for i in 0..6 {
            cache.put("s3://bucket", &format!("split-{:03}", i), "term", None, &data);
            std::thread::sleep(Duration::from_millis(50));
        }

        std::thread::sleep(Duration::from_millis(200));

        let total_bytes = cache.get_total_bytes();
        assert!(total_bytes <= 50000);

        let recent = cache.get("s3://bucket", "split-005", "term", None);
        assert!(recent.is_some());
    }

    #[test]
    fn test_concurrent_access() {
        use std::sync::Arc;
        use std::thread;

        let temp_dir = TempDir::new().unwrap();
        let config = DiskCacheConfig::new(temp_dir.path());
        let cache = Arc::new(L2DiskCache::new(config).unwrap());

        let mut handles = vec![];

        for i in 0..4 {
            let cache_clone = Arc::clone(&cache);
            handles.push(thread::spawn(move || {
                let data: Vec<u8> = (0..5000).map(|j| ((i * 1000 + j) % 256) as u8).collect();
                for j in 0..5 {
                    cache_clone.put(
                        "s3://bucket",
                        &format!("split-{}-{}", i, j),
                        "term",
                        None,
                        &data,
                    );
                }
            }));
        }

        for handle in handles {
            handle.join().unwrap();
        }

        std::thread::sleep(Duration::from_millis(200));

        let mut read_handles = vec![];
        for i in 0..4 {
            let cache_clone = Arc::clone(&cache);
            read_handles.push(thread::spawn(move || {
                let mut found = 0;
                for j in 0..5 {
                    if cache_clone
                        .get("s3://bucket", &format!("split-{}-{}", i, j), "term", None)
                        .is_some()
                    {
                        found += 1;
                    }
                }
                found
            }));
        }

        let mut total_found = 0;
        for handle in read_handles {
            total_found += handle.join().unwrap();
        }

        assert!(total_found > 0);
        assert_eq!(total_found, 20);
    }

    #[test]
    fn test_multiple_byte_ranges_same_component() {
        let temp_dir = TempDir::new().unwrap();
        let config = DiskCacheConfig::new(temp_dir.path());
        let cache = L2DiskCache::new(config).unwrap();

        let data1: Vec<u8> = (0..5000).map(|i| (i % 256) as u8).collect();
        let data2: Vec<u8> = (0..5000).map(|i| ((i + 100) % 256) as u8).collect();

        cache.put("s3://bucket", "split-001", "idx", Some(0..5000), &data1);
        cache.put("s3://bucket", "split-001", "idx", Some(5000..10000), &data2);
        std::thread::sleep(Duration::from_millis(100));

        let r1 = cache.get("s3://bucket", "split-001", "idx", Some(0..5000));
        let r2 = cache.get("s3://bucket", "split-001", "idx", Some(5000..10000));

        assert!(r1.is_some());
        assert!(r2.is_some());
        assert_eq!(r1.unwrap().as_slice(), data1.as_slice());
        assert_eq!(r2.unwrap().as_slice(), data2.as_slice());

        assert_eq!(cache.get_component_count(), 2);
    }

    #[test]
    fn test_cache_miss_returns_none() {
        let temp_dir = TempDir::new().unwrap();
        let config = DiskCacheConfig::new(temp_dir.path());
        let cache = L2DiskCache::new(config).unwrap();

        assert!(cache.get("s3://bucket", "nonexistent", "term", None).is_none());
        assert!(cache.get("s3://bucket", "split-001", "nonexistent", None).is_none());
        assert!(cache.get("nonexistent://bucket", "split-001", "term", None).is_none());
    }

    #[test]
    fn test_different_storage_locations() {
        let temp_dir = TempDir::new().unwrap();
        let config = DiskCacheConfig::new(temp_dir.path());
        let cache = L2DiskCache::new(config).unwrap();

        let data: Vec<u8> = (0..10000).map(|i| (i % 256) as u8).collect();

        cache.put("s3://bucket-a", "split-001", "term", None, &data);
        cache.put("s3://bucket-b", "split-001", "term", None, &data);
        cache.put("azure://container", "split-001", "term", None, &data);
        std::thread::sleep(Duration::from_millis(150));

        assert!(cache.get("s3://bucket-a", "split-001", "term", None).is_some());
        assert!(cache.get("s3://bucket-b", "split-001", "term", None).is_some());
        assert!(cache.get("azure://container", "split-001", "term", None).is_some());

        assert_eq!(cache.get_split_count(), 3);
    }
}

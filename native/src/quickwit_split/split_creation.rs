// split_creation.rs - Split file creation using Quickwit's SplitPayloadBuilder
// Extracted from mod.rs during refactoring
// Contains: FooterOffsets struct, create_quickwit_split function

use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};

use anyhow::{Context, Result};
use tokio::io::AsyncWriteExt;

use quickwit_storage::{PutPayload, SplitPayloadBuilder};
use quickwit_directories::{DebugProxyDirectory, StaticDirectoryCacheBuilder, list_index_files};

use crate::debug_println;
use super::{SplitConfig, QuickwitSplitMetadata};

// Debug logging macro - controlled by TANTIVY4JAVA_DEBUG environment variable
macro_rules! debug_log {
    ($($arg:tt)*) => {
        debug_println!("DEBUG: {}", format!($($arg)*))
    };
}

// Memory profiling - controlled by TANTIVY4JAVA_MEMORY_PROFILE environment variable
static MEMORY_PROFILE_ENABLED: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
static HOTCACHE_BYTES_COPIED: AtomicUsize = AtomicUsize::new(0);
static HOTCACHE_FILES_PROCESSED: AtomicUsize = AtomicUsize::new(0);

pub fn is_memory_profile_enabled() -> bool {
    *MEMORY_PROFILE_ENABLED.get_or_init(|| {
        std::env::var("TANTIVY4JAVA_MEMORY_PROFILE").map(|v| v == "1").unwrap_or(false)
    })
}

macro_rules! memory_profile {
    ($($arg:tt)*) => {
        if is_memory_profile_enabled() {
            eprintln!("📊 MEMORY_PROFILE: {}", format!($($arg)*));
        }
    };
}

/// Get current process RSS (Resident Set Size) in bytes on supported platforms
fn get_rss_bytes() -> Option<usize> {
    #[cfg(target_os = "macos")]
    {
        use std::mem::MaybeUninit;
        let mut rusage = MaybeUninit::<libc::rusage>::uninit();
        let ret = unsafe { libc::getrusage(libc::RUSAGE_SELF, rusage.as_mut_ptr()) };
        if ret == 0 {
            let rusage = unsafe { rusage.assume_init() };
            // On macOS, ru_maxrss is in bytes
            Some(rusage.ru_maxrss as usize)
        } else {
            None
        }
    }
    #[cfg(target_os = "linux")]
    {
        // Read from /proc/self/statm
        if let Ok(statm) = std::fs::read_to_string("/proc/self/statm") {
            let parts: Vec<&str> = statm.split_whitespace().collect();
            if parts.len() >= 2 {
                // Second field is RSS in pages
                if let Ok(pages) = parts[1].parse::<usize>() {
                    let page_size = unsafe { libc::sysconf(libc::_SC_PAGESIZE) as usize };
                    return Some(pages * page_size);
                }
            }
        }
        None
    }
    #[cfg(not(any(target_os = "macos", target_os = "linux")))]
    {
        None
    }
}

fn format_bytes(bytes: usize) -> String {
    if bytes >= 1024 * 1024 * 1024 {
        format!("{:.2} GB", bytes as f64 / (1024.0 * 1024.0 * 1024.0))
    } else if bytes >= 1024 * 1024 {
        format!("{:.2} MB", bytes as f64 / (1024.0 * 1024.0))
    } else if bytes >= 1024 {
        format!("{:.2} KB", bytes as f64 / 1024.0)
    } else {
        format!("{} bytes", bytes)
    }
}

pub fn log_memory_checkpoint(label: &str) {
    if is_memory_profile_enabled() {
        if let Some(rss) = get_rss_bytes() {
            memory_profile!("[{}] Process RSS: {}", label, format_bytes(rss));
        }
    }
}

/// Footer offset information for lazy loading optimization
#[derive(Debug, Clone)]
pub struct FooterOffsets {
    pub footer_start_offset: u64,    // Where metadata begins (excludes hotcache)
    pub footer_end_offset: u64,      // End of file
    pub hotcache_start_offset: u64,  // Where hotcache begins
    pub hotcache_length: u64,        // Size of hotcache
}

/// Memory-bounded hotcache generation using mmap-only reads.
///
/// Unlike Quickwit's write_hotcache(), this does NOT wrap the directory
/// in CachingDirectory, avoiding O(index_size) heap allocation.
///
/// # Memory Usage
/// - Before (CachingDirectory): O(index_size) - caches ALL data read during traversal
/// - After (direct mmap): O(hotcache_output_size) - only copies hotcache-worthy slices
///
/// For a 4GB index, this reduces peak memory from ~4GB to ~10-50MB.
pub fn write_hotcache_mmap<W: std::io::Write>(
    directory: tantivy::directory::MmapDirectory,
    output: &mut W,
) -> tantivy::Result<()> {
    use std::collections::{HashMap, HashSet};
    use tantivy::ReloadPolicy;
    use tantivy::directory::Directory;
    use tantivy::HasLen;

    debug_log!("🔧 MEMORY-BOUNDED HOTCACHE: Using mmap-only reads (no CachingDirectory)");
    log_memory_checkpoint("hotcache_start");

    // Reset counters for this hotcache generation
    HOTCACHE_BYTES_COPIED.store(0, Ordering::SeqCst);
    HOTCACHE_FILES_PROCESSED.store(0, Ordering::SeqCst);

    // ✅ KEY DIFFERENCE: Use DebugProxyDirectory directly on MmapDirectory
    // NO CachingDirectory wrapper - reads stay mmap-backed (zero heap copy)
    let debug_proxy_directory = DebugProxyDirectory::wrap(directory);

    let index = tantivy::Index::open(debug_proxy_directory.clone())?;
    let schema = index.schema();
    let reader = index
        .reader_builder()
        .reload_policy(ReloadPolicy::Manual)
        .try_into()?;
    let searcher = reader.searcher();

    log_memory_checkpoint("hotcache_after_index_open");

    // Touch all indexed fields to record what needs to be in hotcache
    // Data is read via mmap (no heap allocation)
    for (field, field_entry) in schema.fields() {
        if !field_entry.is_indexed() {
            continue;
        }
        for segment_reader in searcher.segment_readers() {
            let _inv_idx = segment_reader.inverted_index(field)?;
        }
    }

    log_memory_checkpoint("hotcache_after_field_touch");

    // Collect what was read (just metadata, not data)
    let mut cache_builder = StaticDirectoryCacheBuilder::default();
    let read_operations = debug_proxy_directory.drain_read_operations();
    let mut per_file_slices: HashMap<std::path::PathBuf, HashSet<std::ops::Range<usize>>> = HashMap::default();

    // Track total bytes that WILL be copied
    let mut total_bytes_to_copy: usize = 0;

    for read_op in read_operations {
        per_file_slices
            .entry(read_op.path)
            .or_default()
            .insert(read_op.offset..read_op.offset + read_op.num_bytes);
    }

    debug_log!("🔧 MEMORY-BOUNDED HOTCACHE: Collected {} files with read operations", per_file_slices.len());

    // First pass: calculate total bytes that will be copied
    if is_memory_profile_enabled() {
        for (file_path, intervals) in &per_file_slices {
            let file_path_str = file_path.to_string_lossy();
            for byte_range in intervals {
                let len = byte_range.len();
                if file_path_str.ends_with("store")
                    || file_path_str.ends_with("term")
                    || len < 10_000_000
                {
                    total_bytes_to_copy += len;
                }
            }
        }
        memory_profile!("Total bytes to copy to hotcache: {} ({} files)",
            format_bytes(total_bytes_to_copy), per_file_slices.len());
    }

    // Build hotcache by reading slices directly from mmap (zero-copy read)
    let index_files = list_index_files(&index)?;
    for file_path in index_files {
        let file_slice_res = debug_proxy_directory.open_read(&file_path);
        if let Err(tantivy::directory::error::OpenReadError::FileDoesNotExist(_)) = file_slice_res {
            continue;
        }
        let file_slice = file_slice_res?;
        let file_cache_builder = cache_builder.add_file(&file_path, file_slice.len() as u64);

        let mut file_bytes_copied: usize = 0;

        if let Some(intervals) = per_file_slices.get(&file_path) {
            for byte_range in intervals {
                let len = byte_range.len();
                let file_path_str = file_path.to_string_lossy();
                // Only include slices < 10MB, or required files (store, term)
                if file_path_str.ends_with("store")
                    || file_path_str.ends_with("term")
                    || len < 10_000_000
                {
                    // ✅ Direct mmap read - data comes from OS page cache, not heap
                    let bytes = file_slice.read_bytes_slice(byte_range.clone())?;

                    // Track this copy for profiling
                    file_bytes_copied += bytes.len();
                    HOTCACHE_BYTES_COPIED.fetch_add(bytes.len(), Ordering::SeqCst);

                    // THIS IS THE COPY - add_bytes copies to internal Vec
                    file_cache_builder.add_bytes(bytes.as_slice(), byte_range.start);
                }
            }
        }

        if is_memory_profile_enabled() && file_bytes_copied > 0 {
            memory_profile!("  File '{}': copied {} to hotcache (file total: {})",
                file_path.display(),
                format_bytes(file_bytes_copied),
                format_bytes(file_slice.len()));
            HOTCACHE_FILES_PROCESSED.fetch_add(1, Ordering::SeqCst);
        }
    }

    log_memory_checkpoint("hotcache_after_add_bytes");

    let total_copied = HOTCACHE_BYTES_COPIED.load(Ordering::SeqCst);
    let files_processed = HOTCACHE_FILES_PROCESSED.load(Ordering::SeqCst);
    memory_profile!("Hotcache add_bytes complete: {} total from {} files",
        format_bytes(total_copied), files_processed);

    cache_builder.write(output)?;
    output.flush()?;

    log_memory_checkpoint("hotcache_after_write");

    debug_log!("✅ MEMORY-BOUNDED HOTCACHE: Generation complete");
    Ok(())
}

/// Create a Quickwit split file from a Tantivy index directory
pub async fn create_quickwit_split(
    _tantivy_index: &tantivy::Index,
    index_dir: &PathBuf,
    output_path: &PathBuf,
    _split_metadata: &QuickwitSplitMetadata,
    config: &SplitConfig
) -> Result<FooterOffsets, anyhow::Error> {
    use tantivy::directory::MmapDirectory;
    use uuid::Uuid;

    debug_log!("🔧 OFFICIAL API: Using Quickwit's SplitPayloadBuilder for proper split creation");
    debug_log!("create_quickwit_split called with output_path: {:?}", output_path);

    log_memory_checkpoint("split_creation_start");

    // ✅ Function is already called from async context, no need for separate runtime
    // ✅ STEP 1: Collect all Tantivy index files first
    let split_id = Uuid::new_v4().to_string();
    debug_log!("✅ OFFICIAL API: Creating split with split_id: {}", split_id);

    // Get files from the directory by reading the filesystem
    let file_entries: Vec<_> = std::fs::read_dir(index_dir)?
        .filter_map(|entry| entry.ok())
        .filter(|entry| {
            let path = entry.path();
            if !path.is_file() {
                return false;
            }

            let filename = path.file_name().unwrap().to_string_lossy();
            // Skip lock files and split files, include only Tantivy index files
            !filename.starts_with(".tantivy") && !filename.ends_with(".split")
        })
        .map(|entry| entry.path())
        .collect();

    debug_log!("✅ OFFICIAL API: Found {} files in index directory", file_entries.len());
    for file_path in &file_entries {
        debug_log!("✅ OFFICIAL API: Will include file: {}", file_path.display());
    }

    // ✅ STEP 2: Generate hotcache using memory-bounded mmap-only implementation
    // Uses write_hotcache_mmap to avoid O(index_size) heap allocation from CachingDirectory
    log_memory_checkpoint("before_hotcache_generation");

    let mmap_directory = MmapDirectory::open(index_dir)?;
    let hotcache = {
        let mut hotcache_buffer = Vec::new();

        debug_log!("✅ MEMORY-BOUNDED: Generating hotcache using write_hotcache_mmap (no CachingDirectory)");
        write_hotcache_mmap(mmap_directory, &mut hotcache_buffer)
            .map_err(|e| anyhow::anyhow!("Failed to generate hotcache: {}", e))?;

        debug_log!("✅ MEMORY-BOUNDED: Generated {} bytes of hotcache", hotcache_buffer.len());
        memory_profile!("Hotcache buffer size: {}", format_bytes(hotcache_buffer.len()));
        hotcache_buffer
    };

    log_memory_checkpoint("after_hotcache_generation");

    // ✅ STEP 3: Create empty serialized split fields (for now)
    let serialized_split_fields = Vec::new();
    debug_log!("✅ OFFICIAL API: Using empty serialized split fields");

    // ✅ STEP 4: Create split payload using official API
    // NOTE: SplitPayloadBuilder.get_split_payload() creates a FilePayload that streams
    // index files from disk. The hotcache is stored in memory in the payload's footer.
    debug_log!("✅ OFFICIAL API: Creating split payload with official get_split_payload()");
    log_memory_checkpoint("before_split_payload_builder");

    let split_payload = SplitPayloadBuilder::get_split_payload(
        &file_entries,
        &serialized_split_fields,
        &hotcache
    )?;

    log_memory_checkpoint("after_split_payload_builder");
    memory_profile!("SplitPayloadBuilder created - total payload size: {}", format_bytes(split_payload.len() as usize));

    // ✅ STEP 5: Write payload to output file using streaming I/O to minimize memory usage
    // Memory reduction: Instead of loading entire split into memory (O(file_size)),
    // we stream in configurable chunks (O(chunk_size) = ~8-64MB constant)
    debug_log!("✅ STREAMING CONFIG: chunk_size={}MB, progress_tracking={}, streaming_io={}",
              config.streaming_chunk_size / 1024 / 1024,
              config.enable_progress_tracking,
              config.enable_streaming_io);
    let total_size = split_payload.len();

    log_memory_checkpoint("before_payload_write");

    let bytes_written = if config.enable_streaming_io && total_size > config.streaming_chunk_size {
        // ✅ MEMORY OPTIMIZATION: Stream payload in chunks instead of loading entire file
        // This reduces peak memory from O(file_size) to O(chunk_size)
        debug_log!("✅ STREAMING WRITE: Writing {} bytes in {} byte chunks",
                  total_size, config.streaming_chunk_size);
        memory_profile!("Using STREAMING write mode (chunk_size: {})", format_bytes(config.streaming_chunk_size as usize));

        let output_file = tokio::fs::File::create(output_path).await
            .context("Failed to create output file for streaming write")?;
        let mut output_writer = tokio::io::BufWriter::with_capacity(
            8 * 1024 * 1024,  // 8MB buffer for efficient disk I/O
            output_file
        );

        let chunk_size = config.streaming_chunk_size;
        let mut offset = 0u64;
        let mut total_written = 0u64;
        let start_time = std::time::Instant::now();

        while offset < total_size {
            let end = (offset + chunk_size).min(total_size);
            let range = offset..end;

            // Get byte stream for this chunk
            let byte_stream = split_payload.range_byte_stream(range.clone()).await
                .map_err(|e| anyhow::anyhow!("Failed to get byte stream for range {:?}: {}", range, e))?;

            // Read bytes from stream using async reader
            let mut async_reader = tokio::io::BufReader::new(byte_stream.into_async_read());
            let chunk_written = tokio::io::copy(&mut async_reader, &mut output_writer).await
                .map_err(|e| anyhow::anyhow!("Failed to write chunk at offset {}: {}", offset, e))?;

            total_written += chunk_written;

            if config.enable_progress_tracking {
                let progress = (offset as f64 / total_size as f64) * 100.0;
                let elapsed = start_time.elapsed().as_secs_f64();
                let speed_mbps = if elapsed > 0.0 {
                    (total_written as f64 / 1024.0 / 1024.0) / elapsed
                } else {
                    0.0
                };
                debug_log!("✅ PROGRESS: {:.1}% ({}/{} bytes) - {:.1} MB/s",
                          progress, total_written, total_size, speed_mbps);
            }

            offset = end;
        }

        output_writer.flush().await
            .context("Failed to flush output buffer")?;

        let elapsed = start_time.elapsed();
        debug_log!("✅ STREAMING COMPLETE: Wrote {} bytes in {:.2}s ({:.1} MB/s)",
                  total_written, elapsed.as_secs_f64(),
                  (total_written as f64 / 1024.0 / 1024.0) / elapsed.as_secs_f64().max(0.001));

        total_written
    } else {
        // Fallback for small files or when streaming is disabled
        debug_log!("✅ DIRECT WRITE: File size {} < chunk size {}, using read_all()",
                  total_size, config.streaming_chunk_size);
        memory_profile!("Using DIRECT write mode (read_all) - THIS LOADS ENTIRE PAYLOAD INTO MEMORY!");
        log_memory_checkpoint("before_read_all");

        let payload_bytes = split_payload.read_all().await?;

        log_memory_checkpoint("after_read_all");
        memory_profile!("read_all() loaded {} into heap", format_bytes(payload_bytes.len()));

        std::fs::write(output_path, &payload_bytes)?;
        payload_bytes.len() as u64
    };

    log_memory_checkpoint("after_payload_write");

    debug_log!("✅ PAYLOAD WRITTEN: Successfully wrote split file: {:?} ({} bytes total, {} written)",
              output_path, total_size, bytes_written);

    // ✅ STEP 6: Extract actual footer information from the created split
    // Read footer from the created file instead of keeping entire payload in memory
    let file_len = total_size;

    // Read the last 4 bytes to get hotcache length (Quickwit uses u32, not u64)
    if file_len < 8 {
        return Err(anyhow::anyhow!("Split file too small: {} bytes", file_len));
    }

    // Read hotcache length from last 4 bytes of file
    let mut split_file = std::fs::File::open(output_path)?;
    use std::io::{Seek, SeekFrom, Read};
    split_file.seek(SeekFrom::End(-4))?;
    let mut hotcache_len_bytes = [0u8; 4];
    split_file.read_exact(&mut hotcache_len_bytes)?;
    let hotcache_length = u32::from_le_bytes(hotcache_len_bytes) as u64;

    debug_log!("✅ OFFICIAL API: Read hotcache length from split: {} bytes", hotcache_length);

    // Calculate hotcache start (4 bytes before hotcache for length field)
    let hotcache_start_offset = file_len - 4 - hotcache_length;

    // Read metadata length from 4 bytes before hotcache
    if hotcache_start_offset < 4 {
        return Err(anyhow::anyhow!("Invalid hotcache start offset: {}", hotcache_start_offset));
    }

    // Read metadata length from 4 bytes before hotcache
    let metadata_len_start = hotcache_start_offset - 4;
    split_file.seek(SeekFrom::Start(metadata_len_start))?;
    let mut metadata_len_bytes = [0u8; 4];
    split_file.read_exact(&mut metadata_len_bytes)?;
    let metadata_length = u32::from_le_bytes(metadata_len_bytes) as u64;

    debug_log!("✅ OFFICIAL API: Read metadata length from split: {} bytes", metadata_length);

    // Calculate footer start (where BundleStorageFileOffsets JSON begins)
    let footer_start_offset = hotcache_start_offset - 4 - metadata_length;

    debug_log!("✅ OFFICIAL API: Calculated footer offsets from actual split structure:");
    debug_log!("   footer_start_offset = {} (where BundleStorageFileOffsets begins)", footer_start_offset);
    debug_log!("   hotcache_start_offset = {} (where hotcache begins)", hotcache_start_offset);
    debug_log!("   file_len = {} (total file size)", file_len);
    debug_log!("   metadata_length = {} bytes", metadata_length);
    debug_log!("   hotcache_length = {} bytes", hotcache_length);

    // Validate footer structure
    if footer_start_offset >= hotcache_start_offset {
        return Err(anyhow::anyhow!(
            "Invalid footer structure: footer_start({}) >= hotcache_start({})",
            footer_start_offset, hotcache_start_offset
        ));
    }

    // Verify we can read the metadata section
    let metadata_start = footer_start_offset;
    let metadata_end = footer_start_offset + metadata_length;
    if metadata_end > total_size {
        return Err(anyhow::anyhow!(
            "Metadata section extends beyond file: {}..{} > {}",
            metadata_start, metadata_end, total_size
        ));
    }

    // Debug: Read and verify we can parse the metadata section
    split_file.seek(SeekFrom::Start(metadata_start))?;
    let mut metadata_bytes = vec![0u8; metadata_length as usize];
    split_file.read_exact(&mut metadata_bytes)?;
    match std::str::from_utf8(&metadata_bytes) {
        Ok(metadata_str) => {
            debug_log!("✅ OFFICIAL API: Successfully extracted metadata section ({} bytes)", metadata_str.len());
            debug_log!("✅ OFFICIAL API: Metadata preview: {}",
                &metadata_str[..std::cmp::min(200, metadata_str.len())]);
        },
        Err(e) => {
            debug_log!("⚠️  OFFICIAL API: Metadata section is not UTF-8 (binary format): {}", e);
        }
    }

    let footer_offsets = FooterOffsets {
        footer_start_offset,
        footer_end_offset: file_len,
        hotcache_start_offset,
        hotcache_length,
    };

    log_memory_checkpoint("split_creation_complete");

    // Summary of memory profiling
    if is_memory_profile_enabled() {
        let hotcache_copied = HOTCACHE_BYTES_COPIED.load(Ordering::SeqCst);
        memory_profile!("=== SPLIT CREATION MEMORY SUMMARY ===");
        memory_profile!("  Hotcache bytes copied via add_bytes(): {}", format_bytes(hotcache_copied));
        memory_profile!("  Final hotcache size in split: {}", format_bytes(hotcache_length as usize));
        memory_profile!("  Total split file size: {}", format_bytes(file_len as usize));
        memory_profile!("=========================================");
    }

    debug_log!("✅ OFFICIAL API: Split creation completed successfully with proper Quickwit format");
    Ok(footer_offsets)
}

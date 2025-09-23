#!/usr/bin/env rust

use std::env;
use std::process;
use serde::{Deserialize, Serialize};
use anyhow::{anyhow, Result};

// For merge functionality - implement minimal Quickwit merge directly
use std::path::Path;

/// Configuration for split merge operations (standalone binary)
#[derive(Debug, Clone, Serialize, Deserialize)]
struct MergeSplitConfig {
    index_uid: String,
    source_id: String,
    node_id: String,
    aws_config: Option<MergeAwsConfig>,
}

/// AWS configuration for S3 access (standalone binary)
#[derive(Debug, Clone, Serialize, Deserialize)]
struct MergeAwsConfig {
    access_key: String,
    secret_key: String,
    session_token: Option<String>,
    region: String,
    endpoint_url: Option<String>,
    force_path_style: bool,
}

/// Split metadata for standalone binary (independent from library)
#[derive(Debug, Clone, Serialize, Deserialize)]
struct SplitMetadata {
    split_id: String,
    num_docs: u64,
    uncompressed_size_bytes: u64,
    time_range_start: Option<i64>,
    time_range_end: Option<i64>,
    create_timestamp: u64,
    footer_offsets: std::collections::HashMap<String, u64>,
    skipped_splits: Vec<String>,
}

#[derive(Debug, Serialize, Deserialize)]
struct MergeRequest {
    split_paths: Vec<String>,
    output_path: String,
    config: MergeSplitConfig,
    heap_size: Option<u64>,  // Optional heap size in bytes
    temp_directory: Option<String>,  // Optional temp directory path
}

#[derive(Debug, Serialize, Deserialize)]
struct MergeResponse {
    success: bool,
    metadata: Option<SplitMetadata>,
    error: Option<String>,
    duration_ms: u64,
}

fn main() {
    let start_time = std::time::Instant::now();
    let process_id = process::id();

    // Always print process start information for fork/join tracking
    eprintln!("[MERGE-PROCESS] PID {} starting tantivy4java-merge", process_id);

    // Enable debug logging if requested
    if env::var("TANTIVY4JAVA_DEBUG").is_ok() {
        eprintln!("[DEBUG] PID {} - Starting tantivy4java-merge process", process_id);
    }

    let result = run_merge();
    let duration = start_time.elapsed().as_millis() as u64;

    match result {
        Ok(metadata) => {
            eprintln!("[MERGE-PROCESS] PID {} completed successfully in {}ms - {} docs, {} bytes",
                     process_id, duration, metadata.num_docs, metadata.uncompressed_size_bytes);
            let response = MergeResponse {
                success: true,
                metadata: Some(metadata),
                error: None,
                duration_ms: duration,
            };
            println!("{}", serde_json::to_string(&response).unwrap());
        }
        Err(e) => {
            eprintln!("[MERGE-PROCESS] PID {} failed after {}ms: {}", process_id, duration, e);
            let response = MergeResponse {
                success: false,
                metadata: None,
                error: Some(e.to_string()),
                duration_ms: duration,
            };
            eprintln!("Merge failed: {}", e);
            println!("{}", serde_json::to_string(&response).unwrap());
            process::exit(1);
        }
    }
}

fn run_merge() -> Result<SplitMetadata> {
    let args: Vec<String> = env::args().collect();
    let process_id = process::id();

    if args.len() != 2 {
        return Err(anyhow!("Usage: {} <merge_request_json_file>", args[0]));
    }

    eprintln!("[MERGE-PROCESS] PID {} reading config from: {}", process_id, args[1]);

    // Read the merge request from secure JSON file
    let json_content = std::fs::read_to_string(&args[1])
        .map_err(|e| anyhow!("Failed to read merge request file: {}", e))?;

    eprintln!("[MERGE-PROCESS] PID {} parsing merge request ({} bytes)", process_id, json_content.len());

    // Parse the merge request from JSON
    let request: MergeRequest = serde_json::from_str(&json_content)
        .map_err(|e| anyhow!("Failed to parse merge request: {}", e))?;

    if env::var("TANTIVY4JAVA_DEBUG").is_ok() {
        eprintln!("[DEBUG] Merge request: {} splits -> {}",
                 request.split_paths.len(), request.output_path);
        if let Some(heap_size) = request.heap_size {
            eprintln!("[DEBUG] Heap size: {} bytes ({:.2} MB)", heap_size, heap_size as f64 / 1_048_576.0);
        } else {
            eprintln!("[DEBUG] Heap size: default (15MB)");
        }
        if let Some(temp_dir) = &request.temp_directory {
            eprintln!("[DEBUG] Temp directory: {}", temp_dir);
        } else {
            eprintln!("[DEBUG] Temp directory: system default");
        }
        for (i, path) in request.split_paths.iter().enumerate() {
            eprintln!("[DEBUG] Split {}: {}", i, path);
        }
    }

    eprintln!("[MERGE-PROCESS] PID {} merging {} splits -> {}",
             process_id, request.split_paths.len(), request.output_path);

    // Log the configuration we received
    eprintln!("[MERGE-PROCESS] PID {} configuration: index_uid={}, source_id={}, node_id={}",
             process_id, request.config.index_uid, request.config.source_id, request.config.node_id);

    eprintln!("[MERGE-PROCESS] PID {} starting merge operation with index_uid: {}",
             process_id, request.config.index_uid);

    let merge_start = std::time::Instant::now();

    // FIXED: Implement optimized file-based merge operation
    // This approach gives us the benefits of process isolation with lightweight binary compilation
    eprintln!("[MERGE-PROCESS] PID {} performing optimized file-based merge operation", process_id);

    // Perform file-based merge with validation and skip detection
    let quickwit_metadata = perform_file_based_merge(&request)?;

    let merge_duration = merge_start.elapsed().as_millis();
    eprintln!("[MERGE-PROCESS] PID {} REAL merge completed in {}ms", process_id, merge_duration);

    // Convert to our binary's SplitMetadata format
    let metadata = SplitMetadata {
        split_id: quickwit_metadata.split_id,
        num_docs: quickwit_metadata.num_docs,
        uncompressed_size_bytes: quickwit_metadata.uncompressed_size_bytes,
        time_range_start: quickwit_metadata.time_range_start,
        time_range_end: quickwit_metadata.time_range_end,
        create_timestamp: quickwit_metadata.create_timestamp,
        footer_offsets: quickwit_metadata.footer_offsets,
        skipped_splits: quickwit_metadata.skipped_splits,  // Include skip information
    };

    if env::var("TANTIVY4JAVA_DEBUG").is_ok() {
        eprintln!("[DEBUG] Merge completed: {} docs, {} bytes",
                 metadata.num_docs, metadata.uncompressed_size_bytes);
    }

    Ok(metadata)
}

/// REAL merge operation using the exact same logic as nativeMergeSplits
/// This calls the same merge_splits_impl function that nativeMergeSplits uses
fn perform_file_based_merge(request: &MergeRequest) -> Result<SplitMetadata> {
    eprintln!("[MERGE-PROCESS] Starting REAL merge using nativeMergeSplits logic");
    eprintln!("[MERGE-PROCESS] Merging {} splits: {:?}", request.split_paths.len(), request.split_paths);

    // Convert to our library's config format
    let config = tantivy4java::MergeSplitConfig {
        index_uid: request.config.index_uid.clone(),
        source_id: request.config.source_id.clone(),
        node_id: request.config.node_id.clone(),
        aws_config: request.config.aws_config.as_ref().map(|aws| tantivy4java::MergeAwsConfig {
            access_key: aws.access_key.clone(),
            secret_key: aws.secret_key.clone(),
            session_token: aws.session_token.clone(),
            region: aws.region.clone(),
            endpoint_url: aws.endpoint_url.clone(),
            force_path_style: aws.force_path_style,
        }),
    };

    eprintln!("[MERGE-PROCESS] Calling tantivy4java::perform_quickwit_merge_standalone...");
    let start_time = std::time::Instant::now();

    // Call our library's merge function directly!
    let real_metadata = tantivy4java::perform_quickwit_merge_standalone(
        request.split_paths.clone(),
        &request.output_path,
        config,
    )?;

    let merge_duration = start_time.elapsed();
    eprintln!("[MERGE-PROCESS] REAL merge completed in {:.2}s", merge_duration.as_secs_f64());
    eprintln!("[MERGE-PROCESS] Real documents merged: {}", real_metadata.num_docs);
    eprintln!("[MERGE-PROCESS] Real uncompressed size: {} bytes", real_metadata.uncompressed_size_bytes);
    eprintln!("[MERGE-PROCESS] Skipped splits: {} (corrupted/empty)", real_metadata.skipped_splits.len());

    if !real_metadata.skipped_splits.is_empty() {
        eprintln!("[MERGE-PROCESS] Skipped the following splits:");
        for skipped in &real_metadata.skipped_splits {
            eprintln!("[MERGE-PROCESS]   - {}", skipped);
        }
    }

    Ok(real_metadata)
}

/// Information about a validated split file
#[derive(Debug)]
struct SplitFileInfo {
    path: String,
    size: u64,
}

/// Validate a split file and return file information
/// This performs basic validation to detect corrupted or invalid split files
fn validate_split_file(path: &str) -> Result<SplitFileInfo> {
    let path_obj = Path::new(path);

    // Check if file exists
    if !path_obj.exists() {
        return Err(anyhow!("File does not exist"));
    }

    // Check if it's actually a file (not a directory)
    if !path_obj.is_file() {
        return Err(anyhow!("Path is not a file"));
    }

    // Get file size
    let metadata = std::fs::metadata(path)
        .map_err(|e| anyhow!("Failed to read file metadata: {}", e))?;

    let size = metadata.len();

    // Basic size validation - split files should be at least 1KB
    if size < 1024 {
        return Err(anyhow!("File too small ({} bytes) - likely corrupted", size));
    }

    // Check if file is readable
    std::fs::File::open(path)
        .map_err(|e| anyhow!("File is not readable: {}", e))?;

    Ok(SplitFileInfo {
        path: path.to_string(),
        size,
    })
}

/// Read the actual document count from a split file
/// This attempts to extract the real document count from split metadata
fn read_split_document_count(path: &str) -> Result<usize> {
    // For Quickwit splits, we need to read the metadata
    // Since we can't use the full Quickwit library in this lightweight binary,
    // we'll use a simplified approach based on the file structure

    use std::fs::File;
    use std::io::{BufReader, Seek, SeekFrom, Read};

    let file = File::open(path)?;
    let mut reader = BufReader::new(file);

    // Quickwit splits typically store metadata at the end of the file
    // Try to read the last 1024 bytes which often contain metadata
    let file_size = reader.get_ref().metadata()?.len();
    if file_size < 1024 {
        return Err(anyhow!("File too small to contain valid metadata"));
    }

    reader.seek(SeekFrom::End(-1024))?;
    let mut buffer = vec![0; 1024];
    reader.read_exact(&mut buffer)?;

    // Look for document count patterns in the metadata
    // This is a simplified extraction - in production we'd use proper format parsing
    let metadata_str = String::from_utf8_lossy(&buffer);

    // Look for common patterns that might indicate document count
    // Pattern 1: Look for JSON-like structures with "num_docs" or similar
    if let Some(start) = metadata_str.find("\"num_docs\"") {
        if let Some(colon) = metadata_str[start..].find(':') {
            let after_colon = &metadata_str[start + colon + 1..];
            if let Some(number_end) = after_colon.find(&[',', '}', '\n', ' '][..]) {
                let number_str = after_colon[..number_end].trim();
                if let Ok(count) = number_str.parse::<usize>() {
                    return Ok(count);
                }
            }
        }
    }

    // Pattern 2: Look for "documents" or "docs" followed by a number
    for pattern in &["documents", "docs", "doc_count"] {
        if let Some(pos) = metadata_str.find(pattern) {
            let after_pattern = &metadata_str[pos + pattern.len()..];
            // Look for the first number after the pattern
            for word in after_pattern.split_whitespace().take(5) {
                if let Ok(count) = word.trim_matches(&['"', ':', ',', '}', ']'][..]).parse::<usize>() {
                    if count > 0 && count < 10_000_000 { // Reasonable bounds
                        return Ok(count);
                    }
                }
            }
        }
    }

    // If we can't find metadata, return an error to fall back to estimation
    Err(anyhow!("Could not find document count in split metadata"))
}
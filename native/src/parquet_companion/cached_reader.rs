// cached_reader.rs - AsyncFileReader implementation backed by Storage
//
// Bridges the parquet crate's AsyncFileReader trait to Quickwit's Storage trait,
// allowing parquet reads to go through the existing L1/L2/L3 cache stack.

use std::ops::Range;
use std::sync::Arc;

use bytes::Bytes;
use futures::future::{BoxFuture, FutureExt};
use parquet::arrow::async_reader::AsyncFileReader;
use parquet::arrow::arrow_reader::ArrowReaderOptions;
use parquet::file::metadata::ParquetMetaData;

use quickwit_storage::Storage;

use crate::debug_println;

/// An AsyncFileReader that delegates to Quickwit's Storage trait.
/// This ensures all parquet reads go through the L2 disk cache.
pub struct CachedParquetReader {
    storage: Arc<dyn Storage>,
    path: std::path::PathBuf,
    file_size: u64,
    metadata: Option<Arc<ParquetMetaData>>,
}

impl CachedParquetReader {
    /// Create a new reader for a parquet file at the given path
    pub fn new(
        storage: Arc<dyn Storage>,
        path: impl Into<std::path::PathBuf>,
        file_size: u64,
    ) -> Self {
        Self {
            storage,
            path: path.into(),
            file_size,
            metadata: None,
        }
    }

    /// Create a reader with pre-loaded metadata
    pub fn with_metadata(
        storage: Arc<dyn Storage>,
        path: impl Into<std::path::PathBuf>,
        file_size: u64,
        metadata: Arc<ParquetMetaData>,
    ) -> Self {
        Self {
            storage,
            path: path.into(),
            file_size,
            metadata: Some(metadata),
        }
    }

    /// Helper: convert Range<u64> to Range<usize> for Storage trait
    fn to_usize_range(range: &Range<u64>) -> Range<usize> {
        range.start as usize..range.end as usize
    }
}

/// A group of byte ranges that have been coalesced into a single fetch
struct CoalescedGroup {
    /// The merged byte range to fetch
    fetch_range: Range<u64>,
    /// Original (index, range) pairs contained within this fetch
    members: Vec<(usize, Range<u64>)>,
}

impl AsyncFileReader for CachedParquetReader {
    fn get_bytes(&mut self, range: Range<u64>) -> BoxFuture<'_, parquet::errors::Result<Bytes>> {
        let storage = self.storage.clone();
        let path = self.path.clone();
        debug_println!(
            "📖 PARQUET_READ: get_bytes path={:?} range={}..{} ({} bytes)",
            path, range.start, range.end, range.end - range.start
        );

        async move {
            let usize_range = Self::to_usize_range(&range);
            let owned_bytes = storage
                .get_slice(&path, usize_range)
                .await
                .map_err(|e| {
                    parquet::errors::ParquetError::External(Box::new(std::io::Error::new(
                        std::io::ErrorKind::Other,
                        format!("Storage read failed for {:?} range {:?}: {}", path, range, e),
                    )))
                })?;

            Ok(Bytes::from(owned_bytes.to_vec()))
        }
        .boxed()
    }

    fn get_byte_ranges(
        &mut self,
        ranges: Vec<Range<u64>>,
    ) -> BoxFuture<'_, parquet::errors::Result<Vec<Bytes>>> {
        let storage = self.storage.clone();
        let path = self.path.clone();
        debug_println!(
            "📖 PARQUET_READ: get_byte_ranges path={:?} count={} ranges",
            path, ranges.len()
        );

        async move {
            // Coalesce nearby ranges to reduce the number of storage round-trips.
            // This is critical for S3/Azure where each request has ~50-200ms latency.
            // We merge ranges separated by at most MAX_COALESCE_GAP bytes into a single
            // fetch, then slice out the original ranges from the coalesced buffer.
            const MAX_COALESCE_GAP: u64 = 64 * 1024; // 64 KB

            if ranges.len() <= 1 {
                // Single range: no coalescing needed
                let futs: Vec<_> = ranges
                    .into_iter()
                    .map(|range| {
                        let storage = storage.clone();
                        let path = path.clone();
                        async move {
                            let usize_range = range.start as usize..range.end as usize;
                            let owned_bytes = storage
                                .get_slice(&path, usize_range)
                                .await
                                .map_err(|e| {
                                    parquet::errors::ParquetError::External(Box::new(
                                        std::io::Error::new(
                                            std::io::ErrorKind::Other,
                                            format!(
                                                "Storage read failed for {:?} range {:?}: {}",
                                                path, range, e
                                            ),
                                        ),
                                    ))
                                })?;
                            Ok::<Bytes, parquet::errors::ParquetError>(Bytes::from(
                                owned_bytes.to_vec(),
                            ))
                        }
                    })
                    .collect();
                return futures::future::try_join_all(futs).await;
            }

            // Build sorted index of ranges for coalescing
            let mut indexed_ranges: Vec<(usize, Range<u64>)> = ranges
                .iter()
                .enumerate()
                .map(|(i, r)| (i, r.clone()))
                .collect();
            indexed_ranges.sort_by_key(|(_, r)| r.start);

            // Coalesce nearby ranges into groups
            let mut groups: Vec<CoalescedGroup> = Vec::new();
            let mut group_start = indexed_ranges[0].1.start;
            let mut group_end = indexed_ranges[0].1.end;
            let mut group_members = vec![indexed_ranges[0].clone()];

            for &(idx, ref range) in &indexed_ranges[1..] {
                if range.start <= group_end + MAX_COALESCE_GAP {
                    // Merge into current group
                    group_end = group_end.max(range.end);
                    group_members.push((idx, range.clone()));
                } else {
                    // Start a new group
                    groups.push(CoalescedGroup {
                        fetch_range: group_start..group_end,
                        members: std::mem::take(&mut group_members),
                    });
                    group_start = range.start;
                    group_end = range.end;
                    group_members = vec![(idx, range.clone())];
                }
            }
            groups.push(CoalescedGroup {
                fetch_range: group_start..group_end,
                members: group_members,
            });

            debug_println!(
                "📖 PARQUET_READ: coalesced {} ranges into {} fetches for {:?}",
                ranges.len(), groups.len(), path
            );

            // Fetch coalesced ranges in parallel
            let group_futs: Vec<_> = groups
                .into_iter()
                .map(|group| {
                    let storage = storage.clone();
                    let path = path.clone();
                    async move {
                        let usize_range = group.fetch_range.start as usize
                            ..group.fetch_range.end as usize;
                        let coalesced_bytes = storage
                            .get_slice(&path, usize_range)
                            .await
                            .map_err(|e| {
                                parquet::errors::ParquetError::External(Box::new(
                                    std::io::Error::new(
                                        std::io::ErrorKind::Other,
                                        format!(
                                            "Storage read failed for {:?} range {:?}: {}",
                                            path, group.fetch_range, e
                                        ),
                                    ),
                                ))
                            })?;

                        // Slice out individual ranges from the coalesced buffer
                        let coalesced_vec = coalesced_bytes.to_vec();
                        let results: Vec<(usize, Bytes)> = group
                            .members
                            .iter()
                            .map(|(original_idx, range)| {
                                let local_start =
                                    (range.start - group.fetch_range.start) as usize;
                                let local_end =
                                    (range.end - group.fetch_range.start) as usize;
                                let slice = &coalesced_vec[local_start..local_end];
                                (*original_idx, Bytes::copy_from_slice(slice))
                            })
                            .collect();

                        Ok::<Vec<(usize, Bytes)>, parquet::errors::ParquetError>(results)
                    }
                })
                .collect();

            let group_results = futures::future::try_join_all(group_futs).await?;

            // Reassemble results in original order
            let mut ordered: Vec<Option<Bytes>> = vec![None; ranges.len()];
            for results in group_results {
                for (idx, bytes) in results {
                    ordered[idx] = Some(bytes);
                }
            }

            Ok(ordered
                .into_iter()
                .map(|opt| opt.unwrap_or_else(|| Bytes::new()))
                .collect())
        }
        .boxed()
    }

    fn get_metadata<'a>(
        &'a mut self,
        _options: Option<&'a ArrowReaderOptions>,
    ) -> BoxFuture<'a, parquet::errors::Result<Arc<ParquetMetaData>>> {
        if let Some(ref metadata) = self.metadata {
            let metadata = metadata.clone();
            return async move { Ok(metadata) }.boxed();
        }

        let storage = self.storage.clone();
        let path = self.path.clone();
        let file_size = self.file_size;

        async move {
            // Read the tail of the file to get parquet footer metadata
            // The parquet footer is: [metadata bytes] [4-byte metadata length] [4-byte magic "PAR1"]
            let footer_read_size = std::cmp::min(file_size, 64 * 1024) as usize;
            let footer_start = file_size as usize - footer_read_size;
            let footer_bytes = storage
                .get_slice(&path, footer_start..file_size as usize)
                .await
                .map_err(|e| {
                    parquet::errors::ParquetError::External(Box::new(std::io::Error::new(
                        std::io::ErrorKind::Other,
                        format!("Failed to read parquet footer from {:?}: {}", path, e),
                    )))
                })?;

            // Use parquet's metadata reader to parse the footer
            let metadata = parquet::file::metadata::ParquetMetaDataReader::new()
                .parse_and_finish(&bytes::Bytes::from(footer_bytes.to_vec()))
                .map_err(|e| {
                    parquet::errors::ParquetError::External(Box::new(std::io::Error::new(
                        std::io::ErrorKind::Other,
                        format!("Failed to decode parquet metadata from {:?}: {}", path, e),
                    )))
                })?;

            Ok(Arc::new(metadata))
        }
        .boxed()
    }
}

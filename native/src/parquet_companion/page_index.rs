// page_index.rs - Compute page-level offset index for parquet column chunks
//
// For parquet files that lack a native offset index in their footer, this module
// scans the Thrift-encoded page headers within each column chunk to compute
// page locations (offset, compressed_page_size, first_row_index). These are
// stored in the manifest and injected at read time to enable page-level byte
// range reads instead of full column chunk downloads.
//
// For nested columns (List/Map/Struct), DataPageV1 headers only report
// num_values (leaf values), not num_rows. To compute exact first_row_index,
// this module decompresses V1 page data and counts rep_level == 0 occurrences
// in the RLE/Bit-Packed encoded repetition levels. DataPageV2 headers have an
// explicit num_rows field and need no decompression.
//
// Uses a self-contained minimal Thrift compact protocol parser — no dependency
// on the parquet crate's internal (non-public) Thrift types. Decompression uses
// the parquet crate's codec infrastructure.

use std::io::{Read, Seek, SeekFrom};

use anyhow::{Context, Result};
use parquet::basic::Compression;

use super::manifest::PageLocationEntry;

/// Parquet page type constants (from parquet.thrift)
const PAGE_TYPE_DATA_PAGE: i32 = 0;
const PAGE_TYPE_INDEX_PAGE: i32 = 1;
const PAGE_TYPE_DICTIONARY_PAGE: i32 = 2;
const PAGE_TYPE_DATA_PAGE_V2: i32 = 3;

/// Compute page locations for a single column chunk by scanning
/// Thrift-encoded page headers from the raw column chunk bytes.
///
/// This is used for parquet files that don't have an offset index
/// in their footer. The computed locations enable page-level byte
/// range reads at query time.
///
/// For nested columns (List/Map/Struct), DataPageV1 headers only report
/// `num_values` (leaf values), not `num_rows`. To compute exact
/// `first_row_index`, this function decompresses V1 page data and counts
/// `rep_level == 0` occurrences in the RLE/Bit-Packed repetition levels.
/// DataPageV2 headers have an explicit `num_rows` field — no decompression
/// needed for those.
///
/// # Arguments
/// * `file` - Open file handle for the parquet file
/// * `data_page_offset` - Byte offset of the first data page in this column chunk
/// * `total_compressed_size` - Total compressed size of the column chunk
/// * `dictionary_page_offset` - Optional byte offset of the dictionary page
/// * `max_rep_level` - Max repetition level from schema (0 = flat, >0 = nested/List/Map)
/// * `rg_num_rows` - Total number of rows in the row group
/// * `compression` - Compression codec used for this column chunk
pub fn compute_page_locations_from_column_chunk(
    file: &std::fs::File,
    data_page_offset: u64,
    total_compressed_size: u64,
    dictionary_page_offset: Option<u64>,
    max_rep_level: i16,
    rg_num_rows: i64,
    compression: Compression,
) -> Result<Vec<PageLocationEntry>> {
    let start_offset = dictionary_page_offset.unwrap_or(data_page_offset);
    let end_offset = start_offset + total_compressed_size;

    if end_offset <= start_offset {
        return Ok(Vec::new());
    }

    let chunk_len = (end_offset - start_offset) as usize;

    let mut buf = vec![0u8; chunk_len];
    let mut file_reader = std::io::BufReader::new(file);
    file_reader
        .seek(SeekFrom::Start(start_offset))
        .context("Failed to seek to column chunk start")?;
    file_reader
        .read_exact(&mut buf)
        .context("Failed to read column chunk bytes")?;

    let is_nested = max_rep_level > 0;
    let bit_width = if is_nested {
        num_required_bits(max_rep_level as u32)
    } else {
        0
    };

    // Reusable decompression buffer for nested V1 pages
    let mut decompress_buf: Vec<u8> = Vec::new();

    struct PageInfo {
        file_offset: i64,
        total_page_size: i32,
        num_rows: i64,
        /// Whether this page starts with continuation data from the previous page's
        /// last row (first rep_level > 0). Only meaningful for nested columns.
        starts_with_continuation: bool,
    }

    let mut pages: Vec<PageInfo> = Vec::new();
    let mut pos: usize = 0;

    while pos < chunk_len {
        let page_file_offset = start_offset + pos as u64;
        let remaining = &buf[pos..];

        let header = parse_page_header(remaining)
            .with_context(|| format!(
                "Failed to parse page header at offset {} (pos {} of {} chunk bytes)",
                page_file_offset, pos, chunk_len
            ))?;

        let total_page_size = header.header_size as i32 + header.compressed_page_size;

        match header.page_type {
            PAGE_TYPE_DATA_PAGE => {
                let (num_rows, starts_with_continuation) = if !is_nested {
                    // Flat column: num_values == num_rows, never has continuation
                    (header.num_values as i64, false)
                } else {
                    // Nested V1 page: decompress and count rep_level == 0
                    let page_data_start = pos + header.header_size;
                    let page_data_end = page_data_start + header.compressed_page_size as usize;
                    if page_data_end > chunk_len {
                        anyhow::bail!(
                            "Page data extends past chunk end at offset {}",
                            page_file_offset
                        );
                    }
                    let compressed_data = &buf[page_data_start..page_data_end];

                    let info = count_rows_in_v1_page(
                        compressed_data,
                        header.uncompressed_page_size as usize,
                        &compression,
                        &mut decompress_buf,
                        bit_width,
                        header.num_values as usize,
                    )
                    .with_context(|| format!(
                        "Failed to count rows in V1 nested page at offset {}",
                        page_file_offset
                    ))?;
                    (info.num_rows as i64, info.starts_with_continuation)
                };
                pages.push(PageInfo {
                    file_offset: page_file_offset as i64,
                    total_page_size,
                    num_rows,
                    starts_with_continuation,
                });
            }
            PAGE_TYPE_DATA_PAGE_V2 => {
                let (num_rows, starts_with_continuation) = if !is_nested {
                    (header.num_values as i64, false)
                } else {
                    // V2 has explicit num_rows in header.
                    // Check first rep_level to detect continuation pages.
                    // In V2, rep levels are stored uncompressed at the start of page data.
                    let cont = if header.rep_levels_byte_length > 0 {
                        let page_data_start = pos + header.header_size;
                        let rep_end = page_data_start + header.rep_levels_byte_length as usize;
                        if rep_end <= chunk_len {
                            let rep_data = &buf[page_data_start..rep_end];
                            !first_rep_is_zero(rep_data, bit_width)
                        } else {
                            false
                        }
                    } else {
                        false
                    };
                    (header.num_rows as i64, cont)
                };
                pages.push(PageInfo {
                    file_offset: page_file_offset as i64,
                    total_page_size,
                    num_rows,
                    starts_with_continuation,
                });
            }
            PAGE_TYPE_DICTIONARY_PAGE | PAGE_TYPE_INDEX_PAGE => {
                // Not included in offset index.
            }
            _ => {}
        }

        let advance = header.header_size + header.compressed_page_size as usize;
        if advance == 0 {
            break;
        }
        pos += advance;
    }

    // For nested columns, merge pages where a row spans a page boundary.
    //
    // The parquet spec requires that when an offset_index is present, pages must
    // begin on row boundaries (rep_level=0). Files written without an offset index
    // don't follow this constraint — a row with many array elements can span
    // multiple pages. arrow-rs's scan_ranges() uses first_row_index to select
    // pages and will miss continuation pages, causing decode errors.
    //
    // Fix: merge each continuation page (first rep_level > 0) into the preceding
    // page so that no row straddles a merged page boundary.
    let pages_to_use: Vec<PageInfo> = if is_nested {
        let mut merged: Vec<PageInfo> = Vec::new();
        for page in pages {
            if page.starts_with_continuation && !merged.is_empty() {
                // Extend the previous merged page to cover this continuation page
                let prev = merged.last_mut().unwrap();
                let new_end = page.file_offset + page.total_page_size as i64;
                prev.total_page_size = (new_end - prev.file_offset) as i32;
                prev.num_rows += page.num_rows;
            } else {
                merged.push(page);
            }
        }
        merged
    } else {
        pages
    };

    // Build locations with exact cumulative row counts
    let mut cumulative_rows: i64 = 0;
    let mut locations = Vec::with_capacity(pages_to_use.len());

    for page in &pages_to_use {
        locations.push(PageLocationEntry {
            offset: page.file_offset,
            compressed_page_size: page.total_page_size,
            first_row_index: cumulative_rows,
        });
        cumulative_rows += page.num_rows;
    }

    Ok(locations)
}

/// Result of counting rows in a nested page.
struct NestedPageRowInfo {
    /// Number of top-level rows that START in this page (rep_level == 0 count).
    num_rows: usize,
    /// Whether the first value in this page is a continuation of the previous
    /// page's last row (first rep_level > 0). When true, the page's data begins
    /// with elements belonging to a row that started in an earlier page.
    starts_with_continuation: bool,
}

/// Count the number of rows in a DataPageV1 for a nested column by
/// decompressing the page and counting rep_level == 0 in the RLE/Bit-Packed
/// encoded repetition levels. Also detects whether the page starts with
/// continuation data from the previous page.
fn count_rows_in_v1_page(
    compressed_data: &[u8],
    uncompressed_size: usize,
    compression: &Compression,
    decompress_buf: &mut Vec<u8>,
    bit_width: u8,
    num_values: usize,
) -> Result<NestedPageRowInfo> {
    let data: &[u8] = match *compression {
        Compression::UNCOMPRESSED => compressed_data,
        Compression::SNAPPY => {
            decompress_buf.resize(uncompressed_size, 0);
            let len = snap::raw::Decoder::new()
                .decompress(compressed_data, decompress_buf)
                .context("Snappy decompression failed")?;
            &decompress_buf[..len]
        }
        Compression::ZSTD(_) => {
            decompress_buf.clear();
            decompress_buf.reserve(uncompressed_size);
            let decoded = zstd::decode_all(std::io::Cursor::new(compressed_data))
                .context("ZSTD decompression failed")?;
            *decompress_buf = decoded;
            decompress_buf.as_slice()
        }
        Compression::LZ4_RAW => {
            decompress_buf.resize(uncompressed_size, 0);
            let len = lz4_flex::decompress_into(compressed_data, decompress_buf)
                .map_err(|e| anyhow::anyhow!("LZ4 decompression failed: {}", e))?;
            &decompress_buf[..len]
        }
        Compression::GZIP(_) => {
            decompress_buf.clear();
            decompress_buf.reserve(uncompressed_size);
            let mut decoder = flate2::read::GzDecoder::new(compressed_data);
            decoder.read_to_end(decompress_buf)
                .context("GZIP decompression failed")?;
            decompress_buf.as_slice()
        }
        other => {
            anyhow::bail!("Unsupported compression codec for page index scanning: {:?}", other);
        }
    };

    count_rows_from_decompressed(data, bit_width, num_values)
}

/// Parse the rep levels from decompressed V1 page data and count rows.
/// Returns row count and whether the page starts with continuation data.
fn count_rows_from_decompressed(data: &[u8], bit_width: u8, num_values: usize) -> Result<NestedPageRowInfo> {
    // V1 page layout (after decompression):
    //   [4 bytes LE: rep_levels_length] [rep_levels_data] [def_levels...] [values...]
    if data.len() < 4 {
        anyhow::bail!(
            "Decompressed page data too short for rep levels length prefix ({} bytes)",
            data.len()
        );
    }
    let rep_levels_len = u32::from_le_bytes([data[0], data[1], data[2], data[3]]) as usize;
    if data.len() < 4 + rep_levels_len {
        anyhow::bail!(
            "Decompressed page data too short for rep levels: need {} but have {}",
            4 + rep_levels_len,
            data.len()
        );
    }

    let rep_data = &data[4..4 + rep_levels_len];
    let num_rows = count_rle_bp_zeros(rep_data, bit_width, num_values);
    let starts_with_continuation = rep_levels_len > 0 && !first_rep_is_zero(rep_data, bit_width);
    Ok(NestedPageRowInfo { num_rows, starts_with_continuation })
}

/// Check whether the first repetition level in RLE/Bit-Packed hybrid data is zero.
/// Returns true if the first rep_level is 0 (meaning a new row starts at the
/// beginning of the page). Returns false if the first rep_level > 0 (continuation).
fn first_rep_is_zero(rep_data: &[u8], bit_width: u8) -> bool {
    if bit_width == 0 || rep_data.is_empty() {
        return true; // bit_width 0 means all values are 0 (single level of nesting)
    }

    // Read the first RLE/Bit-Packed run header
    let (header, vlen) = read_vlq(rep_data);
    if vlen == 0 {
        return true; // No data to read
    }

    if header & 1 == 0 {
        // RLE run: the repeated value follows the header
        let value_byte_width = ((bit_width as usize) + 7) / 8;
        if vlen + value_byte_width > rep_data.len() {
            return true; // Can't read value, assume start of row
        }
        let mut value: u64 = 0;
        for i in 0..value_byte_width {
            value |= (rep_data[vlen + i] as u64) << (i * 8);
        }
        value == 0
    } else {
        // Bit-packed run: values are packed starting after the header
        if vlen >= rep_data.len() {
            return true; // Can't read packed data
        }
        // First value is in the lowest bit_width bits of the first byte
        let mask = (1u8 << bit_width) - 1;
        (rep_data[vlen] & mask) == 0
    }
}

/// Count the number of zero values in RLE/Bit-Packed hybrid encoded data.
///
/// In parquet repetition levels, rep_level == 0 means "start of a new row."
/// This function counts those zeros to determine the exact number of rows
/// in a data page for nested columns.
///
/// RLE/Bit-Packed Hybrid encoding (from parquet spec):
/// - Varint header for each run
/// - If header & 1 == 0: RLE run (repeat count = header >> 1, value follows)
/// - If header & 1 == 1: bit-packed run (num_groups = header >> 1, 8 values per group)
fn count_rle_bp_zeros(data: &[u8], bit_width: u8, total_values: usize) -> usize {
    if bit_width == 0 {
        return total_values; // all values are 0
    }

    let mut zeros = 0usize;
    let mut pos = 0usize;
    let mut counted = 0usize;

    while pos < data.len() && counted < total_values {
        // Read unsigned varint header
        let (header, vlen) = read_vlq(&data[pos..]);
        if vlen == 0 {
            break;
        }
        pos += vlen;

        if header & 1 == 0 {
            // RLE run: repeat count = header >> 1
            let run_len = ((header >> 1) as usize).min(total_values - counted);
            let value_byte_width = ((bit_width as usize) + 7) / 8;
            if pos + value_byte_width > data.len() {
                break;
            }

            let mut value: u64 = 0;
            for i in 0..value_byte_width {
                value |= (data[pos + i] as u64) << (i * 8);
            }
            pos += value_byte_width;

            if value == 0 {
                zeros += run_len;
            }
            counted += run_len;
        } else {
            // Bit-packed run: num_groups = header >> 1, 8 values per group
            let num_groups = (header >> 1) as usize;
            let num_values_in_run = (num_groups * 8).min(total_values - counted);
            let byte_count = num_groups * bit_width as usize;
            if pos + byte_count > data.len() {
                break;
            }

            if bit_width == 1 {
                // Optimized: 8 values per byte, zero bit = zero rep level = new row
                for i in 0..byte_count {
                    let byte_val = data[pos + i];
                    let vals_in_byte = if (i + 1) * 8 <= num_values_in_run {
                        8
                    } else {
                        num_values_in_run.saturating_sub(i * 8)
                    };
                    if vals_in_byte == 0 {
                        break;
                    }
                    let mask = if vals_in_byte >= 8 {
                        0xFFu8
                    } else {
                        (1u8 << vals_in_byte) - 1
                    };
                    zeros += vals_in_byte - (byte_val & mask).count_ones() as usize;
                }
            } else {
                // General: extract each value bit by bit
                'values: for i in 0..num_values_in_run {
                    let bit_offset = i * bit_width as usize;
                    let byte_idx = bit_offset / 8;
                    let bit_idx = bit_offset % 8;
                    let mut val: u64 = 0;
                    for b in 0..bit_width as usize {
                        let cur_byte = byte_idx + (bit_idx + b) / 8;
                        let cur_bit = (bit_idx + b) % 8;
                        if pos + cur_byte >= data.len() {
                            // Truncated data — stop counting entirely rather than
                            // reading a partial value that could be falsely zero.
                            break 'values;
                        }
                        val |= (((data[pos + cur_byte] >> cur_bit) & 1) as u64) << b;
                    }
                    if val == 0 {
                        zeros += 1;
                    }
                }
            }

            pos += byte_count;
            counted += num_values_in_run;
        }
    }

    zeros
}

/// Read an unsigned varint (LEB128) from a byte slice.
/// Returns (value, bytes_consumed). Returns (0, 0) if data is empty.
fn read_vlq(data: &[u8]) -> (u64, usize) {
    let mut result: u64 = 0;
    let mut shift: u32 = 0;
    for (i, &b) in data.iter().enumerate() {
        result |= ((b & 0x7F) as u64) << shift;
        if b & 0x80 == 0 {
            return (result, i + 1);
        }
        shift += 7;
        if shift >= 64 {
            return (result, i + 1);
        }
    }
    (0, 0)
}

/// Compute the number of bits required to represent `max_val`.
/// Returns ceil(log2(max_val + 1)), minimum 1 for max_val > 0.
fn num_required_bits(max_val: u32) -> u8 {
    if max_val == 0 {
        return 0;
    }
    (32 - max_val.leading_zeros()) as u8
}

/// Parsed page header — only the fields we need.
struct ParsedPageHeader {
    page_type: i32,
    uncompressed_page_size: i32,
    compressed_page_size: i32,
    /// Number of values in this page (from data_page_header or data_page_header_v2).
    /// 0 for dictionary/index pages.
    num_values: i32,
    /// For DataPageHeaderV2: the explicit num_rows field.
    /// 0 for V1 pages and dictionary/index pages.
    num_rows: i32,
    /// For DataPageHeaderV2: byte length of uncompressed repetition levels.
    /// 0 for V1 and dictionary/index pages.
    rep_levels_byte_length: i32,
    /// Number of bytes consumed by the Thrift header encoding.
    header_size: usize,
}

/// Parse a Thrift compact-encoded PageHeader from the start of `data`.
///
/// PageHeader fields (from parquet.thrift):
///   1: required PageType type         (i32 enum)
///   2: required i32 uncompressed_page_size
///   3: required i32 compressed_page_size
///   4: optional i32 crc
///   5: optional DataPageHeader data_page_header
///   6: optional IndexPageHeader index_page_header
///   7: optional DictionaryPageHeader dictionary_page_header
///   8: optional DataPageHeaderV2 data_page_header_v2
fn parse_page_header(data: &[u8]) -> Result<ParsedPageHeader> {
    let mut reader = ThriftReader::new(data);

    let mut page_type: Option<i32> = None;
    let mut uncompressed_page_size: Option<i32> = None;
    let mut compressed_page_size: Option<i32> = None;
    let mut num_values: i32 = 0;
    let mut num_rows: i32 = 0;
    let mut rep_levels_byte_length: i32 = 0;

    // Read struct fields until stop
    let mut last_field_id: i16 = 0;
    loop {
        let (field_type, field_id) = reader.read_field_header(last_field_id)?;
        if field_type == THRIFT_TYPE_STOP {
            break;
        }
        last_field_id = field_id;

        match field_id {
            1 => {
                // type: i32 (PageType enum)
                page_type = Some(reader.read_i32()?);
            }
            2 => {
                // uncompressed_page_size: i32
                uncompressed_page_size = Some(reader.read_i32()?);
            }
            3 => {
                // compressed_page_size: i32
                compressed_page_size = Some(reader.read_i32()?);
            }
            4 => {
                // crc: optional i32
                reader.read_i32()?;
            }
            5 => {
                // data_page_header: DataPageHeader struct
                // field 1 is num_values (i32)
                num_values = read_first_i32_from_struct(&mut reader)?;
            }
            6 => {
                // index_page_header: empty struct
                reader.skip_field(field_type)?;
            }
            7 => {
                // dictionary_page_header: DictionaryPageHeader struct
                reader.skip_field(field_type)?;
            }
            8 => {
                // data_page_header_v2: DataPageHeaderV2 struct
                let v2_info = read_data_page_v2_fields(&mut reader)?;
                num_values = v2_info.num_values;
                num_rows = v2_info.num_rows;
                rep_levels_byte_length = v2_info.rep_levels_byte_length;
            }
            _ => {
                reader.skip_field(field_type)?;
            }
        }
    }

    let page_type = page_type
        .ok_or_else(|| anyhow::anyhow!("PageHeader missing required field: type"))?;
    let uncompressed_page_size = uncompressed_page_size
        .ok_or_else(|| anyhow::anyhow!("PageHeader missing required field: uncompressed_page_size"))?;
    let compressed_page_size = compressed_page_size
        .ok_or_else(|| anyhow::anyhow!("PageHeader missing required field: compressed_page_size"))?;

    Ok(ParsedPageHeader {
        page_type,
        uncompressed_page_size,
        compressed_page_size,
        num_values,
        num_rows,
        rep_levels_byte_length,
        header_size: reader.position(),
    })
}

/// Read the first i32 field from a sub-struct, then skip the rest.
/// Used for DataPageHeader.num_values.
fn read_first_i32_from_struct(reader: &mut ThriftReader) -> Result<i32> {
    let mut result: i32 = 0;
    let mut last_field_id: i16 = 0;
    let mut got_first = false;

    loop {
        let (field_type, field_id) = reader.read_field_header(last_field_id)?;
        if field_type == THRIFT_TYPE_STOP {
            break;
        }
        last_field_id = field_id;

        if !got_first && field_id == 1 {
            result = reader.read_i32()?;
            got_first = true;
        } else {
            reader.skip_field(field_type)?;
        }
    }

    Ok(result)
}

/// Parsed fields from a DataPageHeaderV2 struct.
struct DataPageV2Info {
    num_values: i32,
    num_rows: i32,
    /// Byte length of uncompressed repetition levels (field 6).
    /// In V2 pages, rep levels are stored uncompressed at the start of the page data.
    rep_levels_byte_length: i32,
}

/// Read num_values (field 1), num_rows (field 3), and rep_levels_byte_length (field 6)
/// from a DataPageHeaderV2 struct.
///
/// DataPageHeaderV2 layout (parquet.thrift):
///   1: required i32 num_values
///   2: required i32 num_nulls
///   3: required i32 num_rows
///   4: required Encoding encoding
///   5: required i32 definition_levels_byte_length
///   6: required i32 repetition_levels_byte_length
///   7: optional bool is_compressed
///   8: optional Statistics statistics
fn read_data_page_v2_fields(reader: &mut ThriftReader) -> Result<DataPageV2Info> {
    let mut num_values: i32 = 0;
    let mut num_rows: i32 = 0;
    let mut rep_levels_byte_length: i32 = 0;
    let mut last_field_id: i16 = 0;

    loop {
        let (field_type, field_id) = reader.read_field_header(last_field_id)?;
        if field_type == THRIFT_TYPE_STOP {
            break;
        }
        last_field_id = field_id;

        match field_id {
            1 => num_values = reader.read_i32()?,
            3 => num_rows = reader.read_i32()?,
            6 => rep_levels_byte_length = reader.read_i32()?,
            _ => reader.skip_field(field_type)?,
        }
    }

    Ok(DataPageV2Info { num_values, num_rows, rep_levels_byte_length })
}

// ─── Minimal Thrift Compact Protocol Parser ────────────────────────────────

/// Thrift compact protocol type constants
const THRIFT_TYPE_STOP: u8 = 0;
const THRIFT_TYPE_BOOL_TRUE: u8 = 1;
const THRIFT_TYPE_BOOL_FALSE: u8 = 2;
const THRIFT_TYPE_I8: u8 = 3;
const THRIFT_TYPE_I16: u8 = 4;
const THRIFT_TYPE_I32: u8 = 5;
const THRIFT_TYPE_I64: u8 = 6;
const THRIFT_TYPE_DOUBLE: u8 = 7;
const THRIFT_TYPE_BINARY: u8 = 8;
const THRIFT_TYPE_LIST: u8 = 9;
const THRIFT_TYPE_SET: u8 = 10;
const THRIFT_TYPE_MAP: u8 = 11;
const THRIFT_TYPE_STRUCT: u8 = 12;

/// Minimal Thrift compact protocol reader operating on a byte slice.
struct ThriftReader<'a> {
    data: &'a [u8],
    pos: usize,
}

impl<'a> ThriftReader<'a> {
    fn new(data: &'a [u8]) -> Self {
        Self { data, pos: 0 }
    }

    fn position(&self) -> usize {
        self.pos
    }

    fn read_byte(&mut self) -> Result<u8> {
        if self.pos >= self.data.len() {
            anyhow::bail!("Thrift: unexpected end of data at pos {}", self.pos);
        }
        let b = self.data[self.pos];
        self.pos += 1;
        Ok(b)
    }

    /// Read a varint (unsigned LEB128)
    fn read_varint(&mut self) -> Result<u64> {
        let mut result: u64 = 0;
        let mut shift: u32 = 0;
        loop {
            let b = self.read_byte()?;
            result |= ((b & 0x7F) as u64) << shift;
            if b & 0x80 == 0 {
                return Ok(result);
            }
            shift += 7;
            if shift >= 64 {
                anyhow::bail!("Thrift: varint too long at pos {}", self.pos);
            }
        }
    }

    /// Read a zigzag-encoded i32
    fn read_i32(&mut self) -> Result<i32> {
        let n = self.read_varint()? as u32;
        Ok(((n >> 1) as i32) ^ -((n & 1) as i32))
    }

    /// Read a zigzag-encoded i64
    #[allow(dead_code)]
    fn read_i64(&mut self) -> Result<i64> {
        let n = self.read_varint()?;
        Ok(((n >> 1) as i64) ^ -((n & 1) as i64))
    }

    /// Read a zigzag-encoded i16
    fn read_i16(&mut self) -> Result<i16> {
        let n = self.read_varint()? as u16;
        Ok(((n >> 1) as i16) ^ -((n & 1) as i16))
    }

    /// Read a field header. Returns (field_type, field_id).
    /// field_type == THRIFT_TYPE_STOP (0) means end of struct.
    fn read_field_header(&mut self, last_field_id: i16) -> Result<(u8, i16)> {
        let b = self.read_byte()?;
        if b == 0 {
            return Ok((THRIFT_TYPE_STOP, 0));
        }

        let delta = (b >> 4) & 0x0F;
        let field_type = b & 0x0F;

        let field_id = if delta != 0 {
            last_field_id + delta as i16
        } else {
            // Absolute field ID encoded as zigzag i16
            self.read_i16()?
        };

        Ok((field_type, field_id))
    }

    /// Skip a field of the given type
    fn skip_field(&mut self, field_type: u8) -> Result<()> {
        match field_type {
            THRIFT_TYPE_BOOL_TRUE | THRIFT_TYPE_BOOL_FALSE => {}
            THRIFT_TYPE_I8 => {
                self.read_byte()?;
            }
            THRIFT_TYPE_I16 | THRIFT_TYPE_I32 => {
                self.read_varint()?;
            }
            THRIFT_TYPE_I64 => {
                self.read_varint()?;
            }
            THRIFT_TYPE_DOUBLE => {
                if self.pos + 8 > self.data.len() {
                    anyhow::bail!("Thrift: unexpected end of data skipping double");
                }
                self.pos += 8;
            }
            THRIFT_TYPE_BINARY => {
                let len = self.read_varint()? as usize;
                if self.pos + len > self.data.len() {
                    anyhow::bail!("Thrift: unexpected end of data skipping binary of len {}", len);
                }
                self.pos += len;
            }
            THRIFT_TYPE_LIST | THRIFT_TYPE_SET => {
                self.skip_list()?;
            }
            THRIFT_TYPE_MAP => {
                self.skip_map()?;
            }
            THRIFT_TYPE_STRUCT => {
                self.skip_struct()?;
            }
            _ => {
                anyhow::bail!("Thrift: unknown field type {} at pos {}", field_type, self.pos);
            }
        }
        Ok(())
    }

    fn skip_struct(&mut self) -> Result<()> {
        let mut last_field_id: i16 = 0;
        loop {
            let (ft, fid) = self.read_field_header(last_field_id)?;
            if ft == THRIFT_TYPE_STOP {
                break;
            }
            last_field_id = fid;
            self.skip_field(ft)?;
        }
        Ok(())
    }

    fn skip_list(&mut self) -> Result<()> {
        let header = self.read_byte()?;
        let (size, elem_type) = if (header >> 4) == 0x0F {
            let size = self.read_varint()? as usize;
            let elem_type = header & 0x0F;
            (size, elem_type)
        } else {
            let size = ((header >> 4) & 0x0F) as usize;
            let elem_type = header & 0x0F;
            (size, elem_type)
        };
        for _ in 0..size {
            self.skip_field(elem_type)?;
        }
        Ok(())
    }

    fn skip_map(&mut self) -> Result<()> {
        let size = self.read_varint()? as usize;
        if size == 0 {
            return Ok(());
        }
        let types = self.read_byte()?;
        let key_type = (types >> 4) & 0x0F;
        let val_type = types & 0x0F;
        for _ in 0..size {
            self.skip_field(key_type)?;
            self.skip_field(val_type)?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::NamedTempFile;
    use parquet::file::reader::FileReader;

    /// Create a minimal parquet file without offset index and verify page location computation.
    #[test]
    fn test_compute_page_locations_from_small_parquet() {
        use parquet::file::properties::WriterProperties;
        use parquet::arrow::ArrowWriter;
        use arrow_array::{Int64Array, RecordBatch};
        use arrow_schema::{Schema, Field, DataType};
        use std::sync::Arc;

        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
        ]));

        let mut tmpfile = NamedTempFile::new().unwrap();

        let props = WriterProperties::builder()
            .set_data_page_size_limit(64)
            .set_write_batch_size(10)
            .set_max_row_group_size(1000)
            .build();

        let ids: Vec<i64> = (0..100).collect();
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int64Array::from(ids))],
        ).unwrap();

        let mut writer = ArrowWriter::try_new(
            tmpfile.as_file_mut(),
            schema.clone(),
            Some(props),
        ).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();

        let file = std::fs::File::open(tmpfile.path()).unwrap();
        let reader = parquet::file::serialized_reader::SerializedFileReader::new(file).unwrap();
        let metadata = reader.metadata();

        assert!(metadata.num_row_groups() > 0);
        let rg = metadata.row_group(0);
        assert!(rg.num_columns() > 0);
        let col = rg.column(0);

        let data_page_offset = col.data_page_offset() as u64;
        let compressed_size = col.compressed_size() as u64;
        let dict_offset = col.dictionary_page_offset().map(|o| o as u64);

        let file = std::fs::File::open(tmpfile.path()).unwrap();
        let locations = compute_page_locations_from_column_chunk(
            &file,
            data_page_offset,
            compressed_size,
            dict_offset,
            0,
            rg.num_rows(),
            col.compression(),
        ).unwrap();

        assert!(!locations.is_empty(), "Expected at least one data page location");

        assert!(
            locations[0].offset >= data_page_offset as i64,
            "First data page offset ({}) should be >= data_page_offset ({})",
            locations[0].offset, data_page_offset
        );

        for i in 1..locations.len() {
            assert!(
                locations[i].first_row_index >= locations[i - 1].first_row_index,
                "first_row_index should be monotonically non-decreasing"
            );
        }

        for loc in &locations {
            assert!(
                loc.compressed_page_size > 0,
                "compressed_page_size should be positive, got {}",
                loc.compressed_page_size
            );
        }
    }

    /// Test with a parquet file that has a dictionary page
    #[test]
    fn test_compute_page_locations_with_dictionary() {
        use parquet::file::properties::WriterProperties;
        use parquet::arrow::ArrowWriter;
        use arrow_array::{StringArray, RecordBatch};
        use arrow_schema::{Schema, Field, DataType};
        use std::sync::Arc;

        let schema = Arc::new(Schema::new(vec![
            Field::new("category", DataType::Utf8, false),
        ]));

        let mut tmpfile = NamedTempFile::new().unwrap();

        let props = WriterProperties::builder()
            .set_dictionary_enabled(true)
            .set_data_page_size_limit(128)
            .set_max_row_group_size(1000)
            .build();

        let values: Vec<String> = (0..200).map(|i| format!("cat_{}", i % 10)).collect();
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(StringArray::from(values))],
        ).unwrap();

        let mut writer = ArrowWriter::try_new(
            tmpfile.as_file_mut(),
            schema.clone(),
            Some(props),
        ).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();

        let file = std::fs::File::open(tmpfile.path()).unwrap();
        let reader = parquet::file::serialized_reader::SerializedFileReader::new(file).unwrap();
        let metadata = reader.metadata();
        let rg = metadata.row_group(0);
        let col = rg.column(0);

        let data_page_offset = col.data_page_offset() as u64;
        let compressed_size = col.compressed_size() as u64;
        let dict_offset = col.dictionary_page_offset().map(|o| o as u64);

        let file = std::fs::File::open(tmpfile.path()).unwrap();
        let locations = compute_page_locations_from_column_chunk(
            &file,
            data_page_offset,
            compressed_size,
            dict_offset,
            0,
            rg.num_rows(),
            col.compression(),
        ).unwrap();

        assert!(!locations.is_empty());

        assert_eq!(
            locations[0].first_row_index, 0,
            "First data page should have first_row_index = 0"
        );

        if dict_offset.is_some() {
            assert!(
                locations[0].offset >= data_page_offset as i64,
                "First data page offset ({}) should be at or after data_page_offset ({})",
                locations[0].offset, data_page_offset
            );
        }
    }

    /// Test page location computation for a nested List<Utf8> column.
    /// Verifies that rep-level scanning produces exact first_row_index values.
    #[test]
    fn test_compute_page_locations_nested_list_column() {
        use parquet::file::properties::WriterProperties;
        use parquet::arrow::ArrowWriter;
        use arrow_array::{ArrayRef, Int64Array, RecordBatch, ListArray, StringArray, builder::ListBuilder, builder::StringBuilder};
        use arrow_schema::{Schema, Field, DataType};
        use std::sync::Arc;

        // Create schema with a List<Utf8> column
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("tags", DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))), true),
        ]));

        let num_rows = 1000;
        let mut tmpfile = NamedTempFile::new().unwrap();

        // Small page size to force multiple pages
        let props = WriterProperties::builder()
            .set_data_page_size_limit(512)
            .set_write_batch_size(100)
            .set_max_row_group_size(num_rows)
            .set_dictionary_enabled(false)
            .build();

        // Build arrays: alternating 1 and 3 element lists (avg 2 values/row)
        let ids: Vec<i64> = (0..num_rows as i64).collect();
        let mut list_builder = ListBuilder::new(StringBuilder::new());
        for i in 0..num_rows {
            if i % 2 == 0 {
                list_builder.values().append_value(format!("a_{}", i));
                list_builder.append(true);
            } else {
                list_builder.values().append_value(format!("x_{}", i));
                list_builder.values().append_value(format!("y_{}", i));
                list_builder.values().append_value(format!("z_{}", i));
                list_builder.append(true);
            }
        }

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(ids)) as ArrayRef,
                Arc::new(list_builder.finish()) as ArrayRef,
            ],
        ).unwrap();

        let mut writer = ArrowWriter::try_new(
            tmpfile.as_file_mut(),
            schema.clone(),
            Some(props),
        ).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();

        // Read metadata
        let file = std::fs::File::open(tmpfile.path()).unwrap();
        let reader = parquet::file::serialized_reader::SerializedFileReader::new(file).unwrap();
        let metadata = reader.metadata();
        let rg = metadata.row_group(0);

        // The List<Utf8> column is the leaf column (tags.item) — find it
        let schema_descr = metadata.file_metadata().schema_descr();
        let mut list_col_idx = None;
        for i in 0..rg.num_columns() {
            if schema_descr.column(i).max_rep_level() > 0 {
                list_col_idx = Some(i);
                break;
            }
        }
        let col_idx = list_col_idx.expect("Should find a nested column");
        let col = rg.column(col_idx);
        let col_desc = schema_descr.column(col_idx);

        assert!(col_desc.max_rep_level() > 0, "Column should be nested");

        let data_page_offset = col.data_page_offset() as u64;
        let compressed_size = col.compressed_size() as u64;
        let dict_offset = col.dictionary_page_offset().map(|o| o as u64);

        let file = std::fs::File::open(tmpfile.path()).unwrap();
        let locations = compute_page_locations_from_column_chunk(
            &file,
            data_page_offset,
            compressed_size,
            dict_offset,
            col_desc.max_rep_level(),
            rg.num_rows(),
            col.compression(),
        ).unwrap();

        assert!(!locations.is_empty(), "Should have data pages for list column");

        // first_row_index[0] must be 0
        assert_eq!(locations[0].first_row_index, 0, "First page should start at row 0");

        // first_row_index must be monotonically non-decreasing
        for i in 1..locations.len() {
            assert!(
                locations[i].first_row_index >= locations[i - 1].first_row_index,
                "first_row_index should be monotonically non-decreasing: page {} has {} but prev has {}",
                i, locations[i].first_row_index, locations[i - 1].first_row_index
            );
        }

        // Sum of rows across all pages should equal num_rows
        // We can compute this from the differences in first_row_index
        // The last page's rows = rg.num_rows() - last.first_row_index
        let last = locations.last().unwrap();
        assert!(
            last.first_row_index < rg.num_rows(),
            "Last page first_row_index ({}) should be < num_rows ({})",
            last.first_row_index, rg.num_rows()
        );
    }

    #[test]
    fn test_thrift_reader_varint() {
        let data = [0x00];
        let mut reader = ThriftReader::new(&data);
        assert_eq!(reader.read_varint().unwrap(), 0);

        let data = [0x01];
        let mut reader = ThriftReader::new(&data);
        assert_eq!(reader.read_varint().unwrap(), 1);

        let data = [0xAC, 0x02];
        let mut reader = ThriftReader::new(&data);
        assert_eq!(reader.read_varint().unwrap(), 300);
    }

    #[test]
    fn test_thrift_reader_zigzag_i32() {
        let data = [0x00];
        let mut reader = ThriftReader::new(&data);
        assert_eq!(reader.read_i32().unwrap(), 0);

        let data = [0x01];
        let mut reader = ThriftReader::new(&data);
        assert_eq!(reader.read_i32().unwrap(), -1);

        let data = [0x02];
        let mut reader = ThriftReader::new(&data);
        assert_eq!(reader.read_i32().unwrap(), 1);

        let data = [0x04];
        let mut reader = ThriftReader::new(&data);
        assert_eq!(reader.read_i32().unwrap(), 2);
    }

    #[test]
    fn test_count_rle_bp_zeros_simple() {
        // RLE run of 5 zeros with bit_width=1: header=0x0A (5<<1|0=10), value=0x00
        let data = [0x0A, 0x00];
        assert_eq!(count_rle_bp_zeros(&data, 1, 5), 5);

        // RLE run of 5 ones with bit_width=1: header=0x0A, value=0x01
        let data = [0x0A, 0x01];
        assert_eq!(count_rle_bp_zeros(&data, 1, 5), 0);

        // Bit-packed: 1 group of 8, bit_width=1, byte=0b10101010 → 4 zeros
        let data = [0x03, 0xAA]; // header=3 (1<<1|1=3, 1 group), data=0xAA=10101010
        assert_eq!(count_rle_bp_zeros(&data, 1, 8), 4);
    }

    #[test]
    fn test_num_required_bits() {
        assert_eq!(num_required_bits(0), 0);
        assert_eq!(num_required_bits(1), 1);
        assert_eq!(num_required_bits(2), 2);
        assert_eq!(num_required_bits(3), 2);
        assert_eq!(num_required_bits(4), 3);
        assert_eq!(num_required_bits(7), 3);
        assert_eq!(num_required_bits(8), 4);
    }

    /// Helper: find a leaf column with the given max_rep_level and compute its page locations.
    fn compute_locations_for_nested_col(
        tmpfile: &NamedTempFile,
        min_rep_level: i16,
    ) -> (Vec<PageLocationEntry>, i64, i16) {
        let file = std::fs::File::open(tmpfile.path()).unwrap();
        let reader = parquet::file::serialized_reader::SerializedFileReader::new(file).unwrap();
        let metadata = reader.metadata();
        let rg = metadata.row_group(0);
        let schema_descr = metadata.file_metadata().schema_descr();

        let mut col_idx = None;
        for i in 0..rg.num_columns() {
            if schema_descr.column(i).max_rep_level() >= min_rep_level {
                col_idx = Some(i);
                break;
            }
        }
        let col_idx = col_idx.expect("Should find a nested column");
        let col = rg.column(col_idx);
        let col_desc = schema_descr.column(col_idx);

        let file = std::fs::File::open(tmpfile.path()).unwrap();
        let locations = compute_page_locations_from_column_chunk(
            &file,
            col.data_page_offset() as u64,
            col.compressed_size() as u64,
            col.dictionary_page_offset().map(|o| o as u64),
            col_desc.max_rep_level(),
            rg.num_rows(),
            col.compression(),
        ).unwrap();

        (locations, rg.num_rows(), col_desc.max_rep_level())
    }

    /// Helper: validate standard page location invariants.
    fn assert_valid_locations(locations: &[PageLocationEntry], num_rows: i64, label: &str) {
        assert!(!locations.is_empty(), "{}: should have data pages", label);
        assert_eq!(locations[0].first_row_index, 0, "{}: first page starts at row 0", label);

        for i in 1..locations.len() {
            assert!(
                locations[i].first_row_index >= locations[i - 1].first_row_index,
                "{}: first_row_index not monotonic at page {}: {} < {}",
                label, i, locations[i].first_row_index, locations[i - 1].first_row_index
            );
        }

        let last = locations.last().unwrap();
        assert!(
            last.first_row_index < num_rows,
            "{}: last page first_row_index ({}) should be < num_rows ({})",
            label, last.first_row_index, num_rows
        );
    }

    /// Test page locations for a Struct containing a List<Utf8>.
    /// Schema: Struct{ name: Utf8, tags: List<Utf8> }
    /// The leaf column `tags.item` has max_rep_level = 1 (from the List).
    #[test]
    fn test_page_locations_struct_with_array() {
        use parquet::file::properties::WriterProperties;
        use parquet::arrow::ArrowWriter;
        use arrow_array::*;
        use arrow_array::builder::*;
        use arrow_schema::{Schema, Field, DataType, Fields};
        use std::sync::Arc;

        let inner_fields = Fields::from(vec![
            Field::new("name", DataType::Utf8, true),
            Field::new("tags", DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))), true),
        ]);
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("info", DataType::Struct(inner_fields.clone()), true),
        ]));

        let num_rows = 500;
        let mut tmpfile = NamedTempFile::new().unwrap();

        let props = WriterProperties::builder()
            .set_data_page_size_limit(256)
            .set_dictionary_enabled(false)
            .build();

        let ids: Vec<i64> = (0..num_rows).collect();

        // Build struct array with inner list
        let mut name_builder = StringBuilder::new();
        let mut tags_builder = ListBuilder::new(StringBuilder::new());
        for i in 0..num_rows as usize {
            name_builder.append_value(format!("name_{}", i));
            // Alternate 1 and 3 tags per row
            if i % 2 == 0 {
                tags_builder.values().append_value(format!("tag_{}", i));
                tags_builder.append(true);
            } else {
                tags_builder.values().append_value(format!("a_{}", i));
                tags_builder.values().append_value(format!("b_{}", i));
                tags_builder.values().append_value(format!("c_{}", i));
                tags_builder.append(true);
            }
        }

        let struct_array = StructArray::from(vec![
            (Arc::new(Field::new("name", DataType::Utf8, true)), Arc::new(name_builder.finish()) as ArrayRef),
            (Arc::new(Field::new("tags", DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))), true)), Arc::new(tags_builder.finish()) as ArrayRef),
        ]);

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(ids)) as ArrayRef,
                Arc::new(struct_array) as ArrayRef,
            ],
        ).unwrap();

        let mut writer = ArrowWriter::try_new(tmpfile.as_file_mut(), schema, Some(props)).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();

        let (locations, num_rows, max_rep) = compute_locations_for_nested_col(&tmpfile, 1);
        assert_eq!(max_rep, 1, "Struct.List leaf should have max_rep_level=1");
        assert_valid_locations(&locations, num_rows, "struct_with_array");
    }

    /// Test page locations for List<Struct{ tags: List<Utf8> }>.
    /// The innermost leaf column has max_rep_level = 2 (outer list + inner list).
    /// This exercises bit_width = 2 in the RLE/Bit-Packed decoder.
    #[test]
    fn test_page_locations_array_of_struct_with_array() {
        use parquet::file::properties::WriterProperties;
        use parquet::arrow::ArrowWriter;
        use arrow_array::*;
        use arrow_array::builder::*;
        use arrow_schema::{Schema, Field, DataType, Fields};
        use std::sync::Arc;

        // Schema: List<Struct{ tags: List<Utf8> }>
        let inner_struct_fields = Fields::from(vec![
            Field::new("tags", DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))), true),
        ]);
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("entries",
                DataType::List(Arc::new(Field::new("item",
                    DataType::Struct(inner_struct_fields.clone()), true))),
                true),
        ]));

        let num_rows = 10_000;
        let mut tmpfile = NamedTempFile::new().unwrap();

        let props = WriterProperties::builder()
            .set_data_page_size_limit(256)
            .set_dictionary_enabled(false)
            .build();

        let ids: Vec<i64> = (0..num_rows).collect();

        // Build: List<Struct{ tags: List<Utf8> }>
        // Inner: tags list builder
        let tags_builder = ListBuilder::new(StringBuilder::new());
        // Struct builder wrapping the tags
        let struct_builder = StructBuilder::new(
            inner_struct_fields.clone(),
            vec![Box::new(tags_builder)],
        );
        // Outer list wrapping the struct
        let mut outer_list_builder = ListBuilder::new(struct_builder);

        for i in 0..num_rows as usize {
            let struct_b = outer_list_builder.values();
            // Each row has 1-2 struct entries in the outer list
            let n_entries = if i % 2 == 0 { 1 } else { 2 };
            for _ in 0..n_entries {
                let tags_b = struct_b.field_builder::<ListBuilder<StringBuilder>>(0).unwrap();
                // Each struct has 1-2 tags
                tags_b.values().append_value(format!("t_{}_{}", i, 0));
                if i % 3 != 0 {
                    tags_b.values().append_value(format!("t_{}_{}", i, 1));
                }
                tags_b.append(true);
                struct_b.append(true);
            }
            outer_list_builder.append(true);
        }

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(ids)) as ArrayRef,
                Arc::new(outer_list_builder.finish()) as ArrayRef,
            ],
        ).unwrap();

        let mut writer = ArrowWriter::try_new(tmpfile.as_file_mut(), schema, Some(props)).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();

        let (locations, num_rows, max_rep) = compute_locations_for_nested_col(&tmpfile, 2);
        assert_eq!(max_rep, 2, "List<Struct<List<Utf8>>> leaf should have max_rep_level=2");
        assert_valid_locations(&locations, num_rows, "array_of_struct_with_array");

        // With bit_width=2, verify we have multiple pages
        assert!(
            locations.len() >= 2,
            "Expected multiple pages for {} rows with 256B page limit, got {}",
            num_rows, locations.len()
        );
    }

    // ===================================================================
    // Error / malformed data tests
    // ===================================================================

    #[test]
    fn test_count_rle_bp_zeros_empty_data() {
        // Empty data should return 0 zeros
        assert_eq!(count_rle_bp_zeros(&[], 1, 10), 0);
    }

    #[test]
    fn test_count_rle_bp_zeros_truncated_rle_value() {
        // RLE header says 5 values but no value byte follows (bit_width=1 → needs 1 byte)
        let data = [0x0A]; // header=10 (5<<1|0), but missing value byte
        // Should gracefully stop, not panic
        assert_eq!(count_rle_bp_zeros(&data, 1, 5), 0);
    }

    #[test]
    fn test_count_rle_bp_zeros_truncated_bitpacked() {
        // Bit-packed header says 1 group (8 values) but no data bytes follow
        let data = [0x03]; // header=3 (1<<1|1), but no packed bytes
        // Should gracefully stop, not panic
        assert_eq!(count_rle_bp_zeros(&data, 1, 8), 0);
    }

    #[test]
    fn test_count_rle_bp_zeros_bitwidth_2_general_path() {
        // Test bit_width=2 which takes the general bit-extraction path
        // RLE run of 4 zeros: header=0x08 (4<<1|0=8), value byte=0x00
        let data = [0x08, 0x00];
        assert_eq!(count_rle_bp_zeros(&data, 2, 4), 4);

        // RLE run of 4 with value=1: header=0x08, value byte=0x01
        let data = [0x08, 0x01];
        assert_eq!(count_rle_bp_zeros(&data, 2, 4), 0);

        // Bit-packed with bit_width=2: 1 group, 8 values packed in 2 bytes
        // Values: 0,1,2,3,0,1,2,3 → bits: 00 01 10 11 00 01 10 11
        // Byte 0: 00_01_10_11 reversed per byte = 0b11_10_01_00 = 0xE4
        // Byte 1: 00_01_10_11 reversed per byte = 0b11_10_01_00 = 0xE4
        // Actually in little-endian bit order: val0=bits[0:1], val1=bits[2:3], ...
        // val0=0 → 00, val1=1 → 01, val2=2 → 10, val3=3 → 11
        // byte0 = 0b_11_10_01_00 = 0xE4
        // val4=0 → 00, val5=1 → 01, val6=2 → 10, val7=3 → 11
        // byte1 = 0b_11_10_01_00 = 0xE4
        let data = [0x03, 0xE4, 0xE4]; // header=3 (1 group bit-packed)
        assert_eq!(count_rle_bp_zeros(&data, 2, 8), 2); // val0=0 and val4=0
    }

    #[test]
    fn test_count_rows_from_decompressed_too_short() {
        // Data shorter than 4 bytes (rep_levels length prefix) should error
        let result = count_rows_from_decompressed(&[0x00, 0x01], 1, 10);
        assert!(result.is_err());
    }

    #[test]
    fn test_count_rows_from_decompressed_rep_len_exceeds_data() {
        // rep_levels_len = 100 but only 6 bytes of data total
        let data = [100u8, 0, 0, 0, 0, 0];
        let result = count_rows_from_decompressed(&data, 1, 10);
        assert!(result.is_err());
    }

    #[test]
    fn test_compute_page_locations_zero_size_chunk() {
        // A column chunk with total_compressed_size = 0 should return empty
        let tmpfile = tempfile::NamedTempFile::new().unwrap();
        let file = std::fs::File::open(tmpfile.path()).unwrap();
        let result = compute_page_locations_from_column_chunk(
            &file,
            0,    // data_page_offset
            0,    // total_compressed_size = 0
            None, // no dictionary
            0,    // flat column
            100,  // rg_num_rows
            Compression::UNCOMPRESSED,
        );
        assert!(result.is_ok());
        assert!(result.unwrap().is_empty());
    }

    #[test]
    fn test_count_rle_bp_zeros_total_values_zero() {
        // If total_values is 0, should return 0 regardless of data
        let data = [0x0A, 0x00]; // would normally be 5 zeros
        assert_eq!(count_rle_bp_zeros(&data, 1, 0), 0);
    }

    #[test]
    fn test_count_rle_bp_zeros_bit_width_zero() {
        // bit_width=0 means all values are implicitly 0
        assert_eq!(count_rle_bp_zeros(&[], 0, 100), 100);
    }

    /// Test that pages are merged for List columns with large arrays (650+ elements)
    /// that span page boundaries. Without merging, arrow-rs's scan_ranges() would
    /// miss continuation pages and produce decode errors.
    #[test]
    fn test_page_merging_large_array_elements() {
        use parquet::file::properties::WriterProperties;
        use parquet::arrow::ArrowWriter;
        use arrow_array::*;
        use arrow_array::builder::*;
        use arrow_schema::{Schema, Field, DataType};
        use std::sync::Arc;

        let schema = Arc::new(Schema::new(vec![
            Field::new("tags", DataType::List(Arc::new(
                Field::new("item", DataType::Utf8, true)
            )), true),
        ]));

        let num_rows = 100;
        let elements_per_row = 700; // Large enough to span pages
        let mut tmpfile = NamedTempFile::new().unwrap();

        // Very small page size to force rows to span multiple pages
        let props = WriterProperties::builder()
            .set_data_page_size_limit(1024) // 1KB pages — a single row with 700 elements won't fit
            .set_dictionary_enabled(false)
            .set_max_row_group_size(num_rows)
            .build();

        let mut list_builder = ListBuilder::new(StringBuilder::new());
        for i in 0..num_rows {
            for j in 0..elements_per_row {
                list_builder.values().append_value(format!("v_{}_{}", i, j));
            }
            list_builder.append(true);
        }

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(list_builder.finish()) as ArrayRef],
        ).unwrap();

        let mut writer = ArrowWriter::try_new(
            tmpfile.as_file_mut(),
            schema.clone(),
            Some(props),
        ).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();

        // Compute page locations with merging
        let file = std::fs::File::open(tmpfile.path()).unwrap();
        let reader = parquet::file::serialized_reader::SerializedFileReader::new(file).unwrap();
        let metadata = reader.metadata();
        let rg = metadata.row_group(0);
        let schema_descr = metadata.file_metadata().schema_descr();

        // Find the list leaf column
        let mut list_col_idx = None;
        for i in 0..rg.num_columns() {
            if schema_descr.column(i).max_rep_level() > 0 {
                list_col_idx = Some(i);
                break;
            }
        }
        let col_idx = list_col_idx.expect("Should find list leaf column");
        let col = rg.column(col_idx);
        let col_desc = schema_descr.column(col_idx);

        let file = std::fs::File::open(tmpfile.path()).unwrap();
        let locations = compute_page_locations_from_column_chunk(
            &file,
            col.data_page_offset() as u64,
            col.compressed_size() as u64,
            col.dictionary_page_offset().map(|o| o as u64),
            col_desc.max_rep_level(),
            rg.num_rows(),
            col.compression(),
        ).unwrap();

        // Validate standard invariants
        assert_valid_locations(&locations, rg.num_rows(), "large_array_merged");

        // Key check: first_row_index must be STRICTLY increasing for merged pages.
        // This proves that continuation pages were merged and each merged page
        // represents at least one complete new row.
        for i in 1..locations.len() {
            assert!(
                locations[i].first_row_index > locations[i - 1].first_row_index,
                "After merging, first_row_index should be strictly increasing: \
                 page {} has fri={} but page {} has fri={}",
                i, locations[i].first_row_index, i - 1, locations[i - 1].first_row_index
            );
        }

        // With 700 elements per row at 1KB page limit, each row's data is ~10-15KB,
        // spanning 10-15 pages. After merging, each merged "page" covers at least
        // one row, so we should have at most num_rows merged pages.
        assert!(
            locations.len() <= num_rows,
            "Merged page count ({}) should be <= num_rows ({})",
            locations.len(), num_rows
        );

        // Each merged page's byte range should be contiguous and non-overlapping
        for i in 1..locations.len() {
            let prev_end = locations[i - 1].offset + locations[i - 1].compressed_page_size as i64;
            assert!(
                locations[i].offset >= prev_end,
                "Merged pages should not overlap: page {} ends at {} but page {} starts at {}",
                i - 1, prev_end, i, locations[i].offset
            );
        }
    }

    /// Test first_rep_is_zero with RLE-encoded zero value.
    #[test]
    fn test_first_rep_is_zero_rle_zero() {
        // RLE: header=0x0A (5<<1|0 = 10), value=0x00 → first rep is 0
        let data = [0x0A, 0x00];
        assert!(first_rep_is_zero(&data, 1));
    }

    /// Test first_rep_is_zero with RLE-encoded non-zero value.
    #[test]
    fn test_first_rep_is_zero_rle_nonzero() {
        // RLE: header=0x0A (5<<1|0 = 10), value=0x01 → first rep is 1
        let data = [0x0A, 0x01];
        assert!(!first_rep_is_zero(&data, 1));
    }

    /// Test first_rep_is_zero with bit-packed zero.
    #[test]
    fn test_first_rep_is_zero_bitpacked_zero() {
        // Bit-packed: header=0x03 (1<<1|1 = 3, 1 group), first value = 0
        // byte = 0b11111110 → lowest bit is 0
        let data = [0x03, 0xFE];
        assert!(first_rep_is_zero(&data, 1));
    }

    /// Test first_rep_is_zero with bit-packed non-zero.
    #[test]
    fn test_first_rep_is_zero_bitpacked_nonzero() {
        // Bit-packed: header=0x03, first value = 1
        // byte = 0b00000001 → lowest bit is 1
        let data = [0x03, 0x01];
        assert!(!first_rep_is_zero(&data, 1));
    }
}

// page_index.rs - Compute page-level offset index for parquet column chunks
//
// For parquet files that lack a native offset index in their footer, this module
// scans the Thrift-encoded page headers within each column chunk to compute
// page locations (offset, compressed_page_size, first_row_index). These are
// stored in the manifest and injected at read time to enable page-level byte
// range reads instead of full column chunk downloads.
//
// Uses a self-contained minimal Thrift compact protocol parser — no dependency
// on the parquet crate's internal (non-public) Thrift types.

use std::io::{Read, Seek, SeekFrom};

use anyhow::{Context, Result};

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
/// # Arguments
/// * `file` - Open file handle for the parquet file
/// * `data_page_offset` - Byte offset of the first data page in this column chunk
/// * `total_compressed_size` - Total compressed size of the column chunk
/// * `dictionary_page_offset` - Optional byte offset of the dictionary page
pub fn compute_page_locations_from_column_chunk(
    file: &std::fs::File,
    data_page_offset: u64,
    total_compressed_size: u64,
    dictionary_page_offset: Option<u64>,
) -> Result<Vec<PageLocationEntry>> {
    // The column chunk starts at dictionary_page_offset (if present) or data_page_offset.
    // total_compressed_size covers ALL pages in the chunk (dict + data), measured from the
    // start of the column chunk.
    let start_offset = dictionary_page_offset.unwrap_or(data_page_offset);
    let end_offset = start_offset + total_compressed_size;

    if end_offset <= start_offset {
        return Ok(Vec::new());
    }

    let chunk_len = (end_offset - start_offset) as usize;

    // Read column chunk bytes into buffer
    let mut buf = vec![0u8; chunk_len];
    let mut file_reader = std::io::BufReader::new(file);
    file_reader
        .seek(SeekFrom::Start(start_offset))
        .context("Failed to seek to column chunk start")?;
    file_reader
        .read_exact(&mut buf)
        .context("Failed to read column chunk bytes")?;

    let mut locations = Vec::new();
    let mut pos: usize = 0;
    let mut cumulative_rows: i64 = 0;

    while pos < chunk_len {
        let page_file_offset = start_offset + pos as u64;
        let remaining = &buf[pos..];

        // Parse the Thrift-encoded PageHeader
        let header = parse_page_header(remaining)
            .with_context(|| format!(
                "Failed to parse page header at offset {} (pos {} of {} chunk bytes)",
                page_file_offset, pos, chunk_len
            ))?;

        // total_page_size = header_size + compressed_page_body_size
        let total_page_size = header.header_size as i32 + header.compressed_page_size;

        // Only include DATA pages in the offset index — per the Parquet spec,
        // the OffsetIndex contains page locations for data pages only. Dictionary
        // pages are NOT included; the reader handles them separately via the
        // column chunk's dictionary_page_offset metadata.
        match header.page_type {
            PAGE_TYPE_DATA_PAGE => {
                locations.push(PageLocationEntry {
                    offset: page_file_offset as i64,
                    compressed_page_size: total_page_size,
                    first_row_index: cumulative_rows,
                });
                cumulative_rows += header.num_values as i64;
            }
            PAGE_TYPE_DATA_PAGE_V2 => {
                locations.push(PageLocationEntry {
                    offset: page_file_offset as i64,
                    compressed_page_size: total_page_size,
                    first_row_index: cumulative_rows,
                });
                cumulative_rows += header.num_values as i64;
            }
            PAGE_TYPE_DICTIONARY_PAGE | PAGE_TYPE_INDEX_PAGE => {
                // Dictionary/index pages are NOT part of the offset index.
                // The reader fetches dictionary pages separately using
                // the column chunk metadata's dictionary_page_offset.
            }
            _ => {}
        }

        // Advance past header + compressed page body
        let advance = header.header_size + header.compressed_page_size as usize;
        if advance == 0 {
            // Safety: prevent infinite loop on malformed data
            break;
        }
        pos += advance;
    }

    Ok(locations)
}

/// Parsed page header — only the fields we need.
struct ParsedPageHeader {
    page_type: i32,
    compressed_page_size: i32,
    /// Number of values in this page (from data_page_header or data_page_header_v2).
    /// 0 for dictionary/index pages.
    num_values: i32,
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
    let mut compressed_page_size: Option<i32> = None;
    let mut num_values: i32 = 0;

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
                // uncompressed_page_size: i32 — read and discard
                reader.read_i32()?;
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
                // field 1 is num_values — we don't need it but must skip the struct
                reader.skip_field(field_type)?;
            }
            8 => {
                // data_page_header_v2: DataPageHeaderV2 struct
                // field 1 is num_values (i32)
                num_values = read_first_i32_from_struct(&mut reader)?;
            }
            _ => {
                reader.skip_field(field_type)?;
            }
        }
    }

    let page_type = page_type
        .ok_or_else(|| anyhow::anyhow!("PageHeader missing required field: type"))?;
    let compressed_page_size = compressed_page_size
        .ok_or_else(|| anyhow::anyhow!("PageHeader missing required field: compressed_page_size"))?;

    Ok(ParsedPageHeader {
        page_type,
        compressed_page_size,
        num_values,
        header_size: reader.position(),
    })
}

/// Read the first i32 field from a sub-struct, then skip the rest.
/// Used for DataPageHeader.num_values and DataPageHeaderV2.num_values.
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

        // Create a small parquet file with known structure
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
        ]));

        let mut tmpfile = NamedTempFile::new().unwrap();

        // Write with small page size to get multiple pages
        let props = WriterProperties::builder()
            .set_data_page_size_limit(64) // Force small pages
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

        // Read metadata to get column chunk info
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

        // Compute page locations
        let file = std::fs::File::open(tmpfile.path()).unwrap();
        let locations = compute_page_locations_from_column_chunk(
            &file,
            data_page_offset,
            compressed_size,
            dict_offset,
        ).unwrap();

        // Verify: should have at least one page (data pages only, not dictionary)
        assert!(!locations.is_empty(), "Expected at least one data page location");

        // First data page offset should be at or after data_page_offset
        // (dictionary pages come before data_page_offset and are excluded)
        assert!(
            locations[0].offset >= data_page_offset as i64,
            "First data page offset ({}) should be >= data_page_offset ({})",
            locations[0].offset, data_page_offset
        );

        // first_row_index should be monotonically non-decreasing
        for i in 1..locations.len() {
            assert!(
                locations[i].first_row_index >= locations[i - 1].first_row_index,
                "first_row_index should be monotonically non-decreasing"
            );
        }

        // All compressed_page_size values should be positive
        for loc in &locations {
            assert!(
                loc.compressed_page_size > 0,
                "compressed_page_size should be positive, got {}",
                loc.compressed_page_size
            );
        }

        // Total first_row_index of last data page + its rows should account for all rows
        // (We can't verify exact count without knowing per-page row counts, but the
        // cumulative_rows tracking should equal num_rows for proper data pages)
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

        // Use dictionary encoding (default for strings) with small pages
        let props = WriterProperties::builder()
            .set_dictionary_enabled(true)
            .set_data_page_size_limit(128)
            .set_max_row_group_size(1000)
            .build();

        // Create repeated strings to trigger dictionary encoding
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

        // Read metadata
        let file = std::fs::File::open(tmpfile.path()).unwrap();
        let reader = parquet::file::serialized_reader::SerializedFileReader::new(file).unwrap();
        let metadata = reader.metadata();
        let col = metadata.row_group(0).column(0);

        let data_page_offset = col.data_page_offset() as u64;
        let compressed_size = col.compressed_size() as u64;
        let dict_offset = col.dictionary_page_offset().map(|o| o as u64);

        let file = std::fs::File::open(tmpfile.path()).unwrap();
        let locations = compute_page_locations_from_column_chunk(
            &file,
            data_page_offset,
            compressed_size,
            dict_offset,
        ).unwrap();

        assert!(!locations.is_empty());

        // Dictionary pages should NOT appear in the offset index — only data pages.
        // First data page should have first_row_index = 0.
        assert_eq!(
            locations[0].first_row_index, 0,
            "First data page should have first_row_index = 0"
        );

        // Verify the first location offset is at or after the data_page_offset
        // (dictionary pages, if any, come before data_page_offset)
        if dict_offset.is_some() {
            assert!(
                locations[0].offset >= data_page_offset as i64,
                "First data page offset ({}) should be at or after data_page_offset ({})",
                locations[0].offset, data_page_offset
            );
        }
    }

    #[test]
    fn test_thrift_reader_varint() {
        // Test varint decoding
        let data = [0x00]; // varint 0
        let mut reader = ThriftReader::new(&data);
        assert_eq!(reader.read_varint().unwrap(), 0);

        let data = [0x01]; // varint 1
        let mut reader = ThriftReader::new(&data);
        assert_eq!(reader.read_varint().unwrap(), 1);

        let data = [0xAC, 0x02]; // varint 300
        let mut reader = ThriftReader::new(&data);
        assert_eq!(reader.read_varint().unwrap(), 300);
    }

    #[test]
    fn test_thrift_reader_zigzag_i32() {
        // zigzag: 0 → 0, 1 → -1, 2 → 1, 3 → -2, 4 → 2
        let data = [0x00]; // zigzag 0 → 0
        let mut reader = ThriftReader::new(&data);
        assert_eq!(reader.read_i32().unwrap(), 0);

        let data = [0x01]; // zigzag 1 → -1
        let mut reader = ThriftReader::new(&data);
        assert_eq!(reader.read_i32().unwrap(), -1);

        let data = [0x02]; // zigzag 2 → 1
        let mut reader = ThriftReader::new(&data);
        assert_eq!(reader.read_i32().unwrap(), 1);

        let data = [0x04]; // zigzag 4 → 2
        let mut reader = ThriftReader::new(&data);
        assert_eq!(reader.read_i32().unwrap(), 2);
    }
}

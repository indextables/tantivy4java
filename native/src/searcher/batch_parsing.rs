// batch_parsing.rs - Batch document parsing from ByteBuffer
// Extracted from mod.rs during refactoring
// Contains: parse_batch_buffer, parse_batch_documents, parse_single_document,
//           parse_field, parse_field_value, and type-safe value adders

use jni::objects::JByteBuffer;
use jni::JNIEnv;

use tantivy::{IndexWriter as TantivyIndexWriter, DateTime};
use tantivy::schema::{TantivyDocument, Field, Schema, Facet, OwnedValue};
use std::net::IpAddr;
use std::collections::BTreeMap;
use std::sync::Mutex;

use crate::utils::with_arc_safe;

/// Parse batch document buffer according to the Tantivy4Java batch protocol
pub(crate) fn parse_batch_buffer(env: &mut JNIEnv, writer_ptr: i64, buffer: JByteBuffer) -> Result<Vec<u64>, String> {
    // Get the direct ByteBuffer from Java
    let byte_buffer = match env.get_direct_buffer_address(&buffer) {
        Ok(address) => address,
        Err(e) => return Err(format!("Failed to get buffer address: {}", e)),
    };

    let buffer_size = match env.get_direct_buffer_capacity(&buffer) {
        Ok(capacity) => capacity,
        Err(e) => return Err(format!("Failed to get buffer capacity: {}", e)),
    };

    // Validate buffer parameters before creating slice
    if byte_buffer.is_null() || buffer_size == 0 {
        return Err("Invalid buffer: null pointer or zero size".to_string());
    }

    // Additional safety check - limit maximum buffer size to prevent crashes
    if buffer_size > 100_000_000 {  // 100MB limit
        return Err("Buffer too large: exceeds 100MB limit".to_string());
    }

    // Create a slice from the direct buffer safely
    let buffer_slice = unsafe {
        // SAFETY: We've validated byte_buffer is not null and buffer_size is reasonable
        std::slice::from_raw_parts(byte_buffer as *const u8, buffer_size)
    };

    // Parse the buffer according to the batch protocol
    parse_batch_documents(env, writer_ptr, buffer_slice)
}

/// Parse the batch document format and add documents to the writer
fn parse_batch_documents(_env: &mut JNIEnv, writer_ptr: i64, buffer: &[u8]) -> Result<Vec<u64>, String> {
    if buffer.len() < 16 {
        return Err("Buffer too small for batch format".to_string());
    }

    // Read footer to get document count and offset table position
    let footer_start = buffer.len() - 12;
    let offset_table_pos = u32::from_ne_bytes([
        buffer[footer_start],
        buffer[footer_start + 1],
        buffer[footer_start + 2],
        buffer[footer_start + 3]
    ]) as usize;

    let doc_count = u32::from_ne_bytes([
        buffer[footer_start + 4],
        buffer[footer_start + 5],
        buffer[footer_start + 6],
        buffer[footer_start + 7]
    ]) as usize;

    let footer_magic = u32::from_ne_bytes([
        buffer[footer_start + 8],
        buffer[footer_start + 9],
        buffer[footer_start + 10],
        buffer[footer_start + 11]
    ]);

    // Validate magic numbers
    const MAGIC_NUMBER: u32 = 0x54414E54; // "TANT"
    let header_magic = u32::from_ne_bytes([buffer[0], buffer[1], buffer[2], buffer[3]]);

    if header_magic != MAGIC_NUMBER || footer_magic != MAGIC_NUMBER {
        return Err(format!("Invalid magic number: header={:x}, footer={:x}", header_magic, footer_magic));
    }

    // Read document offsets
    let mut offsets = Vec::with_capacity(doc_count);
    for i in 0..doc_count {
        let offset_pos = offset_table_pos + (i * 4);
        if offset_pos + 4 > buffer.len() {
            return Err("Invalid offset table".to_string());
        }

        let offset = u32::from_ne_bytes([
            buffer[offset_pos],
            buffer[offset_pos + 1],
            buffer[offset_pos + 2],
            buffer[offset_pos + 3]
        ]) as usize;

        offsets.push(offset);
    }

    // Process each document
    let opstamps = with_arc_safe::<Mutex<TantivyIndexWriter>, Result<Vec<u64>, String>>(writer_ptr, |writer_mutex| {
        let writer = writer_mutex.lock().unwrap();
        let schema = writer.index().schema();
        let mut opstamps = Vec::with_capacity(doc_count);

        for (doc_index, &doc_offset) in offsets.iter().enumerate() {
            match parse_single_document(&schema, buffer, doc_offset) {
                Ok(document) => {
                    match writer.add_document(document) {
                        Ok(opstamp) => opstamps.push(opstamp),
                        Err(e) => return Err(format!("Failed to add document {}: {}", doc_index, e)),
                    }
                },
                Err(e) => return Err(format!("Failed to parse document {}: {}", doc_index, e)),
            }
        }

        Ok(opstamps)
    })
    .ok_or_else(|| "Invalid IndexWriter pointer".to_string())??;

    Ok(opstamps)
}

/// Parse a single document from the buffer
fn parse_single_document(schema: &Schema, buffer: &[u8], offset: usize) -> Result<TantivyDocument, String> {
    if offset + 2 > buffer.len() {
        return Err("Document offset out of bounds".to_string());
    }

    let mut pos = offset;
    let field_count = u16::from_ne_bytes([buffer[pos], buffer[pos + 1]]) as usize;
    pos += 2;

    let mut document = TantivyDocument::default();

    for _ in 0..field_count {
        let (field_name, field_values, new_pos) = parse_field(buffer, pos)?;
        pos = new_pos;

        // Look up field in schema
        let field = match schema.get_field(&field_name) {
            Ok(f) => f,
            Err(_) => return Err(format!("Field '{}' not found in schema", field_name)),
        };

        // Add values to document with schema-aware type conversion
        for value in field_values {
            match value {
                FieldValue::Text(text) => document.add_text(field, &text),
                FieldValue::Integer(int_val) => {
                    // Check schema field type and convert safely
                    add_integer_value_safely(&mut document, schema, field, &field_name, int_val)?;
                },
                FieldValue::Unsigned(uint_val) => {
                    // Check schema field type and convert safely
                    add_unsigned_value_safely(&mut document, schema, field, uint_val);
                },
                FieldValue::Float(float_val) => {
                    // Check schema field type and validate
                    add_float_value_safely(&mut document, schema, field, &field_name, float_val)?;
                },
                FieldValue::Boolean(bool_val) => {
                    // Check schema field type and validate
                    add_boolean_value_safely(&mut document, schema, field, &field_name, bool_val)?;
                },
                FieldValue::Date(date_nanos) => {
                    // Check schema field type and validate (nanoseconds for microsecond precision)
                    add_date_value_safely(&mut document, schema, field, &field_name, date_nanos)?;
                },
                FieldValue::Bytes(bytes) => document.add_bytes(field, &bytes),
                FieldValue::Json(json_str) => {
                    // Parse JSON string and add as JSON object using tantivy::schema::OwnedValue
                    match serde_json::from_str::<tantivy::schema::OwnedValue>(&json_str) {
                        Ok(json_value) => {
                            match json_value {
                                OwnedValue::Object(obj) => {
                                    // Convert Vec<(String, OwnedValue)> to BTreeMap for tantivy
                                    let json_map: BTreeMap<String, OwnedValue> = obj.into_iter().collect();
                                    document.add_object(field, json_map);
                                },
                                _ => return Err(format!("JSON field '{}' must be an object/map, not a primitive value", field_name)),
                            }
                        },
                        Err(e) => return Err(format!("Invalid JSON in field '{}': {}", field_name, e)),
                    }
                },
                FieldValue::IpAddr(ip_str) => {
                    // Parse IP address - convert to IPv6 format for Tantivy
                    match ip_str.parse::<std::net::IpAddr>() {
                        Ok(ip) => {
                            let ipv6 = match ip {
                                IpAddr::V4(ipv4) => ipv4.to_ipv6_mapped(),
                                IpAddr::V6(ipv6) => ipv6,
                            };
                            document.add_ip_addr(field, ipv6);
                        },
                        Err(e) => return Err(format!("Invalid IP address in field '{}': {}", field_name, e)),
                    }
                },
                FieldValue::Facet(facet_path) => {
                    // Parse facet path
                    let facet = Facet::from(&facet_path);
                    document.add_facet(field, facet);
                },
            }
        }
    }

    Ok(document)
}

/// Field value types for batch parsing
#[derive(Debug)]
enum FieldValue {
    Text(String),
    Integer(i64),
    Unsigned(u64),
    Float(f64),
    Boolean(bool),
    Date(i64),
    Bytes(Vec<u8>),
    Json(String),
    IpAddr(String),
    Facet(String),
}

/// Parse a field from the buffer
fn parse_field(buffer: &[u8], mut pos: usize) -> Result<(String, Vec<FieldValue>, usize), String> {
    if pos + 2 > buffer.len() {
        return Err("Field name length out of bounds".to_string());
    }

    // Read field name
    let name_len = u16::from_ne_bytes([buffer[pos], buffer[pos + 1]]) as usize;
    pos += 2;

    if pos + name_len + 1 + 2 > buffer.len() {
        return Err("Field name out of bounds".to_string());
    }

    let field_name = String::from_utf8(buffer[pos..pos + name_len].to_vec())
        .map_err(|e| format!("Invalid field name UTF-8: {}", e))?;
    pos += name_len;

    // Read field type
    let field_type = buffer[pos];
    pos += 1;

    // Read value count
    let value_count = u16::from_ne_bytes([buffer[pos], buffer[pos + 1]]) as usize;
    pos += 2;

    // Read values
    let mut values = Vec::with_capacity(value_count);
    for _ in 0..value_count {
        let (value, new_pos) = parse_field_value(buffer, pos, field_type)?;
        values.push(value);
        pos = new_pos;
    }

    Ok((field_name, values, pos))
}

/// Parse a field value based on type
fn parse_field_value(buffer: &[u8], mut pos: usize, field_type: u8) -> Result<(FieldValue, usize), String> {
    match field_type {
        0 | 6 | 7 => { // TEXT, JSON, IP_ADDR
            if pos + 4 > buffer.len() {
                return Err("String length out of bounds".to_string());
            }

            let str_len = u32::from_ne_bytes([buffer[pos], buffer[pos + 1], buffer[pos + 2], buffer[pos + 3]]) as usize;
            pos += 4;

            if pos + str_len > buffer.len() {
                return Err("String data out of bounds".to_string());
            }

            let string_val = String::from_utf8(buffer[pos..pos + str_len].to_vec())
                .map_err(|e| format!("Invalid UTF-8: {}", e))?;
            pos += str_len;

            let value = match field_type {
                0 => FieldValue::Text(string_val),
                6 => FieldValue::Json(string_val),
                7 => FieldValue::IpAddr(string_val),
                _ => unreachable!(),
            };
            Ok((value, pos))
        },
        1 => { // INTEGER
            if pos + 8 > buffer.len() {
                return Err("Integer value out of bounds".to_string());
            }

            let int_val = i64::from_ne_bytes([
                buffer[pos], buffer[pos + 1], buffer[pos + 2], buffer[pos + 3],
                buffer[pos + 4], buffer[pos + 5], buffer[pos + 6], buffer[pos + 7]
            ]);
            pos += 8;

            Ok((FieldValue::Integer(int_val), pos))
        },
        8 => { // UNSIGNED
            if pos + 8 > buffer.len() {
                return Err("Unsigned integer value out of bounds".to_string());
            }

            let uint_val = u64::from_ne_bytes([
                buffer[pos], buffer[pos + 1], buffer[pos + 2], buffer[pos + 3],
                buffer[pos + 4], buffer[pos + 5], buffer[pos + 6], buffer[pos + 7]
            ]);
            pos += 8;

            Ok((FieldValue::Unsigned(uint_val), pos))
        },
        2 => { // FLOAT
            if pos + 8 > buffer.len() {
                return Err("Float value out of bounds".to_string());
            }

            let float_val = f64::from_ne_bytes([
                buffer[pos], buffer[pos + 1], buffer[pos + 2], buffer[pos + 3],
                buffer[pos + 4], buffer[pos + 5], buffer[pos + 6], buffer[pos + 7]
            ]);
            pos += 8;

            Ok((FieldValue::Float(float_val), pos))
        },
        3 => { // BOOLEAN
            if pos + 1 > buffer.len() {
                return Err("Boolean value out of bounds".to_string());
            }

            let bool_val = buffer[pos] != 0;
            pos += 1;

            Ok((FieldValue::Boolean(bool_val), pos))
        },
        4 => { // DATE
            if pos + 8 > buffer.len() {
                return Err("Date value out of bounds".to_string());
            }

            let date_millis = i64::from_ne_bytes([
                buffer[pos], buffer[pos + 1], buffer[pos + 2], buffer[pos + 3],
                buffer[pos + 4], buffer[pos + 5], buffer[pos + 6], buffer[pos + 7]
            ]);
            pos += 8;

            Ok((FieldValue::Date(date_millis), pos))
        },
        5 => { // BYTES
            if pos + 4 > buffer.len() {
                return Err("Bytes length out of bounds".to_string());
            }

            let bytes_len = u32::from_ne_bytes([buffer[pos], buffer[pos + 1], buffer[pos + 2], buffer[pos + 3]]) as usize;
            pos += 4;

            if pos + bytes_len > buffer.len() {
                return Err("Bytes data out of bounds".to_string());
            }

            let bytes = buffer[pos..pos + bytes_len].to_vec();
            pos += bytes_len;

            Ok((FieldValue::Bytes(bytes), pos))
        },
        9 => { // FACET
            if pos + 4 > buffer.len() {
                return Err("Facet length out of bounds".to_string());
            }

            let facet_len = u32::from_ne_bytes([buffer[pos], buffer[pos + 1], buffer[pos + 2], buffer[pos + 3]]) as usize;
            pos += 4;

            if pos + facet_len > buffer.len() {
                return Err("Facet data out of bounds".to_string());
            }

            let facet_path = String::from_utf8(buffer[pos..pos + facet_len].to_vec())
                .map_err(|e| format!("Invalid facet UTF-8: {}", e))?;
            pos += facet_len;

            Ok((FieldValue::Facet(facet_path), pos))
        },
        _ => Err(format!("Unknown field type: {}", field_type)),
    }
}

/// Safely add an integer value to document, converting based on schema field type
fn add_integer_value_safely(document: &mut TantivyDocument, schema: &Schema, field: Field, field_name: &str, int_val: i64) -> Result<(), String> {
    let field_entry = schema.get_field_entry(field);
    let field_type = field_entry.field_type();

    match field_type {
        tantivy::schema::FieldType::U64(_) => {
            // Schema expects unsigned, but we have signed - convert safely
            if int_val < 0 {
                // Negative value can't be converted to unsigned - use 0 as fallback
                document.add_u64(field, 0);
            } else {
                document.add_u64(field, int_val as u64);
            }
            Ok(())
        },
        tantivy::schema::FieldType::I64(_) => {
            // Schema expects signed - direct assignment
            document.add_i64(field, int_val);
            Ok(())
        },
        tantivy::schema::FieldType::Date(_) => {
            // Schema expects Date, but we have integer - this is a type mismatch
            Err(format!(
                "Type mismatch for field '{}': attempting to add INTEGER value to Date field. Use addDate() instead.",
                field_name
            ))
        },
        _ => {
            // For other field types, return error
            Err(format!(
                "Type mismatch for field '{}': attempting to add INTEGER value to {:?} field. Expected I64 or U64 field in schema.",
                field_name, field_type
            ))
        }
    }
}

/// Safely add an unsigned value to document, converting based on schema field type
fn add_unsigned_value_safely(document: &mut TantivyDocument, schema: &Schema, field: Field, uint_val: u64) {
    let field_entry = schema.get_field_entry(field);
    let field_type = field_entry.field_type();

    match field_type {
        tantivy::schema::FieldType::I64(_) => {
            // Schema expects signed, but we have unsigned - convert safely
            if uint_val > i64::MAX as u64 {
                // Value too large for signed - use MAX as fallback
                document.add_i64(field, i64::MAX);
            } else {
                document.add_i64(field, uint_val as i64);
            }
        },
        tantivy::schema::FieldType::U64(_) => {
            // Schema expects unsigned - direct assignment
            document.add_u64(field, uint_val);
        },
        _ => {
            // For other field types, try to add as unsigned (fallback)
            document.add_u64(field, uint_val);
        }
    }
}

/// Safely add a date value to document, validating schema field type and date range
fn add_date_value_safely(document: &mut TantivyDocument, schema: &Schema, field: Field, field_name: &str, date_nanos: i64) -> Result<(), String> {
    let field_entry = schema.get_field_entry(field);
    let field_type = field_entry.field_type();

    match field_type {
        tantivy::schema::FieldType::Date(_) => {
            // Schema expects date - validate range first
            // Tantivy DateTime supports approximately years 1677-2262
            // Safe range in nanoseconds:
            // Year 1677: approximately -9,223,000,000,000,000,000 ns
            // Year 2262: approximately 9,223,000,000,000,000,000 ns
            const MIN_SAFE_NANOS: i64 = -9_223_000_000_000_000_000;
            const MAX_SAFE_NANOS: i64 = 9_223_000_000_000_000_000;

            if date_nanos < MIN_SAFE_NANOS || date_nanos > MAX_SAFE_NANOS {
                return Err(format!(
                    "Date value out of range for field '{}': {} nanoseconds. Tantivy DateTime supports approximately years 1677-2262 (range: {} to {} nanoseconds).",
                    field_name, date_nanos, MIN_SAFE_NANOS, MAX_SAFE_NANOS
                ));
            }

            // Convert nanoseconds to DateTime for microsecond precision
            let date_time = DateTime::from_timestamp_nanos(date_nanos);
            document.add_date(field, date_time);
            Ok(())
        },
        _ => {
            // Schema field type doesn't match - return error
            Err(format!(
                "Type mismatch for field '{}': attempting to add DATE value to {:?} field. Expected Date field in schema.",
                field_name, field_type
            ))
        }
    }
}

/// Safely add a float value to document, validating schema field type
fn add_float_value_safely(document: &mut TantivyDocument, schema: &Schema, field: Field, field_name: &str, float_val: f64) -> Result<(), String> {
    let field_entry = schema.get_field_entry(field);
    let field_type = field_entry.field_type();

    match field_type {
        tantivy::schema::FieldType::F64(_) => {
            // Schema expects float - direct assignment
            document.add_f64(field, float_val);
            Ok(())
        },
        _ => {
            // Schema field type doesn't match - return error
            Err(format!(
                "Type mismatch for field '{}': attempting to add FLOAT value to {:?} field. Expected F64 field in schema.",
                field_name, field_type
            ))
        }
    }
}

/// Safely add a boolean value to document, validating schema field type
fn add_boolean_value_safely(document: &mut TantivyDocument, schema: &Schema, field: Field, field_name: &str, bool_val: bool) -> Result<(), String> {
    let field_entry = schema.get_field_entry(field);
    let field_type = field_entry.field_type();

    match field_type {
        tantivy::schema::FieldType::Bool(_) => {
            // Schema expects boolean - direct assignment
            document.add_bool(field, bool_val);
            Ok(())
        },
        _ => {
            // Schema field type doesn't match - return error
            Err(format!(
                "Type mismatch for field '{}': attempting to add BOOLEAN value to {:?} field. Expected Bool field in schema.",
                field_name, field_type
            ))
        }
    }
}

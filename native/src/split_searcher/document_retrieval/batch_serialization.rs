// batch_serialization.rs - Serialize documents to byte buffer for efficient JNI transfer
// This mirrors the format used by Java's BatchDocumentBuilder for zero-copy bulk retrieval

use tantivy::schema::{OwnedValue, Schema, TantivyDocument};
use std::collections::BTreeMap;

/// Magic number for batch protocol validation (same as Java: "TANT")
const MAGIC_NUMBER: u32 = 0x54414E54;

/// Field type codes (same as Java BatchDocument.FieldType)
const FIELD_TYPE_TEXT: u8 = 0;
const FIELD_TYPE_INTEGER: u8 = 1;
const FIELD_TYPE_FLOAT: u8 = 2;
const FIELD_TYPE_BOOLEAN: u8 = 3;
const FIELD_TYPE_DATE: u8 = 4;
const FIELD_TYPE_BYTES: u8 = 5;
const FIELD_TYPE_JSON: u8 = 6;
const FIELD_TYPE_IP_ADDR: u8 = 7;
const FIELD_TYPE_UNSIGNED: u8 = 8;
const FIELD_TYPE_FACET: u8 = 9;

/// Serialize multiple TantivyDocuments to a byte buffer using the batch protocol.
///
/// Binary Format:
/// [Header Magic: 4 bytes]
/// [Document 1 Data]
/// [Document 2 Data]
/// ...
/// [Document N Data]
/// [Offset 1: 4 bytes] // Offset to Document 1
/// ...
/// [Offset N: 4 bytes] // Offset to Document N
/// [Offset Table Position: 4 bytes]
/// [Document Count: 4 bytes]
/// [Footer Magic: 4 bytes]
pub fn serialize_documents_to_buffer(
    documents: &[(TantivyDocument, Schema)],
) -> Result<Vec<u8>, String> {
    serialize_documents_to_buffer_with_filter(documents, None)
}

/// Serialize documents to byte buffer with optional field filtering.
/// When field_filter is Some, only the specified fields are included in the output.
pub fn serialize_documents_to_buffer_with_filter(
    documents: &[(TantivyDocument, Schema)],
    field_filter: Option<&std::collections::HashSet<String>>,
) -> Result<Vec<u8>, String> {
    // Estimate buffer size (will grow if needed)
    let estimated_size = estimate_buffer_size(documents);
    let mut buffer = Vec::with_capacity(estimated_size);

    // Write header magic
    buffer.extend_from_slice(&MAGIC_NUMBER.to_ne_bytes());

    // Write documents and collect offsets
    let mut offsets = Vec::with_capacity(documents.len());
    for (doc, schema) in documents {
        offsets.push(buffer.len() as u32);
        serialize_document_with_filter(&mut buffer, doc, schema, field_filter)?;
    }

    // Write offset table
    let offset_table_start = buffer.len() as u32;
    for offset in &offsets {
        buffer.extend_from_slice(&offset.to_ne_bytes());
    }

    // Write footer
    buffer.extend_from_slice(&offset_table_start.to_ne_bytes());
    buffer.extend_from_slice(&(documents.len() as u32).to_ne_bytes());
    buffer.extend_from_slice(&MAGIC_NUMBER.to_ne_bytes());

    Ok(buffer)
}

/// Serialize a single TantivyDocument to the buffer (no filtering)
fn serialize_document(buffer: &mut Vec<u8>, doc: &TantivyDocument, schema: &Schema) -> Result<(), String> {
    serialize_document_with_filter(buffer, doc, schema, None)
}

/// Serialize a single TantivyDocument to the buffer with optional field filtering
fn serialize_document_with_filter(
    buffer: &mut Vec<u8>,
    doc: &TantivyDocument,
    schema: &Schema,
    field_filter: Option<&std::collections::HashSet<String>>,
) -> Result<(), String> {
    // Collect fields into a map grouped by field name
    let mut field_map: BTreeMap<String, Vec<(u8, OwnedValue)>> = BTreeMap::new();

    for (field, value) in doc.field_values() {
        let field_name = schema.get_field_name(field).to_string();

        // Skip fields not in the filter (if filter is specified)
        if let Some(filter) = field_filter {
            if !filter.contains(&field_name) {
                continue;
            }
        }

        // Convert CompactDocValue to OwnedValue
        let owned_value: OwnedValue = value.into();
        let field_type = get_field_type(&owned_value);
        field_map.entry(field_name).or_default().push((field_type, owned_value));
    }

    // Write field count
    if field_map.len() > 65535 {
        return Err(format!("Too many fields in document: {}", field_map.len()));
    }
    buffer.extend_from_slice(&(field_map.len() as u16).to_ne_bytes());

    // Write each field
    for (field_name, values) in field_map {
        serialize_field(buffer, &field_name, &values)?;
    }

    Ok(())
}

/// Get the field type code for a value
fn get_field_type(value: &OwnedValue) -> u8 {
    match value {
        OwnedValue::Str(_) => FIELD_TYPE_TEXT,
        OwnedValue::I64(_) => FIELD_TYPE_INTEGER,
        OwnedValue::U64(_) => FIELD_TYPE_UNSIGNED,
        OwnedValue::F64(_) => FIELD_TYPE_FLOAT,
        OwnedValue::Bool(_) => FIELD_TYPE_BOOLEAN,
        OwnedValue::Date(_) => FIELD_TYPE_DATE,
        OwnedValue::Bytes(_) => FIELD_TYPE_BYTES,
        OwnedValue::Object(_) => FIELD_TYPE_JSON,
        OwnedValue::IpAddr(_) => FIELD_TYPE_IP_ADDR,
        OwnedValue::Facet(_) => FIELD_TYPE_FACET,
        OwnedValue::Array(_) => FIELD_TYPE_JSON, // Serialize arrays as JSON
        OwnedValue::PreTokStr(_) => FIELD_TYPE_TEXT, // Pre-tokenized strings as text
        OwnedValue::Null => FIELD_TYPE_TEXT, // Null as empty text
    }
}

/// Serialize a field with all its values
fn serialize_field(buffer: &mut Vec<u8>, field_name: &str, values: &[(u8, OwnedValue)]) -> Result<(), String> {
    // Write field name
    let name_bytes = field_name.as_bytes();
    if name_bytes.len() > 65535 {
        return Err(format!("Field name too long: {}", field_name));
    }
    buffer.extend_from_slice(&(name_bytes.len() as u16).to_ne_bytes());
    buffer.extend_from_slice(name_bytes);

    // Write field type (use first value's type)
    let field_type = values.first().map(|(t, _)| *t).unwrap_or(FIELD_TYPE_TEXT);
    buffer.push(field_type);

    // Write value count
    if values.len() > 65535 {
        return Err(format!("Too many values for field {}: {}", field_name, values.len()));
    }
    buffer.extend_from_slice(&(values.len() as u16).to_ne_bytes());

    // Write values
    for (_, value) in values {
        serialize_field_value(buffer, value)?;
    }

    Ok(())
}

/// Serialize a single field value
fn serialize_field_value(buffer: &mut Vec<u8>, value: &OwnedValue) -> Result<(), String> {
    match value {
        OwnedValue::Str(s) => {
            let bytes = s.as_bytes();
            buffer.extend_from_slice(&(bytes.len() as u32).to_ne_bytes());
            buffer.extend_from_slice(bytes);
        }
        OwnedValue::I64(v) => {
            buffer.extend_from_slice(&v.to_ne_bytes());
        }
        OwnedValue::U64(v) => {
            buffer.extend_from_slice(&v.to_ne_bytes());
        }
        OwnedValue::F64(v) => {
            buffer.extend_from_slice(&v.to_ne_bytes());
        }
        OwnedValue::Bool(v) => {
            buffer.push(if *v { 1 } else { 0 });
        }
        OwnedValue::Date(dt) => {
            // Convert DateTime to nanoseconds since epoch
            let nanos = dt.into_timestamp_nanos();
            buffer.extend_from_slice(&nanos.to_ne_bytes());
        }
        OwnedValue::Bytes(bytes) => {
            buffer.extend_from_slice(&(bytes.len() as u32).to_ne_bytes());
            buffer.extend_from_slice(bytes);
        }
        OwnedValue::Object(obj) => {
            // Serialize object as JSON string
            let json_str = serialize_object_to_json(obj);
            let bytes = json_str.as_bytes();
            buffer.extend_from_slice(&(bytes.len() as u32).to_ne_bytes());
            buffer.extend_from_slice(bytes);
        }
        OwnedValue::Array(arr) => {
            // Serialize array as JSON string
            let json_str = serialize_array_to_json(arr);
            let bytes = json_str.as_bytes();
            buffer.extend_from_slice(&(bytes.len() as u32).to_ne_bytes());
            buffer.extend_from_slice(bytes);
        }
        OwnedValue::IpAddr(ip) => {
            // Serialize IP address as string
            let ip_str = ip.to_string();
            let bytes = ip_str.as_bytes();
            buffer.extend_from_slice(&(bytes.len() as u32).to_ne_bytes());
            buffer.extend_from_slice(bytes);
        }
        OwnedValue::Facet(facet) => {
            // Serialize facet as path string
            let facet_str = facet.to_path_string();
            let bytes = facet_str.as_bytes();
            buffer.extend_from_slice(&(bytes.len() as u32).to_ne_bytes());
            buffer.extend_from_slice(bytes);
        }
        OwnedValue::PreTokStr(pts) => {
            // Serialize pre-tokenized string as regular text
            let text = &pts.text;
            let bytes = text.as_bytes();
            buffer.extend_from_slice(&(bytes.len() as u32).to_ne_bytes());
            buffer.extend_from_slice(bytes);
        }
        OwnedValue::Null => {
            // Serialize null as empty string
            buffer.extend_from_slice(&0u32.to_ne_bytes());
        }
    }

    Ok(())
}

/// Serialize an object to JSON string
fn serialize_object_to_json(obj: &[(String, OwnedValue)]) -> String {
    let json_value = owned_value_to_json(&OwnedValue::Object(obj.to_vec()));
    serde_json::to_string(&json_value).unwrap_or_else(|_| "{}".to_string())
}

/// Serialize an array to JSON string
fn serialize_array_to_json(arr: &[OwnedValue]) -> String {
    let json_value = owned_value_to_json(&OwnedValue::Array(arr.to_vec()));
    serde_json::to_string(&json_value).unwrap_or_else(|_| "[]".to_string())
}

/// Convert OwnedValue to serde_json::Value for JSON serialization
fn owned_value_to_json(value: &OwnedValue) -> serde_json::Value {
    match value {
        OwnedValue::Str(s) => serde_json::Value::String(s.clone()),
        OwnedValue::I64(v) => serde_json::Value::Number(serde_json::Number::from(*v)),
        OwnedValue::U64(v) => serde_json::Value::Number(serde_json::Number::from(*v)),
        OwnedValue::F64(v) => {
            serde_json::Number::from_f64(*v)
                .map(serde_json::Value::Number)
                .unwrap_or(serde_json::Value::Null)
        }
        OwnedValue::Bool(v) => serde_json::Value::Bool(*v),
        OwnedValue::Date(dt) => {
            // Convert DateTime to ISO8601 format using timestamp
            let nanos = dt.into_timestamp_nanos();
            let secs = nanos / 1_000_000_000;
            let nsec = (nanos % 1_000_000_000) as u32;
            // Format as RFC3339 timestamp
            let datetime = chrono::DateTime::from_timestamp(secs, nsec)
                .map(|d| d.format("%Y-%m-%dT%H:%M:%S%.9fZ").to_string())
                .unwrap_or_else(|| nanos.to_string());
            serde_json::Value::String(datetime)
        }
        OwnedValue::Bytes(bytes) => {
            // Encode bytes as base64
            use base64::{engine::general_purpose::STANDARD, Engine};
            serde_json::Value::String(STANDARD.encode(bytes))
        }
        OwnedValue::Object(obj) => {
            let map: serde_json::Map<String, serde_json::Value> = obj
                .iter()
                .map(|(k, v)| (k.clone(), owned_value_to_json(v)))
                .collect();
            serde_json::Value::Object(map)
        }
        OwnedValue::Array(arr) => {
            let values: Vec<serde_json::Value> = arr.iter().map(owned_value_to_json).collect();
            serde_json::Value::Array(values)
        }
        OwnedValue::IpAddr(ip) => serde_json::Value::String(ip.to_string()),
        OwnedValue::Facet(facet) => serde_json::Value::String(facet.to_path_string()),
        OwnedValue::PreTokStr(pts) => serde_json::Value::String(pts.text.clone()),
        OwnedValue::Null => serde_json::Value::Null,
    }
}

/// Estimate buffer size needed for serialization
fn estimate_buffer_size(documents: &[(TantivyDocument, Schema)]) -> usize {
    // Start with header + footer overhead
    let mut size = 4 + 12; // header magic + footer (offset_table_pos + doc_count + footer_magic)

    // Add space for offset table
    size += documents.len() * 4;

    // Estimate per-document size (rough average of 500 bytes per doc)
    size += documents.len() * 500;

    size
}

#[cfg(test)]
mod tests {
    use super::*;
    use tantivy::schema::{SchemaBuilder, TEXT};

    #[test]
    fn test_serialize_empty_documents() {
        let docs: Vec<(TantivyDocument, Schema)> = vec![];
        let result = serialize_documents_to_buffer(&docs);
        assert!(result.is_ok());
        let buffer = result.unwrap();

        // Check header and footer magic
        let header_magic = u32::from_ne_bytes([buffer[0], buffer[1], buffer[2], buffer[3]]);
        assert_eq!(header_magic, MAGIC_NUMBER);

        let footer_magic = u32::from_ne_bytes([
            buffer[buffer.len() - 4],
            buffer[buffer.len() - 3],
            buffer[buffer.len() - 2],
            buffer[buffer.len() - 1],
        ]);
        assert_eq!(footer_magic, MAGIC_NUMBER);

        // Check document count
        let doc_count = u32::from_ne_bytes([
            buffer[buffer.len() - 8],
            buffer[buffer.len() - 7],
            buffer[buffer.len() - 6],
            buffer[buffer.len() - 5],
        ]);
        assert_eq!(doc_count, 0);
    }

    #[test]
    fn test_serialize_single_document() {
        let mut schema_builder = SchemaBuilder::new();
        let title_field = schema_builder.add_text_field("title", TEXT);
        let schema = schema_builder.build();

        let mut doc = TantivyDocument::default();
        doc.add_text(title_field, "Hello World");

        let docs = vec![(doc, schema.clone())];
        let result = serialize_documents_to_buffer(&docs);
        assert!(result.is_ok());
        let buffer = result.unwrap();

        // Check document count
        let doc_count = u32::from_ne_bytes([
            buffer[buffer.len() - 8],
            buffer[buffer.len() - 7],
            buffer[buffer.len() - 6],
            buffer[buffer.len() - 5],
        ]);
        assert_eq!(doc_count, 1);
    }
}

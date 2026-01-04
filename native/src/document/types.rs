// types.rs - Document type definitions
// Extracted from document.rs during refactoring

use std::collections::BTreeMap;
use std::net::IpAddr;

use tantivy::schema::{Document, OwnedValue, Schema, TantivyDocument};
use tantivy::DateTime;

use crate::debug_println;

/// Unified document type that can handle both creation and retrieval
#[derive(Clone)]
pub enum DocumentWrapper {
    Builder(DocumentBuilder),
    Retrieved(RetrievedDocument),
}

/// Intermediate document structure that stores field values by name
#[derive(Clone)]
pub struct DocumentBuilder {
    pub(crate) field_values: BTreeMap<String, Vec<OwnedValue>>,
}

/// Document retrieved from search results with field values
#[derive(Debug, Clone)]
pub struct RetrievedDocument {
    pub field_values: BTreeMap<String, Vec<OwnedValue>>,
}

impl DocumentBuilder {
    pub fn new() -> Self {
        Self {
            field_values: BTreeMap::new(),
        }
    }

    pub fn add_text(&mut self, field_name: String, text: String) {
        self.field_values
            .entry(field_name)
            .or_default()
            .push(OwnedValue::Str(text));
    }

    pub fn add_integer(&mut self, field_name: String, value: i64) {
        self.field_values
            .entry(field_name)
            .or_default()
            .push(OwnedValue::I64(value));
    }

    pub fn add_float(&mut self, field_name: String, value: f64) {
        self.field_values
            .entry(field_name)
            .or_default()
            .push(OwnedValue::F64(value));
    }

    pub fn add_unsigned(&mut self, field_name: String, value: u64) {
        self.field_values
            .entry(field_name)
            .or_default()
            .push(OwnedValue::U64(value));
    }

    pub fn add_boolean(&mut self, field_name: String, value: bool) {
        self.field_values
            .entry(field_name)
            .or_default()
            .push(OwnedValue::Bool(value));
    }

    pub fn add_date(&mut self, field_name: String, value: DateTime) {
        self.field_values
            .entry(field_name)
            .or_default()
            .push(OwnedValue::Date(value));
    }

    pub fn add_ip_addr(&mut self, field_name: String, value: IpAddr) {
        // Convert IpAddr to Ipv6Addr (Tantivy stores all IPs as Ipv6)
        let ipv6_value = match value {
            IpAddr::V4(ipv4) => ipv4.to_ipv6_mapped(),
            IpAddr::V6(ipv6) => ipv6,
        };

        self.field_values
            .entry(field_name)
            .or_default()
            .push(OwnedValue::IpAddr(ipv6_value));
    }

    pub fn add_json(&mut self, field_name: String, json_object: BTreeMap<String, OwnedValue>) {
        // Convert BTreeMap to Vec<(String, OwnedValue)> for tantivy
        let json_vec: Vec<(String, OwnedValue)> = json_object.into_iter().collect();
        self.field_values
            .entry(field_name)
            .or_default()
            .push(OwnedValue::Object(json_vec));
    }

    pub fn build(self, schema: &Schema) -> Result<TantivyDocument, String> {
        let mut doc = TantivyDocument::new();

        for (field_key, values) in self.field_values {
            // Check if this is a field-ID based key
            let field = if field_key.starts_with("__field_id_") && field_key.ends_with("__") {
                // Extract field ID from the key
                let id_str = &field_key[11..field_key.len() - 2]; // Remove prefix and suffix
                let field_id: u32 = id_str
                    .parse()
                    .map_err(|_| format!("Invalid field ID in key: {}", field_key))?;
                tantivy::schema::Field::from_field_id(field_id)
            } else {
                // Regular field name lookup
                schema
                    .get_field(&field_key)
                    .map_err(|_| format!("Field '{}' not found in schema", field_key))?
            };

            for value in values {
                match value {
                    OwnedValue::Str(text) => doc.add_text(field, &text),
                    OwnedValue::I64(num) => doc.add_i64(field, num),
                    OwnedValue::F64(num) => doc.add_f64(field, num),
                    OwnedValue::U64(num) => doc.add_u64(field, num),
                    OwnedValue::Bool(b) => doc.add_bool(field, b),
                    OwnedValue::Date(dt) => doc.add_date(field, dt),
                    OwnedValue::IpAddr(ipv6) => doc.add_ip_addr(field, ipv6),
                    OwnedValue::Object(obj) => {
                        // Convert Vec<(String, OwnedValue)> to BTreeMap for tantivy
                        let json_map: BTreeMap<String, OwnedValue> = obj.into_iter().collect();
                        doc.add_object(field, json_map);
                    }
                    _ => return Err(format!("Unsupported value type for field '{}'", field_key)),
                }
            }
        }

        Ok(doc)
    }
}

impl Default for DocumentBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl DocumentWrapper {
    pub fn get_field_values(&self, field_name: &str) -> Option<&Vec<OwnedValue>> {
        match self {
            DocumentWrapper::Builder(builder) => builder.field_values.get(field_name),
            DocumentWrapper::Retrieved(retrieved) => retrieved.field_values.get(field_name),
        }
    }

    pub fn get_all_fields(&self) -> &BTreeMap<String, Vec<OwnedValue>> {
        match self {
            DocumentWrapper::Builder(builder) => &builder.field_values,
            DocumentWrapper::Retrieved(retrieved) => &retrieved.field_values,
        }
    }

    pub fn is_empty(&self) -> bool {
        match self {
            DocumentWrapper::Builder(builder) => builder.field_values.is_empty(),
            DocumentWrapper::Retrieved(retrieved) => retrieved.field_values.is_empty(),
        }
    }

    pub fn num_fields(&self) -> usize {
        match self {
            DocumentWrapper::Builder(builder) => builder.field_values.len(),
            DocumentWrapper::Retrieved(retrieved) => retrieved.field_values.len(),
        }
    }
}

impl RetrievedDocument {
    pub fn new(_document: TantivyDocument) -> Self {
        // For now, create an empty document - we'll need schema to convert properly
        // This is a simplified implementation that will be enhanced later
        Self {
            field_values: BTreeMap::new(),
        }
    }

    pub fn from_json(json_str: &str) -> anyhow::Result<Self> {
        let json_value: serde_json::Value = serde_json::from_str(json_str)?;
        let mut field_values = BTreeMap::new();

        if let serde_json::Value::Object(obj) = json_value {
            for (field_name, value) in obj {
                let owned_value = match value {
                    serde_json::Value::String(s) => OwnedValue::Str(s),
                    serde_json::Value::Number(n) => {
                        if let Some(i) = n.as_i64() {
                            OwnedValue::I64(i)
                        } else if let Some(f) = n.as_f64() {
                            OwnedValue::F64(f)
                        } else {
                            continue; // Skip unsupported number format
                        }
                    }
                    serde_json::Value::Bool(b) => OwnedValue::Bool(b),
                    _ => continue, // Skip unsupported value types for now
                };
                field_values.insert(field_name, vec![owned_value]);
            }
        }

        Ok(Self { field_values })
    }

    pub fn new_with_schema(document: TantivyDocument, schema: &tantivy::schema::Schema) -> Self {
        // Following the Python implementation: doc.to_named_doc(schema)
        let named_doc = document.to_named_doc(schema);
        Self {
            field_values: named_doc.0,
        }
    }

    #[allow(dead_code)]
    pub fn get_field_values(&self, field_name: &str) -> Option<&Vec<OwnedValue>> {
        self.field_values.get(field_name)
    }

    #[allow(dead_code)]
    pub fn get_all_fields(&self) -> &BTreeMap<String, Vec<OwnedValue>> {
        &self.field_values
    }
}

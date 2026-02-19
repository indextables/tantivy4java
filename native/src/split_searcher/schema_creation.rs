// schema_creation.rs - Schema creation from field mappings JSON
// Extracted from mod.rs during refactoring

use crate::debug_println;

/// Create Tantivy schema from field mappings JSON array
/// This handles the field mappings array format used by QuickwitSplit
pub fn create_schema_from_doc_mapping(doc_mapping_json: &str) -> anyhow::Result<tantivy::schema::Schema> {
    debug_println!("RUST DEBUG: Creating schema from field mappings JSON");
    debug_println!("RUST DEBUG: 🔍 RAW FIELD MAPPINGS JSON ({} chars): '{}'", doc_mapping_json.len(), doc_mapping_json);

    // Parse the field mappings JSON array
    #[derive(serde::Deserialize)]
    struct FieldMapping {
        name: String,
        #[serde(rename = "type")]
        field_type: String,
        stored: Option<bool>,
        indexed: Option<bool>,
        fast: Option<bool>,
        tokenizer: Option<String>,
        fast_tokenizer: Option<String>,
        expand_dots: Option<bool>,
    }

    let field_mappings: Vec<FieldMapping> = serde_json::from_str(doc_mapping_json)
        .map_err(|e| anyhow::anyhow!("Failed to parse field mappings JSON: {}", e))?;

    debug_println!("RUST DEBUG: Parsed {} field mappings", field_mappings.len());

    // Create Tantivy schema builder
    let mut schema_builder = tantivy::schema::Schema::builder();

    // Add each field to the schema
    for field_mapping in field_mappings {
        let stored = field_mapping.stored.unwrap_or(false);
        let indexed = field_mapping.indexed.unwrap_or(false);
        let fast = field_mapping.fast.unwrap_or(false);
        let tokenizer = field_mapping.tokenizer.as_deref().unwrap_or("default");

        debug_println!("RUST DEBUG: Adding field '{}' type '{}' stored={} indexed={} fast={} tokenizer='{}'",
            field_mapping.name, field_mapping.field_type, stored, indexed, fast, tokenizer);

        match field_mapping.field_type.as_str() {
            "text" => {
                let mut text_options = tantivy::schema::TextOptions::default();
                if stored {
                    text_options = text_options.set_stored();
                }
                if indexed {
                    let text_indexing = tantivy::schema::TextFieldIndexing::default()
                        .set_tokenizer(tokenizer)
                        .set_index_option(tantivy::schema::IndexRecordOption::WithFreqsAndPositions);
                    text_options = text_options.set_indexing_options(text_indexing);
                }
                if fast {
                    text_options = text_options.set_fast(Some(tokenizer));
                }
                schema_builder.add_text_field(&field_mapping.name, text_options);
            },
            "i64" => {
                let mut int_options = tantivy::schema::NumericOptions::default();
                if stored {
                    int_options = int_options.set_stored();
                }
                if indexed {
                    int_options = int_options.set_indexed();
                }
                if fast {
                    int_options = int_options.set_fast();
                }
                schema_builder.add_i64_field(&field_mapping.name, int_options);
            },
            "f64" => {
                let mut float_options = tantivy::schema::NumericOptions::default();
                if stored {
                    float_options = float_options.set_stored();
                }
                if indexed {
                    float_options = float_options.set_indexed();
                }
                if fast {
                    float_options = float_options.set_fast();
                }
                schema_builder.add_f64_field(&field_mapping.name, float_options);
            },
            "bool" => {
                let mut bool_options = tantivy::schema::NumericOptions::default();
                if stored {
                    bool_options = bool_options.set_stored();
                }
                if indexed {
                    bool_options = bool_options.set_indexed();
                }
                if fast {
                    bool_options = bool_options.set_fast();
                }
                schema_builder.add_bool_field(&field_mapping.name, bool_options);
            },
            "u64" => {
                let mut uint_options = tantivy::schema::NumericOptions::default();
                if stored {
                    uint_options = uint_options.set_stored();
                }
                if indexed {
                    uint_options = uint_options.set_indexed();
                }
                if fast {
                    uint_options = uint_options.set_fast();
                }
                schema_builder.add_u64_field(&field_mapping.name, uint_options);
            },
            "datetime" | "date" => {
                let mut date_options = tantivy::schema::DateOptions::default();
                if stored {
                    date_options = date_options.set_stored();
                }
                if indexed {
                    date_options = date_options.set_indexed();
                }
                if fast {
                    date_options = date_options.set_fast();
                }
                schema_builder.add_date_field(&field_mapping.name, date_options);
            },
            "bytes" => {
                let mut bytes_options = tantivy::schema::BytesOptions::default();
                if stored {
                    bytes_options = bytes_options.set_stored();
                }
                if indexed {
                    bytes_options = bytes_options.set_indexed();
                }
                if fast {
                    bytes_options = bytes_options.set_fast();
                }
                schema_builder.add_bytes_field(&field_mapping.name, bytes_options);
            },
            "ip" | "ip_addr" => {
                let mut ip_options = tantivy::schema::IpAddrOptions::default();
                if stored {
                    ip_options = ip_options.set_stored();
                }
                if indexed {
                    ip_options = ip_options.set_indexed();
                }
                if fast {
                    ip_options = ip_options.set_fast();
                }
                schema_builder.add_ip_addr_field(&field_mapping.name, ip_options);
            },
            "object" => {
                debug_println!("RUST DEBUG: 🔧 JSON FIELD RECONSTRUCTION: field '{}', stored={}, indexed={}, fast={}, expand_dots={:?}, tokenizer={:?}, fast_tokenizer={:?}",
                    field_mapping.name, stored, indexed, fast, field_mapping.expand_dots, field_mapping.tokenizer, field_mapping.fast_tokenizer);

                let mut json_options = tantivy::schema::JsonObjectOptions::default();

                // Set stored attribute
                if stored {
                    json_options = json_options.set_stored();
                }

                // Set indexing options with tokenizer
                if indexed {
                    let tokenizer_name = field_mapping.tokenizer.as_deref().unwrap_or("default");
                    let text_indexing = tantivy::schema::TextFieldIndexing::default()
                        .set_tokenizer(tokenizer_name)
                        .set_index_option(tantivy::schema::IndexRecordOption::WithFreqsAndPositions);
                    json_options = json_options.set_indexing_options(text_indexing);
                }

                // Set fast field options with optional tokenizer
                if fast {
                    let fast_tokenizer_name = field_mapping.fast_tokenizer.as_deref();
                    json_options = json_options.set_fast(fast_tokenizer_name);
                }

                // Set expand_dots if enabled
                if field_mapping.expand_dots.unwrap_or(false) {
                    json_options = json_options.set_expand_dots_enabled();
                }

                schema_builder.add_json_field(&field_mapping.name, json_options);
                debug_println!("RUST DEBUG: 🔧 JSON FIELD RECONSTRUCTION: Successfully added JSON field '{}'", field_mapping.name);
            },
            other => {
                debug_println!("RUST DEBUG: ⚠️ Unsupported field type '{}' for field '{}', treating as text", other, field_mapping.name);
                // Fallback to text field for unknown types
                let mut text_options = tantivy::schema::TextOptions::default();
                if stored {
                    text_options = text_options.set_stored();
                }
                if indexed {
                    text_options = text_options.set_indexing_options(
                        tantivy::schema::TextFieldIndexing::default()
                            .set_tokenizer("default")
                            .set_index_option(tantivy::schema::IndexRecordOption::WithFreqsAndPositions)
                    );
                }
                schema_builder.add_text_field(&field_mapping.name, text_options);
            }
        }
    }

    let schema = schema_builder.build();
    debug_println!("RUST DEBUG: Successfully created Tantivy schema from field mappings with {} fields", schema.num_fields());
    Ok(schema)
}

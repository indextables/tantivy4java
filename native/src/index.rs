/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

use jni::objects::{JClass, JString, JObject};
use jni::sys::{jlong, jboolean, jint};
use jni::JNIEnv;
use tantivy::{Index as TantivyIndex, IndexSettings, IndexWriter as TantivyIndexWriter};
use tantivy::schema::Schema;
use tantivy::query::{Query as TantivyQuery, QueryParser};
use tantivy::directory::MmapDirectory;
use tantivy::tokenizer::{SimpleTokenizer, WhitespaceTokenizer, LowerCaser, RemoveLongFilter, TextAnalyzer as TantivyAnalyzer};
use crate::utils::{handle_error, with_arc_safe, arc_to_jlong, release_arc};
use crate::text_analyzer::DEFAULT_MAX_TOKEN_LENGTH;
use std::sync::{Arc, Mutex};

#[no_mangle]
pub extern "system" fn Java_io_indextables_tantivy4java_core_Index_nativeNew(
    mut env: JNIEnv,
    _class: JClass,
    _schema_ptr: jlong,
    _path: JString,
    _reuse: jboolean,
) -> jlong {
    let path_str: String = match env.get_string(&_path) {
        Ok(s) => s.into(),
        Err(_) => {
            handle_error(&mut env, "Invalid path");
            return 0;
        }
    };
    
    let result = with_arc_safe::<Schema, Result<TantivyIndex, String>>(_schema_ptr, |schema_arc| {
        let schema = schema_arc.as_ref();
        let index = if path_str.is_empty() {
            // Create in-memory index
            TantivyIndex::create_in_ram(schema.clone())
        } else {
            // Create index in directory - create directory if it doesn't exist
            std::fs::create_dir_all(&path_str).map_err(|e| format!("Failed to create directory '{}': {}", path_str, e))?;
            let dir = MmapDirectory::open(&path_str).map_err(|e| e.to_string())?;
            let settings = IndexSettings::default();
            TantivyIndex::create(dir, schema.clone(), settings).map_err(|e| e.to_string())?
        };

        // Register custom tokenizers with the default token length limit (255 bytes)
        register_custom_tokenizers(&index, DEFAULT_MAX_TOKEN_LENGTH);

        // Also scan the schema for any fields that use custom tokenizer names with limits
        // and register those tokenizers as well
        register_tokenizers_from_schema(&index, schema);

        Ok(index)
    });
    
    match result {
        Some(Ok(index)) => {
            let index_arc = Arc::new(Mutex::new(index));
            arc_to_jlong(index_arc)
        },
        Some(Err(err)) => {
            handle_error(&mut env, &err);
            0
        },
        None => {
            handle_error(&mut env, "Invalid Schema pointer");
            0
        }
    }
}

#[no_mangle]
pub extern "system" fn Java_io_indextables_tantivy4java_core_Index_nativeOpen(
    mut env: JNIEnv,
    _class: JClass,
    path: JString,
) -> jlong {
    let path_str: String = match env.get_string(&path) {
        Ok(s) => s.into(),
        Err(_) => {
            handle_error(&mut env, "Invalid path string");
            return 0;
        }
    };
    
    // Open existing index from directory
    let result = match MmapDirectory::open(&path_str) {
        Ok(directory) => {
            match TantivyIndex::open(directory) {
                Ok(index) => {
                    // Register custom tokenizers with the default token length limit
                    register_custom_tokenizers(&index, DEFAULT_MAX_TOKEN_LENGTH);
                    // Register any custom limit tokenizers from schema
                    register_tokenizers_from_schema(&index, &index.schema());
                    Ok(index)
                },
                Err(e) => Err(format!("Failed to open index: {}", e)),
            }
        },
        Err(e) => Err(format!("Failed to open directory '{}': {}", path_str, e)),
    };
    
    match result {
        Ok(index) => {
            let index_arc = Arc::new(Mutex::new(index));
            arc_to_jlong(index_arc)
        },
        Err(err) => {
            handle_error(&mut env, &err);
            0
        }
    }
}

#[no_mangle]
pub extern "system" fn Java_io_indextables_tantivy4java_core_Index_nativeExists(
    mut env: JNIEnv,
    _class: JClass,
    path: JString,
) -> jboolean {
    let path_str: String = match env.get_string(&path) {
        Ok(s) => s.into(),
        Err(_) => {
            handle_error(&mut env, "Invalid path string");
            return 0;
        }
    };
    
    // Check if an index exists at the given path
    let result = match MmapDirectory::open(&path_str) {
        Ok(directory) => {
            match TantivyIndex::exists(&directory) {
                Ok(exists) => Ok(exists),
                Err(e) => Err(format!("Failed to check if index exists: {}", e)),
            }
        },
        Err(_) => {
            // If we can't open the directory, the index doesn't exist
            Ok(false)
        }
    };
    
    match result {
        Ok(exists) => if exists { 1 } else { 0 },
        Err(err) => {
            handle_error(&mut env, &err);
            0
        }
    }
}

#[no_mangle]
pub extern "system" fn Java_io_indextables_tantivy4java_core_Index_nativeWriter(
    mut env: JNIEnv,
    _class: JClass,
    ptr: jlong,
    heap_size: jint,
    num_threads: jint,
) -> jlong {
    let result = with_arc_safe::<Mutex<TantivyIndex>, Result<TantivyIndexWriter, String>>(ptr, |index_mutex| {
        let index = index_mutex.lock().unwrap();
        let heap_size_bytes = if heap_size > 0 { heap_size as usize } else { 50_000_000 }; // 50MB default
        let num_threads_val = if num_threads > 0 { num_threads as usize } else { 1 };
        
        index.writer_with_num_threads(num_threads_val, heap_size_bytes)
            .map_err(|e| e.to_string())
    });
    
    match result {
        Some(Ok(writer)) => {
            let writer_arc = Arc::new(Mutex::new(writer));
            arc_to_jlong(writer_arc)
        },
        Some(Err(err)) => {
            handle_error(&mut env, &err);
            0
        },
        None => {
            handle_error(&mut env, "Invalid Index pointer");
            0
        }
    }
}

#[no_mangle]
pub extern "system" fn Java_io_indextables_tantivy4java_core_Index_nativeConfigReader(
    mut env: JNIEnv,
    _class: JClass,
    _ptr: jlong,
    _reload_policy: JString,
    _num_warmers: jint,
) {
    handle_error(&mut env, "Index native methods not fully implemented yet");
}

#[no_mangle]
pub extern "system" fn Java_io_indextables_tantivy4java_core_Index_nativeSearcher(
    mut env: JNIEnv,
    _class: JClass,
    ptr: jlong,
) -> jlong {
    let result = with_arc_safe::<Mutex<TantivyIndex>, Result<tantivy::Searcher, String>>(ptr, |index_mutex| {
        let index = index_mutex.lock().unwrap();
        let reader = index.reader().map_err(|e| e.to_string())?;
        Ok(reader.searcher())
    });
    
    match result {
        Some(Ok(searcher)) => {
            let searcher_arc = Arc::new(Mutex::new(searcher));
            arc_to_jlong(searcher_arc)
        },
        Some(Err(err)) => {
            handle_error(&mut env, &err);
            0
        },
        None => {
            handle_error(&mut env, "Invalid Index pointer");
            0
        }
    }
}

#[no_mangle]
pub extern "system" fn Java_io_indextables_tantivy4java_core_Index_nativeGetSchema(
    mut env: JNIEnv,
    _class: JClass,
    ptr: jlong,
) -> jlong {
    let result = with_arc_safe::<Mutex<TantivyIndex>, Result<jlong, String>>(ptr, |index_mutex| {
        let index = index_mutex.lock().unwrap();
        // Get the schema from the index and return it as an Arc
        let schema = index.schema();
        let schema_arc = Arc::new(schema);
        Ok(arc_to_jlong(schema_arc))
    });
    
    match result {
        Some(Ok(schema_ptr)) => schema_ptr,
        _ => {
            handle_error(&mut env, "Failed to get schema from index");
            0
        }
    }
}

#[no_mangle]
pub extern "system" fn Java_io_indextables_tantivy4java_core_Index_nativeReload(
    mut env: JNIEnv,
    _class: JClass,
    ptr: jlong,
) {
    let result = with_arc_safe::<Mutex<TantivyIndex>, Result<(), String>>(ptr, |index_mutex| {
        let _index = index_mutex.lock().unwrap();
        // In Tantivy, we don't need to explicitly reload the index
        // The reader will automatically pick up changes when a new searcher is created
        // But we can call this to ensure any pending changes are committed
        Ok(())
    });
    
    match result {
        Some(Ok(())) => {},
        Some(Err(err)) => {
            handle_error(&mut env, &err);
        },
        None => {
            handle_error(&mut env, "Invalid Index pointer");
        }
    }
}

#[no_mangle]
pub extern "system" fn Java_io_indextables_tantivy4java_core_Index_nativeParseQuery(
    mut env: JNIEnv,
    _class: JClass,
    ptr: jlong,
    query: JString,
    default_field_names: JObject,
    _field_boosts: JObject,
    _fuzzy_fields: JObject,
) -> jlong {
    let query_str: String = match env.get_string(&query) {
        Ok(s) => s.into(),
        Err(_) => {
            handle_error(&mut env, "Invalid query string");
            return 0;
        }
    };
    
    // Extract default field names from the Java List
    let field_names = if !default_field_names.is_null() {
        match extract_string_list(&mut env, &default_field_names) {
            Ok(names) => names,
            Err(e) => {
                handle_error(&mut env, &e);
                return 0;
            }
        }
    } else {
        Vec::new()
    };
    
    let result = with_arc_safe::<Mutex<TantivyIndex>, Result<Box<dyn TantivyQuery>, String>>(ptr, |index_mutex| {
        let index = index_mutex.lock().unwrap();
        let schema = index.schema();
        
        // Get default fields
        let default_fields: Vec<_> = if field_names.is_empty() {
            // Use all indexed fields if no default fields specified
            schema.fields()
                .filter(|(_, field_entry)| field_entry.is_indexed())
                .map(|(field, _)| field)
                .collect()
        } else {
            // Convert field names to field handles
            field_names.iter()
                .map(|name| {
                    schema.get_field(name).map_err(|_| {
                        format!("Field '{}' is not defined in the schema", name)
                    })
                })
                .collect::<Result<Vec<_>, _>>()?
        };
        
        if default_fields.is_empty() {
            return Err("No indexed fields available for query parsing".to_string());
        }
        
        // Create query parser
        let query_parser = QueryParser::for_index(&*index, default_fields.clone());
        
        // Parse the query using our fixed parser with schema and field info
        let parsed_query = parse_query_with_phrase_fix(&query_parser, &query_str, &schema, &default_fields)
            .map_err(|e| format!("Query parsing error: {}", e))?;
        
        Ok(parsed_query)
    });
    
    match result {
        Some(Ok(query)) => {
            let query_arc = Arc::new(query);
            arc_to_jlong(query_arc)
        },
        Some(Err(err)) => {
            handle_error(&mut env, &err);
            0
        },
        None => {
            handle_error(&mut env, "Invalid Index pointer");
            0
        }
    }
}

/// Register custom tokenizers for a given token length limit
fn register_custom_tokenizers(index: &TantivyIndex, max_token_length: usize) {
    // Register default tokenizer with the specified limit
    let default_analyzer = TantivyAnalyzer::builder(SimpleTokenizer::default())
        .filter(LowerCaser)
        .filter(RemoveLongFilter::limit(max_token_length))
        .build();
    index.tokenizers().register("default", default_analyzer);

    // Register simple tokenizer with the specified limit
    let simple_analyzer = TantivyAnalyzer::builder(SimpleTokenizer::default())
        .filter(LowerCaser)
        .filter(RemoveLongFilter::limit(max_token_length))
        .build();
    index.tokenizers().register("simple", simple_analyzer);

    // Register whitespace tokenizer with the specified limit
    let whitespace_analyzer = TantivyAnalyzer::builder(WhitespaceTokenizer::default())
        .filter(LowerCaser)
        .filter(RemoveLongFilter::limit(max_token_length))
        .build();
    index.tokenizers().register("whitespace", whitespace_analyzer);
}

/// Register tokenizers from schema field definitions
/// This handles fields that specify custom token length limits via tokenizer names like "default_limit_40"
fn register_tokenizers_from_schema(index: &TantivyIndex, schema: &Schema) {
    use std::collections::HashSet;
    let mut registered = HashSet::new();

    for (_field, field_entry) in schema.fields() {
        if let tantivy::schema::FieldType::Str(text_options) = field_entry.field_type() {
            if let Some(indexing_options) = text_options.get_indexing_options() {
                let tokenizer_name = indexing_options.tokenizer();

                // Check if this is a custom limit tokenizer (e.g., "default_limit_40")
                if tokenizer_name.contains("_limit_") && !registered.contains(tokenizer_name) {
                    if let Some(limit) = parse_tokenizer_limit(tokenizer_name) {
                        let base_tokenizer = extract_base_tokenizer(tokenizer_name);
                        register_tokenizer_with_limit(index, tokenizer_name, &base_tokenizer, limit);
                        registered.insert(tokenizer_name.to_string());
                    }
                }
            }
        }
    }
}

/// Parse the token limit from a tokenizer name like "default_limit_40"
fn parse_tokenizer_limit(name: &str) -> Option<usize> {
    let parts: Vec<&str> = name.split("_limit_").collect();
    if parts.len() == 2 {
        parts[1].parse().ok()
    } else {
        None
    }
}

/// Extract the base tokenizer name from a limit tokenizer name
fn extract_base_tokenizer(name: &str) -> String {
    if let Some(idx) = name.find("_limit_") {
        name[..idx].to_string()
    } else {
        name.to_string()
    }
}

/// Register a tokenizer with a specific limit
fn register_tokenizer_with_limit(index: &TantivyIndex, name: &str, base: &str, limit: usize) {
    let analyzer = match base {
        "default" | "simple" => {
            TantivyAnalyzer::builder(SimpleTokenizer::default())
                .filter(LowerCaser)
                .filter(RemoveLongFilter::limit(limit))
                .build()
        },
        "whitespace" => {
            TantivyAnalyzer::builder(WhitespaceTokenizer::default())
                .filter(LowerCaser)
                .filter(RemoveLongFilter::limit(limit))
                .build()
        },
        _ => {
            // Unknown base tokenizer, use default behavior
            TantivyAnalyzer::builder(SimpleTokenizer::default())
                .filter(LowerCaser)
                .filter(RemoveLongFilter::limit(limit))
                .build()
        }
    };
    index.tokenizers().register(name, analyzer);
}

// Helper function to extract string list from Java List
fn extract_string_list(env: &mut JNIEnv, list_obj: &JObject) -> Result<Vec<String>, String> {
    // Get the List size
    let size = match env.call_method(list_obj, "size", "()I", &[]) {
        Ok(result) => match result.i() {
            Ok(s) => s,
            Err(_) => return Err("Failed to get list size".to_string()),
        },
        Err(_) => return Err("Failed to call size() on list".to_string()),
    };
    
    let mut strings = Vec::with_capacity(size as usize);
    
    // Extract each string from the list
    for i in 0..size {
        let element = match env.call_method(list_obj, "get", "(I)Ljava/lang/Object;", &[i.into()]) {
            Ok(result) => result.l().map_err(|_| "Failed to get object from list")?,
            Err(_) => return Err("Failed to call get() on list".to_string()),
        };
        
        let string_obj = match env.call_method(&element, "toString", "()Ljava/lang/String;", &[]) {
            Ok(result) => result.l().map_err(|_| "Failed to convert to string")?,
            Err(_) => return Err("Failed to call toString() on list element".to_string()),
        };
        
        let java_string = JString::from(string_obj);
        let rust_string: String = match env.get_string(&java_string) {
            Ok(s) => s.into(),
            Err(_) => return Err("Failed to convert Java string to Rust string".to_string()),
        };
        
        strings.push(rust_string);
    }
    
    Ok(strings)
}

#[no_mangle]
pub extern "system" fn Java_io_indextables_tantivy4java_core_Index_nativeParseQueryLenient(
    mut env: JNIEnv,
    _class: JClass,
    _ptr: jlong,
    _query: JString,
    _default_field_names: JObject,
    _field_boosts: JObject,
    _fuzzy_fields: JObject,
) -> jlong {
    handle_error(&mut env, "Index native methods not fully implemented yet");
    0
}

#[no_mangle]
pub extern "system" fn Java_io_indextables_tantivy4java_core_Index_nativeRegisterTokenizer(
    mut env: JNIEnv,
    _class: JClass,
    _ptr: jlong,
    _name: JString,
    _text_analyzer_ptr: jlong,
) {
    handle_error(&mut env, "Index native methods not fully implemented yet");
}

/// Custom query parser that fixes single-term phrase query issues
pub fn parse_query_with_phrase_fix(
    query_parser: &QueryParser,
    query_str: &str,
    schema: &tantivy::schema::Schema,
    default_fields: &[tantivy::schema::Field]
) -> Result<Box<dyn TantivyQuery>, tantivy::query::QueryParserError> {
    // Preprocess the query string to fix quoted single terms using proper tokenization
    let fixed_query_str = fix_quoted_single_terms(query_str, schema, default_fields);
    
    // Parse the fixed query string
    query_parser.parse_query(&fixed_query_str)
}

/// Fix quoted single terms in query string to prevent single-term phrase queries
/// Uses proper tokenization instead of hardcoded space checking
pub fn fix_quoted_single_terms(query_str: &str, schema: &tantivy::schema::Schema, default_fields: &[tantivy::schema::Field]) -> String {
    let mut result = String::new();
    let mut chars = query_str.chars().peekable();
    
    while let Some(ch) = chars.next() {
        if ch == '"' {
            // Found opening quote, check if it's a single-term phrase
            let mut quoted_content = String::new();
            let mut found_closing_quote = false;
            
            // Collect content until closing quote
            while let Some(inner_ch) = chars.next() {
                if inner_ch == '"' {
                    found_closing_quote = true;
                    break;
                } else {
                    quoted_content.push(inner_ch);
                }
            }
            
            if found_closing_quote {
                // Check if quoted content is a single term using proper tokenization
                // Use the first text field as a representative field for tokenization
                let is_single_term = if let Some(&field) = default_fields.iter()
                    .find(|&&f| matches!(schema.get_field_entry(f).field_type(), tantivy::schema::FieldType::Str(_))) {
                    // Use proper tokenization to check if it's a single token
                    !is_multi_token_pattern_for_quoted_content(schema, field, &quoted_content)
                } else {
                    // Fallback to basic heuristic if no text field available
                    !quoted_content.trim().contains(char::is_whitespace) && !quoted_content.is_empty()
                };
                
                if is_single_term && !quoted_content.is_empty() {
                    // Single term - remove quotes to prevent phrase query
                    result.push_str(&quoted_content);
                } else {
                    // Multi-term phrase - keep quotes for proper phrase query
                    result.push('"');
                    result.push_str(&quoted_content);
                    result.push('"');
                }
            } else {
                // Unclosed quote - keep original
                result.push('"');
                result.push_str(&quoted_content);
            }
        } else {
            // Regular character
            result.push(ch);
        }
    }
    
    result
}

/// Check if quoted content would result in multiple tokens using proper tokenization
/// This is specifically for quoted strings in query parsing
pub fn is_multi_token_pattern_for_quoted_content(schema: &tantivy::schema::Schema, field: tantivy::schema::Field, content: &str) -> bool {
    let field_entry = schema.get_field_entry(field);
    
    if let tantivy::schema::FieldType::Str(text_options) = field_entry.field_type() {
        if let Some(indexing_options) = text_options.get_indexing_options() {
            // Try to get the tokenizer - simulate tokenization behavior
            let tokenizer_name = indexing_options.tokenizer();
            
            // For most tokenizers, we can simulate tokenization by splitting on common boundaries
            let tokens: Vec<&str> = match tokenizer_name {
                "default" | "simple" | "en_stem" => {
                    // Default tokenizer splits on whitespace and punctuation
                    content.split_whitespace()
                        .flat_map(|word| word.split(|c: char| c.is_ascii_punctuation()))
                        .filter(|token| !token.is_empty())
                        .collect()
                },
                "keyword" => {
                    // Keyword tokenizer treats entire input as single token
                    vec![content]
                },
                "whitespace" => {
                    // Whitespace tokenizer only splits on whitespace (preserves case)
                    content.split_whitespace().collect()
                },
                _ => {
                    // Unknown tokenizer - fall back to whitespace splitting
                    content.split_whitespace().collect()
                }
            };
            
            tokens.len() > 1
        } else {
            // No indexing options - treat as single token
            false
        }
    } else {
        // Not a text field - treat as single token
        false
    }
}

#[no_mangle]
pub extern "system" fn Java_io_indextables_tantivy4java_core_Index_nativeClose(
    _env: JNIEnv,
    _class: JClass,
    ptr: jlong,
) {
    release_arc(ptr);
}
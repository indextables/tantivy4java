// jni_utils.rs - JNI utility functions for SplitSearcher
// Extracted from mod.rs during refactoring
// Contains: misc JNI functions like metadata, tokenize, cache status, etc.

use std::sync::Arc;
use jni::objects::{JClass, JString, JObject};
use jni::sys::{jlong, jobject, jstring, jboolean};
use jni::JNIEnv;

use crate::debug_println;
use crate::common::to_java_exception;
use crate::utils::with_arc_safe;
use crate::runtime_manager::block_on_operation;
use super::types::CachedSearcherContext;
use super::schema_creation::create_schema_from_doc_mapping;

/// Replacement for Java_io_indextables_tantivy4java_split_SplitSearcher_getComponentCacheStatusNative
/// Simple implementation that returns an empty HashMap
#[no_mangle]
pub extern "system" fn Java_io_indextables_tantivy4java_split_SplitSearcher_getComponentCacheStatusNative(
    mut env: JNIEnv,
    _class: JClass,
    _searcher_ptr: jlong,
) -> jobject {
    debug_println!("RUST DEBUG: getComponentCacheStatusNative called - creating empty HashMap");
    
    // Create an empty HashMap for component status
    match env.new_object("java/util/HashMap", "()V", &[]) {
        Ok(hashmap) => hashmap.into_raw(),
        Err(_) => {
            // Return null if we can't create the HashMap
            std::ptr::null_mut()
        }
    }
}

// Range query fixing functions moved to range_query_fix.rs

// Stub method implementations
#[no_mangle]
pub extern "system" fn Java_io_indextables_tantivy4java_split_SplitSearcher_evictComponentsNative(
    _env: JNIEnv, _class: JClass, _searcher_ptr: jlong, _components: JObject
) -> jboolean { 0 }

#[no_mangle]
pub extern "system" fn Java_io_indextables_tantivy4java_split_SplitSearcher_parseQueryNative(
    _env: JNIEnv, _class: JClass, _searcher_ptr: jlong, _query: JString
) -> jobject { std::ptr::null_mut() }

#[no_mangle]
pub extern "system" fn Java_io_indextables_tantivy4java_split_SplitSearcher_getSchemaJsonNative(
    _env: JNIEnv, _class: JClass, _searcher_ptr: jlong
) -> jstring { std::ptr::null_mut() }

/// Get split metadata including component sizes from bundle footer
/// Returns SplitMetadata object with:
/// - splitId: The split identifier
/// - totalSize: Total size of all components
/// - hotCacheSize: Size of hotcache (from footer)
/// - numComponents: Number of component files
/// - componentSizes: Map<String, Long> of file path -> size in bytes
#[no_mangle]
pub extern "system" fn Java_io_indextables_tantivy4java_split_SplitSearcher_getSplitMetadataNative(
    mut env: JNIEnv, _class: JClass, searcher_ptr: jlong
) -> jobject {
    // Get the searcher context
    let result = with_arc_safe(searcher_ptr, |ctx: &Arc<CachedSearcherContext>| {
        let bundle_offsets = &ctx.bundle_file_offsets;

        // Calculate component sizes from ranges
        let mut component_sizes: Vec<(String, u64)> = bundle_offsets
            .iter()
            .map(|(path, range)| {
                let path_str = path.to_string_lossy().to_string();
                let size = range.end - range.start;
                (path_str, size)
            })
            .collect();
        component_sizes.sort_by(|a, b| a.0.cmp(&b.0));

        // Calculate total size
        let total_size: u64 = component_sizes.iter().map(|(_, s)| *s).sum();

        // Get split ID from URI
        let split_id = ctx.split_uri
            .rsplit('/')
            .next()
            .unwrap_or(&ctx.split_uri)
            .trim_end_matches(".split")
            .to_string();

        // Hotcache size from footer
        let hotcache_size = ctx.footer_end - ctx.footer_start;

        (split_id, total_size, hotcache_size, component_sizes)
    });

    let (split_id, total_size, hotcache_size, component_sizes) = match result {
        Some(data) => data,
        None => {
            to_java_exception(&mut env, &anyhow::anyhow!("Failed to access searcher context"));
            return std::ptr::null_mut();
        }
    };

    // Create Java HashMap for component sizes
    let hash_map = match env.new_object("java/util/HashMap", "()V", &[]) {
        Ok(m) => m,
        Err(e) => {
            to_java_exception(&mut env, &anyhow::anyhow!("Failed to create HashMap: {}", e));
            return std::ptr::null_mut();
        }
    };

    // Populate the HashMap
    for (path, size) in &component_sizes {
        let j_path = match env.new_string(path) {
            Ok(s) => s,
            Err(_) => continue,
        };
        let j_size = match env.new_object("java/lang/Long", "(J)V", &[jni::objects::JValue::Long(*size as i64)]) {
            Ok(l) => l,
            Err(_) => continue,
        };
        let _ = env.call_method(
            &hash_map,
            "put",
            "(Ljava/lang/Object;Ljava/lang/Object;)Ljava/lang/Object;",
            &[(&j_path).into(), (&j_size).into()]
        );
    }

    // Create SplitMetadata object
    let j_split_id = match env.new_string(&split_id) {
        Ok(s) => s,
        Err(e) => {
            to_java_exception(&mut env, &anyhow::anyhow!("Failed to create split ID string: {}", e));
            return std::ptr::null_mut();
        }
    };

    let metadata = match env.new_object(
        "io/indextables/tantivy4java/split/SplitSearcher$SplitMetadata",
        "(Ljava/lang/String;JJILjava/util/Map;)V",
        &[
            (&j_split_id).into(),
            jni::objects::JValue::Long(total_size as i64),
            jni::objects::JValue::Long(hotcache_size as i64),
            jni::objects::JValue::Int(component_sizes.len() as i32),
            (&hash_map).into(),
        ]
    ) {
        Ok(m) => m,
        Err(e) => {
            to_java_exception(&mut env, &anyhow::anyhow!("Failed to create SplitMetadata: {}", e));
            return std::ptr::null_mut();
        }
    };

    metadata.into_raw()
}

/// Get per-field component sizes (e.g., "score.fastfield" -> 133, "content.fieldnorm" -> 50)
///
/// Returns a Java Map<String, Long> with keys like "field_name.component" and values as sizes in bytes.
/// Components include: fastfield, fieldnorm
///
/// This is async and uses the prewarm module's get_per_field_component_sizes function.
#[no_mangle]
pub extern "system" fn Java_io_indextables_tantivy4java_split_SplitSearcher_getPerFieldComponentSizesNative(
    mut env: JNIEnv, _class: JClass, searcher_ptr: jlong
) -> jobject {
    use crate::prewarm::get_per_field_component_sizes;

    debug_println!("📊 JNI: getPerFieldComponentSizesNative called");

    // Execute async function using block_on_operation
    let field_sizes = match block_on_operation(get_per_field_component_sizes(searcher_ptr)) {
        Ok(sizes) => sizes,
        Err(e) => {
            to_java_exception(&mut env, &anyhow::anyhow!("Failed to get per-field sizes: {}", e));
            return std::ptr::null_mut();
        }
    };

    // Create Java HashMap
    let hash_map = match env.new_object("java/util/HashMap", "()V", &[]) {
        Ok(m) => m,
        Err(e) => {
            to_java_exception(&mut env, &anyhow::anyhow!("Failed to create HashMap: {}", e));
            return std::ptr::null_mut();
        }
    };

    // Populate the HashMap
    for (key, size) in &field_sizes {
        let j_key = match env.new_string(key) {
            Ok(s) => s,
            Err(_) => continue,
        };
        let j_size = match env.new_object("java/lang/Long", "(J)V", &[jni::objects::JValue::Long(*size as i64)]) {
            Ok(l) => l,
            Err(_) => continue,
        };
        let _ = env.call_method(
            &hash_map,
            "put",
            "(Ljava/lang/Object;Ljava/lang/Object;)Ljava/lang/Object;",
            &[(&j_key).into(), (&j_size).into()]
        );
    }

    debug_println!("📊 JNI: getPerFieldComponentSizesNative returning {} entries", field_sizes.len());
    hash_map.into_raw()
}

#[no_mangle]
pub extern "system" fn Java_io_indextables_tantivy4java_split_SplitSearcher_getLoadingStatsNative(
    _env: JNIEnv, _class: JClass, _searcher_ptr: jlong
) -> jobject { std::ptr::null_mut() }
/// Stub implementation for docsBulkNative - focusing on docBatchNative optimization
/// The main performance improvement comes from the optimized docBatchNative method
#[no_mangle]
pub extern "system" fn Java_io_indextables_tantivy4java_split_SplitSearcher_docsBulkNative(
    mut env: JNIEnv,
    _class: JClass,
    _searcher_ptr: jlong,
    _segments: jni::sys::jintArray,
    _doc_ids: jni::sys::jintArray,
) -> jobject {
    // For now, return null - the main optimization is in docBatchNative
    // This method is not currently used by the test, but docBatch is
    to_java_exception(&mut env, &anyhow::anyhow!("docsBulkNative not implemented - use docBatch for optimized bulk retrieval"));
    std::ptr::null_mut()
}
/// Stub implementation for parseBulkDocsNative - focusing on docBatch optimization
#[no_mangle]
pub extern "system" fn Java_io_indextables_tantivy4java_split_SplitSearcher_parseBulkDocsNative(
    mut env: JNIEnv,
    _class: JClass,
    _buffer_jobject: jobject,
) -> jobject {
    // Return empty ArrayList since docsBulkNative is not implemented
    match env.new_object("java/util/ArrayList", "()V", &[]) {
        Ok(empty_list) => empty_list.into_raw(),
        Err(_) => std::ptr::null_mut(),
    }
}

#[no_mangle]
pub extern "system" fn Java_io_indextables_tantivy4java_split_SplitSearcher_tokenizeNative(
    mut env: JNIEnv,
    _class: JClass,
    searcher_ptr: jlong,
    field_name: JString,
    text: JString,
) -> jobject {
    // Extract field name and text from JNI
    let field_name_str: String = match env.get_string(&field_name) {
        Ok(s) => s.into(),
        Err(e) => {
            to_java_exception(&mut env, &anyhow::anyhow!("Invalid field name: {}", e));
            return std::ptr::null_mut();
        }
    };

    let text_str: String = match env.get_string(&text) {
        Ok(s) => s.into(),
        Err(e) => {
            to_java_exception(&mut env, &anyhow::anyhow!("Invalid text: {}", e));
            return std::ptr::null_mut();
        }
    };

    debug_println!("RUST DEBUG: tokenizeNative called for field '{}' with text '{}'", field_name_str, text_str);

    // Get the searcher context and schema (same pattern as get_schema_from_split)
    let result = with_arc_safe(searcher_ptr, |searcher_context: &Arc<CachedSearcherContext>| {
        let context = searcher_context.as_ref();
        let doc_mapping_json = &context.doc_mapping_json;

        // Get schema from doc mapping - throw exception if not available
        let schema = if let Some(doc_mapping) = doc_mapping_json {
            match create_schema_from_doc_mapping(doc_mapping) {
                Ok(schema) => schema,
                Err(e) => {
                    return Err(anyhow::anyhow!("Failed to create schema from doc mapping for tokenization: {}", e));
                }
            }
        } else {
            return Err(anyhow::anyhow!("Doc mapping not available for tokenization - split searcher not properly initialized"));
        };

        // Find the field in the schema
        let field = match schema.get_field(&field_name_str) {
            Ok(field) => field,
            Err(_) => {
                return Err(anyhow::anyhow!("Field '{}' not found in schema", field_name_str));
            }
        };

        // Get the field entry to determine the tokenizer
        let field_entry = schema.get_field_entry(field);

        // Create a text analyzer for the field
        let mut tokenizer = match field_entry.field_type() {
            tantivy::schema::FieldType::Str(text_options) => {
                // For text fields, get the tokenizer from indexing options
                if let Some(indexing_options) = text_options.get_indexing_options() {
                    let tokenizer_name = indexing_options.tokenizer();
                    debug_println!("RUST DEBUG: Field '{}' uses tokenizer '{}'", field_name_str, tokenizer_name);

                    // Create the tokenizer based on the name
                    match tokenizer_name {
                        "default" => {
                            tantivy::tokenizer::TextAnalyzer::builder(tantivy::tokenizer::SimpleTokenizer::default())
                                .filter(tantivy::tokenizer::RemoveLongFilter::limit(40))
                                .filter(tantivy::tokenizer::LowerCaser)
                                .build()
                        },
                        "raw" => {
                            // For string fields (raw tokenizer), return the original text as a single token
                            debug_println!("RUST DEBUG: Using raw tokenizer for field '{}'", field_name_str);
                            let tokens = vec![text_str.clone()];
                            return create_token_list(&mut env, tokens);
                        },
                        "whitespace" => {
                            tantivy::tokenizer::TextAnalyzer::builder(tantivy::tokenizer::WhitespaceTokenizer::default())
                                .build()
                        },
                        "keyword" => {
                            // Keyword tokenizer treats the entire input as a single token
                            let tokens = vec![text_str.clone()];
                            return create_token_list(&mut env, tokens);
                        },
                        _ => {
                            // Default to simple tokenizer for unknown tokenizers
                            debug_println!("RUST DEBUG: Unknown tokenizer '{}', using default", tokenizer_name);
                            tantivy::tokenizer::TextAnalyzer::builder(tantivy::tokenizer::SimpleTokenizer::default())
                                .filter(tantivy::tokenizer::RemoveLongFilter::limit(40))
                                .filter(tantivy::tokenizer::LowerCaser)
                                .build()
                        }
                    }
                } else {
                    // No indexing options means it's not indexed, but we can still tokenize
                    tantivy::tokenizer::TextAnalyzer::builder(tantivy::tokenizer::SimpleTokenizer::default())
                        .filter(tantivy::tokenizer::RemoveLongFilter::limit(40))
                        .filter(tantivy::tokenizer::LowerCaser)
                        .build()
                }
            },
            _ => {
                // For non-text fields (numbers, dates, etc.), return the original text as a single token
                debug_println!("RUST DEBUG: Non-text field '{}', returning original text as single token", field_name_str);
                let tokens = vec![text_str.clone()];
                return create_token_list(&mut env, tokens);
            }
        };

        // Tokenize the text
        let mut token_stream = tokenizer.token_stream(&text_str);
        let mut tokens = Vec::new();

        while let Some(token) = token_stream.next() {
            tokens.push(token.text.clone());
        }

        debug_println!("RUST DEBUG: Tokenized '{}' into {} tokens: {:?}", text_str, tokens.len(), tokens);

        create_token_list(&mut env, tokens)
    });

    match result {
        Some(Ok(tokens_list)) => tokens_list,
        Some(Err(e)) => {
            to_java_exception(&mut env, &e);
            std::ptr::null_mut()
        },
        None => {
            to_java_exception(&mut env, &anyhow::anyhow!("Invalid SplitSearcher pointer"));
            std::ptr::null_mut()
        }
    }
}

/// Helper function to create a Java List<String> from a vector of tokens
fn create_token_list(env: &mut JNIEnv, tokens: Vec<String>) -> Result<jobject, anyhow::Error> {
    // Create ArrayList
    let array_list_class = env.find_class("java/util/ArrayList")?;
    let array_list = env.new_object(&array_list_class, "()V", &[])?;

    // Add each token to the list
    for token in tokens {
        let java_string = env.new_string(&token)?;
        env.call_method(
            &array_list,
            "add",
            "(Ljava/lang/Object;)Z",
            &[(&java_string).into()],
        )?;
    }

    Ok(array_list.into_raw())
}

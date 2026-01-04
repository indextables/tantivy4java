// jni_prewarm.rs - JNI prewarm/preload functions for SplitSearcher
// Extracted from mod.rs during refactoring
// Contains: preloadComponentsNative, preloadFieldsNative, helper functions

use jni::objects::{JClass, JString, JObject};
use jni::sys::{jlong, jobject, jboolean};
use jni::JNIEnv;

use crate::debug_println;
use crate::runtime_manager::block_on_operation;

/// Preload index components into cache for improved search performance.
///
/// This implements Quickwit's warm_up_term_dict_fields() pattern for the TERM component,
/// which preloads entire term dictionaries (FSTs) into the disk cache. Once cached,
/// sub-range requests for different terms are served from the cached data via get_coalesced().
///
/// Components supported:
/// - TERM: Preloads term dictionaries for all indexed text fields
/// - (Other components currently no-op, can be extended in future)
#[no_mangle]
pub extern "system" fn Java_io_indextables_tantivy4java_split_SplitSearcher_preloadComponentsNative(
    mut env: JNIEnv,
    _class: JClass,
    searcher_ptr: jlong,
    components: jobject,
) -> jboolean {
    debug_println!("🔥 PREWARM: preloadComponentsNative called with pointer: {}", searcher_ptr);

    if searcher_ptr == 0 {
        debug_println!("❌ PREWARM: Invalid searcher pointer (0)");
        return 0;
    }

    // Parse the IndexComponent array from Java to determine what to prewarm
    let components_set = match parse_index_components(&mut env, components) {
        Ok(set) => set,
        Err(e) => {
            debug_println!("❌ PREWARM: Failed to parse components: {}", e);
            return 0;
        }
    };

    debug_println!("🔥 PREWARM: Requested components: {:?}", components_set);

    // Check which components are requested
    let prewarm_term = components_set.contains("TERM");
    let prewarm_postings = components_set.contains("POSTINGS");
    let prewarm_fieldnorm = components_set.contains("FIELDNORM");
    let prewarm_fastfield = components_set.contains("FASTFIELD");
    let prewarm_store = components_set.contains("STORE");

    if !prewarm_term && !prewarm_postings && !prewarm_fieldnorm && !prewarm_fastfield && !prewarm_store {
        debug_println!("🔥 PREWARM: No supported components requested, returning success");
        return 1; // Success - nothing to do
    }

    // Perform the warmup using async runtime
    match block_on_operation(async move {
        let mut errors = Vec::new();

        // Prewarm TERM component (FST/term dictionaries)
        if prewarm_term {
            debug_println!("🔥 PREWARM: Warming up TERM component (FST)...");
            if let Err(e) = crate::prewarm::prewarm_term_dictionaries_impl(searcher_ptr).await {
                debug_println!("⚠️ PREWARM: TERM warmup failed: {}", e);
                errors.push(format!("TERM: {}", e));
            } else {
                debug_println!("✅ PREWARM: TERM warmup completed");
            }
        }

        // Prewarm POSTINGS component
        if prewarm_postings {
            debug_println!("🔥 PREWARM: Warming up POSTINGS component...");
            if let Err(e) = crate::prewarm::prewarm_postings_impl(searcher_ptr).await {
                debug_println!("⚠️ PREWARM: POSTINGS warmup failed: {}", e);
                errors.push(format!("POSTINGS: {}", e));
            } else {
                debug_println!("✅ PREWARM: POSTINGS warmup completed");
            }
        }

        // Prewarm FIELDNORM component
        if prewarm_fieldnorm {
            debug_println!("🔥 PREWARM: Warming up FIELDNORM component...");
            if let Err(e) = crate::prewarm::prewarm_fieldnorms_impl(searcher_ptr).await {
                debug_println!("⚠️ PREWARM: FIELDNORM warmup failed: {}", e);
                errors.push(format!("FIELDNORM: {}", e));
            } else {
                debug_println!("✅ PREWARM: FIELDNORM warmup completed");
            }
        }

        // Prewarm FASTFIELD component
        if prewarm_fastfield {
            debug_println!("🔥 PREWARM: Warming up FASTFIELD component...");
            if let Err(e) = crate::prewarm::prewarm_fastfields_impl(searcher_ptr).await {
                debug_println!("⚠️ PREWARM: FASTFIELD warmup failed: {}", e);
                errors.push(format!("FASTFIELD: {}", e));
            } else {
                debug_println!("✅ PREWARM: FASTFIELD warmup completed");
            }
        }

        // Prewarm STORE component (document storage)
        if prewarm_store {
            debug_println!("🔥 PREWARM: Warming up STORE component (document storage)...");
            if let Err(e) = crate::prewarm::prewarm_store_impl(searcher_ptr).await {
                debug_println!("⚠️ PREWARM: STORE warmup failed: {}", e);
                errors.push(format!("STORE: {}", e));
            } else {
                debug_println!("✅ PREWARM: STORE warmup completed");
            }
        }

        if errors.is_empty() {
            Ok(())
        } else {
            Err(anyhow::anyhow!("Some warmups failed: {}", errors.join(", ")))
        }
    }) {
        Ok(_) => {
            debug_println!("✅ PREWARM: All requested components warmed up successfully");
            1 // success
        },
        Err(e) => {
            debug_println!("❌ PREWARM: Component warmup failed: {}", e);
            0 // failure
        }
    }
}

/// Field-specific preloading - preloads a single component type for only the specified fields
///
/// This provides fine-grained control over which fields are preloaded, reducing cache usage
/// and prewarm time compared to preloadComponentsNative which preloads all fields.
#[no_mangle]
pub extern "system" fn Java_io_indextables_tantivy4java_split_SplitSearcher_preloadFieldsNative(
    mut env: JNIEnv,
    _class: JClass,
    searcher_ptr: jlong,
    component: jobject,
    field_names: jobject,
) -> jboolean {
    debug_println!("🔥 PREWARM_FIELDS: preloadFieldsNative called with pointer: {}", searcher_ptr);

    if searcher_ptr == 0 {
        debug_println!("❌ PREWARM_FIELDS: Invalid searcher pointer (0)");
        return 0;
    }

    // Parse the single IndexComponent enum
    let component_name = match parse_single_index_component(&mut env, component) {
        Ok(name) => name,
        Err(e) => {
            debug_println!("❌ PREWARM_FIELDS: Failed to parse component: {}", e);
            return 0;
        }
    };

    // Parse the field names array
    let fields = match parse_string_array(&mut env, field_names) {
        Ok(fields) => fields,
        Err(e) => {
            debug_println!("❌ PREWARM_FIELDS: Failed to parse field names: {}", e);
            return 0;
        }
    };

    if fields.is_empty() {
        debug_println!("❌ PREWARM_FIELDS: No field names provided");
        return 0;
    }

    debug_println!("🔥 PREWARM_FIELDS: Component: {}, Fields: {:?}", component_name, fields);

    // Convert to HashSet for efficient lookup
    let field_filter: std::collections::HashSet<String> = fields.into_iter().collect();

    // Perform the warmup using async runtime
    match block_on_operation(async move {
        match component_name.as_str() {
            "TERM" => {
                debug_println!("🔥 PREWARM_FIELDS: Warming up TERM for specific fields...");
                crate::prewarm::prewarm_term_dictionaries_for_fields(searcher_ptr, &field_filter).await
            },
            "POSTINGS" => {
                debug_println!("🔥 PREWARM_FIELDS: Warming up POSTINGS for specific fields...");
                crate::prewarm::prewarm_postings_for_fields(searcher_ptr, &field_filter).await
            },
            "POSITIONS" => {
                debug_println!("🔥 PREWARM_FIELDS: Warming up POSITIONS for specific fields...");
                crate::prewarm::prewarm_positions_for_fields(searcher_ptr, &field_filter).await
            },
            "FIELDNORM" => {
                debug_println!("🔥 PREWARM_FIELDS: Warming up FIELDNORM for specific fields...");
                crate::prewarm::prewarm_fieldnorms_for_fields(searcher_ptr, &field_filter).await
            },
            "FASTFIELD" => {
                debug_println!("🔥 PREWARM_FIELDS: Warming up FASTFIELD for specific fields...");
                crate::prewarm::prewarm_fastfields_for_fields(searcher_ptr, &field_filter).await
            },
            "STORE" => {
                // STORE is not field-specific, just call the regular implementation
                debug_println!("🔥 PREWARM_FIELDS: STORE is not field-specific, warming all...");
                crate::prewarm::prewarm_store_impl(searcher_ptr).await
            },
            _ => {
                Err(anyhow::anyhow!("Unsupported component for field-specific preloading: {}", component_name))
            }
        }
    }) {
        Ok(_) => {
            debug_println!("✅ PREWARM_FIELDS: Field-specific warmup completed successfully");
            1 // success
        },
        Err(e) => {
            debug_println!("❌ PREWARM_FIELDS: Field-specific warmup failed: {}", e);
            0 // failure
        }
    }
}

/// Parse a single Java IndexComponent enum into its name
fn parse_single_index_component(env: &mut JNIEnv, component: jobject) -> anyhow::Result<String> {
    if component.is_null() {
        return Err(anyhow::anyhow!("Component is null"));
    }

    let element = unsafe { JObject::from_raw(component) };

    // Get the enum name using Enum.name() method
    let name_obj = env.call_method(&element, "name", "()Ljava/lang/String;", &[])
        .map_err(|e| anyhow::anyhow!("Failed to call name() on enum: {}", e))?
        .l()
        .map_err(|e| anyhow::anyhow!("Failed to get string from name(): {}", e))?;

    let name_jstring = JString::from(name_obj);
    let name: String = env.get_string(&name_jstring)
        .map_err(|e| anyhow::anyhow!("Failed to convert JString: {}", e))?
        .into();

    Ok(name)
}

/// Parse a Java String[] array into a Vec<String>
fn parse_string_array(env: &mut JNIEnv, array_obj: jobject) -> anyhow::Result<Vec<String>> {
    use jni::objects::JObjectArray;

    let mut result = Vec::new();

    if array_obj.is_null() {
        return Ok(result);
    }

    let array = unsafe { JObjectArray::from_raw(array_obj) };
    let length = env.get_array_length(&array)
        .map_err(|e| anyhow::anyhow!("Failed to get array length: {}", e))?;

    for i in 0..length {
        let element = env.get_object_array_element(&array, i)
            .map_err(|e| anyhow::anyhow!("Failed to get array element {}: {}", i, e))?;

        if element.is_null() {
            continue;
        }

        let jstring = JString::from(element);
        let s: String = env.get_string(&jstring)
            .map_err(|e| anyhow::anyhow!("Failed to convert JString at index {}: {}", i, e))?
            .into();

        result.push(s);
    }

    Ok(result)
}

/// Parse the Java IndexComponent[] array into a HashSet of component names
fn parse_index_components(env: &mut JNIEnv, components: jobject) -> anyhow::Result<std::collections::HashSet<String>> {
    use jni::objects::JObjectArray;

    let mut result = std::collections::HashSet::new();

    if components.is_null() {
        return Ok(result);
    }

    let array = unsafe { JObjectArray::from_raw(components) };
    let length = env.get_array_length(&array)
        .map_err(|e| anyhow::anyhow!("Failed to get array length: {}", e))?;

    for i in 0..length {
        let element = env.get_object_array_element(&array, i)
            .map_err(|e| anyhow::anyhow!("Failed to get array element {}: {}", i, e))?;

        if element.is_null() {
            continue;
        }

        // Get the enum name using Enum.name() method
        let name_obj = env.call_method(&element, "name", "()Ljava/lang/String;", &[])
            .map_err(|e| anyhow::anyhow!("Failed to call name() on enum: {}", e))?
            .l()
            .map_err(|e| anyhow::anyhow!("Failed to get string from name(): {}", e))?;

        let name_jstring = JString::from(name_obj);
        let name: String = env.get_string(&name_jstring)
            .map_err(|e| anyhow::anyhow!("Failed to convert JString: {}", e))?
            .into();

        debug_println!("🔥 PREWARM: Parsed component: {}", name);
        result.insert(name);
    }

    Ok(result)
}

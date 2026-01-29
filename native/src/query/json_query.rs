// json_query.rs - JSON field query JNI methods
// Extracted from mod.rs during refactoring
// Contains: nativeJsonTermQuery, nativeJsonRangeQuery, nativeJsonExistsQuery

use jni::objects::{JClass, JString, JObject};
use jni::sys::{jlong, jboolean, jobject};
use jni::JNIEnv;
use tantivy::query::{Query as TantivyQuery, TermQuery, RangeQuery, ExistsQuery};
use tantivy::schema::{Schema, Term, IndexRecordOption};
use std::ops::Bound;
use std::sync::Arc;
use crate::utils::{handle_error, with_arc_safe, arc_to_jlong};
use crate::extract_helpers::extract_long_value;

#[no_mangle]
pub extern "system" fn Java_io_indextables_tantivy4java_query_Query_nativeJsonTermQuery(
    mut env: JNIEnv,
    _class: JClass,
    schema_ptr: jlong,
    field_name: JString,
    json_path: JString,
    term_value: jobject,
) -> jlong {
    let field_name_str: String = match env.get_string(&field_name) {
        Ok(s) => s.into(),
        Err(_) => {
            handle_error(&mut env, "Invalid field name");
            return 0;
        }
    };

    let json_path_str: String = match env.get_string(&json_path) {
        Ok(s) => s.into(),
        Err(_) => {
            handle_error(&mut env, "Invalid JSON path");
            return 0;
        }
    };

    let result = with_arc_safe::<Schema, Result<Box<dyn TantivyQuery>, String>>(schema_ptr, |schema_arc| {
        let schema = schema_arc.as_ref();

        // Get field by name
        let field = match schema.get_field(&field_name_str) {
            Ok(f) => f,
            Err(_) => return Err(format!("Field '{}' not found in schema", field_name_str)),
        };

        // Validate field is JSON type
        let field_type = schema.get_field_entry(field).field_type();
        match field_type {
            tantivy::schema::FieldType::JsonObject(_) => {},
            _ => return Err(format!("Field '{}' is not a JSON field", field_name_str)),
        }

        // Safely validate term_value before use
        if term_value.is_null() {
            return Err("Term value cannot be null".to_string());
        }

        // Use safe JObject construction from validated jobject
        let term_value_obj = unsafe {
            // SAFETY: We've validated term_value is not null above
            JObject::from_raw(term_value)
        };

        // Extract the value and convert to appropriate type
        // For now, treat as string - in future could detect type
        let value_str: String = match env.get_string(&JString::from(term_value_obj)) {
            Ok(s) => s.into(),
            Err(_) => return Err("Invalid term value".to_string()),
        };

        // Check if expand_dots is enabled in the field options
        let expand_dots = match field_type {
            tantivy::schema::FieldType::JsonObject(opts) => opts.is_expand_dots_enabled(),
            _ => false,
        };

        // Lowercase the value to match default tokenizer behavior
        // JSON fields with default indexing use the "default" tokenizer which lowercases text
        // TODO: In future, properly tokenize based on the actual tokenizer configured
        let tokenized_value = value_str.to_lowercase();

        // Create JSON term using the path properly with Term::from_field_json_path
        let mut term = Term::from_field_json_path(field, &json_path_str, expand_dots);
        // Append the string value with proper type marker
        term.append_type_and_str(&tokenized_value);

        let query = TermQuery::new(term, IndexRecordOption::Basic);
        Ok(Box::new(query) as Box<dyn TantivyQuery>)
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
            handle_error(&mut env, "Invalid schema pointer");
            0
        }
    }
}

#[no_mangle]
pub extern "system" fn Java_io_indextables_tantivy4java_query_Query_nativeJsonRangeQuery(
    mut env: JNIEnv,
    _class: JClass,
    schema_ptr: jlong,
    field_name: JString,
    json_path: JString,
    lower_bound: jobject,
    upper_bound: jobject,
    include_lower: jboolean,
    include_upper: jboolean,
) -> jlong {
    let field_name_str: String = match env.get_string(&field_name) {
        Ok(s) => s.into(),
        Err(_) => {
            handle_error(&mut env, "Invalid field name");
            return 0;
        }
    };

    let json_path_str: String = match env.get_string(&json_path) {
        Ok(s) => s.into(),
        Err(_) => {
            handle_error(&mut env, "Invalid JSON path");
            return 0;
        }
    };

    let result = with_arc_safe::<Schema, Result<Box<dyn TantivyQuery>, String>>(schema_ptr, |schema_arc| {
        let schema = schema_arc.as_ref();

        // Get field by name
        let field = match schema.get_field(&field_name_str) {
            Ok(f) => f,
            Err(_) => return Err(format!("Field '{}' not found in schema", field_name_str)),
        };

        // Validate field is JSON type
        let field_type = schema.get_field_entry(field).field_type();
        match field_type {
            tantivy::schema::FieldType::JsonObject(_) => {},
            _ => return Err(format!("Field '{}' is not a JSON field", field_name_str)),
        }

        // Extract bounds - for now assume numeric (i64)
        // In future could detect type from Java object
        let lower_obj = unsafe { JObject::from_raw(lower_bound) };
        let upper_obj = unsafe { JObject::from_raw(upper_bound) };

        let lower_val = if !lower_bound.is_null() {
            match extract_long_value(&mut env, &lower_obj) {
                Ok(val) => if include_lower != 0 { Bound::Included(val) } else { Bound::Excluded(val) },
                Err(e) => return Err(format!("Failed to extract lower bound: {}", e)),
            }
        } else {
            Bound::Unbounded
        };

        let upper_val = if !upper_bound.is_null() {
            match extract_long_value(&mut env, &upper_obj) {
                Ok(val) => if include_upper != 0 { Bound::Included(val) } else { Bound::Excluded(val) },
                Err(e) => return Err(format!("Failed to extract upper bound: {}", e)),
            }
        } else {
            Bound::Unbounded
        };

        // For JSON fields, we need to create JSON-typed terms with the path
        // Check if expand_dots is enabled in the field options
        let expand_dots = match field_type {
            tantivy::schema::FieldType::JsonObject(opts) => opts.is_expand_dots_enabled(),
            _ => false,
        };

        // Create Term bounds for JSON field range query
        let lower_term = match lower_val {
            Bound::Included(val) => {
                let mut term = Term::from_field_json_path(field, &json_path_str, expand_dots);
                term.append_type_and_fast_value(val);
                Bound::Included(term)
            },
            Bound::Excluded(val) => {
                let mut term = Term::from_field_json_path(field, &json_path_str, expand_dots);
                term.append_type_and_fast_value(val);
                Bound::Excluded(term)
            },
            Bound::Unbounded => Bound::Unbounded,
        };

        let upper_term = match upper_val {
            Bound::Included(val) => {
                let mut term = Term::from_field_json_path(field, &json_path_str, expand_dots);
                term.append_type_and_fast_value(val);
                Bound::Included(term)
            },
            Bound::Excluded(val) => {
                let mut term = Term::from_field_json_path(field, &json_path_str, expand_dots);
                term.append_type_and_fast_value(val);
                Bound::Excluded(term)
            },
            Bound::Unbounded => Bound::Unbounded,
        };

        // Create range query on the JSON field path
        let query = RangeQuery::new(lower_term, upper_term);
        Ok(Box::new(query) as Box<dyn TantivyQuery>)
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
            handle_error(&mut env, "Invalid schema pointer");
            0
        }
    }
}

/// Creates a JSON ExistsQuery to check if a JSON path has any non-null value.
/// Uses json_subpaths=true by default to match any subpath under the specified path.
///
/// The JSON field MUST be configured as a FAST field in the schema.
///
/// # Arguments
/// * `schema_ptr` - Pointer to the Schema object
/// * `field_name` - Name of the JSON field
/// * `json_path` - JSON path within the field (e.g., "user.name")
///
/// # Returns
/// * Pointer to the created Query object, or 0 on error
#[no_mangle]
pub extern "system" fn Java_io_indextables_tantivy4java_query_Query_nativeJsonExistsQuery(
    mut env: JNIEnv,
    _class: JClass,
    schema_ptr: jlong,
    field_name: JString,
    json_path: JString,
) -> jlong {
    let field_name_str: String = match env.get_string(&field_name) {
        Ok(s) => s.into(),
        Err(_) => {
            handle_error(&mut env, "Invalid field name");
            return 0;
        }
    };

    let json_path_str: String = match env.get_string(&json_path) {
        Ok(s) => s.into(),
        Err(_) => {
            handle_error(&mut env, "Invalid JSON path");
            return 0;
        }
    };

    let result = with_arc_safe::<Schema, Result<Box<dyn TantivyQuery>, String>>(schema_ptr, |schema_arc| {
        let schema = schema_arc.as_ref();

        // Get field by name
        let field = match schema.get_field(&field_name_str) {
            Ok(f) => f,
            Err(_) => return Err(format!("Field '{}' not found in schema", field_name_str)),
        };

        // Validate field is JSON type
        let field_type = schema.get_field_entry(field).field_type();
        match field_type {
            tantivy::schema::FieldType::JsonObject(_) => {},
            _ => return Err(format!("Field '{}' is not a JSON field", field_name_str)),
        }

        // Verify field is fast (required by ExistsQuery)
        if !field_type.is_fast() {
            return Err(format!(
                "JSON field '{}' is not a fast field. ExistsQuery requires fast fields. \
                Use JsonObjectOptions.full() or add FAST flag when creating the field.",
                field_name_str
            ));
        }

        // Build full path: field_name.json_path (or just field_name if path is empty)
        let full_path = if json_path_str.is_empty() {
            field_name_str.clone()
        } else {
            format!("{}.{}", field_name_str, json_path_str)
        };

        // Create ExistsQuery with json_subpaths=true to match subpaths
        let query = ExistsQuery::new(full_path, true);
        Ok(Box::new(query) as Box<dyn TantivyQuery>)
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
            handle_error(&mut env, "Invalid schema pointer");
            0
        }
    }
}

/// Creates a JSON ExistsQuery with control over subpath matching behavior.
///
/// When check_subpaths is true, the query will match if ANY subpath under the
/// specified path exists. When false, only exact path matches are considered.
///
/// # Arguments
/// * `schema_ptr` - Pointer to the Schema object
/// * `field_name` - Name of the JSON field
/// * `json_path` - JSON path within the field (e.g., "user.name")
/// * `check_subpaths` - If true, matches if any subpath exists
///
/// # Returns
/// * Pointer to the created Query object, or 0 on error
#[no_mangle]
pub extern "system" fn Java_io_indextables_tantivy4java_query_Query_nativeJsonExistsQueryWithSubpaths(
    mut env: JNIEnv,
    _class: JClass,
    schema_ptr: jlong,
    field_name: JString,
    json_path: JString,
    check_subpaths: jboolean,
) -> jlong {
    let field_name_str: String = match env.get_string(&field_name) {
        Ok(s) => s.into(),
        Err(_) => {
            handle_error(&mut env, "Invalid field name");
            return 0;
        }
    };

    let json_path_str: String = match env.get_string(&json_path) {
        Ok(s) => s.into(),
        Err(_) => {
            handle_error(&mut env, "Invalid JSON path");
            return 0;
        }
    };

    let result = with_arc_safe::<Schema, Result<Box<dyn TantivyQuery>, String>>(schema_ptr, |schema_arc| {
        let schema = schema_arc.as_ref();

        // Get field by name
        let field = match schema.get_field(&field_name_str) {
            Ok(f) => f,
            Err(_) => return Err(format!("Field '{}' not found in schema", field_name_str)),
        };

        // Validate field is JSON type
        let field_type = schema.get_field_entry(field).field_type();
        match field_type {
            tantivy::schema::FieldType::JsonObject(_) => {},
            _ => return Err(format!("Field '{}' is not a JSON field", field_name_str)),
        }

        // Verify field is fast (required by ExistsQuery)
        if !field_type.is_fast() {
            return Err(format!(
                "JSON field '{}' is not a fast field. ExistsQuery requires fast fields. \
                Use JsonObjectOptions.full() or add FAST flag when creating the field.",
                field_name_str
            ));
        }

        // Build full path: field_name.json_path (or just field_name if path is empty)
        let full_path = if json_path_str.is_empty() {
            field_name_str.clone()
        } else {
            format!("{}.{}", field_name_str, json_path_str)
        };

        // Create ExistsQuery with the specified subpaths behavior
        let query = ExistsQuery::new(full_path, check_subpaths != 0);
        Ok(Box::new(query) as Box<dyn TantivyQuery>)
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
            handle_error(&mut env, "Invalid schema pointer");
            0
        }
    }
}
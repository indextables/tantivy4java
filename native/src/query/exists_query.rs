// exists_query.rs - ExistsQuery JNI methods for field existence checks
// Implements IS NOT NULL / IS NULL query functionality
// Contains: nativeExistsQuery

use jni::objects::{JClass, JString};
use jni::sys::jlong;
use jni::JNIEnv;
use tantivy::query::{Query as TantivyQuery, ExistsQuery};
use tantivy::schema::Schema;
use std::sync::Arc;
use crate::utils::{handle_error, with_arc_safe, arc_to_jlong};

/// Creates an ExistsQuery to check if a field has any non-null value.
/// This is equivalent to an IS NOT NULL query in SQL.
///
/// The field MUST be configured as a FAST field in the schema.
/// If the field is not a fast field, an error will be thrown.
///
/// # Arguments
/// * `schema_ptr` - Pointer to the Schema object
/// * `field_name` - Name of the field to check for existence
///
/// # Returns
/// * Pointer to the created Query object, or 0 on error
#[no_mangle]
pub extern "system" fn Java_io_indextables_tantivy4java_query_Query_nativeExistsQuery(
    mut env: JNIEnv,
    _class: JClass,
    schema_ptr: jlong,
    field_name: JString,
) -> jlong {
    let field_name_str: String = match env.get_string(&field_name) {
        Ok(s) => s.into(),
        Err(_) => {
            handle_error(&mut env, "Invalid field name");
            return 0;
        }
    };

    let result = with_arc_safe::<Schema, Result<Box<dyn TantivyQuery>, String>>(schema_ptr, |schema_arc| {
        let schema = schema_arc.as_ref();

        // Verify field exists in schema
        let field = match schema.get_field(&field_name_str) {
            Ok(f) => f,
            Err(_) => return Err(format!("Field '{}' not found in schema", field_name_str)),
        };

        // Verify field is a fast field (required by ExistsQuery)
        let field_type = schema.get_field_entry(field).field_type();
        if !field_type.is_fast() {
            return Err(format!(
                "Field '{}' is not a fast field. ExistsQuery requires fast fields. \
                Add the FAST flag when creating the field in the schema.",
                field_name_str
            ));
        }

        // Create ExistsQuery with json_subpaths=false for regular fields
        let query = ExistsQuery::new(field_name_str.clone(), false);
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

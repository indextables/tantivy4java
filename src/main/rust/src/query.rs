use jni::objects::{JClass, JString};
use jni::sys::jlong;
use jni::JNIEnv;
use tantivy::query::{Query, TermQuery, RangeQuery, BooleanQuery, AllQuery, Occur};
use tantivy::schema::{Field, Term};

use crate::{drop_handle, ptr_to_handle, handle_to_ptr};

#[no_mangle]
pub extern "system" fn Java_com_tantivy4java_Query_nativeTermQuery(
    env: JNIEnv,
    _class: JClass,
    field_handle: jlong,
    term: JString,
) -> jlong {
    if let Some(field) = ptr_to_handle::<Field>(field_handle) {
        if let Ok(term_str) = env.get_string(term) {
            let term_str: String = term_str.into();
            let term = Term::from_field_text(*field, &term_str);
            let query = TermQuery::new(term, tantivy::schema::IndexRecordOption::Basic);
            let query_box: Box<dyn Query> = Box::new(query);
            return handle_to_ptr(query_box);
        }
    }
    0
}

#[no_mangle]
pub extern "system" fn Java_com_tantivy4java_Query_nativeRangeQuery(
    env: JNIEnv,
    _class: JClass,
    field_handle: jlong,
    lower_bound: JString,
    upper_bound: JString,
) -> jlong {
    if let Some(field) = ptr_to_handle::<Field>(field_handle) {
        if let (Ok(lower_str), Ok(upper_str)) = (env.get_string(lower_bound), env.get_string(upper_bound)) {
            let lower_str: String = lower_str.into();
            let upper_str: String = upper_str.into();
            
            let lower_term = Term::from_field_text(*field, &lower_str);
            let upper_term = Term::from_field_text(*field, &upper_str);
            
            let query = RangeQuery::new_term_bounds(
                *field,
                tantivy::query::Bound::Included(lower_term),
                tantivy::query::Bound::Included(upper_term),
            );
            let query_box: Box<dyn Query> = Box::new(query);
            return handle_to_ptr(query_box);
        }
    }
    0
}

#[no_mangle]
pub extern "system" fn Java_com_tantivy4java_Query_nativeBooleanQuery(
    _env: JNIEnv,
    _class: JClass,
) -> jlong {
    let query = BooleanQuery::new(Vec::new());
    let query_box: Box<dyn Query> = Box::new(query);
    handle_to_ptr(query_box)
}

#[no_mangle]
pub extern "system" fn Java_com_tantivy4java_Query_nativeAllQuery(
    _env: JNIEnv,
    _class: JClass,
) -> jlong {
    let query = AllQuery;
    let query_box: Box<dyn Query> = Box::new(query);
    handle_to_ptr(query_box)
}

#[no_mangle]
pub extern "system" fn Java_com_tantivy4java_BooleanQuery_nativeMust(
    _env: JNIEnv,
    _class: JClass,
    bool_query_handle: jlong,
    query_handle: jlong,
) {
    if let (Some(_bool_query), Some(_query)) = (
        ptr_to_handle::<Box<dyn Query>>(bool_query_handle),
        ptr_to_handle::<Box<dyn Query>>(query_handle),
    ) {
    }
}

#[no_mangle]
pub extern "system" fn Java_com_tantivy4java_BooleanQuery_nativeShould(
    _env: JNIEnv,
    _class: JClass,
    bool_query_handle: jlong,
    query_handle: jlong,
) {
    if let (Some(_bool_query), Some(_query)) = (
        ptr_to_handle::<Box<dyn Query>>(bool_query_handle),
        ptr_to_handle::<Box<dyn Query>>(query_handle),
    ) {
    }
}

#[no_mangle]
pub extern "system" fn Java_com_tantivy4java_BooleanQuery_nativeMustNot(
    _env: JNIEnv,
    _class: JClass,
    bool_query_handle: jlong,
    query_handle: jlong,
) {
    if let (Some(_bool_query), Some(_query)) = (
        ptr_to_handle::<Box<dyn Query>>(bool_query_handle),
        ptr_to_handle::<Box<dyn Query>>(query_handle),
    ) {
    }
}

#[no_mangle]
pub extern "system" fn Java_com_tantivy4java_Query_nativeClose(
    _env: JNIEnv,
    _class: JClass,
    query_handle: jlong,
) {
    drop_handle::<Box<dyn Query>>(query_handle);
}
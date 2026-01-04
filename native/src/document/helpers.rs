// helpers.rs - Document helper functions
// Extracted from document.rs during refactoring

use std::sync::{Arc, Mutex};

use chrono::Timelike;
use jni::objects::JObject;
use jni::sys::jlong;
use jni::JNIEnv;
use tantivy::DateTime;

use crate::utils::arc_to_jlong;

use super::types::{DocumentWrapper, RetrievedDocument};

/// Helper function to convert Java LocalDateTime to Tantivy DateTime
pub fn convert_java_localdatetime_to_tantivy(
    env: &mut JNIEnv,
    date_obj: &JObject,
) -> Result<DateTime, String> {
    // Get year, month, day, hour, minute, second from Java LocalDateTime
    let year = match env.call_method(date_obj, "getYear", "()I", &[]) {
        Ok(result) => result.i().map_err(|e| format!("Failed to get year: {}", e))?,
        Err(e) => return Err(format!("Failed to call getYear: {}", e)),
    };

    let month = match env.call_method(date_obj, "getMonthValue", "()I", &[]) {
        Ok(result) => result
            .i()
            .map_err(|e| format!("Failed to get month: {}", e))?,
        Err(e) => return Err(format!("Failed to call getMonthValue: {}", e)),
    };

    let day = match env.call_method(date_obj, "getDayOfMonth", "()I", &[]) {
        Ok(result) => result.i().map_err(|e| format!("Failed to get day: {}", e))?,
        Err(e) => return Err(format!("Failed to call getDayOfMonth: {}", e)),
    };

    let hour = match env.call_method(date_obj, "getHour", "()I", &[]) {
        Ok(result) => result
            .i()
            .map_err(|e| format!("Failed to get hour: {}", e))?,
        Err(e) => return Err(format!("Failed to call getHour: {}", e)),
    };

    let minute = match env.call_method(date_obj, "getMinute", "()I", &[]) {
        Ok(result) => result
            .i()
            .map_err(|e| format!("Failed to get minute: {}", e))?,
        Err(e) => return Err(format!("Failed to call getMinute: {}", e)),
    };

    let second = match env.call_method(date_obj, "getSecond", "()I", &[]) {
        Ok(result) => result
            .i()
            .map_err(|e| format!("Failed to get second: {}", e))?,
        Err(e) => return Err(format!("Failed to call getSecond: {}", e)),
    };

    // Extract nanoseconds to preserve microsecond precision
    let nano = match env.call_method(date_obj, "getNano", "()I", &[]) {
        Ok(result) => result
            .i()
            .map_err(|e| format!("Failed to get nano: {}", e))?,
        Err(e) => return Err(format!("Failed to call getNano: {}", e)),
    };

    // Convert to UTC timestamp with nanosecond precision
    let timestamp_nanos = match chrono::NaiveDate::from_ymd_opt(year, month as u32, day as u32)
        .and_then(|date| {
            date.and_hms_nano_opt(hour as u32, minute as u32, second as u32, nano as u32)
        })
        .map(|naive_dt| {
            let seconds = naive_dt.and_utc().timestamp();
            let nanos = naive_dt.nanosecond() as i64;
            seconds * 1_000_000_000 + nanos // Total nanoseconds since epoch
        }) {
        Some(ts) => ts,
        None => return Err("Invalid date/time values".to_string()),
    };

    Ok(DateTime::from_timestamp_nanos(timestamp_nanos))
}

/// Create a Java Document object from a RetrievedDocument for bulk operations
pub fn create_java_document_from_retrieved(
    _env: &mut JNIEnv,
    retrieved_doc: RetrievedDocument,
) -> anyhow::Result<jlong> {
    // Create a new DocumentWrapper with the retrieved document
    let document_wrapper = DocumentWrapper::Retrieved(retrieved_doc);

    // Register the document and return the pointer
    let wrapper_arc = Arc::new(Mutex::new(document_wrapper));
    let doc_ptr = arc_to_jlong(wrapper_arc) as u64;

    Ok(doc_ptr as jlong)
}

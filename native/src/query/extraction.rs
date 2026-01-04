// extraction.rs - Helper functions for extracting Java values
// Extracted from mod.rs during refactoring
// Contains: extract_occur_queries, extract_date_value, extract_phrase_words, extract_term_set_values

use jni::objects::{JString, JObject};
use jni::JNIEnv;
use tantivy::query::Occur;
use tantivy::schema::{Schema, Term};
use tantivy::DateTime;
use time::Month;

/// Extract boolean query occurrences from a Java List
pub(crate) fn extract_occur_queries(env: &mut JNIEnv, list_obj: &JObject) -> Result<Vec<(Occur, i64)>, String> {
    // Get the List size
    let size = match env.call_method(list_obj, "size", "()I", &[]) {
        Ok(result) => match result.i() {
            Ok(s) => s,
            Err(_) => return Err("Failed to get list size".to_string()),
        },
        Err(_) => return Err("Failed to call size() on list".to_string()),
    };
    
    let mut occur_queries = Vec::with_capacity(size as usize);
    
    // Extract each OccurQuery from the list
    for i in 0..size {
        let element = match env.call_method(list_obj, "get", "(I)Ljava/lang/Object;", &[i.into()]) {
            Ok(result) => result.l().map_err(|_| "Failed to get object from list")?,
            Err(_) => return Err("Failed to call get() on list".to_string()),
        };
        
        // Get the Occur enum value
        let occur_obj = match env.call_method(&element, "getOccur", "()Lio/indextables/tantivy4java/query/Occur;", &[]) {
            Ok(result) => result.l().map_err(|_| "Failed to get Occur object")?,
            Err(_) => return Err("Failed to call getOccur() on OccurQuery".to_string()),
        };
        
        let occur_value = match env.call_method(&occur_obj, "getValue", "()I", &[]) {
            Ok(result) => match result.i() {
                Ok(v) => v,
                Err(_) => return Err("Failed to get occur value".to_string()),
            },
            Err(_) => return Err("Failed to call getValue() on Occur".to_string()),
        };
        
        // Convert Java Occur to Tantivy Occur
        let occur = match occur_value {
            1 => Occur::Must,
            2 => Occur::Should,
            3 => Occur::MustNot,
            _ => return Err(format!("Unknown Occur value: {}", occur_value)),
        };
        
        // Get the Query object
        let query_obj = match env.call_method(&element, "getQuery", "()Lio/indextables/tantivy4java/query/Query;", &[]) {
            Ok(result) => result.l().map_err(|_| "Failed to get Query object")?,
            Err(_) => return Err("Failed to call getQuery() on OccurQuery".to_string()),
        };
        
        // Get the native pointer from the Query
        let query_ptr = match env.call_method(&query_obj, "getNativePtr", "()J", &[]) {
            Ok(result) => match result.j() {
                Ok(ptr) => ptr,
                Err(_) => return Err("Failed to get query native pointer".to_string()),
            },
            Err(_) => return Err("Failed to call getNativePtr() on Query".to_string()),
        };
        
        occur_queries.push((occur, query_ptr));
    }
    
    Ok(occur_queries)
}


// The extract_long_value and extract_double_value functions are now imported from extract_helpers module
// Helper function to extract DateTime value from Java LocalDateTime
pub(crate) fn extract_date_value(env: &mut JNIEnv, obj: &JObject) -> Result<DateTime, String> {
    if obj.is_null() {
        return Err("LocalDateTime object is null".to_string());
    }
    
    // Extract year, month, day, hour, minute, second from LocalDateTime
    let year = match env.call_method(obj, "getYear", "()I", &[]) {
        Ok(result) => match result.i() {
            Ok(y) => y,
            Err(_) => return Err("Failed to get year".to_string()),
        },
        Err(_) => return Err("Failed to call getYear()".to_string()),
    };
    
    let month = match env.call_method(obj, "getMonthValue", "()I", &[]) {
        Ok(result) => match result.i() {
            Ok(m) => m,
            Err(_) => return Err("Failed to get month".to_string()),
        },
        Err(_) => return Err("Failed to call getMonthValue()".to_string()),
    };
    
    let day = match env.call_method(obj, "getDayOfMonth", "()I", &[]) {
        Ok(result) => match result.i() {
            Ok(d) => d,
            Err(_) => return Err("Failed to get day".to_string()),
        },
        Err(_) => return Err("Failed to call getDayOfMonth()".to_string()),
    };
    
    let hour = match env.call_method(obj, "getHour", "()I", &[]) {
        Ok(result) => match result.i() {
            Ok(h) => h,
            Err(_) => return Err("Failed to get hour".to_string()),
        },
        Err(_) => return Err("Failed to call getHour()".to_string()),
    };
    
    let minute = match env.call_method(obj, "getMinute", "()I", &[]) {
        Ok(result) => match result.i() {
            Ok(m) => m,
            Err(_) => return Err("Failed to get minute".to_string()),
        },
        Err(_) => return Err("Failed to call getMinute()".to_string()),
    };
    
    let second = match env.call_method(obj, "getSecond", "()I", &[]) {
        Ok(result) => match result.i() {
            Ok(s) => s,
            Err(_) => return Err("Failed to get second".to_string()),
        },
        Err(_) => return Err("Failed to call getSecond()".to_string()),
    };

    // Extract nanoseconds to preserve microsecond precision in queries
    let nano = match env.call_method(obj, "getNano", "()I", &[]) {
        Ok(result) => match result.i() {
            Ok(n) => n,
            Err(_) => return Err("Failed to get nano".to_string()),
        },
        Err(_) => return Err("Failed to call getNano()".to_string()),
    };

    // Convert month number to Month enum
    let month_enum = match month {
        1 => Month::January,
        2 => Month::February,
        3 => Month::March,
        4 => Month::April,
        5 => Month::May,
        6 => Month::June,
        7 => Month::July,
        8 => Month::August,
        9 => Month::September,
        10 => Month::October,
        11 => Month::November,
        12 => Month::December,
        _ => return Err(format!("Invalid month: {}", month)),
    };

    // Create OffsetDateTime with nanosecond precision and convert to Tantivy DateTime
    match time::Date::from_calendar_date(year, month_enum, day as u8)
        .and_then(|date| {
            time::Time::from_hms_nano(hour as u8, minute as u8, second as u8, nano as u32)
                .map(|time| date.with_time(time))
        })
    {
        Ok(datetime) => {
            let offset_dt = datetime.assume_utc();
            Ok(DateTime::from_utc(offset_dt))
        },
        Err(_) => Err("Invalid date/time values".to_string()),
    }
}

// Helper function to extract phrase words from Java List
// Handles List<String> for phrase queries
pub(crate) fn extract_phrase_words(env: &mut JNIEnv, list_obj: &JObject) -> Result<Vec<(Option<i32>, String)>, String> {
    // Get the List size
    let size = match env.call_method(list_obj, "size", "()I", &[]) {
        Ok(result) => match result.i() {
            Ok(s) => s,
            Err(_) => return Err("Failed to get list size".to_string()),
        },
        Err(_) => return Err("Failed to call size() on list".to_string()),
    };
    
    let mut words = Vec::with_capacity(size as usize);
    
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
        
        words.push((None, rust_string));
    }
    
    Ok(words)
}

// Helper function to extract term values from Java List for TermSetQuery
pub(crate) fn extract_term_set_values(
    env: &mut JNIEnv,
    field_values_list: &JObject,
    field: tantivy::schema::Field,
    schema: &Schema,
) -> Result<Vec<Term>, String> {
    if field_values_list.is_null() {
        return Err("Field values list cannot be null".to_string());
    }
    
    // Get the size of the list
    let list_size = env.call_method(field_values_list, "size", "()I", &[])
        .map_err(|e| format!("Failed to get list size: {}", e))?
        .i()
        .map_err(|e| format!("Failed to convert list size: {}", e))?;
    
    let mut terms = Vec::new();
    
    // Get the field type to determine how to convert values
    let field_entry = schema.get_field_entry(field);
    let field_type = field_entry.field_type();
    
    for i in 0..list_size {
        // Get the element at index i
        let element = env.call_method(field_values_list, "get", "(I)Ljava/lang/Object;", &[i.into()])
            .map_err(|e| format!("Failed to get list element: {}", e))?
            .l()
            .map_err(|e| format!("Failed to convert list element: {}", e))?;
        
        // Convert element to term based on field type
        let term = match field_type {
            tantivy::schema::FieldType::Str(_) => {
                // Handle string values
                let string_obj = env.call_method(&element, "toString", "()Ljava/lang/String;", &[])
                    .map_err(|_| "Failed to call toString on field value")?;
                let java_string = string_obj.l()
                    .map_err(|_| "Failed to get string object")?;
                let java_string_obj = JString::from(java_string);
                let rust_string = env.get_string(&java_string_obj)
                    .map_err(|_| "Failed to convert Java string to Rust string")?;
                let string_value: String = rust_string.into();
                Term::from_field_text(field, &string_value)
            },
            tantivy::schema::FieldType::I64(_) => {
                // Handle integer values
                if let Ok(true) = env.is_instance_of(&element, "java/lang/Long") {
                    let long_value = env.call_method(&element, "longValue", "()J", &[])
                        .map_err(|e| format!("Failed to get long value: {}", e))?
                        .j()
                        .map_err(|e| format!("Failed to convert long value: {}", e))?;
                    Term::from_field_i64(field, long_value)
                } else {
                    return Err("Expected Long value for integer field".to_string());
                }
            },
            tantivy::schema::FieldType::U64(_) => {
                // Handle unsigned values
                if let Ok(true) = env.is_instance_of(&element, "java/lang/Long") {
                    let long_value = env.call_method(&element, "longValue", "()J", &[])
                        .map_err(|e| format!("Failed to get long value: {}", e))?
                        .j()
                        .map_err(|e| format!("Failed to convert long value: {}", e))?;
                    if long_value < 0 {
                        return Err("Unsigned field cannot have negative values".to_string());
                    }
                    Term::from_field_u64(field, long_value as u64)
                } else {
                    return Err("Expected Long value for unsigned field".to_string());
                }
            },
            tantivy::schema::FieldType::F64(_) => {
                // Handle float values
                if let Ok(true) = env.is_instance_of(&element, "java/lang/Double") {
                    let double_value = env.call_method(&element, "doubleValue", "()D", &[])
                        .map_err(|e| format!("Failed to get double value: {}", e))?
                        .d()
                        .map_err(|e| format!("Failed to convert double value: {}", e))?;
                    Term::from_field_f64(field, double_value)
                } else {
                    return Err("Expected Double value for float field".to_string());
                }
            },
            tantivy::schema::FieldType::Bool(_) => {
                // Handle boolean values
                if let Ok(true) = env.is_instance_of(&element, "java/lang/Boolean") {
                    let bool_value = env.call_method(&element, "booleanValue", "()Z", &[])
                        .map_err(|e| format!("Failed to get boolean value: {}", e))?
                        .z()
                        .map_err(|e| format!("Failed to convert boolean value: {}", e))?;
                    Term::from_field_bool(field, bool_value)
                } else {
                    return Err("Expected Boolean value for boolean field".to_string());
                }
            },
            _ => {
                return Err(format!("Unsupported field type for TermSetQuery: {:?}", field_type));
            }
        };
        
        terms.push(term);
    }
    
    Ok(terms)
}

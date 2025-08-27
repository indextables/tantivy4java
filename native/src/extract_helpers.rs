use jni::JNIEnv;
use jni::objects::{JObject, JString};

// Helper function to extract long value from Java Object with simple type checking
pub fn extract_long_value(env: &mut JNIEnv, obj: &JObject) -> Result<i64, String> {
    if obj.is_null() {
        return Err("Object is null".to_string());
    }
    
    // Check if it's a Number first, then try longValue()
    let number_class = env.find_class("java/lang/Number");
    if let Ok(number_class) = number_class {
        if let Ok(true) = env.is_instance_of(obj, &number_class) {
            // It's a Number, so longValue() should work
            if let Ok(result) = env.call_method(obj, "longValue", "()J", &[]) {
                if let Ok(val) = result.j() {
                    return Ok(val);
                }
            }
        }
    }
    
    // Not a Number (e.g., String), try to convert to string and parse
    if let Ok(result) = env.call_method(obj, "toString", "()Ljava/lang/String;", &[]) {
        if let Ok(string_obj) = result.l() {
            if let Ok(java_str) = env.get_string(&JString::from(string_obj)) {
                let rust_str: String = java_str.into();
                if let Ok(val) = rust_str.parse::<i64>() {
                    return Ok(val);
                }
            }
        }
    }
    
    Err("Cannot convert object to long value".to_string())
}

// Helper function to extract double value from Java Object with simple type checking
pub fn extract_double_value(env: &mut JNIEnv, obj: &JObject) -> Result<f64, String> {
    if obj.is_null() {
        return Err("Object is null".to_string());
    }
    
    // Check if it's a Number first, then try doubleValue()
    let number_class = env.find_class("java/lang/Number");
    if let Ok(number_class) = number_class {
        if let Ok(true) = env.is_instance_of(obj, &number_class) {
            // It's a Number, so doubleValue() should work
            if let Ok(result) = env.call_method(obj, "doubleValue", "()D", &[]) {
                if let Ok(val) = result.d() {
                    return Ok(val);
                }
            }
        }
    }
    
    // Not a Number (e.g., String), try to convert to string and parse
    if let Ok(result) = env.call_method(obj, "toString", "()Ljava/lang/String;", &[]) {
        if let Ok(string_obj) = result.l() {
            if let Ok(java_str) = env.get_string(&JString::from(string_obj)) {
                let rust_str: String = java_str.into();
                if let Ok(val) = rust_str.parse::<f64>() {
                    return Ok(val);
                }
            }
        }
    }
    
    Err("Cannot convert object to double value".to_string())
}
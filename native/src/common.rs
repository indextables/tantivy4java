use jni::JNIEnv;
use anyhow::Result;

/// Helper function for consistent JNI error handling

/// Convert Rust error to Java exception
pub fn to_java_exception(env: &mut JNIEnv, error: &anyhow::Error) {
    let error_message = format!("{}", error);
    let _ = env.throw_new("java/lang/RuntimeException", error_message);
}
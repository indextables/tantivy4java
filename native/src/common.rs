use jni::JNIEnv;
use anyhow::Result;

/// Helper function for consistent JNI error handling
pub fn jni_result<T, F>(env: &mut JNIEnv, f: F) -> Result<T>
where
    F: FnOnce() -> Result<T>,
{
    match f() {
        Ok(result) => Ok(result),
        Err(e) => {
            to_java_exception(env, &e);
            Err(e)
        }
    }
}

/// Convert Rust error to Java exception
pub fn to_java_exception(env: &mut JNIEnv, error: &anyhow::Error) {
    let error_message = format!("{}", error);
    let _ = env.throw_new("java/lang/RuntimeException", error_message);
}
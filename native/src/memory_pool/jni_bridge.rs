// memory_pool/jni_bridge.rs - JNI functions for NativeMemoryManager Java class

use std::sync::Arc;

use jni::objects::{JClass, JObject};
use jni::sys::{jboolean, jlong, jobject, JNI_FALSE, JNI_TRUE};
use jni::JNIEnv;

use super::jvm_pool::{JvmMemoryPool, JvmPoolConfig};
use super::{set_global_pool, global_pool, is_pool_configured};
use crate::debug_println;

/// Java_io_indextables_tantivy4java_memory_NativeMemoryManager_nativeSetAccountant
///
/// Sets the global memory pool to a JVM-backed pool using the provided NativeMemoryAccountant.
/// Must be called before any native operations that use memory tracking.
///
/// Returns true if set successfully, false if already set.
#[no_mangle]
pub extern "system" fn Java_io_indextables_tantivy4java_memory_NativeMemoryManager_nativeSetAccountant(
    mut env: JNIEnv,
    _class: JClass,
    accountant: JObject,
    high_watermark: jni::sys::jdouble,
    low_watermark: jni::sys::jdouble,
    acquire_increment_bytes: jlong,
    min_release_bytes: jlong,
) -> jboolean {
    // Ensure JavaVM is captured for later JNI callbacks from JvmMemoryPool
    crate::utils::set_jvm(&env);

    if accountant.is_null() {
        debug_println!("MEMORY_POOL: nativeSetAccountant called with null accountant");
        return JNI_FALSE;
    }

    if is_pool_configured() {
        debug_println!("MEMORY_POOL: Pool already configured, ignoring set request");
        return JNI_FALSE;
    }

    // Create a global reference so the accountant isn't GC'd
    let global_ref = match env.new_global_ref(accountant) {
        Ok(r) => r,
        Err(e) => {
            debug_println!("MEMORY_POOL: Failed to create global ref: {}", e);
            return JNI_FALSE;
        }
    };

    let config = JvmPoolConfig {
        high_watermark: if high_watermark > 0.0 { high_watermark } else { 0.90 },
        low_watermark: if low_watermark > 0.0 { low_watermark } else { 0.25 },
        acquire_increment: if acquire_increment_bytes > 0 {
            acquire_increment_bytes as usize
        } else {
            64 * 1024 * 1024
        },
        min_release_amount: if min_release_bytes > 0 {
            min_release_bytes as usize
        } else {
            64 * 1024 * 1024
        },
    };

    debug_println!(
        "MEMORY_POOL: Creating JvmMemoryPool with config: {:?}",
        config
    );

    match JvmMemoryPool::new(&mut env, global_ref, config) {
        Ok(pool) => {
            match set_global_pool(Arc::new(pool)) {
                Ok(()) => {
                    debug_println!("MEMORY_POOL: Global JVM memory pool set successfully");
                    JNI_TRUE
                }
                Err(_) => {
                    debug_println!("MEMORY_POOL: Failed to set pool (already set)");
                    JNI_FALSE
                }
            }
        }
        Err(e) => {
            debug_println!("MEMORY_POOL: Failed to create JvmMemoryPool: {}", e);
            JNI_FALSE
        }
    }
}

/// Java_io_indextables_tantivy4java_memory_NativeMemoryManager_nativeGetUsedBytes
#[no_mangle]
pub extern "system" fn Java_io_indextables_tantivy4java_memory_NativeMemoryManager_nativeGetUsedBytes(
    _env: JNIEnv,
    _class: JClass,
) -> jlong {
    global_pool().used() as jlong
}

/// Java_io_indextables_tantivy4java_memory_NativeMemoryManager_nativeGetPeakBytes
#[no_mangle]
pub extern "system" fn Java_io_indextables_tantivy4java_memory_NativeMemoryManager_nativeGetPeakBytes(
    _env: JNIEnv,
    _class: JClass,
) -> jlong {
    global_pool().peak() as jlong
}

/// Java_io_indextables_tantivy4java_memory_NativeMemoryManager_nativeGetGrantedBytes
#[no_mangle]
pub extern "system" fn Java_io_indextables_tantivy4java_memory_NativeMemoryManager_nativeGetGrantedBytes(
    _env: JNIEnv,
    _class: JClass,
) -> jlong {
    let granted = global_pool().granted();
    if granted == usize::MAX {
        -1 // Signal unlimited
    } else {
        granted as jlong
    }
}

/// Java_io_indextables_tantivy4java_memory_NativeMemoryManager_nativeIsConfigured
#[no_mangle]
pub extern "system" fn Java_io_indextables_tantivy4java_memory_NativeMemoryManager_nativeIsConfigured(
    _env: JNIEnv,
    _class: JClass,
) -> jboolean {
    if is_pool_configured() {
        JNI_TRUE
    } else {
        JNI_FALSE
    }
}

/// Java_io_indextables_tantivy4java_memory_NativeMemoryManager_nativeResetPeak
///
/// Resets the peak usage counter to current usage. Returns the old peak value.
/// Useful for monitoring windows — call at the start of each window to track
/// per-window peak usage.
#[no_mangle]
pub extern "system" fn Java_io_indextables_tantivy4java_memory_NativeMemoryManager_nativeResetPeak(
    _env: JNIEnv,
    _class: JClass,
) -> jlong {
    let old_peak = global_pool().reset_peak();
    debug_println!(
        "📊 MEMORY_POOL: Peak reset (old peak: {} bytes, current used: {} bytes)",
        old_peak, global_pool().used()
    );
    old_peak as jlong
}

/// Java_io_indextables_tantivy4java_memory_NativeMemoryManager_nativeGetCategoryBreakdown
///
/// Returns a Java HashMap<String, Long> with per-category memory usage.
#[no_mangle]
pub extern "system" fn Java_io_indextables_tantivy4java_memory_NativeMemoryManager_nativeGetCategoryBreakdown(
    mut env: JNIEnv,
    _class: JClass,
) -> jobject {
    let breakdown = global_pool().category_breakdown();

    // Create Java HashMap
    let hashmap_class = match env.find_class("java/util/HashMap") {
        Ok(c) => c,
        Err(_) => return std::ptr::null_mut(),
    };
    let hashmap = match env.new_object(&hashmap_class, "()V", &[]) {
        Ok(m) => m,
        Err(_) => return std::ptr::null_mut(),
    };

    for (category, bytes) in &breakdown {
        let key = match env.new_string(category) {
            Ok(k) => k,
            Err(_) => continue,
        };
        let value = match env.new_object(
            "java/lang/Long",
            "(J)V",
            &[jni::objects::JValue::Long(*bytes as i64)],
        ) {
            Ok(v) => v,
            Err(_) => continue,
        };

        let _ = env.call_method(
            &hashmap,
            "put",
            "(Ljava/lang/Object;Ljava/lang/Object;)Ljava/lang/Object;",
            &[(&key).into(), (&value).into()],
        );
    }

    hashmap.into_raw()
}

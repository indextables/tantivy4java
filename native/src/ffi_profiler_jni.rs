// ffi_profiler_jni.rs — JNI bridge for the FFI read-path profiler.
//
// Exposes 6 native methods on io.indextables.tantivy4java.split.FfiProfiler:
//   - nativeProfilerEnable()          → void   (auto-resets counters)
//   - nativeProfilerDisable()         → void
//   - nativeProfilerSnapshot()        → long[] (section data, 4 per section)
//   - nativeProfilerReset()           → long[] (pre-reset section data)
//   - nativeProfilerCacheSnapshot()   → long[] (cache counter values)
//   - nativeProfilerCacheReset()      → long[] (pre-reset cache counters)

use jni::objects::JClass;
use jni::sys::{jlongArray, jboolean};
use jni::JNIEnv;

use crate::ffi_profiler;

/// Enable profiling (auto-resets all counters for a clean start).
#[no_mangle]
pub extern "system" fn Java_io_indextables_tantivy4java_split_FfiProfiler_nativeProfilerEnable(
    _env: JNIEnv,
    _class: JClass,
) {
    ffi_profiler::enable();
}

/// Disable profiling.
#[no_mangle]
pub extern "system" fn Java_io_indextables_tantivy4java_split_FfiProfiler_nativeProfilerDisable(
    _env: JNIEnv,
    _class: JClass,
) {
    ffi_profiler::disable();
}

/// Check if profiling is enabled.
#[no_mangle]
pub extern "system" fn Java_io_indextables_tantivy4java_split_FfiProfiler_nativeProfilerIsEnabled(
    _env: JNIEnv,
    _class: JClass,
) -> jboolean {
    if ffi_profiler::is_enabled() { 1 } else { 0 }
}

/// Snapshot section counters as a flat long array.
/// Layout: [count0, totalNanos0, minNanos0, maxNanos0, count1, ...] — 4 values per section.
#[no_mangle]
pub extern "system" fn Java_io_indextables_tantivy4java_split_FfiProfiler_nativeProfilerSnapshot(
    mut env: JNIEnv,
    _class: JClass,
) -> jlongArray {
    let flat = ffi_profiler::snapshot_flat();
    match env.new_long_array(flat.len() as i32) {
        Ok(arr) => {
            if env.set_long_array_region(&arr, 0, &flat).is_ok() {
                arr.into_raw()
            } else {
                std::ptr::null_mut()
            }
        }
        Err(_) => std::ptr::null_mut(),
    }
}

/// Reset section counters and return pre-reset values as a flat long array.
/// Same layout as snapshot.
#[no_mangle]
pub extern "system" fn Java_io_indextables_tantivy4java_split_FfiProfiler_nativeProfilerReset(
    mut env: JNIEnv,
    _class: JClass,
) -> jlongArray {
    let flat = ffi_profiler::reset_sections_flat();
    match env.new_long_array(flat.len() as i32) {
        Ok(arr) => {
            if env.set_long_array_region(&arr, 0, &flat).is_ok() {
                arr.into_raw()
            } else {
                std::ptr::null_mut()
            }
        }
        Err(_) => std::ptr::null_mut(),
    }
}

/// Snapshot cache counters as a flat long array.
/// Layout: [counter0, counter1, ...] — 1 value per CacheCounter.
#[no_mangle]
pub extern "system" fn Java_io_indextables_tantivy4java_split_FfiProfiler_nativeProfilerCacheSnapshot(
    mut env: JNIEnv,
    _class: JClass,
) -> jlongArray {
    let flat = ffi_profiler::cache_snapshot_flat();
    match env.new_long_array(flat.len() as i32) {
        Ok(arr) => {
            if env.set_long_array_region(&arr, 0, &flat).is_ok() {
                arr.into_raw()
            } else {
                std::ptr::null_mut()
            }
        }
        Err(_) => std::ptr::null_mut(),
    }
}

/// Reset cache counters and return pre-reset values as a flat long array.
#[no_mangle]
pub extern "system" fn Java_io_indextables_tantivy4java_split_FfiProfiler_nativeProfilerCacheReset(
    mut env: JNIEnv,
    _class: JClass,
) -> jlongArray {
    let flat = ffi_profiler::cache_reset_flat();
    match env.new_long_array(flat.len() as i32) {
        Ok(arr) => {
            if env.set_long_array_region(&arr, 0, &flat).is_ok() {
                arr.into_raw()
            } else {
                std::ptr::null_mut()
            }
        }
        Err(_) => std::ptr::null_mut(),
    }
}

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

use jni::JNIEnv;
use jni::sys::jlong;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::sync::atomic::{AtomicU64, Ordering};
use once_cell::sync::Lazy;

/// Global registry for native objects
pub static OBJECT_REGISTRY: Lazy<Mutex<HashMap<u64, Box<dyn std::any::Any + Send + Sync>>>> = 
    Lazy::new(|| Mutex::new(HashMap::new()));

/// Thread-safe atomic counter for generating unique IDs
static NEXT_ID: AtomicU64 = AtomicU64::new(1);

/// Register a native object and return its handle
pub fn register_object<T: 'static + Send + Sync>(obj: T) -> u64 {
    let mut registry = OBJECT_REGISTRY.lock().unwrap();

    let id = NEXT_ID.fetch_add(1, Ordering::SeqCst);
    registry.insert(id, Box::new(obj));
    id
}

/// Execute a function with a reference to a registered object
pub fn with_object<T: 'static, R>(id: u64, f: impl FnOnce(&T) -> R) -> Option<R> {
    let registry = OBJECT_REGISTRY.lock().unwrap();
    let boxed = registry.get(&id)?;
    let obj = boxed.downcast_ref::<T>()?;
    Some(f(obj))
}

/// Execute a function with a mutable reference to a registered object
pub fn with_object_mut<T: 'static, R>(id: u64, f: impl FnOnce(&mut T) -> R) -> Option<R> {
    let mut registry = OBJECT_REGISTRY.lock().unwrap();
    let boxed = registry.get_mut(&id)?;
    // Safe downcast using Any trait - this is the correct way to do dynamic casting
    let obj = boxed.downcast_mut::<T>()?;
    Some(f(obj))
}

/// Remove an object from the registry
pub fn remove_object(id: u64) -> bool {
    let mut registry = OBJECT_REGISTRY.lock().unwrap();
    registry.remove(&id).is_some()
}

/// Handle errors by throwing Java exceptions
pub fn handle_error(env: &mut JNIEnv, error: &str) {
    let _ = env.throw_new("java/lang/RuntimeException", error);
}

/// Arc-based memory management utilities for preventing crashes

/// Convert Arc to jlong pointer for safe sharing with Java
pub fn arc_to_jlong<T: Send + Sync + 'static>(arc: Arc<T>) -> jlong {
    Box::into_raw(Box::new(arc)) as jlong
}

/// Convert jlong pointer back to Arc reference for safe access
pub fn jlong_to_arc<T: Send + Sync + 'static>(ptr: jlong) -> Option<Arc<T>> {
    eprintln!("RUST DEBUG: jlong_to_arc called with pointer: {}", ptr);
    if ptr == 0 {
        eprintln!("RUST DEBUG: jlong_to_arc - pointer is 0, returning None");
        return None;
    }
    unsafe {
        eprintln!("RUST DEBUG: jlong_to_arc - attempting to convert pointer {} to Box<Arc<T>>", ptr);
        let boxed = Box::from_raw(ptr as *mut Arc<T>);
        let arc = (*boxed).clone();
        Box::into_raw(boxed); // Don't drop the box, keep the pointer alive
        eprintln!("RUST DEBUG: jlong_to_arc - successfully converted pointer {}", ptr);
        Some(arc)
    }
}

/// Execute function with Arc reference - safe memory access
pub fn with_arc_safe<T: Send + Sync + 'static, R>(ptr: jlong, f: impl FnOnce(&Arc<T>) -> R) -> Option<R> {
    let arc = jlong_to_arc(ptr)?;
    Some(f(&arc))
}

/// Convert Rust Result to Java, throwing exception on error
pub fn unwrap_or_throw<T>(env: &mut JNIEnv, result: Result<T, Box<dyn std::error::Error>>) -> Option<T> {
    match result {
        Ok(value) => Some(value),
        Err(error) => {
            handle_error(env, &error.to_string());
            None
        }
    }
}

/// Convert JString to Rust String
pub fn jstring_to_string(env: &mut JNIEnv, jstr: &jni::objects::JString) -> anyhow::Result<String> {
    Ok(env.get_string(jstr)?.into())
}

/// Convert Rust String to JString
pub fn string_to_jstring<'a>(env: &mut JNIEnv<'a>, s: &str) -> anyhow::Result<jni::objects::JString<'a>> {
    Ok(env.new_string(s)?)
}

/// Execute function and convert any errors to Java exceptions
pub fn convert_throwable<T, F>(env: &mut JNIEnv, f: F) -> anyhow::Result<T>
where
    F: FnOnce(&mut JNIEnv) -> anyhow::Result<T>,
{
    match f(env) {
        Ok(value) => Ok(value),
        Err(error) => {
            let _ = env.throw_new("java/lang/RuntimeException", error.to_string());
            Err(error)
        }
    }
}

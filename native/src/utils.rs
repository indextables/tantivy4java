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
use crate::debug_println;

/// Handle errors by throwing Java exceptions
pub fn handle_error(env: &mut JNIEnv, error: &str) {
    let _ = env.throw_new("java/lang/RuntimeException", error);
}

/// SAFE Arc-based memory management utilities using registry pattern

/// Global registry for Arc objects - prevents memory safety issues
pub static ARC_REGISTRY: Lazy<Mutex<HashMap<jlong, Box<dyn std::any::Any + Send + Sync>>>> = 
    Lazy::new(|| Mutex::new(HashMap::new()));

/// Thread-safe atomic counter for generating unique Arc IDs
static ARC_NEXT_ID: AtomicU64 = AtomicU64::new(1);

/// SAFE: Register Arc in global registry and return handle
pub fn arc_to_jlong<T: Send + Sync + 'static>(arc: Arc<T>) -> jlong {
    let mut registry = ARC_REGISTRY.lock().unwrap();
    let id = ARC_NEXT_ID.fetch_add(1, Ordering::SeqCst) as jlong;
    
    // Store the Arc in the registry instead of using unsafe pointer manipulation
    registry.insert(id, Box::new(arc));
    
    
    id
}

/// SAFE: Access Arc from registry without unsafe operations
pub fn jlong_to_arc<T: Send + Sync + 'static>(ptr: jlong) -> Option<Arc<T>> {
    if ptr == 0 {
        debug_println!("RUST DEBUG: jlong_to_arc - ID is 0, returning None");
        return None;
    }
    
    let registry = ARC_REGISTRY.lock().unwrap();
    let boxed = registry.get(&ptr)?;
    let arc = boxed.downcast_ref::<Arc<T>>()?;
    
    
    Some(arc.clone())
}

/// SAFE: Execute function with Arc reference from registry
pub fn with_arc_safe<T: Send + Sync + 'static, R>(ptr: jlong, f: impl FnOnce(&Arc<T>) -> R) -> Option<R> {
    let arc = jlong_to_arc(ptr)?;
    Some(f(&arc))
}

/// SAFE: Remove Arc from registry to prevent memory leaks
pub fn release_arc(ptr: jlong) {
    if ptr != 0 {
        let mut registry = ARC_REGISTRY.lock().unwrap();
        if registry.remove(&ptr).is_some() {
            debug_println!("RUST DEBUG: release_arc - removed Arc with ID: {}", ptr);
        } else {
            debug_println!("RUST DEBUG: release_arc - Arc with ID {} not found in registry", ptr);
        }
    }
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

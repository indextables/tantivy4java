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
use std::collections::HashMap;
use std::sync::Mutex;
use once_cell::sync::Lazy;

/// Global registry for native objects
pub static OBJECT_REGISTRY: Lazy<Mutex<HashMap<u64, Box<dyn std::any::Any + Send + Sync>>>> = 
    Lazy::new(|| Mutex::new(HashMap::new()));

static mut NEXT_ID: u64 = 1;

/// Register a native object and return its handle
pub fn register_object<T: 'static + Send + Sync>(obj: T) -> u64 {
    unsafe {
        let mut registry = OBJECT_REGISTRY.lock().unwrap();

        let id = NEXT_ID;
        NEXT_ID += 1;

        registry.insert(id, Box::new(obj));
        id
    }
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
    // This is safe because we know T implements Send + Sync
    let obj = unsafe { 
        let ptr = boxed.as_mut() as *mut dyn std::any::Any as *mut T;
        &mut *ptr
    };
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

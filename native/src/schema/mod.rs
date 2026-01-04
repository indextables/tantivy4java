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

//! Schema module - SchemaBuilder and Schema JNI bindings
//!
//! This module is organized into:
//! - `jni_builder` - SchemaBuilder JNI functions (field creation, build)
//! - `jni_schema` - Schema JNI functions (introspection, field info)

mod jni_builder;
mod jni_schema;

// Re-export all JNI functions so they're visible at the crate root
pub use jni_builder::*;
pub use jni_schema::*;

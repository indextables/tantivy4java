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

use jni::objects::JClass;
use jni::sys::jstring;
use jni::JNIEnv;

mod schema;
mod document;
mod query;
mod index;
mod searcher;
mod doc_address;
mod utils;
mod quickwit_split;
mod split_searcher;  // Now using proper Quickwit integration
// mod split_searcher_simple;  // Disabled to avoid conflicts
mod common;

pub use schema::*;
pub use document::*;
pub use query::*;
pub use index::*;
pub use searcher::*;
pub use doc_address::*;
pub use utils::*;
pub use quickwit_split::*;
pub use split_searcher::*;  // Now enabled with proper Quickwit integration
// pub use split_searcher_simple::*;  // Disabled to avoid conflicts

#[no_mangle]
pub extern "system" fn Java_com_tantivy4java_Tantivy_getVersion(
    env: JNIEnv,
    _class: JClass,
) -> jstring {
    let version = env.new_string("0.24.0").unwrap();
    version.into_raw()
}
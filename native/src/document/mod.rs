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

//! Document Module
//!
//! This module provides document creation and manipulation functionality,
//! supporting both document building (for indexing) and document retrieval
//! (from search results).
//!
//! ## Module Structure
//!
//! - `types` - Core document types (DocumentWrapper, DocumentBuilder, RetrievedDocument)
//! - `jni_getters` - JNI functions for document creation and field retrieval
//! - `jni_add_fields` - JNI functions for adding field values to documents
//! - `helpers` - Helper functions for date conversion and document creation

mod helpers;
mod jni_add_fields;
mod jni_getters;
mod types;

// Re-export public types
pub use types::{DocumentBuilder, DocumentWrapper, RetrievedDocument};

// Re-export helper functions used by other modules
pub use helpers::{convert_java_localdatetime_to_tantivy, create_java_document_from_retrieved};

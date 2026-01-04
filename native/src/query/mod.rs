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

// Submodules - organized by functionality
pub mod wildcard;        // Wildcard query pattern processing
pub mod extraction;      // Java value extraction helpers
pub mod snippet;         // SnippetGenerator and Snippet JNI
pub mod json_query;      // JSON field query JNI
pub mod jni_core;        // Core Query JNI (term, fuzzy, phrase, boolean)
pub mod jni_advanced;    // Advanced Query JNI (regex, wildcard, range, utility)

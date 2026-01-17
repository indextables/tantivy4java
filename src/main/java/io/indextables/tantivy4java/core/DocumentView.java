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

package io.indextables.tantivy4java.core;

import java.util.List;
import java.util.Map;

/**
 * Read-only interface for accessing document field values.
 *
 * This interface allows code to work with both native-backed Document objects
 * and pure Java MapBackedDocument objects without JNI overhead.
 *
 * Implementations:
 * - Document: Native-backed, uses JNI for field access
 * - MapBackedDocument: Pure Java, zero JNI calls
 */
public interface DocumentView extends AutoCloseable {

    /**
     * Get all values for a field.
     *
     * @param fieldName The field name
     * @return List of values (empty list if field not found)
     */
    List<Object> get(String fieldName);

    /**
     * Get the first value for a field.
     *
     * @param fieldName The field name
     * @return First value or null if field not found or empty
     */
    Object getFirst(String fieldName);

    /**
     * Get all values for a field (alias for get()).
     *
     * @param fieldName The field name
     * @return List of values (empty list if field not found)
     */
    List<Object> getAll(String fieldName);

    /**
     * Convert to a map representation.
     *
     * @return Map of field names to list of values
     */
    Map<String, List<Object>> toMap();

    /**
     * Get the number of fields in the document.
     *
     * @return Number of fields
     */
    int getNumFields();

    /**
     * Check if the document is empty.
     *
     * @return true if the document has no fields
     */
    boolean isEmpty();

    /**
     * Close the document and release any resources.
     */
    @Override
    void close();
}

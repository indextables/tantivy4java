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

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * A pure Java Document implementation backed by a Map.
 *
 * <p>This class extends {@link Document} to provide the same interface but without any JNI calls
 * for read operations. It is used by the byte buffer protocol to avoid the overhead of creating
 * native-backed Document objects and then immediately reading values back out.
 *
 * <p><b>Performance benefit:</b> Eliminates 2×N×M JNI calls (N=documents, M=fields) when
 * processing documents retrieved via the byte buffer protocol.
 *
 * <p><b>Backwards compatible:</b> Since this extends Document, methods returning List&lt;Document&gt;
 * can include MapBackedDocument instances without any API changes for consumers.
 *
 * <p><b>Read-only:</b> This class only supports read operations. Write operations (addText, etc.)
 * will throw UnsupportedOperationException.
 */
public class MapBackedDocument extends Document {

    private final Map<String, List<Object>> fields;

    /**
     * Create a MapBackedDocument from a map of field values.
     * Values can be single objects or Lists. Single objects are wrapped in a List.
     *
     * @param fieldMap Map of field names to values (single object or List)
     */
    @SuppressWarnings("unchecked")
    public MapBackedDocument(Map<String, Object> fieldMap) {
        super(0L);  // No native pointer - pure Java implementation
        this.fields = new HashMap<>();
        for (Map.Entry<String, Object> entry : fieldMap.entrySet()) {
            String fieldName = entry.getKey();
            Object value = entry.getValue();
            if (value instanceof List) {
                this.fields.put(fieldName, (List<Object>) value);
            } else if (value != null) {
                List<Object> list = new ArrayList<>(1);
                list.add(value);
                this.fields.put(fieldName, list);
            } else {
                this.fields.put(fieldName, Collections.emptyList());
            }
        }
    }

    /**
     * Get all values for a field.
     * Pure Java implementation - no JNI call.
     *
     * @param fieldName The field name
     * @return List of values (empty list if field not found)
     */
    @Override
    public List<Object> get(String fieldName) {
        List<Object> values = fields.get(fieldName);
        return values != null ? values : Collections.emptyList();
    }

    /**
     * Get the first value for a field.
     * Pure Java implementation - no JNI call.
     *
     * @param fieldName The field name
     * @return First value or null if field not found or empty
     */
    @Override
    public Object getFirst(String fieldName) {
        List<Object> values = fields.get(fieldName);
        if (values == null || values.isEmpty()) {
            return null;
        }
        return values.get(0);
    }

    /**
     * Get all values for a field (alias for get()).
     * Pure Java implementation - no JNI call.
     *
     * @param fieldName The field name
     * @return List of values (empty list if field not found)
     */
    @Override
    public List<Object> getAll(String fieldName) {
        return get(fieldName);
    }

    /**
     * Convert to a map representation.
     * Pure Java implementation - no JNI call.
     *
     * @return Map of field names to list of values
     */
    @Override
    public Map<String, List<Object>> toMap() {
        return new HashMap<>(fields);
    }

    /**
     * Get the number of fields in the document.
     * Pure Java implementation - no JNI call.
     *
     * @return Number of fields
     */
    @Override
    public int getNumFields() {
        return fields.size();
    }

    /**
     * Check if the document is empty.
     * Pure Java implementation - no JNI call.
     *
     * @return true if the document has no fields
     */
    @Override
    public boolean isEmpty() {
        return fields.isEmpty();
    }

    /**
     * Check if the document contains a field.
     *
     * @param fieldName The field name
     * @return true if the field exists
     */
    public boolean hasField(String fieldName) {
        return fields.containsKey(fieldName);
    }

    /**
     * Get all field names.
     *
     * @return Set of field names
     */
    public java.util.Set<String> getFieldNames() {
        return fields.keySet();
    }

    /**
     * No-op close - no native resources to release.
     */
    @Override
    public void close() {
        // No-op: no native resources to release
        // Don't call super.close() since we have no native pointer
    }

    // ========== Write operations - not supported ==========

    @Override
    public void addText(String fieldName, String text) {
        throw new UnsupportedOperationException("MapBackedDocument is read-only");
    }

    @Override
    public void addUnsigned(String fieldName, long value) {
        throw new UnsupportedOperationException("MapBackedDocument is read-only");
    }

    @Override
    public void addInteger(String fieldName, long value) {
        throw new UnsupportedOperationException("MapBackedDocument is read-only");
    }

    @Override
    public void addFloat(String fieldName, double value) {
        throw new UnsupportedOperationException("MapBackedDocument is read-only");
    }

    @Override
    public void addBoolean(String fieldName, boolean value) {
        throw new UnsupportedOperationException("MapBackedDocument is read-only");
    }

    @Override
    public void addDate(String fieldName, java.time.LocalDateTime date) {
        throw new UnsupportedOperationException("MapBackedDocument is read-only");
    }

    @Override
    public void addBytes(String fieldName, byte[] bytes) {
        throw new UnsupportedOperationException("MapBackedDocument is read-only");
    }

    @Override
    public void addIpAddr(String fieldName, String ipAddr) {
        throw new UnsupportedOperationException("MapBackedDocument is read-only");
    }

    @Override
    public void addString(String fieldName, String value) {
        throw new UnsupportedOperationException("MapBackedDocument is read-only");
    }

    @Override
    public String toString() {
        return "MapBackedDocument{fields=" + fields + "}";
    }
}

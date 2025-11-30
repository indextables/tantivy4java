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

import io.indextables.tantivy4java.util.Facet;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;

/**
 * Represents a document in a Tantivy index.
 * Documents are collections of field values that can be indexed and searched.
 */
public class Document implements AutoCloseable {
    static {
        Tantivy.initialize();
    }

    // PERFORMANCE FIX: Static, pre-configured ObjectMapper for JSON serialization
    // ObjectMapper is thread-safe and expensive to create, so we reuse a single instance
    private static final com.fasterxml.jackson.databind.ObjectMapper JSON_MAPPER;

    static {
        JSON_MAPPER = new com.fasterxml.jackson.databind.ObjectMapper();
        JSON_MAPPER.setDateFormat(new java.text.SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'"));
        // Try to register JavaTimeModule for java.time types if available
        try {
            Class<?> moduleClass = Class.forName("com.fasterxml.jackson.datatype.jsr310.JavaTimeModule");
            Object module = moduleClass.getDeclaredConstructor().newInstance();
            JSON_MAPPER.registerModule((com.fasterxml.jackson.databind.Module) module);
        } catch (ClassNotFoundException e) {
            // JavaTimeModule not available - continue without it
        } catch (Exception e) {
            // Other reflection errors - continue without the module
        }
    }

    private long nativePtr;
    private boolean closed = false;

    /**
     * Create a new empty document.
     */
    public Document() {
        this.nativePtr = nativeNew();
    }

    /**
     * Create a document from a map of field values.
     * @param fields Map of field names to values
     * @param schema Optional schema for validation
     * @return New document instance
     */
    public static Document fromMap(Map<String, Object> fields, Schema schema) {
        long ptr = nativeFromMap(fields, schema != null ? schema.getNativePtr() : 0);
        return new Document(ptr);
    }

    /**
     * Create a document from a map of field values without schema validation.
     * @param fields Map of field names to values
     * @return New document instance
     */
    public static Document fromMap(Map<String, Object> fields) {
        return fromMap(fields, null);
    }

    public Document(long nativePtr) {
        this.nativePtr = nativePtr;
    }

    /**
     * Get all values for a field.
     * @param fieldName Field name
     * @return List of values for the field
     */
    public List<Object> get(String fieldName) {
        if (closed) {
            throw new IllegalStateException("Document has been closed");
        }
        return nativeGet(nativePtr, fieldName);
    }

    /**
     * Get the first value for a field.
     * @param fieldName Field name
     * @return First value for the field, or null if not found
     */
    public Object getFirst(String fieldName) {
        List<Object> values = get(fieldName);
        return values.isEmpty() ? null : values.get(0);
    }

    /**
     * Get all values for all fields.
     * @return Map of field names to lists of values
     */
    public Map<String, List<Object>> toMap() {
        if (closed) {
            throw new IllegalStateException("Document has been closed");
        }
        return nativeToMap(nativePtr);
    }

    /**
     * Extend this document with values from a map.
     * @param fields Map of field names to values
     * @param schema Optional schema for validation
     */
    public void extend(Map<String, Object> fields, Schema schema) {
        if (closed) {
            throw new IllegalStateException("Document has been closed");
        }
        nativeExtend(nativePtr, fields, schema != null ? schema.getNativePtr() : 0);
    }

    /**
     * Extend this document with values from a map without schema validation.
     * @param fields Map of field names to values
     */
    public void extend(Map<String, Object> fields) {
        extend(fields, null);
    }

    /**
     * Add a text value to a field.
     * @param fieldName Field name
     * @param text Text value
     */
    public void addText(String fieldName, String text) {
        if (closed) {
            throw new IllegalStateException("Document has been closed");
        }
        nativeAddText(nativePtr, fieldName, text);
    }

    /**
     * Add an unsigned integer value to a field.
     * @param fieldName Field name
     * @param value Unsigned integer value
     */
    public void addUnsigned(String fieldName, long value) {
        if (closed) {
            throw new IllegalStateException("Document has been closed");
        }
        nativeAddUnsigned(nativePtr, fieldName, value);
    }

    /**
     * Add an integer value to a field.
     * @param fieldName Field name
     * @param value Integer value
     */
    public void addInteger(String fieldName, long value) {
        if (closed) {
            throw new IllegalStateException("Document has been closed");
        }
        nativeAddInteger(nativePtr, fieldName, value);
    }

    /**
     * Add a float value to a field.
     * @param fieldName Field name
     * @param value Float value
     */
    public void addFloat(String fieldName, double value) {
        if (closed) {
            throw new IllegalStateException("Document has been closed");
        }
        nativeAddFloat(nativePtr, fieldName, value);
    }

    /**
     * Add a boolean value to a field.
     * @param fieldName Field name
     * @param value Boolean value
     */
    public void addBoolean(String fieldName, boolean value) {
        if (closed) {
            throw new IllegalStateException("Document has been closed");
        }
        nativeAddBoolean(nativePtr, fieldName, value);
    }

    /**
     * Add a date value to a field.
     * @param fieldName Field name
     * @param date Date value
     */
    public void addDate(String fieldName, LocalDateTime date) {
        if (closed) {
            throw new IllegalStateException("Document has been closed");
        }
        nativeAddDate(nativePtr, fieldName, date);
    }

    /**
     * Add a facet value to a field.
     * @param fieldName Field name
     * @param facet Facet value
     */
    public void addFacet(String fieldName, Facet facet) {
        if (closed) {
            throw new IllegalStateException("Document has been closed");
        }
        nativeAddFacet(nativePtr, fieldName, facet.getNativePtr());
    }

    /**
     * Add a bytes value to a field.
     * @param fieldName Field name
     * @param bytes Byte array value
     */
    public void addBytes(String fieldName, byte[] bytes) {
        if (closed) {
            throw new IllegalStateException("Document has been closed");
        }
        nativeAddBytes(nativePtr, fieldName, bytes);
    }

    /**
     * Add JSON value from string to a JSON field.
     *
     * <p>The JSON string must be valid according to RFC 8259.
     * All nested values will be indexed according to field configuration.
     *
     * <p>Example:
     * <pre>
     * doc.addJson(field, "{\"user\": {\"name\": \"John\", \"age\": 30}}");
     * </pre>
     *
     * @param field JSON field handle
     * @param jsonString Valid JSON string
     * @throws IllegalArgumentException if JSON is invalid
     * @throws IllegalStateException if document has been closed
     */
    public void addJson(Field field, String jsonString) {
        if (closed) {
            throw new IllegalStateException("Document has been closed");
        }
        if (field == null) {
            throw new IllegalArgumentException("Field cannot be null");
        }
        if (jsonString == null) {
            throw new IllegalArgumentException("JSON string cannot be null");
        }
        nativeAddJsonFromString(nativePtr, field.getName(), jsonString);
    }

    /**
     * Add JSON value from Java object to a JSON field.
     *
     * <p>Automatically serializes the object to JSON. Supported types:
     * <ul>
     *   <li>Map&lt;String, Object&gt; → JSON object</li>
     *   <li>List&lt;Object&gt; → JSON array</li>
     *   <li>String, Integer, Long, Float, Double, Boolean → JSON primitives</li>
     *   <li>java.time.Instant, java.util.Date → ISO 8601 date string</li>
     * </ul>
     *
     * <p>Example:
     * <pre>
     * Map&lt;String, Object&gt; data = new HashMap&lt;&gt;();
     * data.put("name", "John");
     * data.put("age", 30);
     * doc.addJson(field, data);
     * </pre>
     *
     * @param field JSON field handle
     * @param value Java object to serialize
     * @throws IllegalArgumentException if object cannot be serialized
     * @throws IllegalStateException if document has been closed
     */
    public void addJson(Field field, Object value) {
        if (closed) {
            throw new IllegalStateException("Document has been closed");
        }
        if (field == null) {
            throw new IllegalArgumentException("Field cannot be null");
        }
        if (value == null) {
            throw new IllegalArgumentException("Value cannot be null");
        }

        // Serialize to JSON string
        String jsonString = serializeToJson(value);
        addJson(field, jsonString);
    }

    /**
     * Add a JSON value to a field using field name.
     *
     * @deprecated Use {@link #addJson(Field, Object)} for better type safety
     *
     * @param fieldName Field name
     * @param value Object to be serialized as JSON
     */
    @Deprecated
    public void addJson(String fieldName, Object value) {
        if (closed) {
            throw new IllegalStateException("Document has been closed");
        }
        String jsonString = convertToJsonString(value);
        nativeAddJson(nativePtr, fieldName, jsonString);
    }

    /**
     * Serialize Java object to JSON string using proper JSON library.
     *
     * <p>Uses Jackson for robust JSON serialization with proper
     * date formatting and type handling. Uses a static pre-configured
     * ObjectMapper for performance (ObjectMapper is thread-safe).
     *
     * @param value Object to serialize
     * @return JSON string representation
     */
    private String serializeToJson(Object value) {
        try {
            // PERFORMANCE FIX: Use static JSON_MAPPER instead of creating new instance per call
            return JSON_MAPPER.writeValueAsString(value);
        } catch (Exception e) {
            throw new IllegalArgumentException("Failed to serialize object to JSON: " + e.getMessage(), e);
        }
    }

    /**
     * Simple JSON conversion for basic objects (fallback for deprecated method).
     *
     * @deprecated Internal method for deprecated addJson(String, Object)
     */
    @Deprecated
    private String convertToJsonString(Object value) {
        if (value == null) {
            return "null";
        }
        if (value instanceof String) {
            return "\"" + ((String) value).replace("\"", "\\\"") + "\"";
        }
        if (value instanceof Number || value instanceof Boolean) {
            return value.toString();
        }
        if (value instanceof java.util.Map) {
            @SuppressWarnings("unchecked")
            java.util.Map<String, Object> map = (java.util.Map<String, Object>) value;
            StringBuilder json = new StringBuilder("{");
            boolean first = true;
            for (java.util.Map.Entry<String, Object> entry : map.entrySet()) {
                if (!first) json.append(",");
                first = false;
                json.append("\"").append(entry.getKey()).append("\":");
                json.append(convertToJsonString(entry.getValue()));
            }
            json.append("}");
            return json.toString();
        }
        if (value instanceof java.util.List) {
            @SuppressWarnings("unchecked")
            java.util.List<Object> list = (java.util.List<Object>) value;
            StringBuilder json = new StringBuilder("[");
            boolean first = true;
            for (Object item : list) {
                if (!first) json.append(",");
                first = false;
                json.append(convertToJsonString(item));
            }
            json.append("]");
            return json.toString();
        }
        // Fallback: convert to string and quote it
        return "\"" + value.toString().replace("\"", "\\\"") + "\"";
    }

    /**
     * Add an IP address value to a field.
     * @param fieldName Field name
     * @param ipAddr IP address as string
     */
    public void addIpAddr(String fieldName, String ipAddr) {
        if (closed) {
            throw new IllegalStateException("Document has been closed");
        }
        nativeAddIpAddr(nativePtr, fieldName, ipAddr);
    }

    /**
     * Add a string value to a field.
     * String fields are for exact string matching (not tokenized like text fields).
     * @param fieldName Field name
     * @param value String value
     */
    public void addString(String fieldName, String value) {
        if (closed) {
            throw new IllegalStateException("Document has been closed");
        }
        nativeAddString(nativePtr, fieldName, value);
    }

    /**
     * Get the number of fields in this document.
     * @return Number of fields
     */
    public int getNumFields() {
        if (closed) {
            throw new IllegalStateException("Document has been closed");
        }
        return nativeGetNumFields(nativePtr);
    }

    /**
     * Check if this document is empty.
     * @return true if the document has no fields, false otherwise
     */
    public boolean isEmpty() {
        if (closed) {
            throw new IllegalStateException("Document has been closed");
        }
        return nativeIsEmpty(nativePtr);
    }

    /**
     * Get all values for a field.
     * @param fieldName Field name
     * @return List of all values for the field
     */
    public List<Object> getAll(String fieldName) {
        return get(fieldName);
    }

    /**
     * Get the native pointer for JNI operations.
     * @return Native pointer
     */
    public long getNativePtr() {
        if (closed) {
            throw new IllegalStateException("Document has been closed");
        }
        return nativePtr;
    }

    @Override
    public void close() {
        if (!closed) {
            nativeClose(nativePtr);
            closed = true;
            nativePtr = 0;
        }
    }


    // Native method declarations
    private static native long nativeNew();
    private static native long nativeFromMap(Map<String, Object> fields, long schemaPtr);
    private static native List<Object> nativeGet(long ptr, String fieldName);
    private static native Map<String, List<Object>> nativeToMap(long ptr);
    private static native void nativeExtend(long ptr, Map<String, Object> fields, long schemaPtr);
    private static native void nativeAddText(long ptr, String fieldName, String text);
    private static native void nativeAddUnsigned(long ptr, String fieldName, long value);
    private static native void nativeAddInteger(long ptr, String fieldName, long value);
    private static native void nativeAddFloat(long ptr, String fieldName, double value);
    private static native void nativeAddBoolean(long ptr, String fieldName, boolean value);
    private static native void nativeAddDate(long ptr, String fieldName, LocalDateTime date);
    private static native void nativeAddFacet(long ptr, String fieldName, long facetPtr);
    private static native void nativeAddBytes(long ptr, String fieldName, byte[] bytes);
    private static native void nativeAddJson(long ptr, String fieldName, String jsonString);
    private static native void nativeAddJsonFromString(long ptr, String fieldName, String jsonString);
    private static native void nativeAddIpAddr(long ptr, String fieldName, String ipAddr);
    private static native void nativeAddString(long ptr, String fieldName, String value);
    private static native int nativeGetNumFields(long ptr);
    private static native boolean nativeIsEmpty(long ptr);
    private static native void nativeClose(long ptr);
}

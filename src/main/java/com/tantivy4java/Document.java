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

package com.tantivy4java;

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

    Document(long nativePtr) {
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
     * Add a JSON value to a field.
     * @param fieldName Field name
     * @param value Object to be serialized as JSON
     */
    public void addJson(String fieldName, Object value) {
        if (closed) {
            throw new IllegalStateException("Document has been closed");
        }
        nativeAddJson(nativePtr, fieldName, value);
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
    long getNativePtr() {
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
    private static native void nativeAddJson(long ptr, String fieldName, Object value);
    private static native void nativeAddIpAddr(long ptr, String fieldName, String ipAddr);
    private static native int nativeGetNumFields(long ptr);
    private static native boolean nativeIsEmpty(long ptr);
    private static native void nativeClose(long ptr);
}

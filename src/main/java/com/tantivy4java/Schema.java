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

import java.util.List;
import java.util.Set;

/**
 * Represents a Tantivy schema definition.
 * A schema defines the structure of documents in a Tantivy index.
 */
public class Schema implements AutoCloseable {
    static {
        Tantivy.initialize();
    }

    private long nativePtr;
    private boolean closed = false;

    Schema(long nativePtr) {
        this.nativePtr = nativePtr;
    }

    /**
     * Get the native pointer for JNI operations.
     * @return Native pointer
     */
    long getNativePtr() {
        if (closed) {
            throw new IllegalStateException("Schema has been closed");
        }
        return nativePtr;
    }
    
    /**
     * Get all field names in the schema.
     * @return List of field names
     */
    public List<String> getFieldNames() {
        if (closed) {
            throw new IllegalStateException("Schema has been closed");
        }
        return nativeGetFieldNames(nativePtr);
    }
    
    /**
     * Get information about a specific field.
     * @param fieldName The name of the field
     * @return FieldInfo object containing field metadata, or null if field not found
     */
    public FieldInfo getFieldInfo(String fieldName) {
        if (closed) {
            throw new IllegalStateException("Schema has been closed");
        }
        return nativeGetFieldInfo(nativePtr, fieldName);
    }
    
    /**
     * Get information about all fields in the schema.
     * @return List of FieldInfo objects for all fields
     */
    public List<FieldInfo> getAllFieldInfo() {
        if (closed) {
            throw new IllegalStateException("Schema has been closed");
        }
        return nativeGetAllFieldInfo(nativePtr);
    }
    
    /**
     * Check if a field exists in the schema.
     * @param fieldName The name of the field to check
     * @return true if the field exists, false otherwise
     */
    public boolean hasField(String fieldName) {
        if (closed) {
            throw new IllegalStateException("Schema has been closed");
        }
        return nativeHasField(nativePtr, fieldName);
    }
    
    /**
     * Get the number of fields in the schema.
     * @return Number of fields
     */
    public int getFieldCount() {
        if (closed) {
            throw new IllegalStateException("Schema has been closed");
        }
        return nativeGetFieldCount(nativePtr);
    }
    
    /**
     * Get field names filtered by type.
     * @param fieldType The type to filter by
     * @return List of field names of the specified type
     */
    public List<String> getFieldNamesByType(FieldType fieldType) {
        if (closed) {
            throw new IllegalStateException("Schema has been closed");
        }
        return nativeGetFieldNamesByType(nativePtr, fieldType.getValue());
    }
    
    /**
     * Get field names filtered by capabilities.
     * @param stored Filter for stored fields (null = no filter)
     * @param indexed Filter for indexed fields (null = no filter)
     * @param fast Filter for fast fields (null = no filter)
     * @return List of field names matching the criteria
     */
    public List<String> getFieldNamesByCapabilities(Boolean stored, Boolean indexed, Boolean fast) {
        if (closed) {
            throw new IllegalStateException("Schema has been closed");
        }
        return nativeGetFieldNamesByCapabilities(nativePtr, 
            stored == null ? -1 : (stored ? 1 : 0),
            indexed == null ? -1 : (indexed ? 1 : 0),
            fast == null ? -1 : (fast ? 1 : 0));
    }
    
    /**
     * Get all stored field names.
     * @return List of stored field names
     */
    public List<String> getStoredFieldNames() {
        return getFieldNamesByCapabilities(true, null, null);
    }
    
    /**
     * Get all indexed field names.
     * @return List of indexed field names
     */
    public List<String> getIndexedFieldNames() {
        return getFieldNamesByCapabilities(null, true, null);
    }
    
    /**
     * Get all fast field names.
     * @return List of fast field names
     */
    public List<String> getFastFieldNames() {
        return getFieldNamesByCapabilities(null, null, true);
    }
    
    /**
     * Get a summary of the schema structure.
     * @return String representation of schema structure
     */
    public String getSchemaSummary() {
        if (closed) {
            throw new IllegalStateException("Schema has been closed");
        }
        return nativeGetSchemaSummary(nativePtr);
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
    private static native void nativeClose(long nativePtr);
    private static native List<String> nativeGetFieldNames(long nativePtr);
    private static native FieldInfo nativeGetFieldInfo(long nativePtr, String fieldName);
    private static native List<FieldInfo> nativeGetAllFieldInfo(long nativePtr);
    private static native boolean nativeHasField(long nativePtr, String fieldName);
    private static native int nativeGetFieldCount(long nativePtr);
    private static native List<String> nativeGetFieldNamesByType(long nativePtr, int fieldType);
    private static native List<String> nativeGetFieldNamesByCapabilities(long nativePtr, int stored, int indexed, int fast);
    private static native String nativeGetSchemaSummary(long nativePtr);
}

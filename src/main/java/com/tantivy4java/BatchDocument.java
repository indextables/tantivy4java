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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Represents a document for batch indexing operations.
 * This class provides the same interface as Document but stores data
 * in memory for efficient batch serialization to byte arrays.
 * 
 * Unlike the regular Document class, this does not create native objects
 * immediately but instead collects field data for later batch processing.
 */
public class BatchDocument {
    private final Map<String, List<FieldValue>> fields = new HashMap<>();
    
    /**
     * Create a new empty batch document.
     */
    public BatchDocument() {
    }
    
    /**
     * Create a batch document from a map of field values.
     * @param fields Map of field names to values
     * @return New batch document instance
     */
    public static BatchDocument fromMap(Map<String, Object> fields) {
        BatchDocument doc = new BatchDocument();
        for (Map.Entry<String, Object> entry : fields.entrySet()) {
            String fieldName = entry.getKey();
            Object value = entry.getValue();
            
            if (value instanceof List) {
                @SuppressWarnings("unchecked")
                List<Object> values = (List<Object>) value;
                for (Object val : values) {
                    doc.addValue(fieldName, val);
                }
            } else {
                doc.addValue(fieldName, value);
            }
        }
        return doc;
    }
    
    private void addValue(String fieldName, Object value) {
        if (value instanceof String) {
            addText(fieldName, (String) value);
        } else if (value instanceof Long) {
            addInteger(fieldName, (Long) value);
        } else if (value instanceof Integer) {
            addInteger(fieldName, ((Integer) value).longValue());
        } else if (value instanceof Double) {
            addFloat(fieldName, (Double) value);
        } else if (value instanceof Float) {
            addFloat(fieldName, ((Float) value).doubleValue());
        } else if (value instanceof Boolean) {
            addBoolean(fieldName, (Boolean) value);
        } else if (value instanceof LocalDateTime) {
            addDate(fieldName, (LocalDateTime) value);
        } else if (value instanceof byte[]) {
            addBytes(fieldName, (byte[]) value);
        } else if (value instanceof Facet) {
            addFacet(fieldName, (Facet) value);
        } else {
            // Default to JSON serialization for complex objects
            addJson(fieldName, value);
        }
    }
    
    /**
     * Add a text value to a field.
     * @param fieldName Field name
     * @param text Text value
     */
    public void addText(String fieldName, String text) {
        addFieldValue(fieldName, FieldType.TEXT, text);
    }
    
    /**
     * Add an unsigned integer value to a field.
     * @param fieldName Field name
     * @param value Unsigned integer value
     */
    public void addUnsigned(String fieldName, long value) {
        addFieldValue(fieldName, FieldType.UNSIGNED, value);
    }
    
    /**
     * Add an integer value to a field.
     * @param fieldName Field name
     * @param value Integer value
     */
    public void addInteger(String fieldName, long value) {
        addFieldValue(fieldName, FieldType.INTEGER, value);
    }
    
    /**
     * Add a float value to a field.
     * @param fieldName Field name
     * @param value Float value
     */
    public void addFloat(String fieldName, double value) {
        addFieldValue(fieldName, FieldType.FLOAT, value);
    }
    
    /**
     * Add a boolean value to a field.
     * @param fieldName Field name
     * @param value Boolean value
     */
    public void addBoolean(String fieldName, boolean value) {
        addFieldValue(fieldName, FieldType.BOOLEAN, value);
    }
    
    /**
     * Add a date value to a field.
     * @param fieldName Field name
     * @param date Date value
     */
    public void addDate(String fieldName, LocalDateTime date) {
        addFieldValue(fieldName, FieldType.DATE, date);
    }
    
    /**
     * Add a facet value to a field.
     * @param fieldName Field name
     * @param facet Facet value
     */
    public void addFacet(String fieldName, Facet facet) {
        addFieldValue(fieldName, FieldType.FACET, facet);
    }
    
    /**
     * Add a bytes value to a field.
     * @param fieldName Field name
     * @param bytes Byte array value
     */
    public void addBytes(String fieldName, byte[] bytes) {
        addFieldValue(fieldName, FieldType.BYTES, bytes);
    }
    
    /**
     * Add a JSON value to a field.
     * @param fieldName Field name
     * @param value Object to be serialized as JSON
     */
    public void addJson(String fieldName, Object value) {
        addFieldValue(fieldName, FieldType.JSON, value);
    }
    
    /**
     * Add an IP address value to a field.
     * @param fieldName Field name
     * @param ipAddr IP address as string
     */
    public void addIpAddr(String fieldName, String ipAddr) {
        addFieldValue(fieldName, FieldType.IP_ADDR, ipAddr);
    }
    
    private void addFieldValue(String fieldName, FieldType type, Object value) {
        fields.computeIfAbsent(fieldName, k -> new ArrayList<>())
              .add(new FieldValue(type, value));
    }
    
    /**
     * Get the number of fields in this document.
     * @return Number of fields
     */
    public int getNumFields() {
        return fields.size();
    }
    
    /**
     * Check if this document is empty.
     * @return true if the document has no fields, false otherwise
     */
    public boolean isEmpty() {
        return fields.isEmpty();
    }
    
    /**
     * Get all field names in this document.
     * @return Set of field names
     */
    public java.util.Set<String> getFieldNames() {
        return fields.keySet();
    }
    
    /**
     * Get all values for a field.
     * @param fieldName Field name
     * @return List of field values for the field
     */
    List<FieldValue> getFieldValues(String fieldName) {
        return fields.getOrDefault(fieldName, new ArrayList<>());
    }
    
    /**
     * Get all fields in this document.
     * @return Map of field names to field value lists
     */
    Map<String, List<FieldValue>> getAllFields() {
        return fields;
    }
    
    /**
     * Field type enumeration for batch serialization.
     */
    enum FieldType {
        TEXT((byte) 0),
        INTEGER((byte) 1), 
        FLOAT((byte) 2),
        BOOLEAN((byte) 3),
        DATE((byte) 4),
        BYTES((byte) 5),
        JSON((byte) 6),
        IP_ADDR((byte) 7),
        UNSIGNED((byte) 8),
        FACET((byte) 9);
        
        private final byte code;
        
        FieldType(byte code) {
            this.code = code;
        }
        
        public byte getCode() {
            return code;
        }
        
        public static FieldType fromCode(byte code) {
            for (FieldType type : values()) {
                if (type.code == code) {
                    return type;
                }
            }
            throw new IllegalArgumentException("Unknown field type code: " + code);
        }
    }
    
    /**
     * Internal representation of a field value with type information.
     */
    static class FieldValue {
        private final FieldType type;
        private final Object value;
        
        public FieldValue(FieldType type, Object value) {
            this.type = type;
            this.value = value;
        }
        
        public FieldType getType() {
            return type;
        }
        
        public Object getValue() {
            return value;
        }
    }
}
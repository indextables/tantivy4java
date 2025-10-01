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

import java.util.Objects;

/**
 * Represents information about a field in a Tantivy schema.
 * Contains field name, type, and configuration details.
 */
public class FieldInfo {
    private final String name;
    private final FieldType type;
    private final boolean stored;
    private final boolean indexed;
    private final boolean fast;
    private final String tokenizerName;
    private final String indexOption;
    
    /**
     * Create a new FieldInfo for text fields.
     */
    public FieldInfo(String name, FieldType type, boolean stored, boolean indexed, boolean fast,
                    String tokenizerName, String indexOption) {
        this.name = Objects.requireNonNull(name, "Field name cannot be null");
        this.type = Objects.requireNonNull(type, "Field type cannot be null");
        this.stored = stored;
        this.indexed = indexed;
        this.fast = fast;
        this.tokenizerName = tokenizerName;
        this.indexOption = indexOption;
    }
    
    /**
     * Create a new FieldInfo for non-text fields.
     */
    public FieldInfo(String name, FieldType type, boolean stored, boolean indexed, boolean fast) {
        this(name, type, stored, indexed, fast, null, null);
    }
    
    /**
     * Get the field name.
     * @return Field name
     */
    public String getName() {
        return name;
    }
    
    /**
     * Get the field type.
     * @return Field type
     */
    public FieldType getType() {
        return type;
    }
    
    /**
     * Check if the field is stored.
     * @return true if the field is stored
     */
    public boolean isStored() {
        return stored;
    }
    
    /**
     * Check if the field is indexed.
     * @return true if the field is indexed
     */
    public boolean isIndexed() {
        return indexed;
    }
    
    /**
     * Check if the field supports fast access.
     * @return true if the field has fast access
     */
    public boolean isFast() {
        return fast;
    }
    
    /**
     * Get the tokenizer name for text fields.
     * @return Tokenizer name, or null for non-text fields
     */
    public String getTokenizerName() {
        return tokenizerName;
    }
    
    /**
     * Get the index option for text fields.
     * @return Index option, or null for non-text fields
     */
    public String getIndexOption() {
        return indexOption;
    }
    
    /**
     * Check if this is a text field.
     * @return true if this is a text field
     */
    public boolean isTextfield() {
        return type == FieldType.TEXT;
    }
    
    /**
     * Check if this is a numeric field.
     * @return true if this is a numeric field
     */
    public boolean isNumeric() {
        return type == FieldType.INTEGER || type == FieldType.UNSIGNED || type == FieldType.FLOAT;
    }
    
    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        FieldInfo fieldInfo = (FieldInfo) obj;
        return stored == fieldInfo.stored &&
               indexed == fieldInfo.indexed &&
               fast == fieldInfo.fast &&
               Objects.equals(name, fieldInfo.name) &&
               type == fieldInfo.type &&
               Objects.equals(tokenizerName, fieldInfo.tokenizerName) &&
               Objects.equals(indexOption, fieldInfo.indexOption);
    }
    
    @Override
    public int hashCode() {
        return Objects.hash(name, type, stored, indexed, fast, tokenizerName, indexOption);
    }
    
    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("FieldInfo{name='").append(name).append("', type=").append(type);
        sb.append(", stored=").append(stored).append(", indexed=").append(indexed);
        sb.append(", fast=").append(fast);
        if (tokenizerName != null) {
            sb.append(", tokenizer='").append(tokenizerName).append("'");
        }
        if (indexOption != null) {
            sb.append(", indexOption='").append(indexOption).append("'");
        }
        sb.append("}");
        return sb.toString();
    }
}
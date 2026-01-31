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

/**
 * Builder for creating Tantivy schemas.
 * Provides methods to add different types of fields to a schema.
 */
public class SchemaBuilder implements AutoCloseable {
    static {
        Tantivy.initialize();
    }

    private long nativePtr;
    private boolean closed = false;

    public SchemaBuilder() {
        this.nativePtr = nativeNew();
    }

    SchemaBuilder(long nativePtr) {
        this.nativePtr = nativePtr;
    }

    /**
     * Check if a field name is valid.
     * @param name Field name to validate
     * @return true if the name is valid, false otherwise
     */
    public static boolean isValidFieldName(String name) {
        return nativeIsValidFieldName(name);
    }

    /**
     * Add a text field to the schema.
     * @param name Field name
     * @param stored Whether the field should be stored
     * @param fast Whether the field should support fast access
     * @param tokenizerName Tokenizer to use for this field
     * @param indexOption Index option for the field
     * @return This builder for method chaining
     */
    public SchemaBuilder addTextField(String name, boolean stored, boolean fast,
                                    String tokenizerName, String indexOption) {
        return addTextField(name, stored, fast, tokenizerName, indexOption, TokenLength.DEFAULT);
    }

    /**
     * Add a text field to the schema with configurable token length limit.
     * @param name Field name
     * @param stored Whether the field should be stored
     * @param fast Whether the field should support fast access
     * @param tokenizerName Tokenizer to use for this field
     * @param indexOption Index option for the field
     * @param maxTokenLength Maximum token length in bytes (tokens longer than this are filtered out)
     * @return This builder for method chaining
     * @throws IllegalArgumentException if maxTokenLength is outside valid range [1, 65530]
     */
    public SchemaBuilder addTextField(String name, boolean stored, boolean fast,
                                    String tokenizerName, String indexOption, int maxTokenLength) {
        if (closed) {
            throw new IllegalStateException("SchemaBuilder has been closed");
        }
        TokenLength.validate(maxTokenLength);
        nativeAddTextField(nativePtr, name, stored, fast, tokenizerName, indexOption, maxTokenLength);
        return this;
    }

    /**
     * Add a text field with default options.
     * @param name Field name
     * @return This builder for method chaining
     */
    public SchemaBuilder addTextField(String name) {
        return addTextField(name, false, false, "default", "position", TokenLength.DEFAULT);
    }

    /**
     * Add a text field to the schema using TextFieldIndexing configuration.
     * @param name Field name
     * @param stored Whether the field should be stored
     * @param fast Whether the field should support fast access
     * @param indexing Text field indexing configuration
     * @return This builder for method chaining
     */
    public SchemaBuilder addTextField(String name, boolean stored, boolean fast,
                                    TextFieldIndexing indexing) {
        if (closed) {
            throw new IllegalStateException("SchemaBuilder has been closed");
        }
        String indexOptionStr = indexOptionToString(indexing.getIndexOption());
        nativeAddTextField(nativePtr, name, stored, fast, indexing.getTokenizerName(),
                          indexOptionStr, indexing.getMaxTokenLength());
        return this;
    }

    private String indexOptionToString(TextFieldIndexing.IndexOption option) {
        switch (option) {
            case BASIC:
                return "basic";
            case WITH_FREQS:
                return "freq";
            case WITH_FREQS_AND_POSITIONS:
            default:
                return "position";
        }
    }

    /**
     * Add an integer field to the schema.
     * @param name Field name
     * @param stored Whether the field should be stored
     * @param indexed Whether the field should be indexed
     * @param fast Whether the field should support fast access
     * @return This builder for method chaining
     */
    public SchemaBuilder addIntegerField(String name, boolean stored, boolean indexed, boolean fast) {
        if (closed) {
            throw new IllegalStateException("SchemaBuilder has been closed");
        }
        nativeAddIntegerField(nativePtr, name, stored, indexed, fast);
        return this;
    }

    /**
     * Add an integer field with default options.
     * @param name Field name
     * @return This builder for method chaining
     */
    public SchemaBuilder addIntegerField(String name) {
        return addIntegerField(name, false, false, false);
    }

    /**
     * Add a float field to the schema.
     * @param name Field name
     * @param stored Whether the field should be stored
     * @param indexed Whether the field should be indexed
     * @param fast Whether the field should support fast access
     * @return This builder for method chaining
     */
    public SchemaBuilder addFloatField(String name, boolean stored, boolean indexed, boolean fast) {
        if (closed) {
            throw new IllegalStateException("SchemaBuilder has been closed");
        }
        nativeAddFloatField(nativePtr, name, stored, indexed, fast);
        return this;
    }

    /**
     * Add a float field with default options.
     * @param name Field name
     * @return This builder for method chaining
     */
    public SchemaBuilder addFloatField(String name) {
        return addFloatField(name, false, false, false);
    }

    /**
     * Add an unsigned field to the schema.
     * @param name Field name
     * @param stored Whether the field should be stored
     * @param indexed Whether the field should be indexed
     * @param fast Whether the field should support fast access
     * @return This builder for method chaining
     */
    public SchemaBuilder addUnsignedField(String name, boolean stored, boolean indexed, boolean fast) {
        if (closed) {
            throw new IllegalStateException("SchemaBuilder has been closed");
        }
        nativeAddUnsignedField(nativePtr, name, stored, indexed, fast);
        return this;
    }

    /**
     * Add an unsigned field with default options.
     * @param name Field name
     * @return This builder for method chaining
     */
    public SchemaBuilder addUnsignedField(String name) {
        return addUnsignedField(name, false, false, false);
    }

    /**
     * Add a boolean field to the schema.
     * @param name Field name
     * @param stored Whether the field should be stored
     * @param indexed Whether the field should be indexed
     * @param fast Whether the field should support fast access
     * @return This builder for method chaining
     */
    public SchemaBuilder addBooleanField(String name, boolean stored, boolean indexed, boolean fast) {
        if (closed) {
            throw new IllegalStateException("SchemaBuilder has been closed");
        }
        nativeAddBooleanField(nativePtr, name, stored, indexed, fast);
        return this;
    }

    /**
     * Add a boolean field with default options.
     * @param name Field name
     * @return This builder for method chaining
     */
    public SchemaBuilder addBooleanField(String name) {
        return addBooleanField(name, false, false, false);
    }

    /**
     * Add a date field to the schema.
     * @param name Field name
     * @param stored Whether the field should be stored
     * @param indexed Whether the field should be indexed
     * @param fast Whether the field should support fast access
     * @return This builder for method chaining
     */
    public SchemaBuilder addDateField(String name, boolean stored, boolean indexed, boolean fast) {
        if (closed) {
            throw new IllegalStateException("SchemaBuilder has been closed");
        }
        nativeAddDateField(nativePtr, name, stored, indexed, fast);
        return this;
    }

    /**
     * Add a date field with default options.
     * @param name Field name
     * @return This builder for method chaining
     */
    public SchemaBuilder addDateField(String name) {
        return addDateField(name, false, false, false);
    }

    /**
     * Add a JSON field to the schema with full configuration options.
     *
     * <p>JSON fields allow storing and indexing arbitrary nested JSON objects.
     * All nested values (strings, numbers, booleans, dates, arrays) are
     * automatically indexed according to their type and can be queried using
     * JSONPath syntax.
     *
     * <p>Example:
     * <pre>
     * Field attrs = schemaBuilder.addJsonField("attributes",
     *     JsonObjectOptions.storedAndIndexed()
     *         .setExpandDots(true));
     * </pre>
     *
     * @param name Field name
     * @param options JSON field configuration options
     * @return Field handle for the created JSON field
     * @throws IllegalArgumentException if name is null or empty
     * @throws IllegalStateException if builder has been closed
     */
    public Field addJsonField(String name, JsonObjectOptions options) {
        if (closed) {
            throw new IllegalStateException("SchemaBuilder has been closed");
        }
        if (name == null || name.isEmpty()) {
            throw new IllegalArgumentException("Field name cannot be null or empty");
        }
        if (options == null) {
            throw new IllegalArgumentException("JsonObjectOptions cannot be null");
        }

        // Call native method with all options
        int fieldId = nativeAddJsonFieldWithOptions(
            nativePtr,
            name,
            options.isStored(),
            options.getIndexing() != null,
            options.isFast(),
            options.getFastTokenizer(),
            options.isExpandDots(),
            options.getIndexing() != null ? options.getIndexing().getTokenizerName() : null,
            options.getIndexing() != null ? options.getIndexing().getIndexOptionCode() : 0
        );
        return new Field(name, fieldId);
    }

    /**
     * Add a JSON field to the schema.
     *
     * @deprecated Use {@link #addJsonField(String, JsonObjectOptions)} instead for better type safety
     *
     * @param name Field name
     * @param stored Whether the field should be stored
     * @param tokenizerName Tokenizer to use for this field
     * @param indexOption Index option for the field
     * @return This builder for method chaining
     */
    @Deprecated
    public SchemaBuilder addJsonField(String name, boolean stored, String tokenizerName, String indexOption) {
        if (closed) {
            throw new IllegalStateException("SchemaBuilder has been closed");
        }
        nativeAddJsonField(nativePtr, name, stored, tokenizerName, indexOption);
        return this;
    }

    /**
     * Add a JSON field with default options.
     *
     * @param name Field name
     * @return This builder for method chaining
     */
    public SchemaBuilder addJsonField(String name) {
        return addJsonField(name, false, "default", "position");
    }

    /**
     * Add a facet field to the schema.
     * @param name Field name
     * @return This builder for method chaining
     */
    public SchemaBuilder addFacetField(String name) {
        if (closed) {
            throw new IllegalStateException("SchemaBuilder has been closed");
        }
        nativeAddFacetField(nativePtr, name);
        return this;
    }

    /**
     * Add a bytes field to the schema.
     * @param name Field name
     * @param stored Whether the field should be stored
     * @param indexed Whether the field should be indexed
     * @param fast Whether the field should support fast access
     * @param indexOption Index option for the field
     * @return This builder for method chaining
     */
    public SchemaBuilder addBytesField(String name, boolean stored, boolean indexed, boolean fast, String indexOption) {
        if (closed) {
            throw new IllegalStateException("SchemaBuilder has been closed");
        }
        nativeAddBytesField(nativePtr, name, stored, indexed, fast, indexOption);
        return this;
    }

    /**
     * Add a bytes field with default options.
     * @param name Field name
     * @return This builder for method chaining
     */
    public SchemaBuilder addBytesField(String name) {
        return addBytesField(name, false, false, false, "position");
    }

    /**
     * Add an IP address field to the schema.
     * @param name Field name
     * @param stored Whether the field should be stored
     * @param indexed Whether the field should be indexed
     * @param fast Whether the field should support fast access
     * @return This builder for method chaining
     */
    public SchemaBuilder addIpAddrField(String name, boolean stored, boolean indexed, boolean fast) {
        if (closed) {
            throw new IllegalStateException("SchemaBuilder has been closed");
        }
        nativeAddIpAddrField(nativePtr, name, stored, indexed, fast);
        return this;
    }

    /**
     * Add an IP address field with default options.
     * @param name Field name
     * @return This builder for method chaining
     */
    public SchemaBuilder addIpAddrField(String name) {
        return addIpAddrField(name, false, false, false);
    }

    /**
     * Add a string field to the schema.
     * String fields are for exact string matching (not tokenized like text fields).
     * @param name Field name
     * @param stored Whether the field should be stored
     * @param indexed Whether the field should be indexed
     * @param fast Whether the field should support fast access
     * @return This builder for method chaining
     */
    public SchemaBuilder addStringField(String name, boolean stored, boolean indexed, boolean fast) {
        if (closed) {
            throw new IllegalStateException("SchemaBuilder has been closed");
        }
        nativeAddStringField(nativePtr, name, stored, indexed, fast);
        return this;
    }

    /**
     * Add a string field with default options.
     * @param name Field name
     * @return This builder for method chaining
     */
    public SchemaBuilder addStringField(String name) {
        return addStringField(name, false, false, false);
    }

    /**
     * Build the schema from the configured fields.
     * @return A new Schema instance
     */
    public Schema build() {
        if (closed) {
            throw new IllegalStateException("SchemaBuilder has been closed");
        }
        long schemaPtr = nativeBuild(nativePtr);
        return new Schema(schemaPtr);
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
    private static native boolean nativeIsValidFieldName(String name);
    private static native void nativeAddTextField(long ptr, String name, boolean stored, boolean fast, String tokenizerName, String indexOption, int maxTokenLength);
    private static native void nativeAddIntegerField(long ptr, String name, boolean stored, boolean indexed, boolean fast);
    private static native void nativeAddFloatField(long ptr, String name, boolean stored, boolean indexed, boolean fast);
    private static native void nativeAddUnsignedField(long ptr, String name, boolean stored, boolean indexed, boolean fast);
    private static native void nativeAddBooleanField(long ptr, String name, boolean stored, boolean indexed, boolean fast);
    private static native void nativeAddDateField(long ptr, String name, boolean stored, boolean indexed, boolean fast);
    private static native void nativeAddJsonField(long ptr, String name, boolean stored, String tokenizerName, String indexOption);
    private static native int nativeAddJsonFieldWithOptions(long ptr, String name, boolean stored, boolean indexed, boolean fast, String fastTokenizer, boolean expandDots, String tokenizerName, int recordOption);
    private static native void nativeAddFacetField(long ptr, String name);
    private static native void nativeAddBytesField(long ptr, String name, boolean stored, boolean indexed, boolean fast, String indexOption);
    private static native void nativeAddIpAddrField(long ptr, String name, boolean stored, boolean indexed, boolean fast);
    private static native void nativeAddStringField(long ptr, String name, boolean stored, boolean indexed, boolean fast);
    private static native long nativeBuild(long ptr);
    private static native void nativeClose(long ptr);
}

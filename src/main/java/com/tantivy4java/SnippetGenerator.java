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

/**
 * Generates text snippets with highlighted query matches.
 * Used for search result highlighting and snippet generation.
 */
public class SnippetGenerator implements AutoCloseable {
    static {
        Tantivy.initialize();
    }

    private long nativePtr;
    private boolean closed = false;

    SnippetGenerator(long nativePtr) {
        this.nativePtr = nativePtr;
    }

    /**
     * Create a new snippet generator.
     * @param searcher Searcher instance
     * @param query Query to highlight
     * @param schema Schema for field validation
     * @param fieldName Field to generate snippets from
     * @return New SnippetGenerator instance
     */
    public static SnippetGenerator create(Searcher searcher, Query query, Schema schema, String fieldName) {
        long ptr = nativeCreate(searcher.getNativePtr(), query.getNativePtr(), schema.getNativePtr(), fieldName);
        return new SnippetGenerator(ptr);
    }

    /**
     * Generate a snippet from a document.
     * @param doc Document to generate snippet from
     * @return Snippet with highlighted terms
     */
    public Snippet snippetFromDoc(Document doc) {
        if (closed) {
            throw new IllegalStateException("SnippetGenerator has been closed");
        }
        long ptr = nativeSnippetFromDoc(nativePtr, doc.getNativePtr());
        return new Snippet(ptr);
    }

    /**
     * Set the maximum number of characters in the snippet.
     * @param maxNumChars Maximum number of characters
     */
    public void setMaxNumChars(int maxNumChars) {
        if (closed) {
            throw new IllegalStateException("SnippetGenerator has been closed");
        }
        if (maxNumChars <= 0) {
            throw new IllegalArgumentException("maxNumChars must be positive");
        }
        nativeSetMaxNumChars(nativePtr, maxNumChars);
    }

    /**
     * Get the native pointer for JNI operations.
     * @return Native pointer
     */
    long getNativePtr() {
        if (closed) {
            throw new IllegalStateException("SnippetGenerator has been closed");
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
    private static native long nativeCreate(long searcherPtr, long queryPtr, long schemaPtr, String fieldName);
    private static native long nativeSnippetFromDoc(long ptr, long docPtr);
    private static native void nativeSetMaxNumChars(long ptr, int maxNumChars);
    private static native void nativeClose(long ptr);
}
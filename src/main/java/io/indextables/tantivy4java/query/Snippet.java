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

package io.indextables.tantivy4java.query;

import io.indextables.tantivy4java.core.Tantivy;

import java.util.List;

/**
 * Represents a text snippet with highlighted query matches.
 * Used for search result highlighting and snippet generation.
 */
public class Snippet implements AutoCloseable {
    static {
        Tantivy.initialize();
    }

    private long nativePtr;
    private boolean closed = false;

    Snippet(long nativePtr) {
        this.nativePtr = nativePtr;
    }

    /**
     * Convert the snippet to HTML with highlighted terms.
     * @return HTML string with highlighted terms
     */
    public String toHtml() {
        if (closed) {
            throw new IllegalStateException("Snippet has been closed");
        }
        return nativeToHtml(nativePtr);
    }

    /**
     * Get the highlighted ranges in the snippet.
     * @return List of ranges that are highlighted
     */
    public List<Range> getHighlighted() {
        if (closed) {
            throw new IllegalStateException("Snippet has been closed");
        }
        return nativeGetHighlighted(nativePtr);
    }

    /**
     * Get the plain text fragment of the snippet.
     * @return Plain text fragment without highlighting
     */
    public String getFragment() {
        if (closed) {
            throw new IllegalStateException("Snippet has been closed");
        }
        return nativeGetFragment(nativePtr);
    }

    /**
     * Get the native pointer for JNI operations.
     * @return Native pointer
     */
    long getNativePtr() {
        if (closed) {
            throw new IllegalStateException("Snippet has been closed");
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

    @Override
    public String toString() {
        if (closed) {
            return "Snippet[closed]";
        }
        return "Snippet[" + getFragment() + "]";
    }

    // Native method declarations
    private static native String nativeToHtml(long ptr);
    private static native List<Range> nativeGetHighlighted(long ptr);
    private static native String nativeGetFragment(long ptr);
    private static native void nativeClose(long ptr);
}
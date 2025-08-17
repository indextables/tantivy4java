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

/**
 * Text analyzer for processing text during indexing and querying.
 * Combines a tokenizer with optional filters.
 */
public class TextAnalyzer implements AutoCloseable {
    static {
        Tantivy.initialize();
    }

    private long nativePtr;
    private boolean closed = false;

    TextAnalyzer(long nativePtr) {
        this.nativePtr = nativePtr;
    }

    /**
     * Analyze text and return the resulting tokens.
     * @param text Text to analyze
     * @return List of tokens
     */
    public List<String> analyze(String text) {
        if (closed) {
            throw new IllegalStateException("TextAnalyzer has been closed");
        }
        return nativeAnalyze(nativePtr, text);
    }

    /**
     * Get the native pointer for JNI operations.
     * @return Native pointer
     */
    long getNativePtr() {
        if (closed) {
            throw new IllegalStateException("TextAnalyzer has been closed");
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
    protected void finalize() throws Throwable {
        close();
        super.finalize();
    }

    // Native method declarations
    private static native List<String> nativeAnalyze(long ptr, String text);
    private static native void nativeClose(long ptr);
}
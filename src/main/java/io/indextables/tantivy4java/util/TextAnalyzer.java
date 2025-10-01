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

package io.indextables.tantivy4java.util;

import io.indextables.tantivy4java.core.Tantivy;

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
     * Create a TextAnalyzer with the specified tokenizer.
     * @param tokenizerName Name of the tokenizer ("default", "keyword", "whitespace", "raw", etc.)
     * @return New TextAnalyzer instance
     */
    public static TextAnalyzer create(String tokenizerName) {
        long ptr = nativeCreateAnalyzer(tokenizerName);
        return new TextAnalyzer(ptr);
    }

    /**
     * Tokenize text using Tantivy's default tokenizer.
     * This is a static convenience method that doesn't require creating an analyzer instance.
     * @param text Text to tokenize
     * @return List of tokens
     */
    public static List<String> tokenize(String text) {
        return tokenize(text, "default");
    }

    /**
     * Tokenize text using the specified Tantivy tokenizer.
     * This is a static convenience method that doesn't require creating an analyzer instance.
     * @param text Text to tokenize
     * @param tokenizerName Name of the tokenizer ("default", "keyword", "whitespace", "raw", etc.)
     * @return List of tokens
     */
    public static List<String> tokenize(String text, String tokenizerName) {
        return nativeTokenize(text, tokenizerName);
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
    public long getNativePtr() {
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

    // Native method declarations
    private static native long nativeCreateAnalyzer(String tokenizerName);
    private static native List<String> nativeTokenize(String text, String tokenizerName);
    private static native List<String> nativeAnalyze(long ptr, String text);
    private static native void nativeClose(long ptr);
}

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
 * Represents a range of characters in a text snippet that are highlighted.
 * Used for text highlighting and snippet generation.
 */
public class Range implements AutoCloseable {
    static {
        Tantivy.initialize();
    }

    private long nativePtr;
    private boolean closed = false;

    Range(long nativePtr) {
        this.nativePtr = nativePtr;
    }

    /**
     * Get the start position of the highlighted range.
     * @return Start position in the text
     */
    public int getStart() {
        if (closed) {
            throw new IllegalStateException("Range has been closed");
        }
        return nativeGetStart(nativePtr);
    }

    /**
     * Get the end position of the highlighted range.
     * @return End position in the text
     */
    public int getEnd() {
        if (closed) {
            throw new IllegalStateException("Range has been closed");
        }
        return nativeGetEnd(nativePtr);
    }

    /**
     * Get the native pointer for JNI operations.
     * @return Native pointer
     */
    long getNativePtr() {
        if (closed) {
            throw new IllegalStateException("Range has been closed");
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
            return "Range[closed]";
        }
        return "Range[" + getStart() + ", " + getEnd() + "]";
    }

    // Native method declarations
    private static native int nativeGetStart(long ptr);
    private static native int nativeGetEnd(long ptr);
    private static native void nativeClose(long ptr);
}
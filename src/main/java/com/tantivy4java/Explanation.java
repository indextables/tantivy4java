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
 * Represents an explanation of how a query scored a document.
 * Provides detailed information about the scoring process.
 */
public class Explanation implements AutoCloseable {
    static {
        Tantivy.initialize();
    }

    private long nativePtr;
    private boolean closed = false;

    Explanation(long nativePtr) {
        this.nativePtr = nativePtr;
    }

    /**
     * Convert the explanation to JSON format.
     * @return JSON representation of the explanation
     */
    public String toJson() {
        if (closed) {
            throw new IllegalStateException("Explanation has been closed");
        }
        return nativeToJson(nativePtr);
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

    @Override
    public String toString() {
        return toJson();
    }

    // Native method declarations
    private static native String nativeToJson(long ptr);
    private static native void nativeClose(long ptr);
}
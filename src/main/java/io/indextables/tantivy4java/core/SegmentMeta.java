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
 * Metadata for a Tantivy segment.
 * Contains information about a segment including its ID and other properties.
 */
public class SegmentMeta implements AutoCloseable {
    static {
        Tantivy.initialize();
    }

    private long nativePtr;
    private boolean closed = false;

    SegmentMeta(long nativePtr) {
        this.nativePtr = nativePtr;
    }

    /**
     * Get the segment ID as a string.
     * @return Segment ID
     */
    public String getSegmentId() {
        if (closed) {
            throw new IllegalStateException("SegmentMeta has been closed");
        }
        return nativeGetSegmentId(nativePtr);
    }

    /**
     * Get the maximum document ID in the segment.
     * @return Maximum document ID
     */
    public long getMaxDoc() {
        if (closed) {
            throw new IllegalStateException("SegmentMeta has been closed");
        }
        return nativeGetMaxDoc(nativePtr);
    }

    /**
     * Get the number of deleted documents in the segment.
     * @return Number of deleted documents
     */
    public long getNumDeletedDocs() {
        if (closed) {
            throw new IllegalStateException("SegmentMeta has been closed");
        }
        return nativeGetNumDeletedDocs(nativePtr);
    }

    long getNativePtr() {
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
    private static native String nativeGetSegmentId(long ptr);
    private static native long nativeGetMaxDoc(long ptr);
    private static native long nativeGetNumDeletedDocs(long ptr);
    private static native void nativeClose(long ptr);
}
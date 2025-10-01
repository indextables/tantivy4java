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
 * Represents a document address in Tantivy.
 * A document address uniquely identifies a document within an index.
 */
public class DocAddress implements AutoCloseable {
    static {
        Tantivy.initialize();
    }

    private long nativePtr;
    private boolean closed = false;

    /**
     * Create a new document address.
     * @param segmentOrd Segment ordinal
     * @param doc Document ID within the segment
     */
    public DocAddress(int segmentOrd, int doc) {
        this.nativePtr = nativeNew(segmentOrd, doc);
    }

    DocAddress(long nativePtr) {
        this.nativePtr = nativePtr;
    }

    /**
     * Get the segment ordinal.
     * @return Segment ordinal
     */
    public int getSegmentOrd() {
        if (closed) {
            throw new IllegalStateException("DocAddress has been closed");
        }
        return nativeGetSegmentOrd(nativePtr);
    }

    /**
     * Get the document ID within the segment.
     * @return Document ID
     */
    public int getDoc() {
        if (closed) {
            throw new IllegalStateException("DocAddress has been closed");
        }
        return nativeGetDoc(nativePtr);
    }

    /**
     * Get the native pointer for JNI operations.
     * @return Native pointer
     */
    public long getNativePtr() {
        if (closed) {
            throw new IllegalStateException("DocAddress has been closed");
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
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        DocAddress that = (DocAddress) obj;
        return getSegmentOrd() == that.getSegmentOrd() && getDoc() == that.getDoc();
    }

    @Override
    public int hashCode() {
        return 31 * getSegmentOrd() + getDoc();
    }

    @Override
    public String toString() {
        return String.format("DocAddress{segmentOrd=%d, doc=%d}", getSegmentOrd(), getDoc());
    }

    // Native method declarations
    private static native long nativeNew(int segmentOrd, int doc);
    private static native int nativeGetSegmentOrd(long ptr);
    private static native int nativeGetDoc(long ptr);
    private static native void nativeClose(long ptr);
}

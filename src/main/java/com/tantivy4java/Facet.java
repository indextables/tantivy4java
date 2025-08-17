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
 * Represents a facet value in Tantivy.
 * Facets are hierarchical categories used for faceted search.
 */
public class Facet implements AutoCloseable {
    static {
        Tantivy.initialize();
    }

    private long nativePtr;
    private boolean closed = false;

    /**
     * Create a facet from encoded bytes.
     * @param encodedBytes Encoded facet bytes
     * @return New Facet instance
     */
    public static Facet fromEncoded(byte[] encodedBytes) {
        long ptr = nativeFromEncoded(encodedBytes);
        return new Facet(ptr);
    }

    /**
     * Create a root facet.
     * @return Root facet instance
     */
    public static Facet root() {
        long ptr = nativeRoot();
        return new Facet(ptr);
    }

    /**
     * Create a facet from a string representation.
     * @param facetString String representation of the facet
     * @return New Facet instance
     */
    public static Facet fromString(String facetString) {
        long ptr = nativeFromString(facetString);
        return new Facet(ptr);
    }

    Facet(long nativePtr) {
        this.nativePtr = nativePtr;
    }

    /**
     * Check if this is a root facet.
     * @return true if this is a root facet, false otherwise
     */
    public boolean isRoot() {
        if (closed) {
            throw new IllegalStateException("Facet has been closed");
        }
        return nativeIsRoot(nativePtr);
    }

    /**
     * Check if this facet is a prefix of another facet.
     * @param other The other facet to check
     * @return true if this facet is a prefix of the other, false otherwise
     */
    public boolean isPrefixOf(Facet other) {
        if (closed) {
            throw new IllegalStateException("Facet has been closed");
        }
        return nativeIsPrefixOf(nativePtr, other.getNativePtr());
    }

    /**
     * Get the path components of this facet.
     * @return List of path components
     */
    public List<String> toPath() {
        if (closed) {
            throw new IllegalStateException("Facet has been closed");
        }
        return nativeToPath(nativePtr);
    }

    /**
     * Get the string representation of the facet path.
     * @return String representation of the path
     */
    public String toPathStr() {
        if (closed) {
            throw new IllegalStateException("Facet has been closed");
        }
        return nativeToPathStr(nativePtr);
    }

    /**
     * Get the native pointer for JNI operations.
     * @return Native pointer
     */
    long getNativePtr() {
        if (closed) {
            throw new IllegalStateException("Facet has been closed");
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

    @Override
    public String toString() {
        return toPathStr();
    }

    // Native method declarations
    private static native long nativeFromEncoded(byte[] encodedBytes);
    private static native long nativeRoot();
    private static native long nativeFromString(String facetString);
    private static native boolean nativeIsRoot(long ptr);
    private static native boolean nativeIsPrefixOf(long ptr, long otherPtr);
    private static native List<String> nativeToPath(long ptr);
    private static native String nativeToPathStr(long ptr);
    private static native void nativeClose(long ptr);
}
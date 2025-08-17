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
 * Represents the result of a search operation.
 * Contains the matching documents and their scores.
 */
public class SearchResult implements AutoCloseable {
    static {
        Tantivy.initialize();
    }

    private long nativePtr;
    private boolean closed = false;

    SearchResult(long nativePtr) {
        this.nativePtr = nativePtr;
    }

    /**
     * Get the search hits (score, DocAddress pairs).
     * @return List of search hits
     */
    public List<Hit> getHits() {
        if (closed) {
            throw new IllegalStateException("SearchResult has been closed");
        }
        return nativeGetHits(nativePtr);
    }

    @Override
    public void close() {
        if (!closed) {
            nativeClose(nativePtr);
            closed = true;
            nativePtr = 0;
        }
    }

    /**
     * Represents a single search hit.
     */
    public static class Hit {
        private final double score;
        private final DocAddress docAddress;

        public Hit(double score, DocAddress docAddress) {
            this.score = score;
            this.docAddress = docAddress;
        }

        public double getScore() {
            return score;
        }

        public DocAddress getDocAddress() {
            return docAddress;
        }

        @Override
        public String toString() {
            return String.format("Hit{score=%.4f, docAddress=%s}", score, docAddress);
        }
    }

    // Native method declarations
    private static native List<Hit> nativeGetHits(long ptr);
    private static native void nativeClose(long ptr);
}

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

import io.indextables.tantivy4java.query.Query;

import io.indextables.tantivy4java.batch.BatchDocumentBuilder;
import java.util.List;

/**
 * Writer for adding documents to a Tantivy index.
 * Provides methods for adding, updating, and deleting documents.
 */
public class IndexWriter implements AutoCloseable {
    static {
        Tantivy.initialize();
    }

    private long nativePtr;
    private boolean closed = false;

    IndexWriter(long nativePtr) {
        this.nativePtr = nativePtr;
    }

    /**
     * Add a document to the index.
     * @param doc Document to add
     * @return Operation stamp
     */
    public long addDocument(Document doc) {
        if (closed) {
            throw new IllegalStateException("IndexWriter has been closed");
        }
        return nativeAddDocument(nativePtr, doc.getNativePtr());
    }

    /**
     * Add a document from JSON string.
     * @param json JSON representation of the document
     * @return Operation stamp
     */
    public long addJson(String json) {
        if (closed) {
            throw new IllegalStateException("IndexWriter has been closed");
        }
        return nativeAddJson(nativePtr, json);
    }

    /**
     * Add multiple documents from a serialized batch buffer.
     * This method provides high-performance bulk indexing by minimizing JNI calls
     * and using zero-copy semantics where possible.
     * 
     * The buffer must be formatted according to the Tantivy4Java batch protocol.
     * Use BatchDocumentBuilder to create properly formatted buffers.
     * 
     * @param buffer Direct ByteBuffer containing serialized document batch
     * @return Array of operation stamps, one for each document added
     * @throws IllegalArgumentException if buffer is null or not direct
     * @throws RuntimeException if buffer format is invalid or processing fails
     */
    public long[] addDocumentsByBuffer(java.nio.ByteBuffer buffer) {
        if (closed) {
            throw new IllegalStateException("IndexWriter has been closed");
        }
        if (buffer == null) {
            throw new IllegalArgumentException("Buffer cannot be null");
        }
        if (!buffer.isDirect()) {
            throw new IllegalArgumentException("Buffer must be a direct ByteBuffer for zero-copy operations");
        }
        if (!buffer.hasRemaining()) {
            throw new IllegalArgumentException("Buffer is empty");
        }
        
        return nativeAddDocumentsByBuffer(nativePtr, buffer);
    }

    /**
     * Add multiple documents from a BatchDocumentBuilder.
     * This is a convenience method that builds the buffer and adds the documents
     * in a single operation.
     * 
     * @param builder BatchDocumentBuilder containing documents to add
     * @return Array of operation stamps, one for each document added
     * @throws IllegalArgumentException if builder is null or empty
     */
    public long[] addDocumentsBatch(BatchDocumentBuilder builder) {
        if (builder == null) {
            throw new IllegalArgumentException("Builder cannot be null");
        }
        if (builder.isEmpty()) {
            throw new IllegalArgumentException("Builder contains no documents");
        }
        
        java.nio.ByteBuffer buffer = builder.build();
        return addDocumentsByBuffer(buffer);
    }

    /**
     * Commit all pending changes to the index.
     * @return Operation stamp of the commit
     */
    public long commit() {
        if (closed) {
            throw new IllegalStateException("IndexWriter has been closed");
        }
        return nativeCommit(nativePtr);
    }

    /**
     * Rollback all uncommitted changes.
     * @return Operation stamp
     */
    public long rollback() {
        if (closed) {
            throw new IllegalStateException("IndexWriter has been closed");
        }
        return nativeRollback(nativePtr);
    }

    /**
     * Garbage collect unused files.
     */
    public void garbageCollectFiles() {
        if (closed) {
            throw new IllegalStateException("IndexWriter has been closed");
        }
        nativeGarbageCollectFiles(nativePtr);
    }

    /**
     * Delete all documents from the index.
     */
    public void deleteAllDocuments() {
        if (closed) {
            throw new IllegalStateException("IndexWriter has been closed");
        }
        nativeDeleteAllDocuments(nativePtr);
    }

    /**
     * Get the commit operation stamp.
     * @return Commit operation stamp
     */
    public long getCommitOpstamp() {
        if (closed) {
            throw new IllegalStateException("IndexWriter has been closed");
        }
        return nativeGetCommitOpstamp(nativePtr);
    }

    /**
     * Delete documents matching a field value.
     * @param fieldName Field name to match
     * @param fieldValue Field value to match
     * @return Number of documents deleted
     */
    public long deleteDocuments(String fieldName, Object fieldValue) {
        if (closed) {
            throw new IllegalStateException("IndexWriter has been closed");
        }
        return nativeDeleteDocuments(nativePtr, fieldName, fieldValue);
    }

    /**
     * Delete documents matching a term.
     * @param fieldName Field name to match
     * @param fieldValue Field value to match
     * @return Number of documents deleted
     */
    public long deleteDocumentsByTerm(String fieldName, Object fieldValue) {
        if (closed) {
            throw new IllegalStateException("IndexWriter has been closed");
        }
        return nativeDeleteDocumentsByTerm(nativePtr, fieldName, fieldValue);
    }

    /**
     * Delete documents matching a query.
     * @param query Query to match documents for deletion
     * @return Number of documents deleted
     */
    public long deleteDocumentsByQuery(Query query) {
        if (closed) {
            throw new IllegalStateException("IndexWriter has been closed");
        }
        return nativeDeleteDocumentsByQuery(nativePtr, query.getNativePtr());
    }

    /**
     * Wait for all merging threads to complete.
     */
    public void waitMergingThreads() {
        if (closed) {
            throw new IllegalStateException("IndexWriter has been closed");
        }
        nativeWaitMergingThreads(nativePtr);
    }

    /**
     * Merge segments specified by their segment IDs.
     * This operation combines multiple segments into a single segment,
     * which can improve search performance and reduce storage overhead.
     * 
     * @param segmentIds List of segment ID strings to merge
     * @return SegmentMeta of the resulting merged segment
     * @throws IllegalArgumentException if segmentIds is null or empty
     */
    public SegmentMeta merge(List<String> segmentIds) {
        if (closed) {
            throw new IllegalStateException("IndexWriter has been closed");
        }
        if (segmentIds == null) {
            throw new IllegalArgumentException("Segment IDs list cannot be null");
        }
        if (segmentIds.isEmpty()) {
            throw new IllegalArgumentException("Segment IDs list cannot be empty");
        }
        
        long segmentMetaPtr = nativeMerge(nativePtr, segmentIds);
        if (segmentMetaPtr == 0) {
            throw new RuntimeException("Failed to merge segments");
        }
        return new SegmentMeta(segmentMetaPtr);
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
    private static native long nativeAddDocument(long ptr, long docPtr);
    private static native long nativeAddJson(long ptr, String json);
    private static native long[] nativeAddDocumentsByBuffer(long ptr, java.nio.ByteBuffer buffer);
    private static native long nativeCommit(long ptr);
    private static native long nativeRollback(long ptr);
    private static native void nativeGarbageCollectFiles(long ptr);
    private static native void nativeDeleteAllDocuments(long ptr);
    private static native long nativeGetCommitOpstamp(long ptr);
    private static native long nativeDeleteDocuments(long ptr, String fieldName, Object fieldValue);
    private static native long nativeDeleteDocumentsByTerm(long ptr, String fieldName, Object fieldValue);
    private static native long nativeDeleteDocumentsByQuery(long ptr, long queryPtr);
    private static native void nativeWaitMergingThreads(long ptr);
    private static native long nativeMerge(long ptr, List<String> segmentIds);
    private static native void nativeClose(long ptr);
}

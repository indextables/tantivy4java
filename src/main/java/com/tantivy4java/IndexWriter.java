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
    private static native long nativeCommit(long ptr);
    private static native long nativeRollback(long ptr);
    private static native void nativeGarbageCollectFiles(long ptr);
    private static native void nativeDeleteAllDocuments(long ptr);
    private static native long nativeGetCommitOpstamp(long ptr);
    private static native long nativeDeleteDocuments(long ptr, String fieldName, Object fieldValue);
    private static native long nativeDeleteDocumentsByTerm(long ptr, String fieldName, Object fieldValue);
    private static native long nativeDeleteDocumentsByQuery(long ptr, long queryPtr);
    private static native void nativeWaitMergingThreads(long ptr);
    private static native void nativeClose(long ptr);
}

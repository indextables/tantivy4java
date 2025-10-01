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
import io.indextables.tantivy4java.result.SearchResult;
import io.indextables.tantivy4java.query.Order;
import io.indextables.tantivy4java.aggregation.*;
import java.util.Map;
import java.util.List;
import java.util.ArrayList;

/**
 * Searcher for querying a Tantivy index.
 * Provides search and aggregation capabilities.
 */
public class Searcher implements AutoCloseable {
    static {
        Tantivy.initialize();
    }

    private long nativePtr;
    private boolean closed = false;

    Searcher(long nativePtr) {
        this.nativePtr = nativePtr;
    }

    /**
     * Search the index with a query.
     * @param query Query to execute
     * @param limit Maximum number of results to return
     * @param count Whether to count total matches
     * @param orderByField Field to order results by
     * @param offset Number of results to skip
     * @param order Sort order (ASC or DESC)
     * @return Search results
     */
    public SearchResult search(Query query, int limit, boolean count, 
                             String orderByField, int offset, Order order) {
        if (closed) {
            throw new IllegalStateException("Searcher has been closed");
        }
        long ptr = nativeSearch(nativePtr, query.getNativePtr(), limit, count, 
                               orderByField, offset, order.getValue());
        return new SearchResult(ptr);
    }

    /**
     * Search the index with default options.
     * @param query Query to execute
     * @param limit Maximum number of results to return
     * @return Search results
     */
    public SearchResult search(Query query, int limit) {
        return search(query, limit, true, null, 0, Order.DESC);
    }

    /**
     * Search the index with default limit.
     * @param query Query to execute
     * @return Search results
     */
    public SearchResult search(Query query) {
        return search(query, 10);
    }

    /**
     * Execute an aggregation query.
     * @param searchQuery Search query to filter documents
     * @param aggQuery Aggregation query definition
     * @return Aggregation results as a map
     */
    public Map<String, Object> aggregate(Query searchQuery, Map<String, Object> aggQuery) {
        if (closed) {
            throw new IllegalStateException("Searcher has been closed");
        }
        return nativeAggregate(nativePtr, searchQuery.getNativePtr(), aggQuery);
    }

    /**
     * Get the total number of documents in the index.
     * @return Number of documents
     */
    public int getNumDocs() {
        if (closed) {
            throw new IllegalStateException("Searcher has been closed");
        }
        return nativeGetNumDocs(nativePtr);
    }

    /**
     * Get the number of segments in the index.
     * @return Number of segments
     */
    public int getNumSegments() {
        if (closed) {
            throw new IllegalStateException("Searcher has been closed");
        }
        return nativeGetNumSegments(nativePtr);
    }

    /**
     * Get a document by its address.
     * @param docAddress Document address
     * @return Document instance
     */
    public Document doc(DocAddress docAddress) {
        if (closed) {
            throw new IllegalStateException("Searcher has been closed");
        }
        long ptr = nativeDoc(nativePtr, docAddress.getNativePtr());
        return new Document(ptr);
    }

    /**
     * Get multiple documents by their addresses in a single batch operation.
     * This is significantly more efficient than calling doc() multiple times,
     * especially for large numbers of documents.
     * 
     * @param docAddresses List of document addresses
     * @return List of documents in the same order as the input addresses
     */
    public List<Document> docBatch(List<DocAddress> docAddresses) {
        if (closed) {
            throw new IllegalStateException("Searcher has been closed");
        }
        if (docAddresses == null || docAddresses.isEmpty()) {
            return new ArrayList<>();
        }
        
        // Convert DocAddress list to array of native pointers
        long[] addressPtrs = new long[docAddresses.size()];
        for (int i = 0; i < docAddresses.size(); i++) {
            addressPtrs[i] = docAddresses.get(i).getNativePtr();
        }
        
        // Call native batch retrieval method
        long[] docPtrs = nativeDocBatch(nativePtr, addressPtrs);
        
        // Convert native document pointers to Document objects
        List<Document> documents = new ArrayList<>(docPtrs.length);
        for (long ptr : docPtrs) {
            documents.add(new Document(ptr));
        }
        
        return documents;
    }

    /**
     * Get the document frequency for a field value.
     * @param fieldName Field name
     * @param fieldValue Field value
     * @return Document frequency
     */
    public int docFreq(String fieldName, Object fieldValue) {
        if (closed) {
            throw new IllegalStateException("Searcher has been closed");
        }
        return nativeDocFreq(nativePtr, fieldName, fieldValue);
    }

    /**
     * Get the segment IDs in the index.
     * This method returns a list of segment ID strings that can be used
     * with IndexWriter.merge() to merge specific segments.
     * 
     * @return List of segment ID strings
     */
    public List<String> getSegmentIds() {
        if (closed) {
            throw new IllegalStateException("Searcher has been closed");
        }
        return nativeGetSegmentIds(nativePtr);
    }

    /**
     * Get the native pointer for JNI operations.
     * @return Native pointer
     */
    public long getNativePtr() {
        if (closed) {
            throw new IllegalStateException("Searcher has been closed");
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
    private static native long nativeSearch(long ptr, long queryPtr, int limit, boolean count, String orderByField, int offset, int order);
    private static native Map<String, Object> nativeAggregate(long ptr, long queryPtr, Map<String, Object> aggQuery);
    private static native int nativeGetNumDocs(long ptr);
    private static native int nativeGetNumSegments(long ptr);
    private static native long nativeDoc(long ptr, long docAddressPtr);
    private static native long[] nativeDocBatch(long ptr, long[] docAddressPtrs);
    private static native int nativeDocFreq(long ptr, String fieldName, Object fieldValue);
    private static native List<String> nativeGetSegmentIds(long ptr);
    private static native void nativeClose(long ptr);
}

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
import java.util.Map;

/**
 * Represents a Tantivy index.
 * An index contains documents and provides search capabilities.
 */
public class Index implements AutoCloseable {
    static {
        Tantivy.initialize();
    }

    private long nativePtr;
    private boolean closed = false;
    private String indexPath; // Store the index path for file-based indices

    /**
     * Create a new index with the given schema.
     * @param schema Schema for the index
     * @param path Path to store the index (null for in-memory)
     * @param reuse Whether to reuse existing index at path
     */
    public Index(Schema schema, String path, boolean reuse) {
        this.nativePtr = nativeNew(schema.getNativePtr(), path, reuse);
        this.indexPath = (path != null && !path.isEmpty()) ? path : null;
    }

    /**
     * Create a new in-memory index.
     * @param schema Schema for the index
     */
    public Index(Schema schema) {
        this(schema, null, true);
    }

    /**
     * Create a new index at the specified path.
     * @param schema Schema for the index
     * @param path Path to store the index
     */
    public Index(Schema schema, String path) {
        this(schema, path, true);
    }

    /**
     * Open an existing index from a path.
     * @param path Path to the index
     * @return Index instance
     */
    public static Index open(String path) {
        long ptr = nativeOpen(path);
        Index index = new Index(ptr);
        index.indexPath = path; // Store the path for opened indices
        return index;
    }

    /**
     * Check if an index exists at the given path.
     * @param path Path to check
     * @return true if index exists, false otherwise
     */
    public static boolean exists(String path) {
        return nativeExists(path);
    }

    Index(long nativePtr) {
        this.nativePtr = nativePtr;
        this.indexPath = null; // Path unknown for this constructor
    }

    /**
     * Create a new index writer.
     * @param heapSize Heap size for the writer in bytes
     * @param numThreads Number of indexing threads (0 for default)
     * @return New IndexWriter instance
     */
    public IndexWriter writer(int heapSize, int numThreads) {
        if (closed) {
            throw new IllegalStateException("Index has been closed");
        }
        long ptr = nativeWriter(nativePtr, heapSize, numThreads);
        return new IndexWriter(ptr);
    }

    /**
     * Create a new index writer with default settings.
     * @return New IndexWriter instance
     */
    public IndexWriter writer() {
        return writer(128_000_000, 0);
    }

    /**
     * Configure the index reader.
     * @param reloadPolicy Reload policy ("commit" or "on_commit")
     * @param numWarmers Number of warmer threads
     */
    public void configReader(String reloadPolicy, int numWarmers) {
        if (closed) {
            throw new IllegalStateException("Index has been closed");
        }
        nativeConfigReader(nativePtr, reloadPolicy, numWarmers);
    }

    /**
     * Configure the index reader with default settings.
     */
    public void configReader() {
        configReader("commit", 0);
    }

    /**
     * Get a searcher for this index.
     * @return New Searcher instance
     */
    public Searcher searcher() {
        if (closed) {
            throw new IllegalStateException("Index has been closed");
        }
        long ptr = nativeSearcher(nativePtr);
        return new Searcher(ptr);
    }

    /**
     * Get the schema of this index.
     * @return Schema instance
     */
    public Schema getSchema() {
        if (closed) {
            throw new IllegalStateException("Index has been closed");
        }
        long ptr = nativeGetSchema(nativePtr);
        return new Schema(ptr);
    }

    /**
     * Reload the index to pick up new commits.
     */
    public void reload() {
        if (closed) {
            throw new IllegalStateException("Index has been closed");
        }
        nativeReload(nativePtr);
    }

    /**
     * Parse a query string into a Query object.
     * @param query Query string
     * @param defaultFieldNames Default fields to search in
     * @param fieldBoosts Field boost factors
     * @param fuzzyFields Fuzzy search configuration
     * @return Parsed Query instance
     */
    public Query parseQuery(String query, List<String> defaultFieldNames, 
                          Map<String, Double> fieldBoosts, 
                          Map<String, FuzzyConfig> fuzzyFields) {
        if (closed) {
            throw new IllegalStateException("Index has been closed");
        }
        long ptr = nativeParseQuery(nativePtr, query, defaultFieldNames, fieldBoosts, fuzzyFields);
        return new Query(ptr);
    }

    /**
     * Parse a query string with default settings.
     * @param query Query string
     * @param defaultFieldNames Default fields to search in
     * @return Parsed Query instance
     */
    public Query parseQuery(String query, List<String> defaultFieldNames) {
        return parseQuery(query, defaultFieldNames, null, null);
    }

    /**
     * Parse a query string leniently, returning parsing errors.
     * @param query Query string
     * @param defaultFieldNames Default fields to search in
     * @param fieldBoosts Field boost factors
     * @param fuzzyFields Fuzzy search configuration
     * @return ParseResult with query and errors
     */
    public ParseResult parseQueryLenient(String query, List<String> defaultFieldNames,
                                       Map<String, Double> fieldBoosts,
                                       Map<String, FuzzyConfig> fuzzyFields) {
        if (closed) {
            throw new IllegalStateException("Index has been closed");
        }
        long ptr = nativeParseQueryLenient(nativePtr, query, defaultFieldNames, fieldBoosts, fuzzyFields);
        return new ParseResult(ptr);
    }

    /**
     * Parse a query string leniently with default settings.
     * @param query Query string
     * @param defaultFieldNames Default fields to search in
     * @return ParseResult with query and errors
     */
    public ParseResult parseQueryLenient(String query, List<String> defaultFieldNames) {
        return parseQueryLenient(query, defaultFieldNames, null, null);
    }

    /**
     * Register a custom tokenizer.
     * @param name Tokenizer name
     * @param textAnalyzer Text analyzer to use
     */
    public void registerTokenizer(String name, TextAnalyzer textAnalyzer) {
        if (closed) {
            throw new IllegalStateException("Index has been closed");
        }
        nativeRegisterTokenizer(nativePtr, name, textAnalyzer.getNativePtr());
    }

    /**
     * Get the native pointer for JNI operations.
     * @return Native pointer
     */
    public long getNativePtr() {
        if (closed) {
            throw new IllegalStateException("Index has been closed");
        }
        return nativePtr;
    }

    /**
     * Get the index directory path if this is a file-based index.
     * @return Index path, or null if this is an in-memory index
     */
    public String getIndexPath() {
        if (closed) {
            throw new IllegalStateException("Index has been closed");
        }
        return indexPath;
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
     * Helper class for fuzzy search configuration.
     */
    public static class FuzzyConfig {
        private final boolean prefix;
        private final int distance;
        private final boolean transpositionCostOne;

        public FuzzyConfig(boolean prefix, int distance, boolean transpositionCostOne) {
            this.prefix = prefix;
            this.distance = distance;
            this.transpositionCostOne = transpositionCostOne;
        }

        public boolean isPrefix() { return prefix; }
        public int getDistance() { return distance; }
        public boolean isTranspositionCostOne() { return transpositionCostOne; }
    }

    /**
     * Result of lenient query parsing.
     */
    public static class ParseResult implements AutoCloseable {
        private long nativePtr;
        private boolean closed = false;

        ParseResult(long nativePtr) {
            this.nativePtr = nativePtr;
        }

        /**
         * Get the parsed query.
         * @return Query instance
         */
        public Query getQuery() {
            if (closed) {
                throw new IllegalStateException("ParseResult has been closed");
            }
            long ptr = nativeGetQuery(nativePtr);
            return new Query(ptr);
        }

        /**
         * Get parsing errors.
         * @return List of error messages
         */
        public List<String> getErrors() {
            if (closed) {
                throw new IllegalStateException("ParseResult has been closed");
            }
            return nativeGetErrors(nativePtr);
        }

        @Override
        public void close() {
            if (!closed) {
                nativeCloseParseResult(nativePtr);
                closed = true;
                nativePtr = 0;
            }
        }


        private static native long nativeGetQuery(long ptr);
        private static native List<String> nativeGetErrors(long ptr);
        private static native void nativeCloseParseResult(long ptr);
    }

    /**
     * Create a QuickwitSplitGenerator for this index (if quickwit-splits4java is available).
     * 
     * @param targetDocsPerSplit Target number of documents per split
     * @return QuickwitSplitGenerator instance
     * @throws UnsupportedOperationException if quickwit-splits4java is not available
     * @throws IllegalArgumentException if targetDocsPerSplit <= 0
     */
    public Object createSplitGenerator(int targetDocsPerSplit) {
        if (closed) {
            throw new IllegalStateException("Index has been closed");
        }
        
        try {
            // Use reflection to create QuickwitSplitGenerator if available
            Class<?> generatorClass = Class.forName("com.tantivy4java.splits.QuickwitSplitGenerator");
            return generatorClass.getConstructor(Index.class, int.class)
                    .newInstance(this, targetDocsPerSplit);
        } catch (ClassNotFoundException e) {
            throw new UnsupportedOperationException(
                "QuickwitSplitGenerator not available. Add quickwit-splits4java dependency.", e);
        } catch (Exception e) {
            throw new RuntimeException("Failed to create QuickwitSplitGenerator", e);
        }
    }

    // Native method declarations
    private static native long nativeNew(long schemaPtr, String path, boolean reuse);
    private static native long nativeOpen(String path);
    private static native boolean nativeExists(String path);
    private static native long nativeWriter(long ptr, int heapSize, int numThreads);
    private static native void nativeConfigReader(long ptr, String reloadPolicy, int numWarmers);
    private static native long nativeSearcher(long ptr);
    private static native long nativeGetSchema(long ptr);
    private static native void nativeReload(long ptr);
    private static native long nativeParseQuery(long ptr, String query, List<String> defaultFieldNames, Map<String, Double> fieldBoosts, Map<String, FuzzyConfig> fuzzyFields);
    private static native long nativeParseQueryLenient(long ptr, String query, List<String> defaultFieldNames, Map<String, Double> fieldBoosts, Map<String, FuzzyConfig> fuzzyFields);
    private static native void nativeRegisterTokenizer(long ptr, String name, long textAnalyzerPtr);
    private static native void nativeClose(long ptr);
}

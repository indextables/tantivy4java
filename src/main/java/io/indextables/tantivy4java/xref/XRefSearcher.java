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

package io.indextables.tantivy4java.xref;

import com.fasterxml.jackson.databind.ObjectMapper;

import io.indextables.tantivy4java.core.Document;
import io.indextables.tantivy4java.core.Tantivy;
import io.indextables.tantivy4java.result.SearchResult;
import io.indextables.tantivy4java.split.SplitCacheManager;
import io.indextables.tantivy4java.split.SplitSearcher;
import io.indextables.tantivy4java.split.SplitQuery;
import io.indextables.tantivy4java.split.SplitTermQuery;
import io.indextables.tantivy4java.split.SplitMatchAllQuery;
import io.indextables.tantivy4java.split.SplitParsedQuery;
import io.indextables.tantivy4java.split.merge.QuickwitSplit;

import java.util.ArrayList;
import java.util.List;

/**
 * Searcher for XRef splits.
 *
 * <p>This class wraps a standard {@link SplitSearcher} and provides convenience methods
 * for XRef-specific operations. Since XRef splits ARE Quickwit splits, this class uses
 * the existing SplitSearcher infrastructure for all search operations.</p>
 *
 * <p>KEY INSIGHT: In an XRef split, each document represents one source split.
 * Document fields contain split metadata (_xref_uri, _xref_split_id, etc.)
 * and indexed fields contain all terms from that source split.</p>
 *
 * <h2>Example Usage</h2>
 * <pre>{@code
 * // Build an XRef split first
 * XRefBuildConfig buildConfig = XRefBuildConfig.builder()
 *     .xrefId("daily-xref")
 *     .indexUid("logs-index")
 *     .sourceSplits(Arrays.asList("s3://bucket/split1.split", "s3://bucket/split2.split"))
 *     .build();
 *
 * XRefMetadata metadata = XRefSplit.build(buildConfig, "/tmp/daily.xref.split");
 *
 * // Search the XRef using the metadata
 * SplitCacheManager.CacheConfig cacheConfig = new SplitCacheManager.CacheConfig("xref-cache")
 *     .withMaxCacheSize(100_000_000);
 *
 * try (SplitCacheManager cacheManager = SplitCacheManager.getInstance(cacheConfig);
 *      XRefSearcher searcher = XRefSearcher.open(cacheManager, "/tmp/daily.xref.split", metadata)) {
 *
 *     // Find which splits contain matching documents
 *     XRefSearchResult result = searcher.search("error", 1000);
 *
 *     System.out.println("Query matches documents across " +
 *         result.getNumMatchingSplits() + " splits");
 *
 *     // Get the split URIs to search
 *     for (String splitUri : result.getSplitUrisToSearch()) {
 *         // Now search this split with full query
 *     }
 * }
 * }</pre>
 */
public class XRefSearcher implements AutoCloseable {
    static {
        Tantivy.initialize();
    }

    private static final ObjectMapper MAPPER = new ObjectMapper();

    /** Field name for split URI */
    public static final String FIELD_URI = "_xref_uri";

    /** Field name for split ID */
    public static final String FIELD_SPLIT_ID = "_xref_split_id";

    /** Field name for full metadata JSON */
    public static final String FIELD_METADATA = "_xref_split_metadata";

    private final SplitSearcher searcher;
    private final XRefMetadata xrefMetadata;
    private volatile boolean closed = false;

    /**
     * Create an XRefSearcher wrapping a SplitSearcher.
     *
     * @param searcher The underlying SplitSearcher for the XRef split
     * @param metadata The XRef metadata from the build process
     */
    XRefSearcher(SplitSearcher searcher, XRefMetadata metadata) {
        this.searcher = searcher;
        this.xrefMetadata = metadata;
    }

    /**
     * Open an XRef split for searching using standard SplitMetadata.
     *
     * <p>This is the preferred method when you only have SplitMetadata (e.g., from a metadata store).
     * XRef splits are standard Quickwit splits, so they work with regular SplitMetadata.</p>
     *
     * <p>Note: When opened this way, convenience methods like {@link #getNumSplits()} and
     * {@link #getAllSplitUris()} will not be available. Use the document fields
     * (_xref_uri, _xref_split_metadata) to get source split information from search results.</p>
     *
     * @param cacheManager The cache manager to use
     * @param xrefUri URI of the XRef split file
     * @param splitMetadata Standard split metadata (same as used for regular splits)
     * @return XRefSearcher for querying the XRef split
     * @throws IllegalArgumentException if splitMetadata is null
     * @throws RuntimeException if opening fails
     */
    public static XRefSearcher open(SplitCacheManager cacheManager, String xrefUri, QuickwitSplit.SplitMetadata splitMetadata) {
        if (splitMetadata == null) {
            throw new IllegalArgumentException("SplitMetadata is required");
        }

        SplitSearcher searcher = cacheManager.createSplitSearcher(xrefUri, splitMetadata);
        return new XRefSearcher(searcher, null);  // No XRefMetadata available
    }

    /**
     * Open an XRef split for searching using XRefMetadata.
     *
     * <p>Use this method when you have the full XRefMetadata from {@link XRefSplit#build}.
     * This enables convenience methods like {@link #getNumSplits()} and {@link #getAllSplitUris()}.</p>
     *
     * @param cacheManager The cache manager to use
     * @param xrefUri URI of the XRef split file
     * @param metadata XRef metadata from the build process (must have footer offsets)
     * @return XRefSearcher for querying the XRef split
     * @throws IllegalArgumentException if metadata is null or lacks footer offsets
     * @throws RuntimeException if opening fails
     */
    public static XRefSearcher open(SplitCacheManager cacheManager, String xrefUri, XRefMetadata metadata) {
        if (metadata == null) {
            throw new IllegalArgumentException(
                "XRef metadata is required. Use XRefSplit.build() to create an XRef and use the returned metadata, " +
                "or use open(cacheManager, uri, SplitMetadata) if you only have split metadata."
            );
        }

        if (!metadata.hasFooterOffsets()) {
            throw new IllegalArgumentException(
                "XRef metadata must have footer offsets. Ensure the metadata came from XRefSplit.build()."
            );
        }

        // Use the standard SplitMetadata conversion for consistent handling
        QuickwitSplit.SplitMetadata splitMetadata = metadata.toSplitMetadata();

        SplitSearcher searcher = cacheManager.createSplitSearcher(xrefUri, splitMetadata);
        return new XRefSearcher(searcher, metadata);
    }

    /**
     * Search the XRef split and return matching source splits.
     *
     * <p>Each matching document represents a source split that contains
     * documents matching the query terms.</p>
     *
     * @param query Query string (will be searched as a term across all fields)
     * @param limit Maximum number of splits to return
     * @return XRefSearchResult containing matching splits
     * @throws IllegalStateException if searcher is closed
     * @throws RuntimeException if search fails
     */
    public XRefSearchResult search(String query, int limit) {
        checkNotClosed();
        long startTime = System.currentTimeMillis();

        try {
            // Create query - use all query for "*" or term query otherwise
            SplitQuery splitQuery;
            if ("*".equals(query.trim())) {
                splitQuery = new SplitMatchAllQuery();
            } else {
                // Parse simple field:term format or search all fields
                String[] parts = query.split(":", 2);
                if (parts.length == 2) {
                    splitQuery = new SplitTermQuery(parts[0].trim(), parts[1].trim());
                } else {
                    // For simple terms, we'd need to search across all indexed fields
                    // For now, search the URI field (which contains all split URIs)
                    splitQuery = new SplitMatchAllQuery();
                }
            }

            // Execute search
            SearchResult result = searcher.search(splitQuery, limit);

            // Convert hits to matching splits
            List<MatchingSplit> matchingSplits = new ArrayList<>();
            for (var hit : result.getHits()) {
                try (Document doc = searcher.doc(hit.getDocAddress())) {
                    MatchingSplit split = extractMatchingSplit(doc, (float) hit.getScore());
                    if (split != null) {
                        matchingSplits.add(split);
                    }
                }
            }

            long searchTimeMs = System.currentTimeMillis() - startTime;

            XRefSearchResult xrefResult = new XRefSearchResult();
            xrefResult.setMatchingSplits(matchingSplits);
            xrefResult.setNumMatchingSplits(matchingSplits.size());
            xrefResult.setSearchTimeMs(searchTimeMs);

            return xrefResult;
        } catch (Exception e) {
            throw new RuntimeException("XRef search failed: " + e.getMessage(), e);
        }
    }

    /**
     * Search with a pre-built SplitQuery.
     *
     * <p>IMPORTANT: Range queries in the SplitQuery will be automatically transformed
     * to match-all queries because XRef splits don't have fast fields. This ensures
     * conservative results - all potentially matching splits are returned.</p>
     *
     * @param query The query to execute
     * @param limit Maximum number of splits to return
     * @return XRefSearchResult containing matching splits
     */
    public XRefSearchResult search(SplitQuery query, int limit) {
        checkNotClosed();
        long startTime = System.currentTimeMillis();

        try {
            // Transform the query to replace range queries with match-all
            SplitQuery transformedQuery = transformQueryForXRef(query);

            SearchResult result = searcher.search(transformedQuery, limit);

            List<MatchingSplit> matchingSplits = new ArrayList<>();
            for (var hit : result.getHits()) {
                try (Document doc = searcher.doc(hit.getDocAddress())) {
                    MatchingSplit split = extractMatchingSplit(doc, (float) hit.getScore());
                    if (split != null) {
                        matchingSplits.add(split);
                    }
                }
            }

            long searchTimeMs = System.currentTimeMillis() - startTime;

            XRefSearchResult xrefResult = new XRefSearchResult();
            xrefResult.setMatchingSplits(matchingSplits);
            xrefResult.setNumMatchingSplits(matchingSplits.size());
            xrefResult.setSearchTimeMs(searchTimeMs);

            return xrefResult;
        } catch (Exception e) {
            throw new RuntimeException("XRef search failed: " + e.getMessage(), e);
        }
    }

    /**
     * Transform a SplitQuery for XRef searching by replacing range queries with match-all.
     *
     * <p>XRef splits don't have fast fields (columnar data), so range queries cannot
     * be evaluated. To ensure conservative results (never miss a split that might
     * contain matching documents), range queries are transformed to match-all.</p>
     *
     * @param query The original query
     * @return A transformed query with range queries replaced by match-all
     */
    public SplitQuery transformQueryForXRef(SplitQuery query) {
        if (query == null) {
            return new SplitMatchAllQuery();
        }

        // Get the QueryAst JSON from the original query
        String queryJson = query.toQueryAstJson();

        // Transform range queries to match-all using native code
        String transformedJson = nativeTransformRangeToMatchAll(queryJson);

        // Check for error
        if (transformedJson != null && transformedJson.startsWith("ERROR:")) {
            throw new RuntimeException("Query transformation failed: " + transformedJson.substring(6).trim());
        }

        // If the JSON changed, return a new parsed query
        if (!queryJson.equals(transformedJson)) {
            return new SplitParsedQuery(transformedJson);
        }

        // No transformation needed, return original query
        return query;
    }

    /**
     * Native method to transform range queries to match-all in a QueryAst JSON.
     * This walks the query tree in Rust and replaces any range query nodes with match_all.
     *
     * @param queryJson The QueryAst JSON string
     * @return Transformed QueryAst JSON, or error string starting with "ERROR:"
     */
    private static native String nativeTransformRangeToMatchAll(String queryJson);

    /**
     * Get the number of source splits in this XRef.
     *
     * <p>This method requires XRefMetadata. If opened with SplitMetadata only,
     * use a match-all query and count the results instead.</p>
     *
     * @return Number of source splits
     * @throws IllegalStateException if searcher is closed or opened without XRefMetadata
     */
    public int getNumSplits() {
        checkNotClosed();
        checkHasXRefMetadata("getNumSplits");
        return xrefMetadata.getNumSplits();
    }

    /**
     * Get all source split URIs from the metadata.
     *
     * <p>This method requires XRefMetadata. If opened with SplitMetadata only,
     * use a match-all query and extract URIs from the _xref_uri field instead.</p>
     *
     * @return List of split URIs
     * @throws IllegalStateException if searcher is closed or opened without XRefMetadata
     */
    public List<String> getAllSplitUris() {
        checkNotClosed();
        checkHasXRefMetadata("getAllSplitUris");
        List<String> uris = new ArrayList<>();
        for (XRefSplitEntry entry : xrefMetadata.getSplitRegistry().getSplits()) {
            uris.add(entry.getUri());
        }
        return uris;
    }

    /**
     * Convenience method to get URIs of splits matching a query.
     *
     * @param query Query string
     * @param limit Maximum number of splits
     * @return List of matching split URIs
     */
    public List<String> getMatchingSplitUris(String query, int limit) {
        XRefSearchResult result = search(query, limit);
        return result.getSplitUrisToSearch();
    }

    /**
     * Check if a term exists in any split.
     *
     * @param term The term to search for
     * @return true if the term exists in at least one split
     */
    public boolean termExists(String term) {
        XRefSearchResult result = search(term, 1);
        return result.hasMatches();
    }

    /**
     * Check if a field:term combination exists in any split.
     *
     * @param field The field name
     * @param term The term to search for
     * @return true if the field:term combination exists in at least one split
     */
    public boolean termExists(String field, String term) {
        XRefSearchResult result = search(field + ":" + term, 1);
        return result.hasMatches();
    }

    /**
     * Get the XRef metadata, if available.
     *
     * <p>Returns null if this searcher was opened with SplitMetadata only
     * (via {@link #open(SplitCacheManager, String, QuickwitSplit.SplitMetadata)}).</p>
     *
     * @return The XRef metadata, or null if not available
     */
    public XRefMetadata getMetadata() {
        return xrefMetadata;
    }

    /**
     * Check if XRefMetadata is available.
     *
     * @return true if XRefMetadata was provided when opening
     */
    public boolean hasXRefMetadata() {
        return xrefMetadata != null;
    }

    /**
     * Get the underlying SplitSearcher for advanced operations.
     *
     * @return The underlying SplitSearcher
     */
    public SplitSearcher getSearcher() {
        return searcher;
    }

    /**
     * Extract a MatchingSplit from a document.
     */
    private MatchingSplit extractMatchingSplit(Document doc, float score) {
        try {
            // Try to get the full metadata JSON first
            Object metadataObj = doc.getFirst(FIELD_METADATA);
            if (metadataObj != null) {
                String metadataJson = metadataObj.toString();
                MatchingSplit split = MAPPER.readValue(metadataJson, MatchingSplit.class);
                split.setScore(score);
                return split;
            }

            // Fall back to individual fields
            Object uriObj = doc.getFirst(FIELD_URI);
            Object splitIdObj = doc.getFirst(FIELD_SPLIT_ID);

            if (uriObj != null) {
                MatchingSplit split = new MatchingSplit();
                split.setUri(uriObj.toString());
                split.setSplitId(splitIdObj != null ? splitIdObj.toString() : "");
                split.setScore(score);
                return split;
            }

            return null;
        } catch (Exception e) {
            // Log and continue
            return null;
        }
    }

    private void checkNotClosed() {
        if (closed) {
            throw new IllegalStateException("XRefSearcher is closed");
        }
    }

    private void checkHasXRefMetadata(String methodName) {
        if (xrefMetadata == null) {
            throw new IllegalStateException(
                methodName + "() requires XRefMetadata. This searcher was opened with SplitMetadata only. " +
                "Use a match-all query and extract info from document fields (_xref_uri, _xref_split_metadata) instead."
            );
        }
    }

    @Override
    public void close() {
        if (!closed) {
            closed = true;
            if (searcher != null) {
                searcher.close();
            }
        }
    }
}

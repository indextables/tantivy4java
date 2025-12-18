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

import io.indextables.tantivy4java.core.Tantivy;
import io.indextables.tantivy4java.split.SplitCacheManager;
import io.indextables.tantivy4java.split.SplitQuery;
import io.indextables.tantivy4java.split.SplitMatchAllQuery;
import io.indextables.tantivy4java.split.merge.QuickwitSplit;

import java.util.ArrayList;
import java.util.List;

/**
 * Searcher for FuseXRef (Binary Fuse Filter) splits.
 *
 * <p>FuseXRef is a compact index format that uses Binary Fuse8 filters for fast
 * split-level query routing. Each filter stores hashed field:term pairs from
 * a source split, enabling quick identification of which splits may contain
 * documents matching a query.</p>
 *
 * <p>KEY INSIGHT: FuseXRef uses a custom binary format (NOT Quickwit split format).
 * This searcher uses native FuseXRef operations for all queries.</p>
 *
 * <h2>Example Usage</h2>
 * <pre>{@code
 * // Build an XRef split first
 * XRefBuildConfig buildConfig = XRefBuildConfig.builder()
 *     .xrefId("daily-xref")
 *     .indexUid("logs-index")
 *     .sourceSplits(Arrays.asList(source1, source2))
 *     .build();
 *
 * XRefMetadata metadata = XRefSplit.build(buildConfig, "/tmp/daily.xref.split");
 *
 * // Search the XRef
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
public class XRefSearcher implements AutoCloseable, io.indextables.tantivy4java.split.QueryParser {
    static {
        Tantivy.initialize();
    }

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final long handle;
    private final String xrefPath;
    private final XRefMetadata xrefMetadata;
    private volatile boolean closed = false;

    /**
     * Create an XRefSearcher with a native handle.
     *
     * @param handle The native FuseXRef handle
     * @param xrefPath Path to the XRef file
     * @param metadata The XRef metadata from the build process (optional)
     */
    XRefSearcher(long handle, String xrefPath, XRefMetadata metadata) {
        this.handle = handle;
        this.xrefPath = xrefPath;
        this.xrefMetadata = metadata;
    }

    /**
     * Open an XRef split for searching using XRefMetadata.
     *
     * <p>Use this method when you have the full XRefMetadata from {@link XRefSplit#build}.
     * This enables convenience methods like {@link #getNumSplits()} and {@link #getAllSplitUris()}.</p>
     *
     * <p>Supports local files, S3 URLs (s3://), and Azure URLs (azure://).</p>
     *
     * @param cacheManager The cache manager with cloud credentials
     * @param xrefUri URI of the XRef split file (file://, s3://, azure://)
     * @param metadata XRef metadata from the build process
     * @return XRefSearcher for querying the XRef split
     * @throws IllegalArgumentException if metadata is null
     * @throws RuntimeException if opening fails
     */
    public static XRefSearcher open(SplitCacheManager cacheManager, String xrefUri, XRefMetadata metadata) {
        if (metadata == null) {
            throw new IllegalArgumentException(
                "XRef metadata is required. Use XRefSplit.build() to create an XRef and use the returned metadata."
            );
        }

        long handle;
        String pathForDisplay = xrefUri;

        if (xrefUri.startsWith("s3://") || xrefUri.startsWith("azure://")) {
            // Remote URL - use native loader with credentials
            java.util.Map<String, String> awsConfig = cacheManager != null ? cacheManager.getAwsConfig() : java.util.Collections.emptyMap();
            java.util.Map<String, String> azureConfig = cacheManager != null ? cacheManager.getAzureConfig() : java.util.Collections.emptyMap();

            // Get endpoint - check both "endpoint" (SplitCacheManager) and "endpoint_url" (legacy)
            String endpoint = awsConfig.get("endpoint");
            if (endpoint == null) {
                endpoint = awsConfig.get("endpoint_url");
            }

            // Get path style access - check both "path_style_access" (SplitCacheManager) and "force_path_style" (legacy)
            boolean pathStyleAccess = "true".equals(awsConfig.get("path_style_access"))
                || "true".equals(awsConfig.get("force_path_style"));

            handle = nativeLoadFuseXRefFromUri(
                xrefUri,
                // AWS credentials
                awsConfig.get("access_key"),
                awsConfig.get("secret_key"),
                awsConfig.get("session_token"),
                awsConfig.get("region"),
                endpoint,
                pathStyleAccess,
                // Azure credentials
                azureConfig.get("account_name"),
                azureConfig.get("access_key"),
                azureConfig.get("bearer_token")
            );
        } else {
            // Local file path
            String path = xrefUri;
            if (path.startsWith("file://")) {
                path = path.substring(7);
            }
            pathForDisplay = path;
            handle = nativeLoadFuseXRef(path);
        }

        if (handle < 0) {
            throw new RuntimeException("Failed to open FuseXRef: " + pathForDisplay);
        }

        return new XRefSearcher(handle, pathForDisplay, metadata);
    }

    /**
     * Open an XRef split for searching using standard SplitMetadata.
     *
     * <p>Note: When opened this way, convenience methods like {@link #getNumSplits()} and
     * {@link #getAllSplitUris()} will not be available.</p>
     *
     * @param cacheManager The cache manager (not used for FuseXRef)
     * @param xrefUri URI of the XRef split file
     * @param splitMetadata Standard split metadata (not used for FuseXRef, but kept for API compatibility)
     * @return XRefSearcher for querying the XRef split
     * @throws RuntimeException if opening fails
     */
    public static XRefSearcher open(SplitCacheManager cacheManager, String xrefUri, QuickwitSplit.SplitMetadata splitMetadata) {
        long handle;
        String pathForDisplay = xrefUri;

        if (xrefUri.startsWith("s3://") || xrefUri.startsWith("azure://")) {
            // Remote URL - use native loader with credentials
            java.util.Map<String, String> awsConfig = cacheManager != null ? cacheManager.getAwsConfig() : java.util.Collections.emptyMap();
            java.util.Map<String, String> azureConfig = cacheManager != null ? cacheManager.getAzureConfig() : java.util.Collections.emptyMap();

            // Get endpoint - check both "endpoint" (SplitCacheManager) and "endpoint_url" (legacy)
            String endpoint = awsConfig.get("endpoint");
            if (endpoint == null) {
                endpoint = awsConfig.get("endpoint_url");
            }

            // Get path style access - check both "path_style_access" (SplitCacheManager) and "force_path_style" (legacy)
            boolean pathStyleAccess = "true".equals(awsConfig.get("path_style_access"))
                || "true".equals(awsConfig.get("force_path_style"));

            handle = nativeLoadFuseXRefFromUri(
                xrefUri,
                // AWS credentials
                awsConfig.get("access_key"),
                awsConfig.get("secret_key"),
                awsConfig.get("session_token"),
                awsConfig.get("region"),
                endpoint,
                pathStyleAccess,
                // Azure credentials
                azureConfig.get("account_name"),
                azureConfig.get("access_key"),
                azureConfig.get("bearer_token")
            );
        } else {
            // Local file path - strip file:// prefix if present
            String path = xrefUri;
            if (path.startsWith("file://")) {
                path = path.substring(7);
            }
            pathForDisplay = path;
            handle = nativeLoadFuseXRef(path);
        }

        if (handle < 0) {
            throw new RuntimeException("Failed to open FuseXRef: " + pathForDisplay);
        }

        return new XRefSearcher(handle, pathForDisplay, null);
    }

    /**
     * Search the XRef split using Quickwit query syntax.
     *
     * <p>This method parses the query string using the schema stored in the FuseXRef,
     * allowing you to use Quickwit query syntax without needing a SplitSearcher.</p>
     *
     * <p>Supported query syntax:</p>
     * <ul>
     *   <li>{@code field:value} - term query</li>
     *   <li>{@code field:value AND field2:value2} - boolean AND</li>
     *   <li>{@code field:value OR field2:value2} - boolean OR</li>
     *   <li>{@code NOT field:value} - boolean NOT</li>
     *   <li>{@code *} - match all</li>
     *   <li>Unqualified terms search across all text fields</li>
     * </ul>
     *
     * <p>Note: Range and wildcard queries cannot be evaluated by filters and will
     * cause all splits to be returned (with hasUnevaluatedClauses=true).</p>
     *
     * @param query Query string in Quickwit syntax
     * @param limit Maximum number of splits to return
     * @return XRefSearchResult containing matching splits
     * @throws IllegalStateException if searcher is closed
     * @throws RuntimeException if search fails
     */
    public XRefSearchResult search(String query, int limit) {
        checkNotClosed();
        long startTime = System.currentTimeMillis();

        try {
            // Use native query string parsing which leverages the stored schema
            String resultJson = nativeSearchWithQueryString(handle, query, limit);

            if (resultJson == null || resultJson.startsWith("ERROR:")) {
                String error = resultJson == null ? "null result" : resultJson.substring(6).trim();
                throw new RuntimeException("FuseXRef search failed: " + error);
            }

            XRefSearchResult result = MAPPER.readValue(resultJson, XRefSearchResult.class);
            result.setSearchTimeMs(System.currentTimeMillis() - startTime);

            return result;
        } catch (Exception e) {
            throw new RuntimeException("XRef search failed: " + e.getMessage(), e);
        }
    }

    /**
     * Search with a pre-built SplitQuery.
     *
     * <p>IMPORTANT: Range queries and wildcard queries cannot be evaluated by
     * Binary Fuse Filters. When such queries are detected, hasUnevaluatedClauses
     * will be set to true in the result, and all splits will be returned.</p>
     *
     * @param query The query to execute
     * @param limit Maximum number of splits to return
     * @return XRefSearchResult containing matching splits
     */
    public XRefSearchResult search(SplitQuery query, int limit) {
        checkNotClosed();
        long startTime = System.currentTimeMillis();

        try {
            String queryJson = query.toQueryAstJson();
            String resultJson = nativeSearchFuseXRef(handle, queryJson, limit);

            if (resultJson == null || resultJson.startsWith("ERROR:")) {
                String error = resultJson == null ? "null result" : resultJson.substring(6).trim();
                throw new RuntimeException("FuseXRef search failed: " + error);
            }

            XRefSearchResult result = MAPPER.readValue(resultJson, XRefSearchResult.class);
            result.setSearchTimeMs(System.currentTimeMillis() - startTime);

            return result;
        } catch (Exception e) {
            throw new RuntimeException("XRef search failed: " + e.getMessage(), e);
        }
    }

    /**
     * Transform a SplitQuery for XRef searching by replacing range queries with match-all.
     *
     * <p>FuseXRef (Binary Fuse Filters) cannot evaluate range queries because they
     * don't store term values, only hashes. To ensure conservative results (never
     * miss a split that might contain matching documents), range queries are
     * transformed to match-all.</p>
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
            return new io.indextables.tantivy4java.split.SplitParsedQuery(transformedJson);
        }

        // No transformation needed, return original query
        return query;
    }

    /**
     * Get the number of source splits in this XRef.
     *
     * <p>This method requires XRefMetadata. If opened with SplitMetadata only,
     * use the native split count method instead.</p>
     *
     * @return Number of source splits
     * @throws IllegalStateException if searcher is closed
     */
    public int getNumSplits() {
        checkNotClosed();
        if (xrefMetadata != null) {
            return xrefMetadata.getNumSplits();
        }
        return nativeGetFuseXRefSplitCount(handle);
    }

    /**
     * Get all source split URIs from the metadata.
     *
     * <p>This method requires XRefMetadata.</p>
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
     * Get the schema stored in the FuseXRef.
     *
     * <p>The schema is preserved from the source splits during XRef build
     * and can be used for query construction and validation.</p>
     *
     * @return The schema for this XRef
     * @throws IllegalStateException if searcher is closed
     */
    @Override
    public io.indextables.tantivy4java.core.Schema getSchema() {
        checkNotClosed();
        long schemaPtr = nativeGetSchema(handle);
        if (schemaPtr <= 0) {
            throw new RuntimeException("Failed to get schema from FuseXRef");
        }
        return new io.indextables.tantivy4java.core.Schema(schemaPtr);
    }

    /**
     * Get the XRef metadata, if available.
     *
     * <p>Returns null if this searcher was opened without XRefMetadata.</p>
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
     * Get the native handle for advanced operations.
     *
     * @return The native FuseXRef handle
     */
    public long getHandle() {
        return handle;
    }

    private void checkNotClosed() {
        if (closed) {
            throw new IllegalStateException("XRefSearcher is closed");
        }
    }

    private void checkHasXRefMetadata(String methodName) {
        if (xrefMetadata == null) {
            throw new IllegalStateException(
                methodName + "() requires XRefMetadata. This searcher was opened without XRefMetadata."
            );
        }
    }

    private static String escapeJson(String s) {
        return s.replace("\\", "\\\\")
                .replace("\"", "\\\"")
                .replace("\n", "\\n")
                .replace("\r", "\\r")
                .replace("\t", "\\t");
    }

    @Override
    public void close() {
        if (!closed) {
            closed = true;
            nativeCloseFuseXRef(handle);
        }
    }

    /**
     * Parse a query string using Quickwit syntax and return as a SplitQuery.
     *
     * <p>This method parses the query string using the schema stored in the FuseXRef,
     * returning a SplitQuery that can be used with {@link #search(SplitQuery, int)}.</p>
     *
     * <p>This provides API compatibility with {@link io.indextables.tantivy4java.split.SplitSearcher#parseQuery(String)},
     * allowing the same query objects to be used with both searchers.</p>
     *
     * @param queryString Query string in Quickwit syntax (e.g., "field:value", "age:[1 TO 100]")
     * @return SplitQuery that can be used with search(SplitQuery, limit)
     * @throws IllegalStateException if searcher is closed
     * @throws RuntimeException if parsing fails
     */
    public SplitQuery parseQuery(String queryString) {
        checkNotClosed();

        String queryJson = nativeParseQuery(handle, queryString);
        if (queryJson == null || queryJson.startsWith("ERROR:")) {
            String error = queryJson == null ? "null result" : queryJson.substring(6).trim();
            throw new RuntimeException("Query parsing failed: " + error);
        }

        return new io.indextables.tantivy4java.split.SplitParsedQuery(queryJson);
    }

    /**
     * Parse a query string and return the raw QueryAst JSON.
     *
     * <p>This method is useful for debugging or when you need to inspect
     * the parsed query structure.</p>
     *
     * @param queryString Query string in Quickwit syntax
     * @return JSON representation of the parsed QueryAst
     * @throws IllegalStateException if searcher is closed
     * @throws RuntimeException if parsing fails
     */
    public String parseQueryToJson(String queryString) {
        checkNotClosed();

        String result = nativeParseQuery(handle, queryString);
        if (result == null || result.startsWith("ERROR:")) {
            String error = result == null ? "null result" : result.substring(6).trim();
            throw new RuntimeException("Query parsing failed: " + error);
        }

        return result;
    }

    /**
     * Parse a query string and return as a SplitQuery.
     *
     * @param queryString Query string in Quickwit syntax
     * @return SplitQuery that can be used with search(SplitQuery, limit)
     * @deprecated Use {@link #parseQuery(String)} instead
     */
    @Deprecated
    public SplitQuery parseQueryToSplitQuery(String queryString) {
        return parseQuery(queryString);
    }

    // Native methods for FuseXRef operations
    private static native long nativeLoadFuseXRef(String path);
    private static native long nativeLoadFuseXRefFromUri(
        String uri,
        // AWS credentials
        String awsAccessKey,
        String awsSecretKey,
        String awsSessionToken,
        String awsRegion,
        String awsEndpointUrl,
        boolean awsForcePathStyle,
        // Azure credentials
        String azureAccountName,
        String azureAccountKey,
        String azureBearerToken
    );
    private static native void nativeCloseFuseXRef(long handle);
    private static native String nativeSearchFuseXRef(long handle, String queryJson, int limit);
    private static native String nativeSearchWithQueryString(long handle, String queryString, int limit);
    private static native String nativeParseQuery(long handle, String queryString);
    private static native String nativeGetFuseXRefMetadata(long handle);
    private static native int nativeGetFuseXRefSplitCount(long handle);
    private static native String nativeTransformRangeToMatchAll(String queryJson);
    private static native long nativeGetSchema(long handle);
}

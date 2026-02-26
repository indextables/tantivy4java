package io.indextables.tantivy4java.split;

import io.indextables.tantivy4java.batch.BatchDocumentReader;
import io.indextables.tantivy4java.core.DocAddress;

import io.indextables.tantivy4java.core.Document;
import io.indextables.tantivy4java.core.MapBackedDocument;
import io.indextables.tantivy4java.core.Schema;
import io.indextables.tantivy4java.query.Query;
import io.indextables.tantivy4java.result.SearchResult;
import io.indextables.tantivy4java.core.Tantivy;
import java.nio.file.Path;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.CompletableFuture;

/**
 * SplitSearcher provides efficient search capabilities over Quickwit split files
 * with hot cache optimization and incremental loading of index components.
 * 
 * <h3>Shared Cache Architecture</h3>
 * SplitSearcher instances MUST be created through {@link SplitCacheManager#createSplitSearcher(String)}
 * to ensure proper shared cache management. All SplitSearcher instances created through the same
 * SplitCacheManager share global caches for:
 * <ul>
 *   <li>LeafSearchCache - Query result caching per split_id + query</li>
 *   <li>ByteRangeCache - Storage byte range caching per file_path + range</li>
 *   <li>ComponentCache - Index component caching (fast fields, postings, etc.)</li>
 * </ul>
 * 
 * <h3>Proper Usage Pattern</h3>
 * <pre>{@code
 * // Create shared cache manager (reuse across application)
 * SplitCacheManager.CacheConfig config = new SplitCacheManager.CacheConfig("main-cache")
 *     .withMaxCacheSize(200_000_000); // 200MB shared across all splits
 * SplitCacheManager cacheManager = SplitCacheManager.getInstance(config);
 * 
 * // Create searchers that share the cache
 * SplitSearcher searcher1 = cacheManager.createSplitSearcher("s3://bucket/split1.split");
 * SplitSearcher searcher2 = cacheManager.createSplitSearcher("s3://bucket/split2.split");
 * }</pre>
 */
public class SplitSearcher implements AutoCloseable {
    
    static {
        Tantivy.initialize();
    }

    private long nativePtr;
    private final String splitPath;
    // Configuration now comes from SplitCacheManager
    private final SplitCacheManager cacheManager;
    
    
    /**
     * Index components that can be preloaded for optimal performance.
     *
     * Preloading components before search can significantly improve latency by
     * fetching data in larger chunks rather than on-demand byte-range requests.
     */
    public enum IndexComponent {
        SCHEMA,      // Schema and field metadata
        STORE,       // Document storage
        FASTFIELD,   // Fast field data for sorting/filtering
        POSTINGS,    // Term postings lists
        POSITIONS,   // Term positions for phrase queries
        FIELDNORM,   // Field norm data for scoring
        TERM         // Term dictionaries (FST) - preloads entire FST for all indexed fields
    }
    
    /**
     * Statistics about cache performance and loading behavior
     */
    public static class CacheStats {
        private final long hitCount;
        private final long missCount;
        private final long evictionCount;
        private final long totalSize;
        private final long maxSize;
        
        public CacheStats(long hitCount, long missCount, long evictionCount, long totalSize, long maxSize) {
            this.hitCount = hitCount;
            this.missCount = missCount;
            this.evictionCount = evictionCount;
            this.totalSize = totalSize;
            this.maxSize = maxSize;
        }
        
        public double getHitRate() { 
            long total = hitCount + missCount;
            return total > 0 ? (double) hitCount / total : 0.0;
        }
        
        public long getHitCount() { return hitCount; }
        public long getMissCount() { return missCount; }
        public long getEvictionCount() { return evictionCount; }
        public long getTotalSize() { return totalSize; }
        public long getMaxSize() { return maxSize; }
        public long getTotalRequests() { return hitCount + missCount; }
        public double getUtilization() { return (double) totalSize / maxSize; }
    }
    
    /**
     * Statistics about component loading performance
     */
    public static class LoadingStats {
        private final long totalBytesLoaded;
        private final long totalLoadTime;
        private final int activeConcurrentLoads;
        private final Map<IndexComponent, ComponentStats> componentStats;
        
        public LoadingStats(long totalBytesLoaded, long totalLoadTime, int activeConcurrentLoads, 
                           Map<IndexComponent, ComponentStats> componentStats) {
            this.totalBytesLoaded = totalBytesLoaded;
            this.totalLoadTime = totalLoadTime;
            this.activeConcurrentLoads = activeConcurrentLoads;
            this.componentStats = new HashMap<>(componentStats);
        }
        
        public long getTotalBytesLoaded() { return totalBytesLoaded; }
        public long getTotalLoadTime() { return totalLoadTime; }
        public int getActiveConcurrentLoads() { return activeConcurrentLoads; }
        public Map<IndexComponent, ComponentStats> getComponentStats() { return componentStats; }
        public double getAverageLoadSpeed() { 
            return totalLoadTime > 0 ? (double) totalBytesLoaded / totalLoadTime * 1000 : 0; // bytes per second
        }
    }
    
    /**
     * Per-component loading statistics
     */
    public static class ComponentStats {
        private final long bytesLoaded;
        private final long loadTime;
        private final int loadCount;
        private final boolean isPreloaded;
        
        public ComponentStats(long bytesLoaded, long loadTime, int loadCount, boolean isPreloaded) {
            this.bytesLoaded = bytesLoaded;
            this.loadTime = loadTime;
            this.loadCount = loadCount;
            this.isPreloaded = isPreloaded;
        }
        
        public long getBytesLoaded() { return bytesLoaded; }
        public long getLoadTime() { return loadTime; }
        public int getLoadCount() { return loadCount; }
        public boolean isPreloaded() { return isPreloaded; }
        public double getAverageLoadTime() { return loadCount > 0 ? (double) loadTime / loadCount : 0; }
    }
    
    /**
     * Constructor for SplitSearcher (now always uses shared cache)
     */
    SplitSearcher(String splitPath, SplitCacheManager cacheManager, Map<String, Object> splitConfig) {
        this.splitPath = splitPath;
        this.cacheManager = cacheManager;
        this.nativePtr = createNativeWithSharedCache(splitPath, cacheManager.getNativePtr(), splitConfig);
    }
    
    /**
     * @deprecated REMOVED - Use SplitCacheManager.createSplitSearcher() instead.
     * 
     * Proper usage pattern:
     * <pre>{@code
     * SplitCacheManager.CacheConfig config = new SplitCacheManager.CacheConfig("my-cache")
     *     .withMaxCacheSize(200_000_000);
     * SplitCacheManager cacheManager = SplitCacheManager.getInstance(config);
     * SplitSearcher searcher = cacheManager.createSplitSearcher(splitPath);
     * }</pre>
     * 
     * This ensures proper shared cache management across all splits.
     */
    @Deprecated
    private static SplitSearcher create(String splitPath) {
        throw new UnsupportedOperationException(
            "SplitSearcher.create() is deprecated. Use SplitCacheManager.createSplitSearcher() instead. " +
            "Create a SplitCacheManager with proper configuration and use it to create searchers."
        );
    }
    
    /**
     * Get the schema for this split
     */
    public Schema getSchema() {
        long schemaPtr = getSchemaFromNative(nativePtr);
        return new Schema(schemaPtr);
    }
    
    /**
     * Parse a query string using Quickwit's proven query parser.
     * This method leverages Quickwit's robust query parsing libraries for reliable
     * and efficient query parsing with the split's schema.
     * 
     * @param queryString The query string (e.g., "title:hello", "age:[1 TO 100]", "status:active AND priority:high")
     * @return A SplitQuery that can be used for efficient split searching
     */
    public SplitQuery parseQuery(String queryString) {
        if (queryString == null || queryString.trim().isEmpty()) {
            throw new IllegalArgumentException("Query string cannot be null or empty");
        }
        Schema schema = getSchema();
        return SplitQuery.parseQuery(queryString, schema);
    }

    /**
     * Parse a query string with specific default search fields.
     * This provides API compatibility with the main Tantivy Index.parseQuery() method.
     *
     * When no field is specified in the query (e.g., just "hello" instead of "title:hello"),
     * the query will search across all specified default fields.
     *
     * Examples:
     * - parseQuery("machine", Arrays.asList("title")) → searches for "machine" in title field
     * - parseQuery("machine", Arrays.asList("title", "category")) → searches for "machine" in both fields
     * - parseQuery("title:machine", Arrays.asList("category")) → ignores default fields, searches title explicitly
     *
     * @param queryString The query string to parse
     * @param defaultFieldNames List of field names to search when no field is specified in the query
     * @return A SplitQuery that can be used for efficient split searching
     * @throws IllegalArgumentException if queryString is null or empty, or if any field name is invalid
     */
    public SplitQuery parseQuery(String queryString, java.util.List<String> defaultFieldNames) {
        if (queryString == null || queryString.trim().isEmpty()) {
            throw new IllegalArgumentException("Query string cannot be null or empty");
        }
        if (defaultFieldNames == null) {
            defaultFieldNames = java.util.Collections.emptyList();
        }

        // Convert List<String> to String[] for native method
        String[] defaultFieldArray = defaultFieldNames.toArray(new String[0]);

        Schema schema = getSchema();
        return SplitQuery.parseQuery(queryString, schema, defaultFieldArray);
    }

    /**
     * Parse a query string with a single default search field.
     * This is a convenience method for the common case of searching within one specific field.
     *
     * Examples:
     * - parseQuery("machine", "title") → searches for "machine" in title field
     * - parseQuery("title:learning", "category") → ignores default field, searches title explicitly
     *
     * @param queryString The query string to parse
     * @param defaultFieldName Field name to search when no field is specified in the query
     * @return A SplitQuery that can be used for efficient split searching
     * @throws IllegalArgumentException if queryString is null/empty or fieldName is null/empty
     */
    public SplitQuery parseQuery(String queryString, String defaultFieldName) {
        if (defaultFieldName == null || defaultFieldName.trim().isEmpty()) {
            throw new IllegalArgumentException("Default field name cannot be null or empty");
        }
        return parseQuery(queryString, java.util.Arrays.asList(defaultFieldName));
    }
    
    /**
     * Search the split using a SplitQuery with efficient QueryAst conversion.
     * This method converts the SplitQuery to Quickwit's QueryAst format and uses
     * Quickwit's proven search algorithms for optimal performance.
     *
     * <p><b>Smart Wildcard Optimization:</b>
     * When a query contains expensive wildcard patterns (leading wildcards like "*foo"
     * or multi-wildcards like "*foo*bar*") combined with cheap filters (term queries,
     * range queries), this method will first evaluate the cheap filters to potentially
     * short-circuit the expensive wildcard evaluation.
     *
     * @param splitQuery The query to execute (use parseQuery() to create from string)
     * @param limit Maximum number of results to return
     * @return SearchResult containing matching documents and their scores
     */
    public SearchResult search(SplitQuery splitQuery, int limit) {
        if (nativePtr == 0) {
            throw new IllegalStateException("SplitSearcher has been closed or not properly initialized");
        }
        if (splitQuery == null) {
            throw new IllegalArgumentException("SplitQuery cannot be null");
        }

        try {
            // Smart Wildcard Optimization is handled transparently in the native Rust layer.
            // The native layer analyzes QueryAst for expensive wildcards combined with cheap filters,
            // and automatically short-circuits when the cheap filter returns zero results.
            // This applies to ALL query types: programmatic queries AND parseQuery() results.
            return searchWithSplitQuery(nativePtr, splitQuery, limit);
        } catch (Exception e) {
            throw new RuntimeException("Search failed: " + e.getMessage(), e);
        }
    }

    /**
     * Search with a single aggregation using real tantivy computation.
     *
     * @param splitQuery The query to execute
     * @param limit Maximum number of document hits to return
     * @param aggregationName Name for the aggregation in results
     * @param aggregation The aggregation to compute
     * @return SearchResult containing both hits and aggregation results
     */
    public SearchResult search(SplitQuery splitQuery, int limit, String aggregationName, SplitAggregation aggregation) {
        Map<String, SplitAggregation> aggregations = Map.of(aggregationName, aggregation);
        return search(splitQuery, limit, aggregations);
    }

    /**
     * Search with multiple aggregations using real tantivy computation.
     * This uses Quickwit's proven aggregation framework for accurate results.
     *
     * @param splitQuery The query to execute
     * @param limit Maximum number of document hits to return
     * @param aggregations Map of aggregation name to aggregation specification
     * @return SearchResult containing both hits and aggregation results
     */
    public SearchResult search(SplitQuery splitQuery, int limit, Map<String, SplitAggregation> aggregations) {
        if (nativePtr == 0) {
            throw new IllegalStateException("SplitSearcher has been closed or not properly initialized");
        }
        if (splitQuery == null) {
            throw new IllegalArgumentException("SplitQuery cannot be null");
        }
        if (aggregations == null) {
            aggregations = Map.of();
        }

        try {
            return searchWithAggregations(nativePtr, splitQuery, limit, aggregations);
        } catch (Exception e) {
            throw new RuntimeException("Search with aggregations failed: " + e.getMessage(), e);
        }
    }

    /**
     * Perform aggregation-only search (no document hits returned).
     * This is more efficient when you only need aggregation results.
     *
     * @param splitQuery The query to execute
     * @param aggregationName Name for the aggregation in results
     * @param aggregation The aggregation to compute
     * @return SearchResult with no hits but containing aggregation results
     */
    public SearchResult aggregate(SplitQuery splitQuery, String aggregationName, SplitAggregation aggregation) {
        Map<String, SplitAggregation> aggregations = Map.of(aggregationName, aggregation);
        return search(splitQuery, 0, aggregations); // 0 hits for aggregation-only
    }

    /**
     * Perform aggregation-only search with multiple aggregations.
     *
     * @param splitQuery The query to execute
     * @param aggregations Map of aggregation name to aggregation specification
     * @return SearchResult with no hits but containing aggregation results
     */
    public SearchResult aggregate(SplitQuery splitQuery, Map<String, SplitAggregation> aggregations) {
        return search(splitQuery, 0, aggregations); // 0 hits for aggregation-only
    }
    
    /**
     * Retrieve a document by its address from the split
     * @param docAddress The document address obtained from search results
     * @return The document with all indexed fields
     */
    public Document doc(DocAddress docAddress) {
        return docNative(nativePtr, docAddress.getSegmentOrd(), docAddress.getDoc());
    }

    /**
     * Retrieve a document with projected fields from a parquet companion split.
     * When the split has a parquet companion manifest, this retrieves document data
     * directly from the external parquet files with column projection for efficiency.
     *
     * <p>If no fields are specified (null or empty), all fields are retrieved.
     * If the split has no parquet companion, this falls back to standard retrieval.
     *
     * @param docAddress The document address obtained from search results
     * @param fields Optional list of field names to project (null = all fields)
     * @return JSON string of field values, e.g. {"name":"Alice","age":30}
     */
    public Document docProjected(DocAddress docAddress, String... fields) {
        if (hasParquetCompanion()) {
            // Route companion splits through unified TANT binary path
            Set<String> fieldSet = (fields != null && fields.length > 0)
                    ? new HashSet<>(Arrays.asList(fields))
                    : null;
            List<Document> docs = docBatchViaParquet(
                    Collections.singletonList(docAddress), fieldSet);
            return docs.isEmpty() ? null : docs.get(0);
        }
        // Non-companion: fall back to standard doc retrieval
        return doc(docAddress);
    }

    /**
     * Batch projected document retrieval returning raw TANT binary bytes.
     * For companion splits, data is serialized directly from Arrow to TANT format.
     * For standard splits, falls back to standard bulk retrieval (also TANT format).
     *
     * @param docAddresses Array of document addresses
     * @param fields Optional list of field names to project (null = all fields)
     * @return List of documents in the same order as the input addresses
     */
    public List<Document> docBatchProjected(DocAddress[] docAddresses, String... fields) {
        int[] segments = new int[docAddresses.length];
        int[] docIds = new int[docAddresses.length];
        for (int i = 0; i < docAddresses.length; i++) {
            segments[i] = docAddresses[i].getSegmentOrd();
            docIds[i] = docAddresses[i].getDoc();
        }
        byte[] result = nativeDocBatchProjected(nativePtr, segments, docIds,
                fields != null && fields.length > 0 ? fields : null);
        if (result == null) {
            return new ArrayList<>();
        }
        java.nio.ByteBuffer buffer = java.nio.ByteBuffer.wrap(result);
        BatchDocumentReader reader = new BatchDocumentReader();
        List<Map<String, Object>> docMaps = reader.parseToMaps(buffer);
        List<Document> docs = new ArrayList<>(docMaps.size());
        for (Map<String, Object> docMap : docMaps) {
            docs.add(new MapBackedDocument(docMap));
        }
        return docs;
    }

    /**
     * Check if this split has a parquet companion manifest.
     * When true, document retrieval can be served from external parquet files.
     *
     * @return true if the split contains a parquet companion manifest
     */
    public boolean hasParquetCompanion() {
        return nativeHasParquetManifest(nativePtr);
    }

    /**
     * Retrieve multiple documents by their addresses in a single batch operation.
     * This is significantly more efficient than calling doc() multiple times,
     * especially for large numbers of documents.
     *
     * <p>This method automatically selects the optimal retrieval method based on
     * the batch size and the configured byteBufferThreshold:
     * <ul>
     *   <li>Below threshold: Uses traditional Document[] method (lower setup overhead)</li>
     *   <li>At/above threshold: Uses byte buffer protocol (lower JNI overhead for large batches)</li>
     * </ul>
     *
     * <p>For the byte buffer path, this returns {@link MapBackedDocument} instances
     * (pure Java, zero JNI calls for field access). For the object path, this returns
     * native-backed {@link Document} instances. Both extend Document, so this is
     * fully backwards compatible.
     *
     * @param docAddresses List of document addresses
     * @return List of documents in the same order as the input addresses
     */
    public List<Document> docBatch(List<DocAddress> docAddresses) {
        return docBatch(docAddresses, null);
    }

    /**
     * Retrieve multiple documents by their addresses, optionally filtering to specific fields.
     *
     * <p>This method automatically selects the optimal retrieval strategy based on batch size:
     * <ul>
     *   <li>Below threshold: Uses traditional Document[] method (lower setup overhead)</li>
     *   <li>At/above threshold: Uses byte buffer protocol (lower JNI overhead for large batches)</li>
     * </ul>
     *
     * <p>When field names are specified, only those fields are included in the returned documents.
     * This can improve performance when you only need a subset of fields.
     *
     * <p>For the byte buffer path, this returns {@link MapBackedDocument} instances
     * (pure Java, zero JNI calls for field access). For the object path, this returns
     * native-backed {@link Document} instances. Both extend Document, so this is
     * fully backwards compatible.
     *
     * @param docAddresses List of document addresses
     * @param fieldNames Set of field names to include, or null to include all fields
     * @return List of documents in the same order as the input addresses
     */
    public List<Document> docBatch(List<DocAddress> docAddresses, Set<String> fieldNames) {
        if (docAddresses == null || docAddresses.isEmpty()) {
            return new ArrayList<>();
        }

        // Companion splits: always use TANT binary path via parquet
        if (hasParquetCompanion()) {
            return docBatchViaParquet(docAddresses, fieldNames);
        }

        // Get threshold from cache manager configuration
        int threshold = cacheManager.getBatchConfig().getByteBufferThreshold();

        if (docAddresses.size() >= threshold) {
            // Use byte buffer protocol for large batches (reduces JNI overhead)
            return docBatchViaByteBuffer(docAddresses, fieldNames);
        } else {
            // Use traditional Document[] method for small batches
            return docBatchViaObjects(docAddresses, fieldNames);
        }
    }

    /**
     * Retrieve documents using the traditional Document[] method.
     * Used for small batches where the byte buffer serialization overhead isn't worth it.
     * Returns native-backed Document instances.
     */
    private List<Document> docBatchViaObjects(List<DocAddress> docAddresses, Set<String> fieldNames) {
        int[] segments = new int[docAddresses.size()];
        int[] docIds = new int[docAddresses.size()];

        for (int i = 0; i < docAddresses.size(); i++) {
            DocAddress addr = docAddresses.get(i);
            segments[i] = addr.getSegmentOrd();
            docIds[i] = addr.getDoc();
        }

        Document[] docs = docBatchNative(nativePtr, segments, docIds);

        // If field filtering is requested, filter each document
        if (fieldNames != null && !fieldNames.isEmpty()) {
            List<Document> filtered = new ArrayList<>(docs.length);
            for (Document doc : docs) {
                if (doc != null) {
                    filtered.add(filterDocumentFields(doc, fieldNames));
                }
            }
            return filtered;
        }

        return Arrays.asList(docs);
    }

    /**
     * Filter a document to only include specified fields.
     * Returns a new MapBackedDocument containing only the requested fields.
     * This avoids JNI overhead by using pure Java document representation.
     */
    private Document filterDocumentFields(Document doc, Set<String> fieldNames) {
        Map<String, List<Object>> allFields = doc.toMap();
        Map<String, Object> filtered = new HashMap<>();
        for (String fieldName : fieldNames) {
            if (allFields.containsKey(fieldName)) {
                List<Object> values = allFields.get(fieldName);
                if (values.size() == 1) {
                    filtered.put(fieldName, values.get(0));
                } else {
                    filtered.put(fieldName, values);
                }
            }
        }
        // Use MapBackedDocument (extends Document) to avoid JNI overhead
        return new MapBackedDocument(filtered);
    }

    /**
     * Build a Document from a map using individual add methods.
     * This avoids using Document.fromMap() which requires unimplemented native methods.
     */
    private Document buildDocumentFromMap(Map<String, Object> fields) {
        Document doc = new Document();
        for (Map.Entry<String, Object> entry : fields.entrySet()) {
            String fieldName = entry.getKey();
            Object value = entry.getValue();
            addValueToDocument(doc, fieldName, value);
        }
        return doc;
    }

    /**
     * Add a value to a document, handling different types and multi-values.
     */
    @SuppressWarnings("unchecked")
    private void addValueToDocument(Document doc, String fieldName, Object value) {
        if (value instanceof List) {
            // Multi-value field
            for (Object v : (List<Object>) value) {
                addSingleValueToDocument(doc, fieldName, v);
            }
        } else {
            addSingleValueToDocument(doc, fieldName, value);
        }
    }

    /**
     * Add a single value to a document based on its type.
     */
    private void addSingleValueToDocument(Document doc, String fieldName, Object value) {
        if (value == null) {
            return;
        }
        if (value instanceof String) {
            doc.addText(fieldName, (String) value);
        } else if (value instanceof Long) {
            // Could be Integer, Unsigned, or Date (epoch nanos)
            // Default to Integer since we can't distinguish
            doc.addInteger(fieldName, (Long) value);
        } else if (value instanceof Integer) {
            doc.addInteger(fieldName, ((Integer) value).longValue());
        } else if (value instanceof Double) {
            doc.addFloat(fieldName, (Double) value);
        } else if (value instanceof Float) {
            doc.addFloat(fieldName, ((Float) value).doubleValue());
        } else if (value instanceof Boolean) {
            doc.addBoolean(fieldName, (Boolean) value);
        } else if (value instanceof byte[]) {
            doc.addBytes(fieldName, (byte[]) value);
        } else if (value instanceof java.time.LocalDateTime) {
            doc.addDate(fieldName, (java.time.LocalDateTime) value);
        } else {
            // Unknown type - convert to string
            doc.addText(fieldName, value.toString());
        }
    }

    /**
     * Retrieve documents using the byte buffer protocol.
     * Used for large batches where the reduced JNI overhead outweighs serialization costs.
     *
     * <p>This method retrieves all documents in a single byte buffer from the native layer,
     * then parses them on the Java side using MapBackedDocument to avoid any JNI calls.
     *
     * <p>When field names are specified, filtering is performed on the native side before
     * serialization, reducing both bandwidth and parsing overhead.
     *
     * <p><b>Performance:</b> This method eliminates the 2×N×M JNI calls that would otherwise
     * occur when using Document objects (N documents × M fields for storing, plus N×M for reading).
     * Instead, all document data stays in pure Java after the single ByteBuffer transfer.
     */
    /**
     * Retrieve documents from parquet companion splits using TANT binary format.
     * Calls nativeDocBatchProjected which serializes Arrow data directly to TANT binary,
     * bypassing the expensive JSON round-trip.
     */
    private List<Document> docBatchViaParquet(List<DocAddress> docAddresses, Set<String> fieldNames) {
        int[] segments = new int[docAddresses.size()];
        int[] docIds = new int[docAddresses.size()];

        for (int i = 0; i < docAddresses.size(); i++) {
            DocAddress addr = docAddresses.get(i);
            segments[i] = addr.getSegmentOrd();
            docIds[i] = addr.getDoc();
        }

        String[] fieldsArray = (fieldNames != null && !fieldNames.isEmpty())
                ? fieldNames.toArray(new String[0])
                : null;

        byte[] result = nativeDocBatchProjected(nativePtr, segments, docIds, fieldsArray);
        if (result == null) {
            return new ArrayList<>();
        }

        java.nio.ByteBuffer buffer = java.nio.ByteBuffer.wrap(result);
        BatchDocumentReader reader = new BatchDocumentReader();
        List<Map<String, Object>> docMaps = reader.parseToMaps(buffer);

        List<Document> docs = new ArrayList<>(docMaps.size());
        for (Map<String, Object> docMap : docMaps) {
            docs.add(new MapBackedDocument(docMap));
        }
        return docs;
    }

    private List<Document> docBatchViaByteBuffer(List<DocAddress> docAddresses, Set<String> fieldNames) {
        // Convert to arrays for JNI transfer
        int[] segments = new int[docAddresses.size()];
        int[] docIds = new int[docAddresses.size()];

        for (int i = 0; i < docAddresses.size(); i++) {
            DocAddress addr = docAddresses.get(i);
            segments[i] = addr.getSegmentOrd();
            docIds[i] = addr.getDoc();
        }

        // Use native-side filtering when field names are specified
        byte[] result;
        if (fieldNames != null && !fieldNames.isEmpty()) {
            String[] fieldNamesArray = fieldNames.toArray(new String[0]);
            result = docsBulkNativeWithFields(nativePtr, segments, docIds, fieldNamesArray);
        } else {
            result = docsBulkNative(nativePtr, segments, docIds);
        }

        if (result == null) {
            return new ArrayList<>();
        }

        java.nio.ByteBuffer buffer = java.nio.ByteBuffer.wrap(result);

        // Parse the byte buffer into maps
        BatchDocumentReader reader = new BatchDocumentReader();
        List<Map<String, Object>> docMaps = reader.parseToMaps(buffer);

        // Wrap each map in MapBackedDocument - ZERO JNI calls!
        // This is the key optimization: MapBackedDocument extends Document but is pure Java,
        // so subsequent getFirst() calls don't cross the JNI boundary.
        List<Document> docs = new ArrayList<>(docMaps.size());
        for (Map<String, Object> docMap : docMaps) {
            docs.add(new MapBackedDocument(docMap));
        }
        return docs;
    }
    
    /**
     * Retrieve multiple documents efficiently using zero-copy semantics.
     * This method serializes all requested documents into a single ByteBuffer
     * for optimal performance when retrieving many documents.
     * 
     * The returned ByteBuffer uses the same binary protocol as batch document
     * indexing but in reverse - for efficient bulk document retrieval.
     * 
     * @param docAddresses List of document addresses to retrieve
     * @return ByteBuffer containing serialized documents, or null if addresses is empty
     * @throws RuntimeException if retrieval fails
     */
    public java.nio.ByteBuffer docsBulk(List<DocAddress> docAddresses) {
        if (docAddresses == null || docAddresses.isEmpty()) {
            return null;
        }
        
        // Convert to arrays for JNI transfer
        int[] segments = new int[docAddresses.size()];
        int[] docIds = new int[docAddresses.size()];
        
        for (int i = 0; i < docAddresses.size(); i++) {
            DocAddress addr = docAddresses.get(i);
            segments[i] = addr.getSegmentOrd();
            docIds[i] = addr.getDoc();
        }
        
        byte[] result = docsBulkNative(nativePtr, segments, docIds);
        return result != null ? java.nio.ByteBuffer.wrap(result) : null;
    }
    
    /**
     * Parse documents from a bulk retrieval ByteBuffer.
     * This method provides a convenient way to extract individual Document objects
     * from the zero-copy ByteBuffer returned by docsBulk().
     * 
     * @param buffer ByteBuffer from docsBulk() call
     * @return List of Document objects in the same order as the original request
     * @throws RuntimeException if parsing fails
     */
    public List<Document> parseBulkDocs(java.nio.ByteBuffer buffer) {
        if (buffer == null) {
            return new ArrayList<>();
        }
        
        return parseBulkDocsNative(buffer);
    }
    
    /**
     * Asynchronously preload specified components into cache for ALL fields.
     *
     * This preloads the specified component types for every applicable field in the schema.
     * For field-specific preloading (to reduce cache usage and prewarm time), use
     * {@link #preloadFields(IndexComponent, String...)} instead.
     *
     * @param components The component types to preload (TERM, POSTINGS, FIELDNORM, FASTFIELD, STORE)
     * @return CompletableFuture that completes when preloading is finished
     */
    public CompletableFuture<Void> preloadComponents(IndexComponent... components) {
        return CompletableFuture.runAsync(() -> {
            preloadComponentsNative(nativePtr, components);
        });
    }

    /**
     * Asynchronously preload a specific component for only the specified fields.
     *
     * This allows fine-grained control over which fields are preloaded, reducing cache usage
     * and prewarm time compared to {@link #preloadComponents(IndexComponent...)} which
     * preloads all fields.
     *
     * <p>Example usage:</p>
     * <pre>{@code
     * // Prewarm term dictionaries only for 'title' and 'content' fields
     * searcher.preloadFields(IndexComponent.TERM, "title", "content").join();
     *
     * // Prewarm fast fields only for 'timestamp' and 'score' fields
     * searcher.preloadFields(IndexComponent.FASTFIELD, "timestamp", "score").join();
     *
     * // Prewarm multiple components for specific fields
     * CompletableFuture.allOf(
     *     searcher.preloadFields(IndexComponent.TERM, "title", "content"),
     *     searcher.preloadFields(IndexComponent.FASTFIELD, "timestamp"),
     *     searcher.preloadFields(IndexComponent.POSTINGS, "content")
     * ).join();
     * }</pre>
     *
     * <p>Supported components for field-specific preloading:</p>
     * <ul>
     *   <li><b>TERM</b> - Term dictionaries (FST) for specified text/JSON fields</li>
     *   <li><b>POSTINGS</b> - Posting lists for specified text/JSON fields</li>
     *   <li><b>FIELDNORM</b> - Field norms for specified text fields</li>
     *   <li><b>FASTFIELD</b> - Fast field data for specified fast fields</li>
     *   <li><b>STORE</b> - Not field-specific (preloads all document storage)</li>
     * </ul>
     *
     * @param component The component type to preload
     * @param fieldNames The specific field names to preload (must exist in schema)
     * @return CompletableFuture that completes when preloading is finished
     * @throws IllegalArgumentException if fieldNames is empty or contains invalid field names
     */
    public CompletableFuture<Void> preloadFields(IndexComponent component, String... fieldNames) {
        if (fieldNames == null || fieldNames.length == 0) {
            throw new IllegalArgumentException("At least one field name must be specified");
        }
        return CompletableFuture.runAsync(() -> {
            preloadFieldsNative(nativePtr, component, fieldNames);
        });
    }

    /**
     * Prewarm parquet fast fields by transcoding specified columns from parquet data.
     *
     * <p>This method is only applicable when the split has a parquet companion manifest
     * with {@code fast_field_mode != DISABLED}. It transcodes parquet columns into
     * tantivy's columnar format and caches them for fast field access during
     * aggregations and sorting.
     *
     * <p>After this call, aggregation queries that use fast fields will read from
     * the transcoded parquet data instead of (or merged with) the native fast fields.
     *
     * @param columns Optional column names to transcode. Pass null or empty to transcode
     *                all applicable columns based on the fast field mode.
     * @return CompletableFuture that completes when transcoding is done
     */
    public CompletableFuture<Void> preloadParquetFastFields(String... columns) {
        return CompletableFuture.runAsync(() -> {
            boolean success = nativePrewarmParquetFastFields(nativePtr, columns);
            if (!success) {
                throw new RuntimeException(
                    "preloadParquetFastFields failed — check stderr for details. "
                    + "Common cause: parquet_table_root not set. "
                    + "Configure via CacheConfig.withParquetTableRoot() or pass table root to createSplitSearcher().");
            }
        });
    }

    /**
     * Pre-fetch parquet column pages into the storage cache (L2) for faster document retrieval.
     *
     * <p>Unlike {@link #preloadParquetFastFields} which transcodes columns for fast field access,
     * this method warms the disk cache with raw parquet column pages. This improves subsequent
     * document retrieval performance by eliminating cold cache misses.</p>
     *
     * @param columns Column names to prewarm, or empty/null for all columns
     * @return CompletableFuture that completes when prewarm is finished
     */
    public CompletableFuture<Void> preloadParquetColumns(String... columns) {
        return CompletableFuture.runAsync(() -> {
            boolean success = nativePrewarmParquetColumns(nativePtr, columns);
            if (!success) {
                throw new RuntimeException(
                    "preloadParquetColumns failed — check stderr for details. "
                    + "Common cause: parquet_table_root not set. "
                    + "Configure via CacheConfig.withParquetTableRoot() or pass table root to createSplitSearcher().");
            }
        });
    }

    /**
     * Get parquet retrieval statistics for this searcher.
     *
     * <p>Returns a JSON string containing metadata about the parquet companion configuration:
     * total files, rows, row groups, columns, fast field mode, file sizes, etc.
     * Returns null if this split does not have a parquet companion manifest.</p>
     *
     * @return JSON string with parquet stats, or null if no parquet manifest
     */
    public String getParquetRetrievalStats() {
        return nativeGetParquetRetrievalStats(nativePtr);
    }

    /**
     * Get the column mapping from the parquet companion manifest.
     *
     * <p>Returns a JSON string describing how tantivy fields map to parquet columns,
     * including both the tantivy type (index type) and parquet type (data type).
     * This is essential for consumers like Spark that need to know the actual data type
     * of fields, particularly for {@code exact_only} fields where the tantivy schema
     * reports U64 (the hash type) but the parquet data is a string.</p>
     *
     * <p>JSON format: array of objects, each with:
     * {@code tantivy_field_name}, {@code parquet_column_name}, {@code tantivy_type},
     * {@code parquet_type}, {@code physical_ordinal}.</p>
     *
     * @return JSON string with column mapping, or null if no parquet manifest
     */
    public String getColumnMapping() {
        return nativeGetColumnMapping(nativePtr);
    }

    /**
     * Get the string indexing modes from the parquet companion manifest.
     *
     * <p>Returns a JSON string mapping field names to their compact string indexing mode.
     * Fields not in this map use standard string indexing. This allows consumers to
     * identify fields where the tantivy schema type (U64) differs from the actual
     * data type (string) due to hash-based indexing.</p>
     *
     * <p>Possible mode values: {@code "exact_only"}, {@code "text_uuid_exactonly"},
     * {@code "text_uuid_strip"}, {@code "text_custom_exactonly:<regex>"},
     * {@code "text_custom_strip:<regex>"}.</p>
     *
     * @return JSON string mapping field name to indexing mode, or null if no parquet manifest
     */
    public String getStringIndexingModes() {
        return nativeGetStringIndexingModes(nativePtr);
    }

    /**
     * Warmup system: Proactively fetch query-relevant components before search execution.
     * This implements Quickwit's sophisticated warmup optimization that analyzes queries
     * and preloads only the data needed for optimal performance.
     * 
     * Benefits:
     * - Parallel async loading of query-relevant components
     * - Reduced search latency through proactive data fetching  
     * - Network request consolidation and caching optimization
     * - Memory-efficient loading of only required data
     * 
     * Usage:
     * <pre>{@code
     * Query query = Query.termQuery(schema, "title", "search term");
     * searcher.warmupQuery(query).thenCompose(v -> 
     *     searcher.searchAsync(query, 10)
     * ).thenAccept(results -> {
     *     // Search executes much faster due to pre-warmed data
     *     System.out.println("Results: " + results.getHits().size());
     * });
     * }</pre>
     * 
     * @param query The query to analyze and warm up components for
     * @return CompletableFuture that completes when warmup is finished
     * @throws RuntimeException if warmup fails
     */
    public CompletableFuture<Void> warmupQuery(Query query) {
        return CompletableFuture.runAsync(() -> {
            warmupQueryNative(nativePtr, query.getNativePtr());
        });
    }
    
    /**
     * Advanced warmup with explicit component control.
     * Allows fine-grained control over which components to warm up for a query.
     * 
     * @param query The query to warm up for
     * @param components Specific components to prioritize (null for auto-detection)
     * @param enableParallel Whether to use parallel loading (recommended: true)
     * @return CompletableFuture that completes when warmup is finished
     */
    public CompletableFuture<WarmupStats> warmupQueryAdvanced(Query query, IndexComponent[] components, boolean enableParallel) {
        return CompletableFuture.supplyAsync(() -> {
            return warmupQueryAdvancedNative(nativePtr, query.getNativePtr(), components, enableParallel);
        });
    }
    
    /**
     * Statistics about warmup performance and effectiveness
     */
    public static class WarmupStats {
        private final long totalBytesLoaded;
        private final long warmupTimeMs;
        private final int componentsLoaded;
        private final Map<IndexComponent, Long> componentSizes;
        private final boolean usedParallelLoading;
        
        public WarmupStats(long totalBytesLoaded, long warmupTimeMs, int componentsLoaded, 
                          Map<IndexComponent, Long> componentSizes, boolean usedParallelLoading) {
            this.totalBytesLoaded = totalBytesLoaded;
            this.warmupTimeMs = warmupTimeMs;
            this.componentsLoaded = componentsLoaded;
            this.componentSizes = new HashMap<>(componentSizes);
            this.usedParallelLoading = usedParallelLoading;
        }
        
        public long getTotalBytesLoaded() { return totalBytesLoaded; }
        public long getWarmupTimeMs() { return warmupTimeMs; }
        public int getComponentsLoaded() { return componentsLoaded; }
        public Map<IndexComponent, Long> getComponentSizes() { return componentSizes; }
        public boolean isUsedParallelLoading() { return usedParallelLoading; }
        public double getLoadingSpeedMBps() { 
            return warmupTimeMs > 0 ? (totalBytesLoaded / 1024.0 / 1024.0) / (warmupTimeMs / 1000.0) : 0.0;
        }
    }
    
    /**
     * Get current cache statistics
     */
    public CacheStats getCacheStats() {
        return getCacheStatsNative(nativePtr);
    }
    
    /**
     * Get current loading statistics
     */
    public LoadingStats getLoadingStats() {
        return getLoadingStatsNative(nativePtr);
    }
    
    /**
     * Check if specific components are currently cached
     */
    public Map<IndexComponent, Boolean> getComponentCacheStatus() {
        return getComponentCacheStatusNative(nativePtr);
    }
    
    /**
     * Force eviction of specified components from cache
     */
    public void evictComponents(IndexComponent... components) {
        evictComponentsNative(nativePtr, components);
    }
    
    /**
     * Get list of files contained in this split
     */
    public List<String> listSplitFiles() {
        return listSplitFilesNative(nativePtr);
    }
    
    /**
     * Validate the integrity of the split file
     */
    public boolean validateSplit() {
        return validateSplitNative(nativePtr);
    }
    
    /**
     * Get split metadata including size, component information, etc.
     */
    public SplitMetadata getSplitMetadata() {
        return getSplitMetadataNative(nativePtr);
    }

    /**
     * Get per-field component sizes for all fields in the index.
     *
     * <p>Returns a map with keys like "field_name.component" and values as sizes in bytes.
     * Uses a hybrid approach combining bundle file offsets with async reader APIs.</p>
     *
     * <p>Per-field components (precise per-field sizes):</p>
     * <ul>
     *   <li>"fastfield" - fast field column size (for sorting/filtering)</li>
     *   <li>"fieldnorm" - field norm size (for scoring)</li>
     * </ul>
     *
     * <p>Segment-level totals (aggregated across all fields):</p>
     * <ul>
     *   <li>"_term_total" - total term dictionary (FST) size</li>
     *   <li>"_postings_total" - total posting lists size</li>
     *   <li>"_positions_total" - total term positions size</li>
     *   <li>"_store" - total document store size</li>
     * </ul>
     *
     * <p>Example output:</p>
     * <pre>{@code
     * {
     *   "score.fastfield": 256,
     *   "id.fastfield": 128,
     *   "content.fieldnorm": 512,
     *   "title.fieldnorm": 256,
     *   "_term_total": 2048,
     *   "_postings_total": 4096,
     *   "_positions_total": 1024,
     *   "_store": 8192
     * }
     * }</pre>
     *
     * @return Map of "field_name.component" or "_component_total" to size in bytes
     */
    public Map<String, Long> getPerFieldComponentSizes() {
        Map<String, Long> result = getPerFieldComponentSizesNative(nativePtr);
        return result != null ? result : new HashMap<>();
    }

    public static class SplitMetadata {
        private final String splitId;
        private final long totalSize;
        private final long hotCacheSize;
        private final int numComponents;
        private final Map<String, Long> componentSizes;
        
        public SplitMetadata(String splitId, long totalSize, long hotCacheSize, 
                           int numComponents, Map<String, Long> componentSizes) {
            this.splitId = splitId;
            this.totalSize = totalSize;
            this.hotCacheSize = hotCacheSize;
            this.numComponents = numComponents;
            this.componentSizes = new HashMap<>(componentSizes);
        }
        
        public String getSplitId() { return splitId; }
        public long getTotalSize() { return totalSize; }
        public long getHotCacheSize() { return hotCacheSize; }
        public int getNumComponents() { return numComponents; }
        public Map<String, Long> getComponentSizes() { return componentSizes; }
        public double getHotCacheRatio() { return (double) hotCacheSize / totalSize; }
    }

    /**
     * Tokenize a string using the proper tokenizer for the specified field.
     * This method uses the field's schema configuration to determine the correct
     * tokenizer and returns the list of tokens that would be generated for indexing.
     *
     * @param fieldName The name of the field (must exist in the schema)
     * @param text The text to tokenize
     * @return List of tokens generated by the field's tokenizer
     * @throws IllegalArgumentException if the field doesn't exist or text is null
     */
    public List<String> tokenize(String fieldName, String text) {
        if (nativePtr == 0) {
            throw new IllegalStateException("SplitSearcher has been closed or not properly initialized");
        }
        if (fieldName == null || fieldName.trim().isEmpty()) {
            throw new IllegalArgumentException("Field name cannot be null or empty");
        }
        if (text == null) {
            throw new IllegalArgumentException("Text to tokenize cannot be null");
        }

        try {
            return tokenizeNative(nativePtr, fieldName, text);
        } catch (Exception e) {
            throw new RuntimeException("Tokenization failed for field '" + fieldName + "': " + e.getMessage(), e);
        }
    }

    @Override
    public void close() {
        if (nativePtr != 0) {
            closeNative(nativePtr);
            nativePtr = 0;
        }
        
        // Always notify cache manager (all instances now use shared cache)
        cacheManager.removeSplitSearcher(splitPath);
    }
    
    // Native methods
    private static native long createNativeWithSharedCache(String splitPath, long cacheManagerPtr, Map<String, Object> splitConfig);
    private static native long getSchemaFromNative(long nativePtr);
    private static native SearchResult searchWithQueryAst(long nativePtr, String queryAstJson, int limit);
    private static native SearchResult searchWithSplitQuery(long nativePtr, SplitQuery splitQuery, int limit);
    private static native SearchResult searchWithAggregations(long nativePtr, SplitQuery splitQuery, int limit, Map<String, SplitAggregation> aggregations);
    private static native Document docNative(long nativePtr, int segment, int docId);
    private static native Document[] docBatchNative(long nativePtr, int[] segments, int[] docIds);
    private static native byte[] docsBulkNative(long nativePtr, int[] segments, int[] docIds);
    private static native byte[] docsBulkNativeWithFields(long nativePtr, int[] segments, int[] docIds, String[] fieldNames);
    private static native List<Document> parseBulkDocsNative(java.nio.ByteBuffer buffer);
    private static native void preloadComponentsNative(long nativePtr, IndexComponent[] components);
    private static native void preloadFieldsNative(long nativePtr, IndexComponent component, String[] fieldNames);
    private static native boolean nativePrewarmParquetFastFields(long nativePtr, String[] columns);
    private static native boolean nativePrewarmParquetColumns(long nativePtr, String[] columns);
    private static native String nativeGetParquetRetrievalStats(long nativePtr);
    private static native String nativeGetColumnMapping(long nativePtr);
    private static native String nativeGetStringIndexingModes(long nativePtr);
    private static native void warmupQueryNative(long nativePtr, long queryPtr);
    private static native WarmupStats warmupQueryAdvancedNative(long nativePtr, long queryPtr, IndexComponent[] components, boolean enableParallel);
    private static native CacheStats getCacheStatsNative(long nativePtr);
    private static native LoadingStats getLoadingStatsNative(long nativePtr);
    private static native Map<IndexComponent, Boolean> getComponentCacheStatusNative(long nativePtr);
    private static native void evictComponentsNative(long nativePtr, IndexComponent[] components);
    private static native List<String> listSplitFilesNative(long nativePtr);
    private static native boolean validateSplitNative(long nativePtr);
    private static native SplitMetadata getSplitMetadataNative(long nativePtr);
    private static native Map<String, Long> getPerFieldComponentSizesNative(long nativePtr);
    private static native List<String> tokenizeNative(long nativePtr, String fieldName, String text);
    private static native void closeNative(long nativePtr);
    // Parquet companion mode native methods
    private static native boolean nativeHasParquetManifest(long nativePtr);
    private static native byte[] nativeDocBatchProjected(long nativePtr, int[] segments, int[] docIds, String[] fields);

    // Smart Wildcard Optimization Statistics (for testing/monitoring)
    private static native void resetSmartWildcardStats();
    private static native long getQueriesAnalyzed();
    private static native long getQueriesOptimizable();
    private static native long getShortCircuitsTriggered();

    /**
     * Statistics for Smart Wildcard AST Skipping optimization.
     * Used for testing and monitoring the optimization's effectiveness.
     */
    public static class SmartWildcardStats {
        public final long queriesAnalyzed;
        public final long queriesOptimizable;
        public final long shortCircuitsTriggered;

        public SmartWildcardStats(long queriesAnalyzed, long queriesOptimizable, long shortCircuitsTriggered) {
            this.queriesAnalyzed = queriesAnalyzed;
            this.queriesOptimizable = queriesOptimizable;
            this.shortCircuitsTriggered = shortCircuitsTriggered;
        }

        @Override
        public String toString() {
            return String.format("SmartWildcardStats{analyzed=%d, optimizable=%d, shortCircuits=%d}",
                queriesAnalyzed, queriesOptimizable, shortCircuitsTriggered);
        }
    }

    /**
     * Reset smart wildcard optimization statistics counters.
     * Useful for isolating statistics in tests.
     */
    public static void resetWildcardOptimizationStats() {
        resetSmartWildcardStats();
    }

    /**
     * Get current smart wildcard optimization statistics.
     * @return Statistics object with counters for analyzed, optimizable, and short-circuited queries
     */
    public static SmartWildcardStats getWildcardOptimizationStats() {
        return new SmartWildcardStats(
            getQueriesAnalyzed(),
            getQueriesOptimizable(),
            getShortCircuitsTriggered()
        );
    }
}
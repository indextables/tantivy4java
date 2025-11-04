package io.indextables.tantivy4java.core;

/**
 * Configuration options for JSON object fields.
 *
 * <p>JSON fields allow storing and indexing arbitrary nested JSON objects.
 * All nested values (strings, numbers, booleans, dates, arrays) are
 * automatically indexed according to their type and can be queried using
 * JSONPath syntax.
 *
 * <p>Example usage:
 * <pre>
 * JsonObjectOptions options = JsonObjectOptions.create()
 *     .setStored(true)
 *     .setIndexing(TextFieldIndexing.withPositions())
 *     .setFast(true)
 *     .setExpandDots(true);
 *
 * Field jsonField = schemaBuilder.addJsonField("attributes", options);
 * </pre>
 *
 * <h3>Configuration Options:</h3>
 * <ul>
 *   <li><b>stored</b> - Store the entire JSON object for retrieval</li>
 *   <li><b>indexing</b> - Enable indexing of nested values with text analyzer</li>
 *   <li><b>fast</b> - Enable columnar storage (fast fields) for efficient filtering</li>
 *   <li><b>fastTokenizer</b> - Tokenizer for fast field text values</li>
 *   <li><b>expandDots</b> - Treat dots in keys as path separators</li>
 * </ul>
 *
 * <h3>Expand Dots Behavior:</h3>
 * <ul>
 *   <li><b>expandDots=false</b>: {"k8s.node.id": 5} → query as k8s\.node\.id:5 (dots escaped)</li>
 *   <li><b>expandDots=true</b>: {"k8s.node.id": 5} → treated as nested object → query as k8s.node.id:5</li>
 * </ul>
 */
public class JsonObjectOptions {
    private boolean stored;
    private TextFieldIndexing indexing;  // null if not indexed
    private boolean fast;
    private String fastTokenizer;  // null or tokenizer name
    private boolean expandDots;

    /**
     * Private constructor. Use static factory methods to create instances.
     */
    private JsonObjectOptions() {
        this.stored = false;
        this.indexing = null;
        this.fast = false;
        this.fastTokenizer = null;
        this.expandDots = false;
    }

    /**
     * Create default JSON field options (not stored, not indexed, not fast).
     *
     * @return New JsonObjectOptions with default settings
     */
    public static JsonObjectOptions create() {
        return new JsonObjectOptions();
    }

    /**
     * Store the entire JSON object for retrieval.
     *
     * <p>When enabled, the original JSON can be retrieved from search results
     * using {@link Document#getFirst(Field)} or similar methods.
     *
     * @param stored true to store the JSON object
     * @return this JsonObjectOptions for method chaining
     */
    public JsonObjectOptions setStored(boolean stored) {
        this.stored = stored;
        return this;
    }

    /**
     * Enable indexing of nested values with text field configuration.
     *
     * <p>When set, all nested values in the JSON object are indexed:
     * <ul>
     *   <li>String values - tokenized with the specified text analyzer</li>
     *   <li>Numeric values - indexed as fast fields</li>
     *   <li>Boolean values - indexed as fast fields</li>
     *   <li>Date values - indexed as fast fields</li>
     *   <li>Arrays - each element indexed at the same path</li>
     * </ul>
     *
     * @param indexing Text field indexing settings (tokenizer, positions, etc.)
     * @return this JsonObjectOptions for method chaining
     */
    public JsonObjectOptions setIndexing(TextFieldIndexing indexing) {
        this.indexing = indexing;
        return this;
    }

    /**
     * Enable columnar storage (fast fields) for efficient filtering and aggregations.
     *
     * <p>Fast fields provide O(1) document value access and are ideal for:
     * <ul>
     *   <li>Range queries on numeric/date fields</li>
     *   <li>Sorting results by field values</li>
     *   <li>Aggregations and faceting</li>
     * </ul>
     *
     * <p>Trade-off: Increases storage by ~20-30% but improves query performance by 3-5x
     * for filtering operations.
     *
     * @param fast true to enable fast fields
     * @return this JsonObjectOptions for method chaining
     */
    public JsonObjectOptions setFast(boolean fast) {
        this.fast = fast;
        return this;
    }

    /**
     * Set tokenizer for fast field text values.
     *
     * <p>This tokenizer is used for text values stored in fast fields.
     * Common options:
     * <ul>
     *   <li><b>"raw"</b> - No tokenization, exact values only</li>
     *   <li><b>"default"</b> - Standard tokenization with lowercasing</li>
     * </ul>
     *
     * @param tokenizer Tokenizer name (e.g., "raw", "default")
     * @return this JsonObjectOptions for method chaining
     */
    public JsonObjectOptions setFastTokenizer(String tokenizer) {
        this.fastTokenizer = tokenizer;
        return this;
    }

    /**
     * Enable dot expansion in JSON keys.
     *
     * <p>When enabled:
     * <ul>
     *   <li>{"k8s.node.id": 5} is treated as {"k8s": {"node": {"id": 5}}}</li>
     *   <li>Query syntax: k8s.node.id:5 (natural path syntax)</li>
     * </ul>
     *
     * <p>When disabled:
     * <ul>
     *   <li>{"k8s.node.id": 5} is treated as a key with literal dots</li>
     *   <li>Query syntax: k8s\.node\.id:5 (dots must be escaped)</li>
     * </ul>
     *
     * @param expandDots true to enable dot expansion
     * @return this JsonObjectOptions for method chaining
     */
    public JsonObjectOptions setExpandDots(boolean expandDots) {
        this.expandDots = expandDots;
        return this;
    }

    // Getters

    /**
     * Check if the JSON object is stored.
     *
     * @return true if stored
     */
    public boolean isStored() {
        return stored;
    }

    /**
     * Get the text field indexing configuration.
     *
     * @return TextFieldIndexing if indexing is enabled, null otherwise
     */
    public TextFieldIndexing getIndexing() {
        return indexing;
    }

    /**
     * Check if fast fields are enabled.
     *
     * @return true if fast fields are enabled
     */
    public boolean isFast() {
        return fast;
    }

    /**
     * Get the fast field tokenizer name.
     *
     * @return Tokenizer name or null if not set
     */
    public String getFastTokenizer() {
        return fastTokenizer;
    }

    /**
     * Check if dot expansion is enabled.
     *
     * @return true if dots are expanded to nested paths
     */
    public boolean isExpandDots() {
        return expandDots;
    }

    // Static factory methods for common configurations

    /**
     * Create JSON field that is only stored (not searchable).
     *
     * <p>Use when you need to retrieve JSON but don't need to query it.
     *
     * @return JsonObjectOptions with stored=true
     */
    public static JsonObjectOptions stored() {
        return create().setStored(true);
    }

    /**
     * Create JSON field that is indexed (searchable) but not stored.
     *
     * <p>Use when you need to query JSON but don't need to retrieve it.
     *
     * @return JsonObjectOptions with default indexing enabled
     */
    public static JsonObjectOptions indexed() {
        return create().setIndexing(TextFieldIndexing.defaultIndexing());
    }

    /**
     * Create JSON field that is both stored and indexed.
     *
     * <p>This is the most common configuration for JSON fields.
     *
     * @return JsonObjectOptions with stored=true and default indexing
     */
    public static JsonObjectOptions storedAndIndexed() {
        return create()
            .setStored(true)
            .setIndexing(TextFieldIndexing.defaultIndexing());
    }

    /**
     * Create JSON field with all features enabled.
     *
     * <p>Enables:
     * <ul>
     *   <li>Stored - can retrieve original JSON</li>
     *   <li>Indexed - can search text values</li>
     *   <li>Fast fields - efficient filtering and aggregations</li>
     *   <li>Dot expansion - natural path syntax</li>
     * </ul>
     *
     * <p>This provides maximum flexibility but uses more storage.
     *
     * @return JsonObjectOptions with all features enabled
     */
    public static JsonObjectOptions full() {
        return create()
            .setStored(true)
            .setIndexing(TextFieldIndexing.withPositionsStatic())
            .setFast(true)
            .setExpandDots(true);
    }

    @Override
    public String toString() {
        return "JsonObjectOptions{" +
                "stored=" + stored +
                ", indexing=" + (indexing != null ? "enabled" : "disabled") +
                ", fast=" + fast +
                ", fastTokenizer='" + fastTokenizer + '\'' +
                ", expandDots=" + expandDots +
                '}';
    }
}

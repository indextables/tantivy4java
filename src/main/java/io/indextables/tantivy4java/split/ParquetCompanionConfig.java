package io.indextables.tantivy4java.split;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.indextables.tantivy4java.delta.DeltaTableSchema;
import io.indextables.tantivy4java.iceberg.IcebergTableSchema;
import io.indextables.tantivy4java.parquet.ParquetSchemaReader;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Configuration for parquet companion mode in SplitSearcher.
 *
 * When a split was created with parquet companion mode, document retrieval
 * is served from external parquet files instead of the tantivy store.
 * This config provides the necessary storage credentials and settings
 * for accessing those parquet files.
 */
public class ParquetCompanionConfig {

    /**
     * Compact string indexing mode constants for use with
     * {@link ParquetCompanionConfig#withTokenizerOverrides(Map)}.
     *
     * <p>These modes reduce index size for string fields by replacing full text
     * indexing with compact hash-based indexing or by stripping patterns (e.g., UUIDs)
     * before text indexing.</p>
     */
    public static final class StringIndexingMode {
        private StringIndexingMode() {} // prevent instantiation

        /** Index xxHash64 as U64 (TERM + fast). No Str field. Term queries auto-hashed. */
        public static final String EXACT_ONLY = "exact_only";

        /** Strip UUIDs, index text with "default" tokenizer. UUIDs stored as hash in companion field. */
        public static final String TEXT_UUID_EXACTONLY = "text_uuid_exactonly";

        /** Strip UUIDs, index text with "default" tokenizer. UUIDs discarded. */
        public static final String TEXT_UUID_STRIP = "text_uuid_strip";

        /** Strip custom regex matches, index text. Matches stored as hash in companion field. */
        public static String textCustomExactonly(String regex) {
            if (regex == null || regex.isEmpty()) {
                throw new IllegalArgumentException("regex must not be null or empty");
            }
            return "text_custom_exactonly:" + regex;
        }

        /** Strip custom regex matches, index text. Matches discarded. */
        public static String textCustomStrip(String regex) {
            if (regex == null || regex.isEmpty()) {
                throw new IllegalArgumentException("regex must not be null or empty");
            }
            return "text_custom_strip:" + regex;
        }
    }

    /**
     * Fast field mode determines how fast fields (columnar data) are served.
     */
    public enum FastFieldMode {
        /** All fast fields from native tantivy columnar data (default). */
        DISABLED,
        /** Numeric/bool/date fast fields native, string fast fields from parquet. */
        HYBRID,
        /** All fast fields decoded from parquet at query time. */
        PARQUET_ONLY
    }

    /**
     * Policy for handling missing or modified parquet files at query time.
     */
    public enum MissingFilePolicy {
        /** Fail the query if any parquet file is missing or modified. */
        FAIL,
        /** Log a warning but continue (may return partial results). */
        WARN
    }

    /**
     * Storage credentials for accessing parquet files.
     * Parquet files may use different credentials than the split storage.
     */
    public static class ParquetStorageConfig {
        private final Map<String, String> awsConfig = new HashMap<>();
        private final Map<String, String> azureConfig = new HashMap<>();

        /** Configure AWS S3 credentials for parquet file access. */
        public ParquetStorageConfig withAwsCredentials(String accessKey, String secretKey) {
            awsConfig.put("access_key", accessKey);
            awsConfig.put("secret_key", secretKey);
            return this;
        }

        /** Configure AWS S3 credentials with session token for parquet file access. */
        public ParquetStorageConfig withAwsCredentials(String accessKey, String secretKey, String sessionToken) {
            awsConfig.put("access_key", accessKey);
            awsConfig.put("secret_key", secretKey);
            awsConfig.put("session_token", sessionToken);
            return this;
        }

        /** Configure AWS region for parquet file access. */
        public ParquetStorageConfig withAwsRegion(String region) {
            awsConfig.put("region", region);
            return this;
        }

        /** Configure custom AWS endpoint (for MinIO, etc). */
        public ParquetStorageConfig withAwsEndpoint(String endpoint) {
            awsConfig.put("endpoint", endpoint);
            return this;
        }

        /** Enable path-style access for S3-compatible storage. */
        public ParquetStorageConfig withAwsPathStyleAccess(boolean pathStyle) {
            awsConfig.put("path_style_access", String.valueOf(pathStyle));
            return this;
        }

        /** Configure Azure storage credentials for parquet file access. */
        public ParquetStorageConfig withAzureCredentials(String accountName, String accessKey) {
            azureConfig.put("account_name", accountName);
            azureConfig.put("access_key", accessKey);
            return this;
        }

        /** Configure Azure OAuth bearer token for parquet file access. */
        public ParquetStorageConfig withAzureBearerToken(String accountName, String bearerToken) {
            azureConfig.put("account_name", accountName);
            azureConfig.put("bearer_token", bearerToken);
            return this;
        }

        public Map<String, String> getAwsConfig() { return Collections.unmodifiableMap(awsConfig); }
        public Map<String, String> getAzureConfig() { return Collections.unmodifiableMap(azureConfig); }
    }

    private String tableRoot;
    private FastFieldMode fastFieldMode = FastFieldMode.DISABLED;
    private ParquetStorageConfig parquetStorage;
    private MissingFilePolicy missingFilePolicy = MissingFilePolicy.FAIL;
    private String[] defaultRetrievalFields;

    // Indexing pipeline fields (Phase 3)
    private String[] statisticsFields;
    private int statisticsTruncateLength = 256;
    private Map<String, String> fieldIdMapping;
    private boolean autoDetectNameMapping = false;
    private String[] skipFields;
    private Map<String, String> tokenizerOverrides;
    private String[] ipAddressFields;
    private String[] jsonFields;
    private String indexUid = "parquet-index";
    private String sourceId = "parquet-source";
    private String nodeId = "parquet-node";
    private long writerHeapSize = 256_000_000L;
    private int readerBatchSize = 8192;
    private boolean stringHashOptimization = true;
    private boolean fieldnormsEnabled = false;

    private static final ObjectMapper MAPPER = new ObjectMapper();

    /** Create a parquet companion config with the table root path. */
    public ParquetCompanionConfig(String tableRoot) {
        this.tableRoot = Objects.requireNonNull(tableRoot, "tableRoot must not be null");
    }

    /** Set the fast field mode. */
    public ParquetCompanionConfig withFastFieldMode(FastFieldMode mode) {
        this.fastFieldMode = Objects.requireNonNull(mode, "mode must not be null");
        return this;
    }

    /** Set the parquet storage configuration (credentials for parquet file access). */
    public ParquetCompanionConfig withParquetStorage(ParquetStorageConfig storage) {
        this.parquetStorage = storage;
        return this;
    }

    /** Set the policy for handling missing parquet files. */
    public ParquetCompanionConfig withMissingFilePolicy(MissingFilePolicy policy) {
        this.missingFilePolicy = Objects.requireNonNull(policy, "policy must not be null");
        return this;
    }

    /** Set default fields to retrieve when no projection is specified. */
    public ParquetCompanionConfig withDefaultRetrievalFields(String... fields) {
        this.defaultRetrievalFields = fields != null ? fields.clone() : null;
        return this;
    }

    /** Set fields to compute column statistics for during indexing. */
    public ParquetCompanionConfig withStatisticsFields(String... fields) {
        this.statisticsFields = fields != null ? fields.clone() : null;
        return this;
    }

    /** Set the max string length for statistics truncation (default: 256). */
    public ParquetCompanionConfig withStatisticsTruncateLength(int length) {
        if (length < 1) {
            throw new IllegalArgumentException("Statistics truncate length must be at least 1. Got: " + length);
        }
        this.statisticsTruncateLength = length;
        return this;
    }

    /** Set explicit field ID mapping (parquet column name to display name). */
    public ParquetCompanionConfig withFieldIdMapping(Map<String, String> mapping) {
        this.fieldIdMapping = mapping;
        return this;
    }

    /** Enable auto-detection of Iceberg field ID mapping from parquet metadata. */
    public ParquetCompanionConfig withAutoDetectNameMapping(boolean autoDetect) {
        this.autoDetectNameMapping = autoDetect;
        return this;
    }

    /**
     * Apply column name mapping from a Delta table schema.
     *
     * <p>When {@code delta.columnMapping.mode} is {@code "name"} or {@code "id"},
     * parquet files use physical column names (e.g. "col-abc123") that differ from
     * the logical Delta column names (e.g. "id", "name"). This method extracts
     * the physical-to-logical mapping from the Delta schema metadata and applies it
     * to this config, so the indexing and reading pipelines use the correct names.
     *
     * <p>If the Delta schema has no column mapping configured (all fields use
     * identity mapping), this method still sets the mapping — identity mappings
     * are harmless and ensure consistent behavior.
     *
     * <h3>Usage Example</h3>
     * <pre>{@code
     * DeltaTableSchema deltaSchema = DeltaTableReader.readSchema(deltaTableUrl, config);
     * ParquetCompanionConfig companionConfig = new ParquetCompanionConfig(tableRoot)
     *     .withDeltaColumnMapping(deltaSchema)
     *     .withFastFieldMode(FastFieldMode.HYBRID);
     * }</pre>
     *
     * @param deltaSchema the Delta table schema (from {@link io.indextables.tantivy4java.delta.DeltaTableReader#readSchema})
     * @return this config for chaining
     * @throws IllegalArgumentException if deltaSchema is null
     */
    public ParquetCompanionConfig withDeltaColumnMapping(DeltaTableSchema deltaSchema) {
        if (deltaSchema == null) {
            throw new IllegalArgumentException("deltaSchema must not be null");
        }
        this.fieldIdMapping = deltaSchema.getColumnNameMapping();
        return this;
    }

    /**
     * Apply column name mapping from an Iceberg table schema by reading field IDs
     * from a sample parquet file.
     *
     * <p>Databricks Unity Catalog (and other engines using Iceberg column mapping)
     * store data in parquet files using physical column names (e.g. "col_1", "col_2")
     * that differ from the logical Iceberg column names (e.g. "id", "name").
     * This method reads a sample parquet file's metadata, extracts field IDs, and
     * maps physical parquet column names to logical Iceberg names.
     *
     * <h3>Usage Example</h3>
     * <pre>{@code
     * IcebergTableSchema icebergSchema = IcebergTableReader.readSchema(
     *     catalog, namespace, table, catalogConfig);
     * List<IcebergFileEntry> files = IcebergTableReader.listFiles(
     *     catalog, namespace, table, catalogConfig);
     * String sampleParquetUrl = files.get(0).getPath();
     *
     * ParquetCompanionConfig companionConfig = new ParquetCompanionConfig(tableRoot)
     *     .withIcebergColumnMapping(sampleParquetUrl, icebergSchema)
     *     .withFastFieldMode(FastFieldMode.HYBRID);
     * }</pre>
     *
     * @param sampleParquetUrl URL of a parquet file from the table (local, file://, s3://, azure://)
     * @param icebergSchema the Iceberg table schema (from {@link io.indextables.tantivy4java.iceberg.IcebergTableReader#readSchema})
     * @return this config for chaining
     * @throws IllegalArgumentException if sampleParquetUrl or icebergSchema is null
     * @throws RuntimeException if the parquet file cannot be read
     */
    public ParquetCompanionConfig withIcebergColumnMapping(
            String sampleParquetUrl, IcebergTableSchema icebergSchema) {
        return withIcebergColumnMapping(sampleParquetUrl, icebergSchema, Collections.emptyMap());
    }

    /**
     * Apply column name mapping from an Iceberg table schema by reading field IDs
     * from a sample parquet file, with storage credentials.
     *
     * @param sampleParquetUrl URL of a parquet file from the table (local, file://, s3://, azure://)
     * @param icebergSchema the Iceberg table schema
     * @param storageConfig storage credentials for accessing the parquet file
     * @return this config for chaining
     * @throws IllegalArgumentException if sampleParquetUrl or icebergSchema is null
     * @throws RuntimeException if the parquet file cannot be read
     * @see #withIcebergColumnMapping(String, IcebergTableSchema)
     */
    public ParquetCompanionConfig withIcebergColumnMapping(
            String sampleParquetUrl,
            IcebergTableSchema icebergSchema,
            Map<String, String> storageConfig) {
        if (sampleParquetUrl == null || sampleParquetUrl.isEmpty()) {
            throw new IllegalArgumentException("sampleParquetUrl must not be null or empty");
        }
        if (icebergSchema == null) {
            throw new IllegalArgumentException("icebergSchema must not be null");
        }
        this.fieldIdMapping = ParquetSchemaReader.readColumnMapping(
                sampleParquetUrl,
                icebergSchema.getFieldIdToNameMap(),
                storageConfig != null ? storageConfig : Collections.emptyMap());
        return this;
    }

    /** Set fields to skip during schema derivation and indexing. */
    public ParquetCompanionConfig withSkipFields(String... fields) {
        this.skipFields = fields != null ? fields.clone() : null;
        return this;
    }

    /** Set custom tokenizer overrides per field (field name to tokenizer name). */
    public ParquetCompanionConfig withTokenizerOverrides(Map<String, String> overrides) {
        this.tokenizerOverrides = overrides;
        return this;
    }

    /**
     * Declare fields that should be treated as IP address type.
     * Parquet has no native IP type, so IPs are stored as UTF8 strings.
     * This tells the indexing pipeline to parse these string columns as IPs
     * and create tantivy ip_addr fields. In HYBRID mode, IP fields use
     * native fast fields (like numerics), not parquet transcoding.
     */
    public ParquetCompanionConfig withIpAddressFields(String... fields) {
        this.ipAddressFields = fields != null ? fields.clone() : null;
        return this;
    }

    /**
     * Declare fields that should be treated as JSON object type.
     * Parquet has no native JSON type, so JSON data is stored as UTF8 strings.
     * This tells the indexing pipeline to parse these string columns as JSON objects
     * and create tantivy json_object fields, enabling nested path queries
     * (e.g. "payload.user:alice").
     */
    public ParquetCompanionConfig withJsonFields(String... fields) {
        this.jsonFields = fields != null ? fields.clone() : null;
        return this;
    }

    /** Set the index UID for split metadata. */
    public ParquetCompanionConfig withIndexUid(String uid) {
        this.indexUid = uid;
        return this;
    }

    /** Set the source ID for split metadata. */
    public ParquetCompanionConfig withSourceId(String id) {
        this.sourceId = id;
        return this;
    }

    /** Set the node ID for split metadata. */
    public ParquetCompanionConfig withNodeId(String id) {
        this.nodeId = id;
        return this;
    }

    /**
     * Set the tantivy writer heap size in bytes.
     * Controls how much memory the indexer can use before flushing to disk.
     * Larger values reduce the chance of creating multiple segments (which is
     * important for maintaining the docid-to-parquet-row mapping) but use more RAM.
     * Default: 256MB. Minimum: 15MB.
     */
    public ParquetCompanionConfig withWriterHeapSize(long heapSize) {
        if (heapSize < 15_000_000L) {
            throw new IllegalArgumentException(
                "Writer heap size must be at least 15MB (15000000 bytes). Got: " + heapSize);
        }
        this.writerHeapSize = heapSize;
        return this;
    }

    /**
     * Enable or disable the string hash fast-field optimization for HYBRID mode.
     *
     * <p>When enabled (default), the indexing pipeline stores a hidden {@code _phash_<field>}
     * U64 fast field alongside each raw-tokenizer string fast field. At query time, aggregations
     * such as {@code terms}, {@code value_count}, and {@code cardinality} on those string fields
     * are transparently redirected to the cheaper U64 hash field. Bucket keys are resolved
     * back to the original strings after the aggregation completes.
     *
     * <p>Disable this if you need exact string ordering semantics for {@code order: _key}
     * terms aggregations, or if you prefer to avoid the extra storage overhead.
     *
     * <p>Default: {@code true}.
     */
    public ParquetCompanionConfig withStringHashOptimization(boolean enable) {
        this.stringHashOptimization = enable;
        return this;
    }

    /**
     * Enable or disable fieldnorms for text fields in companion splits.
     *
     * <p>Fieldnorms store 1 byte per document per indexed field, encoding approximate
     * token count for BM25 length normalization. For companion splits, most text fields
     * use the "raw" tokenizer (1 token per value), making fieldnorms constant and wasteful.
     * For 10M docs with 20 fields, that's ~200MB of redundant data.
     *
     * <p>Default: {@code false} (fieldnorms disabled).
     *
     * @param enable true to enable fieldnorms (needed for BM25 scoring with variable-length text)
     * @return this config for chaining
     */
    public ParquetCompanionConfig withFieldnorms(boolean enable) {
        this.fieldnormsEnabled = enable;
        return this;
    }

    /**
     * Set the number of rows per Arrow RecordBatch when reading parquet files.
     * Larger batches reduce per-batch overhead but use more memory per batch.
     * Default: 8192. Typical range: 1024 to 65536.
     */
    public ParquetCompanionConfig withReaderBatchSize(int batchSize) {
        if (batchSize < 1) {
            throw new IllegalArgumentException("Reader batch size must be at least 1. Got: " + batchSize);
        }
        this.readerBatchSize = batchSize;
        return this;
    }

    public String getTableRoot() { return tableRoot; }
    public FastFieldMode getFastFieldMode() { return fastFieldMode; }
    public ParquetStorageConfig getParquetStorage() { return parquetStorage; }
    public MissingFilePolicy getMissingFilePolicy() { return missingFilePolicy; }
    public String[] getDefaultRetrievalFields() { return defaultRetrievalFields != null ? defaultRetrievalFields.clone() : null; }
    public String[] getStatisticsFields() { return statisticsFields != null ? statisticsFields.clone() : null; }
    public int getStatisticsTruncateLength() { return statisticsTruncateLength; }
    public Map<String, String> getFieldIdMapping() { return fieldIdMapping != null ? Collections.unmodifiableMap(fieldIdMapping) : null; }
    public boolean isAutoDetectNameMapping() { return autoDetectNameMapping; }
    public String[] getSkipFields() { return skipFields != null ? skipFields.clone() : null; }
    public Map<String, String> getTokenizerOverrides() { return tokenizerOverrides != null ? Collections.unmodifiableMap(tokenizerOverrides) : null; }
    public String[] getIpAddressFields() { return ipAddressFields != null ? ipAddressFields.clone() : null; }
    public String[] getJsonFields() { return jsonFields != null ? jsonFields.clone() : null; }
    public String getIndexUid() { return indexUid; }
    public String getSourceId() { return sourceId; }
    public String getNodeId() { return nodeId; }
    public long getWriterHeapSize() { return writerHeapSize; }
    public int getReaderBatchSize() { return readerBatchSize; }
    public boolean isStringHashOptimization() { return stringHashOptimization; }
    public boolean isFieldnormsEnabled() { return fieldnormsEnabled; }

    /**
     * Convert this config to a JSON string for the createFromParquet native method.
     * This includes all indexing pipeline configuration.
     */
    public String toIndexingConfigJson() {
        Map<String, Object> map = new LinkedHashMap<>();
        map.put("table_root", tableRoot);
        map.put("fast_field_mode", fastFieldMode.name());
        map.put("missing_file_policy", missingFilePolicy.name());
        map.put("index_uid", indexUid);
        map.put("source_id", sourceId);
        map.put("node_id", nodeId);
        map.put("statistics_truncate_length", statisticsTruncateLength);
        map.put("auto_detect_name_mapping", autoDetectNameMapping);
        map.put("writer_heap_size", writerHeapSize);
        map.put("reader_batch_size", readerBatchSize);
        map.put("string_hash_optimization", stringHashOptimization);
        map.put("fieldnorms_enabled", fieldnormsEnabled);

        if (statisticsFields != null && statisticsFields.length > 0) {
            map.put("statistics_fields", Arrays.asList(statisticsFields));
        }
        if (defaultRetrievalFields != null && defaultRetrievalFields.length > 0) {
            map.put("default_retrieval_fields", Arrays.asList(defaultRetrievalFields));
        }
        if (skipFields != null && skipFields.length > 0) {
            map.put("skip_fields", Arrays.asList(skipFields));
        }
        if (tokenizerOverrides != null && !tokenizerOverrides.isEmpty()) {
            map.put("tokenizer_overrides", tokenizerOverrides);
        }
        if (ipAddressFields != null && ipAddressFields.length > 0) {
            map.put("ip_address_fields", Arrays.asList(ipAddressFields));
        }
        if (jsonFields != null && jsonFields.length > 0) {
            map.put("json_fields", Arrays.asList(jsonFields));
        }
        if (fieldIdMapping != null && !fieldIdMapping.isEmpty()) {
            map.put("field_id_mapping", fieldIdMapping);
        }
        if (parquetStorage != null) {
            if (!parquetStorage.awsConfig.isEmpty()) {
                map.put("parquet_aws_config", parquetStorage.awsConfig);
            }
            if (!parquetStorage.azureConfig.isEmpty()) {
                map.put("parquet_azure_config", parquetStorage.azureConfig);
            }
        }

        try {
            return MAPPER.writeValueAsString(map);
        } catch (JsonProcessingException e) {
            throw new RuntimeException("Failed to serialize indexing config to JSON", e);
        }
    }
}

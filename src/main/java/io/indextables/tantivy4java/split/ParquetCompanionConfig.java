package io.indextables.tantivy4java.split;

import java.util.HashMap;
import java.util.Map;

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

        public Map<String, String> getAwsConfig() { return awsConfig; }
        public Map<String, String> getAzureConfig() { return azureConfig; }
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
    private String indexUid = "parquet-index";
    private String sourceId = "parquet-source";
    private String nodeId = "parquet-node";

    /** Create a parquet companion config with the table root path. */
    public ParquetCompanionConfig(String tableRoot) {
        this.tableRoot = tableRoot;
    }

    /** Set the fast field mode. */
    public ParquetCompanionConfig withFastFieldMode(FastFieldMode mode) {
        this.fastFieldMode = mode;
        return this;
    }

    /** Set the parquet storage configuration (credentials for parquet file access). */
    public ParquetCompanionConfig withParquetStorage(ParquetStorageConfig storage) {
        this.parquetStorage = storage;
        return this;
    }

    /** Set the policy for handling missing parquet files. */
    public ParquetCompanionConfig withMissingFilePolicy(MissingFilePolicy policy) {
        this.missingFilePolicy = policy;
        return this;
    }

    /** Set default fields to retrieve when no projection is specified. */
    public ParquetCompanionConfig withDefaultRetrievalFields(String... fields) {
        this.defaultRetrievalFields = fields;
        return this;
    }

    /** Set fields to compute column statistics for during indexing. */
    public ParquetCompanionConfig withStatisticsFields(String... fields) {
        this.statisticsFields = fields;
        return this;
    }

    /** Set the max string length for statistics truncation (default: 256). */
    public ParquetCompanionConfig withStatisticsTruncateLength(int length) {
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

    /** Set fields to skip during schema derivation and indexing. */
    public ParquetCompanionConfig withSkipFields(String... fields) {
        this.skipFields = fields;
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
        this.ipAddressFields = fields;
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

    public String getTableRoot() { return tableRoot; }
    public FastFieldMode getFastFieldMode() { return fastFieldMode; }
    public ParquetStorageConfig getParquetStorage() { return parquetStorage; }
    public MissingFilePolicy getMissingFilePolicy() { return missingFilePolicy; }
    public String[] getDefaultRetrievalFields() { return defaultRetrievalFields; }
    public String[] getStatisticsFields() { return statisticsFields; }
    public int getStatisticsTruncateLength() { return statisticsTruncateLength; }
    public Map<String, String> getFieldIdMapping() { return fieldIdMapping; }
    public boolean isAutoDetectNameMapping() { return autoDetectNameMapping; }
    public String[] getSkipFields() { return skipFields; }
    public Map<String, String> getTokenizerOverrides() { return tokenizerOverrides; }
    public String[] getIpAddressFields() { return ipAddressFields; }
    public String getIndexUid() { return indexUid; }
    public String getSourceId() { return sourceId; }
    public String getNodeId() { return nodeId; }

    /**
     * Convert this config to a map for passing through JNI.
     */
    public Map<String, Object> toConfigMap() {
        Map<String, Object> map = new HashMap<>();
        map.put("parquet_table_root", tableRoot);
        map.put("fast_field_mode", fastFieldMode.name());
        map.put("missing_file_policy", missingFilePolicy.name());
        if (defaultRetrievalFields != null) {
            map.put("default_retrieval_fields", defaultRetrievalFields);
        }
        if (parquetStorage != null) {
            if (!parquetStorage.getAwsConfig().isEmpty()) {
                map.put("parquet_aws_config", parquetStorage.getAwsConfig());
            }
            if (!parquetStorage.getAzureConfig().isEmpty()) {
                map.put("parquet_azure_config", parquetStorage.getAzureConfig());
            }
        }
        return map;
    }

    /**
     * Convert this config to a JSON string for the createFromParquet native method.
     * This includes all indexing pipeline configuration.
     */
    public String toIndexingConfigJson() {
        StringBuilder sb = new StringBuilder();
        sb.append("{");
        sb.append("\"table_root\":").append(jsonString(tableRoot));
        sb.append(",\"fast_field_mode\":").append(jsonString(fastFieldMode.name()));
        sb.append(",\"index_uid\":").append(jsonString(indexUid));
        sb.append(",\"source_id\":").append(jsonString(sourceId));
        sb.append(",\"node_id\":").append(jsonString(nodeId));
        sb.append(",\"statistics_truncate_length\":").append(statisticsTruncateLength);
        sb.append(",\"auto_detect_name_mapping\":").append(autoDetectNameMapping);

        if (statisticsFields != null && statisticsFields.length > 0) {
            sb.append(",\"statistics_fields\":[");
            for (int i = 0; i < statisticsFields.length; i++) {
                if (i > 0) sb.append(",");
                sb.append(jsonString(statisticsFields[i]));
            }
            sb.append("]");
        }

        if (skipFields != null && skipFields.length > 0) {
            sb.append(",\"skip_fields\":[");
            for (int i = 0; i < skipFields.length; i++) {
                if (i > 0) sb.append(",");
                sb.append(jsonString(skipFields[i]));
            }
            sb.append("]");
        }

        if (tokenizerOverrides != null && !tokenizerOverrides.isEmpty()) {
            sb.append(",\"tokenizer_overrides\":{");
            boolean first = true;
            for (Map.Entry<String, String> entry : tokenizerOverrides.entrySet()) {
                if (!first) sb.append(",");
                sb.append(jsonString(entry.getKey())).append(":").append(jsonString(entry.getValue()));
                first = false;
            }
            sb.append("}");
        }

        if (ipAddressFields != null && ipAddressFields.length > 0) {
            sb.append(",\"ip_address_fields\":[");
            for (int i = 0; i < ipAddressFields.length; i++) {
                if (i > 0) sb.append(",");
                sb.append(jsonString(ipAddressFields[i]));
            }
            sb.append("]");
        }

        if (fieldIdMapping != null && !fieldIdMapping.isEmpty()) {
            sb.append(",\"field_id_mapping\":{");
            boolean first = true;
            for (Map.Entry<String, String> entry : fieldIdMapping.entrySet()) {
                if (!first) sb.append(",");
                sb.append(jsonString(entry.getKey())).append(":").append(jsonString(entry.getValue()));
                first = false;
            }
            sb.append("}");
        }

        sb.append("}");
        return sb.toString();
    }

    private static String jsonString(String value) {
        if (value == null) return "null";
        return "\"" + value.replace("\\", "\\\\").replace("\"", "\\\"") + "\"";
    }
}

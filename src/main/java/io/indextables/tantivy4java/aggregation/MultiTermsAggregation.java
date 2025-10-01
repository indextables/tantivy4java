package io.indextables.tantivy4java.aggregation;

import io.indextables.tantivy4java.split.SplitAggregation;

import java.util.Arrays;
import java.util.Map;
import java.util.HashMap;

/**
 * Multi-dimensional terms aggregation that groups documents by multiple fields simultaneously.
 * This enables SQL-like GROUP BY operations across multiple fields (e.g., GROUP BY field1, field2, field3).
 *
 * Uses nested TermsAggregation structures internally to leverage Tantivy's native aggregation engine
 * while providing a simplified API for multi-dimensional grouping.
 *
 * Example usage:
 * <pre>{@code
 * // GROUP BY year, month, day
 * MultiTermsAggregation multiGroup = new MultiTermsAggregation("time_analysis",
 *     new String[]{"year", "month", "day"}, 1000);
 *
 * // Add metric sub-aggregations
 * multiGroup.addSubAggregation("total_sales", new SumAggregation("sales_amount"));
 * multiGroup.addSubAggregation("avg_price", new AverageAggregation("price"));
 *
 * SearchResult result = searcher.search(matchAllQuery, 0, "multi_group", multiGroup);
 * MultiTermsResult terms = (MultiTermsResult) result.getAggregation("multi_group");
 *
 * for (MultiTermsResult.MultiTermsBucket bucket : terms.getBuckets()) {
 *     String[] fieldValues = bucket.getFieldValues();
 *     String year = fieldValues[0];
 *     String month = fieldValues[1];
 *     String day = fieldValues[2];
 *
 *     SumResult totalSales = (SumResult) bucket.getSubAggregation("total_sales");
 *     System.out.println(year + "/" + month + "/" + day + ": $" + totalSales.getSum());
 * }
 * }</pre>
 */
public class MultiTermsAggregation extends SplitAggregation {

    private final String[] fields;
    private final int size;
    private final int shardSize;
    private final Map<String, SplitAggregation> subAggregations;

    /**
     * Creates a multi-terms aggregation with default size.
     *
     * @param fields Array of field names to group by (in order)
     */
    public MultiTermsAggregation(String[] fields) {
        this("multi_terms_" + (fields != null ? String.join("_", fields) : "null"), fields, 10, 0);
    }

    /**
     * Creates a multi-terms aggregation with custom size.
     *
     * @param fields Array of field names to group by (in order)
     * @param size Maximum number of top-level buckets to return
     */
    public MultiTermsAggregation(String[] fields, int size) {
        this("multi_terms_" + (fields != null ? String.join("_", fields) : "null"), fields, size, 0);
    }

    /**
     * Creates a named multi-terms aggregation with full configuration.
     *
     * @param name The name for this aggregation
     * @param fields Array of field names to group by (in order)
     * @param size Maximum number of top-level buckets to return
     * @param shardSize Number of buckets to return from each shard (0 = auto)
     */
    public MultiTermsAggregation(String name, String[] fields, int size, int shardSize) {
        super(name);
        if (fields == null || fields.length == 0) {
            throw new IllegalArgumentException("Fields array cannot be null or empty");
        }
        if (fields.length == 1) {
            throw new IllegalArgumentException("Use TermsAggregation for single field grouping");
        }
        for (String field : fields) {
            if (field == null || field.trim().isEmpty()) {
                throw new IllegalArgumentException("Field names cannot be null or empty");
            }
        }
        if (size <= 0) {
            throw new IllegalArgumentException("Size must be positive");
        }

        this.fields = Arrays.copyOf(fields, fields.length);
        this.size = size;
        this.shardSize = shardSize;
        this.subAggregations = new HashMap<>();
    }

    /**
     * Adds a sub-aggregation to be calculated within each multi-dimensional bucket.
     *
     * @param name The name for the sub-aggregation
     * @param aggregation The sub-aggregation to add (SumAggregation, AverageAggregation, etc.)
     */
    public void addSubAggregation(String name, SplitAggregation aggregation) {
        if (name == null || name.trim().isEmpty()) {
            throw new IllegalArgumentException("Sub-aggregation name cannot be null or empty");
        }
        if (aggregation == null) {
            throw new IllegalArgumentException("Sub-aggregation cannot be null");
        }
        this.subAggregations.put(name.trim(), aggregation);
    }

    /**
     * Gets the array of field names being grouped by.
     */
    public String[] getFields() {
        return Arrays.copyOf(fields, fields.length);
    }

    /**
     * Gets the first field name (for compatibility with single-field methods).
     */
    @Override
    public String getFieldName() {
        return fields[0];
    }

    @Override
    public String getAggregationType() {
        return "multi_terms";
    }

    /**
     * Generates nested TermsAggregation JSON structure.
     *
     * Creates a hierarchy like:
     * {
     *   "terms": { "field": "field1", "size": size },
     *   "aggs": {
     *     "field2_terms": {
     *       "terms": { "field": "field2", "size": size },
     *       "aggs": {
     *         "field3_terms": {
     *           "terms": { "field": "field3", "size": size },
     *           "aggs": { ... user sub-aggregations ... }
     *         }
     *       }
     *     }
     *   }
     * }
     */
    @Override
    public String toAggregationJson() {
        return generateNestedAggregationJson(0, subAggregations);
    }

    /**
     * Recursively generates nested terms aggregation JSON.
     */
    private String generateNestedAggregationJson(int fieldIndex, Map<String, SplitAggregation> leafSubAggs) {
        if (fieldIndex >= fields.length) {
            // Base case: no more fields, add user sub-aggregations
            if (leafSubAggs.isEmpty()) {
                return "{}";
            }

            StringBuilder json = new StringBuilder();
            json.append("{");
            boolean first = true;
            for (Map.Entry<String, SplitAggregation> entry : leafSubAggs.entrySet()) {
                if (!first) {
                    json.append(", ");
                }
                json.append("\"").append(entry.getKey()).append("\": ");
                json.append(entry.getValue().toAggregationJson());
                first = false;
            }
            json.append("}");
            return json.toString();
        }

        // Current field terms aggregation
        String currentField = fields[fieldIndex];
        StringBuilder json = new StringBuilder();
        json.append("{\"terms\": {\"field\": \"").append(currentField).append("\"");
        json.append(", \"size\": ").append(size);
        if (shardSize > 0) {
            json.append(", \"shard_size\": ").append(shardSize);
        }
        json.append("}");

        // Add nested aggregation for next field (or leaf sub-aggregations)
        if (fieldIndex < fields.length - 1 || !leafSubAggs.isEmpty()) {
            json.append(", \"aggs\": {");

            if (fieldIndex < fields.length - 1) {
                // More fields to nest
                String nextFieldAggName = fields[fieldIndex + 1] + "_terms";
                json.append("\"").append(nextFieldAggName).append("\": ");
                json.append(generateNestedAggregationJson(fieldIndex + 1, leafSubAggs));
            } else {
                // Last field, add user sub-aggregations directly
                boolean first = true;
                for (Map.Entry<String, SplitAggregation> entry : leafSubAggs.entrySet()) {
                    if (!first) {
                        json.append(", ");
                    }
                    json.append("\"").append(entry.getKey()).append("\": ");
                    json.append(entry.getValue().toAggregationJson());
                    first = false;
                }
            }

            json.append("}");
        }

        json.append("}");
        return json.toString();
    }

    @Override
    public String toString() {
        return "MultiTermsAggregation{" +
                "name='" + getName() + '\'' +
                ", fields=" + Arrays.toString(fields) +
                ", size=" + size +
                ", shardSize=" + shardSize +
                ", subAggregations=" + subAggregations.keySet() +
                '}';
    }
}
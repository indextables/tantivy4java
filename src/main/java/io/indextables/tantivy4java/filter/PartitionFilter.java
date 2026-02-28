package io.indextables.tantivy4java.filter;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;

/**
 * A partition predicate filter that can be passed across JNI as JSON.
 *
 * <p>Supports equality, set membership, comparisons (with optional numeric type hints),
 * null checks, and boolean combinators (AND, OR, NOT).
 *
 * <p>This class is {@link Serializable} so it can be broadcast by Spark.
 * The serialized form is the JSON string (not the ObjectMapper).
 *
 * <p>Usage:
 * <pre>{@code
 * PartitionFilter filter = PartitionFilter.and(
 *     PartitionFilter.eq("year", "2024"),
 *     PartitionFilter.in("month", "01", "02", "03")
 * );
 * String json = filter.toJson(); // pass to native via JNI
 * }</pre>
 */
public class PartitionFilter implements Serializable {
    private static final long serialVersionUID = 1L;
    private static final ObjectMapper MAPPER = new ObjectMapper();

    /** The JSON representation of this filter. This is what gets serialized by Spark. */
    private final String json;

    private PartitionFilter(String json) {
        this.json = json;
    }

    /** Returns the JSON representation suitable for passing to native methods. */
    public String toJson() {
        return json;
    }

    @Override
    public String toString() {
        return "PartitionFilter(" + json + ")";
    }

    // -- Equality / set membership --

    /** Match rows where {@code column == value}. */
    public static PartitionFilter eq(String column, String value) {
        requireNonNull(column, "column");
        requireNonNull(value, "value");
        ObjectNode node = MAPPER.createObjectNode();
        node.put("op", "eq");
        node.put("column", column);
        node.put("value", value);
        return new PartitionFilter(node.toString());
    }

    /** Match rows where {@code column != value}. */
    public static PartitionFilter neq(String column, String value) {
        requireNonNull(column, "column");
        requireNonNull(value, "value");
        ObjectNode node = MAPPER.createObjectNode();
        node.put("op", "neq");
        node.put("column", column);
        node.put("value", value);
        return new PartitionFilter(node.toString());
    }

    /** Match rows where {@code column} is one of {@code values}. */
    public static PartitionFilter in(String column, String... values) {
        requireNonNull(column, "column");
        if (values == null || values.length == 0) {
            throw new IllegalArgumentException("values must not be null or empty");
        }
        ObjectNode node = MAPPER.createObjectNode();
        node.put("op", "in");
        node.put("column", column);
        ArrayNode arr = node.putArray("values");
        for (String v : values) {
            arr.add(v);
        }
        return new PartitionFilter(node.toString());
    }

    /** Match rows where {@code column} is one of {@code values}. */
    public static PartitionFilter in(String column, List<String> values) {
        return in(column, values.toArray(new String[0]));
    }

    // -- Comparisons (string by default) --

    /** Match rows where {@code column > value} (string comparison by default). */
    public static PartitionFilter gt(String column, String value) {
        requireNonNull(column, "column");
        requireNonNull(value, "value");
        return comparison("gt", column, value, null);
    }

    /** Match rows where {@code column >= value} (string comparison by default). */
    public static PartitionFilter gte(String column, String value) {
        requireNonNull(column, "column");
        requireNonNull(value, "value");
        return comparison("gte", column, value, null);
    }

    /** Match rows where {@code column < value} (string comparison by default). */
    public static PartitionFilter lt(String column, String value) {
        requireNonNull(column, "column");
        requireNonNull(value, "value");
        return comparison("lt", column, value, null);
    }

    /** Match rows where {@code column <= value} (string comparison by default). */
    public static PartitionFilter lte(String column, String value) {
        requireNonNull(column, "column");
        requireNonNull(value, "value");
        return comparison("lte", column, value, null);
    }

    // -- Null checks --

    /** Match rows where {@code column} is absent from partition values. */
    public static PartitionFilter isNull(String column) {
        requireNonNull(column, "column");
        ObjectNode node = MAPPER.createObjectNode();
        node.put("op", "is_null");
        node.put("column", column);
        return new PartitionFilter(node.toString());
    }

    /** Match rows where {@code column} is present in partition values. */
    public static PartitionFilter isNotNull(String column) {
        requireNonNull(column, "column");
        ObjectNode node = MAPPER.createObjectNode();
        node.put("op", "is_not_null");
        node.put("column", column);
        return new PartitionFilter(node.toString());
    }

    // -- Boolean combinators --

    /** Match rows where ALL filters match. Requires at least one filter. */
    public static PartitionFilter and(PartitionFilter... filters) {
        if (filters == null || filters.length == 0) {
            throw new IllegalArgumentException("and() requires at least one filter");
        }
        return combinator("and", "filters", Arrays.asList(filters));
    }

    /** Match rows where ALL filters match. Requires at least one filter. */
    public static PartitionFilter and(List<PartitionFilter> filters) {
        if (filters == null || filters.isEmpty()) {
            throw new IllegalArgumentException("and() requires at least one filter");
        }
        return combinator("and", "filters", filters);
    }

    /** Match rows where ANY filter matches. Requires at least one filter. */
    public static PartitionFilter or(PartitionFilter... filters) {
        if (filters == null || filters.length == 0) {
            throw new IllegalArgumentException("or() requires at least one filter");
        }
        return combinator("or", "filters", Arrays.asList(filters));
    }

    /** Match rows where ANY filter matches. Requires at least one filter. */
    public static PartitionFilter or(List<PartitionFilter> filters) {
        if (filters == null || filters.isEmpty()) {
            throw new IllegalArgumentException("or() requires at least one filter");
        }
        return combinator("or", "filters", filters);
    }

    /** Match rows where the given filter does NOT match. */
    public static PartitionFilter not(PartitionFilter filter) {
        if (filter == null) {
            throw new IllegalArgumentException("filter must not be null");
        }
        try {
            ObjectNode node = MAPPER.createObjectNode();
            node.put("op", "not");
            node.set("filter", MAPPER.readTree(filter.json));
            return new PartitionFilter(node.toString());
        } catch (Exception e) {
            throw new RuntimeException("Failed to build NOT filter", e);
        }
    }

    /**
     * Returns a copy of this filter with a type hint for numeric comparison.
     * Only meaningful for gt/gte/lt/lte filters. For other filter types,
     * returns a new filter with the type field added (ignored by Rust for non-comparison ops).
     *
     * @param type "long" or "double" for numeric comparison; "string" for lexicographic
     */
    public PartitionFilter withType(String type) {
        try {
            ObjectNode node = (ObjectNode) MAPPER.readTree(this.json);
            node.put("type", type);
            return new PartitionFilter(node.toString());
        } catch (Exception e) {
            throw new RuntimeException("Failed to set type on filter", e);
        }
    }

    // -- Internal helpers --

    private static void requireNonNull(Object value, String name) {
        if (value == null) {
            throw new IllegalArgumentException(name + " must not be null");
        }
    }

    private static PartitionFilter comparison(String op, String column, String value, String type) {
        ObjectNode node = MAPPER.createObjectNode();
        node.put("op", op);
        node.put("column", column);
        node.put("value", value);
        if (type != null) {
            node.put("type", type);
        }
        return new PartitionFilter(node.toString());
    }

    private static PartitionFilter combinator(String op, String arrayField, List<PartitionFilter> filters) {
        try {
            ObjectNode node = MAPPER.createObjectNode();
            node.put("op", op);
            ArrayNode arr = node.putArray(arrayField);
            for (PartitionFilter f : filters) {
                arr.add(MAPPER.readTree(f.json));
            }
            return new PartitionFilter(node.toString());
        } catch (Exception e) {
            throw new RuntimeException("Failed to build " + op + " filter", e);
        }
    }
}

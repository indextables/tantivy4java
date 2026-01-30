package io.indextables.tantivy4java.split;

/**
 * A wildcard query for split searching that matches documents using wildcard patterns.
 *
 * <p>Supports two wildcard characters:
 * <ul>
 *   <li>{@code *} - matches zero or more characters</li>
 *   <li>{@code ?} - matches exactly one character</li>
 * </ul>
 *
 * <p><b>Performance Considerations:</b>
 * <ul>
 *   <li><b>Prefix wildcards (e.g., "foo*")</b>: Efficient - uses FST prefix traversal</li>
 *   <li><b>Leading wildcards (e.g., "*foo")</b>: Expensive - requires full FST scan</li>
 *   <li><b>Multi-wildcards (e.g., "*foo*bar*")</b>: Very expensive - multiple regex expansions</li>
 * </ul>
 *
 * <p>The {@link SmartWildcardOptimizer} can optimize queries containing expensive wildcards
 * by first evaluating cheap filters (term queries, range queries) to short-circuit when possible.
 *
 * <p>Example usage:
 * <pre>{@code
 * // Efficient prefix wildcard
 * SplitWildcardQuery prefixQuery = new SplitWildcardQuery("title", "hello*");
 *
 * // Expensive leading wildcard (consider using with filters)
 * SplitWildcardQuery leadingQuery = new SplitWildcardQuery("content", "*world");
 *
 * // Very expensive multi-wildcard
 * SplitWildcardQuery multiQuery = new SplitWildcardQuery("description", "*foo*bar*");
 *
 * // Check if pattern is expensive before executing
 * if (leadingQuery.isExpensive()) {
 *     // Consider combining with cheap filters for optimization
 * }
 * }</pre>
 */
public class SplitWildcardQuery extends SplitQuery {
    private final String field;
    private final String pattern;
    private final boolean expensive;

    /**
     * Create a new wildcard query.
     *
     * @param field The field name to search in
     * @param pattern The wildcard pattern (* matches any characters, ? matches single character)
     * @throws IllegalArgumentException if field or pattern is null
     */
    public SplitWildcardQuery(String field, String pattern) {
        if (field == null) {
            throw new IllegalArgumentException("Field cannot be null");
        }
        if (pattern == null) {
            throw new IllegalArgumentException("Pattern cannot be null");
        }
        this.field = field;
        this.pattern = pattern;
        this.expensive = nativeIsExpensivePattern(pattern);
    }

    /**
     * Get the field name this query searches in.
     *
     * @return The field name
     */
    public String getField() {
        return field;
    }

    /**
     * Get the wildcard pattern this query uses.
     *
     * @return The wildcard pattern
     */
    public String getPattern() {
        return pattern;
    }

    /**
     * Check if this wildcard pattern is expensive to evaluate.
     *
     * <p>A pattern is considered expensive if it:
     * <ul>
     *   <li>Starts with a wildcard (leading wildcard, e.g., "*foo" or "?bar")</li>
     *   <li>Contains multiple wildcards (multi-wildcard, e.g., "*foo*bar*")</li>
     * </ul>
     *
     * <p>Expensive patterns require full FST scans or multiple regex evaluations,
     * which can be slow on large indices. Consider combining with cheap filters
     * using {@link SplitBooleanQuery} to enable short-circuit optimization.
     *
     * @return true if the pattern is expensive to evaluate
     */
    public boolean isExpensive() {
        return expensive;
    }

    /**
     * Check if the given pattern is expensive to evaluate.
     * This static method allows checking pattern cost without creating a query object.
     *
     * @param pattern The wildcard pattern to check
     * @return true if the pattern is expensive
     */
    public static boolean isExpensivePattern(String pattern) {
        if (pattern == null) {
            return false;
        }
        return nativeIsExpensivePattern(pattern);
    }

    /**
     * Native method to check if a wildcard pattern is expensive.
     * A pattern is expensive if it has a leading wildcard or multiple wildcards.
     */
    private static native boolean nativeIsExpensivePattern(String pattern);

    @Override
    public native String toQueryAstJson();

    @Override
    public String toString() {
        return String.format("SplitWildcardQuery(field=%s, pattern=%s, expensive=%s)",
                             field, pattern, expensive);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        SplitWildcardQuery that = (SplitWildcardQuery) obj;
        return field.equals(that.field) && pattern.equals(that.pattern);
    }

    @Override
    public int hashCode() {
        return field.hashCode() * 31 + pattern.hashCode();
    }
}

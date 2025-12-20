package io.indextables.tantivy4java.split;

/**
 * A SplitQuery for wildcard pattern matching.
 *
 * <p>Wildcard patterns support:
 * <ul>
 *   <li>{@code *} - matches zero or more characters</li>
 *   <li>{@code ?} - matches exactly one character</li>
 * </ul>
 *
 * <p>Examples:
 * <ul>
 *   <li>{@code hel*} - matches "hello", "help", "helicopter"</li>
 *   <li>{@code *world} - matches "world", "hello world"</li>
 *   <li>{@code h?llo} - matches "hello", "hallo"</li>
 * </ul>
 */
public class SplitWildcardQuery extends SplitQuery {

    private final String field;
    private final String pattern;
    private final boolean lenient;

    /**
     * Creates a wildcard query for the given field and pattern.
     *
     * @param field The field to search
     * @param pattern The wildcard pattern (supports * and ?)
     */
    public SplitWildcardQuery(String field, String pattern) {
        this(field, pattern, false);
    }

    /**
     * Creates a wildcard query for the given field and pattern.
     *
     * @param field The field to search
     * @param pattern The wildcard pattern (supports * and ?)
     * @param lenient If true, allows missing fields without error
     */
    public SplitWildcardQuery(String field, String pattern, boolean lenient) {
        this.field = field;
        this.pattern = pattern;
        this.lenient = lenient;
    }

    /**
     * Get the field name
     * @return The field name
     */
    public String getField() {
        return field;
    }

    /**
     * Get the wildcard pattern
     * @return The wildcard pattern
     */
    public String getPattern() {
        return pattern;
    }

    /**
     * Get whether this query is lenient
     * @return true if lenient mode is enabled
     */
    public boolean isLenient() {
        return lenient;
    }

    @Override
    public native String toQueryAstJson();

    @Override
    public String toString() {
        return "SplitWildcardQuery{field='" + field + "', pattern='" + pattern + "', lenient=" + lenient + "}";
    }
}

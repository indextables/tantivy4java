package io.indextables.tantivy4java.split;

/**
 * A SplitQuery for regular expression pattern matching.
 *
 * <p>Regex patterns follow standard regex syntax supported by Tantivy's FST automaton.
 * The prescan implementation uses FST automaton matching for efficient term dictionary
 * lookup without loading posting lists.
 *
 * <p>Examples:
 * <ul>
 *   <li>{@code hel.*} - matches "hello", "help", "helicopter"</li>
 *   <li>{@code .*world} - matches "world", "helloworld"</li>
 *   <li>{@code h.llo} - matches "hello", "hallo"</li>
 *   <li>{@code [a-z]+} - matches any lowercase word</li>
 * </ul>
 *
 * <p>Note: Invalid regex patterns will cause the prescan to return a conservative
 * result (couldHaveResults=true) to avoid false negatives.
 */
public class SplitRegexQuery extends SplitQuery {

    private final String field;
    private final String pattern;

    /**
     * Creates a regex query for the given field and pattern.
     *
     * @param field The field to search
     * @param pattern The regex pattern
     */
    public SplitRegexQuery(String field, String pattern) {
        if (field == null || pattern == null) {
            throw new IllegalArgumentException("Field and pattern cannot be null");
        }
        this.field = field;
        this.pattern = pattern;
    }

    /**
     * Get the field name
     * @return The field name
     */
    public String getField() {
        return field;
    }

    /**
     * Get the regex pattern
     * @return The regex pattern
     */
    public String getPattern() {
        return pattern;
    }

    @Override
    public native String toQueryAstJson();

    @Override
    public String toString() {
        return "SplitRegexQuery{field='" + field + "', pattern='" + pattern + "'}";
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        SplitRegexQuery that = (SplitRegexQuery) obj;
        return field.equals(that.field) && pattern.equals(that.pattern);
    }

    @Override
    public int hashCode() {
        return field.hashCode() * 31 + pattern.hashCode();
    }
}

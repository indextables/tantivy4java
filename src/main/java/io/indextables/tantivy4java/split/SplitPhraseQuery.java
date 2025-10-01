package io.indextables.tantivy4java.split;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * A phrase query for split searching that matches documents containing a sequence of terms.
 * Equivalent to Tantivy's PhraseQuery but designed for QueryAst conversion.
 *
 * A phrase query matches documents where the specified terms appear in the given order
 * within the specified field, with optional slop (distance) tolerance.
 */
public class SplitPhraseQuery extends SplitQuery {
    private final String field;
    private final List<String> terms;
    private final int slop;

    /**
     * Create a new phrase query with no slop (exact phrase matching).
     *
     * @param field The field name to search in
     * @param terms The sequence of terms that must appear in order
     */
    public SplitPhraseQuery(String field, List<String> terms) {
        this(field, terms, 0);
    }

    /**
     * Create a new phrase query with slop tolerance.
     *
     * @param field The field name to search in
     * @param terms The sequence of terms that must appear in order
     * @param slop Maximum distance allowed between terms (0 = exact phrase)
     */
    public SplitPhraseQuery(String field, List<String> terms, int slop) {
        if (field == null || field.trim().isEmpty()) {
            throw new IllegalArgumentException("Field cannot be null or empty");
        }
        if (terms == null || terms.isEmpty()) {
            throw new IllegalArgumentException("Terms cannot be null or empty");
        }
        if (slop < 0) {
            throw new IllegalArgumentException("Slop cannot be negative");
        }

        this.field = field.trim();
        this.terms = new ArrayList<>();
        for (String term : terms) {
            if (term == null || term.trim().isEmpty()) {
                throw new IllegalArgumentException("Individual terms cannot be null or empty");
            }
            this.terms.add(term.trim());
        }
        this.slop = slop;
    }

    /**
     * Create a new phrase query from an array of terms with no slop.
     *
     * @param field The field name to search in
     * @param terms The sequence of terms that must appear in order
     */
    public SplitPhraseQuery(String field, String... terms) {
        this(field, Arrays.asList(terms), 0);
    }

    /**
     * Create a new phrase query from an array of terms with slop tolerance.
     *
     * @param field The field name to search in
     * @param slop Maximum distance allowed between terms (0 = exact phrase)
     * @param terms The sequence of terms that must appear in order
     */
    public SplitPhraseQuery(String field, int slop, String... terms) {
        this(field, Arrays.asList(terms), slop);
    }

    /**
     * Get the field name this query searches in.
     */
    public String getField() {
        return field;
    }

    /**
     * Get the list of terms in the phrase.
     */
    public List<String> getTerms() {
        return new ArrayList<>(terms);
    }

    /**
     * Get the slop tolerance for this phrase query.
     */
    public int getSlop() {
        return slop;
    }

    /**
     * Check if this is an exact phrase query (slop = 0).
     */
    public boolean isExactPhrase() {
        return slop == 0;
    }

    /**
     * Get the number of terms in this phrase.
     */
    public int getTermCount() {
        return terms.size();
    }

    /**
     * Create an exact phrase query (slop = 0).
     */
    public static SplitPhraseQuery exactPhrase(String field, List<String> terms) {
        return new SplitPhraseQuery(field, terms, 0);
    }

    /**
     * Create an exact phrase query from string terms.
     */
    public static SplitPhraseQuery exactPhrase(String field, String... terms) {
        return new SplitPhraseQuery(field, Arrays.asList(terms), 0);
    }

    /**
     * Create a sloppy phrase query with the specified distance tolerance.
     */
    public static SplitPhraseQuery sloppyPhrase(String field, int slop, List<String> terms) {
        return new SplitPhraseQuery(field, terms, slop);
    }

    /**
     * Create a sloppy phrase query from string terms.
     */
    public static SplitPhraseQuery sloppyPhrase(String field, int slop, String... terms) {
        return new SplitPhraseQuery(field, Arrays.asList(terms), slop);
    }

    @Override
    public native String toQueryAstJson();

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("SplitPhraseQuery(");
        sb.append("field=").append(field);
        sb.append(", terms=[");
        for (int i = 0; i < terms.size(); i++) {
            if (i > 0) sb.append(", ");
            sb.append("\"").append(terms.get(i)).append("\"");
        }
        sb.append("]");
        if (slop > 0) {
            sb.append(", slop=").append(slop);
        }
        sb.append(")");
        return sb.toString();
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        SplitPhraseQuery that = (SplitPhraseQuery) obj;
        return slop == that.slop &&
               field.equals(that.field) &&
               terms.equals(that.terms);
    }

    @Override
    public int hashCode() {
        int result = field.hashCode();
        result = 31 * result + terms.hashCode();
        result = 31 * result + slop;
        return result;
    }
}
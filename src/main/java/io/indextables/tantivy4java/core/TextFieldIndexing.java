package io.indextables.tantivy4java.core;

/**
 * Configuration for text field indexing options.
 *
 * <p>Controls how text values are tokenized and indexed, including:
 * <ul>
 *   <li>Tokenizer selection (e.g., "default", "raw", "en_stem")</li>
 *   <li>Index option (basic, with frequencies, with positions)</li>
 * </ul>
 *
 * <p>Example:
 * <pre>
 * TextFieldIndexing indexing = TextFieldIndexing.create()
 *     .withTokenizer("default")
 *     .withPositions();
 * </pre>
 */
public class TextFieldIndexing {
    private String tokenizerName;
    private IndexOption indexOption;

    /**
     * Index options for text fields.
     */
    public enum IndexOption {
        /** Basic indexing without term frequencies */
        BASIC(0),
        /** Index with term frequencies */
        WITH_FREQS(1),
        /** Index with term frequencies and positions (for phrase queries) */
        WITH_FREQS_AND_POSITIONS(2);

        private final int code;

        IndexOption(int code) {
            this.code = code;
        }

        public int getCode() {
            return code;
        }
    }

    private TextFieldIndexing() {
        this.tokenizerName = "default";
        this.indexOption = IndexOption.WITH_FREQS_AND_POSITIONS;
    }

    /**
     * Create a new TextFieldIndexing configuration with default settings.
     *
     * @return New TextFieldIndexing with default tokenizer and positions
     */
    public static TextFieldIndexing create() {
        return new TextFieldIndexing();
    }

    /**
     * Set the tokenizer to use for this field.
     *
     * <p>Common tokenizers:
     * <ul>
     *   <li><b>"default"</b> - Standard tokenization with lowercasing</li>
     *   <li><b>"raw"</b> - No tokenization, treats entire value as single token</li>
     *   <li><b>"en_stem"</b> - English stemming tokenizer</li>
     * </ul>
     *
     * @param tokenizerName Name of the tokenizer
     * @return this TextFieldIndexing for method chaining
     */
    public TextFieldIndexing withTokenizer(String tokenizerName) {
        this.tokenizerName = tokenizerName;
        return this;
    }

    /**
     * Set index option to basic (no term frequencies).
     *
     * @return this TextFieldIndexing for method chaining
     */
    public TextFieldIndexing withBasic() {
        this.indexOption = IndexOption.BASIC;
        return this;
    }

    /**
     * Set index option to include term frequencies.
     *
     * @return this TextFieldIndexing for method chaining
     */
    public TextFieldIndexing withFreqs() {
        this.indexOption = IndexOption.WITH_FREQS;
        return this;
    }

    /**
     * Set index option to include term frequencies and positions.
     *
     * <p>Required for phrase queries and proximity searches.
     *
     * @return this TextFieldIndexing for method chaining
     */
    public TextFieldIndexing withPositions() {
        this.indexOption = IndexOption.WITH_FREQS_AND_POSITIONS;
        return this;
    }

    /**
     * Get the tokenizer name.
     *
     * @return Tokenizer name
     */
    public String getTokenizerName() {
        return tokenizerName;
    }

    /**
     * Get the index option.
     *
     * @return IndexOption enum value
     */
    public IndexOption getIndexOption() {
        return indexOption;
    }

    /**
     * Get the index option code for native layer.
     *
     * @return Index option code (0=basic, 1=with_freqs, 2=with_freqs_and_positions)
     */
    public int getIndexOptionCode() {
        return indexOption.getCode();
    }

    /**
     * Create default indexing configuration.
     *
     * <p>Uses "default" tokenizer with positions enabled.
     *
     * @return TextFieldIndexing with default settings
     */
    public static TextFieldIndexing defaultIndexing() {
        return create()
            .withTokenizer("default")
            .withPositions();
    }

    /**
     * Create indexing configuration with positions enabled.
     *
     * <p>Uses "default" tokenizer with positions for phrase queries.
     *
     * @return TextFieldIndexing with positions
     */
    public static TextFieldIndexing withPositionsStatic() {
        return create()
            .withTokenizer("default")
            .withPositions();
    }

    /**
     * Create indexing configuration with only frequencies.
     *
     * @return TextFieldIndexing with frequencies but no positions
     */
    public static TextFieldIndexing withFrequencies() {
        return create()
            .withTokenizer("default")
            .withFreqs();
    }

    /**
     * Create basic indexing configuration.
     *
     * @return TextFieldIndexing with basic indexing only
     */
    public static TextFieldIndexing basic() {
        return create()
            .withTokenizer("default")
            .withBasic();
    }

    @Override
    public String toString() {
        return "TextFieldIndexing{" +
                "tokenizerName='" + tokenizerName + '\'' +
                ", indexOption=" + indexOption +
                '}';
    }
}

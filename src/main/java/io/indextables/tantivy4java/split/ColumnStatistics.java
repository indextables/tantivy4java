package io.indextables.tantivy4java.split;

/**
 * Column-level statistics computed during parquet companion split creation.
 * Used for split pruning: if a query's range doesn't overlap a split's statistics,
 * the split can be skipped entirely.
 */
public class ColumnStatistics {
    private final String fieldName;
    private final String fieldType;

    // Numeric statistics (for I64, U64 fields)
    private Long minLong;
    private Long maxLong;

    // Float statistics (for F64 fields)
    private Double minDouble;
    private Double maxDouble;

    // String statistics (for text/keyword fields, truncated)
    private String minString;
    private String maxString;

    // Timestamp statistics (for DateTime fields, in microseconds since epoch)
    private Long minTimestampMicros;
    private Long maxTimestampMicros;

    // Boolean statistics
    private Boolean minBool;
    private Boolean maxBool;

    // Null count
    private long nullCount;

    public ColumnStatistics(String fieldName, String fieldType) {
        this.fieldName = fieldName;
        this.fieldType = fieldType;
    }

    public String getFieldName() { return fieldName; }
    public String getFieldType() { return fieldType; }
    public Long getMinLong() { return minLong; }
    public Long getMaxLong() { return maxLong; }
    public Double getMinDouble() { return minDouble; }
    public Double getMaxDouble() { return maxDouble; }
    public String getMinString() { return minString; }
    public String getMaxString() { return maxString; }
    public Long getMinTimestampMicros() { return minTimestampMicros; }
    public Long getMaxTimestampMicros() { return maxTimestampMicros; }
    public Boolean getMinBool() { return minBool; }
    public Boolean getMaxBool() { return maxBool; }
    public long getNullCount() { return nullCount; }

    // Setters for JNI construction
    void setMinLong(long v) { this.minLong = v; }
    void setMaxLong(long v) { this.maxLong = v; }
    void setMinDouble(double v) { this.minDouble = v; }
    void setMaxDouble(double v) { this.maxDouble = v; }
    void setMinString(String v) { this.minString = v; }
    void setMaxString(String v) { this.maxString = v; }
    void setMinTimestampMicros(long v) { this.minTimestampMicros = v; }
    void setMaxTimestampMicros(long v) { this.maxTimestampMicros = v; }
    void setMinBool(boolean v) { this.minBool = v; }
    void setMaxBool(boolean v) { this.maxBool = v; }
    void setNullCount(long v) { this.nullCount = v; }

    /**
     * Check if a long range overlaps with this column's statistics.
     * Returns true if there could be matching values (conservative).
     */
    public boolean overlapsRange(long lower, long upper) {
        if (minLong == null || maxLong == null) return true; // Unknown, assume overlap
        return !(upper < minLong || lower > maxLong);
    }

    /**
     * Check if a timestamp range (in microseconds) overlaps with this column's statistics.
     */
    public boolean overlapsTimestampRange(long lowerMicros, long upperMicros) {
        if (minTimestampMicros == null || maxTimestampMicros == null) return true;
        return !(upperMicros < minTimestampMicros || lowerMicros > maxTimestampMicros);
    }

    /**
     * Check if a double range overlaps with this column's statistics.
     */
    public boolean overlapsDoubleRange(double lower, double upper) {
        if (minDouble == null || maxDouble == null) return true;
        return !(upper < minDouble || lower > maxDouble);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("ColumnStatistics{field='").append(fieldName).append("', type='").append(fieldType).append("'");
        if (minLong != null) sb.append(", min=").append(minLong).append(", max=").append(maxLong);
        if (minDouble != null) sb.append(", min=").append(minDouble).append(", max=").append(maxDouble);
        if (minString != null) sb.append(", min='").append(minString).append("', max='").append(maxString).append("'");
        if (minTimestampMicros != null) sb.append(", minTs=").append(minTimestampMicros).append(", maxTs=").append(maxTimestampMicros);
        sb.append(", nullCount=").append(nullCount).append("}");
        return sb.toString();
    }
}

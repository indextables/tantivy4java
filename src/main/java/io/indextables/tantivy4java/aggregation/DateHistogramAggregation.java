package io.indextables.tantivy4java.aggregation;

import io.indextables.tantivy4java.split.SplitAggregation;

/**
 * Date histogram aggregation that groups documents by time intervals.
 * This creates buckets for time periods in a date field.
 * Similar to Elasticsearch's date_histogram aggregation and Quickwit's date_histogram aggregation.
 *
 * Example usage:
 * <pre>{@code
 * DateHistogramAggregation dailyStats = new DateHistogramAggregation("daily", "timestamp")
 *     .setFixedInterval("1d")
 *     .setOffset("-4h");
 *
 * SearchResult result = searcher.search(query, 10, Map.of("daily", dailyStats));
 * DateHistogramResult daily = (DateHistogramResult) result.getAggregation("daily");
 * }</pre>
 */
public class DateHistogramAggregation extends SplitAggregation {

    private final String fieldName;
    private String fixedInterval;
    private String calendarInterval;
    private String offset;
    private String timeZone;
    private String format;
    private ExtendedBounds extendedBounds;

    /**
     * Creates a date histogram aggregation for the specified field.
     *
     * @param fieldName The date field to create histogram for
     */
    public DateHistogramAggregation(String fieldName) {
        super("date_histogram_" + fieldName);
        if (fieldName == null || fieldName.trim().isEmpty()) {
            throw new IllegalArgumentException("Field name cannot be null or empty");
        }
        this.fieldName = fieldName.trim();
    }

    /**
     * Creates a named date histogram aggregation for the specified field.
     *
     * @param name The name for this aggregation
     * @param fieldName The date field to create histogram for
     */
    public DateHistogramAggregation(String name, String fieldName) {
        super(name);
        if (fieldName == null || fieldName.trim().isEmpty()) {
            throw new IllegalArgumentException("Field name cannot be null or empty");
        }
        this.fieldName = fieldName.trim();
    }

    /**
     * Sets the fixed interval for the histogram (e.g., "30s", "1m", "1h", "1d").
     */
    public DateHistogramAggregation setFixedInterval(String interval) {
        this.fixedInterval = interval;
        this.calendarInterval = null; // Clear calendar interval
        return this;
    }

    /**
     * Sets the calendar interval for the histogram (e.g., "minute", "hour", "day", "week", "month", "year").
     */
    public DateHistogramAggregation setCalendarInterval(String interval) {
        this.calendarInterval = interval;
        this.fixedInterval = null; // Clear fixed interval
        return this;
    }

    /**
     * Sets the offset to shift the bucket boundaries (e.g., "-4h").
     */
    public DateHistogramAggregation setOffset(String offset) {
        this.offset = offset;
        return this;
    }

    /**
     * Sets the time zone for the histogram (e.g., "UTC", "America/New_York").
     */
    public DateHistogramAggregation setTimeZone(String timeZone) {
        this.timeZone = timeZone;
        return this;
    }

    /**
     * Sets the format for date keys (e.g., "yyyy-MM-dd").
     */
    public DateHistogramAggregation setFormat(String format) {
        this.format = format;
        return this;
    }

    /**
     * Sets extended bounds to include empty buckets.
     */
    public DateHistogramAggregation setExtendedBounds(long min, long max) {
        this.extendedBounds = new ExtendedBounds(min, max);
        return this;
    }

    @Override
    public String getFieldName() {
        return fieldName;
    }

    @Override
    public String getAggregationType() {
        return "date_histogram";
    }

    @Override
    public String toAggregationJson() {
        if (fixedInterval == null && calendarInterval == null) {
            throw new IllegalStateException("Date histogram must have either fixed_interval or calendar_interval");
        }

        StringBuilder json = new StringBuilder();
        json.append("{\"date_histogram\": {\"field\": \"").append(fieldName).append("\"");

        if (fixedInterval != null) {
            json.append(", \"fixed_interval\": \"").append(fixedInterval).append("\"");
        }
        if (calendarInterval != null) {
            json.append(", \"calendar_interval\": \"").append(calendarInterval).append("\"");
        }
        if (offset != null) {
            json.append(", \"offset\": \"").append(offset).append("\"");
        }
        if (timeZone != null) {
            json.append(", \"time_zone\": \"").append(timeZone).append("\"");
        }
        if (format != null) {
            json.append(", \"format\": \"").append(format).append("\"");
        }
        if (extendedBounds != null) {
            json.append(", \"extended_bounds\": {");
            json.append("\"min\": ").append(extendedBounds.min);
            json.append(", \"max\": ").append(extendedBounds.max);
            json.append("}");
        }

        json.append("}}");
        return json.toString();
    }

    public String getFixedInterval() {
        return fixedInterval;
    }

    public String getCalendarInterval() {
        return calendarInterval;
    }

    public String getOffset() {
        return offset;
    }

    public String getTimeZone() {
        return timeZone;
    }

    public String getFormat() {
        return format;
    }

    public ExtendedBounds getExtendedBounds() {
        return extendedBounds;
    }

    @Override
    public String toString() {
        String interval = fixedInterval != null ? fixedInterval : calendarInterval;
        return String.format("DateHistogramAggregation{name='%s', field='%s', interval='%s'}",
                           name, fieldName, interval);
    }

    /**
     * Extended bounds for including empty buckets.
     */
    public static class ExtendedBounds {
        public final long min;
        public final long max;

        public ExtendedBounds(long min, long max) {
            this.min = min;
            this.max = max;
        }

        @Override
        public String toString() {
            return String.format("ExtendedBounds{min=%d, max=%d}", min, max);
        }
    }
}
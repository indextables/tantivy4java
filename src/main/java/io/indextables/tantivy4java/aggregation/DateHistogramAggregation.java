package io.indextables.tantivy4java.aggregation;

import io.indextables.tantivy4java.split.SplitAggregation;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Date histogram aggregation that groups documents by time intervals.
 * This creates buckets for time periods in a date field.
 * Similar to Elasticsearch's date_histogram aggregation and Quickwit's date_histogram aggregation.
 *
 * <p>Supports arbitrary bucket sizes via the fixed_interval parameter. Supported time units:</p>
 * <ul>
 *   <li><b>ms</b> - milliseconds (e.g., "100ms", "500ms")</li>
 *   <li><b>s</b> - seconds (e.g., "15s", "30s")</li>
 *   <li><b>m</b> - minutes (e.g., "5m", "15m")</li>
 *   <li><b>h</b> - hours (e.g., "1h", "6h")</li>
 *   <li><b>d</b> - days (e.g., "1d", "7d")</li>
 * </ul>
 *
 * <p>Example usage:</p>
 * <pre>{@code
 * // Daily buckets with 4-hour offset
 * DateHistogramAggregation dailyStats = new DateHistogramAggregation("daily", "timestamp")
 *     .setFixedInterval("1d")
 *     .setOffset("-4h");
 *
 * // Hourly buckets with stats sub-aggregation
 * DateHistogramAggregation hourlyWithStats = new DateHistogramAggregation("hourly", "timestamp")
 *     .setFixedInterval("1h")
 *     .addSubAggregation(new StatsAggregation("price_stats", "price"));
 *
 * // 15-minute buckets for high-frequency data
 * DateHistogramAggregation highFreq = new DateHistogramAggregation("ticks", "trade_time")
 *     .setFixedInterval("15m")
 *     .setMinDocCount(1);
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
    private HardBounds hardBounds;
    private Long minDocCount;
    private boolean keyed = false;
    private final Map<String, SplitAggregation> subAggregations = new LinkedHashMap<>();

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
     * Sets the fixed interval for the histogram.
     * Supported time units: ms (milliseconds), s (seconds), m (minutes), h (hours), d (days).
     * Examples: "100ms", "30s", "5m", "1h", "1d", "7d"
     *
     * @param interval The interval string (e.g., "1d", "6h", "30m")
     * @return this aggregation for method chaining
     */
    public DateHistogramAggregation setFixedInterval(String interval) {
        this.fixedInterval = interval;
        this.calendarInterval = null; // Clear calendar interval
        return this;
    }

    /**
     * Sets the calendar interval for the histogram.
     * Note: Calendar intervals (month, quarter, year) are NOT supported by Tantivy.
     * Use fixed_interval instead.
     *
     * @param interval The calendar interval (not recommended - use setFixedInterval instead)
     * @return this aggregation for method chaining
     */
    public DateHistogramAggregation setCalendarInterval(String interval) {
        this.calendarInterval = interval;
        this.fixedInterval = null; // Clear fixed interval
        return this;
    }

    /**
     * Sets the offset to shift the bucket boundaries.
     * Use the same time unit format as fixed_interval, with optional +/- prefix.
     * Examples: "-4h" (shift back 4 hours), "+30m" (shift forward 30 minutes)
     *
     * @param offset The offset string (e.g., "-4h", "+1d")
     * @return this aggregation for method chaining
     */
    public DateHistogramAggregation setOffset(String offset) {
        this.offset = offset;
        return this;
    }

    /**
     * Sets the time zone for the histogram (e.g., "UTC", "America/New_York").
     * Note: Time zone support may be limited in the current implementation.
     */
    public DateHistogramAggregation setTimeZone(String timeZone) {
        this.timeZone = timeZone;
        return this;
    }

    /**
     * Sets the format for date keys (e.g., "yyyy-MM-dd").
     * Note: Format parameter is not currently supported by Tantivy.
     */
    public DateHistogramAggregation setFormat(String format) {
        this.format = format;
        return this;
    }

    /**
     * Sets extended bounds to include empty buckets within the specified range.
     * This ensures buckets are created even if no documents fall within them.
     * Note: Cannot be combined with min_doc_count > 0.
     *
     * @param min Minimum timestamp (milliseconds since epoch)
     * @param max Maximum timestamp (milliseconds since epoch)
     * @return this aggregation for method chaining
     */
    public DateHistogramAggregation setExtendedBounds(long min, long max) {
        this.extendedBounds = new ExtendedBounds(min, max);
        return this;
    }

    /**
     * Sets hard bounds to filter values outside the specified range.
     * Documents with timestamps outside these bounds are excluded from aggregation.
     *
     * @param min Minimum timestamp (milliseconds since epoch)
     * @param max Maximum timestamp (milliseconds since epoch)
     * @return this aggregation for method chaining
     */
    public DateHistogramAggregation setHardBounds(long min, long max) {
        this.hardBounds = new HardBounds(min, max);
        return this;
    }

    /**
     * Sets the minimum document count for buckets to be included in results.
     * Buckets with fewer documents will be excluded.
     * Note: Cannot be combined with extended_bounds.
     *
     * @param minDocCount Minimum document count (default: 0)
     * @return this aggregation for method chaining
     */
    public DateHistogramAggregation setMinDocCount(long minDocCount) {
        this.minDocCount = minDocCount;
        return this;
    }

    /**
     * Sets whether to return buckets as a keyed map instead of an array.
     *
     * @param keyed true for keyed output, false for array output (default)
     * @return this aggregation for method chaining
     */
    public DateHistogramAggregation setKeyed(boolean keyed) {
        this.keyed = keyed;
        return this;
    }

    /**
     * Adds a sub-aggregation to be computed within each histogram bucket.
     *
     * @param subAgg The sub-aggregation to add
     * @return this aggregation for method chaining
     */
    public DateHistogramAggregation addSubAggregation(SplitAggregation subAgg) {
        this.subAggregations.put(subAgg.getName(), subAgg);
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
        if (minDocCount != null) {
            json.append(", \"min_doc_count\": ").append(minDocCount);
        }
        if (keyed) {
            json.append(", \"keyed\": true");
        }
        if (extendedBounds != null) {
            json.append(", \"extended_bounds\": {");
            json.append("\"min\": ").append(extendedBounds.min);
            json.append(", \"max\": ").append(extendedBounds.max);
            json.append("}");
        }
        if (hardBounds != null) {
            json.append(", \"hard_bounds\": {");
            json.append("\"min\": ").append(hardBounds.min);
            json.append(", \"max\": ").append(hardBounds.max);
            json.append("}");
        }

        json.append("}");

        // Add sub-aggregations if any
        if (!subAggregations.isEmpty()) {
            json.append(", \"aggs\": {");
            boolean first = true;
            for (Map.Entry<String, SplitAggregation> entry : subAggregations.entrySet()) {
                if (!first) json.append(", ");
                json.append("\"").append(entry.getKey()).append("\": ");
                json.append(entry.getValue().toAggregationJson());
                first = false;
            }
            json.append("}");
        }

        json.append("}");
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

    public HardBounds getHardBounds() {
        return hardBounds;
    }

    public Long getMinDocCount() {
        return minDocCount;
    }

    public boolean isKeyed() {
        return keyed;
    }

    public Map<String, SplitAggregation> getSubAggregations() {
        return subAggregations;
    }

    @Override
    public String toString() {
        String interval = fixedInterval != null ? fixedInterval : calendarInterval;
        return String.format("DateHistogramAggregation{name='%s', field='%s', interval='%s', subAggs=%d}",
                           name, fieldName, interval, subAggregations.size());
    }

    /**
     * Extended bounds for including empty buckets within a range.
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

    /**
     * Hard bounds for filtering values outside the range.
     */
    public static class HardBounds {
        public final long min;
        public final long max;

        public HardBounds(long min, long max) {
            this.min = min;
            this.max = max;
        }

        @Override
        public String toString() {
            return String.format("HardBounds{min=%d, max=%d}", min, max);
        }
    }
}
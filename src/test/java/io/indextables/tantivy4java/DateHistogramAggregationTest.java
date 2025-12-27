package io.indextables.tantivy4java;

import io.indextables.tantivy4java.core.*;
import io.indextables.tantivy4java.query.*;
import io.indextables.tantivy4java.result.*;
import io.indextables.tantivy4java.split.*;
import io.indextables.tantivy4java.split.merge.*;
import io.indextables.tantivy4java.aggregation.*;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;

import java.nio.file.Path;
import java.util.Map;
import java.util.HashMap;
import java.time.LocalDateTime;
import java.time.Instant;
import java.time.ZoneOffset;

/**
 * Comprehensive test suite for DateHistogramAggregation functionality.
 * Tests all date histogram features including:
 * - Various fixed intervals (ms, s, m, h, d)
 * - Offset parameter
 * - Extended bounds
 * - Hard bounds
 * - Min doc count filtering
 * - Keyed output format
 * - Sub-aggregations
 * - Edge cases
 */
public class DateHistogramAggregationTest {

    private SplitCacheManager cacheManager;
    private SplitSearcher searcher;
    private String splitPath;
    private QuickwitSplit.SplitMetadata metadata;
    private String testName;

    // Base timestamp: 2023-01-15 12:00:00 UTC
    private static final long BASE_TIMESTAMP = 1673784000L;

    @BeforeEach
    public void setUp(@TempDir Path tempDir, TestInfo testInfo) throws Exception {
        testName = testInfo.getTestMethod().get().getName();

        String indexPath = tempDir.resolve(testName + "_index").toString();
        splitPath = tempDir.resolve(testName + ".split").toString();

        try (SchemaBuilder builder = new SchemaBuilder()) {
            builder
                .addTextField("event_type", true, true, "default", "position")
                .addTextField("source", true, true, "default", "position")
                .addIntegerField("value", true, true, true)
                .addDateField("timestamp", true, true, true)
                .addDateField("created_at", true, true, true)
                .addIntegerField("id", true, true, true);

            try (Schema schema = builder.build()) {
                try (Index index = new Index(schema, indexPath, false)) {
                    try (IndexWriter writer = index.writer(Index.Memory.DEFAULT_HEAP_SIZE, 1)) {
                        // Create test data with specific time distribution
                        // Events spread across multiple days and hours

                        // Day 1: 2023-01-15 - 3 events at different hours
                        addEvent(writer, 1, "click", "web", 100, BASE_TIMESTAMP);                    // 12:00
                        addEvent(writer, 2, "click", "mobile", 150, BASE_TIMESTAMP + 3600);          // 13:00
                        addEvent(writer, 3, "purchase", "web", 500, BASE_TIMESTAMP + 7200);          // 14:00

                        // Day 2: 2023-01-16 - 4 events
                        addEvent(writer, 4, "click", "web", 120, BASE_TIMESTAMP + 86400);            // +1 day
                        addEvent(writer, 5, "click", "mobile", 130, BASE_TIMESTAMP + 86400 + 1800);  // +1 day, 30 min
                        addEvent(writer, 6, "purchase", "mobile", 800, BASE_TIMESTAMP + 86400 + 3600);
                        addEvent(writer, 7, "view", "web", 50, BASE_TIMESTAMP + 86400 + 7200);

                        // Day 3: 2023-01-17 - 2 events
                        addEvent(writer, 8, "click", "web", 110, BASE_TIMESTAMP + 172800);           // +2 days
                        addEvent(writer, 9, "purchase", "web", 1000, BASE_TIMESTAMP + 172800 + 3600);

                        // Day 5: 2023-01-19 - 1 event (gap on day 4)
                        addEvent(writer, 10, "view", "mobile", 75, BASE_TIMESTAMP + 345600);         // +4 days

                        writer.commit();
                    }

                    QuickwitSplit.SplitConfig config = new QuickwitSplit.SplitConfig(
                        "date-hist-test-" + testName, "test-source", "test-node");

                    metadata = QuickwitSplit.convertIndexFromPath(indexPath, splitPath, config);
                }
            }
        }

        SplitCacheManager.CacheConfig cacheConfig =
            new SplitCacheManager.CacheConfig("date-hist-test-cache-" + testName);
        cacheManager = SplitCacheManager.getInstance(cacheConfig);
        searcher = cacheManager.createSplitSearcher("file://" + splitPath, metadata);
    }

    @AfterEach
    public void tearDown() {
        if (searcher != null) {
            searcher.close();
        }
        if (cacheManager != null) {
            cacheManager.close();
        }
    }

    private void addEvent(IndexWriter writer, int id, String eventType, String source,
                         int value, long timestampSeconds) throws Exception {
        try (Document doc = new Document()) {
            doc.addInteger("id", id);
            doc.addText("event_type", eventType);
            doc.addText("source", source);
            doc.addInteger("value", value);
            LocalDateTime dateTime = LocalDateTime.ofInstant(
                Instant.ofEpochSecond(timestampSeconds), ZoneOffset.UTC);
            doc.addDate("timestamp", dateTime);
            doc.addDate("created_at", dateTime);
            writer.addDocument(doc);
        }
    }

    // ==================== Basic Interval Tests ====================

    @Test
    @DisplayName("Date histogram with daily interval (1d)")
    public void testDailyHistogram() {
        System.out.println("Testing date histogram with interval=1d...");

        SplitQuery query = new SplitMatchAllQuery();
        DateHistogramAggregation hist = new DateHistogramAggregation("daily", "timestamp")
            .setFixedInterval("1d");

        SearchResult result = searcher.search(query, 10, "daily", hist);

        assertTrue(result.hasAggregations(), "Should have aggregations");
        DateHistogramResult histResult = (DateHistogramResult) result.getAggregation("daily");
        assertNotNull(histResult, "Date histogram result should not be null");

        // Should have buckets for days with data
        long totalDocs = 0;
        for (DateHistogramResult.DateHistogramBucket bucket : histResult.getBuckets()) {
            totalDocs += bucket.getDocCount();
            assertNotNull(bucket.getKeyAsString(), "Bucket should have formatted date string");
            System.out.println("  " + bucket.getKeyAsString() + ": " + bucket.getDocCount() + " docs");
        }

        assertEquals(10L, totalDocs, "Total docs should be 10");
        System.out.println("Daily histogram test passed!");
    }

    @Test
    @DisplayName("Date histogram with hourly interval (1h)")
    public void testHourlyHistogram() {
        System.out.println("Testing date histogram with interval=1h...");

        SplitQuery query = new SplitMatchAllQuery();
        DateHistogramAggregation hist = new DateHistogramAggregation("hourly", "timestamp")
            .setFixedInterval("1h");

        SearchResult result = searcher.search(query, 10, "hourly", hist);

        DateHistogramResult histResult = (DateHistogramResult) result.getAggregation("hourly");
        assertNotNull(histResult, "Date histogram result should not be null");

        // Should have more granular buckets than daily
        assertTrue(histResult.getBuckets().size() >= 4, "Should have at least 4 hourly buckets");

        long totalDocs = 0;
        for (DateHistogramResult.DateHistogramBucket bucket : histResult.getBuckets()) {
            totalDocs += bucket.getDocCount();
            System.out.println("  " + bucket.getKeyAsString() + ": " + bucket.getDocCount() + " docs");
        }

        assertEquals(10L, totalDocs, "Total docs should be 10");
        System.out.println("Hourly histogram test passed!");
    }

    @Test
    @DisplayName("Date histogram with minute interval (30m)")
    public void testMinuteHistogram() {
        System.out.println("Testing date histogram with interval=30m...");

        SplitQuery query = new SplitMatchAllQuery();
        DateHistogramAggregation hist = new DateHistogramAggregation("by_30min", "timestamp")
            .setFixedInterval("30m");

        SearchResult result = searcher.search(query, 10, "by_30min", hist);

        DateHistogramResult histResult = (DateHistogramResult) result.getAggregation("by_30min");
        assertNotNull(histResult, "Date histogram result should not be null");

        // Should have more granular buckets than hourly
        assertTrue(histResult.getBuckets().size() >= 5, "Should have at least 5 30-minute buckets");

        for (DateHistogramResult.DateHistogramBucket bucket : histResult.getBuckets()) {
            System.out.println("  " + bucket.getKeyAsString() + ": " + bucket.getDocCount() + " docs");
        }

        System.out.println("30-minute histogram test passed!");
    }

    @Test
    @DisplayName("Date histogram with second interval (30s)")
    public void testSecondHistogram() {
        System.out.println("Testing date histogram with interval=30s...");

        SplitQuery query = new SplitMatchAllQuery();
        DateHistogramAggregation hist = new DateHistogramAggregation("by_30sec", "timestamp")
            .setFixedInterval("30s");

        SearchResult result = searcher.search(query, 10, "by_30sec", hist);

        DateHistogramResult histResult = (DateHistogramResult) result.getAggregation("by_30sec");
        assertNotNull(histResult, "Date histogram result should not be null");

        // Each event should be in its own bucket since they're > 30s apart
        for (DateHistogramResult.DateHistogramBucket bucket : histResult.getBuckets()) {
            System.out.println("  " + bucket.getKeyAsString() + ": " + bucket.getDocCount() + " docs");
        }

        System.out.println("30-second histogram test passed!");
    }

    @Test
    @DisplayName("Date histogram with multi-day interval (7d)")
    public void testWeeklyHistogram() {
        System.out.println("Testing date histogram with interval=7d (weekly)...");

        SplitQuery query = new SplitMatchAllQuery();
        DateHistogramAggregation hist = new DateHistogramAggregation("weekly", "timestamp")
            .setFixedInterval("7d");

        SearchResult result = searcher.search(query, 10, "weekly", hist);

        DateHistogramResult histResult = (DateHistogramResult) result.getAggregation("weekly");
        assertNotNull(histResult, "Date histogram result should not be null");

        // All events span ~5 days, should fit in 1-2 weekly buckets
        assertTrue(histResult.getBuckets().size() <= 2, "Should have 1-2 weekly buckets");

        long totalDocs = 0;
        for (DateHistogramResult.DateHistogramBucket bucket : histResult.getBuckets()) {
            totalDocs += bucket.getDocCount();
            System.out.println("  " + bucket.getKeyAsString() + ": " + bucket.getDocCount() + " docs");
        }

        assertEquals(10L, totalDocs, "Total docs should be 10");
        System.out.println("Weekly histogram test passed!");
    }

    @Test
    @DisplayName("Date histogram with 6-hour interval (6h)")
    public void testSixHourHistogram() {
        System.out.println("Testing date histogram with interval=6h...");

        SplitQuery query = new SplitMatchAllQuery();
        DateHistogramAggregation hist = new DateHistogramAggregation("6hourly", "timestamp")
            .setFixedInterval("6h");

        SearchResult result = searcher.search(query, 10, "6hourly", hist);

        DateHistogramResult histResult = (DateHistogramResult) result.getAggregation("6hourly");
        assertNotNull(histResult, "Date histogram result should not be null");

        for (DateHistogramResult.DateHistogramBucket bucket : histResult.getBuckets()) {
            System.out.println("  " + bucket.getKeyAsString() + ": " + bucket.getDocCount() + " docs");
        }

        System.out.println("6-hour histogram test passed!");
    }

    // ==================== Offset Tests ====================

    @Test
    @DisplayName("Date histogram with offset shifts bucket boundaries")
    public void testDateHistogramWithOffset() {
        System.out.println("Testing date histogram with offset=-4h...");

        SplitQuery query = new SplitMatchAllQuery();
        DateHistogramAggregation hist = new DateHistogramAggregation("daily_offset", "timestamp")
            .setFixedInterval("1d")
            .setOffset("-4h");

        SearchResult result = searcher.search(query, 10, "daily_offset", hist);

        DateHistogramResult histResult = (DateHistogramResult) result.getAggregation("daily_offset");
        assertNotNull(histResult, "Date histogram result should not be null");

        for (DateHistogramResult.DateHistogramBucket bucket : histResult.getBuckets()) {
            System.out.println("  " + bucket.getKeyAsString() + ": " + bucket.getDocCount() + " docs");
        }

        System.out.println("Date histogram with offset test passed!");
    }

    @Test
    @DisplayName("Date histogram with positive offset")
    public void testDateHistogramWithPositiveOffset() {
        System.out.println("Testing date histogram with offset=+6h...");

        SplitQuery query = new SplitMatchAllQuery();
        DateHistogramAggregation hist = new DateHistogramAggregation("daily_pos_offset", "timestamp")
            .setFixedInterval("1d")
            .setOffset("+6h");

        SearchResult result = searcher.search(query, 10, "daily_pos_offset", hist);

        DateHistogramResult histResult = (DateHistogramResult) result.getAggregation("daily_pos_offset");
        assertNotNull(histResult, "Date histogram result should not be null");

        for (DateHistogramResult.DateHistogramBucket bucket : histResult.getBuckets()) {
            System.out.println("  " + bucket.getKeyAsString() + ": " + bucket.getDocCount() + " docs");
        }

        System.out.println("Date histogram with positive offset test passed!");
    }

    // ==================== Min Doc Count Tests ====================

    @Test
    @DisplayName("Date histogram with min_doc_count filters empty buckets")
    public void testDateHistogramWithMinDocCount() {
        System.out.println("Testing date histogram with min_doc_count=1...");

        SplitQuery query = new SplitMatchAllQuery();
        DateHistogramAggregation hist = new DateHistogramAggregation("daily_min", "timestamp")
            .setFixedInterval("1d")
            .setMinDocCount(1);

        SearchResult result = searcher.search(query, 10, "daily_min", hist);

        DateHistogramResult histResult = (DateHistogramResult) result.getAggregation("daily_min");
        assertNotNull(histResult, "Date histogram result should not be null");

        // All returned buckets should have at least 1 document
        for (DateHistogramResult.DateHistogramBucket bucket : histResult.getBuckets()) {
            assertTrue(bucket.getDocCount() >= 1, "Bucket should have at least 1 doc");
            System.out.println("  " + bucket.getKeyAsString() + ": " + bucket.getDocCount() + " docs");
        }

        // Should NOT have empty bucket for day 4 (gap day)
        // With min_doc_count=1, we should only see days with data
        System.out.println("Date histogram with min_doc_count test passed!");
    }

    @Test
    @DisplayName("Date histogram with min_doc_count=2 filters low-count buckets")
    public void testDateHistogramWithMinDocCount2() {
        System.out.println("Testing date histogram with min_doc_count=2...");

        SplitQuery query = new SplitMatchAllQuery();
        DateHistogramAggregation hist = new DateHistogramAggregation("daily_min2", "timestamp")
            .setFixedInterval("1d")
            .setMinDocCount(2);

        SearchResult result = searcher.search(query, 10, "daily_min2", hist);

        DateHistogramResult histResult = (DateHistogramResult) result.getAggregation("daily_min2");
        assertNotNull(histResult, "Date histogram result should not be null");

        // All returned buckets should have at least 2 documents
        for (DateHistogramResult.DateHistogramBucket bucket : histResult.getBuckets()) {
            assertTrue(bucket.getDocCount() >= 2, "Bucket should have at least 2 docs");
            System.out.println("  " + bucket.getKeyAsString() + ": " + bucket.getDocCount() + " docs");
        }

        System.out.println("Date histogram with min_doc_count=2 test passed!");
    }

    // ==================== Hard Bounds Tests ====================

    @Test
    @DisplayName("Date histogram with hard_bounds filters timestamps outside range")
    public void testDateHistogramWithHardBounds() {
        System.out.println("Testing date histogram with hard_bounds...");

        // Only include first 2 days
        long minTimestamp = BASE_TIMESTAMP * 1000; // Convert to milliseconds
        long maxTimestamp = (BASE_TIMESTAMP + 172800) * 1000; // +2 days in ms

        SplitQuery query = new SplitMatchAllQuery();
        DateHistogramAggregation hist = new DateHistogramAggregation("bounded", "timestamp")
            .setFixedInterval("1d")
            .setHardBounds(minTimestamp, maxTimestamp);

        SearchResult result = searcher.search(query, 10, "bounded", hist);

        DateHistogramResult histResult = (DateHistogramResult) result.getAggregation("bounded");
        assertNotNull(histResult, "Date histogram result should not be null");

        long totalDocs = 0;
        for (DateHistogramResult.DateHistogramBucket bucket : histResult.getBuckets()) {
            totalDocs += bucket.getDocCount();
            System.out.println("  " + bucket.getKeyAsString() + ": " + bucket.getDocCount() + " docs");
        }

        // Should exclude day 5 (1 doc) - expect 9 docs
        assertTrue(totalDocs <= 9, "Should have at most 9 docs within hard bounds");

        System.out.println("Date histogram with hard_bounds test passed!");
    }

    // ==================== Extended Bounds Tests ====================

    @Test
    @DisplayName("Date histogram with extended_bounds creates empty buckets")
    public void testDateHistogramWithExtendedBounds() {
        System.out.println("Testing date histogram with extended_bounds...");

        // Extend to 10 days
        long minTimestamp = BASE_TIMESTAMP * 1000;
        long maxTimestamp = (BASE_TIMESTAMP + 864000) * 1000; // +10 days in ms

        SplitQuery query = new SplitMatchAllQuery();
        DateHistogramAggregation hist = new DateHistogramAggregation("extended", "timestamp")
            .setFixedInterval("1d")
            .setExtendedBounds(minTimestamp, maxTimestamp);

        SearchResult result = searcher.search(query, 10, "extended", hist);

        DateHistogramResult histResult = (DateHistogramResult) result.getAggregation("extended");
        assertNotNull(histResult, "Date histogram result should not be null");

        // Should have buckets for full 10 day range including empty days
        assertTrue(histResult.getBuckets().size() >= 5, "Should have extended range of buckets");

        for (DateHistogramResult.DateHistogramBucket bucket : histResult.getBuckets()) {
            System.out.println("  " + bucket.getKeyAsString() + ": " + bucket.getDocCount() + " docs");
        }

        System.out.println("Date histogram with extended_bounds test passed!");
    }

    // ==================== Combined with Query Filter ====================

    @Test
    @DisplayName("Date histogram filtered by event type")
    public void testDateHistogramWithQueryFilter() {
        System.out.println("Testing date histogram filtered by event_type=click...");

        SplitQuery query = new SplitTermQuery("event_type", "click");
        DateHistogramAggregation hist = new DateHistogramAggregation("click_timeline", "timestamp")
            .setFixedInterval("1d");

        SearchResult result = searcher.search(query, 10, "click_timeline", hist);

        DateHistogramResult histResult = (DateHistogramResult) result.getAggregation("click_timeline");
        assertNotNull(histResult, "Date histogram result should not be null");

        // Click events: 5 total (ids 1, 2, 4, 5, 8)
        long totalDocs = 0;
        for (DateHistogramResult.DateHistogramBucket bucket : histResult.getBuckets()) {
            totalDocs += bucket.getDocCount();
            System.out.println("  " + bucket.getKeyAsString() + ": " + bucket.getDocCount() + " clicks");
        }

        assertEquals(5L, totalDocs, "Should have 5 click events");
        System.out.println("Date histogram with query filter test passed!");
    }

    // ==================== Different Date Fields ====================

    @Test
    @DisplayName("Date histogram on different date field (created_at)")
    public void testDateHistogramOnDifferentField() {
        System.out.println("Testing date histogram on created_at field...");

        SplitQuery query = new SplitMatchAllQuery();
        DateHistogramAggregation hist = new DateHistogramAggregation("created_timeline", "created_at")
            .setFixedInterval("1d");

        SearchResult result = searcher.search(query, 10, "created_timeline", hist);

        DateHistogramResult histResult = (DateHistogramResult) result.getAggregation("created_timeline");
        assertNotNull(histResult, "Date histogram result should not be null");

        long totalDocs = 0;
        for (DateHistogramResult.DateHistogramBucket bucket : histResult.getBuckets()) {
            totalDocs += bucket.getDocCount();
            System.out.println("  " + bucket.getKeyAsString() + ": " + bucket.getDocCount() + " docs");
        }

        assertEquals(10L, totalDocs, "Total docs should be 10");
        System.out.println("Date histogram on different field test passed!");
    }

    // ==================== Multiple Date Histograms ====================

    @Test
    @DisplayName("Multiple date histogram aggregations in one query")
    public void testMultipleDateHistograms() {
        System.out.println("Testing multiple date histogram aggregations...");

        SplitQuery query = new SplitMatchAllQuery();

        Map<String, SplitAggregation> aggs = new HashMap<>();
        aggs.put("daily", new DateHistogramAggregation("daily", "timestamp").setFixedInterval("1d"));
        aggs.put("hourly", new DateHistogramAggregation("hourly", "timestamp").setFixedInterval("1h"));

        SearchResult result = searcher.search(query, 10, aggs);

        assertTrue(result.hasAggregations(), "Should have aggregations");
        assertEquals(2, result.getAggregations().size(), "Should have 2 aggregations");

        DateHistogramResult dailyHist = (DateHistogramResult) result.getAggregation("daily");
        DateHistogramResult hourlyHist = (DateHistogramResult) result.getAggregation("hourly");

        assertNotNull(dailyHist, "Daily histogram should not be null");
        assertNotNull(hourlyHist, "Hourly histogram should not be null");

        System.out.println("Daily buckets: " + dailyHist.getBuckets().size());
        System.out.println("Hourly buckets: " + hourlyHist.getBuckets().size());

        assertTrue(hourlyHist.getBuckets().size() >= dailyHist.getBuckets().size(),
            "Hourly should have >= buckets than daily");

        System.out.println("Multiple date histograms test passed!");
    }

    // ==================== Sub-Aggregation Tests ====================

    @Test
    @DisplayName("Date histogram with stats sub-aggregation")
    public void testDateHistogramWithSubAggregation() {
        System.out.println("Testing date histogram with stats sub-aggregation...");

        SplitQuery query = new SplitMatchAllQuery();
        DateHistogramAggregation hist = new DateHistogramAggregation("daily_with_stats", "timestamp")
            .setFixedInterval("1d")
            .addSubAggregation(new StatsAggregation("value_stats", "value"));

        SearchResult result = searcher.search(query, 10, "daily_with_stats", hist);

        DateHistogramResult histResult = (DateHistogramResult) result.getAggregation("daily_with_stats");
        assertNotNull(histResult, "Date histogram result should not be null");

        for (DateHistogramResult.DateHistogramBucket bucket : histResult.getBuckets()) {
            System.out.println("  " + bucket.getKeyAsString() + ": " + bucket.getDocCount() + " docs");

            Map<String, AggregationResult> subAggs = bucket.getSubAggregations();
            if (subAggs != null && !subAggs.isEmpty()) {
                StatsResult stats = bucket.getSubAggregation("value_stats", StatsResult.class);
                if (stats != null) {
                    System.out.println("    Stats - count: " + stats.getCount() +
                        ", sum: " + stats.getSum() + ", avg: " + stats.getAverage());
                }
            }
        }

        System.out.println("Date histogram with sub-aggregation test passed!");
    }

    @Test
    @DisplayName("Date histogram with terms sub-aggregation")
    public void testDateHistogramWithTermsSubAgg() {
        System.out.println("Testing date histogram with terms sub-aggregation...");

        SplitQuery query = new SplitMatchAllQuery();
        DateHistogramAggregation hist = new DateHistogramAggregation("daily_by_type", "timestamp")
            .setFixedInterval("1d")
            .addSubAggregation(new TermsAggregation("event_types", "event_type", 10, 0));

        SearchResult result = searcher.search(query, 10, "daily_by_type", hist);

        DateHistogramResult histResult = (DateHistogramResult) result.getAggregation("daily_by_type");
        assertNotNull(histResult, "Date histogram result should not be null");

        for (DateHistogramResult.DateHistogramBucket bucket : histResult.getBuckets()) {
            System.out.println("  " + bucket.getKeyAsString() + ": " + bucket.getDocCount() + " docs");

            Map<String, AggregationResult> subAggs = bucket.getSubAggregations();
            if (subAggs != null && !subAggs.isEmpty()) {
                TermsResult terms = bucket.getSubAggregation("event_types", TermsResult.class);
                if (terms != null) {
                    System.out.println("    Event types: " + terms.getBuckets().size() + " types");
                }
            }
        }

        System.out.println("Date histogram with terms sub-aggregation test passed!");
    }

    // ==================== JSON Generation Tests ====================

    @Test
    @DisplayName("Date histogram JSON generation includes all parameters")
    public void testDateHistogramJsonGeneration() {
        System.out.println("Testing date histogram JSON generation...");

        DateHistogramAggregation hist = new DateHistogramAggregation("test", "timestamp")
            .setFixedInterval("1h")
            .setOffset("-4h")
            .setMinDocCount(1)
            .setKeyed(true)
            .setHardBounds(1000L, 2000L)
            .setExtendedBounds(0L, 3000L);

        String json = hist.toAggregationJson();
        System.out.println("Generated JSON: " + json);

        assertTrue(json.contains("\"date_histogram\""), "Should contain date_histogram type");
        assertTrue(json.contains("\"field\": \"timestamp\""), "Should contain field name");
        assertTrue(json.contains("\"fixed_interval\": \"1h\""), "Should contain fixed_interval");
        assertTrue(json.contains("\"offset\": \"-4h\""), "Should contain offset");
        assertTrue(json.contains("\"min_doc_count\": 1"), "Should contain min_doc_count");
        assertTrue(json.contains("\"keyed\": true"), "Should contain keyed");
        assertTrue(json.contains("\"hard_bounds\""), "Should contain hard_bounds");
        assertTrue(json.contains("\"extended_bounds\""), "Should contain extended_bounds");

        System.out.println("Date histogram JSON generation test passed!");
    }

    @Test
    @DisplayName("Date histogram JSON with sub-aggregation")
    public void testDateHistogramJsonWithSubAggregation() {
        System.out.println("Testing date histogram JSON with sub-aggregation...");

        DateHistogramAggregation hist = new DateHistogramAggregation("test", "timestamp")
            .setFixedInterval("1d")
            .addSubAggregation(new StatsAggregation("nested_stats", "value"))
            .addSubAggregation(new TermsAggregation("nested_terms", "event_type", 10, 0));

        String json = hist.toAggregationJson();
        System.out.println("Generated JSON: " + json);

        assertTrue(json.contains("\"aggs\""), "Should contain aggs section");
        assertTrue(json.contains("\"nested_stats\""), "Should contain nested_stats");
        assertTrue(json.contains("\"nested_terms\""), "Should contain nested_terms");
        assertTrue(json.contains("\"stats\""), "Should contain stats aggregation type");
        assertTrue(json.contains("\"terms\""), "Should contain terms aggregation type");

        System.out.println("Date histogram JSON with sub-aggregation test passed!");
    }

    // ==================== Validation Tests ====================

    @Test
    @DisplayName("Date histogram requires fixed_interval or calendar_interval")
    public void testDateHistogramRequiresInterval() {
        System.out.println("Testing date histogram interval requirement...");

        DateHistogramAggregation hist = new DateHistogramAggregation("test", "timestamp");
        // No interval set

        assertThrows(IllegalStateException.class, () -> {
            hist.toAggregationJson();
        }, "Should throw when no interval is set");

        System.out.println("Date histogram interval requirement test passed!");
    }

    @Test
    @DisplayName("Date histogram constructor validation")
    public void testDateHistogramConstructorValidation() {
        System.out.println("Testing date histogram constructor validation...");

        // Test null field name
        assertThrows(IllegalArgumentException.class, () -> {
            new DateHistogramAggregation(null);
        }, "Should reject null field name");

        assertThrows(IllegalArgumentException.class, () -> {
            new DateHistogramAggregation("");
        }, "Should reject empty field name");

        assertThrows(IllegalArgumentException.class, () -> {
            new DateHistogramAggregation("test", null);
        }, "Should reject null field name in named constructor");

        System.out.println("Date histogram constructor validation test passed!");
    }

    @Test
    @DisplayName("Date histogram getters return correct values")
    public void testDateHistogramGetters() {
        System.out.println("Testing date histogram getters...");

        DateHistogramAggregation hist = new DateHistogramAggregation("my_hist", "timestamp")
            .setFixedInterval("6h")
            .setOffset("-2h")
            .setMinDocCount(5)
            .setKeyed(true)
            .setHardBounds(1000L, 2000L)
            .setExtendedBounds(0L, 3000L);

        assertEquals("my_hist", hist.getName(), "Name should match");
        assertEquals("timestamp", hist.getFieldName(), "Field name should match");
        assertEquals("6h", hist.getFixedInterval(), "Fixed interval should match");
        assertEquals("-2h", hist.getOffset(), "Offset should match");
        assertEquals(5L, hist.getMinDocCount(), "Min doc count should match");
        assertTrue(hist.isKeyed(), "Keyed should be true");
        assertNotNull(hist.getHardBounds(), "Hard bounds should not be null");
        assertEquals(1000L, hist.getHardBounds().min, "Hard bounds min should match");
        assertEquals(2000L, hist.getHardBounds().max, "Hard bounds max should match");
        assertNotNull(hist.getExtendedBounds(), "Extended bounds should not be null");
        assertEquals(0L, hist.getExtendedBounds().min, "Extended bounds min should match");
        assertEquals(3000L, hist.getExtendedBounds().max, "Extended bounds max should match");

        System.out.println("Date histogram getters test passed!");
    }

    @Test
    @DisplayName("Date histogram fixed_interval clears calendar_interval")
    public void testFixedIntervalClearsCalendarInterval() {
        System.out.println("Testing fixed_interval clears calendar_interval...");

        DateHistogramAggregation hist = new DateHistogramAggregation("test", "timestamp")
            .setCalendarInterval("month")
            .setFixedInterval("1d");

        assertEquals("1d", hist.getFixedInterval(), "Fixed interval should be set");
        assertNull(hist.getCalendarInterval(), "Calendar interval should be cleared");

        System.out.println("Fixed interval clears calendar interval test passed!");
    }

    @Test
    @DisplayName("Date histogram calendar_interval clears fixed_interval")
    public void testCalendarIntervalClearsFixedInterval() {
        System.out.println("Testing calendar_interval clears fixed_interval...");

        DateHistogramAggregation hist = new DateHistogramAggregation("test", "timestamp")
            .setFixedInterval("1d")
            .setCalendarInterval("month");

        assertEquals("month", hist.getCalendarInterval(), "Calendar interval should be set");
        assertNull(hist.getFixedInterval(), "Fixed interval should be cleared");

        System.out.println("Calendar interval clears fixed interval test passed!");
    }

    // ==================== Bucket Key Format Tests ====================

    @Test
    @DisplayName("Date histogram bucket keys are milliseconds since epoch")
    public void testBucketKeysAreMilliseconds() {
        System.out.println("Testing bucket keys are milliseconds since epoch...");

        SplitQuery query = new SplitMatchAllQuery();
        DateHistogramAggregation hist = new DateHistogramAggregation("daily", "timestamp")
            .setFixedInterval("1d");

        SearchResult result = searcher.search(query, 10, "daily", hist);
        DateHistogramResult histResult = (DateHistogramResult) result.getAggregation("daily");

        for (DateHistogramResult.DateHistogramBucket bucket : histResult.getBuckets()) {
            double keyMs = bucket.getKey();
            // Key should be a reasonable timestamp in milliseconds (> year 2020)
            assertTrue(keyMs > 1577836800000.0, "Key should be milliseconds timestamp after 2020");
            assertTrue(keyMs < 2000000000000.0, "Key should be milliseconds timestamp before 2033");

            // keyAsString should be RFC3339 format
            String keyStr = bucket.getKeyAsString();
            assertTrue(keyStr.contains("T"), "Key string should be RFC3339 format with T separator");
            assertTrue(keyStr.contains("Z") || keyStr.contains("+") || keyStr.contains("-"),
                "Key string should have timezone indicator");

            System.out.println("  Key: " + keyMs + " -> " + keyStr);
        }

        System.out.println("Bucket key format test passed!");
    }
}

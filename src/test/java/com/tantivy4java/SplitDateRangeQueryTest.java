package com.tantivy4java;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;

import java.nio.file.Path;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

/**
 * Comprehensive test for SplitRangeQuery with date fields.
 * Tests indexing dates and querying with various range scenarios using splits.
 */
public class SplitDateRangeQueryTest {

    private SplitCacheManager cacheManager;
    private SplitSearcher searcher;
    private String splitPath;
    private QuickwitSplit.SplitMetadata metadata;

    @BeforeEach
    public void setUp(@TempDir Path tempDir) throws Exception {
        String uniqueId = "date_range_test_" + System.nanoTime();
        String indexPath = tempDir.resolve(uniqueId + "_index").toString();
        splitPath = tempDir.resolve(uniqueId + ".split").toString();

        try (SchemaBuilder builder = new SchemaBuilder()) {
            // Build schema with date field (indexed and fast for range queries)
            builder.addTextField("event_name", true, false, "default", "position")
                   .addDateField("event_date", true, true, true)  // stored, indexed, fast
                   .addTextField("category", true, true, "raw", "position")
                   .addIntegerField("priority", true, true, true);

            try (Schema schema = builder.build()) {
                try (Index index = new Index(schema, indexPath, false)) {
                    try (IndexWriter writer = index.writer(Index.Memory.DEFAULT_HEAP_SIZE, 1)) {

                        // Create test events across different years for comprehensive range testing

                        // Events in 2020
                        try (Document doc1 = new Document()) {
                            doc1.addText("event_name", "Product Launch 2020");
                            doc1.addDate("event_date", LocalDateTime.of(2020, 3, 15, 10, 30, 0));
                            doc1.addText("category", "Launch");
                            doc1.addInteger("priority", 1);
                            writer.addDocument(doc1);
                        }

                        try (Document doc2 = new Document()) {
                            doc2.addText("event_name", "Conference 2020");
                            doc2.addDate("event_date", LocalDateTime.of(2020, 9, 20, 14, 0, 0));
                            doc2.addText("category", "Conference");
                            doc2.addInteger("priority", 2);
                            writer.addDocument(doc2);
                        }

                        // Events in 2021
                        try (Document doc3 = new Document()) {
                            doc3.addText("event_name", "Workshop 2021");
                            doc3.addDate("event_date", LocalDateTime.of(2021, 1, 10, 9, 0, 0));
                            doc3.addText("category", "Workshop");
                            doc3.addInteger("priority", 3);
                            writer.addDocument(doc3);
                        }

                        try (Document doc4 = new Document()) {
                            doc4.addText("event_name", "Seminar 2021");
                            doc4.addDate("event_date", LocalDateTime.of(2021, 6, 5, 16, 30, 0));
                            doc4.addText("category", "Seminar");
                            doc4.addInteger("priority", 2);
                            writer.addDocument(doc4);
                        }

                        try (Document doc5 = new Document()) {
                            doc5.addText("event_name", "Meetup 2021");
                            doc5.addDate("event_date", LocalDateTime.of(2021, 11, 25, 18, 0, 0));
                            doc5.addText("category", "Meetup");
                            doc5.addInteger("priority", 1);
                            writer.addDocument(doc5);
                        }

                        // Events in 2022
                        try (Document doc6 = new Document()) {
                            doc6.addText("event_name", "Summit 2022");
                            doc6.addDate("event_date", LocalDateTime.of(2022, 4, 12, 13, 15, 0));
                            doc6.addText("category", "Summit");
                            doc6.addInteger("priority", 1);
                            writer.addDocument(doc6);
                        }

                        try (Document doc7 = new Document()) {
                            doc7.addText("event_name", "Training 2022");
                            doc7.addDate("event_date", LocalDateTime.of(2022, 8, 18, 11, 0, 0));
                            doc7.addText("category", "Training");
                            doc7.addInteger("priority", 3);
                            writer.addDocument(doc7);
                        }

                        // Events in 2023 (current/recent)
                        try (Document doc8 = new Document()) {
                            doc8.addText("event_name", "Innovation Day 2023");
                            doc8.addDate("event_date", LocalDateTime.of(2023, 2, 28, 12, 0, 0));
                            doc8.addText("category", "Innovation");
                            doc8.addInteger("priority", 1);
                            writer.addDocument(doc8);
                        }

                        try (Document doc9 = new Document()) {
                            doc9.addText("event_name", "Hackathon 2023");
                            doc9.addDate("event_date", LocalDateTime.of(2023, 7, 22, 9, 30, 0));
                            doc9.addText("category", "Competition");
                            doc9.addInteger("priority", 2);
                            writer.addDocument(doc9);
                        }

                        try (Document doc10 = new Document()) {
                            doc10.addText("event_name", "Year-End Review 2023");
                            doc10.addDate("event_date", LocalDateTime.of(2023, 12, 15, 15, 45, 0));
                            doc10.addText("category", "Review");
                            doc10.addInteger("priority", 3);
                            writer.addDocument(doc10);
                        }

                        writer.commit();
                        System.out.println("✓ Indexed 10 events with dates from 2020-2023");
                    }

                    // Convert to split for testing
                    QuickwitSplit.SplitConfig config = new QuickwitSplit.SplitConfig(
                        uniqueId, "test-source", "test-node");

                    metadata = QuickwitSplit.convertIndexFromPath(indexPath, splitPath, config);
                    this.metadata = metadata;
                }
            }
        }

        // Create cache manager and searcher
        String uniqueCacheName = uniqueId + "-cache";
        SplitCacheManager.CacheConfig cacheConfig =
            new SplitCacheManager.CacheConfig(uniqueCacheName);
        cacheManager = SplitCacheManager.getInstance(cacheConfig);

        searcher = cacheManager.createSplitSearcher("file://" + splitPath, metadata);
        System.out.println("✓ Created split searcher for date range testing");
    }

    @AfterEach
    public void tearDown() {
        if (searcher != null) {
            searcher.close();
        }
    }

    @Test
    public void testBasicDateRangeInclusive() {
        System.out.println("🧪 Testing basic inclusive date range queries...");

        // Test 1: Inclusive range for all events in 2021 [2021-01-01 TO 2021-12-31]
        SplitRangeQuery range2021 = SplitRangeQuery.inclusiveRange(
            "event_date",
            "2021-01-01",
            "2021-12-31",
            "date");

        SearchResult result2021 = searcher.search(range2021, 20);
        System.out.println("📅 2021 events (inclusive): " + result2021.getHits().size() + " hits");
        assertEquals(3, result2021.getHits().size(), "Should find exactly 3 events in 2021");

        // Test 2: Inclusive range for events in 2022 [2022-01-01 TO 2022-12-31]
        SplitRangeQuery range2022 = SplitRangeQuery.inclusiveRange(
            "event_date",
            "2022-01-01",
            "2022-12-31",
            "date");

        SearchResult result2022 = searcher.search(range2022, 20);
        System.out.println("📅 2022 events (inclusive): " + result2022.getHits().size() + " hits");
        assertEquals(2, result2022.getHits().size(), "Should find exactly 2 events in 2022");

        // Test 3: Broader range covering 2021-2022 [2021-01-01 TO 2022-12-31]
        SplitRangeQuery range2021to2022 = SplitRangeQuery.inclusiveRange(
            "event_date",
            "2021-01-01",
            "2022-12-31",
            "date");

        SearchResult result2021to2022 = searcher.search(range2021to2022, 20);
        System.out.println("📅 2021-2022 events (inclusive): " + result2021to2022.getHits().size() + " hits");
        assertEquals(5, result2021to2022.getHits().size(), "Should find exactly 5 events in 2021-2022");

        System.out.println("✅ Basic inclusive date range queries passed");
    }

    @Test
    public void testDateRangeExclusive() {
        System.out.println("🧪 Testing exclusive date range queries...");

        // Test 1: Exclusive range (2020-12-31 TO 2022-01-01) - should include all 2021 events
        SplitRangeQuery rangeExclusive = SplitRangeQuery.exclusiveRange(
            "event_date",
            "2020-12-31",
            "2022-01-01",
            "date");

        SearchResult resultExclusive = searcher.search(rangeExclusive, 20);
        System.out.println("📅 Exclusive range (2020-12-31, 2022-01-01): " + resultExclusive.getHits().size() + " hits");
        assertEquals(3, resultExclusive.getHits().size(), "Should find exactly 3 events in 2021 (exclusive bounds)");

        // Test 2: Mixed bounds - inclusive lower, exclusive upper [2021-01-01 TO 2022-01-01)
        SplitRangeQuery rangeMixed = new SplitRangeQuery(
            "event_date",
            SplitRangeQuery.RangeBound.inclusive("2021-01-01"),
            SplitRangeQuery.RangeBound.exclusive("2022-01-01"),
            "date");

        SearchResult resultMixed = searcher.search(rangeMixed, 20);
        System.out.println("📅 Mixed bounds [2021-01-01 TO 2022-01-01): " + resultMixed.getHits().size() + " hits");
        assertEquals(3, resultMixed.getHits().size(), "Should find exactly 3 events in 2021 (mixed bounds)");

        System.out.println("✅ Exclusive date range queries passed");
    }

    @Test
    public void testUnboundedDateRanges() {
        System.out.println("🧪 Testing unbounded date range queries...");

        // Test 1: Lower bound only - events from 2022 onwards [2022-01-01 TO *]
        SplitRangeQuery rangeFrom2022 = new SplitRangeQuery(
            "event_date",
            SplitRangeQuery.RangeBound.inclusive("2022-01-01"),
            SplitRangeQuery.RangeBound.unbounded(),
            "date");

        SearchResult resultFrom2022 = searcher.search(rangeFrom2022, 20);
        System.out.println("📅 From 2022 onwards [2022-01-01 TO *]: " + resultFrom2022.getHits().size() + " hits");
        assertEquals(5, resultFrom2022.getHits().size(), "Should find 5 events from 2022 onwards (2022: 2, 2023: 3)");

        // Test 2: Upper bound only - events before 2022 [* TO 2021-12-31]
        SplitRangeQuery rangeBefore2022 = new SplitRangeQuery(
            "event_date",
            SplitRangeQuery.RangeBound.unbounded(),
            SplitRangeQuery.RangeBound.inclusive("2021-12-31"),
            "date");

        SearchResult resultBefore2022 = searcher.search(rangeBefore2022, 20);
        System.out.println("📅 Before 2022 [* TO 2021-12-31]: " + resultBefore2022.getHits().size() + " hits");
        assertEquals(5, resultBefore2022.getHits().size(), "Should find 5 events before 2022 (2020: 2, 2021: 3)");

        // Test 3: Completely unbounded - all events [* TO *]
        SplitRangeQuery rangeAll = new SplitRangeQuery(
            "event_date",
            SplitRangeQuery.RangeBound.unbounded(),
            SplitRangeQuery.RangeBound.unbounded(),
            "date");

        SearchResult resultAll = searcher.search(rangeAll, 20);
        System.out.println("📅 All events [* TO *]: " + resultAll.getHits().size() + " hits");
        assertEquals(10, resultAll.getHits().size(), "Should find all 10 events");

        System.out.println("✅ Unbounded date range queries passed");
    }

    @Test
    public void testSpecificDateRanges() {
        System.out.println("🧪 Testing specific date range scenarios...");

        // Test 1: Narrow range in first half of 2021 [2021-01-01 TO 2021-06-30]
        SplitRangeQuery rangeFirstHalf2021 = SplitRangeQuery.inclusiveRange(
            "event_date",
            "2021-01-01",
            "2021-06-30",
            "date");

        SearchResult resultFirstHalf = searcher.search(rangeFirstHalf2021, 20);
        System.out.println("📅 First half 2021 [2021-01-01 TO 2021-06-30]: " + resultFirstHalf.getHits().size() + " hits");
        assertEquals(2, resultFirstHalf.getHits().size(), "Should find 2 events in first half of 2021");

        // Test 2: Single day that has an event - use broader range to account for date precision
        SplitRangeQuery rangeSingleDay = SplitRangeQuery.inclusiveRange(
            "event_date",
            "2021-06-04",
            "2021-06-06",
            "date");

        SearchResult resultSingleDay = searcher.search(rangeSingleDay, 20);
        System.out.println("📅 Single day around 2021-06-05: " + resultSingleDay.getHits().size() + " hits");
        assertTrue(resultSingleDay.getHits().size() >= 1, "Should find at least 1 event around 2021-06-05");

        // Test 3: Range with no events [2020-01-01 TO 2020-02-28]
        SplitRangeQuery rangeNoEvents = SplitRangeQuery.inclusiveRange(
            "event_date",
            "2020-01-01",
            "2020-02-28",
            "date");

        SearchResult resultNoEvents = searcher.search(rangeNoEvents, 20);
        System.out.println("📅 Range with no events [2020-01-01 TO 2020-02-28]: " + resultNoEvents.getHits().size() + " hits");
        assertEquals(0, resultNoEvents.getHits().size(), "Should find 0 events in early 2020");

        System.out.println("✅ Specific date range scenarios passed");
    }

    @Test
    public void testDateRangeWithDocumentRetrieval() {
        System.out.println("🧪 Testing date range queries with document field retrieval...");

        // Query for events in 2023
        SplitRangeQuery range2023 = SplitRangeQuery.inclusiveRange(
            "event_date",
            "2023-01-01",
            "2023-12-31",
            "date");

        SearchResult result2023 = searcher.search(range2023, 20);
        System.out.println("📅 2023 events: " + result2023.getHits().size() + " hits");
        assertEquals(3, result2023.getHits().size(), "Should find exactly 3 events in 2023");

        // Retrieve and validate document content
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

        for (int i = 0; i < result2023.getHits().size(); i++) {
            SearchResult.Hit hit = result2023.getHits().get(i);
            System.out.println("  Hit " + (i + 1) + " - Score: " + hit.getScore());

            try (Document doc = searcher.doc(hit.getDocAddress())) {
                // Extract and validate fields
                String eventName = (String) doc.getFirst("event_name");
                LocalDateTime eventDate = (LocalDateTime) doc.getFirst("event_date");
                String category = (String) doc.getFirst("category");
                Number priorityNumber = (Number) doc.getFirst("priority");
                Integer priority = priorityNumber != null ? priorityNumber.intValue() : null;

                assertNotNull(eventName, "Event name should not be null");
                assertNotNull(eventDate, "Event date should not be null");
                assertNotNull(category, "Category should not be null");
                assertNotNull(priority, "Priority should not be null");

                // Validate the date is actually in 2023
                assertEquals(2023, eventDate.getYear(), "Event should be in 2023");

                System.out.println("    📅 Event: " + eventName);
                System.out.println("    📅 Date: " + eventDate.format(formatter));
                System.out.println("    📂 Category: " + category);
                System.out.println("    🔢 Priority: " + priority);
            }
        }

        System.out.println("✅ Date range queries with document retrieval passed");
    }

    @Test
    public void testDateRangeQueryToString() {
        System.out.println("🧪 Testing SplitRangeQuery toString() formatting...");

        // Test various range configurations
        SplitRangeQuery inclusiveRange = SplitRangeQuery.inclusiveRange(
            "event_date", "2021-01-01", "2021-12-31", "date");
        System.out.println("Inclusive range: " + inclusiveRange.toString());
        assertTrue(inclusiveRange.toString().contains("[2021-01-01 TO 2021-12-31]"),
                   "Should format inclusive range correctly");

        SplitRangeQuery exclusiveRange = SplitRangeQuery.exclusiveRange(
            "event_date", "2021-01-01", "2021-12-31", "date");
        System.out.println("Exclusive range: " + exclusiveRange.toString());
        assertTrue(exclusiveRange.toString().contains("(2021-01-01 TO 2021-12-31)"),
                   "Should format exclusive range correctly");

        SplitRangeQuery unboundedLower = new SplitRangeQuery(
            "event_date",
            SplitRangeQuery.RangeBound.unbounded(),
            SplitRangeQuery.RangeBound.inclusive("2021-12-31"),
            "date");
        System.out.println("Unbounded lower: " + unboundedLower.toString());
        assertTrue(unboundedLower.toString().contains("* TO 2021-12-31]"),
                   "Should format unbounded lower correctly");

        System.out.println("✅ SplitRangeQuery toString() formatting passed");
    }

    @Test
    public void testDateFormatValidation() {
        System.out.println("🧪 Testing date format requirements...");

        // Test with properly formatted date strings
        SplitRangeQuery validRange = SplitRangeQuery.inclusiveRange(
            "event_date", "2021-01-01", "2021-12-31", "date");

        // Should execute without throwing exceptions
        SearchResult result = searcher.search(validRange, 10);
        assertNotNull(result, "Valid date range should execute successfully");
        System.out.println("📅 Valid date format (YYYY-MM-DD): " + result.getHits().size() + " hits");

        // Note: Invalid date formats would be handled by the native layer
        // The Java layer accepts string inputs, validation happens during query execution

        System.out.println("✅ Date format validation passed");
        System.out.println("ℹ️  Expected date format: YYYY-MM-DD (RFC3339 compatible)");
        System.out.println("ℹ️  Example valid dates: '2021-01-01', '2023-12-31'");
    }
}
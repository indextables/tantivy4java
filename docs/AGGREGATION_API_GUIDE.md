# Tantivy4Java Aggregation API Guide

This document provides a comprehensive guide to the new aggregation functionality in Tantivy4Java, implemented following Quickwit's aggregation model.

## Overview

Tantivy4Java now supports both **metric aggregations** and **bucket aggregations**, providing powerful analytics capabilities over your search data. The implementation follows Quickwit's proven aggregation architecture and supports the same JSON format for maximum compatibility.

### Supported Aggregation Types

#### Metric Aggregations
- **Count** - Document count
- **Sum** - Sum of numeric field values
- **Average** - Average of numeric field values
- **Min** - Minimum value in numeric field
- **Max** - Maximum value in numeric field
- **Stats** - Complete statistics (count, sum, avg, min, max)

#### Bucket Aggregations
- **Terms** - Group documents by field values
- **Range** - Group documents into numeric ranges
- **Histogram** - Group documents into fixed-size numeric intervals
- **Date Histogram** - Group documents into time-based intervals

## Quick Start Examples

### Basic Metric Aggregations

```java
// Create a SplitSearcher (assumes you have cache manager set up)
try (SplitSearcher searcher = cacheManager.createSplitSearcher(splitUrl, metadata)) {

    // Create query (match all documents)
    SplitQuery query = new SplitMatchAllQuery();

    // Single metric aggregation
    StatsAggregation priceStats = new StatsAggregation("price");
    SearchResult result = searcher.search(query, 10, "stats", priceStats);

    // Access aggregation results
    StatsResult stats = (StatsResult) result.getAggregation("stats");
    System.out.println("Count: " + stats.getCount());
    System.out.println("Sum: " + stats.getSum());
    System.out.println("Average: " + stats.getAverage());
    System.out.println("Min: " + stats.getMin());
    System.out.println("Max: " + stats.getMax());
}
```

### Multiple Aggregations in One Query

```java
// Create multiple aggregations
Map<String, SplitAggregation> aggregations = new HashMap<>();
aggregations.put("total_count", new CountAggregation());
aggregations.put("revenue_sum", new SumAggregation("revenue"));
aggregations.put("price_avg", new AverageAggregation("price"));
aggregations.put("categories", new TermsAggregation("category"));

// Execute all aggregations in one search
SearchResult result = searcher.search(query, 10, aggregations);

// Access results
CountResult count = (CountResult) result.getAggregation("total_count");
SumResult revenue = (SumResult) result.getAggregation("revenue_sum");
AverageResult avgPrice = (AverageResult) result.getAggregation("price_avg");
TermsResult categories = (TermsResult) result.getAggregation("categories");
```

## Metric Aggregations

### 1. Count Aggregation

Counts the number of documents.

```java
// Basic count
CountAggregation count = new CountAggregation();
SearchResult result = searcher.search(query, 10, "total", count);
CountResult countResult = (CountResult) result.getAggregation("total");
long totalDocs = countResult.getCount();
```

### 2. Sum Aggregation

Sums numeric values in a field.

```java
// Sum all values in 'revenue' field
SumAggregation revenueSum = new SumAggregation("revenue");
SearchResult result = searcher.search(query, 10, "total_revenue", revenueSum);
SumResult sumResult = (SumResult) result.getAggregation("total_revenue");
double totalRevenue = sumResult.getSum();
```

### 3. Average Aggregation

Calculates the average of numeric values.

```java
// Average price
AverageAggregation avgPrice = new AverageAggregation("price");
SearchResult result = searcher.search(query, 10, "avg_price", avgPrice);
AverageResult avgResult = (AverageResult) result.getAggregation("avg_price");
double averagePrice = avgResult.getAverage();
```

### 4. Min/Max Aggregations

Find minimum and maximum values.

```java
// Minimum price
MinAggregation minPrice = new MinAggregation("price");
SearchResult result = searcher.search(query, 10, "min_price", minPrice);
MinResult minResult = (MinResult) result.getAggregation("min_price");
double minimumPrice = minResult.getMin();

// Maximum price
MaxAggregation maxPrice = new MaxAggregation("price");
SearchResult result2 = searcher.search(query, 10, "max_price", maxPrice);
MaxResult maxResult = (MaxResult) result2.getAggregation("max_price");
double maximumPrice = maxResult.getMax();
```

### 5. Stats Aggregation

Get complete statistics in one aggregation.

```java
// Complete statistics for price field
StatsAggregation priceStats = new StatsAggregation("price");
SearchResult result = searcher.search(query, 10, "price_stats", priceStats);
StatsResult stats = (StatsResult) result.getAggregation("price_stats");

// Access all statistics
long count = stats.getCount();
double sum = stats.getSum();
double avg = stats.getAverage();
double min = stats.getMin();
double max = stats.getMax();
```

## Bucket Aggregations

### 1. Terms Aggregation

Groups documents by field values (like SQL GROUP BY).

```java
// Group by category, get top 10 categories
TermsAggregation categoryTerms = new TermsAggregation("categories", "category", 10, 0);
SearchResult result = searcher.search(query, 10, "categories", categoryTerms);
TermsResult terms = (TermsResult) result.getAggregation("categories");

// Iterate through buckets
for (TermsResult.TermsBucket bucket : terms.getBuckets()) {
    String categoryName = bucket.getKeyAsString();
    long documentCount = bucket.getDocCount();
    System.out.println(categoryName + ": " + documentCount + " documents");
}
```

#### Terms Aggregation Parameters

```java
// Constructor options
new TermsAggregation("field_name");                    // Simple terms
new TermsAggregation("aggregation_name", "field_name"); // Named aggregation
new TermsAggregation("agg_name", "field", size, shardSize); // With size limits

// Parameters:
// - size: Number of top buckets to return (default: 10)
// - shardSize: Number of buckets to collect per shard (default: size * 1.5)
```

### 2. Range Aggregation

Groups documents into numeric ranges.

```java
// Price ranges
RangeAggregation priceRanges = new RangeAggregation("price_ranges", "price")
    .addRange("budget", null, 100.0)        // < $100
    .addRange("mid-range", 100.0, 500.0)    // $100-$500
    .addRange("premium", 500.0, 1000.0)     // $500-$1000
    .addRange("luxury", 1000.0, null);      // > $1000

SearchResult result = searcher.search(query, 10, "ranges", priceRanges);
RangeResult ranges = (RangeResult) result.getAggregation("ranges");

// Access range buckets
for (RangeResult.RangeBucket bucket : ranges.getBuckets()) {
    String rangeName = bucket.getKey();
    Double from = bucket.getFrom();        // null if unbounded
    Double to = bucket.getTo();            // null if unbounded
    long docCount = bucket.getDocCount();

    System.out.println(rangeName + " (" + from + " to " + to + "): " + docCount);
}
```

### 3. Histogram Aggregation

Groups documents into fixed-size numeric intervals.

```java
// Price distribution with $200 intervals
HistogramAggregation priceHist = new HistogramAggregation("price_dist", "price", 200.0);

// Optional: set offset and minimum document count
priceHist.setOffset(50.0)         // Shift intervals by $50
         .setMinDocCount(1.0);    // Only include buckets with at least 1 doc

SearchResult result = searcher.search(query, 10, "histogram", priceHist);
HistogramResult hist = (HistogramResult) result.getAggregation("histogram");

// Access histogram buckets
for (HistogramResult.HistogramBucket bucket : hist.getBuckets()) {
    double intervalStart = bucket.getKey();
    long docCount = bucket.getDocCount();

    System.out.println("$" + intervalStart + "-$" + (intervalStart + 200) +
                      ": " + docCount + " products");
}
```

### 4. Date Histogram Aggregation

Groups documents into time-based intervals.

```java
// Daily sales timeline
DateHistogramAggregation dailyHist = new DateHistogramAggregation("daily", "sale_date")
    .setFixedInterval("1d")          // 1 day intervals
    .setOffset("-4h");               // Shift timezone by -4 hours

// Other interval options:
// "1h", "1d", "1w", "1M", "1y" for fixed intervals

SearchResult result = searcher.search(query, 10, "daily", dailyHist);
DateHistogramResult dateHist = (DateHistogramResult) result.getAggregation("daily");

// Access date buckets
for (DateHistogramResult.DateHistogramBucket bucket : dateHist.getBuckets()) {
    double timestamp = bucket.getKey();           // Milliseconds since epoch
    String dateString = bucket.getKeyAsString();  // Formatted date string
    long docCount = bucket.getDocCount();

    System.out.println(dateString + ": " + docCount + " sales");
}
```

## Advanced Usage Patterns

### Combining Search with Aggregations

```java
// Search for premium products and aggregate
SplitQuery premiumQuery = searcher.parseQuery("premium", "category");

Map<String, SplitAggregation> aggregations = new HashMap<>();
aggregations.put("stats", new StatsAggregation("price"));
aggregations.put("brands", new TermsAggregation("brand"));

SearchResult result = searcher.search(premiumQuery, 20, aggregations);

// Get both search results AND aggregations
List<SearchHit> hits = result.getHits();              // Top 20 premium products
StatsResult priceStats = (StatsResult) result.getAggregation("stats");
TermsResult brands = (TermsResult) result.getAggregation("brands");
```

### Performance Optimization

```java
// For large datasets, use aggregation-only queries (limit = 0)
SearchResult result = searcher.search(query, 0, aggregations);  // No document results

// For terms aggregations, tune size parameters
TermsAggregation terms = new TermsAggregation("categories", "category", 50, 100);
// size = 50: Return top 50 buckets
// shardSize = 100: Collect top 100 per shard for better accuracy
```

## Error Handling

```java
try {
    SearchResult result = searcher.search(query, 10, aggregations);

    if (result.hasAggregations()) {
        // Process aggregations
        for (Map.Entry<String, AggregationResult> entry : result.getAggregations().entrySet()) {
            String name = entry.getKey();
            AggregationResult aggResult = entry.getValue();
            String type = aggResult.getType();  // "stats", "terms", "range", etc.
        }
    }
} catch (Exception e) {
    System.err.println("Aggregation failed: " + e.getMessage());
}
```

## JSON Format Compatibility

All aggregations generate JSON that's compatible with Quickwit and Elasticsearch:

```java
// Get the JSON representation of any aggregation
StatsAggregation stats = new StatsAggregation("price");
String json = stats.toAggregationJson();
// Result: {"stats": {"field": "price"}}

TermsAggregation terms = new TermsAggregation("categories", "category", 10, 20);
String termsJson = terms.toAggregationJson();
// Result: {"terms": {"field": "category", "size": 10, "shard_size": 20}}
```

## Complete Example: E-commerce Analytics

```java
public class EcommerceAnalytics {

    public void analyzeProducts(SplitSearcher searcher) {
        try {
            // Query for active products
            SplitQuery activeProducts = searcher.parseQuery("active", "status");

            // Create comprehensive aggregations
            Map<String, SplitAggregation> aggregations = new HashMap<>();

            // Metric aggregations
            aggregations.put("total_products", new CountAggregation());
            aggregations.put("total_revenue", new SumAggregation("revenue"));
            aggregations.put("price_stats", new StatsAggregation("price"));

            // Bucket aggregations
            aggregations.put("categories", new TermsAggregation("category", 20));
            aggregations.put("price_ranges", new RangeAggregation("price")
                .addRange("budget", null, 50.0)
                .addRange("mid", 50.0, 200.0)
                .addRange("premium", 200.0, null));
            aggregations.put("daily_sales", new DateHistogramAggregation("sale_date")
                .setFixedInterval("1d"));

            // Execute all aggregations
            SearchResult result = searcher.search(activeProducts, 10, aggregations);

            // Process results
            CountResult totalProducts = (CountResult) result.getAggregation("total_products");
            SumResult totalRevenue = (SumResult) result.getAggregation("total_revenue");
            StatsResult priceStats = (StatsResult) result.getAggregation("price_stats");
            TermsResult categories = (TermsResult) result.getAggregation("categories");
            RangeResult priceRanges = (RangeResult) result.getAggregation("price_ranges");
            DateHistogramResult dailySales = (DateHistogramResult) result.getAggregation("daily_sales");

            // Generate report
            System.out.println("=== E-COMMERCE ANALYTICS REPORT ===");
            System.out.println("Total Active Products: " + totalProducts.getCount());
            System.out.println("Total Revenue: $" + totalRevenue.getSum());
            System.out.println("Average Price: $" + String.format("%.2f", priceStats.getAverage()));
            System.out.println("Price Range: $" + priceStats.getMin() + " - $" + priceStats.getMax());

            System.out.println("\nTop Categories:");
            for (TermsResult.TermsBucket bucket : categories.getBuckets()) {
                System.out.println("  " + bucket.getKeyAsString() + ": " + bucket.getDocCount());
            }

            System.out.println("\nPrice Distribution:");
            for (RangeResult.RangeBucket bucket : priceRanges.getBuckets()) {
                System.out.println("  " + bucket.getKey() + ": " + bucket.getDocCount());
            }

            System.out.println("\nDaily Sales Trend:");
            for (DateHistogramResult.DateHistogramBucket bucket : dailySales.getBuckets()) {
                System.out.println("  " + bucket.getKeyAsString() + ": " + bucket.getDocCount());
            }

        } catch (Exception e) {
            System.err.println("Analytics failed: " + e.getMessage());
        }
    }
}
```

## Setup Requirements

### SplitCacheManager Configuration

```java
// Create cache manager for SplitSearcher
SplitCacheManager.CacheConfig config = new SplitCacheManager.CacheConfig("analytics-cache")
    .withMaxCacheSize(200_000_000);  // 200MB cache

SplitCacheManager cacheManager = SplitCacheManager.getInstance(config);

// Create searcher with proper metadata
QuickwitSplit.SplitMetadata metadata = QuickwitSplit.readSplitMetadata(splitPath);
SplitSearcher searcher = cacheManager.createSplitSearcher(splitUrl, metadata);
```

### Schema Requirements

Ensure your schema has the appropriate field types for aggregations:

```java
try (SchemaBuilder builder = new SchemaBuilder()) {
    // Text fields for terms aggregations
    builder.addTextField("category", true, false, "default", "position");
    builder.addTextField("brand", true, false, "default", "position");

    // Numeric fields for metric and range aggregations
    builder.addIntegerField("price", true, true, true);      // stored + indexed + fast
    builder.addIntegerField("revenue", true, true, true);    // stored + indexed + fast
    builder.addIntegerField("sale_date", true, true, true);  // stored + indexed + fast (timestamp)

    // Schema build...
}
```

## Best Practices

1. **Use appropriate field types**: Ensure numeric fields are marked as `fast` for optimal aggregation performance
2. **Batch aggregations**: Run multiple aggregations in one query rather than separate queries
3. **Tune terms aggregation size**: Use appropriate `size` and `shardSize` parameters for terms aggregations
4. **Consider memory usage**: Large aggregations consume memory; monitor cache size
5. **Test with representative data**: Performance characteristics vary with data size and cardinality

## Migration from Basic Search

If you're upgrading from basic search-only functionality:

```java
// Before: Basic search only
SearchResult result = searcher.search(query, 20);
List<SearchHit> hits = result.getHits();

// After: Search + aggregations
Map<String, SplitAggregation> aggregations = new HashMap<>();
aggregations.put("stats", new StatsAggregation("price"));

SearchResult result = searcher.search(query, 20, aggregations);
List<SearchHit> hits = result.getHits();                    // Same search results
StatsResult stats = (StatsResult) result.getAggregation("stats"); // Plus aggregations!
```

This aggregation API provides powerful analytics capabilities while maintaining the performance and memory efficiency that Tantivy4Java is known for.
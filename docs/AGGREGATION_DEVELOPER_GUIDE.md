# Tantivy4Java Aggregation Developer Guide

## ⚠️ Breaking Change Notice

**CountAggregation API Update**: The `CountAggregation` constructor now requires a field name parameter. This eliminates assumptions about field names and makes the API more explicit and reliable.

**Migration Required**:
```java
// OLD (no longer supported)
new CountAggregation()

// NEW (required)
new CountAggregation("field_name")  // Use any field that exists in your schema
```

## Overview

Tantivy4Java provides comprehensive aggregation capabilities through the `SplitSearcher` API, following Quickwit's aggregation model. Aggregations allow you to compute statistical metrics (count, sum, average, min, max) and group documents by field values, similar to Elasticsearch aggregations.

## Key Features

- **Statistical Aggregations**: Count, Sum, Average, Min, Max, Stats (all-in-one)
- **Multiple Aggregations**: Run multiple aggregations in a single search
- **Filtered Aggregations**: Apply aggregations to filtered query results
- **Aggregation-Only Search**: Compute aggregations without retrieving documents
- **Type Safety**: Strongly-typed result objects for each aggregation type
- **Performance Optimized**: Built on Quickwit's proven aggregation engine

## Supported Aggregation Types

### 1. Statistical Aggregations

#### StatsAggregation
Computes all statistical metrics (count, sum, avg, min, max) in one operation:

```java
// Basic stats aggregation
StatsAggregation scoreStats = new StatsAggregation("score");

// Named stats aggregation
StatsAggregation responseStats = new StatsAggregation("response_stats", "response_time");
```

#### CountAggregation
Counts the number of documents that have non-null values for a specified field:

```java
// Count documents that have a non-null "title" field
CountAggregation docCount = new CountAggregation("title");

// Named count aggregation
CountAggregation totalCount = new CountAggregation("total_docs", "title");
```

#### SumAggregation
Computes the sum of numeric field values:

```java
// Sum of score field
SumAggregation scoreSum = new SumAggregation("score_sum", "score");
```

#### AverageAggregation
Computes the average of numeric field values:

```java
// Average response time
AverageAggregation avgResponse = new AverageAggregation("avg_response", "response_time");
```

#### MinAggregation
Finds the minimum value in a numeric field:

```java
// Minimum score
MinAggregation minScore = new MinAggregation("min_score", "score");
```

#### MaxAggregation
Finds the maximum value in a numeric field:

```java
// Maximum response time
MaxAggregation maxResponse = new MaxAggregation("max_response", "response_time");
```

## Basic Usage

### Setup SplitSearcher

```java
// Create cache manager
SplitCacheManager.CacheConfig cacheConfig =
    new SplitCacheManager.CacheConfig("my-cache")
    .withMaxCacheSize(200_000_000); // 200MB cache

SplitCacheManager cacheManager = SplitCacheManager.getInstance(cacheConfig);

// Create searcher from split file
SplitSearcher searcher = cacheManager.createSplitSearcher("file:///path/to/index.split");
```

### Single Aggregation Example

```java
// Create a stats aggregation for the "score" field
StatsAggregation scoreStats = new StatsAggregation("score");

// Execute search with aggregation
SplitQuery query = new SplitMatchAllQuery(); // or any other query
SearchResult result = searcher.search(query, 10, "stats", scoreStats);

// Extract aggregation result
StatsResult stats = (StatsResult) result.getAggregation("stats");

// Access statistical metrics
System.out.println("Document Count: " + stats.getCount());
System.out.println("Sum: " + stats.getSum());
System.out.println("Average: " + stats.getAverage());
System.out.println("Min: " + stats.getMin());
System.out.println("Max: " + stats.getMax());
```

### Multiple Aggregations Example

```java
// Create multiple aggregations
Map<String, SplitAggregation> aggregations = new HashMap<>();
aggregations.put("score_stats", new StatsAggregation("score"));
aggregations.put("response_sum", new SumAggregation("response_time"));
aggregations.put("doc_count", new CountAggregation("title")); // Count docs with non-null title

// Execute search with multiple aggregations
SearchResult result = searcher.search(query, 10, aggregations);

// Access different aggregation results
StatsResult scoreStats = (StatsResult) result.getAggregation("score_stats");
SumResult responseSum = (SumResult) result.getAggregation("response_sum");
CountResult docCount = (CountResult) result.getAggregation("doc_count");

System.out.println("Score average: " + scoreStats.getAverage());
System.out.println("Total response time: " + responseSum.getSum());
System.out.println("Total documents: " + docCount.getCount());
```

### Aggregation-Only Search

For scenarios where you only need aggregation results without document hits:

```java
StatsAggregation scoreStats = new StatsAggregation("score");

// Use aggregate() method for aggregation-only search
SearchResult result = searcher.aggregate(query, "stats", scoreStats);

// No document hits, only aggregation results
assertEquals(0, result.getHits().size());
assertTrue(result.hasAggregations());

StatsResult stats = (StatsResult) result.getAggregation("stats");
```

### Filtered Aggregations

Apply aggregations to filtered query results:

```java
// Create a filtered query (e.g., only premium users)
SplitQuery premiumQuery = searcher.parseQuery("premium", "category");

// Apply aggregation to filtered results
StatsAggregation scoreStats = new StatsAggregation("score");
SearchResult result = searcher.search(premiumQuery, 10, "stats", scoreStats);

// Results only include documents matching the filter
StatsResult stats = (StatsResult) result.getAggregation("stats");
System.out.println("Premium user average score: " + stats.getAverage());
```

## Complete Working Example

Here's a complete example demonstrating various aggregation capabilities:

```java
import com.tantivy4java.*;
import java.util.Map;
import java.util.HashMap;

public class AggregationExample {

    public void demonstrateAggregations() {
        try {
            // Setup
            SplitCacheManager.CacheConfig cacheConfig =
                new SplitCacheManager.CacheConfig("aggregation-cache")
                .withMaxCacheSize(200_000_000);

            SplitCacheManager cacheManager = SplitCacheManager.getInstance(cacheConfig);
            SplitSearcher searcher = cacheManager.createSplitSearcher("file:///path/to/data.split");

            // 1. Single aggregation
            System.out.println("=== Single Stats Aggregation ===");
            StatsAggregation scoreStats = new StatsAggregation("score");
            SplitQuery matchAll = new SplitMatchAllQuery();

            SearchResult result = searcher.search(matchAll, 5, "stats", scoreStats);
            StatsResult stats = (StatsResult) result.getAggregation("stats");

            System.out.println("Documents: " + stats.getCount());
            System.out.println("Average Score: " + stats.getAverage());
            System.out.println("Score Range: " + stats.getMin() + " - " + stats.getMax());

            // 2. Multiple aggregations
            System.out.println("\n=== Multiple Aggregations ===");
            Map<String, SplitAggregation> multipleAggs = new HashMap<>();
            multipleAggs.put("score_stats", new StatsAggregation("score"));
            multipleAggs.put("response_sum", new SumAggregation("response_time"));
            multipleAggs.put("user_count", new CountAggregation());

            SearchResult multiResult = searcher.search(matchAll, 0, multipleAggs);

            StatsResult scoreStats2 = (StatsResult) multiResult.getAggregation("score_stats");
            SumResult responseSum = (SumResult) multiResult.getAggregation("response_sum");
            CountResult userCount = (CountResult) multiResult.getAggregation("user_count");

            System.out.println("Score Stats: " + scoreStats2);
            System.out.println("Total Response Time: " + responseSum.getSum());
            System.out.println("User Count: " + userCount.getCount());

            // 3. Filtered aggregation
            System.out.println("\n=== Filtered Aggregation ===");
            SplitQuery premiumFilter = searcher.parseQuery("premium", "category");
            SearchResult filteredResult = searcher.search(premiumFilter, 0, "premium_stats", scoreStats);

            StatsResult premiumStats = (StatsResult) filteredResult.getAggregation("premium_stats");
            System.out.println("Premium Users: " + premiumStats.getCount());
            System.out.println("Premium Average Score: " + premiumStats.getAverage());

            // 4. Individual metric aggregations
            System.out.println("\n=== Individual Metrics ===");

            CountAggregation count = new CountAggregation("total", "title"); // Count docs with title
            SumAggregation sum = new SumAggregation("score_sum", "score");
            AverageAggregation avg = new AverageAggregation("score_avg", "score");
            MinAggregation min = new MinAggregation("score_min", "score");
            MaxAggregation max = new MaxAggregation("score_max", "score");

            // Run each individually
            CountResult countRes = (CountResult) searcher.search(matchAll, 0, "count", count).getAggregation("count");
            SumResult sumRes = (SumResult) searcher.search(matchAll, 0, "sum", sum).getAggregation("sum");
            AverageResult avgRes = (AverageResult) searcher.search(matchAll, 0, "avg", avg).getAggregation("avg");
            MinResult minRes = (MinResult) searcher.search(matchAll, 0, "min", min).getAggregation("min");
            MaxResult maxRes = (MaxResult) searcher.search(matchAll, 0, "max", max).getAggregation("max");

            System.out.println("Count: " + countRes.getCount());
            System.out.println("Sum: " + sumRes.getSum());
            System.out.println("Average: " + avgRes.getAverage());
            System.out.println("Min: " + minRes.getMin());
            System.out.println("Max: " + maxRes.getMax());

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
```

## Result Types

Each aggregation type returns a specific result object:

### StatsResult
```java
StatsResult stats = (StatsResult) result.getAggregation("stats");
long count = stats.getCount();       // Number of documents
double sum = stats.getSum();         // Sum of values
double avg = stats.getAverage();     // Average value
double min = stats.getMin();         // Minimum value
double max = stats.getMax();         // Maximum value
```

### CountResult
```java
CountResult count = (CountResult) result.getAggregation("count");
long documentCount = count.getCount();
```

### SumResult
```java
SumResult sum = (SumResult) result.getAggregation("sum");
double totalSum = sum.getSum();
```

### AverageResult
```java
AverageResult avg = (AverageResult) result.getAggregation("avg");
double average = avg.getAverage();
```

### MinResult
```java
MinResult min = (MinResult) result.getAggregation("min");
double minimum = min.getMin();
```

### MaxResult
```java
MaxResult max = (MaxResult) result.getAggregation("max");
double maximum = max.getMax();
```

## Performance Considerations

### Schema Requirements

For optimal aggregation performance, ensure numeric fields are configured with the `fast` flag:

```java
SchemaBuilder builder = new SchemaBuilder();
builder.addIntegerField("score", true, true, true);      // stored + indexed + fast
builder.addIntegerField("response_time", true, true, true); // stored + indexed + fast
```

The `fast` flag enables Tantivy's fast field storage, which provides O(1) access to field values during aggregation.

### Cache Configuration

Configure adequate cache size for your workload:

```java
SplitCacheManager.CacheConfig cacheConfig =
    new SplitCacheManager.CacheConfig("my-cache")
    .withMaxCacheSize(500_000_000); // 500MB for larger datasets
```

### Aggregation vs. Search

- Use `search()` when you need both documents and aggregations
- Use `aggregate()` when you only need aggregation results (faster)
- Combine multiple aggregations in a single call for efficiency

## Error Handling

```java
try {
    SearchResult result = searcher.search(query, 10, "stats", aggregation);

    if (result.hasAggregations()) {
        StatsResult stats = (StatsResult) result.getAggregation("stats");
        // Process results
    } else {
        System.out.println("No aggregation results found");
    }

} catch (RuntimeException e) {
    if (e.getMessage().contains("field not found")) {
        System.err.println("Aggregation field does not exist in the schema");
    } else if (e.getMessage().contains("not a numeric field")) {
        System.err.println("Aggregation field must be numeric (integer/float)");
    } else {
        System.err.println("Aggregation failed: " + e.getMessage());
    }
}
```

## Field Type Requirements

Different aggregations have specific field type requirements:

| Aggregation | Supported Field Types | Notes |
|-------------|----------------------|-------|
| Count | All types | Counts documents with non-null field values |
| Sum | Integer, Float | Numeric fields only |
| Average | Integer, Float | Numeric fields only |
| Min | Integer, Float | Numeric fields only |
| Max | Integer, Float | Numeric fields only |
| Stats | Integer, Float | Numeric fields only |

## Best Practices

1. **Use StatsAggregation** when you need multiple metrics for the same field (more efficient than separate aggregations)

2. **Configure fast fields** for numeric fields that will be aggregated frequently

3. **Combine aggregations** in a single search call rather than multiple separate calls

4. **Use appropriate cache sizes** - larger caches improve performance for repeated aggregations

5. **Filter first, aggregate second** - apply filters to reduce the dataset before aggregation

6. **Use aggregation-only search** when you don't need document hits, only metrics

7. **Handle type casting** properly when extracting aggregation results

8. **Monitor memory usage** - large aggregations on high-cardinality fields can consume significant memory

## Advanced Usage

### Custom Aggregation Names

```java
// Use descriptive names for multiple aggregations on the same field
Map<String, SplitAggregation> aggregations = new HashMap<>();
aggregations.put("daily_score_stats", new StatsAggregation("daily_scores", "score"));
aggregations.put("weekly_score_stats", new StatsAggregation("weekly_scores", "score"));
```

### Conditional Aggregations

```java
// Apply different aggregations based on query results
SearchResult initialResult = searcher.search(query, 1);

if (initialResult.getTotalHits() > 1000) {
    // Use stats aggregation for large result sets
    StatsAggregation stats = new StatsAggregation("score");
    result = searcher.aggregate(query, "stats", stats);
} else {
    // Use individual aggregations for small result sets
    SumAggregation sum = new SumAggregation("score_sum", "score");
    result = searcher.search(query, 10, "sum", sum);
}
```

## Troubleshooting

### Common Issues

1. **"Field not found" error**: Ensure the field exists in your schema and is properly indexed
2. **"Not a numeric field" error**: Statistical aggregations require integer or float fields
3. **Null aggregation results**: Check that `result.hasAggregations()` returns true
4. **Performance issues**: Verify numeric fields have the `fast` flag enabled
5. **Memory issues**: Increase cache size or reduce aggregation scope

### Debug Information

Enable debug logging to troubleshoot aggregation issues:

```bash
export TANTIVY4JAVA_DEBUG=1
mvn test -Dtest="YourAggregationTest"
```

This will provide detailed information about:
- Aggregation request JSON generation
- Native aggregation execution
- Result parsing and type conversion
- Cache hit/miss statistics

## Migration from Elasticsearch

If you're migrating from Elasticsearch aggregations, here's a mapping:

| Elasticsearch | Tantivy4Java | Notes |
|---------------|--------------|--------|
| `value_count` | `CountAggregation` | Document counting |
| `sum` | `SumAggregation` | Sum of numeric values |
| `avg` | `AverageAggregation` | Average of numeric values |
| `min` | `MinAggregation` | Minimum value |
| `max` | `MaxAggregation` | Maximum value |
| `stats` | `StatsAggregation` | All stats in one aggregation |

The API patterns are very similar, making migration straightforward.

---

This guide covers the essential aspects of using Tantivy4Java's aggregation capabilities. For more advanced use cases or specific requirements, refer to the test suite in `SplitSearcherAggregationTest.java` for additional examples.
# Cache Statistics Guide

**Tantivy4Java Comprehensive Cache Monitoring**

This guide explains how to use the advanced cache statistics functionality implemented in tantivy4java, providing detailed insights into cache performance across all cache types.

## Table of Contents

- [Overview](#overview)
- [Cache Types](#cache-types)
- [Basic Usage](#basic-usage)
- [Advanced Usage](#advanced-usage)
- [Performance Monitoring](#performance-monitoring)
- [Troubleshooting](#troubleshooting)
- [Best Practices](#best-practices)

## Overview

Tantivy4java implements a comprehensive cache statistics system that provides detailed metrics for all cache types used in the system. This follows Quickwit's proven multi-level caching architecture with real-time performance monitoring.

### Key Features

- **Per-Cache-Type Metrics**: Separate statistics for each cache component
- **Real-Time Monitoring**: Live metrics updated with each cache operation
- **Production-Ready**: Designed for continuous monitoring in production environments
- **Performance Optimization**: Data-driven insights for cache tuning

## Cache Types

The system tracks four distinct cache types, each serving different purposes:

### 1. 📦 **ByteRangeCache**
- **Purpose**: Storage byte range caching for efficient I/O operations
- **Use Case**: Reduces redundant disk/network reads by caching byte ranges
- **Quickwit Mapping**: `shortlived_cache` metrics
- **Optimal For**: File system and S3 storage access patterns

### 2. 📄 **FooterCache** 
- **Purpose**: Split metadata and footer information caching
- **Use Case**: Eliminates repeated parsing of split file metadata
- **Quickwit Mapping**: `split_footer_cache` metrics
- **Optimal For**: Frequent split file access patterns

### 3. ⚡ **FastFieldCache**
- **Purpose**: Component-level fast field caching
- **Use Case**: Accelerates field value lookups and aggregations
- **Quickwit Mapping**: `fast_field_cache` metrics
- **Optimal For**: Field-heavy queries and analytical workloads

### 4. 🔍 **SplitCache**
- **Purpose**: Complete split file caching
- **Use Case**: Caches entire split files for repeated access
- **Quickwit Mapping**: `searcher_split_cache` metrics
- **Optimal For**: Repeated queries against the same splits

## Basic Usage

### Getting Started

```java
import com.tantivy4java.SplitCacheManager;
import com.tantivy4java.SplitCacheManager.GlobalCacheStats;

// Create cache manager with monitoring
SplitCacheManager.CacheConfig config = new SplitCacheManager.CacheConfig("production-cache")
    .withMaxCacheSize(500_000_000); // 500MB

SplitCacheManager cacheManager = SplitCacheManager.getInstance(config);
```

### Basic Cache Statistics

```java
// Get aggregated cache statistics
GlobalCacheStats stats = cacheManager.getGlobalCacheStats();

System.out.println("Total Hits: " + stats.getTotalHits());
System.out.println("Total Misses: " + stats.getTotalMisses());  
System.out.println("Hit Rate: " + String.format("%.2f%%", stats.getHitRate() * 100));
System.out.println("Cache Utilization: " + String.format("%.2f%%", stats.getUtilization() * 100));
System.out.println("Current Size: " + stats.getCurrentSize() + " bytes");
System.out.println("Active Splits: " + stats.getActiveSplits());
```

**Expected Output:**
```
Total Hits: 1,234,567
Total Misses: 123,456
Hit Rate: 90.91%
Cache Utilization: 67.34%
Current Size: 336,700,000 bytes
Active Splits: 42
```

## Advanced Usage

### Comprehensive Cache Statistics

For detailed per-cache-type analysis:

```java
import com.tantivy4java.SplitCacheManager.ComprehensiveCacheStats;
import com.tantivy4java.SplitCacheManager.CacheTypeStats;

// Get detailed breakdown by cache type
ComprehensiveCacheStats detailedStats = cacheManager.getComprehensiveCacheStats();

// Access individual cache metrics
CacheTypeStats byteRangeStats = detailedStats.getByteRangeCache();
CacheTypeStats footerStats = detailedStats.getFooterCache();
CacheTypeStats fastFieldStats = detailedStats.getFastFieldCache();
CacheTypeStats splitStats = detailedStats.getSplitCache();

// Display formatted comprehensive statistics
System.out.println(detailedStats.toString());
```

**Expected Output:**
```
ComprehensiveCacheStats{
  📦 ByteRangeCache: ByteRangeCache{hits=45678, misses=5678, evictions=234, hitRate=88.96%, sizeBytes=125000000}
  📄 FooterCache: FooterCache{hits=12345, misses=1234, evictions=45, hitRate=90.91%, sizeBytes=50000000}
  ⚡ FastFieldCache: FastFieldCache{hits=98765, misses=9876, evictions=456, hitRate=90.91%, sizeBytes=150000000}
  🔍 SplitCache: SplitCache{hits=23456, misses=2345, evictions=123, hitRate=90.91%, sizeBytes=200000000}
  🏆 Aggregated: GlobalCacheStats{hits=180244, misses=19133, evictions=858, hitRate=90.40%, currentSize=525000000, maxSize=500000000, utilization=105.00%, activeSplits=0}
}
```

### Individual Cache Analysis

```java
// Analyze ByteRangeCache performance
CacheTypeStats byteRangeStats = detailedStats.getByteRangeCache();
System.out.println("ByteRangeCache Performance:");
System.out.println("  Hits: " + byteRangeStats.getHits());
System.out.println("  Misses: " + byteRangeStats.getMisses());
System.out.println("  Hit Rate: " + String.format("%.2f%%", byteRangeStats.getHitRate() * 100));
System.out.println("  Size: " + formatBytes(byteRangeStats.getSizeBytes()));
System.out.println("  Evictions: " + byteRangeStats.getEvictions());

// Helper method for byte formatting
private static String formatBytes(long bytes) {
    if (bytes < 1024) return bytes + " B";
    int exp = (int) (Math.log(bytes) / Math.log(1024));
    String pre = "KMGTPE".charAt(exp-1) + "";
    return String.format("%.1f %sB", bytes / Math.pow(1024, exp), pre);
}
```

**Expected Output:**
```
ByteRangeCache Performance:
  Hits: 45,678
  Misses: 5,678
  Hit Rate: 88.96%
  Size: 119.2 MB
  Evictions: 234
```

## Performance Monitoring

### Continuous Monitoring Setup

```java
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class CacheMonitor {
    private final SplitCacheManager cacheManager;
    private final ScheduledExecutorService scheduler;
    
    public CacheMonitor(SplitCacheManager cacheManager) {
        this.cacheManager = cacheManager;
        this.scheduler = Executors.newScheduledThreadPool(1);
    }
    
    public void startMonitoring() {
        // Monitor cache performance every 30 seconds
        scheduler.scheduleAtFixedRate(this::reportCacheStats, 0, 30, TimeUnit.SECONDS);
    }
    
    private void reportCacheStats() {
        ComprehensiveCacheStats stats = cacheManager.getComprehensiveCacheStats();
        
        // Log essential metrics
        System.out.println("=== Cache Performance Report ===");
        System.out.println("Timestamp: " + new java.util.Date());
        
        // Check each cache type
        reportCacheType("ByteRange", stats.getByteRangeCache());
        reportCacheType("Footer", stats.getFooterCache());
        reportCacheType("FastField", stats.getFastFieldCache());
        reportCacheType("Split", stats.getSplitCache());
        
        // Overall performance
        GlobalCacheStats overall = stats.getAggregated();
        System.out.println("Overall Hit Rate: " + String.format("%.2f%%", overall.getHitRate() * 100));
        System.out.println("Total Size: " + formatBytes(overall.getCurrentSize()));
        System.out.println("================================");
    }
    
    private void reportCacheType(String name, CacheTypeStats stats) {
        System.out.printf("%s Cache: %.2f%% hit rate, %s, %d evictions%n",
            name, stats.getHitRate() * 100, formatBytes(stats.getSizeBytes()), stats.getEvictions());
    }
}

// Usage
CacheMonitor monitor = new CacheMonitor(cacheManager);
monitor.startMonitoring();
```

### Performance Alerts

```java
public class CachePerformanceAlert {
    private static final double LOW_HIT_RATE_THRESHOLD = 0.80; // 80%
    private static final double HIGH_EVICTION_RATE_THRESHOLD = 0.10; // 10%
    
    public void checkCacheHealth(ComprehensiveCacheStats stats) {
        checkCacheType("ByteRangeCache", stats.getByteRangeCache());
        checkCacheType("FooterCache", stats.getFooterCache());
        checkCacheType("FastFieldCache", stats.getFastFieldCache());
        checkCacheType("SplitCache", stats.getSplitCache());
    }
    
    private void checkCacheType(String cacheName, CacheTypeStats stats) {
        double hitRate = stats.getHitRate();
        long total = stats.getHits() + stats.getMisses();
        double evictionRate = total > 0 ? (double) stats.getEvictions() / total : 0;
        
        if (hitRate < LOW_HIT_RATE_THRESHOLD) {
            System.err.printf("ALERT: %s hit rate is low: %.2f%% (threshold: %.2f%%)%n",
                cacheName, hitRate * 100, LOW_HIT_RATE_THRESHOLD * 100);
        }
        
        if (evictionRate > HIGH_EVICTION_RATE_THRESHOLD) {
            System.err.printf("ALERT: %s eviction rate is high: %.2f%% (threshold: %.2f%%)%n",
                cacheName, evictionRate * 100, HIGH_EVICTION_RATE_THRESHOLD * 100);
        }
    }
}
```

## Troubleshooting

### Common Issues and Solutions

#### 1. Low Hit Rates

**Symptom**: Cache hit rate below 80%
```java
if (stats.getByteRangeCache().getHitRate() < 0.80) {
    System.out.println("ByteRangeCache hit rate is low - consider:");
    System.out.println("- Increasing cache size allocation");
    System.out.println("- Analyzing access patterns");
    System.out.println("- Checking for cache thrashing");
}
```

**Solutions**:
- Increase cache size: `config.withMaxCacheSize(larger_size)`
- Analyze query patterns for optimization opportunities
- Check for cache thrashing during large scans

#### 2. High Eviction Rates

**Symptom**: Frequent cache evictions
```java
CacheTypeStats fastFieldStats = detailedStats.getFastFieldCache();
long totalOps = fastFieldStats.getHits() + fastFieldStats.getMisses();
double evictionRate = (double) fastFieldStats.getEvictions() / totalOps;

if (evictionRate > 0.10) { // More than 10% eviction rate
    System.out.println("High eviction rate detected: " + String.format("%.2f%%", evictionRate * 100));
    System.out.println("Consider increasing cache size or reviewing access patterns");
}
```

**Solutions**:
- Increase total cache allocation
- Review temporal access patterns
- Consider workload-specific cache tuning

#### 3. Memory Pressure

**Symptom**: Cache size approaching limits
```java
GlobalCacheStats stats = cacheManager.getGlobalCacheStats();
double utilization = stats.getUtilization();

if (utilization > 0.90) { // More than 90% utilization
    System.out.println("High memory pressure detected: " + String.format("%.2f%% utilization", utilization * 100));
    System.out.println("Consider:");
    System.out.println("- Increasing max cache size");
    System.out.println("- Reviewing cache allocation strategy");
    System.out.println("- Checking for memory leaks");
}
```

### Debug Mode

Enable detailed debugging for cache operations:

```bash
export TANTIVY4JAVA_DEBUG=1
```

This provides detailed logging of cache operations:
```
DEBUG: 📊 Comprehensive Cache Metrics:
DEBUG:   📦 ByteRangeCache: 45678 hits, 5678 misses, 234 evictions, 125000000 bytes
DEBUG:   📄 FooterCache: 12345 hits, 1234 misses, 45 evictions, 50000000 bytes
DEBUG:   ⚡ FastFieldCache: 98765 hits, 9876 misses, 456 evictions, 150000000 bytes
DEBUG:   🔍 SplitCache: 23456 hits, 2345 misses, 123 evictions, 200000000 bytes
DEBUG:   🏆 Total: 180244 hits, 19133 misses, 858 evictions, 525000000 bytes (90% hit rate)
```

## Best Practices

### 1. Regular Monitoring

```java
// Monitor cache performance every 5 minutes in production
ScheduledExecutorService monitor = Executors.newScheduledThreadPool(1);
monitor.scheduleAtFixedRate(() -> {
    ComprehensiveCacheStats stats = cacheManager.getComprehensiveCacheStats();
    
    // Log to your monitoring system
    logMetric("cache.byterage.hit_rate", stats.getByteRangeCache().getHitRate());
    logMetric("cache.footer.hit_rate", stats.getFooterCache().getHitRate());
    logMetric("cache.fastfield.hit_rate", stats.getFastFieldCache().getHitRate());
    logMetric("cache.split.hit_rate", stats.getSplitCache().getHitRate());
    
    // Alert on performance degradation
    if (stats.getAggregated().getHitRate() < 0.85) {
        sendAlert("Cache performance degraded: " + 
                 String.format("%.2f%% hit rate", stats.getAggregated().getHitRate() * 100));
    }
}, 0, 5, TimeUnit.MINUTES);
```

### 2. Cache Size Tuning

```java
// Start with baseline measurements
ComprehensiveCacheStats baseline = cacheManager.getComprehensiveCacheStats();

// Monitor for 1 hour under normal load
Thread.sleep(3600000);

// Collect performance data
ComprehensiveCacheStats afterLoad = cacheManager.getComprehensiveCacheStats();

// Calculate optimal cache sizes based on usage patterns
long byteRangeUsage = afterLoad.getByteRangeCache().getSizeBytes();
long footerUsage = afterLoad.getFooterCache().getSizeBytes();
long fastFieldUsage = afterLoad.getFastFieldCache().getSizeBytes();
long splitUsage = afterLoad.getSplitCache().getSizeBytes();

System.out.println("Recommended cache allocation:");
System.out.println("ByteRange: " + formatBytes(byteRangeUsage * 2)); // 2x current usage
System.out.println("Footer: " + formatBytes(footerUsage * 2));
System.out.println("FastField: " + formatBytes(fastFieldUsage * 2));  
System.out.println("Split: " + formatBytes(splitUsage * 2));
```

### 3. Performance Optimization

```java
public class CacheOptimizer {
    public void optimizeBasedOnStats(ComprehensiveCacheStats stats) {
        // Identify underperforming caches
        Map<String, CacheTypeStats> caches = Map.of(
            "ByteRange", stats.getByteRangeCache(),
            "Footer", stats.getFooterCache(),
            "FastField", stats.getFastFieldCache(),
            "Split", stats.getSplitCache()
        );
        
        caches.forEach((name, cacheStats) -> {
            if (cacheStats.getHitRate() < 0.80) {
                System.out.printf("OPTIMIZATION: %s cache hit rate is %.2f%% - consider tuning%n",
                    name, cacheStats.getHitRate() * 100);
                    
                // Suggest specific optimizations
                suggestOptimizations(name, cacheStats);
            }
        });
    }
    
    private void suggestOptimizations(String cacheName, CacheTypeStats stats) {
        switch (cacheName) {
            case "ByteRange":
                System.out.println("  - Consider larger byte range cache allocation");
                System.out.println("  - Review I/O access patterns for optimization");
                break;
            case "Footer":
                System.out.println("  - Increase split footer cache size");
                System.out.println("  - Review split file access patterns");
                break;
            case "FastField":
                System.out.println("  - Consider more fast field cache allocation");
                System.out.println("  - Review field access patterns in queries");
                break;
            case "Split":
                System.out.println("  - Increase split cache size");
                System.out.println("  - Consider split file partitioning strategy");
                break;
        }
    }
}
```

### 4. Integration with Monitoring Systems

#### Prometheus/Grafana Integration

```java
// Export metrics for Prometheus
public class CacheMetricsExporter {
    private final SplitCacheManager cacheManager;
    
    @Scheduled(fixedRate = 30000) // Every 30 seconds
    public void exportMetrics() {
        ComprehensiveCacheStats stats = cacheManager.getComprehensiveCacheStats();
        
        // Export per-cache-type metrics
        exportCacheMetrics("byterange", stats.getByteRangeCache());
        exportCacheMetrics("footer", stats.getFooterCache());
        exportCacheMetrics("fastfield", stats.getFastFieldCache());
        exportCacheMetrics("split", stats.getSplitCache());
    }
    
    private void exportCacheMetrics(String cacheType, CacheTypeStats stats) {
        Metrics.gauge("cache_hit_rate", Tags.of("cache_type", cacheType)).set(stats.getHitRate());
        Metrics.gauge("cache_size_bytes", Tags.of("cache_type", cacheType)).set(stats.getSizeBytes());
        Metrics.counter("cache_hits_total", Tags.of("cache_type", cacheType)).increment(stats.getHits());
        Metrics.counter("cache_misses_total", Tags.of("cache_type", cacheType)).increment(stats.getMisses());
        Metrics.counter("cache_evictions_total", Tags.of("cache_type", cacheType)).increment(stats.getEvictions());
    }
}
```

## Conclusion

The comprehensive cache statistics system provides powerful insights into cache performance across all components. Use these metrics to:

- **Monitor**: Track real-time cache performance
- **Optimize**: Data-driven cache tuning decisions  
- **Alert**: Early warning system for performance issues
- **Debug**: Detailed diagnostics for troubleshooting

For production deployments, establish baseline measurements, implement continuous monitoring, and set up alerting thresholds based on your specific workload requirements.

---

**Need Help?**
- Check the [troubleshooting section](#troubleshooting) for common issues
- Enable debug mode with `TANTIVY4JAVA_DEBUG=1` for detailed logs
- Review cache allocation in your `CacheConfig` setup
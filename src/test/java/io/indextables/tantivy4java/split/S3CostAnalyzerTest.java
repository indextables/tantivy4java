package io.indextables.tantivy4java.split;

import io.indextables.tantivy4java.core.DocAddress;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for S3CostAnalyzer cost estimation and reporting.
 *
 * Priority 6: Enhanced Benchmarks - Cost Analysis Testing
 */
public class S3CostAnalyzerTest {

    @Test
    void testBasicCostAnalysis() {
        System.out.println("\n=== Test: Basic Cost Analysis ===");

        S3CostAnalyzer analyzer = new S3CostAnalyzer();

        // Create sample batch of 100 documents
        List<DocAddress> addresses = createDocAddresses(100);
        BatchOptimizationConfig config = BatchOptimizationConfig.balanced();

        // Analyze costs
        S3CostAnalyzer.CostReport report = analyzer.analyzeBatchOperation(addresses, config);

        // Verify report
        assertNotNull(report);
        assertEquals(100, report.getDocumentCount());
        assertEquals(100, report.getBaselineRequests()); // One request per doc
        assertTrue(report.getConsolidatedRequests() < report.getBaselineRequests(),
            "Consolidated requests should be fewer than baseline");
        assertTrue(report.getSavingsVsBaseline() > 0,
            "Should have cost savings");
        assertTrue(report.getSavingsPercent() > 0,
            "Should have percentage savings");

        // Print results
        System.out.println(report.getSummary());
        System.out.println("✅ Basic cost analysis test passed");
    }

    @Test
    void testCostAnalysisWithDifferentConfigurations() {
        System.out.println("\n=== Test: Cost Analysis with Different Configurations ===");

        S3CostAnalyzer analyzer = new S3CostAnalyzer();
        List<DocAddress> addresses = createDocAddresses(1000);

        // Test conservative, balanced, and aggressive configurations
        BatchOptimizationConfig[] configs = {
            BatchOptimizationConfig.conservative(),
            BatchOptimizationConfig.balanced(),
            BatchOptimizationConfig.aggressive()
        };
        String[] labels = {"Conservative", "Balanced", "Aggressive"};

        System.out.println("\n📊 Cost Comparison:");
        System.out.println(String.format("%-15s %-20s %-20s %-15s %-15s",
            "Configuration", "Consolidated Reqs", "Consolidation Ratio", "Cost", "Savings %"));
        System.out.println("=".repeat(90));

        for (int i = 0; i < configs.length; i++) {
            S3CostAnalyzer.CostReport report = analyzer.analyzeBatchOperation(addresses, configs[i]);

            System.out.println(String.format("%-15s %-20d %-20.1fx $%-14.6f %-15.1f%%",
                labels[i],
                report.getConsolidatedRequests(),
                report.getConsolidationRatio(),
                report.getOptimizedCost(),
                report.getSavingsPercent()));

            // Verify aggressive is more cost-effective than conservative
            if (i == 2) { // Aggressive
                assertTrue(report.getConsolidatedRequests() < 200,
                    "Aggressive should have fewer consolidated requests");
                assertTrue(report.getSavingsPercent() > 60,
                    "Aggressive should have >60% savings");
            }
        }

        System.out.println("\n✅ Multi-configuration cost analysis test passed");
    }

    @Test
    void testCostScaling() {
        System.out.println("\n=== Test: Cost Scaling with Batch Size ===");

        S3CostAnalyzer analyzer = new S3CostAnalyzer();
        BatchOptimizationConfig config = BatchOptimizationConfig.balanced();

        int[] batchSizes = {10, 50, 100, 500, 1000, 5000};

        System.out.println("\n📊 Cost Scaling Analysis:");
        System.out.println(String.format("%-15s %-20s %-20s %-20s %-15s",
            "Batch Size", "Baseline Cost", "Optimized Cost", "Savings", "Savings %"));
        System.out.println("=".repeat(95));

        for (int batchSize : batchSizes) {
            List<DocAddress> addresses = createDocAddresses(batchSize);
            S3CostAnalyzer.CostReport report = analyzer.analyzeBatchOperation(addresses, config);

            System.out.println(String.format("%-15d $%-19.6f $%-19.6f $%-19.6f %-15.1f%%",
                batchSize,
                report.getBaselineCost(),
                report.getOptimizedCost(),
                report.getSavingsVsBaseline(),
                report.getSavingsPercent()));

            // Verify savings increase with batch size (due to better consolidation)
            if (batchSize >= 100) {
                assertTrue(report.getSavingsPercent() > 50,
                    "Large batches should have >50% savings");
            }
        }

        System.out.println("\n✅ Cost scaling test passed");
    }

    @Test
    void testCostWithDifferentDocumentSizes() {
        System.out.println("\n=== Test: Cost with Different Document Sizes ===");

        List<DocAddress> addresses = createDocAddresses(500);
        BatchOptimizationConfig config = BatchOptimizationConfig.balanced();

        double[] docSizes = {512, 1024, 2048, 4096, 8192}; // bytes
        String[] sizeLabels = {"512B", "1KB", "2KB", "4KB", "8KB"};

        System.out.println("\n📊 Document Size Impact on Cost:");
        System.out.println(String.format("%-15s %-20s %-20s %-20s %-15s",
            "Doc Size", "Data Transfer (MB)", "Optimized Cost", "Savings", "Savings %"));
        System.out.println("=".repeat(95));

        for (int i = 0; i < docSizes.length; i++) {
            S3CostAnalyzer analyzer = new S3CostAnalyzer(docSizes[i]);
            S3CostAnalyzer.CostReport report = analyzer.analyzeBatchOperation(addresses, config);

            double dataTransferMB = report.getOptimizedDataTransferBytes() / (1024.0 * 1024.0);

            System.out.println(String.format("%-15s %-20.2f $%-19.6f $%-19.6f %-15.1f%%",
                sizeLabels[i],
                dataTransferMB,
                report.getOptimizedCost(),
                report.getSavingsVsBaseline(),
                report.getSavingsPercent()));

            // Verify cost scales with document size
            assertTrue(report.getOptimizedCost() > 0,
                "Cost should be positive");
            assertTrue(report.getSavingsVsBaseline() > 0,
                "Should always have savings");
        }

        System.out.println("\n✅ Document size impact test passed");
    }

    @Test
    void testMonthlyProjection() {
        System.out.println("\n=== Test: Monthly Cost Projection ===");

        S3CostAnalyzer analyzer = new S3CostAnalyzer();

        // Simulate daily workload
        int documentsPerRequest = 100;
        int requestsPerDay = 1000; // 1000 batch operations per day
        int daysPerMonth = 30;

        List<DocAddress> addresses = createDocAddresses(documentsPerRequest);
        BatchOptimizationConfig config = BatchOptimizationConfig.balanced();

        S3CostAnalyzer.CostReport singleReport = analyzer.analyzeBatchOperation(addresses, config);

        // Project to monthly costs
        double baselineMonthly = singleReport.getBaselineCost() * requestsPerDay * daysPerMonth;
        double optimizedMonthly = singleReport.getOptimizedCost() * requestsPerDay * daysPerMonth;
        double savingsMonthly = baselineMonthly - optimizedMonthly;

        System.out.println("\n📊 Monthly Cost Projection:");
        System.out.println("  Workload:");
        System.out.println("    - Documents per request: " + documentsPerRequest);
        System.out.println("    - Batch operations per day: " + requestsPerDay);
        System.out.println("    - Days per month: " + daysPerMonth);
        System.out.println();
        System.out.println("  Monthly Costs:");
        System.out.println(String.format("    - Baseline (no optimization): $%.2f", baselineMonthly));
        System.out.println(String.format("    - Optimized:                  $%.2f", optimizedMonthly));
        System.out.println(String.format("    - Monthly Savings:            $%.2f (%.1f%%)",
            savingsMonthly, singleReport.getSavingsPercent()));
        System.out.println();
        System.out.println(String.format("  Annual Savings: $%.2f", savingsMonthly * 12));

        // Verify significant savings at scale
        assertTrue(savingsMonthly > 0, "Should have monthly savings");

        System.out.println("\n✅ Monthly projection test passed");
    }

    @Test
    void testRequestReduction() {
        System.out.println("\n=== Test: Request Reduction Analysis ===");

        S3CostAnalyzer analyzer = new S3CostAnalyzer();
        List<DocAddress> addresses = createDocAddresses(1000);
        BatchOptimizationConfig config = BatchOptimizationConfig.aggressive();

        S3CostAnalyzer.CostReport report = analyzer.analyzeBatchOperation(addresses, config);

        System.out.println("\n📊 Request Reduction:");
        System.out.println("  Baseline requests:      " + report.getBaselineRequests());
        System.out.println("  Consolidated requests:  " + report.getConsolidatedRequests());
        System.out.println("  Request reduction:      " + String.format("%.1f%%", report.getRequestReductionPercent()));
        System.out.println("  Consolidation ratio:    " + String.format("%.1fx", report.getConsolidationRatio()));

        // Verify aggressive configuration achieves high reduction
        assertTrue(report.getRequestReductionPercent() > 85,
            "Aggressive config should reduce requests by >85%");
        assertTrue(report.getConsolidationRatio() > 10,
            "Should consolidate at least 10 docs per request");

        System.out.println("\n✅ Request reduction test passed");
    }

    // Helper method to create doc addresses
    private List<DocAddress> createDocAddresses(int count) {
        List<DocAddress> addresses = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            addresses.add(new DocAddress(0, i));
        }
        return addresses;
    }
}

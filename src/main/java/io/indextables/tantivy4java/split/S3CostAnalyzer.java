package io.indextables.tantivy4java.split;

import io.indextables.tantivy4java.core.DocAddress;

import java.util.List;
import java.util.Objects;

/**
 * Analyzes and estimates S3 costs for batch retrieval operations.
 *
 * <p>This class simulates batch operations and calculates estimated costs
 * based on AWS S3 pricing for GET requests and data transfer.
 *
 * <p>Priority 6: Enhanced Benchmarks - Cost Analysis Component
 *
 * <h3>AWS S3 Pricing (as of 2024):</h3>
 * <ul>
 *   <li>GET Request: $0.0004 per 1,000 requests</li>
 *   <li>Data Transfer: $0.09 per GB (first 10 TB/month)</li>
 *   <li>Range Request: Same as GET request</li>
 * </ul>
 *
 * <h3>Example Usage:</h3>
 * <pre>{@code
 * S3CostAnalyzer analyzer = new S3CostAnalyzer();
 *
 * // Analyze batch operation
 * List<DocAddress> addresses = getDocAddresses();
 * BatchOptimizationConfig config = BatchOptimizationConfig.balanced();
 * CostReport report = analyzer.analyzeBatchOperation(addresses, config);
 *
 * // Print results
 * System.out.println("Estimated Cost: $" + report.getEstimatedCost());
 * System.out.println("Cost Savings: $" + report.getSavingsVsBaseline());
 * System.out.println("Savings Percentage: " + report.getSavingsPercent() + "%");
 * }</pre>
 */
public class S3CostAnalyzer {

    // AWS S3 Standard pricing (US East, as of 2024)
    private static final double COST_PER_1000_GET_REQUESTS = 0.0004; // $0.0004 per 1,000 requests
    private static final double COST_PER_GB_TRANSFER = 0.09;         // $0.09 per GB

    // Average document size estimates (configurable)
    private double avgDocumentSizeBytes = 2048; // Default: 2KB per document

    /**
     * Creates a new S3CostAnalyzer with default document size (2KB).
     */
    public S3CostAnalyzer() {
    }

    /**
     * Creates a new S3CostAnalyzer with specified average document size.
     *
     * @param avgDocumentSizeBytes average size of documents in bytes
     */
    public S3CostAnalyzer(double avgDocumentSizeBytes) {
        this.avgDocumentSizeBytes = avgDocumentSizeBytes;
    }

    /**
     * Sets the average document size for cost calculations.
     *
     * @param avgDocumentSizeBytes average document size in bytes
     */
    public void setAvgDocumentSizeBytes(double avgDocumentSizeBytes) {
        this.avgDocumentSizeBytes = avgDocumentSizeBytes;
    }

    /**
     * Analyzes a batch operation and calculates estimated costs.
     *
     * <p>This method simulates the batch operation using the provided
     * configuration and estimates both the optimized cost and the baseline
     * cost (without optimization).
     *
     * @param addresses list of document addresses to retrieve
     * @param config batch optimization configuration
     * @return detailed cost report
     */
    public CostReport analyzeBatchOperation(
            List<DocAddress> addresses,
            BatchOptimizationConfig config) {

        Objects.requireNonNull(addresses, "addresses cannot be null");
        Objects.requireNonNull(config, "config cannot be null");

        int documentCount = addresses.size();

        // Baseline: one request per document (no optimization)
        long baselineRequests = documentCount;
        double baselineDataTransferBytes = documentCount * avgDocumentSizeBytes;

        // Optimized: estimate consolidated requests
        // Conservative estimate: 10-20x consolidation for typical workloads
        // Actual ratio depends on document locality and configuration
        double consolidationRatio = estimateConsolidationRatio(documentCount, config);
        long consolidatedRequests = Math.max(1, (long) (documentCount / consolidationRatio));

        // With optimization, we may fetch some extra data due to gaps
        // Estimate waste factor based on gap tolerance
        double wasteFactor = estimateWasteFactor(config);
        double optimizedDataTransferBytes = baselineDataTransferBytes * (1.0 + wasteFactor);

        // Calculate costs
        double baselineCost = calculateCost(baselineRequests, baselineDataTransferBytes);
        double optimizedCost = calculateCost(consolidatedRequests, optimizedDataTransferBytes);
        double savings = baselineCost - optimizedCost;
        double savingsPercent = (savings / baselineCost) * 100.0;

        return new CostReport(
            documentCount,
            baselineRequests,
            consolidatedRequests,
            baselineDataTransferBytes,
            optimizedDataTransferBytes,
            baselineCost,
            optimizedCost,
            savings,
            savingsPercent,
            consolidationRatio
        );
    }

    /**
     * Estimates the consolidation ratio based on document count and configuration.
     *
     * <p>Consolidation ratio is affected by:
     * <ul>
     *   <li>Document locality (sequential vs random access)</li>
     *   <li>Gap tolerance (larger = more consolidation)</li>
     *   <li>Max range size (limits consolidation)</li>
     * </ul>
     *
     * @param documentCount number of documents
     * @param config optimization configuration
     * @return estimated consolidation ratio (e.g., 10.0 means 10 docs per request)
     */
    private double estimateConsolidationRatio(int documentCount, BatchOptimizationConfig config) {
        // Base consolidation ratio (for sequential access)
        double baseRatio = 10.0;

        // Larger gap tolerance → better consolidation
        double gapFactor = Math.log(config.getGapTolerance() / 128_000.0) + 1.0; // Normalized to 128KB baseline
        gapFactor = Math.max(0.5, Math.min(2.0, gapFactor)); // Clamp to 0.5x - 2.0x

        // Larger max range size → better consolidation (up to a point)
        double rangeFactor = Math.log(config.getMaxRangeSize() / 4_000_000.0) + 1.0; // Normalized to 4MB baseline
        rangeFactor = Math.max(0.5, Math.min(1.5, rangeFactor)); // Clamp to 0.5x - 1.5x

        // More documents → better consolidation (within limits)
        double docFactor = Math.log(documentCount / 100.0) / 5.0 + 1.0; // Logarithmic scale
        docFactor = Math.max(0.8, Math.min(1.5, docFactor)); // Clamp to 0.8x - 1.5x

        double estimatedRatio = baseRatio * gapFactor * rangeFactor * docFactor;

        // Realistic bounds: 3x (poor consolidation) to 50x (excellent consolidation)
        return Math.max(3.0, Math.min(50.0, estimatedRatio));
    }

    /**
     * Estimates the waste factor (extra data transferred due to gaps).
     *
     * @param config optimization configuration
     * @return waste factor (0.0 = no waste, 1.0 = 100% extra data)
     */
    private double estimateWasteFactor(BatchOptimizationConfig config) {
        // Larger gap tolerance → more waste
        // Conservative estimate: 5-15% waste for typical configurations
        double gapToleranceKB = config.getGapTolerance() / 1024.0;

        if (gapToleranceKB <= 128) {
            return 0.05; // 5% waste
        } else if (gapToleranceKB <= 512) {
            return 0.10; // 10% waste
        } else {
            return 0.15; // 15% waste
        }
    }

    /**
     * Calculates S3 cost based on requests and data transfer.
     *
     * @param requests number of GET requests
     * @param dataTransferBytes bytes transferred
     * @return estimated cost in USD
     */
    private double calculateCost(long requests, double dataTransferBytes) {
        double requestCost = (requests / 1000.0) * COST_PER_1000_GET_REQUESTS;
        double transferCost = (dataTransferBytes / (1024.0 * 1024.0 * 1024.0)) * COST_PER_GB_TRANSFER;
        return requestCost + transferCost;
    }

    /**
     * Detailed cost report for a batch operation.
     */
    public static class CostReport {
        private final int documentCount;
        private final long baselineRequests;
        private final long consolidatedRequests;
        private final double baselineDataTransferBytes;
        private final double optimizedDataTransferBytes;
        private final double baselineCost;
        private final double optimizedCost;
        private final double savings;
        private final double savingsPercent;
        private final double consolidationRatio;

        CostReport(int documentCount,
                   long baselineRequests,
                   long consolidatedRequests,
                   double baselineDataTransferBytes,
                   double optimizedDataTransferBytes,
                   double baselineCost,
                   double optimizedCost,
                   double savings,
                   double savingsPercent,
                   double consolidationRatio) {
            this.documentCount = documentCount;
            this.baselineRequests = baselineRequests;
            this.consolidatedRequests = consolidatedRequests;
            this.baselineDataTransferBytes = baselineDataTransferBytes;
            this.optimizedDataTransferBytes = optimizedDataTransferBytes;
            this.baselineCost = baselineCost;
            this.optimizedCost = optimizedCost;
            this.savings = savings;
            this.savingsPercent = savingsPercent;
            this.consolidationRatio = consolidationRatio;
        }

        /** Number of documents analyzed */
        public int getDocumentCount() { return documentCount; }

        /** Baseline S3 GET requests (one per document) */
        public long getBaselineRequests() { return baselineRequests; }

        /** Consolidated S3 GET requests (with optimization) */
        public long getConsolidatedRequests() { return consolidatedRequests; }

        /** Baseline data transfer in bytes */
        public double getBaselineDataTransferBytes() { return baselineDataTransferBytes; }

        /** Optimized data transfer in bytes (may include waste) */
        public double getOptimizedDataTransferBytes() { return optimizedDataTransferBytes; }

        /** Estimated baseline cost in USD */
        public double getBaselineCost() { return baselineCost; }

        /** Estimated optimized cost in USD */
        public double getOptimizedCost() { return optimizedCost; }

        /** Estimated cost savings in USD */
        public double getSavingsVsBaseline() { return savings; }

        /** Estimated cost savings as percentage */
        public double getSavingsPercent() { return savingsPercent; }

        /** Estimated consolidation ratio (docs per request) */
        public double getConsolidationRatio() { return consolidationRatio; }

        /** Request reduction percentage */
        public double getRequestReductionPercent() {
            return ((baselineRequests - consolidatedRequests) / (double) baselineRequests) * 100.0;
        }

        /**
         * Returns a formatted summary of the cost report.
         */
        public String getSummary() {
            return String.format(
                "Cost Analysis Report\n" +
                "===================\n" +
                "Documents:              %,d\n" +
                "Baseline Requests:      %,d\n" +
                "Consolidated Requests:  %,d\n" +
                "Request Reduction:      %.1f%%\n" +
                "Consolidation Ratio:    %.1fx\n" +
                "\n" +
                "Data Transfer:\n" +
                "  Baseline:             %.2f MB\n" +
                "  Optimized:            %.2f MB\n" +
                "\n" +
                "Cost Estimate:\n" +
                "  Baseline:             $%.6f\n" +
                "  Optimized:            $%.6f\n" +
                "  Savings:              $%.6f (%.1f%%)\n",
                documentCount,
                baselineRequests,
                consolidatedRequests,
                getRequestReductionPercent(),
                consolidationRatio,
                baselineDataTransferBytes / (1024.0 * 1024.0),
                optimizedDataTransferBytes / (1024.0 * 1024.0),
                baselineCost,
                optimizedCost,
                savings,
                savingsPercent
            );
        }

        @Override
        public String toString() {
            return getSummary();
        }
    }
}

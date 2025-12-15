/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.indextables.tantivy4java.xref;

import io.indextables.tantivy4java.core.Tantivy;
import io.indextables.tantivy4java.split.SplitCacheManager;
import io.indextables.tantivy4java.split.merge.QuickwitSplit;

import java.util.List;

/**
 * Cross-Reference Split functionality for query routing and split skipping.
 *
 * <p>An XRef split is a lightweight index that consolidates term dictionaries from
 * multiple source splits. Each document in the XRef split represents one source split,
 * enabling fast query routing to determine which splits contain matching documents.</p>
 *
 * <h2>Key Use Case</h2>
 * <p>Query routing and split skipping - quickly determine which splits from a large
 * collection need to be searched, without loading each split's full index.</p>
 *
 * <h2>Design Principle</h2>
 * <p>If the XRef references 1,000 source splits, the XRef split contains exactly
 * 1,000 documents - one per source split. The posting lists point to these split-level
 * documents, NOT to the original documents.</p>
 *
 * <h2>Important: Footer Offsets Required</h2>
 * <p>Source splits must include footer offsets from their SplitMetadata. This follows
 * the same pattern as other split operations in tantivy4java - the caller is responsible
 * for maintaining split metadata after creation and passing it to subsequent operations.</p>
 *
 * <h2>Example Usage</h2>
 * <pre>{@code
 * // Step 1: Create splits and retain their metadata
 * QuickwitSplit.SplitMetadata meta1 = QuickwitSplit.convertIndexFromPath(
 *     index1Path, split1Path, splitConfig);
 * QuickwitSplit.SplitMetadata meta2 = QuickwitSplit.convertIndexFromPath(
 *     index2Path, split2Path, splitConfig);
 *
 * // Step 2: Create XRefSourceSplit objects from the metadata
 * // This is the recommended approach - fromSplitMetadata() extracts footer offsets
 * XRefSourceSplit source1 = XRefSourceSplit.fromSplitMetadata(
 *     "s3://bucket/splits/split-001.split", meta1);
 * XRefSourceSplit source2 = XRefSourceSplit.fromSplitMetadata(
 *     "s3://bucket/splits/split-002.split", meta2);
 *
 * // Step 3: Build XRef config with source splits containing footer offsets
 * XRefBuildConfig config = XRefBuildConfig.builder()
 *     .xrefId("daily-index-xref-2024-01-15")
 *     .indexUid("logs-index")
 *     .sourceSplits(Arrays.asList(source1, source2))
 *     .awsConfig(new QuickwitSplit.AwsConfig("access-key", "secret-key", "us-east-1"))
 *     .includePositions(false)  // Skip positions for faster build
 *     .build();
 *
 * XRefMetadata metadata = XRefSplit.build(config, "s3://bucket/xref/daily-xref.xref.split");
 * System.out.println("Built XRef split with " + metadata.getTotalTerms() + " terms");
 *
 * // Searching an XRef split (uses standard SplitSearcher infrastructure)
 * SplitCacheManager.CacheConfig cacheConfig = new SplitCacheManager.CacheConfig("xref-cache")
 *     .withMaxCacheSize(100_000_000)
 *     .withAwsCredentials("access-key", "secret-key")
 *     .withAwsRegion("us-east-1");
 *
 * try (SplitCacheManager cacheManager = SplitCacheManager.getInstance(cacheConfig);
 *      XRefSearcher searcher = XRefSearcher.open(cacheManager, "s3://bucket/xref/daily-xref.xref.split", metadata)) {
 *
 *     XRefSearchResult result = searcher.search("error AND level:critical", 1000);
 *
 *     System.out.println("Query matches documents across " +
 *         result.getNumMatchingSplits() + " splits");
 *
 *     // Only search the relevant splits
 *     List<String> splitsToSearch = result.getSplitUrisToSearch();
 *     for (String splitUri : splitsToSearch) {
 *         try (SplitSearcher splitSearcher = cacheManager.createSplitSearcher(splitUri)) {
 *             // Perform full search on this split
 *             SearchResult fullResult = splitSearcher.search(query, 100);
 *         }
 *     }
 * }
 * }</pre>
 *
 * <h2>Performance Benefits</h2>
 * <ul>
 *   <li><b>10-100x faster split identification</b> for selective queries</li>
 *   <li><b>Reduced S3/storage requests</b> - single XRef split vs N source splits</li>
 *   <li><b>Bounded size</b> - XRef with 10,000 source splits = 10,000 documents (not billions)</li>
 *   <li><b>Sub-millisecond query routing</b> - searching 10,000 docs is instant</li>
 * </ul>
 *
 * @see XRefBuildConfig
 * @see XRefSourceSplit
 * @see XRefSearcher
 * @see XRefMetadata
 */
public class XRefSplit {
    static {
        Tantivy.initialize();
    }

    // Prevent instantiation
    private XRefSplit() {}

    /**
     * Build an XRef split from multiple source splits.
     *
     * @param config Build configuration containing source splits and options
     * @param outputPath Output path for the XRef split file (local path or URI)
     * @return XRefMetadata containing build statistics and split information
     * @throws RuntimeException if the build fails
     */
    public static XRefMetadata build(XRefBuildConfig config, String outputPath) {
        // Extract AWS config if present
        QuickwitSplit.AwsConfig awsConfig = config.getAwsConfig();
        String awsAccessKey = null;
        String awsSecretKey = null;
        String awsSessionToken = null;
        String awsRegion = null;
        String awsEndpointUrl = null;
        boolean awsForcePathStyle = false;

        if (awsConfig != null) {
            awsAccessKey = awsConfig.getAccessKey();
            awsSecretKey = awsConfig.getSecretKey();
            awsSessionToken = awsConfig.getSessionToken();
            awsRegion = awsConfig.getRegion();
            awsEndpointUrl = awsConfig.getEndpoint();
            awsForcePathStyle = awsConfig.isForcePathStyle();
        }

        // Extract Azure config if present
        QuickwitSplit.AzureConfig azureConfig = config.getAzureConfig();
        String azureAccountName = null;
        String azureAccountKey = null;
        String azureBearerToken = null;
        String azureEndpointUrl = null;

        if (azureConfig != null) {
            azureAccountName = azureConfig.getAccountName();
            azureAccountKey = azureConfig.getAccountKey();
            azureBearerToken = azureConfig.getBearerToken();
            // Azure endpoint URL not currently exposed in config
        }

        // Convert source splits to array
        List<XRefSourceSplit> sourceSplits = config.getSourceSplits();
        XRefSourceSplit[] sourceSplitsArray = sourceSplits.toArray(new XRefSourceSplit[0]);

        // Call native method with XRefSourceSplit objects (containing footer offsets)
        String result = nativeBuildXRefSplit(
            config.getXrefId(),
            config.getIndexUid(),
            sourceSplitsArray,
            outputPath,
            config.isIncludePositions(),
            config.getIncludedFields().isEmpty()
                ? null
                : config.getIncludedFields().toArray(new String[0]),
            awsAccessKey,
            awsSecretKey,
            awsSessionToken,
            awsRegion,
            awsEndpointUrl,
            awsForcePathStyle,
            azureAccountName,
            azureAccountKey,
            azureBearerToken,
            azureEndpointUrl
        );

        // Check for error
        if (result.startsWith("ERROR:")) {
            throw new RuntimeException(result.substring(7).trim());
        }

        // Parse metadata
        try {
            return XRefMetadata.fromJson(result);
        } catch (Exception e) {
            throw new RuntimeException("Failed to parse XRef metadata: " + e.getMessage(), e);
        }
    }

    /**
     * Open an XRef split for searching.
     *
     * <p>This method requires a {@link SplitCacheManager} and the XRef metadata
     * returned from {@link #build(XRefBuildConfig, String)}. The metadata contains
     * footer offsets needed for efficient SplitSearcher initialization.</p>
     *
     * @param cacheManager The cache manager to use
     * @param xrefUri URI of the XRef split file
     * @param metadata XRef metadata from the build process (must have footer offsets)
     * @return XRefSearcher for querying the XRef split
     * @throws IllegalArgumentException if metadata is null or lacks footer offsets
     * @throws RuntimeException if opening fails
     */
    public static XRefSearcher open(SplitCacheManager cacheManager, String xrefUri, XRefMetadata metadata) {
        return XRefSearcher.open(cacheManager, xrefUri, metadata);
    }

    /**
     * Native method for building XRef split.
     *
     * <p>The sourceSplits parameter is an array of XRefSourceSplit objects, each containing:
     * <ul>
     *   <li>uri (String): Split URI</li>
     *   <li>splitId (String): Split identifier</li>
     *   <li>footerStart (long): Footer start offset for efficient split opening</li>
     *   <li>footerEnd (long): Footer end offset for efficient split opening</li>
     *   <li>numDocs (Long): Optional document count</li>
     *   <li>sizeBytes (Long): Optional size in bytes</li>
     *   <li>docMappingJson (String): Optional doc mapping JSON for schema derivation</li>
     * </ul>
     */
    private static native String nativeBuildXRefSplit(
        String xrefId,
        String indexUid,
        XRefSourceSplit[] sourceSplits,
        String outputPath,
        boolean includePositions,
        String[] includedFields,
        String awsAccessKey,
        String awsSecretKey,
        String awsSessionToken,
        String awsRegion,
        String awsEndpointUrl,
        boolean awsForcePathStyle,
        String azureAccountName,
        String azureAccountKey,
        String azureBearerToken,
        String azureEndpointUrl
    );
}

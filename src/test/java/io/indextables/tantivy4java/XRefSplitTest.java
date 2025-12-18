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

package io.indextables.tantivy4java;

import io.indextables.tantivy4java.core.*;
import io.indextables.tantivy4java.split.SplitCacheManager;
import io.indextables.tantivy4java.split.merge.QuickwitSplit;
import io.indextables.tantivy4java.xref.*;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for XRef (Cross-Reference) Split functionality.
 */
public class XRefSplitTest {

    @TempDir
    Path tempDir;

    @Test
    void testXRefBuildConfigBuilder() {
        // Test the builder pattern with XRefSourceSplit objects
        XRefSourceSplit source1 = new XRefSourceSplit("split1.split", "split1", 0, 1000);
        XRefSourceSplit source2 = new XRefSourceSplit("split2.split", "split2", 0, 2000);

        XRefBuildConfig config = XRefBuildConfig.builder()
            .xrefId("test-xref")
            .indexUid("test-index")
            .sourceSplits(Arrays.asList(source1, source2))
            .includedFields(Arrays.asList("title", "content"))
            .build();

        assertEquals("test-xref", config.getXrefId());
        assertEquals("test-index", config.getIndexUid());
        assertEquals(2, config.getSourceSplits().size());
        assertEquals(2, config.getIncludedFields().size());
    }

    @Test
    void testXRefBuildConfigWithAwsConfig() {
        QuickwitSplit.AwsConfig awsConfig = new QuickwitSplit.AwsConfig(
            "access-key", "secret-key", "us-east-1");

        XRefSourceSplit source = new XRefSourceSplit("s3://bucket/split1.split", "split1", 0, 1000);

        XRefBuildConfig config = XRefBuildConfig.builder()
            .xrefId("test-xref")
            .indexUid("test-index")
            .sourceSplits(Arrays.asList(source))
            .awsConfig(awsConfig)
            .build();

        assertNotNull(config.getAwsConfig());
        assertEquals("us-east-1", config.getAwsConfig().getRegion());
    }

    @Test
    void testXRefBuildConfigWithAzureConfig() {
        QuickwitSplit.AzureConfig azureConfig = new QuickwitSplit.AzureConfig(
            "account-name", "account-key");

        XRefSourceSplit source = new XRefSourceSplit("azure://container/split1.split", "split1", 0, 1000);

        XRefBuildConfig config = XRefBuildConfig.builder()
            .xrefId("test-xref")
            .indexUid("test-index")
            .sourceSplits(Arrays.asList(source))
            .azureConfig(azureConfig)
            .build();

        assertNotNull(config.getAzureConfig());
        assertEquals("account-name", config.getAzureConfig().getAccountName());
    }

    @Test
    void testXRefMetadataDeserialization() throws Exception {
        // Test that XRefMetadata can parse JSON
        String json = "{" +
            "\"format_version\": 1," +
            "\"xref_id\": \"test-xref\"," +
            "\"index_uid\": \"test-index\"," +
            "\"split_registry\": {" +
            "    \"splits\": [" +
            "        {" +
            "            \"uri\": \"s3://bucket/split1.split\"," +
            "            \"split_id\": \"split1\"," +
            "            \"num_docs\": 1000," +
            "            \"footer_start\": 0," +
            "            \"footer_end\": 100," +
            "            \"size_bytes\": 50000" +
            "        }" +
            "    ]" +
            "}," +
            "\"fields\": []," +
            "\"total_terms\": 5000," +
            "\"build_stats\": {" +
            "    \"build_duration_ms\": 1000," +
            "    \"bytes_read\": 100000," +
            "    \"output_size_bytes\": 10000," +
            "    \"compression_ratio\": 0.1," +
            "    \"splits_processed\": 1," +
            "    \"splits_skipped\": 0," +
            "    \"unique_terms\": 5000" +
            "}," +
            "\"created_at\": 1700000000," +
            "\"footer_start_offset\": 9000," +
            "\"footer_end_offset\": 10000" +
            "}";

        XRefMetadata metadata = XRefMetadata.fromJson(json);

        assertEquals(1, metadata.getFormatVersion());
        assertEquals("test-xref", metadata.getXrefId());
        assertEquals("test-index", metadata.getIndexUid());
        assertEquals(1, metadata.getNumSplits());
        assertEquals(1000, metadata.getTotalSourceDocs());
        assertEquals(5000, metadata.getTotalTerms());
        assertTrue(metadata.hasFooterOffsets());
        assertEquals(9000, metadata.getFooterStartOffset());
        assertEquals(10000, metadata.getFooterEndOffset());
    }

    @Test
    void testXRefSearchResultSetters() {
        XRefSearchResult result = new XRefSearchResult();

        MatchingSplit split1 = new MatchingSplit();
        split1.setUri("s3://bucket/split1.split");
        split1.setSplitId("split1");
        split1.setScore(1.5f);

        MatchingSplit split2 = new MatchingSplit();
        split2.setUri("s3://bucket/split2.split");
        split2.setSplitId("split2");
        split2.setScore(0.8f);

        result.setMatchingSplits(Arrays.asList(split1, split2));
        result.setNumMatchingSplits(2);
        result.setSearchTimeMs(50);

        assertEquals(2, result.getNumMatchingSplits());
        assertEquals(50, result.getSearchTimeMs());
        assertTrue(result.hasMatches());

        List<String> uris = result.getSplitUrisToSearch();
        assertEquals(2, uris.size());
        assertTrue(uris.contains("s3://bucket/split1.split"));
        assertTrue(uris.contains("s3://bucket/split2.split"));
    }

    @Test
    void testMatchingSplitSetters() {
        MatchingSplit split = new MatchingSplit();

        split.setUri("s3://bucket/test.split");
        split.setSplitId("test-id");
        split.setNumDocs(1000);
        split.setFooterStart(5000);
        split.setFooterEnd(6000);
        split.setTimeRangeStart(1000000L);
        split.setTimeRangeEnd(2000000L);
        split.setSizeBytes(50000);
        split.setScore(2.5f);

        assertEquals("s3://bucket/test.split", split.getUri());
        assertEquals("test-id", split.getSplitId());
        assertEquals(1000, split.getNumDocs());
        assertEquals(5000, split.getFooterStart());
        assertEquals(6000, split.getFooterEnd());
        assertTrue(split.getTimeRangeStart().isPresent());
        assertEquals(1000000L, split.getTimeRangeStart().get().longValue());
        assertTrue(split.getTimeRangeEnd().isPresent());
        assertEquals(2000000L, split.getTimeRangeEnd().get().longValue());
        assertEquals(50000, split.getSizeBytes());
        assertEquals(2.5f, split.getScore(), 0.01f);
    }

    @Test
    void testXRefSearcherRequiresMetadata() {
        // Test that opening without metadata throws an error
        SplitCacheManager.CacheConfig cacheConfig = new SplitCacheManager.CacheConfig("test-cache")
            .withMaxCacheSize(100_000_000);

        try (SplitCacheManager cacheManager = SplitCacheManager.getInstance(cacheConfig)) {
            // Should throw IllegalArgumentException when XRefMetadata is null
            assertThrows(IllegalArgumentException.class, () -> {
                XRefSearcher.open(cacheManager, "file:///nonexistent.split", (XRefMetadata) null);
            });
        }
    }

    @Test
    void testXRefSearcherHandlesNonexistentFile() throws Exception {
        // Test that opening a nonexistent XRef file throws a RuntimeException
        // NOTE: FuseXRef doesn't require footer offsets like the old Tantivy-based XRef.
        // It loads the entire file directly, so the error is about the file not existing.
        String jsonWithoutOffsets = "{" +
            "\"format_version\": 1," +
            "\"xref_id\": \"test-xref\"," +
            "\"index_uid\": \"test-index\"," +
            "\"split_registry\": { \"splits\": [] }," +
            "\"fields\": []," +
            "\"total_terms\": 0," +
            "\"build_stats\": {" +
            "    \"build_duration_ms\": 0," +
            "    \"bytes_read\": 0," +
            "    \"output_size_bytes\": 0," +
            "    \"compression_ratio\": 0," +
            "    \"splits_processed\": 0," +
            "    \"splits_skipped\": 0," +
            "    \"unique_terms\": 0" +
            "}," +
            "\"created_at\": 0," +
            "\"footer_start_offset\": 0," +
            "\"footer_end_offset\": 0" +
            "}";

        XRefMetadata metadataWithoutOffsets = XRefMetadata.fromJson(jsonWithoutOffsets);
        // FuseXRef doesn't use footer offsets, but we can still check the hasFooterOffsets method
        assertFalse(metadataWithoutOffsets.hasFooterOffsets());

        SplitCacheManager.CacheConfig cacheConfig = new SplitCacheManager.CacheConfig("test-cache2")
            .withMaxCacheSize(100_000_000);

        try (SplitCacheManager cacheManager = SplitCacheManager.getInstance(cacheConfig)) {
            // Should throw RuntimeException when opening a nonexistent file
            // FuseXRef loads the entire file, so no footer offsets validation is done
            assertThrows(RuntimeException.class, () -> {
                XRefSearcher.open(cacheManager, "file:///nonexistent.split", metadataWithoutOffsets);
            });
        }
    }
}

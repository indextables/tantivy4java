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

import io.indextables.tantivy4java.split.SplitCacheManager;
import io.indextables.tantivy4java.split.merge.QuickwitSplit;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Basic tests for Azure configuration classes (no native code required).
 *
 * <p>These tests verify the Java-level Azure API works correctly before
 * implementing the native Rust layer.
 */
public class AzureConfigBasicTest {

    @Test
    @DisplayName("Test AzureConfig creation with account name and key")
    public void testAzureConfigBasic() {
        QuickwitSplit.AzureConfig config = new QuickwitSplit.AzureConfig(
            "myaccount", "myaccesskey"
        );

        assertEquals("myaccount", config.getAccountName());
        assertEquals("myaccesskey", config.getAccountKey());
        assertNull(config.getConnectionString());
        assertFalse(config.usesConnectionString());
    }

    @Test
    @DisplayName("Test AzureConfig with custom endpoint")
    public void testAzureConfigWithEndpoint() {
        QuickwitSplit.AzureConfig config = new QuickwitSplit.AzureConfig(
            "testaccount", "testkey");

        assertEquals("testaccount", config.getAccountName());
        assertEquals("testkey", config.getAccountKey());
        assertFalse(config.usesConnectionString());
    }

    @Test
    @DisplayName("Test AzureConfig from connection string")
    public void testAzureConfigFromConnectionString() {
        String connectionString = "DefaultEndpointsProtocol=https;" +
            "AccountName=myaccount;" +
            "AccountKey=mykey;" +
            "EndpointSuffix=core.windows.net";

        QuickwitSplit.AzureConfig config = QuickwitSplit.AzureConfig.fromConnectionString(connectionString);

        assertEquals("myaccount", config.getAccountName());
        assertNull(config.getAccountKey());
        assertEquals(connectionString, config.getConnectionString());
        assertTrue(config.usesConnectionString());
    }

    @Test
    @DisplayName("Test MergeConfig builder with Azure config")
    public void testMergeConfigWithAzure() {
        QuickwitSplit.AzureConfig azureConfig = new QuickwitSplit.AzureConfig(
            "account", "key"
        );

        QuickwitSplit.MergeConfig config = QuickwitSplit.MergeConfig.builder()
            .indexUid("test-index")
            .sourceId("test-source")
            .nodeId("test-node")
            .azureConfig(azureConfig)
            .build();

        assertNotNull(config.getAzureConfig());
        assertEquals("account", config.getAzureConfig().getAccountName());
        assertEquals("test-index", config.getIndexUid());
    }

    @Test
    @DisplayName("Test MergeConfig builder with both AWS and Azure")
    public void testMergeConfigMultiCloud() {
        QuickwitSplit.AwsConfig awsConfig = new QuickwitSplit.AwsConfig(
            "aws-access",
            "aws-secret",
            "us-east-1"
        );

        QuickwitSplit.AzureConfig azureConfig = new QuickwitSplit.AzureConfig(
            "azure-account", "azure-key"
        );

        QuickwitSplit.MergeConfig config = QuickwitSplit.MergeConfig.builder()
            .indexUid("multi-cloud-index")
            .sourceId("multi-source")
            .nodeId("multi-node")
            .awsConfig(awsConfig)
            .azureConfig(azureConfig)
            .build();

        assertNotNull(config.getAwsConfig());
        assertNotNull(config.getAzureConfig());
        assertEquals("aws-access", config.getAwsConfig().getAccessKey());
        assertEquals("azure-account", config.getAzureConfig().getAccountName());
    }

    @Test
    @DisplayName("Test SplitCacheManager.CacheConfig with Azure credentials")
    public void testCacheConfigWithAzure() {
        SplitCacheManager.CacheConfig config = new SplitCacheManager.CacheConfig("azure-cache")
            .withMaxCacheSize(100_000_000)
            .withAzureCredentials("account", "key");

        assertEquals("azure-cache", config.getCacheName());
        assertEquals(100_000_000, config.getMaxCacheSize());
        assertEquals("account", config.getAzureConfig().get("account_name"));
        assertEquals("key", config.getAzureConfig().get("access_key"));  // Changed from account_key to access_key
    }

    @Test
    @DisplayName("Test SplitCacheManager.CacheConfig with Azure connection string")
    public void testCacheConfigWithConnectionString() {
        String connectionString = "DefaultEndpointsProtocol=https;AccountName=test;AccountKey=key";

        SplitCacheManager.CacheConfig config = new SplitCacheManager.CacheConfig("azure-cache")
            .withAzureConnectionString(connectionString);

        assertEquals(connectionString, config.getAzureConfig().get("connection_string"));
    }

    @Test
    @DisplayName("Test builder validation - missing required fields")
    public void testBuilderValidation() {
        assertThrows(IllegalArgumentException.class, () -> {
            QuickwitSplit.MergeConfig.builder().build();
        });

        assertThrows(IllegalArgumentException.class, () -> {
            QuickwitSplit.MergeConfig.builder()
                .indexUid("test")
                .build();
        });

        assertThrows(IllegalArgumentException.class, () -> {
            QuickwitSplit.MergeConfig.builder()
                .indexUid("test")
                .sourceId("source")
                .build();
        });
    }

    @Test
    @DisplayName("Test backward compatibility constructors")
    public void testBackwardCompatibility() {
        // Old constructor should still work
        @SuppressWarnings("deprecation")
        QuickwitSplit.MergeConfig config1 = new QuickwitSplit.MergeConfig(
            "index", "source", "node"
        );

        assertEquals("index", config1.getIndexUid());
        assertEquals("source", config1.getSourceId());
        assertEquals("node", config1.getNodeId());

        // Old constructor with AWS should still work
        QuickwitSplit.AwsConfig awsConfig = new QuickwitSplit.AwsConfig(
            "access", "secret", "us-east-1"
        );

        @SuppressWarnings("deprecation")
        QuickwitSplit.MergeConfig config2 = new QuickwitSplit.MergeConfig(
            "index", "source", "node", awsConfig
        );

        assertNotNull(config2.getAwsConfig());
    }
}

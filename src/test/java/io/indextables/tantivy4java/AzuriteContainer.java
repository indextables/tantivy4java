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

import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.BlobServiceClientBuilder;
import com.azure.storage.common.StorageSharedKeyCredential;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.LogMessageWaitStrategy;

/**
 * Testcontainers wrapper for Azurite emulator.
 *
 * <p>This class provides a Docker-based Azure Blob Storage emulator for integration testing.
 * Based on Apache Iceberg's AzuriteContainer implementation.
 *
 * <p>Key features:
 * <ul>
 *   <li>Automatic Docker image management</li>
 *   <li>Dynamic port allocation (no conflicts in parallel tests)</li>
 *   <li>Wait strategies (ensures Azurite is ready before tests run)</li>
 *   <li>Automatic cleanup (container stops after tests)</li>
 * </ul>
 *
 * <h3>Usage Example:</h3>
 * <pre>{@code
 * AzuriteContainer azurite = new AzuriteContainer();
 * azurite.start();
 *
 * // Create container
 * azurite.createStorageContainer();
 *
 * // Get client for uploads
 * BlobContainerClient client = azurite.containerClient();
 * client.getBlobClient("test.txt").upload(...);
 *
 * // Get Azure URL for SplitSearcher (container only, split filename from metadata)
 * String containerUrl = azurite.azureContainerUrl();
 *
 * // Get Azure URL for merge operations (includes blob path)
 * String blobUrl = azurite.azureUrl("splits/split-1.split");
 *
 * // Cleanup
 * azurite.deleteStorageContainer();
 * azurite.stop();
 * }</pre>
 *
 * @see AzuriteTestBase for base test class using this container
 */
public class AzuriteContainer extends GenericContainer<AzuriteContainer> {

    private static final int DEFAULT_PORT = 10000; // default blob service port
    private static final String DEFAULT_IMAGE = "mcr.microsoft.com/azure-storage/azurite";
    private static final String DEFAULT_TAG = "3.35.0";
    private static final String LOG_WAIT_REGEX =
        "Azurite Blob service is successfully listening at .*";

    /** Default Azurite account name (hardcoded in Azurite) */
    public static final String ACCOUNT = "devstoreaccount1";

    /** Default Azurite account key (hardcoded in Azurite) */
    public static final String KEY = "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==";

    /** Default storage container name for tests */
    public static final String STORAGE_CONTAINER = "test-container";

    /**
     * Create Azurite container with default image.
     */
    public AzuriteContainer() {
        this(DEFAULT_IMAGE + ":" + DEFAULT_TAG);
    }

    /**
     * Create Azurite container with custom image.
     *
     * @param image Docker image name (e.g., "mcr.microsoft.com/azure-storage/azurite:3.35.0")
     */
    public AzuriteContainer(String image) {
        super(image == null ? DEFAULT_IMAGE + ":" + DEFAULT_TAG : image);
        this.addExposedPort(DEFAULT_PORT);
        this.addEnv("AZURITE_ACCOUNTS", ACCOUNT + ":" + KEY);
        this.setWaitStrategy(new LogMessageWaitStrategy().withRegEx(LOG_WAIT_REGEX));
    }

    /**
     * Create the storage container for test files.
     * Call this before uploading blobs.
     */
    public void createStorageContainer() {
        blobServiceClient().createBlobContainer(STORAGE_CONTAINER);
    }

    /**
     * Delete the storage container and all its contents.
     * Call this for cleanup after tests.
     */
    public void deleteStorageContainer() {
        blobServiceClient().deleteBlobContainer(STORAGE_CONTAINER);
    }

    /**
     * Get blob service client for Azurite.
     *
     * @return BlobServiceClient configured for this Azurite instance
     */
    public BlobServiceClient blobServiceClient() {
        return new BlobServiceClientBuilder()
            .endpoint(endpoint())
            .credential(credential())
            .buildClient();
    }

    /**
     * Get blob container client for the default test container.
     *
     * @return BlobContainerClient for uploading/downloading blobs
     */
    public BlobContainerClient containerClient() {
        return blobServiceClient().getBlobContainerClient(STORAGE_CONTAINER);
    }

    /**
     * Get the Azurite HTTP endpoint.
     *
     * @return Endpoint URL (e.g., "http://localhost:12345/devstoreaccount1")
     */
    public String endpoint() {
        return String.format("http://%s:%d/%s", getHost(), getMappedPort(DEFAULT_PORT), ACCOUNT);
    }

    /**
     * Get storage credentials for Azurite.
     *
     * @return StorageSharedKeyCredential with default account and key
     */
    public StorageSharedKeyCredential credential() {
        return new StorageSharedKeyCredential(ACCOUNT, KEY);
    }

    /**
     * Generate Azure URL for a blob path.
     *
     * <p>Quickwit Azure URL format: {@code azure://container/path}
     * <p>The account name comes from credentials configuration, not the URI.
     *
     * @param blobPath Path within the container (e.g., "splits/split-1.split")
     * @return Azure URL (e.g., "azure://test-container/splits/split-1.split")
     */
    public String azureUrl(String blobPath) {
        return String.format("azure://%s/%s", STORAGE_CONTAINER, blobPath);
    }

    /**
     * Generate Azure URL for the container (without blob path).
     *
     * <p>Use this for SplitSearcher when the split filename comes from metadata.
     * The URI should point to the container only, not individual blobs.
     *
     * <p>Quickwit Azure URL format: {@code azure://container}
     * <p>The account name comes from credentials configuration, not the URI.
     *
     * @return Azure container URL (e.g., "azure://test-container")
     */
    public String azureContainerUrl() {
        return String.format("azure://%s", STORAGE_CONTAINER);
    }
}

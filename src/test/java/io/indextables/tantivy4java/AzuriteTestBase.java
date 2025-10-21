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

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;

/**
 * Base test class for Azurite-based tests with automatic lifecycle management.
 *
 * <p>This base class handles the complete lifecycle of the Azurite Docker container:
 * <ul>
 *   <li>Starts Azurite container once before all tests (@BeforeAll)</li>
 *   <li>Creates storage container before each test (@BeforeEach)</li>
 *   <li>Deletes storage container after each test (@AfterEach)</li>
 *   <li>Stops Azurite container after all tests (@AfterAll)</li>
 * </ul>
 *
 * <p>Based on Apache Iceberg's AzuriteTestBase pattern.
 *
 * <h3>Usage Example:</h3>
 * <pre>{@code
 * public class MyAzureTest extends AzuriteTestBase {
 *
 *     @Test
 *     public void testAzureSplitUpload() {
 *         // Container is already created and ready
 *         BlobClient client = AZURITE.containerClient()
 *             .getBlobClient("test-file.split");
 *
 *         client.uploadFromFile("/tmp/test-file.split");
 *
 *         // Use AZURITE.azureUrl() to get Azure URL
 *         String url = AZURITE.azureUrl("test-file.split");
 *
 *         // Container automatically cleaned up after test
 *     }
 * }
 * }</pre>
 *
 * @see AzuriteContainer for container implementation
 */
public class AzuriteTestBase {

    /**
     * Shared Azurite container instance for all tests in the class.
     */
    protected static final AzuriteContainer AZURITE = new AzuriteContainer();

    /**
     * Start Azurite container once before all tests.
     */
    @BeforeAll
    public static void beforeAll() {
        AZURITE.start();
        System.out.println("✅ Azurite started: " + AZURITE.endpoint());
    }

    /**
     * Stop Azurite container after all tests.
     */
    @AfterAll
    public static void afterAll() {
        AZURITE.stop();
        System.out.println("✅ Azurite stopped");
    }

    /**
     * Create storage container before each test.
     */
    @BeforeEach
    public void beforeEach() {
        AZURITE.createStorageContainer();
    }

    /**
     * Delete storage container after each test.
     */
    @AfterEach
    public void afterEach() {
        AZURITE.deleteStorageContainer();
    }
}

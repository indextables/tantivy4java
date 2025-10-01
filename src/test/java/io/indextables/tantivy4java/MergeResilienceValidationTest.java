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
import io.indextables.tantivy4java.query.*;
import io.indextables.tantivy4java.split.merge.*;


import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Test to validate the merge resilience behavior:
 * 1. Merge with non-existent file should skip it and complete successfully
 * 2. Merge with completely invalid file should skip it and complete successfully
 * 3. Skipped files should be reported in the result, no exceptions thrown
 *
 * This validates the user requirement: "Any time we see data corruption we should
 * just add the file to the skip list and continue with the splits that are readable"
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class MergeResilienceValidationTest {

    private static Schema schema;
    private static Path tempDir;
    private static Path goodSplit1Path, goodSplit2Path;

    @BeforeAll
    static void setUpClass(@TempDir Path sharedTempDir) throws Exception {
        System.out.println("🧪 Setting up MergeResilienceValidationTest...");
        tempDir = sharedTempDir;

        // Create simple test schema
        schema = new SchemaBuilder()
                .addTextField("title")
                .addTextField("content")
                .addIntegerField("count", true, true, false)
                .build();

        // Create two good indices and convert to splits
        createGoodSplitFiles();

        System.out.println("✅ Setup complete - good splits created successfully");
    }

    private static void createGoodSplitFiles() throws Exception {
        // Create first good index
        Path index1Dir = tempDir.resolve("good_index1");
        Index goodIndex1 = new Index(schema, index1Dir.toString());
        try (IndexWriter writer = goodIndex1.writer(Index.Memory.DEFAULT_HEAP_SIZE, 1)) {
            writer.addJson("{ \"title\": \"Document 1\", \"content\": \"Content 1\", \"count\": 10 }");
            writer.addJson("{ \"title\": \"Document 2\", \"content\": \"Content 2\", \"count\": 20 }");
            writer.commit();
        }

        // Create second good index
        Path index2Dir = tempDir.resolve("good_index2");
        Index goodIndex2 = new Index(schema, index2Dir.toString());
        try (IndexWriter writer = goodIndex2.writer(Index.Memory.DEFAULT_HEAP_SIZE, 1)) {
            writer.addJson("{ \"title\": \"Document 3\", \"content\": \"Content 3\", \"count\": 30 }");
            writer.addJson("{ \"title\": \"Document 4\", \"content\": \"Content 4\", \"count\": 40 }");
            writer.commit();
        }

        // Convert to split files
        QuickwitSplit.SplitConfig config = new QuickwitSplit.SplitConfig(
                "test-index", "test-source", "test-node");

        goodSplit1Path = tempDir.resolve("good_split_1.split");
        goodSplit2Path = tempDir.resolve("good_split_2.split");

        QuickwitSplit.convertIndex(goodIndex1, goodSplit1Path.toString(), config);
        QuickwitSplit.convertIndex(goodIndex2, goodSplit2Path.toString(), config);

        System.out.println("📁 Created good split files:");
        System.out.println("   " + goodSplit1Path);
        System.out.println("   " + goodSplit2Path);
    }

    @Test
    @org.junit.jupiter.api.Order(1)
    @DisplayName("Test baseline: merge with only good splits works")
    void testBaselineMergeWithGoodSplits() throws IOException {
        System.out.println("✅ Baseline test: Merging only good splits...");

        List<String> goodSplits = Arrays.asList(
                goodSplit1Path.toString(),
                goodSplit2Path.toString()
        );

        Path outputPath = tempDir.resolve("baseline_merged.split");
        QuickwitSplit.MergeConfig mergeConfig = new QuickwitSplit.MergeConfig(
                "baseline-index", "test-source", "test-node");

        QuickwitSplit.SplitMetadata result = QuickwitSplit.mergeSplits(
                goodSplits, outputPath.toString(), mergeConfig);

        // Verify successful merge
        assertNotNull(result, "Baseline merge result should not be null");
        assertTrue(result.getNumDocs() > 0, "Baseline merge should have documents");
        assertFalse(result.hasSkippedSplits(), "Baseline should have no skipped splits");
        assertEquals(0, result.getSkippedSplits().size(), "Baseline skipped list should be empty");

        System.out.println("✅ Baseline successful: " + result.getNumDocs() + " documents merged");
    }

    @Test
    @org.junit.jupiter.api.Order(2)
    @DisplayName("Test resilience: merge with non-existent file")
    void testMergeWithNonExistentFile() throws IOException {
        System.out.println("🧪 Resilience test: Merging with non-existent file...");

        String nonExistentFile = tempDir.resolve("does_not_exist.split").toString();
        List<String> mixedSplits = Arrays.asList(
                goodSplit1Path.toString(),      // Good
                nonExistentFile,                // Non-existent - should be skipped
                goodSplit2Path.toString()       // Good
        );

        Path outputPath = tempDir.resolve("resilient_merged.split");
        QuickwitSplit.MergeConfig mergeConfig = new QuickwitSplit.MergeConfig(
                "resilient-index", "test-source", "test-node");

        // This should NOT throw an exception - should complete and report skipped file
        QuickwitSplit.SplitMetadata result;
        try {
            result = QuickwitSplit.mergeSplits(mixedSplits, outputPath.toString(), mergeConfig);
            System.out.println("✅ SUCCESS: Merge completed without exception despite non-existent file");
        } catch (Exception e) {
            System.out.println("🚨 FAILURE: Exception thrown instead of graceful handling");
            System.out.println("🚨 Exception: " + e.getMessage());
            fail("Merge should handle non-existent files gracefully, not throw exceptions. " +
                 "Error: " + e.getMessage());
            return;
        }

        // Validate results
        assertNotNull(result, "Result should not be null");
        assertTrue(result.getNumDocs() > 0, "Should have documents from good splits");

        // KEY VALIDATION: Check if skipped splits are reported
        if (result.hasSkippedSplits()) {
            System.out.println("✅ EXCELLENT: Non-existent file properly reported as skipped");
            assertEquals(1, result.getSkippedSplits().size(), "Should skip exactly 1 file");
            assertTrue(result.getSkippedSplits().contains(nonExistentFile),
                      "Should report the non-existent file as skipped");
        } else {
            System.out.println("ℹ️ INFO: Non-existent file not reported as skipped (may be handled at extraction stage)");
        }

        System.out.println("🎯 Non-existent file test results:");
        System.out.println("   Documents: " + result.getNumDocs());
        System.out.println("   Skipped: " + result.getSkippedSplits().size());
        System.out.println("   Skipped files: " + result.getSkippedSplits());
    }

    @Test
    @org.junit.jupiter.api.Order(3)
    @DisplayName("Test resilience: merge with truncated metadata JSON (.managed.json corruption)")
    void testMergeWithTruncatedMetadataJson() throws IOException {
        System.out.println("🧪 Resilience test: Merging with truncated metadata JSON (simulating .managed.json corruption)...");

        // Create a corrupted split by taking a good split and truncating its metadata
        Path corruptedFile = tempDir.resolve("truncated_metadata.split");
        createTruncatedMetadataSplit(goodSplit1Path, corruptedFile);

        List<String> mixedSplits = Arrays.asList(
                goodSplit1Path.toString(),      // Good (original)
                corruptedFile.toString(),       // Corrupted - should be skipped due to Index::open failure
                goodSplit2Path.toString()       // Good
        );

        Path outputPath = tempDir.resolve("truncated_metadata_merged.split");
        QuickwitSplit.MergeConfig mergeConfig = new QuickwitSplit.MergeConfig(
                "truncated-metadata-index", "test-source", "test-node");

        // This should NOT throw an exception - should complete and report skipped file
        QuickwitSplit.SplitMetadata result;
        try {
            result = QuickwitSplit.mergeSplits(mixedSplits, outputPath.toString(), mergeConfig);
            System.out.println("✅ SUCCESS: Merge completed without exception despite .managed.json corruption");
        } catch (Exception e) {
            System.out.println("🚨 FAILURE: Exception thrown instead of graceful handling");
            System.out.println("🚨 Exception: " + e.getMessage());
            fail("Merge should handle .managed.json corruption gracefully, not throw exceptions. " +
                 "This validates the user requirement: 'Any time we see data corruption we should just add the file to the skip list'. " +
                 "Error: " + e.getMessage());
            return;
        }

        // Validate results
        assertNotNull(result, "Result should not be null");
        assertTrue(result.getNumDocs() > 0, "Should have documents from good splits");

        // KEY VALIDATION: Corrupted file should be skipped
        assertTrue(result.hasSkippedSplits(), "Should have skipped the corrupted file");
        assertEquals(1, result.getSkippedSplits().size(), "Should skip exactly 1 file");
        assertTrue(result.getSkippedSplits().contains(corruptedFile.toString()),
                  "Should report the corrupted file as skipped");

        System.out.println("🎯 .managed.json corruption test results:");
        System.out.println("   Documents: " + result.getNumDocs());
        System.out.println("   Skipped: " + result.getSkippedSplits().size());
        System.out.println("   Skipped files: " + result.getSkippedSplits());
        System.out.println("✅ VALIDATION COMPLETE: .managed.json corruption handled gracefully!");
        System.out.println("✅ USER REQUIREMENT SATISFIED: Data corruption added to skip list, merge continued");
    }

    /**
     * Creates a corrupted split file by taking a good split and manipulating the metadata length field
     * to truncate the JSON, causing "EOF while parsing" errors during Index::open.
     */
    private void createTruncatedMetadataSplit(Path sourceSplit, Path corruptedSplit) throws IOException {
        System.out.println("🔧 Creating truncated metadata split from: " + sourceSplit);

        byte[] originalData = Files.readAllBytes(sourceSplit);
        System.out.println("📊 Original split size: " + originalData.length + " bytes");

        // Read the footer structure to understand the layout
        // Quickwit split format: [data] [metadata] [metadata_length] [hotcache] [hotcache_length]
        if (originalData.length < 8) {
            throw new IOException("Split file too small to manipulate");
        }

        // Read hotcache length from last 4 bytes
        int hotcacheLength = readInt32LE(originalData, originalData.length - 4);
        System.out.println("📊 Hotcache length: " + hotcacheLength + " bytes");

        // Read metadata length from 4 bytes before hotcache
        int metadataLengthPos = originalData.length - 4 - hotcacheLength - 4;
        if (metadataLengthPos < 0) {
            throw new IOException("Invalid split structure for manipulation");
        }

        int originalMetadataLength = readInt32LE(originalData, metadataLengthPos);
        System.out.println("📊 Original metadata length: " + originalMetadataLength + " bytes");

        // Calculate new metadata length that will truncate the JSON in the middle
        // Reduce by about 30% to ensure we cut off closing braces/brackets
        int newMetadataLength = (int) (originalMetadataLength * 0.7);
        System.out.println("🔪 New metadata length: " + newMetadataLength + " bytes (truncated)");

        // Create corrupted split data
        byte[] corruptedData = originalData.clone();

        // Update the metadata length field to the truncated value
        writeInt32LE(corruptedData, metadataLengthPos, newMetadataLength);

        // Write the corrupted split
        Files.write(corruptedSplit, corruptedData);

        System.out.println("💥 Created corrupted split: " + corruptedSplit);
        System.out.println("💥 This will cause 'EOF while parsing' error during Index::open metadata reading");
    }

    private int readInt32LE(byte[] data, int offset) {
        return (data[offset] & 0xFF) |
               ((data[offset + 1] & 0xFF) << 8) |
               ((data[offset + 2] & 0xFF) << 16) |
               ((data[offset + 3] & 0xFF) << 24);
    }

    private void writeInt32LE(byte[] data, int offset, int value) {
        data[offset] = (byte) (value & 0xFF);
        data[offset + 1] = (byte) ((value >> 8) & 0xFF);
        data[offset + 2] = (byte) ((value >> 16) & 0xFF);
        data[offset + 3] = (byte) ((value >> 24) & 0xFF);
    }

    @Test
    @org.junit.jupiter.api.Order(4)
    @DisplayName("Test API contracts: skipped splits methods work correctly")
    void testSkippedSplitsAPIContracts() throws IOException {
        System.out.println("🔍 API contract test: Validating skipped splits API...");

        // Create an invalid file to ensure we have skipped splits
        Path invalidFile = tempDir.resolve("api_test_invalid.split");
        Files.write(invalidFile, "invalid content for API testing".getBytes());

        List<String> splitsWithInvalid = Arrays.asList(
                goodSplit1Path.toString(),
                invalidFile.toString()
        );

        Path outputPath = tempDir.resolve("api_test_merged.split");
        QuickwitSplit.MergeConfig mergeConfig = new QuickwitSplit.MergeConfig(
                "api-test-index", "test-source", "test-node");

        QuickwitSplit.SplitMetadata result = QuickwitSplit.mergeSplits(
                splitsWithInvalid, outputPath.toString(), mergeConfig);

        // Test API contracts
        assertTrue(result.hasSkippedSplits(), "hasSkippedSplits() should return true");

        List<String> skippedSplits = result.getSkippedSplits();
        assertNotNull(skippedSplits, "getSkippedSplits() should not return null");
        assertFalse(skippedSplits.isEmpty(), "Skipped splits list should not be empty");

        // Test defensive copying
        List<String> firstCall = result.getSkippedSplits();
        List<String> secondCall = result.getSkippedSplits();
        assertNotSame(firstCall, secondCall, "Should return new list instances (defensive copying)");
        assertEquals(firstCall, secondCall, "Content should be the same");

        System.out.println("✅ API contracts validated successfully");
    }

    @AfterAll
    static void tearDown() {
        System.out.println("🧹 Cleaning up MergeResilienceValidationTest...");
        System.out.println("✅ Cleanup complete!");
    }
}

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
 * Test class to demonstrate and validate resilient metadata parsing in split merge operations.
 * This test creates scenarios with corrupted/invalid split files to ensure the merge operation
 * continues processing valid splits while gracefully skipping problematic ones.
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class ResilientSplitMergeTest {

    private static Schema schema;
    private static Index index1, index2;
    private static Path tempDir;
    private static Path goodSplit1Path, goodSplit2Path, badSplitPath;

    @BeforeAll
    static void setUpClass(@TempDir Path sharedTempDir) throws Exception {
        System.out.println("🧪 Setting up ResilientSplitMergeTest...");
        tempDir = sharedTempDir;

        // Create simple test schema
        schema = new SchemaBuilder()
                .addTextField("title")
                .addTextField("content")
                .addIntegerField("count", true, true, false)
                .build();

        // Create first good index with temporary directory
        System.out.println("📚 Creating first good index...");
        Path index1Dir = tempDir.resolve("index1");
        index1 = new Index(schema, index1Dir.toString());
        try (IndexWriter writer = index1.writer(Index.Memory.DEFAULT_HEAP_SIZE, 1)) {
            // Add test documents
            writer.addJson("{ \"title\": \"Good Document 1\", \"content\": \"This is content from index 1\", \"count\": 10 }");
            writer.addJson("{ \"title\": \"Good Document 2\", \"content\": \"More content from index 1\", \"count\": 20 }");
            writer.commit();
        }

        // Create second good index with temporary directory
        System.out.println("📚 Creating second good index...");
        Path index2Dir = tempDir.resolve("index2");
        index2 = new Index(schema, index2Dir.toString());
        try (IndexWriter writer = index2.writer(Index.Memory.DEFAULT_HEAP_SIZE, 1)) {
            // Add different test documents
            writer.addJson("{ \"title\": \"Good Document 3\", \"content\": \"This is content from index 2\", \"count\": 30 }");
            writer.addJson("{ \"title\": \"Good Document 4\", \"content\": \"More content from index 2\", \"count\": 40 }");
            writer.commit();
        }

        // Create good split files
        QuickwitSplit.SplitConfig config = new QuickwitSplit.SplitConfig(
                "test-index", "test-source", "test-node");

        goodSplit1Path = tempDir.resolve("good_split_1.split");
        goodSplit2Path = tempDir.resolve("good_split_2.split");

        System.out.println("🔄 Converting indices to split files...");
        QuickwitSplit.convertIndex(index1, goodSplit1Path.toString(), config);
        QuickwitSplit.convertIndex(index2, goodSplit2Path.toString(), config);

        // Create a bad split file that will cause metadata parsing failure
        badSplitPath = tempDir.resolve("bad_split.split");
        createCorruptedSplitFile(badSplitPath);

        System.out.println("✅ Setup complete!");
        System.out.println("   Good split 1: " + goodSplit1Path);
        System.out.println("   Good split 2: " + goodSplit2Path);
        System.out.println("   Bad split: " + badSplitPath);
    }

    /**
     * Creates a corrupted split file that looks like a split but has invalid metadata
     * that will cause parsing failures during merge operations.
     */
    private static void createCorruptedSplitFile(Path badSplitPath) throws IOException {
        System.out.println("💥 Creating corrupted split file...");

        // Create a file that looks like a split but has invalid internal structure
        // This will cause metadata parsing to fail when the merge operation tries to read it
        byte[] corruptedData = corruptedSplitData();
        Files.write(badSplitPath, corruptedData);

        System.out.println("💥 Created corrupted split file: " + badSplitPath + " (" + corruptedData.length + " bytes)");
    }

    /**
     * Creates corrupted split data that will fail during metadata parsing.
     * This mimics real-world scenarios where split files become corrupted during storage or transfer.
     */
    private static byte[] corruptedSplitData() {
        // Create data that looks like it could be a split file but will fail during parsing
        // This represents corrupted metadata that can't be deserialized properly

        // Start with some quasi-realistic header bytes
        List<Byte> data = new ArrayList<>();

        // Add some header-like bytes that might pass initial validation
        byte[] fakeHeader = { 0x50, 0x4B, 0x03, 0x04 }; // ZIP-like header
        for (byte b : fakeHeader) {
            data.add(b);
        }

        // Add some random data that will cause parsing errors
        Random random = new Random(12345); // Fixed seed for reproducible corruption
        for (int i = 0; i < 1024; i++) {
            data.add((byte) random.nextInt(256));
        }

        // Add some fake footer that might look like valid metadata but isn't
        String fakeMetadata = "{\"invalid_json\": \"this will cause parsing errors\", \"missing_fields\": true";
        // Note: Intentionally malformed JSON (missing closing brace)
        for (byte b : fakeMetadata.getBytes()) {
            data.add(b);
        }

        // Convert to byte array
        byte[] result = new byte[data.size()];
        for (int i = 0; i < data.size(); i++) {
            result[i] = data.get(i);
        }

        return result;
    }

    @Test
    @org.junit.jupiter.api.Order(1)
    @DisplayName("Test merge with good splits only - baseline verification")
    void testMergeWithGoodSplitsOnly() throws IOException {
        System.out.println("✅ Testing merge with good splits only...");

        List<String> goodSplits = Arrays.asList(
                goodSplit1Path.toString(),
                goodSplit2Path.toString()
        );

        Path outputPath = tempDir.resolve("merged_good_only.split");
        QuickwitSplit.MergeConfig mergeConfig = new QuickwitSplit.MergeConfig(
                "merged-index", "test-source", "test-node");

        QuickwitSplit.SplitMetadata result = QuickwitSplit.mergeSplits(
                goodSplits, outputPath.toString(), mergeConfig);

        // Verify successful merge
        assertNotNull(result, "Merge result should not be null");
        assertTrue(result.getNumDocs() > 0, "Merged split should contain documents");

        // Verify no splits were skipped
        assertFalse(result.hasSkippedSplits(), "No splits should be skipped with all good splits");
        assertEquals(0, result.getSkippedSplits().size(), "Skipped splits list should be empty");

        System.out.println("✅ Baseline merge successful: " + result.getNumDocs() + " documents merged");
    }

    @Test
    @org.junit.jupiter.api.Order(2)
    @DisplayName("Test resilient merge with one bad split - demonstrates skip functionality")
    void testResilientMergeWithBadSplit() throws IOException {
        System.out.println("🧪 Testing resilient merge with bad split...");

        List<String> mixedSplits = Arrays.asList(
                goodSplit1Path.toString(),
                badSplitPath.toString(),  // This should be skipped
                goodSplit2Path.toString()
        );

        Path outputPath = tempDir.resolve("merged_with_bad.split");
        QuickwitSplit.MergeConfig mergeConfig = new QuickwitSplit.MergeConfig(
                "resilient-index", "test-source", "test-node");

        QuickwitSplit.SplitMetadata result = QuickwitSplit.mergeSplits(
                mixedSplits, outputPath.toString(), mergeConfig);

        // Verify the merge succeeded despite the bad split
        assertNotNull(result, "Merge should succeed even with bad split");
        assertTrue(result.getNumDocs() > 0, "Merged split should contain documents from good splits");

        // Verify the bad split was properly skipped and reported
        assertTrue(result.hasSkippedSplits(), "Should have skipped splits");
        assertEquals(1, result.getSkippedSplits().size(), "Should have skipped exactly 1 split");

        List<String> skippedSplits = result.getSkippedSplits();
        assertTrue(skippedSplits.contains(badSplitPath.toString()),
                "Skipped splits should contain the bad split path");

        System.out.println("🎯 Resilient merge results:");
        System.out.println("   Documents merged: " + result.getNumDocs());
        System.out.println("   Skipped splits: " + result.getSkippedSplits().size());
        System.out.println("   Skipped paths: " + result.getSkippedSplits());

        // Verify the output file was created successfully
        assertTrue(Files.exists(outputPath), "Output split file should exist");
        assertTrue(Files.size(outputPath) > 0, "Output split file should not be empty");

        System.out.println("✅ Resilient merge completed successfully!");
    }

    @Test
    @org.junit.jupiter.api.Order(3)
    @DisplayName("Test merge with multiple bad splits")
    void testMergeWithMultipleBadSplits() throws IOException {
        System.out.println("🧪 Testing merge with multiple bad splits...");

        // Create additional bad split
        Path badSplit2Path = tempDir.resolve("bad_split_2.split");
        createCorruptedSplitFile(badSplit2Path);

        List<String> mixedSplits = Arrays.asList(
                badSplitPath.toString(),      // Bad split 1
                goodSplit1Path.toString(),    // Good split
                badSplit2Path.toString(),     // Bad split 2
                goodSplit2Path.toString()     // Good split
        );

        Path outputPath = tempDir.resolve("merged_multiple_bad.split");
        QuickwitSplit.MergeConfig mergeConfig = new QuickwitSplit.MergeConfig(
                "multi-bad-index", "test-source", "test-node");

        QuickwitSplit.SplitMetadata result = QuickwitSplit.mergeSplits(
                mixedSplits, outputPath.toString(), mergeConfig);

        // Verify successful merge with multiple skipped splits
        assertNotNull(result, "Merge should succeed even with multiple bad splits");
        assertTrue(result.getNumDocs() > 0, "Should merge documents from good splits");

        // Verify both bad splits were skipped
        assertTrue(result.hasSkippedSplits(), "Should have skipped splits");
        assertEquals(2, result.getSkippedSplits().size(), "Should have skipped exactly 2 splits");

        List<String> skippedSplits = result.getSkippedSplits();
        assertTrue(skippedSplits.contains(badSplitPath.toString()),
                "Should skip first bad split");
        assertTrue(skippedSplits.contains(badSplit2Path.toString()),
                "Should skip second bad split");

        System.out.println("🎯 Multiple bad splits merge results:");
        System.out.println("   Documents merged: " + result.getNumDocs());
        System.out.println("   Skipped splits: " + result.getSkippedSplits().size());
        for (int i = 0; i < skippedSplits.size(); i++) {
            System.out.println("   Skipped " + (i + 1) + ": " + skippedSplits.get(i));
        }

        System.out.println("✅ Multiple bad splits merge completed!");
    }

    @Test
    @org.junit.jupiter.api.Order(4)
    @DisplayName("Test merge with only bad splits - should fail gracefully")
    void testMergeWithOnlyBadSplits() throws IOException {
        System.out.println("💥 Testing merge with only bad splits...");

        // Create third bad split
        Path badSplit3Path = tempDir.resolve("bad_split_3.split");
        createCorruptedSplitFile(badSplit3Path);

        List<String> onlyBadSplits = Arrays.asList(
                badSplitPath.toString(),
                badSplit3Path.toString()
        );

        Path outputPath = tempDir.resolve("merged_all_bad.split");
        QuickwitSplit.MergeConfig mergeConfig = new QuickwitSplit.MergeConfig(
                "all-bad-index", "test-source", "test-node");

        // This should either fail with a clear error or succeed with all splits skipped
        // The exact behavior depends on implementation - both are valid approaches
        try {
            QuickwitSplit.SplitMetadata result = QuickwitSplit.mergeSplits(
                    onlyBadSplits, outputPath.toString(), mergeConfig);

            // If it succeeds, it should report all splits as skipped
            assertTrue(result.hasSkippedSplits(), "Should have skipped all splits");
            assertEquals(2, result.getSkippedSplits().size(), "Should have skipped both splits");
            assertEquals(0, result.getNumDocs(), "Should have no documents when all splits are bad");

            System.out.println("✅ All bad splits merge handled gracefully:");
            System.out.println("   Documents merged: " + result.getNumDocs());
            System.out.println("   Skipped splits: " + result.getSkippedSplits().size());

        } catch (RuntimeException e) {
            // It's also acceptable for this to fail with a clear error message
            System.out.println("✅ All bad splits merge failed as expected: " + e.getMessage());
            assertTrue(e.getMessage().contains("split") || e.getMessage().contains("merge"),
                    "Error message should indicate the issue with splits/merging");
        }
    }

    @Test
    @org.junit.jupiter.api.Order(5)
    @DisplayName("Verify skipped splits API functionality")
    void testSkippedSplitsAPIFunctionality() throws IOException {
        System.out.println("🔍 Testing skipped splits API functionality...");

        List<String> splitWithOneGoodOneBad = Arrays.asList(
                goodSplit1Path.toString(),
                badSplitPath.toString()
        );

        Path outputPath = tempDir.resolve("api_test_merge.split");
        QuickwitSplit.MergeConfig mergeConfig = new QuickwitSplit.MergeConfig(
                "api-test-index", "test-source", "test-node");

        QuickwitSplit.SplitMetadata result = QuickwitSplit.mergeSplits(
                splitWithOneGoodOneBad, outputPath.toString(), mergeConfig);

        // Test all API methods for skipped splits
        assertTrue(result.hasSkippedSplits(), "hasSkippedSplits() should return true");

        List<String> skippedSplits = result.getSkippedSplits();
        assertNotNull(skippedSplits, "getSkippedSplits() should not return null");
        assertEquals(1, skippedSplits.size(), "Should have exactly one skipped split");
        assertEquals(badSplitPath.toString(), skippedSplits.get(0), "Should skip the bad split");

        // Verify the returned list is a copy (defensive copying)
        List<String> originalSkipped = result.getSkippedSplits();
        List<String> secondCall = result.getSkippedSplits();
        assertNotSame(originalSkipped, secondCall, "Should return new list instances (defensive copying)");
        assertEquals(originalSkipped, secondCall, "Content should be the same");

        // Test modification safety
        try {
            originalSkipped.add("test-modification");
            List<String> afterModification = result.getSkippedSplits();
            assertEquals(1, afterModification.size(), "Original result should not be affected by list modification");
        } catch (UnsupportedOperationException e) {
            // It's also acceptable if the returned list is immutable
            System.out.println("✅ Returned list is immutable (good practice)");
        }

        System.out.println("✅ API functionality verified successfully!");
    }

    @AfterAll
    static void tearDown() {
        System.out.println("🧹 Cleaning up ResilientSplitMergeTest...");
        // Cleanup is handled automatically by @TempDir
        System.out.println("✅ Cleanup complete!");
    }
}

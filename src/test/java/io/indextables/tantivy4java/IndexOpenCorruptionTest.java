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
 * Test to verify that Index::open corruption during split merge operations
 * results in graceful failure - the corrupted split is added to the skip list
 * and the merge completes successfully with the remaining valid splits.
 *
 * This validates the specific user requirement: "Any time we see data corruption
 * we should just add the file to the skip list and continue with the splits that are readable"
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class IndexOpenCorruptionTest {

    private static Schema schema;
    private static Index goodIndex1, goodIndex2;
    private static Path tempDir;
    private static Path goodSplit1Path, goodSplit2Path, corruptedSplitPath;

    @BeforeAll
    static void setUpClass(@TempDir Path sharedTempDir) throws Exception {
        System.out.println("🧪 Setting up IndexOpenCorruptionTest...");
        tempDir = sharedTempDir;

        // Create simple test schema
        schema = new SchemaBuilder()
                .addTextField("title")
                .addTextField("content")
                .addIntegerField("count", true, true, false)
                .build();

        // Create first good index
        System.out.println("📚 Creating first good index...");
        Path index1Dir = tempDir.resolve("good_index1");
        goodIndex1 = new Index(schema, index1Dir.toString());
        try (IndexWriter writer = goodIndex1.writer(Index.Memory.DEFAULT_HEAP_SIZE, 1)) {
            writer.addJson("{ \"title\": \"Good Document 1\", \"content\": \"Content from good index 1\", \"count\": 10 }");
            writer.addJson("{ \"title\": \"Good Document 2\", \"content\": \"More content from good index 1\", \"count\": 20 }");
            writer.commit();
        }

        // Create second good index
        System.out.println("📚 Creating second good index...");
        Path index2Dir = tempDir.resolve("good_index2");
        goodIndex2 = new Index(schema, index2Dir.toString());
        try (IndexWriter writer = goodIndex2.writer(Index.Memory.DEFAULT_HEAP_SIZE, 1)) {
            writer.addJson("{ \"title\": \"Good Document 3\", \"content\": \"Content from good index 2\", \"count\": 30 }");
            writer.addJson("{ \"title\": \"Good Document 4\", \"content\": \"More content from good index 2\", \"count\": 40 }");
            writer.commit();
        }

        // Create good split files
        QuickwitSplit.SplitConfig config = new QuickwitSplit.SplitConfig(
                "test-index", "test-source", "test-node");

        goodSplit1Path = tempDir.resolve("good_split_1.split");
        goodSplit2Path = tempDir.resolve("good_split_2.split");

        System.out.println("🔄 Converting good indices to split files...");
        QuickwitSplit.convertIndex(goodIndex1, goodSplit1Path.toString(), config);
        QuickwitSplit.convertIndex(goodIndex2, goodSplit2Path.toString(), config);

        // Create a corrupted split file that will trigger Index::open failure
        corruptedSplitPath = tempDir.resolve("corrupted_split.split");
        createCorruptedSplitWithManagedJsonError(corruptedSplitPath);

        System.out.println("✅ Setup complete!");
        System.out.println("   Good split 1: " + goodSplit1Path);
        System.out.println("   Good split 2: " + goodSplit2Path);
        System.out.println("   Corrupted split: " + corruptedSplitPath);
    }

    /**
     * Creates a corrupted split file that will specifically cause Index::open to fail
     * with the type of ".managed.json" corruption error that the user reported.
     */
    private static void createCorruptedSplitWithManagedJsonError(Path corruptedSplitPath) throws IOException {
        System.out.println("💥 Creating corrupted split file that will cause Index::open failure...");

        // Start with a valid split file structure and then corrupt the critical parts
        byte[] corruptedData = createSplitWithCorruptedManagedJson();
        Files.write(corruptedSplitPath, corruptedData);

        System.out.println("💥 Created corrupted split file: " + corruptedSplitPath + " (will cause Index::open .managed.json corruption)");
    }

    /**
     * Creates split data that will specifically trigger the ".managed.json" corruption
     * error during Index::open that the user reported seeing.
     */
    private static byte[] createSplitWithCorruptedManagedJson() {
        List<Byte> data = new ArrayList<>();

        // Create minimal split structure that looks valid initially but has corrupted .managed.json
        // This simulates the exact corruption pattern the user encountered

        // Add some initial valid-looking split headers
        byte[] splitHeader = {
            0x50, 0x4B, 0x03, 0x04,  // ZIP-like header (splits are bundle format)
            0x14, 0x00, 0x00, 0x00,  // Version info
            0x08, 0x00              // Compression flag
        };
        for (byte b : splitHeader) {
            data.add(b);
        }

        // Add some fake file entries that look like a bundle
        Random random = new Random(54321); // Fixed seed for reproducible corruption
        for (int i = 0; i < 256; i++) {
            data.add((byte) random.nextInt(256));
        }

        // Add fake .managed.json content that is corrupted (EOF while parsing string at column 2291)
        String corruptedManagedJson = "{\n" +
            "  \"segments\": [\n" +
            "    {\n" +
            "      \"segment_id\": \"fake-segment\",\n" +
            "      \"max_doc\": 100,\n" +
            "      \"deletes\": null\n" +
            "    }\n" +
            "  ],\n" +
            "  \"index_settings\": {\n" +
            "    \"docstore_compression\": \"lz4\",\n" +
            "    \"docstore_blocksize\": 16384\n" +
            "  },\n" +
            "  \"corrupted_field\": \"this string will be truncated at exactly column 2291 to simulate the EOF error the user saw... " +
            "padding to reach column 2291 exactly: " +
            "0123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789" +
            "0123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789" +
            "0123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789" +
            "0123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789" +
            "0123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789" +
            "0123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789" +
            "0123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789" +
            "0123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789" +
            "0123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789" +
            "0123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789" +
            "0123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789" +
            "0123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789" +
            "0123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789" +
            "0123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789" +
            "0123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789" +
            "0123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789" +
            "0123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789" +
            "0123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789" +
            "0123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789" +
            "0123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789" +
            "0123456789012345678901234567890123456789012345678901234567890123456789"; // Truncated here - no closing quote!

        // Add the corrupted managed.json (intentionally truncated to cause parsing error)
        for (byte b : corruptedManagedJson.getBytes()) {
            data.add(b);
        }

        // Add some footer data
        byte[] footer = { 0x00, 0x00, 0x00, 0x10 }; // Fake footer length
        for (byte b : footer) {
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
    @DisplayName("Test merge with mixed good and corrupted splits - validates graceful handling")
    void testMergeWithMixedSplitsValidatesGracefulHandling() throws IOException {
        System.out.println("🧪 Testing merge with mixed good and corrupted splits...");
        System.out.println("🎯 Expected behavior: Merge should complete successfully, corrupted split should be in skip list");

        List<String> mixedSplits = Arrays.asList(
                goodSplit1Path.toString(),    // Good split - should be merged
                corruptedSplitPath.toString(), // Corrupted split - should be skipped due to Index::open failure
                goodSplit2Path.toString()     // Good split - should be merged
        );

        Path outputPath = tempDir.resolve("merged_mixed.split");
        QuickwitSplit.MergeConfig mergeConfig = new QuickwitSplit.MergeConfig(
                "mixed-test-index", "test-source", "test-node");

        // ✅ THIS IS THE KEY TEST: The merge should NOT throw an exception
        // Instead, it should complete successfully and report the corrupted split as skipped
        QuickwitSplit.SplitMetadata result;

        try {
            result = QuickwitSplit.mergeSplits(mixedSplits, outputPath.toString(), mergeConfig);
            System.out.println("✅ SUCCESS: Merge completed without throwing exception");
        } catch (Exception e) {
            System.out.println("🚨 FAILURE: Merge threw exception instead of handling corruption gracefully");
            System.out.println("🚨 Exception: " + e.getMessage());
            fail("Merge should not throw exception when encountering corrupted splits. " +
                 "Expected behavior: complete merge and report corrupted split in skip list. " +
                 "Actual: threw exception: " + e.getMessage());
            return; // This line won't execute, but makes intent clear
        }

        // Verify the merge completed successfully with partial data
        assertNotNull(result, "Merge result should not be null when handling corruption gracefully");
        assertTrue(result.getNumDocs() > 0, "Merged split should contain documents from valid splits");

        // ✅ CRITICAL VALIDATION: The corrupted split should be reported as skipped
        assertTrue(result.hasSkippedSplits(), "Should have skipped the corrupted split");
        assertEquals(1, result.getSkippedSplits().size(), "Should have skipped exactly 1 corrupted split");

        List<String> skippedSplits = result.getSkippedSplits();
        assertTrue(skippedSplits.contains(corruptedSplitPath.toString()),
                "Skipped splits should contain the corrupted split path");

        // Verify only good splits were merged (not the corrupted one)
        // We should have documents from 2 good splits, not 3 splits
        System.out.println("🎯 Merge results validation:");
        System.out.println("   Documents merged: " + result.getNumDocs());
        System.out.println("   Skipped splits: " + result.getSkippedSplits().size());
        System.out.println("   Skipped paths: " + result.getSkippedSplits());

        // Verify the output file was created successfully
        assertTrue(Files.exists(outputPath), "Output split file should exist");
        assertTrue(Files.size(outputPath) > 0, "Output split file should not be empty");

        System.out.println("✅ VALIDATION COMPLETE: Index::open corruption handled gracefully!");
        System.out.println("   ✓ No exception thrown to caller");
        System.out.println("   ✓ Merge completed successfully");
        System.out.println("   ✓ Corrupted split reported in skip list");
        System.out.println("   ✓ Valid splits merged successfully");
    }

    @Test
    @org.junit.jupiter.api.Order(2)
    @DisplayName("Test that good splits only merge works as baseline")
    void testGoodSplitsOnlyMergeAsBaseline() throws IOException {
        System.out.println("✅ Baseline test: Testing merge with only good splits...");

        List<String> goodSplitsOnly = Arrays.asList(
                goodSplit1Path.toString(),
                goodSplit2Path.toString()
        );

        Path outputPath = tempDir.resolve("merged_good_only.split");
        QuickwitSplit.MergeConfig mergeConfig = new QuickwitSplit.MergeConfig(
                "baseline-test-index", "test-source", "test-node");

        QuickwitSplit.SplitMetadata result = QuickwitSplit.mergeSplits(
                goodSplitsOnly, outputPath.toString(), mergeConfig);

        // Verify successful merge with no skipped splits
        assertNotNull(result, "Baseline merge result should not be null");
        assertTrue(result.getNumDocs() > 0, "Baseline merged split should contain documents");
        assertFalse(result.hasSkippedSplits(), "Baseline should have no skipped splits");
        assertEquals(0, result.getSkippedSplits().size(), "Baseline skipped splits list should be empty");

        System.out.println("✅ Baseline merge successful: " + result.getNumDocs() + " documents merged, no splits skipped");
    }

    @AfterAll
    static void tearDown() {
        System.out.println("🧹 Cleaning up IndexOpenCorruptionTest...");
        // Cleanup is handled automatically by @TempDir
        System.out.println("✅ Cleanup complete!");
    }
}

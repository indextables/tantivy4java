package io.indextables.tantivy4java;

import io.indextables.tantivy4java.core.*;
import io.indextables.tantivy4java.query.*;
import io.indextables.tantivy4java.result.*;
import io.indextables.tantivy4java.split.merge.*;


import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;
import static org.junit.jupiter.api.Assertions.*;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;

/**
 * Test to verify that fast field configuration is preserved during index operations.
 * This tests the core issue: are fast fields preserved from source to merged index?
 */
public class FastFieldMergeTest {

    @Test
    @DisplayName("Verify fast fields are set in schema")
    public void testFastFieldsInSchema() throws Exception {
        System.out.println("=== Testing fast field configuration in schema ===");

        Path tempDir = Files.createTempDirectory("fast-field-test");
        String tempPath = tempDir.toString();

        try {
            // Create schema with fast field explicitly enabled
            SchemaBuilder builder = new SchemaBuilder();
            builder.addIntegerField("id", true, true, true);  // stored, indexed, FAST
            builder.addTextField("category", true, false, "default", "position");  // stored, not fast
            Schema schema = builder.build();

            // Create index and add documents
            Index index = new Index(schema, tempPath + "/index1", true);
            IndexWriter writer = index.writer(Index.Memory.DEFAULT_HEAP_SIZE, 1);

            for (int i = 0; i < 100; i++) {
                Document doc = new Document();
                doc.addInteger("id", i);
                doc.addText("category", "cat-" + (i % 10));
                writer.addDocument(doc);
            }

            writer.commit();
            writer.close();

            // Reopen and check if we can use fast fields
            index.reload();

            try (Searcher searcher = index.searcher()) {
                System.out.println("✓ Index created with " + searcher.getNumDocs() + " docs");

                // Try a simple search
                try (Query query = Query.allQuery()) {
                    try (SearchResult result = searcher.search(query, 10)) {
                        System.out.println("✓ Search successful, found " + result.getHits().size() + " hits");
                    }
                }
            }

            index.close();
            System.out.println("✓ Fast field test completed successfully");

        } finally {
            deleteDirectory(new File(tempPath));
        }
    }

    @Test
    @DisplayName("Verify fast fields survive split conversion")
    public void testFastFieldsInSplitConversion() throws Exception {
        System.out.println("\n=== Testing fast field preservation in split conversion ===");

        Path tempDir = Files.createTempDirectory("fast-field-split-test");
        String tempPath = tempDir.toString();

        try {
            // Create index with fast fields
            SchemaBuilder builder = new SchemaBuilder();
            builder.addIntegerField("id", true, true, true);  // FAST field
            builder.addTextField("content", true, false, "default", "position");
            Schema schema = builder.build();

            Index index = new Index(schema, tempPath + "/index", true);
            IndexWriter writer = index.writer(Index.Memory.DEFAULT_HEAP_SIZE, 1);

            // Add some documents
            for (int i = 0; i < 50; i++) {
                Document doc = new Document();
                doc.addInteger("id", i);
                doc.addText("content", "Document " + i);
                writer.addDocument(doc);
            }

            writer.commit();
            writer.close();
            index.close();

            // Convert to split
            System.out.println("Converting index to split...");
            QuickwitSplit.SplitConfig splitConfig = new QuickwitSplit.SplitConfig(
                "test-index", "test-source", "test-node");

            String splitPath = tempPath + "/test.split";
            QuickwitSplit.SplitMetadata metadata = QuickwitSplit.convertIndexFromPath(
                tempPath + "/index", splitPath, splitConfig);

            // Check if docMappingJson is present
            String docMapping = metadata.getDocMappingJson();
            System.out.println("DocMappingJson from split: " + (docMapping != null ? docMapping : "NULL"));

            assertNotNull(docMapping, "Split should have docMappingJson");
            assertTrue(docMapping.contains("\"name\":\"id\""), "Should contain id field");

            // CRITICAL CHECK: Does it have fast:true for the id field?
            if (docMapping.contains("\"fast\":true")) {
                System.out.println("✓ GOOD: Split docMappingJson contains fast:true");
            } else {
                System.out.println("✗ BAD: Split docMappingJson does NOT contain fast:true");
                System.out.println("  This means fast fields are NOT being preserved during split creation!");
            }

        } finally {
            deleteDirectory(new File(tempPath));
        }
    }

    @Test
    @DisplayName("Verify Tantivy merge preserves fast fields")
    public void testTantivyMergePreservesFastFields() throws Exception {
        System.out.println("\n=== Testing if Tantivy merge preserves fast fields ===");

        Path tempDir = Files.createTempDirectory("tantivy-merge-test");
        String tempPath = tempDir.toString();

        try {
            // Create index with fast fields
            SchemaBuilder builder = new SchemaBuilder();
            builder.addIntegerField("id", true, true, true);  // FAST
            builder.addTextField("content", true, false, "default", "position");
            Schema schema = builder.build();

            Index index = new Index(schema, tempPath + "/index", true);
            IndexWriter writer = index.writer(Index.Memory.DEFAULT_HEAP_SIZE, 1);

            // Add documents in multiple batches to create multiple segments
            for (int batch = 0; batch < 3; batch++) {
                for (int i = batch * 20; i < (batch + 1) * 20; i++) {
                    Document doc = new Document();
                    doc.addInteger("id", i);
                    doc.addText("content", "Content " + i);
                    writer.addDocument(doc);
                }
                writer.commit();  // Each commit creates a new segment
            }

            // Get segment IDs before merge
            index.reload();
            try (Searcher searcher = index.searcher()) {
                java.util.List<String> segmentIds = searcher.getSegmentIds();
                System.out.println("Segments before merge: " + segmentIds.size());

                if (segmentIds.size() > 1) {
                    // Merge all segments
                    System.out.println("Merging " + segmentIds.size() + " segments...");
                    writer.merge(segmentIds);
                    writer.commit();
                    System.out.println("✓ Merge completed");
                }
            }

            writer.close();
            index.close();

            // Now convert merged index to split and check docMapping
            System.out.println("Converting merged index to split...");
            QuickwitSplit.SplitConfig splitConfig = new QuickwitSplit.SplitConfig(
                "test-index", "test-source", "test-node");

            String splitPath = tempPath + "/merged.split";
            QuickwitSplit.SplitMetadata metadata = QuickwitSplit.convertIndexFromPath(
                tempPath + "/index", splitPath, splitConfig);

            String docMapping = metadata.getDocMappingJson();
            System.out.println("DocMappingJson after Tantivy merge: " + (docMapping != null ? docMapping : "NULL"));

            assertNotNull(docMapping, "Merged split should have docMappingJson");

            // CRITICAL CHECK: Are fast fields still present after Tantivy merge?
            if (docMapping.contains("\"fast\":true")) {
                System.out.println("✓✓✓ EXCELLENT: Fast fields ARE preserved after Tantivy merge!");
            } else {
                System.out.println("✗✗✗ PROBLEM FOUND: Fast fields are LOST after Tantivy merge!");
                System.out.println("  This is the root cause - Tantivy's merge operation is not preserving fast fields");
                System.out.println("  Full docMapping: " + docMapping);
            }

        } finally {
            deleteDirectory(new File(tempPath));
        }
    }

    private void deleteDirectory(File directory) {
        if (directory.exists()) {
            File[] files = directory.listFiles();
            if (files != null) {
                for (File file : files) {
                    if (file.isDirectory()) {
                        deleteDirectory(file);
                    } else {
                        file.delete();
                    }
                }
            }
            directory.delete();
        }
    }
}

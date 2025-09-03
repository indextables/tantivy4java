package com.tantivy4java;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.Order;
import java.time.Duration;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import static org.junit.jupiter.api.Assertions.*;

public class S3MergeFixTest {

    @Test
    @DisplayName("Test S3 merge fix - local output then upload")
    public void testS3MergeFix() throws Exception {
        System.out.println("🔍 TESTING S3 MERGE FIX APPROACH");
        
        List<String> localSplits = new ArrayList<>();
        Path tempDir = Files.createTempDirectory("s3-merge-fix-test");
        
        try {
            // Create 2 simple test indices and convert to splits
            for (int i = 0; i < 2; i++) {
                String indexName = "test" + i;
                System.out.println("Creating test index: " + indexName);
                
                // Create simple schema
                Schema schema = new SchemaBuilder()
                    .addTextField("title", true, false, "default", "position")
                    .addTextField("body", false, false, "default", "position")
                    .build();
                
                // Create index with path
                Path indexPath = tempDir.resolve(indexName + "-index");
                Index index = new Index(schema, indexPath.toString());
                
                // Add a few documents using JSON (simpler)
                try (IndexWriter writer = index.writer(Index.Memory.DEFAULT_HEAP_SIZE, 2)) {
                    for (int docId = 0; docId < 5; docId++) {
                        String jsonDoc = String.format(
                            "{\"title\": \"Title %d in %s\", \"body\": \"Body content for document %d\"}",
                            docId, indexName, docId
                        );
                        writer.addJson(jsonDoc);
                    }
                    writer.commit();
                }
                
                // Convert to split
                String splitPath = tempDir.resolve(indexName + ".split").toString();
                QuickwitSplit.SplitConfig config = new QuickwitSplit.SplitConfig("test-index", "test-source", "test-node");
                QuickwitSplit.SplitMetadata metadata = QuickwitSplit.convertIndexFromPath(indexPath.toString(), splitPath, config);
                
                System.out.println("✅ Created split: " + splitPath);
                System.out.println("   Docs: " + metadata.getNumDocs());
                System.out.println("   Size: " + metadata.getUncompressedSizeBytes());
                
                localSplits.add(splitPath);
            }
            
            // 1. ORIGINAL APPROACH - Direct local merge (should work)
            System.out.println("\n🔄 Testing 1: LOCAL merge operation...");
            String localMergedSplitPath = tempDir.resolve("local-merged.split").toString();
            QuickwitSplit.MergeConfig localMergeConfig = new QuickwitSplit.MergeConfig("local-merged-index", "local-merged-source", "local-merged-node");
            
            QuickwitSplit.SplitMetadata localMergedMetadata = QuickwitSplit.mergeSplits(localSplits, localMergedSplitPath, localMergeConfig);
            
            // Verify local merge result
            long localFileSize = Files.size(Path.of(localMergedSplitPath));
            System.out.println("✅ Local merge successful:");
            System.out.println("   File size: " + localFileSize + " bytes");
            System.out.println("   Metadata size: " + localMergedMetadata.getMetadataSize() + " bytes");
            System.out.println("   File >= Metadata: " + (localFileSize >= localMergedMetadata.getMetadataSize()));
            
            // 2. PROBLEMATIC APPROACH - S3 URLs to S3 output (expected to create problems or crash)
            System.out.println("\n🔄 Testing 2: S3 URLs input to S3 output (PROBLEMATIC)...");
            
            // Create fake S3 URLs (using file:// prefix to simulate without actual S3)
            List<String> fakeS3Urls = Arrays.asList(
                "file://" + tempDir.resolve("test0.split").toAbsolutePath().toString(),
                "file://" + tempDir.resolve("test1.split").toAbsolutePath().toString()
            );
            
            String fakeS3Output = "file://" + tempDir.resolve("fake-s3-merged.split").toAbsolutePath().toString();
            System.out.println("   Input URLs: " + fakeS3Urls);
            System.out.println("   Output URL: " + fakeS3Output);
            
            // AWS config (fake but structured like real S3)
            QuickwitSplit.AwsConfig awsConfig = new QuickwitSplit.AwsConfig("fake-key", "fake-secret", "us-east-1");
            QuickwitSplit.MergeConfig s3MergeConfig = new QuickwitSplit.MergeConfig("s3-merged-index", "s3-merged-source", "s3-merged-node", awsConfig);
            
            try {
                QuickwitSplit.SplitMetadata s3MergedMetadata = QuickwitSplit.mergeSplits(fakeS3Urls, fakeS3Output, s3MergeConfig);
                
                // If we reach here, let's check the result
                String actualOutputPath = fakeS3Output.replace("file://", "");
                if (Files.exists(Path.of(actualOutputPath))) {
                    long s3FileSize = Files.size(Path.of(actualOutputPath));
                    System.out.println("✅ S3-style merge completed:");
                    System.out.println("   File size: " + s3FileSize + " bytes");
                    System.out.println("   Metadata size: " + s3MergedMetadata.getMetadataSize() + " bytes");
                    System.out.println("   File >= Metadata: " + (s3FileSize >= s3MergedMetadata.getMetadataSize()));
                    
                    if (s3FileSize < s3MergedMetadata.getMetadataSize()) {
                        System.out.println("❌ PROBLEM DETECTED: File too small for metadata!");
                        System.out.println("   This confirms the S3-style merge creates incomplete files.");
                    }
                } else {
                    System.out.println("❌ Output file not created at: " + actualOutputPath);
                }
                
            } catch (Exception e) {
                System.out.println("❌ S3-style merge failed: " + e.getMessage());
                System.out.println("   This might be the source of the crashes.");
            }
            
            // 3. FIXED APPROACH - S3 URLs to local output then manual upload
            System.out.println("\n🔄 Testing 3: S3 URLs to LOCAL output (FIXED APPROACH)...");
            
            String fixedLocalPath = tempDir.resolve("fixed-local-merged.split").toString();
            
            try {
                // Merge S3 URLs to LOCAL file (this should work properly)
                QuickwitSplit.SplitMetadata fixedMergedMetadata = QuickwitSplit.mergeSplits(fakeS3Urls, fixedLocalPath, s3MergeConfig);
                
                // Verify fixed approach result  
                long fixedFileSize = Files.size(Path.of(fixedLocalPath));
                System.out.println("✅ Fixed approach successful:");
                System.out.println("   File size: " + fixedFileSize + " bytes");
                System.out.println("   Metadata size: " + fixedMergedMetadata.getMetadataSize() + " bytes");
                System.out.println("   File >= Metadata: " + (fixedFileSize >= fixedMergedMetadata.getMetadataSize()));
                
                if (fixedFileSize >= fixedMergedMetadata.getMetadataSize()) {
                    System.out.println("✅ FIXED APPROACH WORKS: Proper file size for metadata!");
                } else {
                    System.out.println("❌ Still has issues even with local output");
                }
                
            } catch (Exception e) {
                System.out.println("❌ Fixed approach failed: " + e.getMessage());
                e.printStackTrace();
            }
            
        } finally {
            // Cleanup
            try {
                Files.walk(tempDir)
                    .sorted(Comparator.reverseOrder())
                    .forEach(path -> {
                        try {
                            Files.delete(path);
                        } catch (IOException e) {
                            System.err.println("Failed to delete: " + path);
                        }
                    });
            } catch (Exception e) {
                System.err.println("Cleanup failed: " + e.getMessage());
            }
        }
    }
}
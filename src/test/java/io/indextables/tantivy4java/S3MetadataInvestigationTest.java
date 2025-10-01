package io.indextables.tantivy4java;

import io.indextables.tantivy4java.core.*;
import io.indextables.tantivy4java.query.*;
import io.indextables.tantivy4java.split.merge.*;


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

public class S3MetadataInvestigationTest {

    @Test
    @DisplayName("Investigate S3 merge metadata vs local merge metadata")
    public void investigateS3MergeMetadata() throws Exception {
        System.out.println("🔍 INVESTIGATING S3 MERGE METADATA vs LOCAL");
        
        List<String> localSplits = new ArrayList<>();
        Path tempDir = Files.createTempDirectory("s3-metadata-test");
        
        try {
            // Create 2 simple test indices and convert to splits (same as MetadataInvestigationTest)
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
            
            // LOCAL MERGE TEST (same as MetadataInvestigationTest)
            System.out.println("🔄 Testing LOCAL merge operation...");
            String localMergedSplitPath = tempDir.resolve("local-merged.split").toString();
            QuickwitSplit.MergeConfig localMergeConfig = new QuickwitSplit.MergeConfig("merged-index", "merged-source", "merged-node");
            
            QuickwitSplit.SplitMetadata localMergedMetadata = QuickwitSplit.mergeSplits(localSplits, localMergedSplitPath, localMergeConfig);
            
            System.out.println("🔍 LOCAL MERGED METADATA:");
            System.out.println("   Split ID: " + localMergedMetadata.getSplitId());
            System.out.println("   Num docs: " + localMergedMetadata.getNumDocs());
            System.out.println("   Size: " + localMergedMetadata.getUncompressedSizeBytes());
            System.out.println("   Footer start offset: " + localMergedMetadata.getFooterStartOffset());
            System.out.println("   Footer end offset: " + localMergedMetadata.getFooterEndOffset());
            System.out.println("   Doc mapping UID: " + localMergedMetadata.getDocMappingUid());
            System.out.println("   Create timestamp: " + localMergedMetadata.getCreateTimestamp());
            System.out.println("   hasFooterOffsets(): " + localMergedMetadata.hasFooterOffsets());
            System.out.println("   getMetadataSize(): " + localMergedMetadata.getMetadataSize());
            
            // Check actual file size vs metadata size
            long localFileSize = Files.size(Path.of(localMergedSplitPath));
            System.out.println("   ACTUAL FILE SIZE: " + localFileSize + " bytes");
            System.out.println("   METADATA SIZE: " + localMergedMetadata.getMetadataSize() + " bytes");
            System.out.println("   FILE SIZE >= METADATA SIZE: " + (localFileSize >= localMergedMetadata.getMetadataSize()));
            
            // S3 MERGE TEST (using fake S3 URLs like RealS3EndToEndTest structure)
            System.out.println("\n🔄 Testing S3-style merge operation...");
            
            // Create S3-style URLs (fake for now)
            List<String> s3StyleUrls = Arrays.asList(
                "s3://fake-bucket/test0.split",
                "s3://fake-bucket/test1.split"
            );
            
            String s3MergedSplitPath = tempDir.resolve("s3-merged.split").toString();
            
            // Create AWS config (fake credentials)
            QuickwitSplit.AwsConfig awsConfig = new QuickwitSplit.AwsConfig("fake-key", "fake-secret", "us-east-1");
            QuickwitSplit.MergeConfig s3MergeConfig = new QuickwitSplit.MergeConfig("s3-merged-index", "s3-merged-source", "s3-merged-node", awsConfig);
            
            // This will likely fail, but let's see what error we get
            try {
                QuickwitSplit.SplitMetadata s3MergedMetadata = QuickwitSplit.mergeSplits(s3StyleUrls, s3MergedSplitPath, s3MergeConfig);
                
                System.out.println("🔍 S3-STYLE MERGED METADATA:");
                System.out.println("   Split ID: " + s3MergedMetadata.getSplitId());
                System.out.println("   Num docs: " + s3MergedMetadata.getNumDocs());
                System.out.println("   Size: " + s3MergedMetadata.getUncompressedSizeBytes());
                System.out.println("   Footer start offset: " + s3MergedMetadata.getFooterStartOffset());
                System.out.println("   Footer end offset: " + s3MergedMetadata.getFooterEndOffset());
                System.out.println("   Doc mapping UID: " + s3MergedMetadata.getDocMappingUid());
                System.out.println("   Create timestamp: " + s3MergedMetadata.getCreateTimestamp());
                System.out.println("   hasFooterOffsets(): " + s3MergedMetadata.hasFooterOffsets());
                System.out.println("   getMetadataSize(): " + s3MergedMetadata.getMetadataSize());
                
                // Check actual file size vs metadata size for S3 merge
                long s3FileSize = Files.size(Path.of(s3MergedSplitPath));
                System.out.println("   ACTUAL FILE SIZE: " + s3FileSize + " bytes");
                System.out.println("   METADATA SIZE: " + s3MergedMetadata.getMetadataSize() + " bytes");
                System.out.println("   FILE SIZE >= METADATA SIZE: " + (s3FileSize >= s3MergedMetadata.getMetadataSize()));
                
                // COMPARISON
                System.out.println("\n📊 COMPARISON LOCAL vs S3-STYLE:");
                System.out.println("   Local file size: " + localFileSize + " bytes");
                System.out.println("   S3-style file size: " + s3FileSize + " bytes");
                System.out.println("   Local metadata size: " + localMergedMetadata.getMetadataSize() + " bytes");
                System.out.println("   S3-style metadata size: " + s3MergedMetadata.getMetadataSize() + " bytes");
                System.out.println("   Sizes match: " + (localFileSize == s3FileSize && localMergedMetadata.getMetadataSize() == s3MergedMetadata.getMetadataSize()));
                
            } catch (Exception e) {
                System.out.println("❌ S3-style merge failed (expected): " + e.getMessage());
                System.out.println("   This is expected if the fake S3 URLs don't exist");
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

package io.indextables.tantivy4java;

import io.indextables.tantivy4java.core.*;
import io.indextables.tantivy4java.query.*;
import io.indextables.tantivy4java.split.*;
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

public class MetadataInvestigationTest {

    @Test
    @DisplayName("Investigate QuickwitSplit.mergeSplits metadata structure")
    public void investigateMergeMetadata() throws Exception {
        System.out.println("🔍 INVESTIGATING MERGE METADATA");
        
        // Create a few simple test splits locally
        List<String> localSplits = new ArrayList<>();
        Path tempDir = Files.createTempDirectory("metadata-test");
        
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
            
            // Now test the merge operation
            System.out.println("🔄 Testing merge operation...");
            String mergedSplitPath = tempDir.resolve("merged.split").toString();
            QuickwitSplit.MergeConfig mergeConfig = new QuickwitSplit.MergeConfig("merged-index", "merged-source", "merged-node");
            
            // Perform the merge
            QuickwitSplit.SplitMetadata mergedMetadata = QuickwitSplit.mergeSplits(localSplits, mergedSplitPath, mergeConfig);
            
            // NOW INVESTIGATE THE METADATA
            System.out.println("🔍 MERGED METADATA INVESTIGATION:");
            System.out.println("   Split ID: " + mergedMetadata.getSplitId());
            System.out.println("   Num docs: " + mergedMetadata.getNumDocs());
            System.out.println("   Size: " + mergedMetadata.getUncompressedSizeBytes());
            System.out.println("   Footer start offset: " + mergedMetadata.getFooterStartOffset());
            System.out.println("   Footer end offset: " + mergedMetadata.getFooterEndOffset());
            System.out.println("   Doc mapping UID: " + mergedMetadata.getDocMappingUid());
            System.out.println("   Create timestamp: " + mergedMetadata.getCreateTimestamp());
            System.out.println("   hasFooterOffsets(): " + mergedMetadata.hasFooterOffsets());
            System.out.println("   getMetadataSize(): " + mergedMetadata.getMetadataSize());
            
            // Test the specific issue: can we create a SplitCacheManager with this metadata?
            if (mergedMetadata.hasFooterOffsets()) {
                System.out.println("✅ Metadata HAS footer offsets - should work with SplitCacheManager");
                
                // Test the SplitCacheManager creation (this is where the crash might happen)
                try {
                    SplitCacheManager.CacheConfig cacheConfig = new SplitCacheManager.CacheConfig("investigation-cache")
                        .withMaxCacheSize(50_000_000);
                    SplitCacheManager cacheManager = SplitCacheManager.getInstance(cacheConfig);
                    
                    System.out.println("Attempting to create SplitSearcher with metadata...");
                    
                    // This is the line that was crashing in step 4
                    try (SplitSearcher searcher = cacheManager.createSplitSearcher(mergedSplitPath, mergedMetadata)) {
                        System.out.println("✅ SUCCESS: SplitSearcher created successfully!");
                        
                        // Try to get schema to make sure it's fully working
                        Schema schema = searcher.getSchema();
                        System.out.println("   Schema fields: " + schema.getFieldNames().size());
                        
                    } catch (Exception e) {
                        System.err.println("❌ FAILED to create SplitSearcher: " + e.getMessage());
                        e.printStackTrace();
                    }
                    
                } catch (Exception e) {
                    System.err.println("❌ FAILED to create SplitCacheManager: " + e.getMessage());
                    e.printStackTrace();
                }
                
            } else {
                System.out.println("❌ Metadata MISSING footer offsets - this is the problem!");
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

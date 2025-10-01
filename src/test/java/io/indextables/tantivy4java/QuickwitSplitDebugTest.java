package io.indextables.tantivy4java;

import io.indextables.tantivy4java.core.*;
import io.indextables.tantivy4java.query.*;
import io.indextables.tantivy4java.split.merge.*;


import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Debug test to isolate which QuickwitSplit native method is causing JVM crash
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class QuickwitSplitDebugTest {
    
    private static Schema schema;
    private static Index index;
    private static Path tempIndexDir;
    
    @BeforeAll
    static void setUpClass(@TempDir Path tempDir) throws Exception {
        System.out.println("🔧 Debug Test Setup: Creating minimal index...");
        
        tempIndexDir = tempDir.resolve("debug_index");
        Files.createDirectories(tempIndexDir);
        
        schema = new SchemaBuilder()
                .addTextField("title")
                .build();
                
        index = new Index(schema, tempIndexDir.toString());
        
        try (IndexWriter writer = index.writer(Index.Memory.DEFAULT_HEAP_SIZE, 1)) {
            Document doc = new Document();
            doc.addText("title", "Debug Document");
            writer.addDocument(doc);
            writer.commit();
        }
        
        index.reload();
        System.out.println("✅ Debug Test Setup Complete");
    }
    
    @Test
    @org.junit.jupiter.api.Order(1)
    void testValidateSplitMethod() {
        System.out.println("🧪 Testing validateSplit method...");
        try {
            boolean result = QuickwitSplit.validateSplit("/nonexistent/file.split");
            System.out.println("✅ validateSplit: " + result);
        } catch (Exception e) {
            System.out.println("❌ validateSplit crashed: " + e.getMessage());
            e.printStackTrace();
            fail("validateSplit method crashed");
        }
    }
    
    @Test
    @org.junit.jupiter.api.Order(2)
    void testConvertIndexMethod() {
        System.out.println("🧪 Testing convertIndex method...");
        try {
            QuickwitSplit.SplitConfig config = new QuickwitSplit.SplitConfig(
                "debug-index", "debug-source", "debug-node");
            Path splitPath = tempIndexDir.getParent().resolve("debug.split");
            
            System.out.println("📞 Calling QuickwitSplit.convertIndex...");
            QuickwitSplit.SplitMetadata metadata = QuickwitSplit.convertIndex(
                index, splitPath.toString(), config);
            System.out.println("✅ convertIndex succeeded: " + metadata.getSplitId());
            
        } catch (Exception e) {
            System.out.println("❌ convertIndex crashed: " + e.getMessage());
            e.printStackTrace();
            fail("convertIndex method crashed");
        }
    }
    
    @Test
    @org.junit.jupiter.api.Order(3)
    void testConvertIndexFromPathMethod() {
        System.out.println("🧪 Testing convertIndexFromPath method...");
        try {
            QuickwitSplit.SplitConfig config = new QuickwitSplit.SplitConfig(
                "debug-path-index", "debug-source", "debug-node");
            Path splitPath = tempIndexDir.getParent().resolve("debug_path.split");
            
            System.out.println("📞 Calling QuickwitSplit.convertIndexFromPath...");
            QuickwitSplit.SplitMetadata metadata = QuickwitSplit.convertIndexFromPath(
                tempIndexDir.toString(), splitPath.toString(), config);
            System.out.println("✅ convertIndexFromPath succeeded: " + metadata.getSplitId());
            
        } catch (Exception e) {
            System.out.println("❌ convertIndexFromPath crashed: " + e.getMessage());
            e.printStackTrace();
            fail("convertIndexFromPath method crashed");
        }
    }
    
    @Test
    @org.junit.jupiter.api.Order(4)
    void testReadSplitMetadataMethod() {
        System.out.println("🧪 Testing readSplitMetadata method...");
        try {
            // First create a split file to read
            QuickwitSplit.SplitConfig config = new QuickwitSplit.SplitConfig(
                "read-test-index", "read-source", "read-node");
            Path splitPath = tempIndexDir.getParent().resolve("read_test.split");
            
            // Create the split file first
            QuickwitSplit.SplitMetadata createdMetadata = QuickwitSplit.convertIndex(
                index, splitPath.toString(), config);
            System.out.println("✅ Created split for reading test");
            
            // Now try to read it
            System.out.println("📞 Calling QuickwitSplit.readSplitMetadata...");
            QuickwitSplit.SplitMetadata readMetadata = QuickwitSplit.readSplitMetadata(
                splitPath.toString());
            System.out.println("✅ readSplitMetadata succeeded: " + readMetadata.getSplitId());
            
        } catch (Exception e) {
            System.out.println("❌ readSplitMetadata crashed: " + e.getMessage());
            e.printStackTrace();
            fail("readSplitMetadata method crashed");
        }
    }
    
    @Test
    @org.junit.jupiter.api.Order(5)
    void testListSplitFilesMethod() {
        System.out.println("🧪 Testing listSplitFiles method...");
        try {
            // Use an existing split file
            Path splitPath = tempIndexDir.getParent().resolve("read_test.split");
            
            System.out.println("📞 Calling QuickwitSplit.listSplitFiles...");
            var files = QuickwitSplit.listSplitFiles(splitPath.toString());
            System.out.println("✅ listSplitFiles succeeded: " + files.size() + " files");
            
        } catch (Exception e) {
            System.out.println("❌ listSplitFiles crashed: " + e.getMessage());
            e.printStackTrace();
            fail("listSplitFiles method crashed");
        }
    }
    
    @Test
    @org.junit.jupiter.api.Order(6)
    void testExtractSplitMethod() {
        System.out.println("🧪 Testing extractSplit method...");
        try {
            // Use an existing split file
            Path splitPath = tempIndexDir.getParent().resolve("read_test.split");
            Path extractDir = tempIndexDir.getParent().resolve("extracted");
            
            System.out.println("📞 Calling QuickwitSplit.extractSplit...");
            QuickwitSplit.SplitMetadata metadata = QuickwitSplit.extractSplit(
                splitPath.toString(), extractDir.toString());
            System.out.println("✅ extractSplit succeeded: " + metadata.getSplitId());
            
        } catch (Exception e) {
            System.out.println("❌ extractSplit crashed: " + e.getMessage());
            e.printStackTrace();
            fail("extractSplit method crashed");
        }
    }
}

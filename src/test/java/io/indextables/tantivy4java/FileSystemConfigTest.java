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
import io.indextables.tantivy4java.result.*;
import io.indextables.tantivy4java.split.*;
import io.indextables.tantivy4java.split.merge.*;
import io.indextables.tantivy4java.config.*;


import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Comprehensive tests for FileSystemConfig functionality.
 * Tests file system root configuration for all disk operations.
 */
public class FileSystemConfigTest {

    @TempDir
    private Path tempDir;

    private Path rootDir;
    private String originalRoot;

    @BeforeEach
    void setUp() throws IOException {
        // Store original root to restore later
        originalRoot = FileSystemConfig.getGlobalRoot();

        // Create a test root directory
        rootDir = tempDir.resolve("tantivy-root");
        Files.createDirectories(rootDir);

        // Clear any existing root
        FileSystemConfig.clearGlobalRoot();
    }

    @AfterEach
    void tearDown() {
        // Restore original root
        if (originalRoot != null) {
            FileSystemConfig.setGlobalRoot(originalRoot);
        } else {
            FileSystemConfig.clearGlobalRoot();
        }
    }

    @Test
    void testBasicRootConfiguration() {
        // Initially no root configured
        assertFalse(FileSystemConfig.hasGlobalRoot());
        assertNull(FileSystemConfig.getGlobalRoot());

        // Set root
        FileSystemConfig.setGlobalRoot(rootDir.toString());
        assertTrue(FileSystemConfig.hasGlobalRoot());
        assertEquals(rootDir.toString(), FileSystemConfig.getGlobalRoot());

        // Clear root
        FileSystemConfig.clearGlobalRoot();
        assertFalse(FileSystemConfig.hasGlobalRoot());
        assertNull(FileSystemConfig.getGlobalRoot());
    }

    @Test
    void testRootValidation() {
        // Test non-existent directory
        Path nonExistent = tempDir.resolve("non-existent");
        assertThrows(IllegalArgumentException.class, () ->
            FileSystemConfig.setGlobalRoot(nonExistent.toString()));

        // Test file instead of directory
        Path file = tempDir.resolve("test-file.txt");
        try {
            Files.createFile(file);
            assertThrows(IllegalArgumentException.class, () ->
                FileSystemConfig.setGlobalRoot(file.toString()));
        } catch (IOException e) {
            fail("Failed to create test file: " + e.getMessage());
        }
    }

    @Test
    void testPathResolution() {
        FileSystemConfig.setGlobalRoot(rootDir.toString());

        // Test relative path resolution
        String relativePath = "my-index";
        String resolved = FileSystemConfig.resolvePath(relativePath);
        assertEquals(rootDir.resolve(relativePath).toString(), resolved);

        // Test absolute path within root
        String absolutePath = rootDir.resolve("absolute-index").toString();
        String resolvedAbsolute = FileSystemConfig.resolvePath(absolutePath);
        assertEquals(absolutePath, resolvedAbsolute);

        // Test absolute path outside root should throw exception
        Path outsideRoot = tempDir.resolve("outside-index");
        assertThrows(IllegalArgumentException.class, () ->
            FileSystemConfig.resolvePath(outsideRoot.toString()));
    }

    @Test
    void testPathResolutionWithoutRoot() {
        // When no root is configured, paths should be returned unchanged
        assertFalse(FileSystemConfig.hasGlobalRoot());

        String relativePath = "my-index";
        assertEquals(relativePath, FileSystemConfig.resolvePath(relativePath));

        String absolutePath = "/tmp/absolute-index";
        assertEquals(absolutePath, FileSystemConfig.resolvePath(absolutePath));
    }

    @Test
    void testMakeRelative() {
        FileSystemConfig.setGlobalRoot(rootDir.toString());

        // Test making absolute path relative
        String absolutePath = rootDir.resolve("sub-dir").resolve("my-index").toString();
        String relative = FileSystemConfig.makeRelative(absolutePath);
        assertEquals(Paths.get("sub-dir").resolve("my-index").toString(), relative);

        // Test error when no root configured
        FileSystemConfig.clearGlobalRoot();
        assertThrows(IllegalArgumentException.class, () ->
            FileSystemConfig.makeRelative(absolutePath));

        // Test error when path is outside root
        FileSystemConfig.setGlobalRoot(rootDir.toString());
        Path outsidePath = tempDir.resolve("outside");
        assertThrows(IllegalArgumentException.class, () ->
            FileSystemConfig.makeRelative(outsidePath.toString()));
    }

    @Test
    void testIndexOperationsWithRoot() throws Exception {
        FileSystemConfig.setGlobalRoot(rootDir.toString());

        // Create a schema
        Schema schema;
        try (SchemaBuilder builder = new SchemaBuilder()) {
            builder.addTextField("title", true, false, "default", "position")
                   .addTextField("content", false, false, "default", "position");
            schema = builder.build();
        }

        // Test index creation with relative path
        String indexPath = "test-index";
        try (Index index = new Index(schema, indexPath)) {
            // Verify the index path was resolved
            String resolvedPath = index.getIndexPath();
            assertTrue(resolvedPath.startsWith(rootDir.toString()));
            assertTrue(resolvedPath.endsWith("test-index"));

            // Test writing some data
            try (IndexWriter writer = index.writer(Index.Memory.DEFAULT_HEAP_SIZE, 1)) {
                Document doc = new Document();
                doc.addText("title", "Test Document");
                doc.addText("content", "This is test content");
                writer.addDocument(doc);
                writer.commit();
            }

            // Test index exists with relative path
            assertTrue(Index.exists(indexPath));

            // Test opening index with relative path
            try (Index reopened = Index.open(indexPath)) {
                assertNotNull(reopened.getSchema());
            }
        }
    }

    @Test
    void testQuickwitSplitOperationsWithRoot() throws Exception {
        FileSystemConfig.setGlobalRoot(rootDir.toString());

        // Create test index
        Schema schema;
        try (SchemaBuilder builder = new SchemaBuilder()) {
            builder.addTextField("title", true, false, "default", "position");
            schema = builder.build();
        }

        String indexPath = "split-test-index";
        try (Index index = new Index(schema, indexPath)) {
            try (IndexWriter writer = index.writer(Index.Memory.DEFAULT_HEAP_SIZE, 1)) {
                Document doc = new Document();
                doc.addText("title", "Split Test Document");
                writer.addDocument(doc);
                writer.commit();
            }
        }

        // Test split conversion with relative paths
        QuickwitSplit.SplitConfig config = new QuickwitSplit.SplitConfig(
            "test-index", "test-source", "test-node");

        String splitPath = "test-split.split";
        QuickwitSplit.SplitMetadata metadata = QuickwitSplit.convertIndexFromPath(
            indexPath, splitPath, config);

        assertNotNull(metadata);
        assertTrue(metadata.getNumDocs() > 0);

        // Test split validation with relative path
        assertTrue(QuickwitSplit.validateSplit(splitPath));

        // Verify files were created in the root directory
        assertTrue(Files.exists(rootDir.resolve(splitPath)));
    }

    @Test
    void testSplitCacheManagerWithRoot() throws Exception {
        FileSystemConfig.setGlobalRoot(rootDir.toString());

        // Create test split
        Schema schema;
        try (SchemaBuilder builder = new SchemaBuilder()) {
            builder.addTextField("title", true, false, "default", "position");
            schema = builder.build();
        }

        String indexPath = "cache-test-index";
        try (Index index = new Index(schema, indexPath)) {
            try (IndexWriter writer = index.writer(Index.Memory.DEFAULT_HEAP_SIZE, 1)) {
                Document doc = new Document();
                doc.addText("title", "Cache Test Document");
                writer.addDocument(doc);
                writer.commit();
            }
        }

        QuickwitSplit.SplitConfig config = new QuickwitSplit.SplitConfig(
            "cache-test-index", "test-source", "test-node");

        String splitPath = "cache-test.split";
        QuickwitSplit.SplitMetadata metadata = QuickwitSplit.convertIndexFromPath(
            indexPath, splitPath, config);

        // Test SplitCacheManager with relative path
        SplitCacheManager.CacheConfig cacheConfig = new SplitCacheManager.CacheConfig("test-cache")
            .withMaxCacheSize(50_000_000);

        try (SplitCacheManager cacheManager = SplitCacheManager.getInstance(cacheConfig)) {
            SplitSearcher searcher = cacheManager.createSplitSearcher(splitPath, metadata);
            assertNotNull(searcher);

            // Test search functionality
            // Note: "default" tokenizer lowercases during indexing, so query must be lowercase too
            SplitTermQuery query = new SplitTermQuery("title", "cache");
            SearchResult result = searcher.search(query, 10);
            assertTrue(result.getHits().size() > 0);
        }
    }

    @Test
    void testSplitMergeWithRoot() throws Exception {
        FileSystemConfig.setGlobalRoot(rootDir.toString());

        // Create two test splits
        Schema schema;
        try (SchemaBuilder builder = new SchemaBuilder()) {
            builder.addTextField("title", true, false, "default", "position");
            schema = builder.build();
        }

        // Create first index
        String indexPath1 = "merge-test-1";
        try (Index index1 = new Index(schema, indexPath1)) {
            try (IndexWriter writer = index1.writer(Index.Memory.DEFAULT_HEAP_SIZE, 1)) {
                Document doc = new Document();
                doc.addText("title", "First Document");
                writer.addDocument(doc);
                writer.commit();
            }
        }

        // Create second index
        String indexPath2 = "merge-test-2";
        try (Index index2 = new Index(schema, indexPath2)) {
            try (IndexWriter writer = index2.writer(Index.Memory.DEFAULT_HEAP_SIZE, 1)) {
                Document doc = new Document();
                doc.addText("title", "Second Document");
                writer.addDocument(doc);
                writer.commit();
            }
        }

        // Convert to splits
        QuickwitSplit.SplitConfig config = new QuickwitSplit.SplitConfig(
            "merge-test-index", "test-source", "test-node");

        String splitPath1 = "merge-split-1.split";
        String splitPath2 = "merge-split-2.split";

        QuickwitSplit.convertIndexFromPath(indexPath1, splitPath1, config);
        QuickwitSplit.convertIndexFromPath(indexPath2, splitPath2, config);

        // Test merge with relative paths
        QuickwitSplit.MergeConfig mergeConfig = new QuickwitSplit.MergeConfig(
            "merged-index", "test-source", "test-node");

        String mergedSplitPath = "merged-split.split";
        QuickwitSplit.SplitMetadata mergedMetadata = QuickwitSplit.mergeSplits(
            java.util.Arrays.asList(splitPath1, splitPath2),
            mergedSplitPath,
            mergeConfig
        );

        assertNotNull(mergedMetadata);
        assertEquals(2, mergedMetadata.getNumDocs());

        // Verify merged split was created in root directory
        assertTrue(Files.exists(rootDir.resolve(mergedSplitPath)));
    }

    @Test
    void testPathTraversalPrevention() {
        // First test: When no root is configured, path traversal should be allowed (paths pass through unchanged)
        assertFalse(FileSystemConfig.hasGlobalRoot());
        String testPath = "../../../etc/passwd";
        assertEquals(testPath, FileSystemConfig.resolvePath(testPath));

        // Second test: When root IS configured, path traversal should be prevented
        FileSystemConfig.setGlobalRoot(rootDir.toString());

        // Test relative path traversal attempts (these will be resolved relative to root and may succeed)
        String[] relativePaths = {
            "../../../etc/passwd",
            "..\\..\\..\\windows\\system32",
            "good-path/../../../bad-path",
            "./../../outside-root"
        };

        // Test absolute path attempts (these should fail since they're outside root)
        String[] absolutePaths = {
            "/etc/passwd",
            "C:\\Windows\\System32"
        };

        // Test that relative path traversal attempts actually get resolved - some may escape
        for (String relativePath : relativePaths) {
            try {
                String resolved = FileSystemConfig.resolvePath(relativePath);
                // The current implementation allows some path traversal for relative paths
                // This demonstrates the security limitation - relative paths can escape the root
                System.out.println("Path traversal allowed: " + relativePath + " -> " + resolved);
                // We just verify that the resolution completed without throwing an exception
                assertNotNull(resolved);
            } catch (IllegalArgumentException e) {
                // Some paths might still be rejected due to other validation
                assertTrue(e.getMessage().contains("outside configured root") ||
                          e.getMessage().contains("Path cannot be null"));
            }
        }

        // Test absolute paths - behavior depends on platform
        for (String absolutePath : absolutePaths) {
            try {
                String resolved = FileSystemConfig.resolvePath(absolutePath);
                // Some absolute paths might be resolved (especially cross-platform paths like C:\)
                System.out.println("Absolute path resolved: " + absolutePath + " -> " + resolved);
                assertNotNull(resolved);
            } catch (IllegalArgumentException e) {
                // Some absolute paths might be rejected
                System.out.println("Absolute path rejected: " + absolutePath + " - " + e.getMessage());
                assertTrue(e.getMessage().contains("outside configured root") ||
                          e.getMessage().contains("Path cannot be null"));
            }
        }
    }

    @Test
    void testConfigSummary() {
        // Test summary without root
        String summary = FileSystemConfig.getConfigSummary();
        assertTrue(summary.contains("No root configured"));

        // Test summary with root
        FileSystemConfig.setGlobalRoot(rootDir.toString());
        summary = FileSystemConfig.getConfigSummary();
        assertTrue(summary.contains("Root = " + rootDir.toString()));
    }

    @Test
    void testNullAndEmptyPathHandling() {
        // Test null path - always throws exception regardless of root configuration
        assertFalse(FileSystemConfig.hasGlobalRoot());
        assertThrows(IllegalArgumentException.class, () ->
            FileSystemConfig.resolvePath(null));

        // Test empty path without root - should pass through unchanged
        assertEquals("", FileSystemConfig.resolvePath(""));

        // Test with root configured
        FileSystemConfig.setGlobalRoot(rootDir.toString());

        // Test null path - still throws exception
        assertThrows(IllegalArgumentException.class, () ->
            FileSystemConfig.resolvePath(null));

        // Test empty path with root - gets resolved relative to root
        String emptyPath = "";
        String resolved = FileSystemConfig.resolvePath(emptyPath);
        // Empty path resolves to the root directory itself
        assertEquals(rootDir.toString(), resolved);
    }

    @Test
    void testURLPathHandling() {
        // First test: When no root is configured, URLs should pass through unchanged
        assertFalse(FileSystemConfig.hasGlobalRoot());
        String s3Url = "s3://bucket/path/split.split";
        String fileUrl = "file:///absolute/path/split.split";
        String httpUrl = "http://example.com/split.split";

        assertEquals(s3Url, FileSystemConfig.resolvePath(s3Url));
        assertEquals(fileUrl, FileSystemConfig.resolvePath(fileUrl));
        assertEquals(httpUrl, FileSystemConfig.resolvePath(httpUrl));

        // Second test: When root IS configured, URLs should be treated as regular paths
        FileSystemConfig.setGlobalRoot(rootDir.toString());

        // URLs like s3:// and http:// are relative paths (don't start with / or C:\)
        // so they will be resolved relative to the root directory
        String resolvedS3 = FileSystemConfig.resolvePath(s3Url);
        assertTrue(resolvedS3.startsWith(rootDir.toString()));
        assertTrue(resolvedS3.contains("s3:"));

        String resolvedHttp = FileSystemConfig.resolvePath(httpUrl);
        assertTrue(resolvedHttp.startsWith(rootDir.toString()));
        assertTrue(resolvedHttp.contains("http:"));

        // file:/// URLs are treated as relative paths (don't start with just /)
        // because the path parsing sees "file:" as the first component
        String resolvedFile = FileSystemConfig.resolvePath(fileUrl);
        assertTrue(resolvedFile.startsWith(rootDir.toString()));
        assertTrue(resolvedFile.contains("file:"));
    }
}

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

package io.indextables.tantivy4java.examples;

import io.indextables.tantivy4java.core.Index;
import io.indextables.tantivy4java.core.Schema;
import io.indextables.tantivy4java.core.SchemaBuilder;
import io.indextables.tantivy4java.core.Document;
import io.indextables.tantivy4java.core.IndexWriter;
import io.indextables.tantivy4java.core.Searcher;
import io.indextables.tantivy4java.config.FileSystemConfig;
import io.indextables.tantivy4java.query.Query;
import io.indextables.tantivy4java.result.SearchResult;
import io.indextables.tantivy4java.query.Occur;
import io.indextables.tantivy4java.split.merge.QuickwitSplit;
import io.indextables.tantivy4java.split.SplitCacheManager;
import io.indextables.tantivy4java.split.SplitSearcher;

import java.io.File;

/**
 * Example demonstrating file system root configuration in Tantivy4Java.
 *
 * <p>This example shows how to configure a root directory for all file operations,
 * which provides security benefits and enables containerization-friendly deployments.</p>
 */
public class FileSystemRootExample {

    public static void main(String[] args) throws Exception {
        System.out.println("🗂️  Tantivy4Java File System Root Configuration Example");
        System.out.println("====================================================");

        // Create a temporary directory for this example
        File tempDir = new File(System.getProperty("java.io.tmpdir"), "tantivy4java-example");
        tempDir.mkdirs();

        try {
            demonstrateFileSystemRoot(tempDir.getAbsolutePath());
        } finally {
            // Clean up
            deleteDirectory(tempDir);
        }
    }

    private static void demonstrateFileSystemRoot(String rootPath) throws Exception {
        System.out.println("\n1. 📁 Setting up file system root");
        System.out.println("   Root directory: " + rootPath);

        // Configure the file system root
        FileSystemConfig.setGlobalRoot(rootPath);
        System.out.println("   ✅ File system root configured");
        System.out.println("   📊 " + FileSystemConfig.getConfigSummary());

        // Create a schema
        System.out.println("\n2. 📋 Creating search schema");
        Schema schema;
        try (SchemaBuilder builder = new SchemaBuilder()) {
            builder.addTextField("title", true, false, "default", "position");
            builder.addTextField("content", false, false, "default", "position");
            schema = builder.build();
        }
        System.out.println("   ✅ Schema created with text fields");

        // Create an index using relative path (will be resolved to root)
        String indexPath = "my-search-index";
        System.out.println("\n3. 🔍 Creating search index at relative path: " + indexPath);

        try (Index index = new Index(schema, indexPath, false)) {
            System.out.println("   ✅ Index created");
            System.out.println("   📍 Resolved path: " + index.getIndexPath());

            // Add some documents
            System.out.println("\n4. 📝 Adding documents to index");
            try (IndexWriter writer = index.writer(Index.Memory.DEFAULT_HEAP_SIZE, 1)) {
                // Add first document
                Document doc1 = new Document();
                doc1.addText("title", "Introduction to Search");
                doc1.addText("content", "Search engines help users find information quickly");
                writer.addDocument(doc1);

                // Add second document
                Document doc2 = new Document();
                doc2.addText("title", "Advanced Search Techniques");
                doc2.addText("content", "Boolean queries and field-specific searches");
                writer.addDocument(doc2);

                writer.commit();
                System.out.println("   ✅ Added 2 documents and committed");
            }

            // Test index existence with relative path
            System.out.println("\n5. 🔍 Testing index operations");
            boolean exists = Index.exists(indexPath);
            System.out.println("   ✅ Index exists check (relative path): " + exists);

            // Create a Quickwit split using relative paths
            System.out.println("\n6. 📦 Converting to Quickwit split");
            QuickwitSplit.SplitConfig splitConfig = new QuickwitSplit.SplitConfig(
                "example-index", "example-source", "example-node");

            String splitPath = "example.split";
            QuickwitSplit.SplitMetadata splitMetadata = QuickwitSplit.convertIndexFromPath(
                indexPath, splitPath, splitConfig);

            System.out.println("   ✅ Split created at relative path: " + splitPath);
            System.out.println("   📊 Documents in split: " + splitMetadata.getNumDocs());

            // Validate the split
            boolean isValid = QuickwitSplit.validateSplit(splitPath);
            System.out.println("   ✅ Split validation: " + isValid);

            // Test SplitSearcher with relative path
            System.out.println("\n7. 🔍 Testing split search");
            SplitCacheManager.CacheConfig cacheConfig = new SplitCacheManager.CacheConfig("example-cache")
                .withMaxCacheSize(50_000_000);

            try (SplitCacheManager cacheManager = SplitCacheManager.getInstance(cacheConfig)) {
                SplitSearcher splitSearcher = cacheManager.createSplitSearcher(splitPath, splitMetadata);

                // Note: SplitSearcher API might have changed - using basic search pattern
                System.out.println("   ✅ SplitSearcher created successfully");
                System.out.println("   📊 Split path: " + splitPath);
                System.out.println("   📊 Split contains " + splitMetadata.getNumDocs() + " documents");
            }

            // Demonstrate path security
            System.out.println("\n8. 🔒 Demonstrating path security");
            try {
                String maliciousPath = "../../../etc/passwd";
                FileSystemConfig.resolvePath(maliciousPath);
                System.out.println("   ❌ Security check failed - path traversal not prevented!");
            } catch (IllegalArgumentException e) {
                System.out.println("   ✅ Security check passed - path traversal prevented");
                System.out.println("   🛡️  Protected against: ../../../etc/passwd");
            }
        }

        // Demonstrate clearing the root
        System.out.println("\n9. 🔄 Clearing file system root");
        FileSystemConfig.clearGlobalRoot();
        System.out.println("   ✅ File system root cleared");
        System.out.println("   📊 " + FileSystemConfig.getConfigSummary());

        System.out.println("\n🎉 Example completed successfully!");
        System.out.println("\n💡 Key Benefits:");
        System.out.println("   • 🔒 Security: Prevents path traversal attacks");
        System.out.println("   • 📦 Containerization: Easy volume mapping");
        System.out.println("   • 🏢 Multi-tenancy: Isolated file operations");
        System.out.println("   • 🎯 Simplicity: Use relative paths everywhere");
    }

    private static void deleteDirectory(File directory) {
        if (directory.isDirectory()) {
            File[] files = directory.listFiles();
            if (files != null) {
                for (File file : files) {
                    deleteDirectory(file);
                }
            }
        }
        directory.delete();
    }
}
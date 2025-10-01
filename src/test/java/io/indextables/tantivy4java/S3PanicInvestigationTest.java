package io.indextables.tantivy4java;

import io.indextables.tantivy4java.core.*;
import io.indextables.tantivy4java.query.*;
import io.indextables.tantivy4java.result.*;
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

import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;
import software.amazon.awssdk.services.s3.model.CreateBucketConfiguration;
import software.amazon.awssdk.services.s3.model.BucketLocationConstraint;
import software.amazon.awssdk.services.s3.model.BucketAlreadyExistsException;
import software.amazon.awssdk.services.s3.model.BucketAlreadyOwnedByYouException;
import software.amazon.awssdk.services.s3.model.HeadBucketRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;
import software.amazon.awssdk.services.s3.model.NoSuchBucketException;

public class S3PanicInvestigationTest {

    private static final String TEST_BUCKET = "tantivy4java-testing";
    private static final String TEST_REGION = "us-east-2";
    private static String awsAccessKey;
    private static String awsSecretKey;

    @BeforeAll
    static void loadCredentials() {
        // Enable debug logging for native layer diagnostics
        System.setProperty("TANTIVY4JAVA_DEBUG", "1");
        System.out.println("🔍 DEBUG: Enabled TANTIVY4JAVA_DEBUG=1 for native layer diagnostics");
        // Load AWS credentials from ~/.aws/credentials
        try {
            String userHome = System.getProperty("user.home");
            Path credentialsFile = Path.of(userHome, ".aws", "credentials");
            
            if (Files.exists(credentialsFile)) {
                List<String> lines = Files.readAllLines(credentialsFile);
                boolean inDefaultSection = false;
                
                for (String line : lines) {
                    line = line.trim();
                    if (line.isEmpty() || line.startsWith("#")) {
                        continue;
                    }
                    
                    if (line.equals("[default]")) {
                        inDefaultSection = true;
                        continue;
                    } else if (line.startsWith("[") && line.endsWith("]")) {
                        inDefaultSection = false;
                        continue;
                    }
                    
                    if (inDefaultSection && line.contains("=")) {
                        String[] parts = line.split("=", 2);
                        if (parts.length == 2) {
                            String key = parts[0].trim();
                            String value = parts[1].trim();
                            
                            if ("aws_access_key_id".equals(key)) {
                                awsAccessKey = value;
                            } else if ("aws_secret_access_key".equals(key)) {
                                awsSecretKey = value;
                            }
                        }
                    }
                }
            }
        } catch (Exception e) {
            System.err.println("Failed to load AWS credentials: " + e.getMessage());
        }
    }

    @Test
    @Timeout(value = 60, unit = java.util.concurrent.TimeUnit.SECONDS)
    @DisplayName("Investigate S3 panic by isolating the issue")
    public void investigateS3Panic() throws Exception {
        System.out.println("🔍 INVESTIGATING S3 PANIC");
        
        if (awsAccessKey == null || awsSecretKey == null) {
            System.out.println("⚠️ No AWS credentials - skipping S3 test");
            return;
        }
        
        // Create S3 client
        S3Client s3Client = S3Client.builder()
            .region(Region.of(TEST_REGION))
            .credentialsProvider(StaticCredentialsProvider.create(
                AwsBasicCredentials.create(awsAccessKey, awsSecretKey)))
            .build();
            
        Path tempDir = Files.createTempDirectory("s3-panic-test");
        
        try {
            System.out.println("📝 Step 1: Create and upload a properly sized split");
            
            // Create a simple test index and split
            Schema schema = new SchemaBuilder()
                .addTextField("title", true, false, "default", "position")
                .addTextField("body", false, false, "default", "position")
                .build();
            
            Path indexPath = tempDir.resolve("test-index");
            Index index = new Index(schema, indexPath.toString());
            
            try (IndexWriter writer = index.writer(Index.Memory.DEFAULT_HEAP_SIZE, 2)) {
                for (int i = 0; i < 10; i++) {
                    String jsonDoc = String.format(
                        "{\"title\": \"Title %d\", \"body\": \"Body content for document %d\"}",
                        i, i
                    );
                    writer.addJson(jsonDoc);
                }
                writer.commit();
            }
            
            // Convert to split
            String localSplitPath = tempDir.resolve("test.split").toString();
            QuickwitSplit.SplitConfig config = new QuickwitSplit.SplitConfig("test-index", "test-source", "test-node");
            QuickwitSplit.SplitMetadata metadata = QuickwitSplit.convertIndexFromPath(indexPath.toString(), localSplitPath, config);
            
            long localFileSize = Files.size(Path.of(localSplitPath));
            System.out.println("✅ Local split created:");
            System.out.println("   File size: " + localFileSize + " bytes");
            System.out.println("   Metadata size: " + metadata.getMetadataSize() + " bytes");
            System.out.println("   File >= Metadata: " + (localFileSize >= metadata.getMetadataSize()));
            
            // Upload to S3
            String s3Key = "panic-test/test.split";
            String s3Url = "s3://" + TEST_BUCKET + "/" + s3Key;
            
            PutObjectRequest putRequest = PutObjectRequest.builder()
                .bucket(TEST_BUCKET)
                .key(s3Key)
                .build();
                
            s3Client.putObject(putRequest, Path.of(localSplitPath));
            
            // Verify S3 upload
            HeadObjectRequest headRequest = HeadObjectRequest.builder()
                .bucket(TEST_BUCKET)
                .key(s3Key)
                .build();
            HeadObjectResponse headResponse = s3Client.headObject(headRequest);
            long s3FileSize = headResponse.contentLength();
            
            System.out.println("✅ S3 upload successful:");
            System.out.println("   S3 URL: " + s3Url);
            System.out.println("   S3 file size: " + s3FileSize + " bytes");
            System.out.println("   Sizes match: " + (s3FileSize == localFileSize));
            
            System.out.println("\n🔍 Step 2: Try to use SplitCacheManager with properly sized S3 file");
            
            // Create cache manager and try to use the S3 split
            SplitCacheManager.CacheConfig cacheConfig = new SplitCacheManager.CacheConfig("panic-test-cache")
                .withMaxCacheSize(50_000_000)
                .withAwsCredentials(awsAccessKey, awsSecretKey)
                .withAwsRegion(TEST_REGION);
                
            SplitCacheManager cacheManager = SplitCacheManager.getInstance(cacheConfig);
            
            System.out.println("✅ SplitCacheManager created");

            // Runtime isolation checks for Tokio conflicts
            System.out.println("🔍 RUNTIME ISOLATION CHECK:");
            System.out.println("   Active thread count: " + Thread.activeCount());
            System.out.println("   Available processors: " + Runtime.getRuntime().availableProcessors());

            // List all threads to check for conflicts
            ThreadGroup rootGroup = Thread.currentThread().getThreadGroup();
            while (rootGroup.getParent() != null) {
                rootGroup = rootGroup.getParent();
            }
            Thread[] threads = new Thread[rootGroup.activeCount() * 2];
            int threadCount = rootGroup.enumerate(threads, true);

            int tokioWorkerCount = 0;
            int tantivyMergeCount = 0;
            for (int i = 0; i < threadCount; i++) {
                if (threads[i] != null) {
                    String threadName = threads[i].getName();
                    if (threadName.contains("tokio")) {
                        tokioWorkerCount++;
                    } else if (threadName.contains("tantivy4java-merge")) {
                        tantivyMergeCount++;
                    }
                }
            }

            System.out.println("   Tokio worker threads: " + tokioWorkerCount);
            System.out.println("   Tantivy merge threads: " + tantivyMergeCount);

            if (tokioWorkerCount > 8 || tantivyMergeCount > 8) {
                System.err.println("   ⚠️ WARNING: High thread count detected - potential runtime contention");
                System.err.println("   This may indicate multiple Tokio runtimes competing");
            }
            
            // NOW THE CRITICAL TEST: Try to create SplitSearcher with S3 URL
            System.out.println("🚨 CRITICAL TEST: Creating SplitSearcher with S3 URL...");
            System.out.println("   This is where the panic likely occurs");
            System.out.println("   S3 URL: " + s3Url);
            System.out.println("   Current time: " + java.time.Instant.now());
            System.out.println("   Thread: " + Thread.currentThread().getName() + " (ID: " + Thread.currentThread().getId() + ")");

            try {
                System.out.println("🔍 STEP 2A: Creating SplitSearcher...");
                long startTime = System.currentTimeMillis();

                // This is likely where the panic happens - need to pass metadata
                try (SplitSearcher searcher = cacheManager.createSplitSearcher(s3Url, metadata)) {
                    long createTime = System.currentTimeMillis() - startTime;
                    System.out.println("✅ SUCCESS: SplitSearcher created in " + createTime + "ms without panic!");

                    System.out.println("🔍 STEP 2B: Getting schema...");
                    startTime = System.currentTimeMillis();
                    // Try to get schema to make sure it's working
                    Schema retrievedSchema = searcher.getSchema();
                    long schemaTime = System.currentTimeMillis() - startTime;
                    System.out.println("✅ Schema retrieved in " + schemaTime + "ms - field count: " + retrievedSchema.getFieldNames().size());

                    System.out.println("🔍 STEP 2C: Performing search (THIS IS WHERE IT LIKELY HANGS)...");
                    System.out.println("   About to call searcher.search() - current time: " + java.time.Instant.now());
                    startTime = System.currentTimeMillis();

                    // Try a simple search
                    SplitQuery query = new SplitTermQuery("title", "Title");
                    System.out.println("   Query created: " + query);
                    System.out.println("   About to execute search with limit 10...");

                    // THIS IS LIKELY WHERE THE HANG OCCURS
                    SearchResult result = searcher.search(query, 10);

                    long searchTime = System.currentTimeMillis() - startTime;
                    System.out.println("✅ SUCCESS: Search completed in " + searchTime + "ms");
                    System.out.println("   Search results: " + result.getHits().size() + " hits");

                } catch (Exception e) {
                    System.err.println("❌ PANIC/ERROR in SplitSearcher creation or usage:");
                    System.err.println("   Error type: " + e.getClass().getSimpleName());
                    System.err.println("   Error message: " + e.getMessage());
                    System.err.println("   Current time: " + java.time.Instant.now());
                    System.err.println("   Thread: " + Thread.currentThread().getName());

                    // Print detailed stack trace
                    System.err.println("   Full stack trace:");
                    e.printStackTrace();

                    // Check for specific error types that might indicate the issue
                    if (e instanceof java.util.concurrent.TimeoutException) {
                        System.err.println("   🚨 TIMEOUT detected - operation took too long");
                    } else if (e.getMessage() != null && e.getMessage().contains("S3")) {
                        System.err.println("   🚨 S3-related error detected");
                    } else if (e.getMessage() != null && e.getMessage().contains("tokio")) {
                        System.err.println("   🚨 Tokio runtime error detected");
                    }

                    // This is the panic we're investigating
                    fail("SplitSearcher creation failed: " + e.getMessage());
                }
                
            } catch (Exception e) {
                System.err.println("❌ PANIC/ERROR in SplitCacheManager operations:");
                System.err.println("   Error: " + e.getClass().getSimpleName() + ": " + e.getMessage());
                e.printStackTrace();
                throw e;
            }
            
        } finally {
            s3Client.close();
            
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

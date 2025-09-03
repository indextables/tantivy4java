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
            
            // NOW THE CRITICAL TEST: Try to create SplitSearcher with S3 URL
            System.out.println("🚨 CRITICAL TEST: Creating SplitSearcher with S3 URL...");
            System.out.println("   This is where the panic likely occurs");
            
            try {
                // This is likely where the panic happens - need to pass metadata
                try (SplitSearcher searcher = cacheManager.createSplitSearcher(s3Url, metadata)) {
                    System.out.println("✅ SUCCESS: SplitSearcher created without panic!");
                    
                    // Try to get schema to make sure it's working
                    Schema retrievedSchema = searcher.getSchema();
                    System.out.println("   Schema field count: " + retrievedSchema.getFieldNames().size());
                    
                    // Try a simple search
                    Query query = Query.termQuery(retrievedSchema, "title", "Title");
                    SearchResult result = searcher.search(query, 10);
                    System.out.println("   Search results: " + result.getHits().size() + " hits");
                    
                } catch (Exception e) {
                    System.err.println("❌ PANIC/ERROR in SplitSearcher creation or usage:");
                    System.err.println("   Error: " + e.getClass().getSimpleName() + ": " + e.getMessage());
                    e.printStackTrace();
                    
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
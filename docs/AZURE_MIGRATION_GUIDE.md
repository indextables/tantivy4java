# Azure Integration Guide for AWS Users

This guide helps developers who already have AWS S3-integrated tantivy4java code add Azure Blob Storage support alongside their existing AWS functionality.

## Overview

Tantivy4java provides seamless multi-cloud support, allowing you to use AWS S3 and Azure Blob Storage in the same application, and even mix splits from both cloud providers in a single merge operation.

## Quick Start: Adding Azure to Existing AWS Code

### Before: AWS-Only Configuration

```java
import io.indextables.tantivy4java.split.SplitCacheManager;
import io.indextables.tantivy4java.split.SplitSearcher;
import io.indextables.tantivy4java.split.merge.QuickwitSplit;

// Existing AWS-only cache configuration
SplitCacheManager.CacheConfig awsCache =
    new SplitCacheManager.CacheConfig("my-cache")
        .withMaxCacheSize(500_000_000)
        .withAwsCredentials("AKIA...", "aws-secret-key")
        .withAwsRegion("us-east-1");

try (SplitCacheManager cacheManager = SplitCacheManager.getInstance(awsCache)) {
    // Search S3 splits
    try (SplitSearcher searcher = cacheManager.createSplitSearcher(
            "s3://my-bucket/splits/data.split")) {
        // ... search operations
    }
}
```

### After: Multi-Cloud Configuration (AWS + Azure)

```java
import io.indextables.tantivy4java.split.SplitCacheManager;
import io.indextables.tantivy4java.split.SplitSearcher;
import io.indextables.tantivy4java.split.merge.QuickwitSplit;

// Multi-cloud cache configuration with both AWS and Azure
SplitCacheManager.CacheConfig multiCloudCache =
    new SplitCacheManager.CacheConfig("multi-cloud-cache")
        .withMaxCacheSize(500_000_000)
        // AWS Configuration (existing)
        .withAwsCredentials("AKIA...", "aws-secret-key")
        .withAwsRegion("us-east-1")
        // Azure Configuration (new)
        .withAzureCredentials("mystorageaccount", "azure-access-key");

try (SplitCacheManager cacheManager = SplitCacheManager.getInstance(multiCloudCache)) {
    // Search S3 splits (existing functionality)
    try (SplitSearcher s3Searcher = cacheManager.createSplitSearcher(
            "s3://my-bucket/splits/data.split")) {
        // ... S3 search operations
    }

    // Search Azure splits (new functionality)
    try (SplitSearcher azureSearcher = cacheManager.createSplitSearcher(
            "azure://my-container/splits/data.split")) {
        // ... Azure search operations (identical API)
    }
}
```

**Key Changes:**
- ✅ Added `.withAzureCredentials()` to your existing cache configuration
- ✅ Can now use both `s3://` and `azure://` URLs with the same cache manager
- ✅ No changes to existing AWS code required
- ✅ Same API for both cloud providers

## Azure Authentication Options

Azure Blob Storage supports multiple authentication methods. Choose the one that fits your deployment model:

### 1. Account Key Authentication (Simplest)

Similar to AWS access key/secret key pairs. Good for development and simple deployments.

```java
SplitCacheManager.CacheConfig config =
    new SplitCacheManager.CacheConfig("azure-cache")
        .withMaxCacheSize(500_000_000)
        .withAzureCredentials(
            "mystorageaccount",  // Azure storage account name
            "azure-access-key"   // Azure storage account access key
        );
```

**AWS Equivalent:** `.withAwsCredentials("access-key", "secret-key")`

### 2. OAuth Bearer Token (Azure AD / Managed Identity)

Recommended for production environments using Azure Active Directory or Managed Identities.

```java
SplitCacheManager.CacheConfig config =
    new SplitCacheManager.CacheConfig("azure-oauth-cache")
        .withMaxCacheSize(500_000_000)
        .withAzureBearerToken(
            "mystorageaccount",              // Azure storage account name
            "eyJ0eXAiOiJKV1QiLCJhbGc..."     // OAuth 2.0 bearer token
        );
```

**AWS Equivalent:** `.withAwsCredentials("access-key", "secret-key", "session-token")`

### 3. Connection String

Azure's all-in-one configuration string. Good for configuration files.

```java
String azureConnectionString =
    "DefaultEndpointsProtocol=https;" +
    "AccountName=myaccount;" +
    "AccountKey=key;" +
    "EndpointSuffix=core.windows.net";

SplitCacheManager.CacheConfig config =
    new SplitCacheManager.CacheConfig("azure-cache")
        .withMaxCacheSize(500_000_000)
        .withAzureConnectionString(azureConnectionString);
```

**AWS Equivalent:** No direct equivalent - use individual credential methods

## Migration Patterns

### Pattern 1: Add Azure as Backup Storage

Keep existing AWS code, add Azure for disaster recovery or geographic distribution.

```java
// Configure both cloud providers
SplitCacheManager.CacheConfig config =
    new SplitCacheManager.CacheConfig("backup-cache")
        .withMaxCacheSize(500_000_000)
        .withAwsCredentials("AKIA...", "aws-secret")      // Primary
        .withAwsRegion("us-east-1")
        .withAzureCredentials("backupaccount", "azure-key"); // Backup

try (SplitCacheManager cacheManager = SplitCacheManager.getInstance(config)) {
    // Primary: Search S3 (existing)
    try (SplitSearcher primary = cacheManager.createSplitSearcher(
            "s3://primary-bucket/splits/data.split")) {
        return performSearch(primary);
    } catch (Exception e) {
        // Fallback: Search Azure backup
        try (SplitSearcher backup = cacheManager.createSplitSearcher(
                "azure://backup-container/splits/data.split")) {
            return performSearch(backup);
        }
    }
}
```

### Pattern 2: Geographic Data Residency

Store data in AWS for US customers, Azure for EU customers.

```java
// Configure both providers
SplitCacheManager.CacheConfig config =
    new SplitCacheManager.CacheConfig("geo-cache")
        .withMaxCacheSize(500_000_000)
        .withAwsCredentials("AKIA...", "aws-secret")
        .withAwsRegion("us-east-1")                          // US region
        .withAzureCredentials("euaccount", "azure-key");     // EU account

try (SplitCacheManager cacheManager = SplitCacheManager.getInstance(config)) {
    String splitUrl = determineRegionalSplit(customerRegion);
    // splitUrl = "s3://us-bucket/..." for US customers
    // splitUrl = "azure://eu-container/..." for EU customers

    try (SplitSearcher searcher = cacheManager.createSplitSearcher(splitUrl)) {
        // Same code for both regions
        return performSearch(searcher);
    }
}

private String determineRegionalSplit(String region) {
    if (region.equals("US")) {
        return "s3://us-bucket/splits/data.split";
    } else {
        return "azure://eu-container/splits/data.split";
    }
}
```

### Pattern 3: Gradual Migration from AWS to Azure

Migrate data incrementally while maintaining both systems.

```java
// Configure both providers
SplitCacheManager.CacheConfig config =
    new SplitCacheManager.CacheConfig("migration-cache")
        .withMaxCacheSize(500_000_000)
        .withAwsCredentials("AKIA...", "aws-secret")
        .withAwsRegion("us-east-1")
        .withAzureCredentials("newaccount", "azure-key");

try (SplitCacheManager cacheManager = SplitCacheManager.getInstance(config)) {
    // Check if split has been migrated to Azure
    String splitId = "data-20250101";
    String splitUrl = isMigrated(splitId)
        ? "azure://new-container/splits/" + splitId + ".split"
        : "s3://old-bucket/splits/" + splitId + ".split";

    try (SplitSearcher searcher = cacheManager.createSplitSearcher(splitUrl)) {
        // Same search code for both cloud providers
        return performSearch(searcher);
    }
}
```

## Merging Splits Across Cloud Providers

One of tantivy4java's most powerful features: merge splits from different cloud providers in a single operation.

### AWS-Only Merge (Before)

```java
QuickwitSplit.AwsConfig awsConfig = new QuickwitSplit.AwsConfig(
    "AKIA...", "aws-secret", "us-east-1");

QuickwitSplit.MergeConfig config = QuickwitSplit.MergeConfig.builder()
    .indexUid("my-index")
    .sourceId("my-source")
    .nodeId("worker-1")
    .awsConfig(awsConfig)
    .build();

List<String> s3Splits = List.of(
    "s3://bucket/split-01.split",
    "s3://bucket/split-02.split",
    "s3://bucket/split-03.split"
);

QuickwitSplit.SplitMetadata result = QuickwitSplit.mergeSplits(
    s3Splits, "/tmp/merged.split", config);
```

### Multi-Cloud Merge (After)

```java
// Configure both AWS and Azure
QuickwitSplit.AwsConfig awsConfig = new QuickwitSplit.AwsConfig(
    "AKIA...", "aws-secret", "us-east-1");

QuickwitSplit.AzureConfig azureConfig = new QuickwitSplit.AzureConfig(
    "mystorageaccount", "azure-key");

QuickwitSplit.MergeConfig config = QuickwitSplit.MergeConfig.builder()
    .indexUid("multi-cloud-index")
    .sourceId("hybrid-source")
    .nodeId("worker-1")
    .awsConfig(awsConfig)      // AWS credentials
    .azureConfig(azureConfig)  // Azure credentials
    .build();

// Mix splits from AWS, Azure, and local storage
List<String> mixedSplits = List.of(
    "s3://aws-bucket/split-01.split",           // AWS S3
    "azure://azure-container/split-02.split",   // Azure Blob
    "/local/split-03.split",                    // Local file
    "s3://aws-bucket/split-04.split"            // AWS S3 again
);

QuickwitSplit.SplitMetadata result = QuickwitSplit.mergeSplits(
    mixedSplits, "/tmp/multi-cloud-merged.split", config);

System.out.println("Merged " + result.getNumDocs() + " documents");
System.out.println("From " + mixedSplits.size() + " splits across AWS, Azure, and local storage");
```

**Key Benefits:**
- ✅ Consolidate data from multiple cloud providers
- ✅ Migrate data between clouds by merging and re-uploading
- ✅ Create unified indices from distributed sources
- ✅ No intermediate downloads required - streaming merge

## Configuration Comparison: AWS vs Azure

| Feature | AWS S3 | Azure Blob Storage |
|---------|---------|-------------------|
| **Cache Configuration** | `.withAwsCredentials()` | `.withAzureCredentials()` |
| **Basic Auth** | Access Key + Secret Key | Account Name + Access Key |
| **Token Auth** | Session Token (STS) | OAuth Bearer Token (Azure AD) |
| **Region/Endpoint** | `.withAwsRegion("us-east-1")` | Automatic from account |
| **URL Scheme** | `s3://bucket/path` | `azure://container/path` |
| **Local Testing** | S3Mock, LocalStack | Azurite |
| **Merge Config** | `.awsConfig(awsConfig)` | `.azureConfig(azureConfig)` |

## Common Migration Scenarios

### Scenario 1: Hybrid Cloud Deployment

**Use Case:** Run primary infrastructure on AWS but use Azure for specific regions or services.

```java
public class HybridCloudSearchService {
    private final SplitCacheManager cacheManager;

    public HybridCloudSearchService() {
        SplitCacheManager.CacheConfig config =
            new SplitCacheManager.CacheConfig("hybrid-cache")
                .withMaxCacheSize(1_000_000_000)
                // AWS for US operations
                .withAwsCredentials(
                    System.getenv("AWS_ACCESS_KEY"),
                    System.getenv("AWS_SECRET_KEY")
                )
                .withAwsRegion("us-east-1")
                // Azure for EU operations
                .withAzureCredentials(
                    System.getenv("AZURE_ACCOUNT_NAME"),
                    System.getenv("AZURE_ACCESS_KEY")
                );

        this.cacheManager = SplitCacheManager.getInstance(config);
    }

    public SearchResult search(String region, String query) throws Exception {
        String splitUrl = getSplitForRegion(region);

        try (SplitSearcher searcher = cacheManager.createSplitSearcher(splitUrl)) {
            Schema schema = searcher.getSchema();
            SplitQuery splitQuery = new SplitTermQuery("content", query);
            return searcher.search(splitQuery, 10);
        }
    }

    private String getSplitForRegion(String region) {
        return switch (region) {
            case "US" -> "s3://us-bucket/splits/data.split";
            case "EU" -> "azure://eu-container/splits/data.split";
            case "ASIA" -> "s3://asia-bucket/splits/data.split";
            default -> throw new IllegalArgumentException("Unknown region: " + region);
        };
    }

    public void close() throws Exception {
        cacheManager.close();
    }
}
```

### Scenario 2: Cost Optimization

**Use Case:** Use Azure for cold storage (cheaper) and AWS for hot data (faster).

```java
public class TieredStorageSearchService {
    private final SplitCacheManager cacheManager;

    public TieredStorageSearchService() {
        SplitCacheManager.CacheConfig config =
            new SplitCacheManager.CacheConfig("tiered-cache")
                .withMaxCacheSize(2_000_000_000)  // Larger cache for cold storage
                .withAwsCredentials(
                    System.getenv("AWS_ACCESS_KEY"),
                    System.getenv("AWS_SECRET_KEY")
                )
                .withAwsRegion("us-east-1")
                .withAzureCredentials(
                    System.getenv("AZURE_ACCOUNT_NAME"),
                    System.getenv("AZURE_ACCESS_KEY")
                );

        this.cacheManager = SplitCacheManager.getInstance(config);
    }

    public SearchResult search(String dataAge, String query) throws Exception {
        // Recent data (< 30 days): AWS S3 (hot tier)
        // Older data (> 30 days): Azure Blob Storage (cool/archive tier)
        String splitUrl = dataAge.equals("recent")
            ? "s3://hot-data-bucket/splits/recent.split"
            : "azure://cold-data-container/splits/archive.split";

        try (SplitSearcher searcher = cacheManager.createSplitSearcher(splitUrl)) {
            Schema schema = searcher.getSchema();
            SplitQuery splitQuery = new SplitTermQuery("content", query);
            return searcher.search(splitQuery, 10);
        }
    }
}
```

### Scenario 3: Multi-Tenant with Cloud Preference

**Use Case:** Different customers prefer different cloud providers.

```java
public class MultiTenantSearchService {
    private final SplitCacheManager cacheManager;
    private final Map<String, String> tenantCloudPreferences;

    public MultiTenantSearchService() {
        SplitCacheManager.CacheConfig config =
            new SplitCacheManager.CacheConfig("multi-tenant-cache")
                .withMaxCacheSize(1_000_000_000)
                .withAwsCredentials(
                    System.getenv("AWS_ACCESS_KEY"),
                    System.getenv("AWS_SECRET_KEY")
                )
                .withAwsRegion("us-east-1")
                .withAzureCredentials(
                    System.getenv("AZURE_ACCOUNT_NAME"),
                    System.getenv("AZURE_ACCESS_KEY")
                );

        this.cacheManager = SplitCacheManager.getInstance(config);
        this.tenantCloudPreferences = loadTenantPreferences();
    }

    public SearchResult searchForTenant(String tenantId, String query) throws Exception {
        String cloudProvider = tenantCloudPreferences.getOrDefault(tenantId, "aws");
        String splitUrl = getSplitForTenant(tenantId, cloudProvider);

        try (SplitSearcher searcher = cacheManager.createSplitSearcher(splitUrl)) {
            Schema schema = searcher.getSchema();
            SplitQuery splitQuery = new SplitTermQuery("content", query);
            return searcher.search(splitQuery, 10);
        }
    }

    private String getSplitForTenant(String tenantId, String cloudProvider) {
        return cloudProvider.equals("azure")
            ? String.format("azure://tenant-%s/splits/data.split", tenantId)
            : String.format("s3://tenant-%s/splits/data.split", tenantId);
    }

    private Map<String, String> loadTenantPreferences() {
        // Load from configuration
        return Map.of(
            "tenant-001", "aws",
            "tenant-002", "azure",
            "tenant-003", "aws"
        );
    }
}
```

## Testing Strategy

### Local Development with Emulators

Test both AWS and Azure locally without cloud costs.

```java
import io.indextables.tantivy4java.AzuriteContainer;
import io.findify.s3mock.S3Mock;

public class MultiCloudLocalTest {
    private S3Mock s3Mock;
    private AzuriteContainer azurite;
    private SplitCacheManager cacheManager;

    @Before
    public void setUp() {
        // Start S3 mock
        s3Mock = new S3Mock.Builder()
            .withPort(8001)
            .withInMemoryBackend()
            .build();
        s3Mock.start();

        // Start Azurite
        azurite = new AzuriteContainer();
        azurite.start();

        // Configure with both emulators
        SplitCacheManager.CacheConfig config =
            new SplitCacheManager.CacheConfig("test-cache")
                .withMaxCacheSize(100_000_000)
                // AWS Mock
                .withAwsCredentials("test-key", "test-secret")
                .withAwsRegion("us-east-1")
                // Azurite
                .withAzureCredentials(
                    AzuriteContainer.ACCOUNT,
                    AzuriteContainer.KEY
                );

        cacheManager = SplitCacheManager.getInstance(config);
    }

    @After
    public void tearDown() throws Exception {
        if (cacheManager != null) cacheManager.close();
        if (azurite != null) azurite.stop();
        if (s3Mock != null) s3Mock.shutdown();
    }

    @Test
    public void testMultiCloudSearch() throws Exception {
        // Test with S3 mock
        try (SplitSearcher s3Searcher = cacheManager.createSplitSearcher(
                "s3://test-bucket/test.split")) {
            // ... S3 tests
        }

        // Test with Azurite
        try (SplitSearcher azureSearcher = cacheManager.createSplitSearcher(
                "azure://test-container/test.split")) {
            // ... Azure tests
        }
    }
}
```

## Authentication Best Practices

### AWS Session Tokens + Azure OAuth

Production-grade authentication for both providers.

```java
public class SecureMultiCloudConfig {

    public static SplitCacheManager createSecureCache() {
        // AWS: Use temporary credentials from STS
        String awsAccessKey = System.getenv("AWS_ACCESS_KEY_ID");
        String awsSecretKey = System.getenv("AWS_SECRET_ACCESS_KEY");
        String awsSessionToken = System.getenv("AWS_SESSION_TOKEN");
        String awsRegion = System.getenv("AWS_REGION");

        // Azure: Use OAuth bearer token from Azure AD
        String azureAccount = System.getenv("AZURE_STORAGE_ACCOUNT");
        String azureBearerToken = getAzureTokenFromAD();  // Your Azure AD integration

        SplitCacheManager.CacheConfig config =
            new SplitCacheManager.CacheConfig("secure-cache")
                .withMaxCacheSize(500_000_000)
                // AWS with temporary credentials
                .withAwsCredentials(awsAccessKey, awsSecretKey, awsSessionToken)
                .withAwsRegion(awsRegion)
                // Azure with OAuth token
                .withAzureBearerToken(azureAccount, azureBearerToken);

        return SplitCacheManager.getInstance(config);
    }

    private static String getAzureTokenFromAD() {
        // Implement Azure AD token acquisition
        // Example: Use Azure Identity library
        // DefaultAzureCredential credential = new DefaultAzureCredentialBuilder().build();
        // AccessToken token = credential.getToken(new TokenRequestContext()
        //     .addScopes("https://storage.azure.com/.default")).block();
        // return token.getToken();
        return System.getenv("AZURE_BEARER_TOKEN");  // Placeholder
    }
}
```

## Performance Considerations

### Cache Sizing for Multi-Cloud

When using both AWS and Azure, consider increasing cache size:

```java
// Single cloud: 500MB cache
SplitCacheManager.CacheConfig singleCloud =
    new SplitCacheManager.CacheConfig("single-cache")
        .withMaxCacheSize(500_000_000);

// Multi-cloud: 1GB cache (cache splits from both providers)
SplitCacheManager.CacheConfig multiCloud =
    new SplitCacheManager.CacheConfig("multi-cache")
        .withMaxCacheSize(1_000_000_000)  // Double for two providers
        .withMaxConcurrentLoads(16);       // Higher concurrency
```

### Connection Pooling

Both AWS and Azure SDKs handle connection pooling automatically. No additional configuration needed.

### Latency Optimization

Use the cloud provider closest to your application:

```java
public class LatencyOptimizedSearch {

    public String chooseBestProvider(String applicationRegion) {
        // Choose provider based on application deployment location
        return switch (applicationRegion) {
            case "us-east-1", "us-west-2" -> "aws";     // AWS regions
            case "eastus", "westeurope" -> "azure";      // Azure regions
            default -> "aws";                             // Default to AWS
        };
    }

    public String getSplitUrl(String provider, String splitId) {
        return provider.equals("azure")
            ? "azure://container/splits/" + splitId + ".split"
            : "s3://bucket/splits/" + splitId + ".split";
    }
}
```

## Troubleshooting

### Issue: Azure authentication fails with "InvalidAuthenticationInfo"

**Cause:** Incorrect account name or access key.

**Solution:**
```java
// Double-check credentials
String accountName = System.getenv("AZURE_STORAGE_ACCOUNT");
String accessKey = System.getenv("AZURE_STORAGE_KEY");

System.out.println("Account: " + accountName);
System.out.println("Key length: " + (accessKey != null ? accessKey.length() : 0));

// Verify credentials work
SplitCacheManager.CacheConfig config =
    new SplitCacheManager.CacheConfig("test-cache")
        .withAzureCredentials(accountName, accessKey);
```

### Issue: "Split not found" when switching from S3 to Azure

**Cause:** Different URL schemes, split hasn't been uploaded to Azure.

**Solution:**
```java
// Check if split exists before accessing
public boolean splitExists(SplitCacheManager cacheManager, String splitUrl) {
    try (SplitSearcher searcher = cacheManager.createSplitSearcher(splitUrl)) {
        return searcher.validateSplit();
    } catch (Exception e) {
        return false;
    }
}

// Use with fallback
String azureSplit = "azure://container/data.split";
String s3Split = "s3://bucket/data.split";

String splitUrl = splitExists(cacheManager, azureSplit) ? azureSplit : s3Split;
```

### Issue: Merge fails with mixed AWS/Azure splits

**Cause:** Missing credentials for one of the providers.

**Solution:**
```java
// MUST configure BOTH providers when merging mixed splits
QuickwitSplit.AwsConfig awsConfig = new QuickwitSplit.AwsConfig(
    System.getenv("AWS_ACCESS_KEY"),
    System.getenv("AWS_SECRET_KEY"),
    System.getenv("AWS_REGION")
);

QuickwitSplit.AzureConfig azureConfig = new QuickwitSplit.AzureConfig(
    System.getenv("AZURE_ACCOUNT"),
    System.getenv("AZURE_KEY")
);

QuickwitSplit.MergeConfig config = QuickwitSplit.MergeConfig.builder()
    .indexUid("my-index")
    .sourceId("my-source")
    .nodeId("worker-1")
    .awsConfig(awsConfig)      // Required even if only one AWS split
    .azureConfig(azureConfig)  // Required even if only one Azure split
    .build();
```

## Migration Checklist

- [ ] **Add Azure credentials** to your `SplitCacheManager.CacheConfig`
- [ ] **Test locally** with Azurite before touching production
- [ ] **Update URL handling** to support both `s3://` and `azure://` schemes
- [ ] **Configure both providers** in merge operations if using multi-cloud
- [ ] **Increase cache size** if serving splits from both providers
- [ ] **Update monitoring** to track usage across both cloud providers
- [ ] **Document cloud provider** choice for each split in your system
- [ ] **Test failover** between AWS and Azure in staging environment
- [ ] **Review cost implications** of multi-cloud storage
- [ ] **Update backup strategy** to include both providers

## Next Steps

1. **Review existing AWS code** - Identify where splits are created and accessed
2. **Choose authentication method** - Account key for simple, OAuth for production
3. **Test with Azurite** - Verify integration works before cloud deployment
4. **Add Azure configuration** - Update `SplitCacheManager.CacheConfig` with Azure credentials
5. **Deploy incrementally** - Start with non-critical data, expand after validation
6. **Monitor performance** - Compare latency and throughput between providers
7. **Optimize costs** - Use cost analysis tools to balance between AWS and Azure

## Additional Resources

- [README.md](README.md) - Complete API documentation with examples
- [CLAUDE.md](CLAUDE.md) - Comprehensive feature documentation
- [Azure Blob Storage Documentation](https://learn.microsoft.com/en-us/azure/storage/blobs/)
- [AWS S3 Documentation](https://docs.aws.amazon.com/s3/)
- [Azurite Emulator](https://learn.microsoft.com/en-us/azure/storage/common/storage-use-azurite)

## Summary

Adding Azure support to your existing AWS-integrated tantivy4java application is straightforward:

1. ✅ **No code changes** to existing AWS functionality
2. ✅ **Add `.withAzureCredentials()`** to cache configuration
3. ✅ **Use `azure://` URLs** alongside existing `s3://` URLs
4. ✅ **Same API** for both cloud providers
5. ✅ **Mix and match** splits from both providers in single operations

The multi-cloud architecture is transparent to your search code - whether data comes from AWS S3 or Azure Blob Storage, the search API remains identical.

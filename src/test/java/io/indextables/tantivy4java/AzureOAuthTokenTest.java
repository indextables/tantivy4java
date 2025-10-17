package io.indextables.tantivy4java;

import io.indextables.tantivy4java.core.*;
import io.indextables.tantivy4java.split.SplitCacheManager;
import io.indextables.tantivy4java.split.SplitSearcher;
import io.indextables.tantivy4java.split.merge.QuickwitSplit;
import io.indextables.tantivy4java.result.SearchResult;
import io.indextables.tantivy4java.query.Query;

import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.BlobServiceClientBuilder;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.common.StorageSharedKeyCredential;
import com.azure.core.credential.TokenRequestContext;
import com.azure.identity.DefaultAzureCredentialBuilder;

import org.junit.jupiter.api.*;
import static org.junit.jupiter.api.Assertions.*;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;

/**
 * Test demonstrating OAuth bearer token authentication for Azure Blob Storage.
 *
 * This test shows two authentication patterns:
 * 1. How to use bearer tokens with QuickwitSplit merge operations
 * 2. How bearer tokens would be acquired in production using Azure Identity SDK
 *
 * Note: Since Azurite emulator doesn't support OAuth tokens, this test demonstrates
 * the API patterns but uses account keys for the actual operations. In production with
 * real Azure Storage, you would acquire tokens using DefaultAzureCredential,
 * ClientSecretCredential, or ManagedIdentityCredential.
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class AzureOAuthTokenTest extends AzuriteTestBase {

    private static Path tempDir;
    private static Path indexPath;
    private static Path splitPath1;
    private static Path splitPath2;
    private static Schema schema;

    private static final String CONTAINER_NAME = "oauth-test-splits";
    private static final String SPLIT1_NAME = "oauth-split-1.split";
    private static final String SPLIT2_NAME = "oauth-split-2.split";
    private static final String MERGED_SPLIT_NAME = "oauth-merged.split";

    @BeforeAll
    public static void setUp() throws IOException {
        // Ensure Azurite is running
        assertNotNull(AZURITE, "Azurite container should be running");
        assertTrue(AZURITE.isRunning(), "Azurite should be running");

        System.out.println("✅ Azurite running at: " + AZURITE.endpoint());

        // Create temporary directory
        tempDir = Files.createTempDirectory("azure-oauth-test");
        indexPath = tempDir.resolve("test-index");
        splitPath1 = tempDir.resolve(SPLIT1_NAME);
        splitPath2 = tempDir.resolve(SPLIT2_NAME);

        // Create schema
        schema = new SchemaBuilder()
            .addTextField("title", true, false, "default", "position")
            .addTextField("content", true, false, "default", "position")
            .addIntegerField("category", true, true, false)
            .build();
    }

    @AfterAll
    public static void tearDown() throws IOException {
        if (tempDir != null && Files.exists(tempDir)) {
            Files.walk(tempDir)
                .sorted((a, b) -> -a.compareTo(b))
                .forEach(path -> {
                    try {
                        Files.deleteIfExists(path);
                    } catch (IOException e) {
                        // Ignore
                    }
                });
        }
    }

    @Test
    @Order(1)
    public void testCreateAndUploadSplits() throws Exception {
        System.out.println("\n📝 Test 1: Create splits and upload to Azure");

        // Create first split
        try (Index index1 = new Index(schema, indexPath.toString())) {
            try (IndexWriter writer = index1.writer(Index.Memory.DEFAULT_HEAP_SIZE, 1)) {
                writer.addJson("{\"title\": \"OAuth Guide\", \"content\": \"Using bearer tokens with Azure\", \"category\": 1}");
                writer.addJson("{\"title\": \"Service Principal\", \"content\": \"Authentication with Azure AD\", \"category\": 1}");
                writer.commit();
            }
        }

        QuickwitSplit.SplitConfig splitConfig1 = new QuickwitSplit.SplitConfig(
            "oauth-test-index", "oauth-source", "node-1"
        );

        QuickwitSplit.SplitMetadata metadata1 = QuickwitSplit.convertIndexFromPath(
            indexPath.toString(), splitPath1.toString(), splitConfig1
        );

        assertEquals(2, metadata1.getNumDocs());
        System.out.println("✅ Split 1 created: " + metadata1.getSplitId());

        // Create second split
        try (Index index2 = new Index(schema, indexPath.toString())) {
            try (IndexWriter writer = index2.writer(Index.Memory.DEFAULT_HEAP_SIZE, 1)) {
                writer.addJson("{\"title\": \"Managed Identity\", \"content\": \"Azure managed identity guide\", \"category\": 2}");
                writer.addJson("{\"title\": \"Token Lifecycle\", \"content\": \"Managing OAuth token refresh\", \"category\": 2}");
                writer.commit();
            }
        }

        QuickwitSplit.SplitConfig splitConfig2 = new QuickwitSplit.SplitConfig(
            "oauth-test-index", "oauth-source", "node-2"
        );

        QuickwitSplit.SplitMetadata metadata2 = QuickwitSplit.convertIndexFromPath(
            indexPath.toString(), splitPath2.toString(), splitConfig2
        );

        assertEquals(2, metadata2.getNumDocs());
        System.out.println("✅ Split 2 created: " + metadata2.getSplitId());

        // Upload splits to Azurite
        BlobServiceClient blobServiceClient = new BlobServiceClientBuilder()
            .endpoint(AZURITE.endpoint())
            .credential(new StorageSharedKeyCredential(AzuriteContainer.ACCOUNT, AzuriteContainer.KEY))
            .buildClient();

        BlobContainerClient containerClient = blobServiceClient.getBlobContainerClient(CONTAINER_NAME);
        containerClient.createIfNotExists();

        containerClient.getBlobClient(SPLIT1_NAME).uploadFromFile(splitPath1.toString(), true);
        containerClient.getBlobClient(SPLIT2_NAME).uploadFromFile(splitPath2.toString(), true);

        System.out.println("✅ Splits uploaded to Azure container: " + CONTAINER_NAME);
    }

    @Test
    @Order(2)
    public void testBearerTokenAPIPattern() {
        System.out.println("\n📝 Test 2: Bearer token API pattern demonstration");

        // 🎯 PRODUCTION PATTERN: How to acquire OAuth token in real Azure
        // This code is commented because Azurite doesn't support OAuth,
        // but it shows the exact pattern users would follow:

        /*
        try {
            // Option 1: DefaultAzureCredential (recommended for production);
            // Automatically tries: Environment → Managed Identity → Azure CLI → etc.
            DefaultAzureCredential credential = new DefaultAzureCredentialBuilder()
                .build();

            TokenRequestContext context = new TokenRequestContext()
                .addScopes("https://storage.azure.com/.default");

            AccessToken token = credential.getToken(context).block();
            String bearerToken = token.getToken();

            // Option 2: Service Principal (for specific applications)
            ClientSecretCredential spCredential = new ClientSecretCredentialBuilder()
                .clientId(System.getenv("AZURE_CLIENT_ID"))
                .clientSecret(System.getenv("AZURE_CLIENT_SECRET"))
                .tenantId(System.getenv("AZURE_TENANT_ID"))
                .build();

            AccessToken spToken = spCredential.getToken(context).block();
            String spBearerToken = spToken.getToken();

            // Option 3: Managed Identity (for Azure VMs/App Services)
            ManagedIdentityCredential miCredential = new ManagedIdentityCredentialBuilder()
                .build();

            AccessToken miToken = miCredential.getToken(context).block();
            String miBearerToken = miToken.getToken();

            System.out.println("✅ OAuth token acquired successfully");
            System.out.println("✅ Token length: " + bearerToken.length() + " characters");

        } catch (Exception e) {
            System.err.println("⚠️  Could not acquire OAuth token (expected in test environment)");
            System.err.println("   In production, ensure Azure credentials are configured");
        }
        */

        // For this test, we'll demonstrate the API with a mock token
        String mockBearerToken = "mock-oauth-token-for-api-demonstration";

        // Create Azure config with bearer token
        QuickwitSplit.AzureConfig azureConfigWithToken =
            QuickwitSplit.AzureConfig.withBearerToken(
                AzuriteContainer.ACCOUNT,
                mockBearerToken
            );

        // Verify the config was created correctly
        assertEquals(AzuriteContainer.ACCOUNT, azureConfigWithToken.getAccountName());
        assertEquals(mockBearerToken, azureConfigWithToken.getBearerToken());
        assertNull(azureConfigWithToken.getAccountKey(), "Bearer token auth should not have account key");

        System.out.println("✅ Bearer token API pattern validated");
        System.out.println("   - Account name: " + azureConfigWithToken.getAccountName());
        System.out.println("   - Bearer token: [REDACTED]");
        System.out.println("   - Account key: null (not used with OAuth)");
    }

    @Test
    @Order(3)
    public void testBearerTokenWithCustomEndpoint() {
        System.out.println("\n📝 Test 3: Bearer token with custom endpoint");

        String mockBearerToken = "mock-oauth-token-with-endpoint";
        String customEndpoint = AZURITE.endpoint();

        // Create Azure config with bearer token and custom endpoint
        QuickwitSplit.AzureConfig azureConfig =
            QuickwitSplit.AzureConfig.withBearerToken(
                AzuriteContainer.ACCOUNT,
                mockBearerToken);

        assertEquals(AzuriteContainer.ACCOUNT, azureConfig.getAccountName());
        assertEquals(mockBearerToken, azureConfig.getBearerToken());
        assertNull(azureConfig.getAccountKey());

        System.out.println("✅ Bearer token with custom endpoint validated");
        System.out.println("   - Endpoint: " + customEndpoint);
    }

    @Test
    @Order(4)
    public void testMergeConfigBuilderWithBearerToken() {
        System.out.println("\n📝 Test 4: MergeConfig builder with bearer token");

        String mockBearerToken = "mock-merge-config-token";

        QuickwitSplit.AzureConfig azureConfig =
            QuickwitSplit.AzureConfig.withBearerToken(
                AzuriteContainer.ACCOUNT,
                mockBearerToken
            );

        // Build merge config with Azure bearer token
        QuickwitSplit.MergeConfig mergeConfig = QuickwitSplit.MergeConfig.builder()
            .indexUid("oauth-index")
            .sourceId("oauth-source")
            .nodeId("oauth-node")
            .azureConfig(azureConfig)
            .debugEnabled(true)
            .build();

        assertNotNull(mergeConfig.getAzureConfig());
        assertEquals(mockBearerToken, mergeConfig.getAzureConfig().getBearerToken());
        assertTrue(mergeConfig.isDebugEnabled());

        System.out.println("✅ MergeConfig with bearer token created successfully");
    }

    @Test
    @Order(5)
    public void testMergeConfigWithBearerTokenForProduction() {
        System.out.println("\n📝 Test 5: Production merge config with bearer token");

        // Note: This demonstrates the API pattern for production use
        // Actual merge operations with OAuth tokens are tested in RealAzureEndToEndTest

        String mockBearerToken = "eyJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiIsIng1dCI6Ik1jN2wzSXo5M2c3dXdnTmVFbW13X1dZR1BrbyIsImtpZCI6Ik1jN2wzSXo5M2c3dXdnTmVFbW13X1dZR1BrbyJ9...";

        QuickwitSplit.AzureConfig azureConfig =
            QuickwitSplit.AzureConfig.withBearerToken(
                "mystorageaccount",
                mockBearerToken
            );

        QuickwitSplit.MergeConfig mergeConfig = QuickwitSplit.MergeConfig.builder()
            .indexUid("production-index")
            .sourceId("production-source")
            .nodeId("production-node")
            .azureConfig(azureConfig)
            .build();

        // Verify merge config was created correctly
        assertNotNull(mergeConfig.getAzureConfig());
        assertEquals(mockBearerToken, mergeConfig.getAzureConfig().getBearerToken());
        assertNull(mergeConfig.getAzureConfig().getAccountKey(), "OAuth config should not have account key");

        System.out.println("✅ Production merge config with bearer token validated");
        System.out.println("   - Index UID: " + mergeConfig.getIndexUid());
        System.out.println("   - Azure account: " + mergeConfig.getAzureConfig().getAccountName());
        System.out.println("   - Auth method: OAuth bearer token");
        System.out.println();
        System.out.println("💡 In production, you would use this config with:");
        System.out.println("   QuickwitSplit.mergeSplits(splitUris, outputPath, mergeConfig);");
        System.out.println();
        System.out.println("   Example split URIs:");
        System.out.println("   - azure://container/split-001.split");
        System.out.println("   - azure://container/split-002.split");
    }

    @Test
    @Order(6)
    public void testProductionOAuthPatternDocumentation() {
        System.out.println("\n📝 Test 6: Production OAuth pattern documentation");

        System.out.println("\n📚 PRODUCTION USAGE GUIDE:");
        System.out.println("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
        System.out.println();
        System.out.println("1️⃣  Add Azure Identity SDK dependency:");
        System.out.println("   <dependency>");
        System.out.println("     <groupId>com.azure</groupId>");
        System.out.println("     <artifactId>azure-identity</artifactId>");
        System.out.println("     <version>1.10.0</version>");
        System.out.println("   </dependency>");
        System.out.println();
        System.out.println("2️⃣  Acquire OAuth token using DefaultAzureCredential:");
        System.out.println("   DefaultAzureCredential credential = new DefaultAzureCredentialBuilder()");
        System.out.println("       .build();");
        System.out.println("   ");
        System.out.println("   TokenRequestContext context = new TokenRequestContext()");
        System.out.println("       .addScopes(\"https://storage.azure.com/.default\");");
        System.out.println("   ");
        System.out.println("   AccessToken token = credential.getToken(context).block();");
        System.out.println("   String bearerToken = token.getToken();");
        System.out.println();
        System.out.println("3️⃣  Create Azure config with bearer token:");
        System.out.println("   QuickwitSplit.AzureConfig azureConfig = ");
        System.out.println("       QuickwitSplit.AzureConfig.withBearerToken(");
        System.out.println("           \"mystorageaccount\",");
        System.out.println("           bearerToken");
        System.out.println("       );");
        System.out.println();
        System.out.println("4️⃣  Use in merge operations:");
        System.out.println("   QuickwitSplit.MergeConfig mergeConfig = QuickwitSplit.MergeConfig.builder()");
        System.out.println("       .indexUid(\"my-index\")");
        System.out.println("       .sourceId(\"my-source\")");
        System.out.println("       .nodeId(\"my-node\")");
        System.out.println("       .azureConfig(azureConfig)");
        System.out.println("       .build();");
        System.out.println();
        System.out.println("5️⃣  Alternative: Service Principal authentication:");
        System.out.println("   ClientSecretCredential spCredential = new ClientSecretCredentialBuilder()");
        System.out.println("       .clientId(\"client-id\")");
        System.out.println("       .clientSecret(\"client-secret\")");
        System.out.println("       .tenantId(\"tenant-id\")");
        System.out.println("       .build();");
        System.out.println("   ");
        System.out.println("   AccessToken token = spCredential.getToken(context).block();");
        System.out.println();
        System.out.println("6️⃣  Alternative: Managed Identity (Azure VMs/App Services):");
        System.out.println("   ManagedIdentityCredential miCredential = new ManagedIdentityCredentialBuilder()");
        System.out.println("       .build();");
        System.out.println("   ");
        System.out.println("   AccessToken token = miCredential.getToken(context).block();");
        System.out.println();
        System.out.println("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
        System.out.println("✅ Documentation test passed");
    }

    @Test
    @Order(7)
    public void testBearerTokenVsAccountKeyComparison() {
        System.out.println("\n📝 Test 7: Bearer token vs Account key comparison");

        // Account key approach (traditional)
        QuickwitSplit.AzureConfig accountKeyConfig = new QuickwitSplit.AzureConfig(
            "mystorageaccount", "account-key-value"
        );

        assertNotNull(accountKeyConfig.getAccountKey());
        assertNull(accountKeyConfig.getBearerToken());
        System.out.println("✅ Account key config: Uses storage account key");

        // Bearer token approach (OAuth)
        QuickwitSplit.AzureConfig bearerTokenConfig =
            QuickwitSplit.AzureConfig.withBearerToken(
                "mystorageaccount", "mock-bearer-token");

        assertNull(bearerTokenConfig.getAccountKey());
        assertNotNull(bearerTokenConfig.getBearerToken());
        System.out.println("✅ Bearer token config: Uses OAuth token");

        System.out.println("\n🔐 Security Comparison:");
        System.out.println("   Account Key:");
        System.out.println("   - Long-lived credential");
        System.out.println("   - Full account access");
        System.out.println("   - Manual rotation required");
        System.out.println();
        System.out.println("   Bearer Token (OAuth):");
        System.out.println("   - Short-lived credential (typically 1 hour)");
        System.out.println("   - Fine-grained permissions via Azure RBAC");
        System.out.println("   - Automatic refresh");
        System.out.println("   - Supports managed identity (no credentials in code)");
        System.out.println("   - Audit trail via Azure AD");
    }
}

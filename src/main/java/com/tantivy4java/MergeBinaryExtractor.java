package com.tantivy4java;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.List;

/**
 * Utility class for extracting and running the standalone Rust merge binary.
 *
 * The binary is bundled inside the JAR file and extracted to a temporary location
 * when needed. This provides complete process isolation for merge operations,
 * eliminating the thread contention and runtime conflicts seen with JNI-based merging.
 */
public class MergeBinaryExtractor {
    private static Path extractedBinaryPath = null;

    /**
     * Gets the path to the extracted merge binary, extracting it if necessary.
     * The binary is cached after first extraction and cleaned up on JVM exit.
     */
    public static synchronized Path getBinaryPath() throws IOException {
        if (extractedBinaryPath == null) {
            // Extract from JAR to temp location
            String osName = System.getProperty("os.name").toLowerCase();
            String binaryName = osName.contains("windows") ? "tantivy4java-merge.exe" : "tantivy4java-merge";
            String resourcePath = "/native/" + binaryName;

            Path tempBinary = Files.createTempFile("tantivy4java-merge", osName.contains("windows") ? ".exe" : "");

            try (InputStream is = MergeBinaryExtractor.class.getResourceAsStream(resourcePath)) {
                if (is == null) {
                    throw new IOException("Could not find merge binary resource: " + resourcePath);
                }
                Files.copy(is, tempBinary, StandardCopyOption.REPLACE_EXISTING);
            }

            // Make executable on Unix systems
            if (!osName.contains("windows")) {
                tempBinary.toFile().setExecutable(true);
            }

            extractedBinaryPath = tempBinary;

            // Clean up on JVM exit
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                try {
                    Files.deleteIfExists(extractedBinaryPath);
                } catch (IOException ignored) {}
            }));
        }

        return extractedBinaryPath;
    }

    /**
     * Executes a merge operation using the standalone Rust binary.
     * This provides complete process isolation and eliminates JNI threading issues.
     *
     * @param splitPaths List of split file paths to merge
     * @param outputPath Output path for the merged split
     * @param mergeConfig Configuration for the merge operation (includes heap size and temp directory)
     * @return MergeResult containing the operation result
     * @throws IOException If the process fails or cannot be started
     */
    public static MergeResult executeMerge(List<String> splitPaths, String outputPath,
                                          QuickwitSplit.MergeConfig mergeConfig) throws IOException {
        Path binaryPath = getBinaryPath();

        // Convert merge config to JSON
        String configJson = createMergeRequestJson(splitPaths, outputPath, mergeConfig);

        // Create secure temporary file for sensitive data
        Path tempJsonFile = Files.createTempFile("tantivy4java-merge-config", ".json");

        // Set file permissions to be readable only by owner (user)
        try {
            java.util.Set<java.nio.file.attribute.PosixFilePermission> permissions =
                java.util.EnumSet.of(
                    java.nio.file.attribute.PosixFilePermission.OWNER_READ,
                    java.nio.file.attribute.PosixFilePermission.OWNER_WRITE
                );
            Files.setPosixFilePermissions(tempJsonFile, permissions);
        } catch (UnsupportedOperationException e) {
            // Fallback for non-POSIX systems (Windows)
            tempJsonFile.toFile().setReadable(false, false);
            tempJsonFile.toFile().setReadable(true, true);
            tempJsonFile.toFile().setWritable(false, false);
            tempJsonFile.toFile().setWritable(true, true);
        }

        // Write JSON to secure file
        Files.write(tempJsonFile, configJson.getBytes(java.nio.charset.StandardCharsets.UTF_8));

        // Build and execute the process with file path
        ProcessBuilder pb = new ProcessBuilder(binaryPath.toString(), tempJsonFile.toString());

        // Set debug environment if enabled
        if (System.getProperty("TANTIVY4JAVA_DEBUG") != null) {
            pb.environment().put("TANTIVY4JAVA_DEBUG", "1");
        }

        // Always print fork/join debug information
        System.err.println("[FORK-JOIN] Java thread " + Thread.currentThread().getName() +
                          " (ID:" + Thread.currentThread().getId() + ") forking merge process");
        System.err.println("[FORK-JOIN] Command: " + pb.command());
        System.err.println("[FORK-JOIN] Merging " + splitPaths.size() + " splits -> " + outputPath);

        long startTime = System.currentTimeMillis();
        Process process = pb.start();
        long childPid = process.pid();

        System.err.println("[FORK-JOIN] Child process PID " + childPid + " started");

        try {
            int exitCode = process.waitFor();
            long duration = System.currentTimeMillis() - startTime;

            System.err.println("[FORK-JOIN] Child process PID " + childPid + " completed with exit code " +
                              exitCode + " in " + duration + "ms");

            if (exitCode != 0) {
                String error = new String(process.getErrorStream().readAllBytes());
                System.err.println("[FORK-JOIN] Child process PID " + childPid + " STDERR: " + error);
                throw new IOException("Merge process failed with exit code " + exitCode + ": " + error);
            }

            // Parse result from process output
            String output = new String(process.getInputStream().readAllBytes());
            String stderr = new String(process.getErrorStream().readAllBytes());

            // Print the child process debug output
            if (!stderr.isEmpty()) {
                System.err.println("[FORK-JOIN] Child process PID " + childPid + " STDERR output:\n" + stderr);
            }

            System.err.println("[FORK-JOIN] Java thread " + Thread.currentThread().getName() +
                              " successfully joined child process PID " + childPid);

            return parseMergeResult(output, duration);

        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("Merge process was interrupted", e);
        } finally {
            // Clean up the temporary file containing sensitive data
            try {
                Files.deleteIfExists(tempJsonFile);
            } catch (IOException e) {
                // Log but don't fail the operation for cleanup issues
                System.err.println("Warning: Failed to delete temporary config file: " + e.getMessage());
            }
        }
    }

    /**
     * Creates JSON request for the merge binary.
     */
    private static String createMergeRequestJson(List<String> splitPaths, String outputPath,
                                                QuickwitSplit.MergeConfig mergeConfig) {
        // Simple JSON construction - could use a proper JSON library for complex cases
        StringBuilder sb = new StringBuilder();
        sb.append("{");
        sb.append("\"split_paths\":[");
        for (int i = 0; i < splitPaths.size(); i++) {
            if (i > 0) sb.append(",");
            sb.append("\"").append(escapeJson(splitPaths.get(i))).append("\"");
        }
        sb.append("],");
        sb.append("\"output_path\":\"").append(escapeJson(outputPath)).append("\",");
        sb.append("\"config\":{");
        sb.append("\"index_uid\":\"").append(escapeJson(mergeConfig.getIndexUid())).append("\",");
        sb.append("\"source_id\":\"").append(escapeJson(mergeConfig.getSourceId())).append("\",");
        sb.append("\"node_id\":\"").append(escapeJson(mergeConfig.getNodeId())).append("\",");
        // ✅ CRITICAL FIX: Properly serialize AWS credentials for child process
        QuickwitSplit.AwsConfig awsConfig = mergeConfig.getAwsConfig();
        if (awsConfig != null) {
            sb.append("\"aws_config\":{");
            sb.append("\"access_key\":\"").append(escapeJson(awsConfig.getAccessKey())).append("\",");
            sb.append("\"secret_key\":\"").append(escapeJson(awsConfig.getSecretKey())).append("\",");
            sb.append("\"session_token\":").append(awsConfig.getSessionToken() != null ?
                "\"" + escapeJson(awsConfig.getSessionToken()) + "\"" : "null").append(",");
            sb.append("\"region\":\"").append(escapeJson(awsConfig.getRegion())).append("\",");
            sb.append("\"endpoint_url\":").append(awsConfig.getEndpoint() != null ?
                "\"" + escapeJson(awsConfig.getEndpoint()) + "\"" : "null").append(",");
            sb.append("\"force_path_style\":").append(awsConfig.isForcePathStyle());
            sb.append("}");
        } else {
            sb.append("\"aws_config\":null");  // For local file operations
        }
        sb.append("},");
        if (mergeConfig.getHeapSizeBytes() != null) {
            sb.append("\"heap_size\":").append(mergeConfig.getHeapSizeBytes()).append(",");
        } else {
            sb.append("\"heap_size\":null,");
        }
        if (mergeConfig.getTempDirectoryPath() != null) {
            sb.append("\"temp_directory\":\"").append(escapeJson(mergeConfig.getTempDirectoryPath())).append("\"");
        } else {
            sb.append("\"temp_directory\":null");
        }
        sb.append("}");
        return sb.toString();
    }

    /**
     * Parses the merge result from the binary's JSON output.
     */
    private static MergeResult parseMergeResult(String output, long duration) throws IOException {
        // Simple JSON parsing - for production, use a proper JSON library
        if (!output.contains("\"success\":true")) {
            String error = extractJsonValue(output, "error");
            throw new IOException("Merge failed: " + error);
        }

        // Extract metadata
        String splitId = extractJsonValue(output, "split_id");
        String numDocsStr = extractJsonValue(output, "num_docs");
        String sizeStr = extractJsonValue(output, "uncompressed_size_bytes");

        // ✅ COMPLETE METADATA FIX: Extract all metadata fields from native merge result
        String footerStartStr = extractJsonValue(output, "footer_start_offset");
        String footerEndStr = extractJsonValue(output, "footer_end_offset");
        String hotcacheStartStr = extractJsonValue(output, "hotcache_start_offset");
        String hotcacheLengthStr = extractJsonValue(output, "hotcache_length");

        // ✅ COMPLETE METADATA FIX: Extract all missing Quickwit fields
        String indexUid = extractJsonValue(output, "index_uid");
        String sourceId = extractJsonValue(output, "source_id");
        String nodeId = extractJsonValue(output, "node_id");
        String docMappingUid = extractJsonValue(output, "doc_mapping_uid");
        String partitionIdStr = extractJsonValue(output, "partition_id");
        String maturity = extractJsonValue(output, "maturity");
        String deleteOpstampStr = extractJsonValue(output, "delete_opstamp");
        String numMergeOpsStr = extractJsonValue(output, "num_merge_ops");
        String timeRangeStartStr = extractJsonValue(output, "time_range_start");
        String timeRangeEndStr = extractJsonValue(output, "time_range_end");
        String createTimestampStr = extractJsonValue(output, "create_timestamp");

        // ✅ DOCUMENT MAPPING FIX: Extract doc_mapping_json from native merge result
        String docMappingJson = extractJsonString(output, "doc_mapping_json");
        if ("null".equals(docMappingJson) || docMappingJson.isEmpty()) {
            docMappingJson = null; // Handle JSON null values
        }

        int numDocs = Integer.parseInt(numDocsStr);
        long size = Long.parseLong(sizeStr);

        // Parse footer offsets (use -1 as default for missing values)
        long footerStartOffset = footerStartStr.isEmpty() ? -1L : Long.parseLong(footerStartStr);
        long footerEndOffset = footerEndStr.isEmpty() ? -1L : Long.parseLong(footerEndStr);
        long hotcacheStartOffset = hotcacheStartStr.isEmpty() ? -1L : Long.parseLong(hotcacheStartStr);
        long hotcacheLength = hotcacheLengthStr.isEmpty() ? -1L : Long.parseLong(hotcacheLengthStr);

        // Parse all additional Quickwit fields
        long partitionId = partitionIdStr.isEmpty() ? 0L : Long.parseLong(partitionIdStr);
        long deleteOpstamp = deleteOpstampStr.isEmpty() ? 0L : Long.parseLong(deleteOpstampStr);
        int numMergeOps = numMergeOpsStr.isEmpty() ? 1 : Integer.parseInt(numMergeOpsStr);
        Long timeRangeStart = timeRangeStartStr.isEmpty() ? null : Long.parseLong(timeRangeStartStr);
        Long timeRangeEnd = timeRangeEndStr.isEmpty() ? null : Long.parseLong(timeRangeEndStr);
        long createTimestamp = createTimestampStr.isEmpty() ? (System.currentTimeMillis() / 1000) : Long.parseLong(createTimestampStr);

        return new MergeResult(splitId, numDocs, size, duration,
                              footerStartOffset, footerEndOffset, hotcacheStartOffset, hotcacheLength,
                              indexUid, sourceId, nodeId, docMappingUid, partitionId,
                              maturity, deleteOpstamp, numMergeOps,
                              timeRangeStart, timeRangeEnd, createTimestamp, docMappingJson);
    }

    /**
     * Extract JSON string values that might contain nested JSON (like doc_mapping_json).
     * This handles properly escaped JSON strings within JSON.
     */
    private static String extractJsonString(String json, String key) {
        String pattern = "\"" + key + "\":";
        int start = json.indexOf(pattern);
        if (start == -1) return "";

        start += pattern.length();
        // Skip whitespace
        while (start < json.length() && Character.isWhitespace(json.charAt(start))) {
            start++;
        }

        if (start >= json.length()) return "";

        if (json.charAt(start) == 'n' && json.substring(start).startsWith("null")) {
            return "null";
        }

        if (json.charAt(start) == '"') {
            start++; // Skip opening quote
            StringBuilder result = new StringBuilder();
            boolean escaped = false;

            for (int i = start; i < json.length(); i++) {
                char c = json.charAt(i);

                if (escaped) {
                    result.append(c);
                    escaped = false;
                } else if (c == '\\') {
                    result.append(c);
                    escaped = true;
                } else if (c == '"') {
                    // Found closing quote
                    return result.toString();
                } else {
                    result.append(c);
                }
            }
        }

        return "";
    }

    /**
     * Simple JSON value extraction - replace with proper JSON parser for production.
     */
    private static String extractJsonValue(String json, String key) {
        String pattern = "\"" + key + "\":";
        int start = json.indexOf(pattern);
        if (start == -1) return "";

        start += pattern.length();
        if (json.charAt(start) == '"') {
            start++;
            int end = json.indexOf('"', start);
            return json.substring(start, end);
        } else {
            int end = start;
            while (end < json.length() && Character.isDigit(json.charAt(end))) {
                end++;
            }
            return json.substring(start, end);
        }
    }

    /**
     * Simple JSON string escaping.
     */
    private static String escapeJson(String str) {
        return str.replace("\\", "\\\\").replace("\"", "\\\"");
    }

    /**
     * Result of a merge operation.
     * ✅ COMPLETE METADATA FIX: Include all Quickwit-compatible fields
     */
    public static class MergeResult {
        public final String splitId;
        public final int numDocs;
        public final long sizeBytes;
        public final long durationMs;

        // Footer offset information from native merge
        public final long footerStartOffset;
        public final long footerEndOffset;
        public final long hotcacheStartOffset;
        public final long hotcacheLength;

        // ✅ COMPLETE METADATA FIX: Add all missing Quickwit fields
        public final String indexUid;
        public final String sourceId;
        public final String nodeId;
        public final String docMappingUid;
        public final long partitionId;
        public final String maturity;
        public final long deleteOpstamp;
        public final int numMergeOps;
        public final Long timeRangeStart;
        public final Long timeRangeEnd;
        public final long createTimestamp;

        // ✅ DOCUMENT MAPPING FIX: Add document mapping JSON field
        public final String docMappingJson;

        public MergeResult(String splitId, int numDocs, long sizeBytes, long durationMs) {
            this(splitId, numDocs, sizeBytes, durationMs, -1L, -1L, -1L, -1L,
                 "", "", "", "", 0L, "immature", 0L, 1, null, null, System.currentTimeMillis() / 1000, null);
        }

        public MergeResult(String splitId, int numDocs, long sizeBytes, long durationMs,
                          long footerStartOffset, long footerEndOffset,
                          long hotcacheStartOffset, long hotcacheLength) {
            this(splitId, numDocs, sizeBytes, durationMs, footerStartOffset, footerEndOffset,
                 hotcacheStartOffset, hotcacheLength, "", "", "", "", 0L, "immature", 0L, 1,
                 null, null, System.currentTimeMillis() / 1000, null);
        }

        public MergeResult(String splitId, int numDocs, long sizeBytes, long durationMs,
                          long footerStartOffset, long footerEndOffset,
                          long hotcacheStartOffset, long hotcacheLength,
                          String indexUid, String sourceId, String nodeId, String docMappingUid,
                          long partitionId, String maturity, long deleteOpstamp, int numMergeOps,
                          Long timeRangeStart, Long timeRangeEnd, long createTimestamp, String docMappingJson) {
            this.splitId = splitId;
            this.numDocs = numDocs;
            this.sizeBytes = sizeBytes;
            this.durationMs = durationMs;
            this.footerStartOffset = footerStartOffset;
            this.footerEndOffset = footerEndOffset;
            this.hotcacheStartOffset = hotcacheStartOffset;
            this.hotcacheLength = hotcacheLength;

            // ✅ COMPLETE METADATA FIX: Set all Quickwit fields
            this.indexUid = indexUid != null && !indexUid.isEmpty() ? indexUid : "unknown-index";
            this.sourceId = sourceId != null && !sourceId.isEmpty() ? sourceId : "unknown-source";
            this.nodeId = nodeId != null && !nodeId.isEmpty() ? nodeId : "unknown-node";
            this.docMappingUid = docMappingUid != null && !docMappingUid.isEmpty() ? docMappingUid : "default-mapping";
            this.partitionId = partitionId;
            this.maturity = maturity != null && !maturity.isEmpty() ? maturity : "immature";
            this.deleteOpstamp = deleteOpstamp;
            this.numMergeOps = numMergeOps;
            this.timeRangeStart = timeRangeStart;
            this.timeRangeEnd = timeRangeEnd;
            this.createTimestamp = createTimestamp;

            // ✅ DOCUMENT MAPPING FIX: Set document mapping JSON field
            this.docMappingJson = docMappingJson;
        }

        public QuickwitSplit.SplitMetadata toSplitMetadata() {
            // ✅ COMPLETE METADATA FIX: Use all actual values from native merge result
            java.time.Instant timeStart = timeRangeStart != null ? java.time.Instant.ofEpochSecond(timeRangeStart) : null;
            java.time.Instant timeEnd = timeRangeEnd != null ? java.time.Instant.ofEpochSecond(timeRangeEnd) : null;

            return new QuickwitSplit.SplitMetadata(
                splitId,                           // splitId
                indexUid,                          // indexUid ✅ FIXED: Use actual value from native merge
                partitionId,                       // partitionId ✅ FIXED: Use actual value from native merge
                sourceId,                          // sourceId ✅ FIXED: Use actual value from native merge
                nodeId,                            // nodeId ✅ FIXED: Use actual value from native merge
                (long) numDocs,                    // numDocs
                sizeBytes,                         // uncompressedSizeBytes
                timeStart,                         // timeRangeStart ✅ FIXED: Use actual value from native merge
                timeEnd,                           // timeRangeEnd ✅ FIXED: Use actual value from native merge
                createTimestamp,                   // createTimestamp ✅ FIXED: Use actual value from native merge
                maturity,                          // maturity ✅ FIXED: Use actual value from native merge
                new java.util.HashSet<>(),        // tags (empty for now, could be enhanced)
                footerStartOffset,                 // footerStartOffset ✅ FIXED: Use actual value from native merge
                footerEndOffset,                   // footerEndOffset ✅ FIXED: Use actual value from native merge
                deleteOpstamp,                     // deleteOpstamp ✅ FIXED: Use actual value from native merge
                numMergeOps,                       // numMergeOps ✅ FIXED: Use actual value from native merge
                docMappingUid,                     // docMappingUid ✅ FIXED: Use actual value from native merge
                docMappingJson,                    // docMappingJson ✅ FIXED: Use actual value from native merge
                new java.util.ArrayList<>()       // skippedSplits (will be populated elsewhere)
            );
        }
    }
}
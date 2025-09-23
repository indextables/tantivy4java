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
        sb.append("\"aws_config\":null");  // For local testing, no AWS config needed
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

        int numDocs = Integer.parseInt(numDocsStr);
        long size = Long.parseLong(sizeStr);

        return new MergeResult(splitId, numDocs, size, duration);
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
     */
    public static class MergeResult {
        public final String splitId;
        public final int numDocs;
        public final long sizeBytes;
        public final long durationMs;

        public MergeResult(String splitId, int numDocs, long sizeBytes, long durationMs) {
            this.splitId = splitId;
            this.numDocs = numDocs;
            this.sizeBytes = sizeBytes;
            this.durationMs = durationMs;
        }

        public QuickwitSplit.SplitMetadata toSplitMetadata() {
            return new QuickwitSplit.SplitMetadata(
                splitId,                           // splitId
                "process-merge",                   // indexUid
                0L,                                // partitionId
                "process-source",                  // sourceId
                "process-node",                    // nodeId
                (long) numDocs,                    // numDocs
                sizeBytes,                         // uncompressedSizeBytes
                null,                              // timeRangeStart
                null,                              // timeRangeEnd
                System.currentTimeMillis() / 1000, // createTimestamp
                "published",                       // maturity
                new java.util.HashSet<>(),        // tags
                0L,                                // footerStartOffset
                sizeBytes,                         // footerEndOffset
                0L,                                // deleteOpstamp
                1,                                 // numMergeOps
                "",                                // docMappingUid
                "",                                // docMappingJson
                new java.util.ArrayList<>()       // skippedSplits
            );
        }
    }
}
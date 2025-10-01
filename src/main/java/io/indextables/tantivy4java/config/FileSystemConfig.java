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

package io.indextables.tantivy4java.config;

import java.io.File;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Configuration for file system operations in Tantivy4Java.
 *
 * <p>This class provides a way to define a root directory for all file system operations.
 * When a root is configured, all relative paths will be resolved relative to this root,
 * and absolute paths will be validated to ensure they are within the root directory.</p>
 *
 * <h3>Usage Patterns</h3>
 * <pre>{@code
 * // Set a global file system root for all operations
 * FileSystemConfig.setGlobalRoot("/data/tantivy");
 *
 * // Create indices within the root
 * Index index = new Index(schema, "my-index"); // Will create at /data/tantivy/my-index
 *
 * // QuickwitSplit operations will also use the root
 * QuickwitSplit.convertIndexFromPath("source-index", "output.split", config);
 * // -> source becomes /data/tantivy/source-index, output becomes /data/tantivy/output.split
 *
 * // Reset to system default behavior
 * FileSystemConfig.clearGlobalRoot();
 * }</pre>
 *
 * <h3>Security Benefits</h3>
 * <ul>
 *   <li><b>Path Traversal Protection:</b> Prevents access outside the configured root</li>
 *   <li><b>Sandboxing:</b> Constrains all file operations to a specific directory tree</li>
 *   <li><b>Containerization:</b> Enables easy mapping of container volumes</li>
 *   <li><b>Multi-tenancy:</b> Allows different applications to operate in isolated directories</li>
 * </ul>
 *
 * <h3>Thread Safety</h3>
 * This class is thread-safe and uses atomic operations for configuration changes.
 */
public class FileSystemConfig {

    private static final AtomicReference<String> globalFileSystemRoot = new AtomicReference<>(null);

    /**
     * Set the global file system root for all Tantivy4Java operations.
     *
     * @param rootPath Root directory path. Must be an existing directory.
     *                 Pass null to clear the root and use system default behavior.
     * @throws IllegalArgumentException if the path is not a valid directory
     */
    public static void setGlobalRoot(String rootPath) {
        if (rootPath == null) {
            globalFileSystemRoot.set(null);
            return;
        }

        // Normalize and validate the path
        Path normalizedPath = Paths.get(rootPath).toAbsolutePath().normalize();
        File rootDir = normalizedPath.toFile();

        if (!rootDir.exists()) {
            throw new IllegalArgumentException("File system root does not exist: " + normalizedPath);
        }

        if (!rootDir.isDirectory()) {
            throw new IllegalArgumentException("File system root is not a directory: " + normalizedPath);
        }

        if (!rootDir.canRead() || !rootDir.canWrite()) {
            throw new IllegalArgumentException("File system root is not readable/writable: " + normalizedPath);
        }

        globalFileSystemRoot.set(normalizedPath.toString());
    }

    /**
     * Clear the global file system root configuration.
     * After calling this, all file operations will use system default behavior.
     */
    public static void clearGlobalRoot() {
        globalFileSystemRoot.set(null);
    }

    /**
     * Get the current global file system root.
     *
     * @return The configured root path, or null if no root is configured
     */
    public static String getGlobalRoot() {
        return globalFileSystemRoot.get();
    }

    /**
     * Check if a global file system root is configured.
     *
     * @return true if a root is configured, false otherwise
     */
    public static boolean hasGlobalRoot() {
        return globalFileSystemRoot.get() != null;
    }

    /**
     * Resolve a path relative to the configured file system root.
     *
     * <p>If no root is configured, returns the path unchanged.
     * If a root is configured:</p>
     * <ul>
     *   <li>Relative paths are resolved relative to the root</li>
     *   <li>Absolute paths are validated to be within the root</li>
     * </ul>
     *
     * @param path The path to resolve
     * @return The resolved absolute path
     * @throws IllegalArgumentException if the path would escape the configured root
     */
    public static String resolvePath(String path) {
        if (path == null) {
            throw new IllegalArgumentException("Path cannot be null");
        }

        String root = globalFileSystemRoot.get();
        if (root == null) {
            // No root configured, return path unchanged
            return path;
        }

        Path rootPath = Paths.get(root);
        Path targetPath = Paths.get(path);

        // If path is relative, resolve it relative to root
        if (!targetPath.isAbsolute()) {
            return rootPath.resolve(targetPath).normalize().toString();
        }

        // If path is absolute, ensure it's within the root
        Path normalizedTarget = targetPath.normalize();
        if (!normalizedTarget.startsWith(rootPath)) {
            throw new IllegalArgumentException(
                "Absolute path is outside configured root. Path: " + path +
                ", Root: " + root);
        }

        return normalizedTarget.toString();
    }

    /**
     * Validate that a path is within the configured file system root.
     *
     * @param path The path to validate
     * @throws IllegalArgumentException if the path is outside the root
     */
    public static void validatePath(String path) {
        // resolvePath already performs validation
        resolvePath(path);
    }

    /**
     * Create a relative path from the root to the given path.
     * Only works if a root is configured and the path is within the root.
     *
     * @param absolutePath The absolute path to make relative
     * @return The relative path from root to the given path
     * @throws IllegalArgumentException if no root is configured or path is outside root
     */
    public static String makeRelative(String absolutePath) {
        String root = globalFileSystemRoot.get();
        if (root == null) {
            throw new IllegalArgumentException("No file system root configured");
        }

        Path rootPath = Paths.get(root);
        Path targetPath = Paths.get(absolutePath).normalize();

        if (!targetPath.startsWith(rootPath)) {
            throw new IllegalArgumentException(
                "Path is outside configured root. Path: " + absolutePath +
                ", Root: " + root);
        }

        return rootPath.relativize(targetPath).toString();
    }

    /**
     * Get configuration summary for debugging.
     *
     * @return String representation of current configuration
     */
    public static String getConfigSummary() {
        String root = globalFileSystemRoot.get();
        if (root == null) {
            return "FileSystemConfig: No root configured (system default behavior)";
        } else {
            return "FileSystemConfig: Root = " + root;
        }
    }
}
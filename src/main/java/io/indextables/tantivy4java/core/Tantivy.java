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

package io.indextables.tantivy4java.core;

import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;

/**
 * Main entry point for Tantivy4Java library.
 * Handles native library loading and version information.
 */
public class Tantivy {
    private static boolean loaded = false;

    static {
        loadNativeLibrary();
    }

    /**
     * Load the native library for the current platform.
     */
    private static synchronized void loadNativeLibrary() {
        if (loaded) {
            return;
        }

        // Detect actual runtime platform
        String osName = detectOS();
       
        String libraryName;
        if (osName.contains("windows")) {
            libraryName = "tantivy4java.dll";
        } else if (osName.contains("mac") || osName.contains("darwin")) {
            libraryName = "libtantivy4java.dylib";
        } else {
            libraryName = "libtantivy4java.so";
        }

        try {
            // Load from resources
            InputStream is = Tantivy.class.getResourceAsStream("/native/" + libraryName);
            
            if (is != null) {
                Path tempFile = Files.createTempFile("tantivy4java", libraryName);
                Files.copy(is, tempFile, StandardCopyOption.REPLACE_EXISTING);
                System.load(tempFile.toAbsolutePath().toString());
                loaded = true;
                return;
            }
            
            throw new RuntimeException("Native library not found in resources: /native/" + libraryName);
        } catch (Exception e) {
            throw new RuntimeException("Failed to load tantivy4java native library: " + libraryName, e);
        }

    }
    
    /**
     * Detect the actual OS, handling containers correctly.
     */
    private static String detectOS() {
        String osName = System.getProperty("os.name").toLowerCase();
        
        // First check for Linux by looking for /proc/version
        if (new java.io.File("/proc/version").exists()) {
            return "linux";
        }
        
        // Check for common container environment variables
        if (System.getenv("KUBERNETES_SERVICE_HOST") != null ||
            System.getenv("container") != null ||
            new java.io.File("/.dockerenv").exists()) {
            // In a container, check for Linux-specific files
            if (new java.io.File("/etc/os-release").exists() ||
                new java.io.File("/etc/alpine-release").exists()) {
                return "linux";
            }
        }
        
        // Fall back to System property detection
        if (osName.contains("windows")) {
            return "windows";
        } else if (osName.contains("mac") || osName.contains("darwin")) {
            return "darwin";
        } else {
            return "linux";
        }
    }
    
    
    /**
     * Get the version of the Tantivy4Java library.
     * @return Version string
     */
    public static native String getVersion();

    /**
     * Ensure the native library is loaded.
     * This method can be called to trigger library loading if needed.
     */
    public static void initialize() {
        // Library loading happens in static block
    }
}

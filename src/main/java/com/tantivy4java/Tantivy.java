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

package com.tantivy4java;

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

        String osName = System.getProperty("os.name").toLowerCase();
        String osArch = System.getProperty("os.arch").toLowerCase();
        
        // Normalize architecture names
        String normalizedArch;
        if (osArch.equals("x86_64") || osArch.equals("amd64")) {
            normalizedArch = "x86_64";
        } else if (osArch.equals("aarch64") || osArch.equals("arm64")) {
            normalizedArch = "aarch64";
        } else {
            normalizedArch = osArch;
        }
        
        // Determine platform and library name
        String platform;
        String libraryName;
        if (osName.contains("windows")) {
            platform = "windows-" + normalizedArch;
            libraryName = "tantivy4java.dll";
        } else if (osName.contains("mac") || osName.contains("darwin")) {
            platform = "darwin-" + normalizedArch;
            libraryName = "libtantivy4java.dylib";
        } else {
            platform = "linux-" + normalizedArch;
            libraryName = "libtantivy4java.so";
        }

        // Try platform-specific path first, then fallback to generic path
        String[] resourcePaths = {
            "/native/" + platform + "/" + libraryName,
            "/native/" + libraryName
        };
        
        for (String resourcePath : resourcePaths) {
            try {
                InputStream is = Tantivy.class.getResourceAsStream(resourcePath);
                if (is != null) {
                    Path tempFile = Files.createTempFile("tantivy4java-" + platform, libraryName);
                    Files.copy(is, tempFile, StandardCopyOption.REPLACE_EXISTING);
                    System.load(tempFile.toAbsolutePath().toString());
                    loaded = true;
                    return;
                }
            } catch (Exception e) {
                // Try next path or fall back to system library loading
            }
        }

        try {
            System.loadLibrary("tantivy4java");
            loaded = true;
        } catch (UnsatisfiedLinkError e) {
            throw new RuntimeException(
                "Failed to load tantivy4java native library for platform: " + platform + 
                ". Tried paths: " + String.join(", ", resourcePaths), e);
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
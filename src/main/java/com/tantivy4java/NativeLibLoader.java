package com.tantivy4java;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;

public class NativeLibLoader {
    private static final String LIB_NAME = "tantivy4java";
    private static boolean loaded = false;

    public static synchronized void loadNativeLibrary() {
        if (loaded) {
            return;
        }

        String osName = System.getProperty("os.name").toLowerCase();
        String osArch = System.getProperty("os.arch").toLowerCase();
        
        String libExtension;
        if (osName.contains("windows")) {
            libExtension = ".dll";
        } else if (osName.contains("mac")) {
            libExtension = ".dylib";
        } else {
            libExtension = ".so";
        }

        String libFileName = System.mapLibraryName(LIB_NAME);
        if (!libFileName.endsWith(libExtension)) {
            libFileName = "lib" + LIB_NAME + libExtension;
        }

        try {
            String resourcePath = "/native/" + libFileName;
            InputStream libStream = NativeLibLoader.class.getResourceAsStream(resourcePath);
            
            if (libStream == null) {
                throw new RuntimeException("Native library not found in resources: " + resourcePath);
            }

            Path tempFile = Files.createTempFile(LIB_NAME, libExtension);
            tempFile.toFile().deleteOnExit();
            
            Files.copy(libStream, tempFile, StandardCopyOption.REPLACE_EXISTING);
            libStream.close();
            
            System.load(tempFile.toAbsolutePath().toString());
            loaded = true;
            
        } catch (IOException e) {
            throw new RuntimeException("Failed to load native library", e);
        }
    }
}
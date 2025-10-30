// Build script for tantivy4java native library
// Configures symbol visibility for Linux builds

fn main() {
    // Disable ring's assembly optimizations on ARM64 to avoid x86-64 warnings
    #[cfg(target_arch = "aarch64")]
    {
        println!("cargo:rustc-env=RING_PREGENERATE_ASM=1");
        println!("cargo:warning=ARM64 build: Disabling ring assembly optimizations");
    }

    // Only apply version script on Linux builds
    if cfg!(target_os = "linux") {
        println!("cargo:rerun-if-changed=tantivy4java.version");

        // Get the path to the version script
        let version_script = std::env::current_dir()
            .unwrap()
            .join("tantivy4java.version");

        if version_script.exists() {
            println!("cargo:rustc-link-arg=-Wl,--version-script={}", version_script.display());
            println!("cargo:warning=Using Linux version script to limit symbol visibility");
        } else {
            println!("cargo:warning=Linux version script not found, all symbols will be exported");
        }
    }

    // For other platforms, symbols are controlled by default visibility
    #[cfg(not(target_os = "linux"))]
    {
        println!("cargo:warning=Not Linux build - symbol visibility controlled by default");
    }
}
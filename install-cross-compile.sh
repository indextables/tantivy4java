#!/bin/bash

# Install cross-compilation toolchain for Linux on macOS
# This script helps set up cross-compilation from macOS to Linux

set -e

echo "Setting up cross-compilation toolchain for Linux..."

# Check if running on macOS
if [[ "$OSTYPE" != "darwin"* ]]; then
    echo "This script is designed for macOS. On Linux, you can build natively."
    exit 0
fi

# Install cross-compilation targets
echo "Installing Rust targets..."
rustup target add x86_64-unknown-linux-gnu
rustup target add aarch64-unknown-linux-gnu
rustup target add x86_64-apple-darwin
rustup target add aarch64-apple-darwin

# Install cross-compilation toolchain via homebrew
echo "Installing cross-compilation toolchain via Homebrew..."
if ! command -v brew &> /dev/null; then
    echo "Homebrew is required for installing cross-compilation toolchain"
    echo "Install Homebrew: https://brew.sh/"
    exit 1
fi

# Install proper Linux cross-compilation toolchain with sysroot
if ! command -v x86_64-linux-gnu-gcc &> /dev/null; then
    echo "Installing x86_64-linux-gnu toolchain..."
    
    # Uninstall conflicting packages first
    echo "Removing conflicting packages..."
    brew uninstall x86_64-elf-gcc x86_64-linux-gnu-binutils x86_64-unknown-linux-gnu 2>/dev/null || true
    
    # Install messense toolchain with proper sysroot
    echo "Installing messense toolchain with sysroot..."
    if brew tap messense/macos-cross-toolchains &> /dev/null && brew install messense/macos-cross-toolchains/x86_64-unknown-linux-gnu &> /dev/null; then
        # Fix symlink conflicts
        brew unlink x86_64-linux-gnu-binutils 2>/dev/null || true
        brew link --overwrite x86_64-unknown-linux-gnu 2>/dev/null || true
        echo "✅ x86_64-linux-gnu toolchain with sysroot installed"
    else
        echo "⚠️  Could not install x86_64 Linux cross-compiler with sysroot"
        echo "   Linux x86_64 builds may fail"
    fi
else
    echo "✅ x86_64-linux-gnu toolchain already available"
fi

# Install aarch64-linux-gnu toolchain with sysroot
if ! command -v aarch64-unknown-linux-gnu-gcc &> /dev/null; then
    echo "Installing aarch64-linux-gnu toolchain..."
    
    # Install messense toolchain with proper sysroot  
    if brew tap messense/macos-cross-toolchains &> /dev/null && brew install messense/macos-cross-toolchains/aarch64-unknown-linux-gnu &> /dev/null; then
        echo "✅ aarch64-unknown-linux-gnu toolchain with sysroot installed"
    else
        echo "⚠️  Could not install ARM64 Linux cross-compiler with sysroot"
        echo "   Linux ARM64 builds may fail"
    fi
else
    echo "✅ aarch64-unknown-linux-gnu toolchain already available"
fi

# Create .cargo/config.toml for cross-compilation with proper sysroots
echo "Setting up Cargo configuration for cross-compilation..."
mkdir -p .cargo
cat > .cargo/config.toml << 'EOF'
[target.x86_64-unknown-linux-gnu]
linker = "x86_64-linux-gnu-gcc"

[target.aarch64-unknown-linux-gnu]  
linker = "aarch64-linux-gnu-gcc"

# Environment variables for cross-compilation with proper sysroots
[env]
# x86_64 Linux target configuration
CC_x86_64_unknown_linux_gnu = "x86_64-linux-gnu-gcc"
AR_x86_64_unknown_linux_gnu = "x86_64-linux-gnu-ar" 
CARGO_TARGET_X86_64_UNKNOWN_LINUX_GNU_LINKER = "x86_64-linux-gnu-gcc"

# aarch64 Linux target configuration  
CC_aarch64_unknown_linux_gnu = "aarch64-linux-gnu-gcc"
AR_aarch64_unknown_linux_gnu = "aarch64-linux-gnu-ar"
CARGO_TARGET_AARCH64_UNKNOWN_LINUX_GNU_LINKER = "aarch64-linux-gnu-gcc"

# C flags with proper sysroot paths
CFLAGS_x86_64_unknown_linux_gnu = "--sysroot=/opt/homebrew/Cellar/x86_64-unknown-linux-gnu/13.3.0/toolchain/x86_64-unknown-linux-gnu/sysroot"
CXXFLAGS_x86_64_unknown_linux_gnu = "--sysroot=/opt/homebrew/Cellar/x86_64-unknown-linux-gnu/13.3.0/toolchain/x86_64-unknown-linux-gnu/sysroot"

CFLAGS_aarch64_unknown_linux_gnu = "--sysroot=/opt/homebrew/Cellar/aarch64-unknown-linux-gnu/13.3.0/toolchain/aarch64-unknown-linux-gnu/sysroot"
CXXFLAGS_aarch64_unknown_linux_gnu = "--sysroot=/opt/homebrew/Cellar/aarch64-unknown-linux-gnu/13.3.0/toolchain/aarch64-unknown-linux-gnu/sysroot"
EOF

echo ""
echo "🎉 Cross-compilation setup complete!"
echo ""
echo "📦 You can now build platform-specific JARs with:"
echo "   mvn clean package -Pcross-compile"
echo ""
echo "🎯 This will create JARs with native libraries for:"
echo "   ✅ macOS x86_64 (Intel)"
echo "   ✅ macOS ARM64 (Apple Silicon)"
echo "   ✅ Linux x86_64 (AMD64)"
echo "   ✅ Linux ARM64 (AArch64)"
echo ""
echo "💡 Build modes:"
echo "   • Normal build: mvn clean package (current platform only)"
echo "   • Cross-compile: mvn clean package -Pcross-compile (all platforms)"
echo ""
echo "📁 Native libraries will be organized in JAR as:"
echo "   native/darwin-x86_64/libtantivy4java.dylib"
echo "   native/darwin-aarch64/libtantivy4java.dylib" 
echo "   native/linux-x86_64/libtantivy4java.so"
echo "   native/linux-aarch64/libtantivy4java.so"
echo ""
echo "💡 Note: If cross-compilation fails for some targets, the build will"
echo "   continue with available targets. This is normal and expected."
echo ""
echo "🔧 Alternative: Use Docker for guaranteed Linux builds:"
echo "   docker run --rm -v \$(pwd):/workspace -w /workspace rust:latest \\"
echo "     sh -c 'rustup target add x86_64-unknown-linux-gnu aarch64-unknown-linux-gnu && \\"
echo "            cargo build --release --target x86_64-unknown-linux-gnu && \\"
echo "            cargo build --release --target aarch64-unknown-linux-gnu'"
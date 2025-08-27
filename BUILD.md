# Building Tantivy4Java

## Quick Start

### macOS / Linux / Windows
```bash
mvn clean package
```

This will build the native library for your current platform and package it with the JAR.

## Static Linux Builds (No External Dependencies)

For Linux builds with no external dependencies (statically linked), use Docker to build inside a Linux container with musl libc.

> **Note**: Static builds require a Linux environment because Alpine's Rust compiler has limitations with cdylib/proc-macro support for musl targets. The Ubuntu-based approach uses rustup for proper musl toolchain support.

### Method 1: Using Ubuntu Container (Recommended)

```bash
docker run --rm --platform linux/amd64 \
  -v $(pwd):/workspace -w /workspace \
  ubuntu:22.04 sh -c '
    export DEBIAN_FRONTEND=noninteractive &&
    apt-get update &&
    apt-get install -y \
      build-essential \
      curl \
      openjdk-11-jdk \
      maven \
      pkg-config \
      libssl-dev \
      musl-tools \
      musl-dev &&
    
    curl --proto "=https" --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y &&
    export PATH="/root/.cargo/bin:${PATH}" &&
    rustup target add x86_64-unknown-linux-musl &&
    
    export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64 &&
    export RUSTFLAGS="-C target-feature=+crt-static -C target-cpu=x86-64" &&
    export CC_x86_64_unknown_linux_musl=musl-gcc &&
    
    mvn clean package
  '
```

### Method 2: Using the Dockerfile

```bash
docker build --platform linux/amd64 -f Dockerfile.static-build -t tantivy4java-static .
```

This creates a complete Docker image with the statically compiled library.

### Method 3: Native Build Script (Testing)

For testing and validation, you can use the provided test script:

```bash
# Test Ubuntu-based static build
./test-ubuntu-static.sh

# Test native build process
./test-static-build.sh
```

### Benefits of Static Builds
- ✅ **No external dependencies** - Statically linked with musl libc
- ✅ **Universal Linux compatibility** - Works on any Linux distribution
- ✅ **Minimal container images** - Compatible with scratch and distroless images
- ✅ **Serverless ready** - Perfect for AWS Lambda, Google Cloud Functions, etc.
- ✅ **Security hardened** - Eliminates shared library vulnerabilities
- ✅ **x86_64 optimized** - Compiled with target-cpu=x86-64 for performance

### Technical Implementation
- **musl libc static linking** - Uses `RUSTFLAGS="-C target-feature=+crt-static"`
- **Ubuntu + rustup toolchain** - Avoids Alpine's Rust compiler limitations
- **Proper musl-gcc setup** - Cross-compilation support with musl-tools
- **JNI compatibility** - Full Java Native Interface support in static builds

## Build Output

The JAR file will be created in `target/tantivy4java-*.jar` with the native library embedded at:
- macOS: `/native/libtantivy4java.dylib`
- Linux: `/native/libtantivy4java.so`
- Windows: `/native/tantivy4java.dll`

## Requirements

- Java 11+
- Maven 3.6+
- Rust 1.70+ (will be installed by cargo if needed)

## Troubleshooting

### Common Static Build Issues

**"cannot produce cdylib" or "cannot produce proc-macro" errors:**
- This occurs when using Alpine's packaged Rust compiler
- **Solution**: Use the Ubuntu-based approach with rustup (Method 1 above)
- Alpine's Rust 1.87.0 has known limitations with musl targets

**Docker disk space errors:**
- Static builds download many dependencies (1-2GB)
- **Solution**: Ensure adequate disk space or use `docker system prune`

**General build errors:**
1. **Ensure Rust is installed**: `curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh`
2. **Update Rust toolchain**: `rustup update`
3. **Add musl target**: `rustup target add x86_64-unknown-linux-musl`
4. **For Linux static builds**: Always use Docker containers, never cross-compile from macOS
5. **Check musl-gcc**: Ensure `musl-tools` package is installed in Ubuntu

### Verification

To verify a static build has no external dependencies:
```bash
# Check the built library
ldd target/x86_64-unknown-linux-musl/release/libtantivy4java.so
# Should output: "not a dynamic executable" or "statically linked"

# Or in container:
file target/x86_64-unknown-linux-musl/release/libtantivy4java.so
# Should show: "statically linked"
```
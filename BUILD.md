# Building Tantivy4Java

## Quick Start

### macOS / Linux / Windows
```bash
mvn clean package
```

This will build the native library for your current platform and package it with the JAR.

## Linux Container Builds

For consistent Linux builds that work across distributions, use Docker to build inside a Linux container.

> **Important**: True static linking (no external dependencies) is not currently supported due to Rust limitations with `cdylib` crate types and `crt-static`. The builds below produce portable Linux binaries that depend on system libc but work on most Linux distributions.

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
      maven &&
    
    curl --proto "=https" --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y &&
    export PATH="/root/.cargo/bin:${PATH}" &&
    export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64 &&
    
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

### Benefits of Container Builds
- ✅ **Consistent Linux binaries** - Reproducible builds across environments
- ✅ **Broad Linux compatibility** - Works on most modern Linux distributions
- ✅ **Container ready** - Suitable for Docker and container deployments
- ✅ **CI/CD friendly** - Perfect for automated build pipelines
- ✅ **x86_64 optimized** - Native compilation for optimal performance
- ✅ **Clean dependencies** - Only requires system libc (available everywhere)

### Technical Implementation
- **Native compilation** - Builds for the container's native target
- **Ubuntu + rustup toolchain** - Modern Rust toolchain with full feature support
- **Standard glibc linking** - Compatible with virtually all Linux systems
- **JNI compatibility** - Full Java Native Interface support

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

### Common Build Issues

**"cannot produce cdylib" errors:**
- This occurs when attempting true static linking with `crt-static`
- **Root cause**: Rust's `crt-static` feature is incompatible with `cdylib` crate types
- **Solution**: Use regular dynamic linking (Method 1 above) - still very portable

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

To verify the build works correctly:
```bash
# Check the built library
ldd target/rust/release/libtantivy4java.so
# Should show minimal dependencies (typically just libc, libdl, libpthread)

# Or check file type:
file target/rust/release/libtantivy4java.so
# Should show: "ELF 64-bit LSB shared object, x86-64"
```
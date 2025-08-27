#!/bin/bash
# Quick test to verify x86_64 static build works

echo "Testing x86_64 static Linux build in Docker..."

docker run --rm --platform linux/amd64 \
  -v $(pwd):/workspace -w /workspace \
  alpine:latest sh -c '
    echo "=== Installing build dependencies ===" &&
    apk add --no-cache build-base rust cargo openjdk11 maven musl-dev &&
    echo "" &&
    echo "=== System info ===" &&
    echo "Architecture: $(uname -m)" &&
    echo "Rust version: $(rustc --version)" &&
    echo "" &&
    echo "=== Building native library ===" &&
    export JAVA_HOME=/usr/lib/jvm/java-11-openjdk &&
    export RUSTFLAGS="-C target-feature=+crt-static" &&
    echo "Available targets: $(rustc --print target-list | grep musl | head -5)" &&
    echo "Default target: $(rustc -vV | grep host)" &&
    cd native &&
    echo "Building for default Alpine target..." &&
    cargo build --release &&
    echo "" &&
    echo "=== Checking library dependencies ===" &&
    if [ -f target/release/libtantivy4java.so ]; then
      echo "✅ Library built successfully as .so file!" &&
      ldd target/release/libtantivy4java.so 2>&1 || echo "✅ Static library (no dynamic dependencies)"
    else
      echo "❌ Library not found at expected location"
      echo "Checking what was built:"
      find target -name "*tantivy4java*" -type f 2>/dev/null || echo "No tantivy4java files found"
      ls -la target/release/ 2>/dev/null || echo "Release directory not found"
    fi
  '
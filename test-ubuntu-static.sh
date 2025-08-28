#!/bin/bash
# Test Ubuntu-based x86_64 static build

echo "Testing Ubuntu-based x86_64 static Linux build in Docker..."

docker run --rm --platform linux/amd64 \
  -v $(pwd):/workspace -w /workspace \
  ubuntu:22.04 sh -c '
    echo "=== Installing build dependencies ===" &&
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
      musl-dev \
      && echo "" &&
    
    echo "=== Installing Rust via rustup ===" &&
    curl --proto "=https" --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y &&
    export PATH="/root/.cargo/bin:${PATH}" &&
    rustup target add x86_64-unknown-linux-musl &&
    echo "" &&
    
    echo "=== System info ===" &&
    echo "Architecture: $(uname -m)" &&
    echo "Rust version: $(rustc --version)" &&
    echo "Available musl targets: $(rustc --print target-list | grep musl | head -3)" &&
    echo "Default target: $(rustc -vV | grep host)" &&
    echo "" &&
    
    echo "=== Building native library ===" &&
    export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64 &&
    export RUSTFLAGS="-C target-feature=+crt-static -C target-cpu=x86-64" &&
    export CC_x86_64_unknown_linux_musl=musl-gcc &&
    cd native &&
    echo "Building for x86_64-unknown-linux-musl target..." &&
    cargo build --release --target x86_64-unknown-linux-musl &&
    echo "" &&
    
    echo "=== Checking library dependencies ===" &&
    if [ -f target/x86_64-unknown-linux-musl/release/libtantivy4java.so ]; then
      echo "✅ Library built successfully as .so file!" &&
      ldd target/x86_64-unknown-linux-musl/release/libtantivy4java.so 2>&1 || echo "✅ Static library (no dynamic dependencies)"
    else
      echo "❌ Library not found at expected location"
      echo "Checking what was built:"
      find target -name "*tantivy4java*" -type f 2>/dev/null || echo "No tantivy4java files found"
      ls -la target/x86_64-unknown-linux-musl/release/ 2>/dev/null || echo "Release directory not found"
    fi
  '
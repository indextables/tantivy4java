# Linux Symbol Export Control

This document explains how tantivy4java controls symbol visibility on Linux to export only JNI methods and hide internal implementation details.

## 🎯 Purpose

By default, Rust shared libraries export all public symbols, which can:
- Increase binary size
- Expose internal implementation details
- Create potential security vulnerabilities
- Cause symbol conflicts with other libraries

This configuration limits exports to only the `Java_*` functions needed by the JVM.

## 📁 Files

### `native/tantivy4java.version`
GNU ld version script that defines symbol visibility:
```
TANTIVY4JAVA_1.0 {
    global:
        Java_*;  # Export all JNI methods
    local:
        *;       # Hide everything else
};
```

### `native/build.rs`
Cargo build script that automatically applies the version script on Linux builds.

### `build-linux-restricted-symbols.sh`
Convenience script for building with symbol restrictions and analyzing results.

## 🚀 Usage

### Method 1: Automatic (Recommended)
The version script is automatically applied when building on Linux:

```bash
cd native/
cargo build --release
```

The `build.rs` script detects Linux and automatically applies the version script.

### Method 2: Manual Build Script
Use the provided shell script for detailed output:

```bash
./build-linux-restricted-symbols.sh
```

This script:
- Builds the library with symbol restrictions
- Analyzes symbol export efficiency
- Shows statistics about exported symbols

### Method 3: Manual Compilation
Apply the version script manually:

```bash
cd native/
RUSTFLAGS="-C link-args=-Wl,--version-script=tantivy4java.version" cargo build --release
```

## 🔍 Verification

Check exported symbols using standard Linux tools:

```bash
# List all exported symbols
nm -D target/release/libtantivy4java.so | grep " T "

# Count Java_ symbols vs total symbols
nm -D target/release/libtantivy4java.so | grep " T " | grep "Java_" | wc -l
nm -D target/release/libtantivy4java.so | grep " T " | wc -l

# Show only Java_ symbols
nm -D target/release/libtantivy4java.so | grep " T " | grep "Java_"
```

## 📊 Expected Results

With proper symbol restriction:
- ✅ **High efficiency**: >80% of exported symbols are `Java_*` functions
- ✅ **Small symbol table**: Only ~187 exported symbols (all JNI methods)
- ✅ **Secure**: No internal Rust functions exposed
- ✅ **Compatible**: All JNI methods accessible from Java

Without symbol restriction:
- ❌ **Low efficiency**: <50% of exported symbols are JNI methods
- ❌ **Large symbol table**: Thousands of unnecessary symbols
- ❌ **Security risk**: Internal implementation exposed
- ❌ **Potential conflicts**: Name collisions with other libraries

## 🔧 Customization

To export additional symbols, modify `tantivy4java.version`:

```
TANTIVY4JAVA_1.0 {
    global:
        Java_*;              # JNI methods
        my_custom_function;  # Additional exports
    local:
        *;
};
```

## 🌍 Platform Support

- **Linux**: Full support with GNU ld version scripts
- **macOS**: Uses default visibility attributes (symbols controlled by Rust visibility)
- **Windows**: Uses default exports (symbols controlled by Rust visibility)

## 📝 Benefits

1. **Security**: Reduces attack surface by hiding internal functions
2. **Performance**: Smaller symbol table improves dynamic loading time
3. **Compatibility**: Prevents symbol conflicts with other native libraries
4. **Maintenance**: Easier to audit what's exposed to external code
5. **Size**: Reduces shared library metadata size

## 🐛 Troubleshooting

### Symbol not found errors
If you get JNI symbol not found errors:
1. Verify the function exists: `nm -D libtantivy4java.so | grep "function_name"`
2. Check the version script includes the pattern
3. Ensure the function has `#[no_mangle]` in Rust

### Build failures
If the version script causes build failures:
1. Check the file path in `build.rs`
2. Verify GNU ld is available (not just LLVM ld)
3. Test without the version script first

### Symbol analysis shows low efficiency
If many non-JNI symbols are exported:
1. Verify the version script is being applied
2. Check for manual `extern "C"` exports
3. Review dependencies that might export symbols

This symbol export control is essential for production deployments where security and compatibility are critical.
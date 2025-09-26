# Debug Symbols Configuration

This document explains how to build tantivy4java with debug symbols enabled for both Rust native code and Java bytecode.

## Quick Start

### Build with Debug Symbols
```bash
# Use the convenience script
./build-debug.sh

# Or manually with Maven
mvn clean compile -Pdebug
```

### Test with Debug Symbols
```bash
# Run tests with debug build
mvn test -Pdebug -Dtest=YourTestClass

# Run specific test with full backtrace
RUST_BACKTRACE=full mvn test -Pdebug -Dtest=S3PanicInvestigationTest
```

## Debug Configuration Details

### Rust Native Library Debug Symbols

**Debug Profile (`-Pdebug`):**
- **Build Type**: `cargo build` (debug, not release)
- **Optimization**: `-C opt-level=0` (no optimization)  
- **Debug Info**: `-C debuginfo=2` (full debug symbols)
- **Backtrace**: `RUST_BACKTRACE=full`

**Release Profile (default):**
- **Build Type**: `cargo build --release`
- **Optimization**: Release optimizations enabled
- **Debug Info**: `-C debuginfo=2` (debug symbols preserved)
- **Backtrace**: `RUST_BACKTRACE=1`

### Java Debug Symbols

Java compilation always includes debug symbols:
- **debug**: `true`
- **debuglevel**: `lines,vars,source`

This enables:
- Line number information in stack traces
- Variable name preservation
- Source file information

## Debug Profiles in Cargo.toml

```toml
[profile.dev]
debug = true
debug-assertions = true
overflow-checks = true

[profile.release]
debug = true          # Keep debug symbols in release
debug-assertions = false
overflow-checks = false
lto = false          # Disable LTO to preserve debug info
codegen-units = 1    # Better debugging
```

## Debugging Native Crashes

### Using gdb (Linux)
```bash
# Build with debug symbols
./build-debug.sh

# Set environment
export RUST_BACKTRACE=full

# Run under gdb
gdb --args java -Djava.library.path=native/target/debug \
    -cp target/classes:target/test-classes \
    org.junit.runner.JUnitCore com.tantivy4java.YourTestClass

# In gdb:
(gdb) run
(gdb) bt           # Show backtrace when crash occurs
(gdb) info registers
```

### Using lldb (macOS)
```bash
# Build with debug symbols  
./build-debug.sh

# Set environment
export RUST_BACKTRACE=full

# Run under lldb
lldb -- java -Djava.library.path=native/target/debug \
    -cp target/classes:target/test-classes \
    org.junit.runner.JUnitCore com.tantivy4java.YourTestClass

# In lldb:
(lldb) run
(lldb) bt          # Show backtrace when crash occurs
(lldb) register read
```

### Analyzing Crash Dumps

**With debug symbols enabled, you'll get:**
- Function names in backtraces
- Source file names and line numbers
- Variable values and names
- Stack frame information

**Example with debug symbols:**
```
Stack trace:
   0: rust_begin_unwind
   1: core::panicking::panic_fmt
   2: tantivy4java::split_searcher::SplitSearcher::create_with_shared_cache
             at /path/to/split_searcher.rs:123
   3: Java_com_tantivy4java_SplitCacheManager_createSplitSearcher
             at /path/to/split_cache_manager.rs:45
```

## Environment Variables for Debugging

```bash
# Full Rust backtraces with all details
export RUST_BACKTRACE=full

# Enable tantivy4java debug logging
export TANTIVY4JAVA_DEBUG=1

# Enable specific Rust logging
export RUST_LOG=tantivy4java=debug,tantivy=debug

# JVM debug options
export JAVA_OPTS="-XX:+ShowCodeDetailsInExceptionMessages -XX:+UnlockDiagnosticVMOptions"
```

## File Sizes and Performance

**Debug builds are larger and slower:**
- **Debug native library**: ~10-50MB (vs ~5-20MB release)
- **Compilation time**: 2-5x slower
- **Runtime performance**: 2-10x slower

**Use debug builds for:**
- Crash investigation
- Performance profiling with symbols
- Development and testing

**Use release builds for:**
- Production deployments  
- Performance benchmarking
- CI/CD pipelines (unless debugging)

## Maven Profiles Summary

| Profile | Rust Build | Debug Symbols | Optimization | Use Case |
|---------|------------|---------------|--------------|----------|
| default | `--release` | ✅ (level 2) | Full | Production with debug info |
| debug | debug | ✅ (level 2) | None | Development & debugging |
| linux-optimized | `--release` | ✅ (level 2) | x86-64 + full | Linux production |

## IDE Integration

### IntelliJ IDEA
1. Import as Maven project
2. Build with `mvn compile -Pdebug`
3. Set VM options: `-Djava.library.path=native/target/debug`
4. Enable "Break on exceptions" for native crashes

### VS Code
1. Install Rust and Java extensions
2. Use launch.json with debug profile:
```json
{
    "type": "java",
    "request": "launch",
    "vmArgs": "-Djava.library.path=native/target/debug",
    "env": {
        "RUST_BACKTRACE": "full",
        "TANTIVY4JAVA_DEBUG": "1"
    }
}
```

## Troubleshooting

**If debug symbols aren't working:**

1. **Check build output**: Look for debug info in compiler output
2. **Verify file sizes**: Debug binaries should be significantly larger
3. **Test backtrace**: Run `RUST_BACKTRACE=1 your-program` and check output quality
4. **Check strip**: Ensure binaries aren't being stripped: `file native/target/debug/libtantivy4java.*`

**Common issues:**
- **Missing symbols**: Rebuild with `mvn clean compile -Pdebug`
- **Optimized away variables**: Use debug profile, not release
- **Path issues**: Check `-Djava.library.path` points to debug build directory
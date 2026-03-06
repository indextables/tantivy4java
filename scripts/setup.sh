#!/usr/bin/env bash

# setup.sh - Install and verify development dependencies for tantivy4java
#
# Usage:
#   ./scripts/setup.sh          # Install/verify all dependencies
#   ./scripts/setup.sh -h       # Show help
#
# Supports macOS (Homebrew) and Linux (apt-get, yum, dnf).

set -euo pipefail

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
REQUIRED_JAVA_MAJOR=11

# Sibling repo configuration (required by Cargo path dependencies)
QUICKWIT_REPO="https://github.com/indextables/quickwit.git"
TANTIVY_REPO="https://github.com/indextables/tantivy.git"

# ---------------------------------------------------------------------------
# Locate project root
# ---------------------------------------------------------------------------
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
info()  { echo "[INFO]  $*"; }
warn()  { echo "[WARN]  $*" >&2; }
error() { echo "[ERROR] $*" >&2; }
ok()    { echo "[OK]    $*"; }

# ---------------------------------------------------------------------------
# Argument parsing
# ---------------------------------------------------------------------------
while [[ $# -gt 0 ]]; do
    case "$1" in
        -h|--help)
            echo "Usage: $0"
            echo ""
            echo "Install and verify development dependencies for tantivy4java."
            echo ""
            echo "Dependencies installed:"
            echo "  - Java 11 (OpenJDK 11)"
            echo "  - Maven"
            echo "  - Rust toolchain (rustc, cargo via rustup)"
            echo "  - Protobuf compiler (protoc)"
            echo ""
            echo "Sibling repos cloned (if not present):"
            echo "  - quickwit (required by Cargo path dependencies)"
            echo "  - tantivy (required by Cargo path dependencies)"
            echo ""
            echo "Supported platforms:"
            echo "  - macOS (via Homebrew)"
            echo "  - Linux (apt-get, yum, or dnf)"
            exit 0
            ;;
        *)
            error "Unknown option: $1"
            echo "Run '$0 --help' for usage." >&2
            exit 1
            ;;
    esac
done

# ---------------------------------------------------------------------------
# OS detection
# ---------------------------------------------------------------------------
OS="$(uname -s)"
case "$OS" in
    Darwin) PLATFORM="macos" ;;
    Linux)  PLATFORM="linux" ;;
    *)
        error "Unsupported operating system: $OS"
        error "This script supports macOS (Darwin) and Linux."
        exit 1
        ;;
esac

info "Detected platform: $PLATFORM ($OS)"

# ---------------------------------------------------------------------------
# Java version detection
# ---------------------------------------------------------------------------
# Returns the major Java version (e.g. 11, 17, 21) or empty string if not found.
get_java_major_version() {
    local java_cmd="$1"
    if ! command -v "$java_cmd" &>/dev/null; then
        echo ""
        return
    fi
    # java -version outputs to stderr; parse "11.0.x" or "1.8.0" style
    local version_output
    version_output=$("$java_cmd" -version 2>&1 | head -1)
    # Extract version string between quotes
    local version
    version=$(echo "$version_output" | sed -n 's/.*"\(.*\)".*/\1/p')
    if [[ -z "$version" ]]; then
        echo ""
        return
    fi
    # Handle 1.x.y (Java 8 and earlier) vs x.y.z (Java 9+)
    local major
    major=$(echo "$version" | cut -d. -f1)
    if [[ "$major" == "1" ]]; then
        major=$(echo "$version" | cut -d. -f2)
    fi
    echo "$major"
}

# Check if Java 11 is already available
check_java() {
    # First check JAVA_HOME if set
    if [[ -n "${JAVA_HOME:-}" ]] && [[ -x "${JAVA_HOME}/bin/java" ]]; then
        local major
        major=$(get_java_major_version "${JAVA_HOME}/bin/java")
        if [[ "$major" == "$REQUIRED_JAVA_MAJOR" ]]; then
            return 0
        fi
    fi
    # Then check java on PATH
    if command -v java &>/dev/null; then
        local major
        major=$(get_java_major_version java)
        if [[ "$major" == "$REQUIRED_JAVA_MAJOR" ]]; then
            return 0
        fi
    fi
    return 1
}

# Check if Maven is available
check_maven() {
    command -v mvn &>/dev/null
}

# Check if Rust toolchain is available
check_rust() {
    command -v rustc &>/dev/null && command -v cargo &>/dev/null
}

# Check if protoc is available
check_protoc() {
    command -v protoc &>/dev/null
}

# ---------------------------------------------------------------------------
# macOS installation (Homebrew)
# ---------------------------------------------------------------------------
install_macos() {
    info "Checking Homebrew..."
    if ! command -v brew &>/dev/null; then
        error "Homebrew is not installed."
        error "Install it from https://brew.sh and re-run this script."
        error "  /bin/bash -c \"\$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)\""
        exit 1
    fi
    ok "Homebrew found: $(brew --prefix)"

    # On macOS, openjdk@11 is keg-only and may not be on PATH.
    # Check the Homebrew keg path directly before deciding to install.
    if [[ -z "${JAVA_HOME:-}" ]]; then
        for brew_prefix in /opt/homebrew /usr/local; do
            local keg_java="${brew_prefix}/opt/openjdk@11/bin/java"
            if [[ -x "$keg_java" ]]; then
                local keg_major
                keg_major=$(get_java_major_version "$keg_java")
                if [[ "$keg_major" == "$REQUIRED_JAVA_MAJOR" ]]; then
                    export JAVA_HOME="${brew_prefix}/opt/openjdk@11"
                    break
                fi
            fi
        done
    fi

    # --- Java 11 ---
    if check_java; then
        ok "Java $REQUIRED_JAVA_MAJOR already installed"
    else
        info "Installing OpenJDK $REQUIRED_JAVA_MAJOR via Homebrew..."
        brew install openjdk@11
        ok "OpenJDK $REQUIRED_JAVA_MAJOR installed"
    fi

    # --- Maven ---
    if check_maven; then
        ok "Maven already installed"
    else
        info "Installing Maven via Homebrew..."
        brew install maven
        ok "Maven installed"
    fi

    # --- Protobuf compiler ---
    if check_protoc; then
        ok "Protobuf compiler (protoc) already installed"
    else
        info "Installing protobuf via Homebrew..."
        brew install protobuf
        ok "Protobuf compiler installed"
    fi
}

# ---------------------------------------------------------------------------
# Linux installation (apt-get / yum / dnf)
# ---------------------------------------------------------------------------
install_linux() {
    # Detect package manager
    local pkg_mgr=""
    if command -v apt-get &>/dev/null; then
        pkg_mgr="apt-get"
    elif command -v dnf &>/dev/null; then
        pkg_mgr="dnf"
    elif command -v yum &>/dev/null; then
        pkg_mgr="yum"
    else
        error "No supported package manager found (apt-get, dnf, yum)."
        exit 1
    fi
    info "Package manager: $pkg_mgr"

    # Check sudo availability
    local sudo_cmd=""
    if [[ "$(id -u)" -ne 0 ]]; then
        if command -v sudo &>/dev/null; then
            sudo_cmd="sudo"
        else
            error "Not running as root and 'sudo' is not available."
            error "Please run as root or install sudo."
            exit 1
        fi
    fi

    # --- Java 11 ---
    if check_java; then
        ok "Java $REQUIRED_JAVA_MAJOR already installed"
    else
        info "Installing OpenJDK $REQUIRED_JAVA_MAJOR..."
        case "$pkg_mgr" in
            apt-get)
                $sudo_cmd apt-get update -qq
                $sudo_cmd apt-get install -y openjdk-11-jdk
                ;;
            dnf)
                $sudo_cmd dnf install -y java-11-openjdk-devel
                ;;
            yum)
                $sudo_cmd yum install -y java-11-openjdk-devel
                ;;
        esac
        ok "OpenJDK $REQUIRED_JAVA_MAJOR installed"
    fi

    # --- Maven ---
    if check_maven; then
        ok "Maven already installed"
    else
        info "Installing Maven..."
        case "$pkg_mgr" in
            apt-get)
                $sudo_cmd apt-get install -y maven
                ;;
            dnf)
                $sudo_cmd dnf install -y maven
                ;;
            yum)
                $sudo_cmd yum install -y maven
                ;;
        esac
        ok "Maven installed"
    fi

    # --- Protobuf compiler ---
    if check_protoc; then
        ok "Protobuf compiler (protoc) already installed"
    else
        info "Installing protobuf compiler..."
        case "$pkg_mgr" in
            apt-get)
                $sudo_cmd apt-get install -y protobuf-compiler
                ;;
            dnf)
                $sudo_cmd dnf install -y protobuf-compiler
                ;;
            yum)
                $sudo_cmd yum install -y protobuf-compiler
                ;;
        esac
        ok "Protobuf compiler installed"
    fi

    # --- OpenSSL dev headers + pkg-config (required by Rust openssl-sys crate) ---
    info "Ensuring OpenSSL development headers and pkg-config are installed..."
    case "$pkg_mgr" in
        apt-get)
            $sudo_cmd apt-get install -y libssl-dev pkg-config
            ;;
        dnf)
            $sudo_cmd dnf install -y openssl-devel pkgconfig
            ;;
        yum)
            $sudo_cmd yum install -y openssl-devel pkgconfig
            ;;
    esac
    ok "OpenSSL dev headers and pkg-config installed"
}

# ---------------------------------------------------------------------------
# Rust toolchain installation (cross-platform via rustup)
# ---------------------------------------------------------------------------
install_rust() {
    if check_rust; then
        ok "Rust toolchain already installed: $(rustc --version)"
        return
    fi

    info "Installing Rust toolchain via rustup..."
    curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y
    # Source cargo environment for the rest of this script
    # shellcheck disable=SC1091
    . "$HOME/.cargo/env" 2>/dev/null || export PATH="$HOME/.cargo/bin:$PATH"
    ok "Rust toolchain installed: $(rustc --version)"
}

# Ensure cargo is discoverable by Maven's exec-maven-plugin.
# Maven spawns subprocesses that may not inherit the shell PATH, so we
# symlink cargo/rustc into a directory that is on the default system PATH.
ensure_cargo_on_system_path() {
    # Check if cargo is already in a system PATH directory that Maven
    # subprocesses would have on their default PATH.
    local system_dirs=("/usr/local/bin" "/usr/bin")
    if [[ "$PLATFORM" == "macos" ]] && command -v brew &>/dev/null; then
        system_dirs=("$(brew --prefix)/bin" "${system_dirs[@]}")
    fi

    for dir in "${system_dirs[@]}"; do
        if [[ -x "$dir/cargo" ]]; then
            ok "cargo already on system PATH: $dir/cargo"
            return
        fi
    done

    local cargo_bin="$HOME/.cargo/bin/cargo"
    local rustc_bin="$HOME/.cargo/bin/rustc"
    if [[ ! -x "$cargo_bin" ]]; then
        error "cargo not found at $cargo_bin"
        exit 1
    fi

    # Pick a target directory that is on the default PATH
    local target_dir=""
    if [[ "$PLATFORM" == "macos" ]]; then
        target_dir="$(brew --prefix 2>/dev/null)/bin"
    fi
    if [[ -z "$target_dir" ]] || [[ ! -d "$target_dir" ]]; then
        target_dir="/usr/local/bin"
    fi

    if [[ -w "$target_dir" ]]; then
        ln -sf "$cargo_bin" "$target_dir/cargo"
        ln -sf "$rustc_bin" "$target_dir/rustc"
        ok "Symlinked cargo/rustc into $target_dir"
    else
        warn "Cannot write to $target_dir; trying with sudo..."
        sudo ln -sf "$cargo_bin" "$target_dir/cargo"
        sudo ln -sf "$rustc_bin" "$target_dir/rustc"
        ok "Symlinked cargo/rustc into $target_dir (via sudo)"
    fi
}

# ---------------------------------------------------------------------------
# Sibling repositories (required by Cargo path dependencies)
# ---------------------------------------------------------------------------
setup_sibling_repos() {
    local parent_dir
    parent_dir="$(cd "$PROJECT_ROOT/.." && pwd)"

    # --- quickwit ---
    if [[ -d "$parent_dir/quickwit" ]]; then
        ok "quickwit repo already present: $parent_dir/quickwit"
    else
        info "Cloning quickwit (required by Cargo path dependencies)..."
        git clone --depth 1 "$QUICKWIT_REPO" "$parent_dir/quickwit"
        ok "quickwit cloned to $parent_dir/quickwit"
    fi

    # --- tantivy ---
    # The Cargo.toml patches redirect all tantivy references to ../../tantivy.
    # We extract the pinned rev from Cargo.toml and check out that exact revision.
    local cargo_toml="$PROJECT_ROOT/native/Cargo.toml"
    if [[ -d "$parent_dir/tantivy" ]]; then
        ok "tantivy repo already present: $parent_dir/tantivy"
    else
        local tantivy_rev
        tantivy_rev=$(
            grep 'github.com/indextables/tantivy' "$cargo_toml" \
            | grep -o 'rev = "[^"]*"' \
            | head -1 \
            | sed 's/rev = "\(.*\)"/\1/'
        )
        if [[ -n "$tantivy_rev" ]]; then
            info "Cloning tantivy (pinned rev: ${tantivy_rev})..."
            git clone "$TANTIVY_REPO" "$parent_dir/tantivy"
            git -C "$parent_dir/tantivy" checkout "$tantivy_rev" --quiet
        else
            info "Cloning tantivy (HEAD)..."
            git clone --depth 1 "$TANTIVY_REPO" "$parent_dir/tantivy"
        fi
        ok "tantivy cloned to $parent_dir/tantivy"
    fi
}

# ---------------------------------------------------------------------------
# Install dependencies
# ---------------------------------------------------------------------------
echo "=================================================================="
info "tantivy4java - Development Environment Setup"
echo "=================================================================="
echo ""

case "$PLATFORM" in
    macos) install_macos ;;
    linux) install_linux ;;
esac

echo ""
echo "=================================================================="
info "Rust toolchain"
echo "=================================================================="
install_rust
ensure_cargo_on_system_path

echo ""
echo "=================================================================="
info "Sibling repositories"
echo "=================================================================="
setup_sibling_repos

# ---------------------------------------------------------------------------
# Validation
# ---------------------------------------------------------------------------
echo ""
echo "=================================================================="
info "Validating installation"
echo "=================================================================="

ERRORS=0

# Validate Java 11
# On macOS with Homebrew, set JAVA_HOME hint if not already pointing to Java 11
if [[ "$PLATFORM" == "macos" ]]; then
    BREW_JAVA_HOME="/opt/homebrew/opt/openjdk@11"
    # Fall back to Intel Homebrew path
    if [[ ! -d "$BREW_JAVA_HOME" ]]; then
        BREW_JAVA_HOME="/usr/local/opt/openjdk@11"
    fi
    if [[ -d "$BREW_JAVA_HOME" ]]; then
        export JAVA_HOME="$BREW_JAVA_HOME"
    fi
fi

if check_java; then
    # Determine which java we're reporting on
    if [[ -n "${JAVA_HOME:-}" ]] && [[ -x "${JAVA_HOME}/bin/java" ]]; then
        JAVA_VERSION_OUTPUT=$("${JAVA_HOME}/bin/java" -version 2>&1 | head -1)
    else
        JAVA_VERSION_OUTPUT=$(java -version 2>&1 | head -1)
    fi
    ok "Java $REQUIRED_JAVA_MAJOR: $JAVA_VERSION_OUTPUT"
else
    error "Java $REQUIRED_JAVA_MAJOR not found or wrong version"
    if command -v java &>/dev/null; then
        error "  Found: $(java -version 2>&1 | head -1)"
    fi
    ERRORS=$((ERRORS + 1))
fi

# Validate Maven
if check_maven; then
    MVN_VERSION=$(mvn --version 2>&1 | head -1)
    ok "Maven: $MVN_VERSION"
else
    error "Maven not found on PATH"
    ERRORS=$((ERRORS + 1))
fi

# Validate Rust
if check_rust; then
    ok "Rust: $(rustc --version)"
else
    error "Rust toolchain not found"
    ERRORS=$((ERRORS + 1))
fi

# Validate protoc
if check_protoc; then
    ok "Protoc: $(protoc --version)"
else
    error "Protobuf compiler (protoc) not found"
    ERRORS=$((ERRORS + 1))
fi

# Validate sibling repos
PARENT_DIR="$(cd "$PROJECT_ROOT/.." && pwd)"
if [[ -d "$PARENT_DIR/quickwit" ]]; then
    ok "quickwit: $PARENT_DIR/quickwit"
else
    error "quickwit repo not found at $PARENT_DIR/quickwit"
    ERRORS=$((ERRORS + 1))
fi

if [[ -d "$PARENT_DIR/tantivy" ]]; then
    ok "tantivy: $PARENT_DIR/tantivy"
else
    error "tantivy repo not found at $PARENT_DIR/tantivy"
    ERRORS=$((ERRORS + 1))
fi

# ---------------------------------------------------------------------------
# Summary
# ---------------------------------------------------------------------------
echo ""
echo "=================================================================="
if [[ "$ERRORS" -gt 0 ]]; then
    error "Setup completed with $ERRORS error(s). See above for details."
    exit 1
else
    info "Setup complete. All dependencies verified."
fi
echo "=================================================================="

# JAVA_HOME hint for macOS
if [[ "$PLATFORM" == "macos" ]] && [[ -n "${JAVA_HOME:-}" ]]; then
    echo ""
    info "Tip: Set JAVA_HOME in your shell profile for builds:"
    info "  export JAVA_HOME=\"$JAVA_HOME\""
fi

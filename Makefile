# tantivy4java Makefile
# Convenience wrapper around Maven for building and testing

.DEFAULT_GOAL := help

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
export JAVA_HOME ?= /opt/homebrew/opt/openjdk@11

MVN := mvn

# ---------------------------------------------------------------------------
# Targets
# ---------------------------------------------------------------------------
.PHONY: help setup compile test package clean

help: ## Show this help message
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-15s\033[0m %s\n", $$1, $$2}'

setup: ## Install/verify development dependencies (Java 11, Maven, Rust, protoc)
	./scripts/setup.sh

compile: ## Compile sources (Java + Rust native via cargo)
	$(MVN) clean compile

test: ## Run JUnit 5 tests
	$(MVN) test

package: ## Build JAR, skip tests (mvn clean package -DskipTests)
	$(MVN) clean package -DskipTests

clean: ## Remove build artifacts (mvn clean)
	$(MVN) clean

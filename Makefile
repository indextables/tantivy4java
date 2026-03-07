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
# Integration test class patterns for -Dtest (simple class name globs, not ANT paths)
INTEGRATION_TESTS = Real*Test,AzureIntegrationTest,AzureOAuthTokenTest

.PHONY: help setup compile test test-cloud test-all package clean

help: ## Show this help message
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-20s\033[0m %s\n", $$1, $$2}'

setup: ## Install/verify development dependencies (Java 11, Maven, Rust, protoc)
	./scripts/setup.sh

compile: ## Compile sources (Java + Rust native via cargo)
	$(MVN) clean compile

test: ## Run unit tests (excludes cloud/Docker integration tests)
	$(MVN) test

test-cloud: ## Run cloud/Docker integration tests only (requires credentials/Docker)
	$(MVN) test -Dtest="$(INTEGRATION_TESTS)" -DfailIfNoTests=false

test-all: ## Run all tests (unit + cloud/Docker integration)
	$(MVN) test -Pintegration-tests

package: ## Build JAR, skip tests (mvn clean package -DskipTests)
	$(MVN) clean package -DskipTests

clean: ## Remove build artifacts (mvn clean)
	$(MVN) clean

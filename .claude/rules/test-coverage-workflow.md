# Test Coverage Workflow

When writing or reviewing tests for a code change, follow this sequence:

1. **Understand the primary use case** — Ask what this code is supposed to do in production. What's the happy path?
2. **Identify edge cases** — Null/empty inputs, boundary values, concurrent access, error conditions, malformed input
3. **Classify tests by type:**
   - **Unit tests** — isolated logic, mocked dependencies, fast
   - **Integration tests** — real dependencies (S3, Kafka, Spark), tagged separately
   - **E2E tests** — full pipeline validation, run last
4. **Review coverage once** — don't write tests piecemeal. Compile the full list first.
5. **Present to user** — Show a table with each test, its type, and severity before writing any test code:

   | Test | Type | Severity | Why |
   |------|------|----------|-----|
   | `search_withNullQuery_throwsIllegalArgument` | Unit | Critical | Null queries crash in production |
   | `search_emptyIndex_returnsZeroResults` | Unit | High | Common cold-start scenario |
   | `search_withKafkaTimeout_retriesAndSucceeds` | Integration | Medium | Transient failures in prod |

   Severity levels: **Critical** (data loss, crash), **High** (wrong results, degraded UX), **Medium** (edge case, unlikely path), **Low** (cosmetic, defensive)

6. **Wait for approval** — User may reprioritize or cut scope before you start writing tests.

## When to trigger

- After completing any coding task (as part of the auto-review flow)
- When the user asks for tests explicitly
- After each phase of a multi-phase plan

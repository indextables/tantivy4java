# Research Before Code

When discussing any code change, NEVER start coding immediately. Always follow this sequence:

1. **Research** — Read the relevant files, understand the current implementation, check how similar things are done elsewhere in the codebase
2. **Architecture** — Consider how the change fits into the existing architecture, what it touches, what it could break
3. **Options** — Propose 2-3 approaches with pros/cons for each
4. **Recommendation** — State which option you'd pick and why
5. **Wait** — Get explicit approval before writing any code

This applies to bug fixes, new features, refactors, and infrastructure changes. The only exceptions are trivial typo fixes or config value changes where there's only one obvious approach.

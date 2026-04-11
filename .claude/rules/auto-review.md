# Automatic Code Reviews

After completing a coding task (or a phase within a multi-phase plan), automatically run these reviews before reporting completion to the user:

1. **Code review** — launch the `pr-review-toolkit:code-reviewer` agent, focused on the files changed in the task/phase
2. **Test coverage** — follow the test coverage workflow rule: identify primary use case, edge cases, classify tests by type, and present a severity-prioritized table for approval

Run step 1, then step 2. Present combined results to the user.

## When to trigger

- After finishing a single coding task
- After each phase of a multi-phase plan (not just at the end)
- After fixing issues found by a previous review round

## When NOT to trigger

- Config-only changes (settings.json, CLAUDE.md, rule files)
- Pure research / read-only tasks with no code changes
- When the user explicitly says to skip reviews

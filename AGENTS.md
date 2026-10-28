# Working agreement

This file is the single source of truth for every agent and every human working in this repository. Tool-specific entry points such as `CLAUDE.md` import it and add only what is specific to that tool.

## Principles

- Correctness over speed. A change is done when it is tested, reviewed and documented, not when it compiles.
- Small, reversible steps. Several focused commits beat one large one.
- Explicit over implicit. Names, types and errors carry the meaning; comments do not.
- Code documents what the system does. Architecture decision records document why.

## Workflow

1. Understand: read the relevant code and the records in `docs/adr` before proposing a change.
2. Plan: anything that touches architecture gets an ADR first (`/adr`).
3. Test first: write the failing test, make it pass, refactor (`/tdd`). User-facing behaviour gets an end-to-end scenario (`/e2e`).
4. Verify: run the narrowest check first, the full suite before opening a pull request.
5. Review: run `/review` on your own diff before asking anyone else to read it.
6. Ship: `/ship` opens the pull request with the template filled in.

## Definition of done

- Tests cover the new behaviour and the whole suite passes.
- No new lint or type errors.
- Public behaviour changes are reflected in `README.md`.
- Architectural changes have an accepted ADR.
- Commit messages follow Conventional Commits.

## Conventions

Detailed rules live in `.claude/rules`. The short version:

- English everywhere: code, identifiers, commits, documentation.
- No code comments. If something needs explaining, rename it, extract it or write an ADR.
- Fail loudly: no swallowed errors, no silent fallbacks.
- Dependencies point inward: the domain never imports infrastructure.

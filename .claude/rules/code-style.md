# Code style

- Write everything in English: identifiers, messages shown to developers, commit messages, documentation.
- Do not write comments. Express intent through naming, small functions and types. Delete commented-out code on sight.
- Keep functions short and single-purpose. A function that needs a section header needs to be split.
- Name things by what they are, not by how they are used: `invoice`, not `data`; `retryBudget`, not `n`.
- Prefer immutability and pure functions at the core; push side effects to the edges.
- Handle errors explicitly at the boundary that can act on them. Never swallow an error or log and continue without a decision recorded in an ADR.
- No dead code, no permanently-on feature flags, no TODO markers. Unfinished work becomes an issue, not a note in the source.
- Respect the formatter and linter configured in the repository. Never disable a rule inline without an ADR explaining why.

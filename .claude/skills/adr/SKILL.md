---
name: adr
description: Record an architecture decision in docs/adr. Use before changing storage, protocols, public API shape, concurrency model, or any decision that is hard to reverse.
---

# Architecture decision record

1. Read the existing records in `docs/adr` so the new one builds on, references or supersedes them.
2. Take the next sequential number and a short kebab-case title: `docs/adr/NNNN-title.md`.
3. Copy `docs/adr/template.md` and fill every section. Context describes the forces, not the solution. Decision is one paragraph in active voice. Consequences list both gains and costs.
4. List at least two alternatives that were considered and why each lost.
5. Set the status to `Proposed`. It becomes `Accepted` in the pull request that lands the implementation.
6. If the record replaces an older one, set the old record's status to `Superseded by NNNN`.

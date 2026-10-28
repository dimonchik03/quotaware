---
name: review
description: Self-review the current diff against the repository rules before opening a pull request. Use after finishing a change and before /ship.
---

# Self-review

1. Run `git diff --stat main...HEAD` and read every changed file in full, not only the hunks.
2. Dispatch the `reviewer` subagent with the diff and collect its findings.
3. For each finding decide: fix it now, or record in the pull request why not.
4. Walk the checklist:
   - Does every behaviour change have a test? Would the test fail without the change?
   - Is anything in the diff unrelated to the stated goal? Split it out.
   - Are there comments, TODO markers, debug output or commented-out code? Remove them.
   - Are errors handled at the right boundary and never swallowed?
   - Did the public surface change? Update `README.md`.
   - Is there an architectural decision hiding in the diff? Write the ADR.
5. Run the full test and lint commands listed in `AGENTS.md`.
6. Report every finding and what was done about it.

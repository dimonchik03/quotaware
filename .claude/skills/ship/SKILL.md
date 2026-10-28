---
name: ship
description: Open a pull request for the current branch with the repository template filled in. Use after /review passes.
---

# Ship

1. Confirm the branch is not `main` and the working tree is clean.
2. Confirm `/review` ran on the final diff.
3. Rebase on `main` if the branch is behind. Resolve conflicts and re-run the tests.
4. Push the branch.
5. Open the pull request with `gh pr create`, filling every section of `.github/pull_request_template.md`: what changed, why, how it was tested, linked ADRs.
6. Report the pull request URL.

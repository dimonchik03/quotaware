---
name: e2e
description: Write or extend an end-to-end scenario that exercises a user journey through the real application wiring. Use for new user-facing behaviour or for regressions found in production.
---

# End-to-end scenario

1. Describe the journey as a numbered list of user-visible steps ending in an observable outcome.
2. Find the existing e2e suite, its bootstrap and its fixtures. Reuse them; never construct internal objects by hand.
3. Replace external systems only at the process boundary (HTTP, database, clock) with the doubles the suite already provides.
4. Write the scenario top-down: setup, each step as a call through the public surface, assertions on observable state.
5. Run the scenario alone. Confirm it fails without the feature and passes with it.
6. Keep it deterministic: fixed clock, seeded data, no sleeps. Wait on conditions, not on time.
7. Run the full e2e suite once before finishing.

## Rules

- One scenario per journey. Never assert on implementation details.
- A scenario that runs for more than a few seconds needs a justification in its name or an ADR.

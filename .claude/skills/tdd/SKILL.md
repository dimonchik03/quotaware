---
name: tdd
description: Implement a behaviour change test-first. Use when adding a feature, fixing a bug, or when the user asks for TDD.
---

# TDD loop

1. State the behaviour as one sentence of the form "when X, the system does Y".
2. Locate the public interface the test should go through. Do not test internals.
3. Write one failing test named after that sentence. Run only that test and confirm it fails for the expected reason.
4. Write the smallest change that makes it pass. No speculative generality.
5. Run the test again and confirm it is green.
6. Refactor with the test green: naming, duplication, structure. Re-run after each refactor step.
7. Repeat from step 1 for the next behaviour.
8. Before finishing, run the full test command listed in `AGENTS.md` once.

## Rules

- Never write production code without a failing test that demands it.
- Never change a test and the code it covers in the same step.
- If the test is hard to write, the design is wrong. Stop and fix the design first.

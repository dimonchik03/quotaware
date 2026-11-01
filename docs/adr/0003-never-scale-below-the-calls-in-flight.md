# 0003. Never scale below the calls in flight

Date: 2026-11-01

## Status

Accepted

## Context

The scaler reports a replica count and the HPA applies it. When the quota is
exhausted the headroom formula yields zero and the HPA would terminate pods that
still have provider calls open. Those calls are lost, the worker retries them after
restart, and the retries count against the same quota that caused the scale-down.

## Decision

After every other rule, including the frozen and default paths, the ceiling is
raised to at least `ceil(in_flight / concurrency)`, the number of pods needed to
finish the calls currently open. The floor is capped at `max_replicas`: the HPA
cannot exceed the maximum anyway, and a metric above it would misreport the
scaler's intent. This is the one place where the specification's unconditional
floor is narrowed.

## Alternatives considered

- Rely on `terminationGracePeriodSeconds` and a `preStop` drain in the worker: it
  protects the calls but still churns pods that the next evaluation brings back.
- Ignore calls in flight and let SDK retries absorb the loss: the retries burn the
  quota the scaler is trying to protect.

## Consequences

A burst of open calls holds pods for at least one more evaluation. The proxy must
track in-flight calls per bucket. If `concurrency` is configured lower than what
the worker really runs, the floor is overestimated and pods linger; the trigger
metadata has to match the worker.

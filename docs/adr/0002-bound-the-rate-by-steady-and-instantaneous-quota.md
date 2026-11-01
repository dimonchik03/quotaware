# 0002. Bound the rate by both the steady-state and the instantaneous quota

Date: 2026-11-01

## Status

Accepted

## Context

Provider rate-limit headers give two views of one quota: the limit per window
(`limit`) and what is left until the window resets (`remaining`, `reset`). Either
view alone produces a bad replica ceiling. Dividing `remaining` by the time until
reset collapses to zero whenever a window is exhausted early and spikes when a
small remainder is divided by a few hundred milliseconds, so the ceiling sawtooths
once per window. Dividing `limit` by the window ignores other consumers of the same
organization quota and the current depletion, so the ceiling stays high while the
provider is already answering 429.

Requests and tokens are separate constraints with separate windows. Tokens per call
are not known for a bucket until the proxy has seen usage.

## Decision

The allowed call rate for a bucket is the minimum of the steady-state rate
(`limit / window`) and the instantaneous rate (`remaining / seconds_until_reset`),
taken per constraint (requests, tokens) and across constraints, then multiplied by
the safety factor and the share. A window whose reset is already in the past
contributes no instantaneous term, because its `remaining` describes a window that
no longer exists. The token constraint is skipped while tokens per call are unknown,
so a fresh bucket is bounded by its request limit until usage is observed. The
staleness tiers (freeze after `stale_after`, default after `stale_hard`) are
evaluated in the domain next to the formula; the application layer only records
when a bucket was last observed.

## Alternatives considered

- Instantaneous rate only: honest about depletion but sawtooths every window and
  spikes near reset.
- Steady-state rate only: stable but blind to other consumers and to a window that
  is already empty.
- Predicting depletion from the history of `remaining`: closer to the truth but
  needs per-bucket history and tuning; not justified before the benchmark exists.

## Consequences

The ceiling is stable within a window and never exceeds what the full window could
feed. When another consumer stops using the shared quota the ceiling only recovers
at the next reset. A bucket with no observed usage can be scaled on requests alone
and only then learns its token cost; the request limit keeps that bounded.

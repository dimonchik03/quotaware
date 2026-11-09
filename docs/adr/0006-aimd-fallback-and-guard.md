# 0006. AIMD as the fallback without headers and as a guard with them

Date: 2026-11-01

## Status

Accepted. ADR 0013 limits the guard on a header ceiling to 429s the
headers do not explain.

## Context

Not every provider or gateway returns rate-limit headers (Gemini, some proxies,
stripped responses), and a bucket whose last quota is older than `staleHard` says
nothing about the present. ADR 0002 sends such buckets to `defaultCeiling`, which is
safe but learns nothing. The only signal that is always available is the share of
responses that came back 429 and the `retry-after` they carried.

## Decision

Each ScaledObject and bucket runs additive-increase, multiplicative-decrease over a
one-minute window of responses (twelve five-second bins kept in the store and read
relative to the current time, so the share decays once traffic stops). Without a
usable quota the window drives the ceiling: start at `defaultCeiling`, multiply by
`aimdDecrease` when the 429 share exceeds `aimdThreshold`, add `aimdIncrease` per
`stepInterval` of clean traffic, never increase while holding. A decrease holds for
the longest `retry-after` seen in the window, at least one window, and holds are
checked before anything else so one burst is halved once. With a usable quota the
same rule guards the header-derived ceiling: a 429 share above the threshold halves
it and the hold keeps the halved value even when the headers would allow more.

The guard and the fallback work on the ceiling before the drain guard and the store
keeps that bound apart from the reported value, so open calls cannot inflate what
gets halved. A bucket whose quota goes stale restarts AIMD from `defaultCeiling`,
which keeps the `staleHard` tier of ADR 0002; ADR 0002's "default" is now the start
of AIMD rather than a fixed number.

## Alternatives considered

- Default ceiling only: safe, blind, and the reason this record exists.
- Probing upward until 429 and backing off by a fixed amount: slower to react and
  no better founded than TCP's rule.
- Estimating the window from the 429 cadence: needs more state and a model of the
  provider's limiter that the headers already give when they exist.

## Consequences

Buckets without headers recover slowly on purpose, one step per interval. A burst of
429s on a bucket with headers costs one halving and one window of hold even when the
headers already dropped the ceiling. The 429 share is exported as
`quotaware_rate_limited_ratio`, so the rule's input is visible.

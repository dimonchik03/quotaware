# 0013. Look past an imminent reset and guard only against unexplained 429s

Date: 2026-11-05

## Status

Accepted. Amends ADR 0002 (the instantaneous rate) and ADR 0006 (the guard).

## Context

The changing-quota benchmark of 2026-11-05 cut the request quota from 600 to 180
for one minute. Ten workers spent the 180 requests within seconds, and every later
call in that minute got a 429 whose headers reported nothing remaining and a reset
seconds away. Two rules turned those 429s into a ceiling of one pod that outlasted
the window:

- The instantaneous rate divides what remains by the time to the reset. Once a
  window is spent it stays zero until the reset, so the ceiling sits at
  `minReplicas` even when the reset is closer than the time a new worker needs to
  start serving. After the reset the step-up rule needs several intervals to climb
  back, and the refilled window runs short of workers.
- The AIMD guard of ADR 0006 counted those 429s as congestion, halved the bound and
  held it for at least one window. The hold ended about 30 seconds after the reset
  had restored the quota. The guard exists for 429s the headers do not predict,
  such as acceleration limits and `slow_down`; these were predicted exactly.

A replay of the dip through the scaler reproduces the minute at one pod and the
climb that starts half a window late.

## Decision

The instantaneous rate looks `lookAhead` ahead, a trigger setting from zero to one
window with a default of 30 seconds. A reset further away than that keeps the rate
at `remaining / until_reset`. A reset inside the horizon makes the rate the average
over the horizon: what remains plus the part of a full window the reset brings
before the horizon ends, `(remaining + limit * (lookAhead - until_reset) / window) /
lookAhead`. The rate is continuous where the reset enters the horizon and meets the
steady-state rate as the reset arrives, so the ceiling climbs while the reset
approaches. It assumes the next window carries the limit of the current one.

A 429 is explained when its own headers report a window that cannot admit another
call: no request remaining, or fewer tokens remaining than a call uses. The store
counts explained and unexplained 429s apart. The guard on a header ceiling reacts to
the share of unexplained 429s only; the AIMD fallback without a usable quota keeps
reacting to every 429. `quotaware_unexplained_429_total` and
`quotaware_unexplained_rate_limited_ratio` expose what the guard reads.

## Alternatives considered

- Drop the instantaneous rate within some seconds of a reset: simpler, but the
  ceiling jumps between two rates at the cutoff and ignores a remainder that is
  still there.
- Shorten the guard's minimum hold to the `retry-after`: the same 429s stay in the
  one-minute window and halve the ceiling again when the hold ends.
- Release the guard when the headers show a new window: the guard would need window
  boundaries, and it would still halve a ceiling the spent window already bounds.
- Model continuous refill as a token bucket, the known limitation of the fixed-window
  term: the better long-term answer for both providers, but it needs successive
  `remaining` values per bucket and a benchmark against a refilling provider first.

## Consequences

Within `lookAhead` of a reset the ceiling can rise above what the current window
still serves, so calls in those seconds may meet 429s until the reset; `lookAhead:
"0s"` restores the rate of ADR 0002. A next window smaller than the current one is
over-estimated for at most `lookAhead`, until the first poll that sees its headers.
A provider that sends 429s without headers, or with quota still remaining, triggers
the guard as before. Each 429 is judged by its own headers, so one that carries none
is unexplained.

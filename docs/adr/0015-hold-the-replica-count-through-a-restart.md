# 0015. Hold the replica count through a restart

Date: 2026-11-09

## Status

Accepted. Amends ADR 0009.

## Context

The scaler keeps its state in memory. A process that has just started knows no
bucket, and `GetMetrics` answered such a poll with `defaultCeiling`, one by default.
KEDA passes that number to the HPA as the replica count, so every restart, including
the one the chart performs on each upgrade, asked for one pod before a single call
had been observed. The combined ceiling recorded that one, and once calls arrived the
ceiling climbed back by `maxStepUp` per `stepInterval` instead of jumping to what the
headers allow. The HPA's scale-down stabilization hides the dip for its window, 300
seconds by default, but a climb that outlasts the window removes pods a working fleet
was using. KEDA keeps the current count when a scaler answers an error, which the
README already relies on for an unreachable quotaware.

## Decision

A poll for a trigger that no answered call matches is answered with gRPC
`Unavailable` while the process is younger than the trigger's `stepInterval`, and
with `defaultCeiling` after that. The trigger's own interval is the warm-up because it
is the cadence at which the trigger expects its ceiling to move, so a trigger tuned
for slow steps also waits longer for its first answer, and because it needs no new
setting. Nothing is recorded for a poll answered this way, so the first ceiling after
the warm-up is computed from the headers without a previous value and skips the
`maxStepUp` limit, as ADR 0009 describes for a fresh entry.

## Alternatives considered

- Report the drain floor of the calls in flight: a bucket whose calls have begun but
  not been answered knows only that floor, which can be lower than the count serving
  those calls, so the first poll would scale down on no information.
- A `-warm-up` flag: one more knob to mirror between the chart and the triggers, for
  a duration the trigger already states.
- Persist the ceilings across restarts: the README lists in-memory state as a known
  limitation, and a store is a larger decision than this one.

## Consequences

A restart keeps the replica count for one `stepInterval` (15 s by default) and then
behaves as before. Every `Unavailable` poll counts as a failed poll for a KEDA
`fallback` when one is configured, so a `stepInterval` longer than the poll interval
times `failureThreshold` hands the deployment to the fallback replicas during the
warm-up; the examples configure no fallback. The e2e queue scenario waits out the
warm-up before it polls a bucket the proxy never sees. ADR 0009's description of a
new process holds once the warm-up has passed.

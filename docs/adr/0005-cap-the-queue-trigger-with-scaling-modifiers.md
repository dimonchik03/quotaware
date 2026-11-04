# 0005. Cap the queue trigger with the quota ceiling through scalingModifiers

Date: 2026-11-01

## Status

Accepted. ADR 0010 replaces the sum across credentials with one shared quota by
default.

## Context

Demand for LLM workers comes from the queue; the quota decides how many workers
can be fed. KEDA evaluates every trigger of a ScaledObject and takes the largest
replica count, which is the wrong combination here: a full queue next to an empty
quota would still scale out and start the 429 feedback loop the project exists to
break. The two numbers have to be combined as a minimum, and the combination has to
survive KEDA restarts, HPA sync and the operator's own caching.

## Decision

The external scaler reports the ceiling as a plain replica count (`targetSize: 1`
with `AverageValue` semantics) and never activates a ScaledObject on its own. The
queue trigger keeps providing demand. The two are combined by KEDA's
`scalingModifiers` with the formula `demand / N < ceiling ? demand / N : ceiling`,
where `N` is the jobs-per-replica target: inside a formula KEDA uses the raw trigger
values and ignores each trigger's own target, so the division has to be written out.
The ternary is the documented form; `min()` from the bundled expression language is
not relied on until it is verified. With `scalingModifiers` KEDA activates the
ScaledObject from the formula result, not from the individual triggers, so an
exhausted quota deactivates it; deployments that must stay warm keep
`minReplicaCount` at one or more. For clusters without `scalingModifiers`, the scaler
offers a single-trigger mode in which it reads the queue depth itself and returns
the minimum, with activation from the queue.

Ceilings are computed per ScaledObject and per observed bucket under the store's lock, so
hysteresis is tracked per consumer and concurrent polls (KEDA asks from its polling
loop and from every HPA sync) cannot raise the ceiling twice. A raise is limited to
`maxStepUp` per `stepInterval`, measured from the last change; a drop is immediate.
Ceilings are summed across credentials that share a model and clamped to
`maxReplicas`.

## Alternatives considered

- Computing the minimum inside the scaler for every setup: makes quotaware a queue
  client for every queue KEDA already supports, and hides the demand signal from the
  HPA and dashboards.
- Reporting raw headroom (calls per second) and letting the HPA divide by a target:
  moves the concurrency and latency arithmetic into ScaledObject metadata where it
  cannot be tested.
- Mutating the ScaledObject's `maxReplicaCount` from a controller: fights the
  owner's manifests and GitOps, and reacts only on reconcile.

## Consequences

Mode A needs KEDA 2.12 or newer for `scalingModifiers`. The formula takes the queue
term whenever it is lower, so the drain floor inside the ceiling (ADR 0003) does not
stop a queue-driven scale-down in this mode; Mode B applies it to the value it
returns. The formula names the
triggers, so both triggers must carry a `name`. In mode B quotaware depends on a
queue client (Redis lists in v0.1). The ceiling series exported by the metrics
endpoint is labelled by ScaledObject so operators can see which consumer a number
belongs to.

# 0005. Cap the queue trigger with the quota ceiling through scalingModifiers

Date: 2026-11-01

## Status

Accepted

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
queue trigger keeps providing demand and activation. The two are combined by KEDA's
`scalingModifiers` with the formula `demand < ceiling ? demand : ceiling`, which is
the documented ternary form; `min()` from the bundled expression language is not
relied on until it is verified. For clusters without `scalingModifiers`, the scaler
offers a single-trigger mode in which it reads the queue depth itself and returns
the minimum, with activation from the queue.

Ceilings are computed per ScaledObject and per observed bucket, so hysteresis is
tracked per consumer, and summed across credentials that share a model.

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

Mode A needs KEDA 2.11 or newer for `scalingModifiers`. The formula names the
triggers, so both triggers must carry a `name`. In mode B quotaware depends on a
queue client (Redis lists in v0.1). The ceiling series exported by the metrics
endpoint is labelled by ScaledObject so operators can see which consumer a number
belongs to.

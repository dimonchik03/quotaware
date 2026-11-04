# Design

quotaware turns the rate-limit headers a provider already returns into a replica
ceiling that KEDA can read. This document describes how the pieces fit; the reasons
behind the hard choices are in `docs/adr`.

## Layers

    domain        internal/headroom   the ceiling formula, guards and staleness tiers
                  internal/aimd       congestion-control fallback and guard
    application   internal/state      per-bucket store: quotas, smoothed usage and
                                      latency, calls in flight, a one-minute 429 window,
                                      reported ceilings per consumer
                  internal/headers    provider header parsers over http.Header
                  internal/usage      usage extraction from JSON and SSE bodies, body
                                      capture, credential hashing
    adapters      internal/proxy      reverse proxy and the /v1/observe endpoint
                  internal/scaler     KEDA ExternalScaler over gRPC
                  internal/queue      Redis list reader for single-trigger mode
                  internal/metrics    Prometheus collector over the store
                  collector           Go RoundTripper and Python httpx hooks that feed
                                      /v1/observe without the proxy hop
    entry point   cmd/quotaware       proxy | all | simulate

Dependencies point inward. The domain imports nothing from the repository and takes
the clock as a parameter, so every rule is a table test. Adapters never import each
other; `cmd/quotaware` wires them.

## Data flow

1. A worker calls the provider through the proxy, which reads the body within a
   deadline, reads the model from it, hashes the credential, registers a call in
   flight for the bucket `(provider, model, credential)` and forwards the body as
   replayable so the transport may retry a stale connection. A collector inside the worker observes the same
   call and posts it to `/v1/observe` afterwards; collector traffic therefore never
   counts as in flight and the drain guard only sees proxied calls.
2. When the response arrives, the rate-limit headers are parsed into quotas. The body
   streams to the worker untouched while a bounded capture keeps a copy; at the end
   of the body the usage object is extracted (JSON, the final OpenAI chunk, or
   Anthropic's `message_start` plus `message_delta`), inflated first when the provider
   compressed it.
3. `state.Store.End` applies the sample: the windows that parsed replace the known
   ones even when another header was malformed, tokens per call and latency are
   smoothed with an EWMA, counters move, the 429 window records the response, and the
   call leaves the in-flight count exactly once. How the body ended decides the
   counter: completed, abandoned by the client (an abort), or broken by the upstream
   (an upstream error); a client that leaves before any response is an abort.
4. KEDA asks the scaler for the ceiling of a ScaledObject. For every bucket that
   matches the trigger metadata, the scaler evaluates the domain under the store's
   lock: the headroom formula when a usable quota is known, AIMD otherwise, with
   the AIMD guard on top while 429s exceed the threshold. Raises are limited to
   `maxStepUp` per `stepInterval` from the last change of the bound, the ceiling before
   the drain guard, so open calls moving the drain floor never delay a raise; drops are
   immediate; the drain guard never reports fewer pods than the open calls need.
5. The credentials of the model that the provider has accepted at least once with a
   2xx or a 429 are views of one quota: the bound of the one with the newest headers
   is the ceiling, raised to one drain floor over the calls open on every credential
   the provider has not refused, clamped, and returned as a replica count. With
   `quotaScope: credential` the ceilings are summed instead (ADR 0010). KEDA's `scalingModifiers` take the minimum of the queue demand
   and the ceiling (ADR 0005); the metrics endpoint exposes every number along the way.

## The ceiling in one formula

    pod_rate     = concurrency / latency
    steady_rate  = min(limit_requests, limit_tokens / tokens_per_call) / window
    instant_rate = min(remaining_requests / until_reset, remaining_tokens / tokens_per_call / until_reset)
    ceiling      = floor(min(steady_rate, instant_rate) * safety * share / pod_rate)

bounded by `minReplicas` and `maxReplicas`, raised by at most `maxStepUp` per
`stepInterval`, never below `ceil(in_flight / concurrency)`, frozen after
`staleAfter` and handed to AIMD, starting from `defaultCeiling`, after `staleHard`
(ADR 0002, 0003, 0006).

## Without headers

Buckets that never showed a usable quota, or whose last observation is older than
`staleHard`, are driven by AIMD: start at `defaultCeiling`, add `aimdIncrease` per
`stepInterval` of clean traffic, multiply by `aimdDecrease` when the 429 share of the
last minute exceeds `aimdThreshold`, then hold for the longest `retry-after` seen,
at least one window, and never halve twice on one burst. The window counts only the
responses that carry the provider's verdict on the quota, 2xx and 429, so a 401 or
an outage neither dilutes the share nor passes for clean traffic (ADR 0009). The same rule guards a
header-derived ceiling: a burst of 429s halves it even when the headers still look
healthy. Both act on the ceiling before the drain guard, and the store keeps that
bound next to the reported value (ADR 0006).

The shared trigger also honors an active retry hold on any matching credential.
An optional workload label identifies which open calls contribute to its drain floor.
The combined gauge shows the available ceiling before the Redis demand cap.
[ADR 0012](adr/0012-shared-holds-and-workload-drain.md) records both scopes.

## Concurrency model

The store is a mutex around plain maps. The proxy touches it twice per call, the
scaler once per bucket per poll, the metrics collector once per scrape. Ceilings are
updated with a closure under the same lock so KEDA's polling loop and the HPA sync
cannot interleave. The in-memory store means one quotaware replica per cluster; a
shared backend is the planned step towards several.

## Trust boundaries

Nothing that arrives over the network is trusted on its own (ADR 0008). The
observe endpoint needs a bearer token unless the operator opts out explicitly.
Trigger metadata may only point single-trigger mode at Redis addresses the operator
listed. Ceiling state is keyed by a hash of the trigger metadata next to the
ScaledObject, so a caller that sends other metadata for the same name evaluates its
own entry and cannot move the one KEDA continues from, and one gRPC connection may
hold at most two streams per ScaledObject it may serve (ADR 0009). The store caps the
buckets and the ceilings it tracks and counts what it refused, and the scaler bounds
the names, models, credentials and request sizes it accepts, so an unauthenticated
caller cannot grow memory or label cardinality without limit. It can still fill both
tables and lock new buckets or ScaledObjects out until eviction; the NetworkPolicy is
the fence against that (ADR 0009).

## What is deliberately not here

No request routing, retries, authentication or cost accounting. The proxy copies
bytes and reads; the workers keep their own SDKs and retry logic. A missing usage
object is counted, never estimated.

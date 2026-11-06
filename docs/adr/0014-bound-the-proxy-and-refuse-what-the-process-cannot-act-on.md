# 0014. Bound the proxy's open calls and refuse what the process cannot act on

Date: 2026-11-06

## Status

Accepted. Extends ADR 0008.

## Context

Three places still accepted more than the process could act on. The proxy cloned
the default transport, which waits for response headers without limit, and admitted
any number of calls, each with up to `-max-request-bytes` of request and
`-max-capture-bytes` of response buffered: a provider that accepted connections and
never answered parked every worker call, its buffers and its in-flight count, and
with it the drain floor, until the SDK's own timeout, and a runaway worker fleet
could grow the proxy past its memory limit. The shutdown gave open calls a hard-coded
ten seconds and reported the deadline as a failure, so an upgrade of a busy proxy
exited 1 every time. The scaler read only the metadata keys it knew, so a misspelt
`safety` ran on the default without a trace, and `/v1/observe` answered 202 for an
observation the bucket limit had discarded, so collectors counted a delivery that
never landed.

## Decision

The proxy waits at most `-upstream-header-timeout` (10 min, the SDKs' own default)
for a provider's response headers and answers 504 past it, counted as an upstream
error. It keeps at most `-max-inflight` (256) calls open and answers 503 before
reading the body of any call beyond that, counted in `quotaware_calls_rejected_total`.
Shutdown gives open calls `-drain-timeout` (10 s), then closes them with a warning in
the log and a clean exit status. The scaler rejects a trigger metadata key outside
its settings, apart from the keys KEDA reads for itself (`scalerAddress` and the TLS
settings), with InvalidArgument. `/v1/observe` answers 503 when the bucket limit
discards an observation.

## Alternatives considered

- A connection limit on the proxy listener: SDK pools keep idle connections, so
  connections do not count calls; the semaphore counts what is buffered.
- A write timeout on the proxy server: it would cut long streams; the header timeout
  bounds only the wait for the provider to start answering.
- Log an unknown metadata key once per trigger: a log line is easy to miss, while the
  gRPC error reaches the ScaledObject's condition where the operator looks.
- Keep 202 and rely on `quotaware_buckets_rejected_total`: the collector's own failure
  counter is what its operator watches.

## Consequences

A non-streaming call whose provider needs more than ten minutes to answer fails with
504; raise the flag for such workloads. The proxy's worst-case memory is
`-max-inflight` times the two body limits and the README says so. A trigger with a
key quotaware does not know stops scaling until the key is removed, which the upgrade
notes call out. Collectors behind a full store see failed deliveries, as they should.

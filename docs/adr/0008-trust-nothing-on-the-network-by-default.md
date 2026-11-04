# 0008. Trust nothing on the network by default

Date: 2026-11-02

## Status

Accepted. ADR 0009 closes the gRPC and proxy paths this record left open.

## Context

quotaware listens on three ports inside the cluster. The proxy forwards provider
calls, the admin port serves metrics and `/v1/observe`, and the gRPC port serves
KEDA. In v0.1.0 an observation was accepted from anyone when no token was set, the
chart could not set one, trigger metadata could point single-trigger mode at any
Redis address, every address was dialled and cached forever, and the store grew with
every distinct bucket or ScaledObject name a caller invented. A pod in the same
cluster could therefore pin a workload to `minReplicas`, push it to `maxReplicas`
and into a 429 storm, make quotaware connect to arbitrary hosts, or grow its memory
and label cardinality without bound. A NetworkPolicy is the right outer fence, but
quotaware cannot assume one is in place.

## Decision

The defaults refuse what cannot be trusted. The process does not start without an
observe token unless `-observe-insecure` is passed, and the chart does not render
without `observe.token`, `observe.existingSecret` or an explicit `observe.insecure`.
Single-trigger mode only reads the Redis addresses listed with `-redis-address`;
any other `redisAddress` in trigger metadata is answered with PermissionDenied, and
without the flag queue mode is off. ScaledObject names and namespaces are bounded to
253 characters. The store tracks at most `-max-buckets` buckets and
`-max-scaled-objects` ScaledObjects, counts what it refused in
`quotaware_buckets_rejected_total` and `quotaware_scaled_objects_rejected_total`,
and the scaler answers ResourceExhausted for a ScaledObject beyond the limit.

## Alternatives considered

- Rely on a NetworkPolicy alone: it is invisible to the chart, easy to forget, and
  absent on clusters without a policy-aware CNI.
- mTLS on the gRPC and admin ports: KEDA supports it, but it adds certificate
  management to the first deployment for a threat the allow list and the token already
  cover; it remains possible later.
- Unbounded maps with eviction: eviction only runs after ten minutes of silence, so a
  caller that keeps inventing names keeps the maps growing.

## Consequences

A fresh install has one more value to set, and the demo and benchmark pass
`observe.insecure=true` explicitly. Mode B needs the Redis address in two places,
the trigger and the scaler flag, which is deliberate. The limits are high enough that
a real deployment never meets them; the counters show when something does.

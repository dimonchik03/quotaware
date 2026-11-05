# 0012. Keep congestion with the quota and drain with the workload

Date: 2026-11-04

## Status

Accepted. Extends ADRs 0003 and 0010.

## Context

Rotating API keys must preserve a provider's retry hold when the keys share quota.
Two Redis queues can use the same key and model while running independent workers.
The key identifies a view of the quota but cannot identify the deployment that
owns an open call.

## Decision

In shared mode bound the newest quota estimate by every matching credential's
active congestion hold. Keep the per-credential mode independent. A worker may
send a short DNS label in `X-Quotaware-Workload`. The proxy strips it before
forwarding and counts admitted open calls by workload within the existing
bucket. A trigger with `workload` uses those calls for its drain floor and
activation, while its quota estimate still sees all matching traffic. An omitted
workload keeps the existing bucket-wide floor. The combined Prometheus gauge
remains a quota ceiling measured before Redis demand.

## Alternatives considered

- Pin one key per workload. Key rotation and workers that must share credentials
  make this unreliable.
- Split every observation into workload buckets. This duplicates provider quota
  and introduces persistent metric labels for ephemeral caller input.
- Ignore open calls on an empty queue. A worker can still be streaming its last job.

## Consequences

Workload names are operator-supplied routing metadata. Distinct queues sharing
a bucket must use distinct labels or separate observers. An unseen label holds
no workers, so configure the worker and its trigger together. Workload entries
exist only while their calls remain open and never become Prometheus labels.

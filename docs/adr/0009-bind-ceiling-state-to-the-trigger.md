# 0009. Bind ceiling state to the trigger and count only accepted responses

Date: 2026-11-02

## Status

Accepted. ADR 0010 replaces the sum across credentials with one shared quota by
default.

## Context

ADR 0008 closed the observe endpoint, the Redis addresses and the size of the
store, and rejected mTLS on the gRPC port because the allow list and the token
covered the threats it listed. A review of that claim found two paths still open to
any pod that can reach quotaware.

The ceiling state of a ScaledObject is keyed by the namespace and name KEDA sends,
and every call recomputes it from the metadata of that call. A caller that sends the
same name with its own metadata overwrites the bound, the change time and the hold
that KEDA's next poll continues from: `maxReplicas: "1"` pins the workload near its
minimum because a raise is limited to `maxStepUp` per `stepInterval`, and a large
`aimdIncrease` on a header-less bucket lifts the stored bound to whatever the real
trigger clamps it to. The gRPC server also accepts an unbounded number of streams per
connection, and a cap-trigger `StreamIsActive` parks a goroutine until the client
leaves.

The second path runs through the proxy. The one-minute window that drives AIMD
counts every response, so a credential the provider answers with 401 looks like
clean traffic and climbs by `aimdIncrease` per `stepInterval` to `maxReplicas`. The
scaler sums the ceilings of every bucket of a model, so one bogus key sent through
the proxy every fifteen seconds pushes a ScaledObject that does not pin `credential`
to its maximum. A worker with a revoked key does the same without an attacker.

## Decision

The consumer key gains a SHA-256 of the trigger metadata, encoded with a length
prefix on every key and value so no two metadata maps share it, and kept to 128 bits;
a call with different metadata evaluates its own entry and never moves the one KEDA's
trigger reads, and the `quotaware_ceiling` series carries the hash as the `trigger`
label. The gRPC server limits the streams one connection may hold to two per
ScaledObject it serves, a parked `StreamIsActive` and a call in flight, because KEDA
multiplexes every ScaledObject of a scaler address over one connection. The Helm
chart ships a NetworkPolicy, on by default, that admits KEDA's operator to the gRPC
port and the release namespace to the proxy and admin ports, with peer lists for
anything else. The 429 window counts only responses that carry the provider's verdict
on the quota, 2xx and 429, so a 401, a 403 or a 5xx neither dilutes the ratio nor
counts as clean traffic. A bucket enters a ScaledObject's sum only once the provider
has answered its credential at least once with a 2xx or a 429; until then it is
tracked and exported but not scaled on. Calls open on credentials that have no
answer yet count through one drain floor across all of them, so ADR 0003 still holds
for a credential whose first calls are in flight, and a credential the provider has
refused adds nothing.

## Alternatives considered

- mTLS on the gRPC port: still the complete answer, still deferred for the reason in
  ADR 0008; the metadata hash removes the overwrite without certificates.
- A shared secret in the trigger metadata through a TriggerAuthentication: KEDA
  merges it into the metadata, so it would work, but it adds a secret per
  ScaledObject and leaves the consumer table open to filling; it remains possible on
  top of the hash.
- Counting 5xx as congestion in the AIMD window: the provider's limiter speaks in
  429s; an outage should hold the ceiling, not halve it or grow it.
- Requiring `credential` in every trigger: it would close the sum to bogus keys but
  break the documented case of several keys feeding one model.

## Consequences

Editing a ScaledObject's trigger metadata, to tune `safety` for instance, starts its
entry from nothing, as a new quotaware process would: an AIMD ceiling restarts at
`defaultCeiling`, a header ceiling skips the `maxStepUp` limit on that first
evaluation, and a guard hold is lost. Seeding the new entry from the old one would
reopen the overwrite, because the old entry is found by name, which is what a caller
can forge. The old entry is evicted after ten minutes of silence and both are
visible under their `trigger` label meanwhile.

Clusters whose CNI enforces NetworkPolicy and whose KEDA does not run in the `keda`
namespace with the label `app: keda-operator` must set `networkPolicy.keda`, or KEDA
cannot reach the scaler; the README says so next to the other peer lists.

A credential that only ever receives 401 or 403 never scales anything and shows up
in the bucket metrics for the operator to notice. A 429 counts as an answer,
including OpenAI's `insufficient_quota` for an unfunded key, so a real key of
another team enters an unpinned ScaledObject's sum; pinning `credential` is the
answer when keys of several teams share one proxy. A caller that keeps many calls
open on fresh keys still holds `ceil(open / concurrency)` pods until the provider
answers them, as any caller of the proxy can.

The stream limit bounds what one connection can park; the NetworkPolicy, not the
limit, is the fence against many connections, and against a caller filling the
ScaledObject table with invented names or metadata, which ADR 0008 bounds but does
not prevent.

# 0010. Credentials of a model share one quota unless the trigger says otherwise

Date: 2026-11-04

## Status

Accepted. Amends ADR 0009, which kept summing across credentials.

## Context

A bucket is keyed by provider, model and a hash of the credential, and until now
the scaler added up the ceilings of every bucket of a model that the provider had
accepted. That is right only when every key carries a quota of its own. OpenAI
scopes rate limits to the organization and the project, Anthropic to the
organization and the workspace; several keys of one organization therefore report
the same remaining quota, and the sum counts it once per key. The repository's own
fake provider keeps one window per model for every key, and the scaler test that
pinned the sum fed two keys identical headers and expected twice the ceiling.

The overcount is not academic. Rotating a key leaves the old bucket fresh for
`staleAfter` and frozen until `staleHard`, so for up to ten minutes the old and the
new key each report the full ceiling. Every accepted bucket is also clamped to
`minReplicas` and raised by `maxStepUp` before the sum, so N keys put the floor at N
and the raise at N times `maxStepUp` per interval. The kind demo avoided all of this
only because its trigger pins `credential`.

Two smaller faults sat in the same path. In single-trigger mode the value was
`min(ceiling, demand)`, so an empty queue reported zero and turned the trigger
inactive while calls were still open, against ADR 0003. And a credential whose
first answer was a 5xx counted as answered but not accepted, so its open calls left
every drain floor, although ADR 0009 meant to drop only credentials the provider
refused.

## Decision

By default the credentials that match a trigger are treated as views of one quota.
The ceiling is the bound of the bucket with the newest rate-limit headers, the
newest view of that quota (the largest bound when no bucket has headers yet), raised
to one drain floor over the calls open on every matching credential that the
provider has not refused. A trigger whose keys each carry their own quota, such as
keys of separate organizations, sets `quotaScope: credential` and gets the sum as
before. A bucket is refused only when its answers so far are 401 or 403 and none was
a 2xx or a 429; the open calls of every other credential count towards the drain
floor. In single-trigger mode the value is `max(min(ceiling, demand), drain floor)`
and the trigger stays active while that floor is above zero.

## Alternatives considered

- Group buckets by the organization or project the provider names in its response
  headers: the exact answer, but OpenAI and Anthropic do not document such headers
  as stable on every response, and a gateway may strip them. It remains possible as
  a third scope.
- Take the largest bound: simple, but a bucket frozen on headers that were healthy
  two minutes ago would outvote a fresh one that reports the quota spent.
- Take the smallest bound: a bucket frozen on an exhausted window would hold the
  workload near `minReplicas` until `staleHard`.
- Keep the sum and document `credential`: leaves the unsafe behaviour as the default
  for the common case of several keys in one organization.

## Consequences

A ScaledObject that relied on the sum across keys of separate organizations scales
lower until it sets `quotaScope: credential`. Changing the scope changes the trigger
metadata, so ADR 0009's rule applies and its ceiling state starts afresh.
`quotaware_ceiling` still reports one series per credential; the number KEDA
receives is no longer their sum by default. Single-trigger mode keeps pods for open
calls after the queue drains, so a scale to zero waits for them to finish.

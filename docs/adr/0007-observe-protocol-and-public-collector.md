# 0007. The /v1/observe protocol and the public collector package

Date: 2026-11-01

## Status

Accepted. ADR 0008 makes the observe token required by default; an optional
`headers_age_ms` field dates the headers of an observation.

## Context

ADR 0004 chose the proxy as the first observer and deferred collectors that run
inside the worker. Teams that already route provider calls through a gateway, or
that cannot add a hop, still need their calls observed, and the collectors ship
inside worker images that outlive any single quotaware release.

## Decision

quotaware accepts observations on `POST /v1/observe` on the admin port: one JSON
object per provider call with the provider, the model, the credential hash as the
proxy computes it (twelve hex characters of SHA-256 over the credential header, or
`anonymous`), the status, the rate-limit and `retry-after` headers, the usage in
tokens when the body carried it, the latency and whether the body completed. The
endpoint validates every field, bounds the model length, the latency and the usage,
and when `-observe-token` is set it requires the token as a bearer. The path carries
the version; a breaking change gets `/v2/observe` and the old path stays for one
minor release. Observations do not register calls in flight, so the drain guard only
sees proxied traffic.

Two collectors ship in the repository and share the behaviour: an `http.RoundTripper`
in the public Go package `github.com/dimonchik03/quotaware/collector` and `httpx`
event hooks in `collector/python`. Both post through a bounded queue, drop when it is
full, never raise into the call they observe, and count what they could not deliver.
The Go package imports this module's internal packages; consumers inherit the
module's dependency graph, and since the repository forbids documentation comments
the README is the package's contract. A nested module is the escape hatch if the
graph becomes a burden.

## Alternatives considered

- Collectors as separate repositories: version skew with no shared test suite.
- gRPC instead of JSON over HTTP: more to vendor in every worker language for a
  message that fits in one JSON object.
- Trusting the network and skipping the token: the admin port is reachable
  cluster-wide and a buggy collector can move a ceiling.

## Consequences

Every worker language needs a collector or the proxy. The wire type lives in one Go
type used by the endpoint and the Go collector, and the Python hook mirrors it in its
tests. Operators who expose the admin port beyond the namespace have to set the
token.

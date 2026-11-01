# 0004. Observe through a transparent proxy first

Date: 2026-11-01

## Status

Accepted

## Context

The scaler needs, per bucket, the rate-limit headers of every provider response,
the usage object from the body, the wall-clock latency and the number of calls in
flight. Workers are written in several languages and use several SDKs. The
observation has to be collected somewhere that sees every call without asking each
worker to change first.

## Decision

Version 0.1 observes through a transparent reverse proxy. Workers point their
`base_url` at it; it forwards `POST /v1/chat/completions` and `POST /v1/messages`
unchanged, including server-sent event streams, and records what it sees. Buckets
are keyed by provider, model and a hash of the credential, so the key never appears
in a label. Usage is read from the response body when it is present: the `usage`
object of a JSON response, the final chunk of an OpenAI stream when the client asked
for `stream_options.include_usage`, and `message_start` plus `message_delta` of an
Anthropic stream. A response without usage is counted, not estimated. SDK-side
collectors that post the same observation without the extra hop are a later step.

## Alternatives considered

- SDK hooks only: no extra hop, but every worker has to be changed before the
  scaler sees anything, and each language needs its own collector.
- Reading the provider's usage or admin API: lags by minutes, has no latency or
  in-flight information, and is not available for every provider.
- A service mesh or eBPF capture: sees the traffic but has to terminate TLS and
  parse bodies anyway, with far more operational weight.

## Consequences

Every provider call takes one more hop inside the cluster. The proxy must stream
without buffering and must never alter a request or response. Usage extraction
depends on the client asking for usage in streams, which the README has to say.
Multi-replica proxies need shared state before the observation is complete, which is
why the in-memory store sits behind an interface-sized surface.

# 0011. Discard the errors of writes and cleanup that nothing can act on

Date: 2026-11-04

## Status

Accepted

## Context

The working agreement forbids swallowing an error or logging and continuing without
a recorded decision. A few call sites produce errors that no code in the process can
act on: the reply to an HTTP client that has already gone, the drain and close of a
response body whose status has already been handled, and the close of a client at
process exit. Handling them anyway means inventing a reaction (a log line per
disconnected client, a failure counter for a delivery that succeeded), and that
reaction is noise or a wrong number.

## Decision

Three kinds of error are discarded with `_ =` and nothing else:

- writing an error or response body to an HTTP client, in the proxy, the observe
  endpoint and the bench fake provider: the status line is already sent, and a failed
  body write means the client left;
- draining and closing a response body after its status was read and acted on, in the
  Go collector's delivery and the bench worker's success path: the outcome is already
  recorded, and a failed drain only costs the connection its reuse;
- closing a client at process exit in the bench binaries: the process ends either
  way.

Every other error is returned, counted or turns into a status code. A new `_ =` must
fit one of these three kinds.

## Alternatives considered

- Log each one at debug level: still logging and continuing, and a busy proxy would
  log every client that disconnects mid-error.
- Count them in a metric: no operator would act on a counter of clients that left
  before reading a 400.

## Consequences

The three kinds are the only discarded errors; review checks new ones against this
list. A body that breaks before its status is handled, or before a job counts as
complete, is not covered and must be handled.

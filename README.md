# quotaware

**Scale LLM workers on provider quota, not queue depth.**

A KEDA external scaler for LLM worker deployments. It reads the rate-limit headers
your provider already returns (OpenAI, Anthropic), computes how many worker pods the
remaining quota can actually feed, and caps your existing queue-based trigger at that
number.

## Why

Queue-depth autoscaling past the provider's RPM/TPM limit is a feedback loop: more
pods, more 429s, more backoff, longer queue, more pods. The headers to stop it are on
every response; nothing wires them to the autoscaler.

## How it works

1. `quotaware proxy` sits in front of the provider and records rate-limit headers,
   usage, latency and 429s per `(provider, model, key)`.
2. The headroom algorithm turns remaining quota, tokens per call and latency into a
   replica ceiling, with an AIMD fallback when headers are missing.
3. `quotaware scaler` serves that ceiling to KEDA over gRPC; a `scalingModifiers`
   formula takes the minimum of your queue trigger and the ceiling.

## Try it without a cluster

`quotaware simulate` replays a JSONL file of observations through the headroom
algorithm and prints the replica ceiling it would report for each one:

    go run ./cmd/quotaware simulate -concurrency 4 -max 40 -input cmd/quotaware/testdata/observations.jsonl | column -t

    at                    ceiling  reason    allowed_rate  pod_rate
    2026-11-01T18:00:00Z  4        headroom  8.36          2.00
    2026-11-01T18:00:20Z  3        headroom  7.44          2.00
    2026-11-01T18:00:55Z  3        headroom  6.80          2.00
    2026-11-01T18:00:59Z  3        drain     0.00          2.00
    2026-11-01T18:01:05Z  4        headroom  8.50          2.00
    2026-11-01T18:04:00Z  4        frozen    0.00          0.00

Each input line is one JSON object describing a bucket at a point in time:

    {"at":"2026-11-01T18:00:00Z","requests":{"limit":1000,"remaining":990,"reset_at":"2026-11-01T18:01:00Z"},"tokens":{"limit":600000,"remaining":590000,"reset_at":"2026-11-01T18:01:00Z"},"tokens_per_call":1000,"latency_ms":2000,"in_flight":4}

`at` is the evaluation time. `observed_at` is optional and defaults to `at`; set it
earlier to replay a stale bucket. `requests` and `tokens` carry the provider's
limit, remaining and reset for the window; leave one out when the provider does
not report it. `tokens_per_call` and `latency_ms` are the smoothed averages the
proxy would hold, `in_flight` the calls currently open. Unknown keys are rejected.

Output is tab-separated. The reason column says which rule produced the number:
the headroom formula, a clamp to `-min` or `-max`, the `-max-step-up` cap on
raises, the drain guard that keeps pods alive for calls in flight, a frozen
ceiling for stale observations, or the default when nothing usable was observed.
Run with `-h` for every knob.

## Planned usage

    make demo        # kind cluster, KEDA, fake provider, workers, quotaware
    make bench       # two runs, same seed: queue trigger vs queue trigger + quotaware

Both targets land with milestones 3 and 4 of `docs/SPEC.md`. Benchmark charts and
numbers will be published in `docs/benchmark.md`.

## Non-goals

Not a gateway, not a rate limiter, not a cost dashboard. See `docs/SPEC.md`.

## Status

v0.1 in progress. Milestone 1 (header parsers, headroom algorithm, `simulate`) is
done; the proxy, the scaler and the benchmark follow. Specification in `docs/SPEC.md`,
decisions in `docs/adr`.

## License

MIT

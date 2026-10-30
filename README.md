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

## Planned usage

    make demo        # kind cluster, KEDA, fake provider, workers, quotaware
    make bench       # two runs, same seed: queue trigger vs queue trigger + quotaware

Both targets land with milestones 3 and 4 of `docs/SPEC.md`. Benchmark charts and
numbers will be published in `docs/benchmark.md`.

## Non-goals

Not a gateway, not a rate limiter, not a cost dashboard. See `docs/SPEC.md`.

## Status

v0.1 in progress. Specification in `docs/SPEC.md`, decisions in `docs/adr`.

## License

MIT

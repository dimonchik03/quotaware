# quotaware — specification

## One-liner

A KEDA external scaler, written in Go, that sizes LLM worker deployments by the
provider's remaining rate-limit headroom instead of by queue depth.

## Problem

Asynchronous LLM workloads on Kubernetes (transcript analysis, enrichment,
classification, agent steps) run as queue consumers. The standard way to autoscale
them is KEDA with a queue trigger: Redis list length, RabbitMQ queue depth, SQS
messages. That works until the real bottleneck appears, which is not CPU and not the
queue. It is the provider's rate limit: requests per minute and tokens per minute.

Past that point queue-depth scaling keeps adding pods the quota cannot feed:

1. Queue grows, so the autoscaler adds pods.
2. More pods means more concurrent provider calls, which trip the limit.
3. The provider answers 429 with `retry-after`; SDKs back off exponentially.
4. Pods sit idle in backoff while the queue still looks long, so the autoscaler keeps
   them and adds more.
5. Spiky traffic also trips acceleration limits (Anthropic) and `slow_down` (OpenAI),
   which fire even below the nominal RPM/TPM.

Cost: pod-seconds burned while waiting and retry traffic that counts
against RPM. The extra pods buy no throughput, because the quota caps it either way.

The signal needed to stop it already exists. Both OpenAI and Anthropic return
limit, remaining and reset headers on every response. Nothing today feeds that signal
into the autoscaler. quotaware does.

## Non-goals

- Not an LLM gateway: no auth, routing, failover or provider abstraction. Use LiteLLM
  or Bifrost for that; quotaware can sit beside or behind them.
- Not a rate limiter: it never rejects, delays or reorders requests. It only informs
  scaling.
- Not a cost dashboard. It exposes a few Prometheus metrics, nothing more.
- Not an SDK. Workers keep their own clients and retry logic.

## How it works

One Go binary, `quotaware`, with subcommands `proxy`, `all` (proxy and scaler in
one process, the v0.1 deployment) and `simulate`. A standalone `scaler` arrives with
the shared state backend, since the scaler reads what the proxy observed.

### 1. Observation: `quotaware proxy`

A transparent reverse proxy for the OpenAI Chat Completions API (`/v1/chat/completions`)
and the Anthropic Messages API (`/v1/messages`). Workers point their `base_url` at it;
request and response bodies pass through untouched, including SSE streams.

Per response it records, keyed by bucket `(provider, model, api-key hash)`:

- status code, `retry-after` on 429;
- rate-limit headers (see the provider matrix below);
- token usage from the response body (`usage` object; for streams, the final
  `message_delta` on Anthropic and the usage chunk on OpenAI when
  `stream_options.include_usage` is set; otherwise the call is counted as usage
  missing, never estimated);
- wall-clock latency and in-flight count;
- how the body ended: completed, abandoned by the client, or broken by the upstream.
  An abandoned body counts as an abort, a broken one as an upstream error, and a
  client that leaves before the upstream answers is an abort, not an upstream error.

The request body is read within `-read-body-timeout` (30 s, then 408) and handed to
the transport as replayable, so a pooled connection found dead before the request is
written is retried.

Collectors that remove the extra hop: a Go `http.RoundTripper` wrapper and a Python
`httpx` event hook that post the same observation to `POST /v1/observe` on the admin
port behind a bearer token that is required unless `-observe-insecure` is passed
(ADR 0007, ADR 0008). An optional `headers_age_ms` measures the time from receipt of the provider headers
to sending the report, including local delivery backlog. The observer uses this age to
date quota windows and duration resets.

Quotas are dated by when their headers arrived, on the proxy as on the observe path,
and a sample whose headers are older than the ones already applied leaves the quotas
alone, so a long stream that ends after a short 429 cannot restore the remaining quota
the 429 reported. Each window keeps the time it was last reported, and a window not
reported for longer than `stale_after` stops bounding the ceiling.

### 2. State

Per bucket, in memory (Redis backend in phase 2 for multi-replica proxies):

- `limit_requests`, `remaining_requests`, `reset_requests_at`
- `limit_tokens`, `remaining_tokens`, `reset_tokens_at` (Anthropic: input and output
  tracked separately as well)
- EWMA of tokens per call (uncached input plus cache writes plus output), EWMA of latency
- in-flight calls, 429 count and ratio over the last window, last `retry-after`
- `observed_at` for staleness

### 3. Decision: the headroom algorithm

Inputs for one bucket, with `k` = concurrent calls per worker pod (trigger metadata)
and `s` = safety factor (default 0.85):

    pod_rate      = k / latency_ewma
    steady_rate   = min(limit_tokens / tokens_per_call, limit_requests) / window_seconds
    instant_rate  = min(remaining_tokens / tokens_per_call, remaining_requests) / seconds_until_reset
    allowed_rate  = min(steady_rate, instant_rate) * s
    ceiling       = floor(allowed_rate / pod_rate)
    ceiling       = clamp(ceiling, min_replicas, max_replicas)

`pod_rate` is the number of calls per second one pod can make. For Anthropic's token
bucket, `window_seconds` is the refill period implied by the per-minute limit and
`seconds_until_reset` comes from the RFC 3339 reset header. For OpenAI, reset headers
are durations.

Why both rates: using only "remaining until reset" produces a sawtooth (the ceiling
collapses at the end of every window and jumps back after reset); using only
"limit per window" ignores other consumers of the same organization quota. The minimum
of the two is stable and honest.

Guards:

- Hysteresis: raise the ceiling by at most `max_step_up` pods per `step_interval`
  (default 2 per 15 s); lower it immediately.
- Drain guard: never report a ceiling below `ceil(in_flight / k)`, capped at
  `max_replicas`, so the ceiling itself never asks for fewer pods than the open calls
  need; in the two-trigger mode the queue term can still go lower, and Kubernetes
  picks which pods go.
- Staleness: observations older than `stale_after` (default 120 s) freeze the last
  ceiling; older than `stale_hard` (default 10 min) fall back to `default_ceiling`.
- `share`: fraction of the organization quota this workload may use (default 1.0).
  Headers reflect org-wide usage, so two quotaware instances for two services would
  each claim the whole headroom. Documented limitation in v0.1.
- `default_ceiling` must lie within `[min_replicas, max_replicas]`; a trigger that
  says otherwise is rejected instead of clamped.

AIMD fallback when headers are absent, unparseable or stale (Gemini, some gateways,
Azure variants to be verified): classic congestion control. If the share of 429 among
the 2xx and 429 responses of the last minute exceeds `aimdThreshold` (default 0.02),
multiply the ceiling by
`aimdDecrease` (default 0.5) and hold it until the longest observed `retry-after`
elapses, at least one minute and at most `staleHard` (a `retry-after` beyond a day is
malformed); otherwise add `aimdIncrease` (default 1) per
`stepInterval` up to `maxReplicas`. Header mode and AIMD run together; AIMD acts as a
guard even when headers exist (ADR 0006).

### 4. Actuation: the scaler in `quotaware all`

A gRPC server implementing KEDA's `ExternalScaler` service
(`pkg/scalers/externalscaler/externalscaler.proto`, vendored):

- `GetMetricSpec` returns metric `quotaware-ceiling` with `targetSize: 1` (the
  quota ceiling before queue demand is `quotaware_ceiling{key="combined"}`; the
  other `quotaware_ceiling` series are the per-credential ceilings it combines).
- `GetMetrics` returns the ceiling as `metricValue`. With KEDA's default
  `AverageValue` semantics the HPA computes `ceil(metricValue / targetSize)`, so the
  returned number is the replica count itself.
- `IsActive` is always false for a cap trigger, so activation stays with the demand
  trigger; in single-trigger mode it is true while the queue has items or calls are
  open.
- `StreamIsActive` sends the state once and, in single-trigger mode, pushes changes
  of the queue activity so the trigger can run as `external-push`. A push only
  re-runs activation; the HPA metric is still refreshed on its own cadence.
- A raise of the ceiling is limited to `maxStepUp` per `stepInterval` (default 15 s)
  however often KEDA polls, counted from the last change of the ceiling before the
  drain guard; a drop is immediate. The limit applies to each credential's ceiling and
  again to the value KEDA receives, so a credential seen for the first time, or the
  switch from the default ceiling to the first answered one, cannot jump past it. Ceilings are evaluated under the
  store's lock, so concurrent polls cannot raise twice.

Trigger metadata: `scalerAddress`, `bucket` (`openai/gpt-4.1-mini`), `concurrency` (k),
and optionally `credential` (the hashed key label, to narrow a shared model to one
key), `quotaScope` (`shared`, or `credential` when every key has a quota of its own),
`safety` (0.85), `share` (1), `maxStepUp` (2), `stepInterval` (15s),
`defaultCeiling` (1), `minReplicas` (1), `maxReplicas` (100, mirror
`maxReplicaCount` by hand), `staleAfter` (2m), `staleHard` (10m), `window` (1m),
`aimdThreshold` (0.02), `aimdDecrease` (0.5), `aimdIncrease` (1).
Ceilings are computed per ScaledObject, trigger and bucket: the state a poll continues
from is keyed by the namespace, the name and a hash of the trigger metadata, so a
caller that sends other metadata for the same name evaluates its own entry and never
moves the one KEDA's trigger reads (ADR 0009). Only credentials the provider has
accepted at least once with a 2xx or a 429 are scaled on. By default they are views
of one quota, since providers scope limits to the organization or project: the
bound of the bucket with the newest headers is the ceiling, raised to one drain floor
over the calls open on every matching credential the provider has not refused with
only 401s and 403s. With `quotaScope: credential` the ceilings are summed instead,
plus one drain floor for credentials not answered yet (ADR 0010). The result is
clamped to `maxReplicas`. A cap trigger never reports `IsActive`;
activation stays with the demand trigger. The scaler trusts nothing in the request
beyond what it validates: ScaledObject names and namespaces are bounded to 253
characters, at most `-max-scaled-objects` (1000) ceilings are tracked, one per
ScaledObject, trigger and credential, and a poll that needs another gets
ResourceExhausted, and single-trigger mode only dials the Redis addresses
listed with `-redis-address`; any other `redisAddress` is PermissionDenied (ADR 0008).
One gRPC connection may hold at most 2 × `-max-scaled-objects` (2000) concurrent
streams, room for a parked `StreamIsActive` and a call in flight per ScaledObject,
because KEDA multiplexes every ScaledObject of a scaler address over one connection;
a caller that parks streams cannot grow the process beyond that (ADR 0009). Editing a trigger's metadata starts its ceiling state
afresh, as a new quotaware process would.

The optional `workload` trigger label matches the `X-Quotaware-Workload` request header for drain and activation. An omitted label includes all matching calls. In shared mode, every active credential hold bounds the shared estimate. See [ADR 0012](adr/0012-shared-holds-and-workload-drain.md).

### 5. Combining with the existing queue trigger

KEDA takes the maximum across triggers, which is the opposite of what we need: demand
should come from the queue and the cap from the quota. Two supported modes.

Mode A, recommended, uses `scalingModifiers` with the formula
`demand / 10 < ceiling ? demand / 10 : ceiling`, where `demand` is the raw queue
length (inside a formula KEDA ignores each trigger's own target, so the
jobs-per-replica division moves into the formula) and `ceiling` is this scaler's
replica count. The full manifest is `deploy/examples/scaledobject-mode-a.yaml`. The
ternary form is documented by KEDA; `min()` from the bundled expression language is
not relied on until it is verified. With `scalingModifiers` the ScaledObject is active
while the formula result is above `activationTarget`; an exhausted quota scales to
zero only with `minReplicaCount: 0` and the trigger's `minReplicas: "0"`. The formula
takes the queue term whenever it is lower, so the drain floor inside the ceiling does
not protect calls in flight from a queue-driven scale-down in this mode; Mode B keeps
it.

Mode B, single trigger, for clusters where `scalingModifiers` is unavailable:
quotaware reads the queue depth itself (Redis list in v0.1) and returns
`max(min(demand_replicas, ceiling), drain_floor)`; activation follows the queue and
the calls still open. The manifest is
`deploy/examples/scaledobject-mode-b.yaml`.

### 6. Metrics

Prometheus on `/metrics`, labelled by `provider`, `model` and `key` (a hash of the
credential): `quotaware_limit_requests`, `quotaware_remaining_requests`,
`quotaware_limit_tokens` and `quotaware_remaining_tokens` (with a `window` label for
the organization, project, input and output token windows), `quotaware_reset_seconds`
per window, `quotaware_tokens_per_call`, `quotaware_latency_seconds`,
`quotaware_inflight`, `quotaware_retry_after_seconds`, `quotaware_rate_limited_ratio`,
`quotaware_ceiling` (labelled by `namespace`, `scaledobject` and `trigger` as well),
and the counters `quotaware_responses_total`,
`quotaware_429_total`, `quotaware_aborted_total`, `quotaware_upstream_errors_total`,
`quotaware_headers_missing_total`, `quotaware_headers_malformed_total` and
`quotaware_usage_missing_total`. Unlabelled: `quotaware_buckets`,
`quotaware_scaled_objects`, `quotaware_buckets_rejected_total` and
`quotaware_scaled_objects_rejected_total`. A Grafana dashboard JSON ships in
`deploy/grafana/`.

## Provider header matrix

| Provider | Headers | Reset format | Notes |
|----------|---------|--------------|-------|
| OpenAI | `x-ratelimit-{limit,remaining,reset}-{requests,tokens}`, plus `-project-tokens` variants when a project limit applies | Durations such as `1s`, `6m0s` | Limits per model or model family at org and project level. 429 may carry `Retry-After`; `slow_down` fires on rapid ramps even inside limits. |
| Anthropic | `anthropic-ratelimit-{requests,tokens,input-tokens,output-tokens}-{limit,remaining,reset}`, `retry-after` | RFC 3339 timestamp | Token bucket with continuous refill. `remaining` rounded to the nearest thousand. `tokens-*` shows the most restrictive active limit. ITPM counts uncached input plus cache writes; cache reads do not count on most models. Acceleration-limit 429s exist. |
| Gemini | unverified | n/a | AIMD-only until verified with a live request. |
| Azure OpenAI | to verify | | |

Parsers live in `internal/headers/` with fixture tests over raw response heads.
A malformed header never breaks scaling and never discards the rest of the response:
an unparseable reset keeps its window without a reset, an unparseable limit or
remaining leaves only that window unknown, and each one increments
`quotaware_headers_malformed_total`. A response with no usable window leaves the last
quotas in place, which freeze after `stale_after` and hand the bucket to AIMD after
`stale_hard`; one without any rate-limit header increments
`quotaware_headers_missing_total`.

## Benchmark

Runs on a laptop with `kind`, costs zero tokens, reproducible with `make bench`.
The exported series, the three SVG charts, the summary table and `run.json` (start,
end, label, the commit the images were built from and whether the tree was dirty)
are committed under `bench/results/<date>/`; the manifests and the fake provider's
limits are the ones in `bench/` at that commit.

Components under `bench/`:

- `fakeprovider/` (Go): serves `/v1/chat/completions` and `/v1/messages`. Per-model
  RPM and TPM limits in two modes, fixed window (a simplification of OpenAI's limiter) and token bucket
  (Anthropic-like); correct headers in both formats; 429 plus `retry-after` when
  exhausted; optional acceleration 429 on steep ramps; configurable latency
  distribution (default 1.5 s with jitter); synthetic usage derived from request size
  plus a fixed output size; latency jitter and response text seeded.
- `producer/`: pushes jobs into a Redis list following a phased schedule: 0.5x quota
  for three minutes, 1.0x for three, a 3x burst for four, then four minutes idle. The
  job schedule and prompt sizes come from one seed, so every run sees the same jobs.
- `worker/` (Go): `BRPOP`, `k` concurrent calls through the proxy, SDK-style retry on
  429 (exponential backoff with jitter, never before `retry-after`), graceful shutdown
  on SIGTERM that counts the jobs it had to abandon and lingers for the last scrape.
- `kind/` and `prometheus/`: cluster config, KEDA via Helm, a plain Prometheus scrape
  of the workers, the producer and quotaware; replicas are counted as worker targets
  that are up.
- `plot/`: turns the Prometheus export into three SVG charts and a Markdown table,
  one column per run.

Three runs that differ by design only in the ScaledObject:

- A, baseline: Redis list trigger only (`listLength: 10`).
- B, quotaware: the same trigger capped by the quotaware ceiling (Mode A) at the
  default safety factor 0.85.
- C, quotaware at `safety: "1"`, so the ceiling is the full quota.

Measured per run: replicas over time, 429 per minute, retries, jobs completed per
minute, job latency p50 and p95, time to drain the burst, total pod-seconds, idle
pod-seconds (seconds in backoff divided by the calls per pod), jobs abandoned on
shutdown and jobs unaccounted for.

Hypothesis, stated before running: under quota both runs look the same, and quotaware
must not hurt there; at and above quota B shows fewer 429s, fewer pod-seconds and a
lower p95 at the same throughput, because throughput is capped by the quota either
way. If B does not win, that is still the write-up, and a bug hunt.

## Repository layout

    quotaware/
      cmd/quotaware/          main: proxy | all | simulate
      internal/headroom/      the algorithm as pure functions; table-driven tests
      internal/aimd/          congestion-control fallback, pure
      internal/state/         bucket state, EWMA, staleness, limits; in-memory backend
      internal/headers/       openai.go, anthropic.go, testdata/, *_test.go
      internal/usage/         usage extraction, body capture, credential hashing
      internal/proxy/         reverse proxy, SSE passthrough, the /v1/observe endpoint
      internal/scaler/        gRPC ExternalScaler; proto vendored from KEDA
      internal/queue/         queue depth readers for single-trigger mode
      internal/metrics/       Prometheus registry
      collector/              Go RoundTripper and Python httpx hooks posting to /v1/observe
      deploy/                 Helm chart (Deployment, Service, ScaledObject examples), grafana/
      bench/                  fakeprovider/, producer/, worker/, kind/, prometheus/, plot/, results/
      docs/                   SPEC.md, design.md, adr/, benchmark.md, release.md
      .github/workflows/      race tests, lint, tidy, chart checks, compose e2e and the Python tests on PR; release on tag

Dependency direction, inward only: `headroom` and `aimd` are the domain and import
nothing from the repository; `state` is the application layer and depends on the
domain, as do `headers` and `usage`; `proxy`, `scaler`, `queue` and `metrics` are
adapters and depend on the application and the domain, never on each other. `cmd/quotaware` wires them together.

Tooling: Go 1.26 (pinned in `go.mod`), `google.golang.org/grpc`,
`prometheus/client_golang`, standard library `httputil.ReverseProxy`, `golangci-lint`,
`goreleaser` for the v0.1.0 release and a `ghcr.io` image. No web framework.

## Milestones, one pull request each

1. Headers and algorithm. Parsers for OpenAI and Anthropic with fixture tests; the
   headroom functions with table tests; `quotaware simulate` that replays a JSONL of
   observations and prints the ceiling over time. Runnable with no cluster.
2. Proxy and metrics. Proxy in front of `fakeprovider`, Prometheus endpoint, Grafana
   dashboard. Runnable with Docker Compose.
3. External scaler. gRPC service, Helm chart, ScaledObject examples for Mode A and
   Mode B, kind demo script.
4. Benchmark. Producer, worker, phased schedule, Prometheus export, plots, first
   committed results.
5. Polish and release. README with the charts, `docs/design.md`, AIMD mode,
   Python and Go collector hooks, v0.1.0.

## Architecture decisions

Recorded in `docs/adr`: 0002 the minimum of steady and instantaneous rate, 0003 the
drain guard, 0004 the proxy first, 0005 the cap through `scalingModifiers` (which also
covers reporting the replica count with `targetSize: 1`), 0006 the AIMD fallback and
guard, 0007 the observe protocol, 0008 and 0009 the network defaults, 0010 one quota
across credentials, 0011 the errors discarded on purpose. Counting uncached input
and cache writes for Anthropic is described above and in the README rather than in
a record of its own.

## Risks and open questions

- Anthropic rounds `remaining` to the nearest thousand; small limits need smoothing.
- Headers reflect organization-wide usage; `share` is a crude answer, a real one needs
  coordination between quotaware instances (phase 2, Redis).
- Streaming usage extraction is provider-specific; headers alone are enough for the
  ceiling, usage only refines tokens per call.
- HPA stabilization and KEDA cooldown blunt fast reactions; measure it.
- `scalingModifiers` function set depends on the installed KEDA; Mode B is the fallback.
- Gemini and Azure header semantics are unverified; AIMD covers them until then.
- Header semantics can change. Fixture tests plus the unknown-header-to-AIMD rule keep
  that from breaking scaling.
- OpenAI admits a call against its token limit on prompt plus `max_tokens`, not on
  usage; tokens per call from usage under-states that charge for clients with a large
  `max_tokens`.
- Anthropic's input and output token windows are tracked but the formula bounds by the
  combined `tokens-*` window over input plus output per call; a split bound needs
  separate per-call averages for input and output.
- The instant term models a fixed window while both providers refill continuously; a
  token-bucket model would read the refill rate and other consumers from successive
  `remaining` values.
- Usage capture buffers up to `-max-capture-bytes` per open call and decodes only
  identity and gzip; parsing SSE incrementally would bound memory and keep the usage of
  long streams.

## References

- Anthropic rate limits: https://platform.claude.com/docs/en/api/rate-limits
- OpenAI rate limits: https://developers.openai.com/api/docs/guides/rate-limits
- KEDA external scalers: https://keda.sh/docs/latest/concepts/external-scalers/
- KEDA scaling modifiers: https://keda.sh/docs/latest/concepts/scaling-deployments/
- KEDA external trigger: https://keda.sh/docs/latest/scalers/external/

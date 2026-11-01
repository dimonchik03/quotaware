# Benchmark

Two runs on a laptop `kind` cluster, identical except for the ScaledObject, zero
provider tokens spent. Both scale the same worker deployment against the same fake
provider with the same job schedule and seed.

## Setup

- Provider: `bench/fakeprovider`, 600 requests and 2,000,000 tokens per minute for
  the model, fixed window, 2 s latency with 200 ms jitter, OpenAI-style headers,
  429 with `retry-after` when the window is spent.
- Workers: `bench/worker`, two calls in flight per pod, SDK-style retry (exponential
  backoff from one second, capped at a minute, `retry-after` honoured, 25 % jitter),
  six attempts per job, graceful shutdown on SIGTERM.
- Producer: `bench/producer`, one process pushing jobs into a Redis list at a
  multiple of the provider quota (10 jobs per second): `half:3m:0.5`, `full:3m:1`,
  `burst:4m:3`, `idle:4m:0`. Prompt sizes vary within 200 ± 50 tokens from a fixed
  seed, so both runs see the same jobs.
- Run A, the baseline: KEDA Redis list trigger, ten jobs per replica, 1 to 30 pods.
- Run B: the same trigger capped by the quotaware ceiling through `scalingModifiers`
  (`bench/manifests/scaledobject-b.yaml`).
- Prometheus scrapes the workers, the producer and quotaware every five seconds;
  `bench/bench.sh` exports the series and `bench/cmd/plot` draws the charts and the
  table. Everything is committed under `bench/results/<date>/`.

## Hypothesis, written before the first run

Under quota both runs look the same, and quotaware must not hurt there. At and above
quota, run B shows fewer 429s, fewer pod-seconds and a lower p95 at the same
throughput, because throughput is capped by the quota either way. If B does not win,
that is still the write-up, and a bug hunt.

## What is measured per run

Replicas over time, 429 per minute, retries, jobs completed per minute, job latency
p50 and p95 from enqueue to completion, time to drain the burst (from the end of the
burst phase until the queue is empty), total pod-seconds (the area under the replica
curve), idle pod-seconds (seconds spent in backoff divided by the calls per pod),
and the time from the first 429 to the first scale-down.

## Running it

    make bench          # full schedule, about 40 minutes for both runs
    make bench-smoke    # two-minute schedule to check the pipeline

The cluster is the one `make demo` uses, with its own kubeconfig under `bench/kind/`;
`make demo-down` deletes it.

## Results, 2026-11-01

`bench/results/2026-11-01/`, produced by `make bench` on a MacBook with 16 GB of
memory, kind v0.33, KEDA 2.21.0, Go 1.26.5. Run A took 1105 s, run B 1356 s,
including the drain after the schedule.

![replicas](../bench/results/2026-11-01/replicas.svg)

![429 per minute](../bench/results/2026-11-01/rate_limits.svg)

![queue length](../bench/results/2026-11-01/queue.svg)

| Metric | A (queue trigger) | B (queue trigger + quotaware) |
|---|---|---|
| Jobs produced | 9900 | 9900 |
| Jobs completed | 9848 | 9875 |
| Jobs failed | 0 | 0 |
| 429 responses | 1140 | 0 |
| Retries | 1140 | 0 |
| Peak 429 per minute | 111 | 0 |
| Job latency p50 | 157.9 s | 263.4 s |
| Job latency p95 | 454.8 s | 689.3 s |
| Peak replicas | 30 | 8 |
| Pod-seconds | 23700 | 9945 |
| Idle pod-seconds (in backoff) | 13681 | 0 |
| Time to drain the burst | 465 s | 720 s |
| Time to react after the first 429 | n/a (no ceiling) | no 429 observed |

Reading the table: totals are the per-pod maxima of each counter over the run, so a
pod's last few seconds before termination are not counted, which is why both runs
complete slightly fewer jobs than were produced although none failed. Latencies are
from enqueue to completion across the whole run. Pod-seconds integrate the replica
series; idle pod-seconds are the seconds workers spent sleeping on a 429 divided by
the two calls per pod. The drain time runs from the end of the 3x phase to the first
empty queue. Run B's ceiling settled at 8 because 600 requests per minute, one call
per second per pod and a safety factor of 0.85 give eight pods; the knob is
`safety`. The interpretation of these numbers is left to the reader; the hypothesis
above was written before the first run.

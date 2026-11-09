# Benchmark

## Changing quota against a static cap, 2026-11-06

Nine trials are saved with their raw Prometheus exports and charts under
[2026-11-06-changing](../bench/results/2026-11-06-changing/). They ran on one
GitHub-hosted Ubuntu 24.04 amd64 runner from clean commit
[6e3723b](https://github.com/dimonchik03/quotaware/commit/6e3723bf8bfbcf9d10568354d85e1e3e04aeb8b4).
[CI execution](https://github.com/dimonchik03/quotaware/actions/runs/42232333742).

Three passes ran the arms in the orders A/B/C, C/B/A and B/C/A; each cell shows the
lowest and the highest value. Quotaware uses `safety: "1"`, `maxStepUp: "4"` and the
default `lookAhead` of 30 seconds. The default safety setting is 0.85. All arms use
two concurrent provider calls per worker.

| Metric | Queue only | Static cap of 10 | Quotaware |
|---|---|---|---|
| Jobs completed, out of 3,960 | 3,960 | 3,960 | 3,960 |
| Jobs handed back on shutdown | 12–14 | 0 | 18 |
| HTTP 429 responses | 442 | 88–92 | 24–26 |
| Worker pod-seconds | 13,598–13,680 | 5,738–5,805 | 4,215–4,402 |
| Pod-seconds of scheduled retry backoff | 9,602–9,641 | 1,676–1,678 | 416–424 |
| Mean job latency | 113.2–114.5 s | 130.3–130.7 s | 156.0–161.6 s |
| Job latency p50 | 161.2–162.8 s | 196.2–196.4 s | 221.5–226.6 s |
| Job latency p95 | 199.8–200.7 s | 213.2–213.4 s | 238.2–243.4 s |
| Time from end of load phase to empty queue | 195 s | 210 s | 240 s |

Compared pass by pass with the static cap, Quotaware completes every job with
23–27% fewer worker pod-seconds, about 72% fewer 429s and about 75% less scheduled
backoff. Its mean job latency is 19–24% higher, p50 13–15%, p95 12–14%, and its
queue empties 30 seconds later. Compared with the queue trigger alone, it uses 68–69%
fewer pod-seconds and receives about 94% fewer 429s, with a p95 19–21% higher. The static
cap holds the advantage it was designed with: its maximum equals what this
provider's quota feeds, which an operator has to know in advance and keep current.

A later run on
[main at 0da8771](https://github.com/dimonchik03/quotaware/commit/0da87711096c3a6d8b458e2607a47e9749dc1885)
([CI execution](https://github.com/dimonchik03/quotaware/actions/runs/42584621163)),
after the guard changes of ADR 0014, repeated the shape: per pass Quotaware used
23–28% fewer pod-seconds and received 72–76% fewer 429s than the static cap, with a
mean latency 18–29% and a p95 10–17% higher and a queue that emptied 15–30 seconds
later.

### Where Quotaware loses time

The queued-job gap to the static cap grows at three moments, each adding 60 to 130
jobs in every pass:

- Start. The ceiling rises 1, 5, 9, 10 in 15-second steps of at most four workers,
  while the static arm scales to ten within 30 seconds.
- The first reduced minute. Ten workers spend its 180 requests in under 20 seconds,
  and its reset is then still more than 30 seconds away, beyond the look-ahead, so
  the instantaneous rate is zero and the ceiling falls to one worker. The look-ahead raises it as the
  reset approaches, and three workers serve the following minutes. The static arm
  keeps ten workers waiting in backoff and fires all of them at each reset.
- Recovery to 600. The look-ahead assumes the next window has the limit of the
  current one, so the ceiling stays at three until the first response of the new
  window reports 600; ten workers are serving about 30 seconds later.

The first two depend on settings this experiment did not vary: a larger `maxStepUp`
climbs faster at the start and a longer `lookAhead` raises the ceiling earlier before
a reset, both at some cost in 429s and pod-seconds. The third is inherent to reading
the quota from response headers: no header announces a raise before it happens.

### Method

`make bench-changing` compares three KEDA policies on the same Redis queue. A reads
queue depth with a maximum of 30 workers. B reads the same queue with a static
maximum of 10. C allows up to 30 workers and caps the queue trigger with Quotaware
through `scalingModifiers`. Ten workers match the nominal 600 RPM at two concurrent
calls per worker and about two seconds per call.

The producer enqueues 11 jobs per second, 10% above the nominal quota, for six
minutes, then nothing for one minute. The fake provider starts its window clock on
the first request for the model. It admits 600 requests in each of the first two
minutes, 180 in each of the next four and 600 in every minute after that. Its token
limit stays at two million per minute. The provider and scaler restart between
arms, and each arm uses fresh workers, the same seed and the same HPA settings.
`BENCH_RPM_WINDOWS`, `BENCH_PHASES` and `BENCH_PASSES` override the schedule. On a
Mac, `caffeinate -i make bench-changing` keeps the host awake for the experiment.
The experiment changes the provider's advertised RPM directly. The kind demo in the
README exercises a separate workload spending a shared quota.

Prometheus scrapes every five seconds. The replica, queue, ceiling and observed
provider RPM series are exported on a 15-second grid. Pod-seconds integrate live
worker metrics endpoints, including their 15-second shutdown linger. Job latency
runs from enqueue to completion and includes queue wait. The mean comes from the
histogram's sum and count; p50 and p95 are interpolated in five-second buckets up to
twenty minutes.

The worker pops a job from Redis before processing it. It finishes a provider call
already in progress, but SIGTERM interrupts a retry wait; the worker then pushes the
job back to the end of the list that workers read from, with its original enqueue
time, and counts it as handed back. A job it cannot hand back counts as abandoned.
The retry and backoff counters are charged when a wait is scheduled.

The CI workflow runs on pushes to main that change application, chart or benchmark
sources, excluding saved measurements, and can be dispatched manually on any branch.

### Scope of the conclusion

The schedule contains one four-minute reduction followed by recovery. It tests an
advertised request quota, while token quota, model and latency distribution stay
fixed. The fixed-window fake provider charges admitted calls only, and every arm
shares one quota with no other consumer. Three passes on one runner agree closely,
but other schedules, continuously refilled limits, shared quotas and real providers
remain unmeasured.

## Earlier changing-quota trial, 2026-11-05

This six-trial experiment is saved with its raw Prometheus exports and charts
under [2026-11-05-changing](../bench/results/2026-11-05-changing/).
It ran on one GitHub-hosted Ubuntu 24.04 amd64 runner from clean commit
[4b602e1](https://github.com/dimonchik03/quotaware/commit/4b602e16c46a0eefed44002e16dbc223820ca2a1).
[CI execution](https://github.com/dimonchik03/quotaware/actions/runs/41928303907).

Each cell shows pass 1 / pass 2. The second pass reverses A/B/C to C/B/A.
Quotaware uses `safety: "1"` and `maxStepUp: "4"`. The default safety setting is
0.85. All arms use two concurrent provider calls per worker.

| Metric | Queue only | Static cap of 10 | Quotaware |
|---|---|---|---|
| Jobs completed, out of 1,800 | 1,800 / 1,800 | 1,800 / 1,800 | 1,784 / 1,782 |
| Jobs abandoned on shutdown | 0 / 0 | 0 / 0 | 16 / 18 |
| HTTP 429 responses | 158 / 150 | 24 / 24 | 18 / 20 |
| Worker pod-seconds | 4,875 / 5,280 | 2,385 / 2,310 | 2,032.5 / 2,295 |
| Job latency p95 | 92.1 s / 92.1 s | 136.5 s / 136.5 s | 197.9 s / 206.9 s |
| Time from end of load phase to empty queue | 75 s / 75 s | 105 s / 105 s | 150 s / 165 s |

The adaptive ceiling follows the reduction in available quota and keeps fewer
workers alive through it. Across these two trials it uses about 8% fewer worker
pod-seconds than the static cap, while p95 rises into the next histogram bucket.
Its worker also abandoned jobs when a scale-down interrupted retry backoff. Completed
jobs alone enter the latency histogram, so the resource totals must be read with
the completion counts. The static ten-worker cap completes this workload sooner
and preserves every job.

The worker used a destructive Redis pop; it hands jobs back since
[2603524](https://github.com/dimonchik03/quotaware/commit/2603524e76090a333af17789ab0e32a7d128ae37)
(2026-11-06). A production worker needs queue leases or requeue-on-shutdown
behaviour for work between provider calls. Quotaware's drain
floor sees open HTTP calls. It cannot preserve a job sleeping between retries,
and KEDA can choose any worker to terminate.

The experiment demonstrates adaptation and its cost under a brief quota reduction.
The static cap performs well for this schedule. Performance for other quota schedules and real providers remains unmeasured.

### What the 2026-11-05 trials exposed

The quotaware arm's ceiling series reads 9, 9, 9, 9, 1, 1, 1, 1, 5, 9, 10 in pass 1,
while 180 requests per minute feed three pods. Once the 180 requests were spent,
every later call in that minute got a 429 that reported nothing remaining. The AIMD
guard counted those 429s as congestion and held the ceiling for one window, about
30 seconds past the reset, so the climb back to ten pods began half a window late.
A replay of the dip through the scaler reproduces the series.
[ADR 0013](adr/0013-look-past-a-reset-and-guard-unexplained-429s.md) changes two
rules: the guard on a header ceiling ignores 429s whose own headers show a spent
window, and the instantaneous rate counts the refill of a reset that is less than
`lookAhead` away. The bench worker also hands a job back to the queue when shutdown
interrupts its retry wait, so a scale-down no longer loses work. The trials above
predate these changes.

### Method, 2026-11-05

At commit 4b602e1, `make bench-changing` compares three KEDA policies on the same Redis queue. A reads
queue depth with a maximum of 30 workers. B reads the same queue with a static
maximum of 10. C allows up to 30 workers and applies Quotaware with `safety: "1"`
and `maxStepUp: "4"`. Ten workers match the nominal 600 RPM at two concurrent
calls per worker and about two seconds per call.

The producer enqueues 15 jobs per second for two minutes, then stops adding work.
The fake provider starts its window clock on the first request for the model. It
admits 600 requests in the first minute, 180 in the second, and 600 in every
subsequent minute. Its token limit stays at two million per minute. The provider
and scaler restart between arms, and each arm uses fresh workers, the same seed
and the same HPA settings. The second pass reverses the order from A/B/C to C/B/A. On a Mac,
`caffeinate -i make bench-changing` keeps the host awake for the experiment.
The experiment changes the provider's advertised RPM directly. The kind demo in
the README exercises a separate workload spending a shared quota.

Prometheus scrapes every five seconds. The replica, queue, ceiling and observed
provider RPM series are exported on a 15-second grid. Pod-seconds integrate live
worker metrics endpoints, including their 15-second shutdown linger. Job latency
includes queue wait and is interpolated from a histogram. With two repetitions per
arm, report both observations and avoid extrapolating to production latency or cost.

The worker pops jobs from Redis before processing them. It finishes a provider call
already in progress, but SIGTERM interrupts a retry wait. A popped job can then
be abandoned; the worker counts it separately. The retry and backoff counters
are charged when a wait is scheduled. Only completed jobs enter the latency
histogram. An open-call drain floor cannot protect a job that is between attempts.


### Scope, 2026-11-05

This schedule contains a one-minute reduction followed by recovery. It tests an
advertised request quota, while token quota, model and latency distribution stay
fixed. Its fixed-window fake provider charges admitted calls only. Repeat counts
are small, and the latency histogram is coarse. A longer reduction, continuously
refilled limits or reliable job requeueing would be separate experiments.

## Historical fixed-quota experiment, 2026-11-02

Three runs on a laptop `kind` cluster, zero provider tokens spent. They share the
workers, the producer schedule and seed and the HPA behaviour; only the ScaledObject
differs by design. The fake provider and Prometheus ran on across the runs, so each run
met the minute window at a different phase, and each variant ran once, in the order
A, B, C.

### Setup

- Provider: `bench/fakeprovider`, 600 requests and 2,000,000 tokens per minute for
  the model in one fixed minute window that every key shares, 2 s latency with 200 ms
  jitter, OpenAI-style headers, 429 with `retry-after` up to the reset when the window
  is spent; a rejected call does not count against the window and acceleration
  limits are off. Only the request limit binds: about 365 tokens per call.
- Workers: `bench/worker`, two calls in flight per pod, SDK-style retry (exponential
  backoff from one second, capped at a minute, then ±25 % jitter, never before
  `retry-after`), six attempts per job, graceful shutdown on SIGTERM that counts the
  jobs it had to abandon and lingers 15 s after SIGTERM so the last counters are scraped.
- Producer: `bench/producer`, one process pushing jobs into a Redis list at a
  multiple of the provider quota (10 jobs per second): `half:3m:0.5`, `full:3m:1`,
  `burst:4m:3`, `idle:4m:0`. Prompt sizes vary within 200 ± 50 tokens from a fixed
  seed, so every run sees the same jobs.
- Run A, the baseline: KEDA Redis list trigger, ten jobs per replica, 1 to 30 pods.
- Run B: the same trigger capped by the quotaware ceiling through `scalingModifiers`
  at the default `safety` of 0.85, with `concurrency: "2"` to match the worker and
  `maxStepUp: "4"`, twice the default, so the ceiling may rise by four pods per
  15 s step (`bench/manifests/scaledobject-b.yaml`).
- Run C: the same cap with `safety: "1"` (`bench/manifests/scaledobject-c.yaml`).
  C is exploratory: it was added after the first results showed B losing on latency,
  to measure what the default safety factor costs, and the hypothesis below predates
  it.
- Prometheus scrapes the workers, the producer and quotaware every five seconds;
  `bench/bench.sh` exports the series and `bench/cmd/plot` draws the charts and the
  table. Everything, including the commit the images were built from, is committed
  under `bench/results/<date>/`.

### Hypothesis, written before the first run

Under quota both runs look the same, and quotaware must not hurt there. At and above
quota, run B shows fewer 429s, fewer pod-seconds and a lower p95 at the same
throughput, because throughput is capped by the quota either way. If B does not win,
that is still the write-up, and a bug hunt.

### What is measured per run

Replicas over time, 429 per minute, retries, jobs completed per minute, job latency
p50 and p95 from enqueue to completion, time to drain the burst (from the end of the
burst phase until the queue is empty), total pod-seconds (the area under the replica
curve), idle pod-seconds (seconds spent in backoff divided by the calls per pod),
jobs abandoned on shutdown and jobs unaccounted for (produced minus completed,
failed and abandoned).

### Running it

    make bench          # full schedule, about an hour for the three runs
    make bench-smoke    # short schedule, about three minutes per run, to check the pipeline

The cluster is the one `make demo` uses, with its own kubeconfig under `bench/kind/`;
`make demo-down` deletes it.

### Results, 2026-11-02

`bench/results/2026-11-02/`, built from commit `bfffc1c` with no changes to tracked files, produced
by `make bench` on a MacBook with 16 GB of memory, kind v0.33, KEDA 2.21.0, Go 1.26.5.
Run A took 1091 s, run B 1352 s, run C 1128 s, including the drain after the schedule.
Each variant ran once. HPA behaviour was the same in every run: no stabilization
window, scale up by up to 30 pods and down by up to 100 % per 10 s. The table and the
retries chart were re-rendered from these series at `2022e38`, which plots retries
instead of 429s per minute; no series changed.

Later commits change the scaler in ways these runs exercise only in part: ADR 0009 and
0010 change how unanswered and several credentials combine, which one pinned
credential does not exercise beyond the first second; the step interval now counts
from the last change of the bound and applies to the combined value; quotas are dated
by their headers. A run on the current commit may step up differently.

![replicas](../bench/results/2026-11-02/replicas.svg)

![retries per minute, each after a 429](../bench/results/2026-11-02/retries.svg)

![queue length](../bench/results/2026-11-02/queue.svg)

| Metric | A (queue trigger) | B (queue trigger + quotaware, safety 0.85) | C (queue trigger + quotaware, safety 1) |
|---|---|---|---|
| Jobs produced | 9900 | 9900 | 9900 |
| Jobs completed | 9900 | 9900 | 9900 |
| Jobs failed | 0 | 0 | 0 |
| Jobs abandoned on shutdown | 0 | 0 | 0 |
| Jobs unaccounted for | 0 | 0 | 0 |
| 429 responses | 720 | 0 | 15 |
| Retries | 720 | 0 | 15 |
| Peak retries per minute | 91 | 0 | 5 |
| Job latency p50 | 128.1 s | 263.6 s | 164.8 s |
| Job latency p95 | 457.7 s | 689.5 s | 463.1 s |
| Peak replicas | 30 | 8 | 10 |
| Pod-seconds | 23580 | 10050 | 10125 |
| Idle pod-seconds (in backoff) | 13300 | 0 | 8 |
| Time to drain the burst | 450 s | 720 s | 495 s |

### Result against the hypothesis

- *Under quota both runs look the same.* Holds in the 0.5x phase. In the 1.0x phase B
  already falls behind: its ceiling of 8 pods completes about 480 jobs per minute
  against 600 produced, so its queue grows from 87 to 417 while A's goes from 93 to
  101. C nearly keeps up: its queue grows from 87 to 124.
- *Fewer 429s.* Holds. A 720, B 0, C 15.
- *Fewer pod-seconds.* Holds. A 23580, B 10050, C 10125. A spent 13300 pod-seconds,
  more than half of its total, in backoff.
- *Lower p95 at the same throughput.* Does not hold. B's p95 is 689.5 s against A's
  457.7 s and the burst drains in 720 s instead of 450 s, because the default safety
  factor leaves 20 % of the quota unused: floor(600 / 60 × 0.85) is 8 pods, which
  sustain 480 calls per minute. C reaches the full quota with 10 pods and ties A on
  p95 (463.1 s against 457.7 s, inside the same histogram bucket, see below), but
  its p50 sits one bucket higher (164.8 s against 128.1 s, about 29 % by
  interpolation) and it takes 10 % longer to drain the burst (495 s against 450 s).

The trade-off is the safety factor. At 0.85 quotaware buys zero 429s and less than
half the pod-seconds with a p95 one bucket and a p50 two buckets higher (about 50 %
and twice by interpolation) under sustained overload. At 1 it ties the baseline's
p95, its p50 is one bucket higher and the drain 10 % longer, at 43 % of the
pod-seconds and 2 % of the 429s. Throughput is capped by the quota either way, so
the queue trigger alone does not lengthen the queue here; what it wastes is pods
sleeping in backoff and the retries they send. C's 15
429s are the fixed window's edge: ten pods at about one call per second each sit
exactly on 600 per minute, and latency jitter occasionally pushes a window over.

What this benchmark does not show: the quota, the latency and the set of consumers
are constant, so a static `maxReplicaCount` of 10 on the queue trigger would likely
match run C. The ceiling is meant for headroom that moves, for instance when another
credential spends the shared quota; the kind demo in the README shows that case and
a later benchmark run should measure it. B's ceiling sat at 8 from its first 30
seconds to the end, so B behaved as a static cap of 8.

The fixed window also flatters the baseline. When A jumps to 30 pods it spends the
rest of one window and all of the next back to back, up to 1026 completions in one
minute against a quota of 600, and then front-loads every later window, because each
429 carries a `retry-after` up to the reset and all of A's call slots wake together.
A's queue runs about 180 to 480 jobs shorter than C's through the burst, some 40 of
them jobs A holds in backoff, about the size of C's p50 and drain gaps. The fake provider does not charge a rejected call against its window,
while OpenAI counts unsuccessful requests against the per-minute limit. A provider
that refills continuously, or one with acceleration limits on, would remove A's head
start and raise its 429 count; neither was run.

Reading the table: totals are the per-pod maxima of each counter over the run, and
the worker lingers 15 s after SIGTERM so the final values are scraped, which is
why every produced job is accounted for. Latencies are from enqueue to completion
across the whole run, interpolated from a histogram whose buckets grow by a factor
of 1.5: p95 values between 328 s and 493 s share one bucket, so differences of a few
percent inside it are not measured, while the three p50 values fall in three
different buckets. Between buckets only the order is measured; the percentages above
come from the interpolation. Pod-seconds integrate the replica series, which counts
worker pods whose metrics endpoint is up, including the 15 s linger, and stops at
the last sample, when A still had 23 pods up while B and C were back to one, so A's
scale-down tail, about 1 % of its total, is not counted; idle pod-seconds are
the seconds workers spent sleeping on a 429 divided by the two calls per pod. The
drain time runs from the end of the 3x phase to the first empty queue, at the 15 s
resolution of the export, so 450 s and 495 s are three samples apart. The 429 row
counts each pod's 429 total; the per-minute row and chart use retries, because the
429 series of a pod only appears at its first 429 and a rate misses that step. In
these runs every retry followed a 429.

### Superseded results, 2026-11-01

`bench/results/2026-11-01/` is kept for the record and should not be quoted. The
worker applied its jitter after `retry-after` and so retried up to 25 % early, which
inflated run A's 429s to 1140 against the 720 measured here; the export missed run
B's 429 series; the images came from an uncommitted tree; and 77 jobs were lost to
the last unscraped seconds of terminated pods. The direction of every row was the
same as above.

| Metric | A (queue only) | B (static maximum of 10) | C (quotaware, safety 1) |
|---|---|---|---|
| Jobs produced | 3960 | 3960 | 3960 |
| Jobs completed | 3960 | 3960 | 3960 |
| Jobs failed | 0 | 0 | 0 |
| Jobs abandoned on shutdown | 0 | 0 | 0 |
| Jobs handed back on shutdown | 12 | 0 | 18 |
| Jobs unaccounted for | 0 | 0 | 0 |
| 429 responses | 442 | 88 | 24 |
| Retries scheduled | 442 | 88 | 24 |
| Peak scheduled retries per minute | 71 | 22 | 22 |
| Job latency mean | 114.5 s | 130.5 s | 161.6 s |
| Job latency p50 | 162.8 s | 196.3 s | 226.6 s |
| Job latency p95 | 200.7 s | 213.2 s | 243.4 s |
| Peak replicas | 30 | 10 | 10 |
| Pod-seconds | 13680 | 5738 | 4402 |
| Scheduled backoff (pod-seconds) | 9636 | 1676 | 416 |
| Time from last load phase to empty queue | 195 s | 210 s | 240 s |

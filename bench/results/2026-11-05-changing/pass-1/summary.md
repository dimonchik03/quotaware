| Metric | A (queue only) | B (static maximum of 10) | C (quotaware, safety 1) |
|---|---|---|---|
| Jobs produced | 1800 | 1800 | 1800 |
| Jobs completed | 1800 | 1800 | 1784 |
| Jobs failed | 0 | 0 | 0 |
| Jobs abandoned on shutdown | 0 | 0 | 16 |
| Jobs unaccounted for | 0 | 0 | 0 |
| 429 responses | 158 | 24 | 18 |
| Retries scheduled | 158 | 24 | 18 |
| Peak scheduled retries per minute | 74 | 21 | 20 |
| Job latency p50 | 48.9 s | 75.4 s | 114.0 s |
| Job latency p95 | 92.1 s | 136.5 s | 197.9 s |
| Peak replicas | 30 | 10 | 10 |
| Pod-seconds | 4875 | 2385 | 2032 |
| Scheduled backoff (pod-seconds) | 2824 | 417 | 355 |
| Time from last load phase to empty queue | 75 s | 105 s | 150 s |

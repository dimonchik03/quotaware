| Metric | A (queue only) | B (static maximum of 10) | C (quotaware, safety 1) |
|---|---|---|---|
| Jobs produced | 3960 | 3960 | 3960 |
| Jobs completed | 3960 | 3960 | 3960 |
| Jobs failed | 0 | 0 | 0 |
| Jobs abandoned on shutdown | 0 | 0 | 0 |
| Jobs handed back on shutdown | 12 | 0 | 18 |
| Jobs unaccounted for | 0 | 0 | 0 |
| 429 responses | 442 | 92 | 26 |
| Retries scheduled | 442 | 92 | 26 |
| Peak scheduled retries per minute | 82 | 23 | 21 |
| Job latency mean | 114.2 s | 130.3 s | 156.0 s |
| Job latency p50 | 162.5 s | 196.2 s | 221.5 s |
| Job latency p95 | 200.6 s | 213.4 s | 238.2 s |
| Peak replicas | 30 | 10 | 10 |
| Pod-seconds | 13665 | 5775 | 4215 |
| Scheduled backoff (pod-seconds) | 9641 | 1678 | 424 |
| Time from last load phase to empty queue | 195 s | 210 s | 240 s |

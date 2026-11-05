| Metric | A (queue only) | B (static maximum of 10) | C (quotaware, safety 1) |
|---|---|---|---|
| Jobs produced | 1800 | 1800 | 1800 |
| Jobs completed | 1800 | 1800 | 1782 |
| Jobs failed | 0 | 0 | 0 |
| Jobs abandoned on shutdown | 0 | 0 | 18 |
| Jobs unaccounted for | 0 | 0 | 0 |
| 429 responses | 150 | 24 | 20 |
| Retries scheduled | 150 | 24 | 20 |
| Peak scheduled retries per minute | 65 | 21 | 22 |
| Job latency p50 | 48.9 s | 75.4 s | 121.1 s |
| Job latency p95 | 92.1 s | 136.5 s | 206.9 s |
| Peak replicas | 30 | 10 | 10 |
| Pod-seconds | 5280 | 2310 | 2295 |
| Scheduled backoff (pod-seconds) | 2856 | 417 | 414 |
| Time from last load phase to empty queue | 75 s | 105 s | 165 s |

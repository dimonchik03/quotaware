| Metric | A (queue only) | B (static maximum of 10) | C (quotaware, safety 1) |
|---|---|---|---|
| Jobs produced | 3960 | 3960 | 3960 |
| Jobs completed | 3960 | 3960 | 3960 |
| Jobs failed | 0 | 0 | 0 |
| Jobs abandoned on shutdown | 0 | 0 | 0 |
| Jobs handed back on shutdown | 14 | 0 | 18 |
| Jobs unaccounted for | 0 | 0 | 0 |
| 429 responses | 442 | 90 | 25 |
| Retries scheduled | 442 | 90 | 25 |
| Peak scheduled retries per minute | 83 | 22 | 22 |
| Job latency mean | 113.2 s | 130.7 s | 156.1 s |
| Job latency p50 | 161.2 s | 196.4 s | 221.5 s |
| Job latency p95 | 199.8 s | 213.4 s | 238.3 s |
| Peak replicas | 30 | 10 | 10 |
| Pod-seconds | 13598 | 5805 | 4260 |
| Scheduled backoff (pod-seconds) | 9602 | 1677 | 423 |
| Time from last load phase to empty queue | 195 s | 210 s | 240 s |

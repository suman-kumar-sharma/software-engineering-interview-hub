# HLD — Global Gaming Leaderboard (2026-10-19)

Original product-company SDE2 system-design exercise. Assume 10 million players, 50,000 score events/s peak and 20,000 read requests/s. Need season top 100, player rank and neighbors. Source of scores is authenticated game servers, not untrusted clients. Target in-region read p99 <150ms and eventual projection freshness <5s in normal operation.

## Requirements, sizing and architecture

Clarify cumulative vs maximum score, season rollover, ties, anti-cheat, late events and exact-vs-eventual rank. Assumptions: 50k events/s at 200B payload ~=10MB/s before replication. Peak is not average. 10M ZSET members require memory benchmarking; raw data size severely underestimates Redis overhead.

~~~text
Game servers -> Auth Ingest API -> durable event log (Kafka)
                                      | key=(season,player)
                                      v
                              Idempotent consumers
                                      |
                              Score database (truth)
                                      |
                              Redis ZSET projection
                                      |
                      Read API -> top100, rank, neighbors
~~~

Kafka key keeps per-player event order. Stable eventId unique in DB prevents duplicate increments after replay. Acknowledge ingest only after durable acceptance. Redis is rebuildable from DB/events, not the sole durable source.

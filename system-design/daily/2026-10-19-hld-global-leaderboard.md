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

## Data model, APIs and projection correctness

~~~sql
CREATE TABLE score_events(
 event_id UUID PRIMARY KEY,
 season_id BIGINT NOT NULL,
 player_id BIGINT NOT NULL,
 delta BIGINT NOT NULL
);
CREATE TABLE player_scores(
 season_id BIGINT NOT NULL,
 player_id BIGINT NOT NULL,
 score BIGINT NOT NULL,
 version BIGINT NOT NULL,
 PRIMARY KEY(season_id,player_id)
);
~~~

POST /v1/score-events (internal server only); GET /v1/seasons/{season}/top?limit=100; GET /v1/seasons/{season}/players/{id}/rank; GET /v1/seasons/{season}/players/{id}/neighbors?radius=5.

Redis ZSET supports ZREVRANGE top scores and ZREVRANK player rank. **Important:** blind ZINCRBY on replay double-counts. Prefer durable DB aggregate + version and atomic conditional Redis projection; rebuilding from authoritative rows is possible. Redis ZSET scores use floating point: integers above 2^53 cannot be represented exactly. Tie policy must be explicit; ZSET ordering is not automatically the business tie rule.

## Scaling trade-offs

A single Redis ZSET can become a hot write key. Sharded top-K works for global top-K, but exact arbitrary rank requires a global index or expensive merge. Choose one write region per season and expose read-replica freshness lag.

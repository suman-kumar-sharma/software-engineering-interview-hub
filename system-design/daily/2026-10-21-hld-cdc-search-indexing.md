# HLD — Change Data Capture Search Indexing Platform
**Date:** 2026-10-21 | **Level:** SDE2 / Senior Backend

## Problem
A multi-tenant SaaS product stores canonical records in PostgreSQL but needs searchable documents in OpenSearch within 10 seconds. Updates/deletes must propagate reliably; a database commit must never be lost just because the indexer is down. Assume 5k changes/s sustained, 20k/s bursts, tenant isolation, and rebuild capability.

Clarify freshness SLA, delete semantics, searchable fields, retention, tenant-size skew, and whether search results may briefly lag the source database.

## Architecture
~~~text
Application -> PostgreSQL (source of truth)
                    |
                WAL / CDC connector
                    |
                 Kafka topic (key = tenantId + entityId)
                    |
             Normalize + schema version
                    |
          Idempotent indexer workers
                    |
                OpenSearch
                    |
           Search API + tenant filter

DLQ + replay tooling; snapshot/backfill job; lag/consistency metrics
~~~

Use CDC when all writers must be captured and application dual-write is unsafe. The database remains canonical; OpenSearch is a rebuildable projection. A transactional outbox is an alternative when the application owns event semantics and can reliably write the outbox row in the same DB transaction.

## Event contract and ordering
Each event carries eventId, tenantId, entityType, entityId, operation, schemaVersion, source commit/LSN or monotonically increasing entity version, and payload or tombstone. Partition by stable entity key to preserve per-entity order. Do not claim global ordering across partitions.

Indexer stores the last applied version or uses external versioning/conditional writes. If version 12 arrives after version 13, discard 12 rather than overwriting newer state. Delete events need tombstones/version markers so delayed older updates cannot resurrect deleted documents.

## Bootstrap without a gap
A naive “take snapshot, then start CDC” can miss or duplicate changes. Use the connector's consistent snapshot/LSN handoff: capture a snapshot at a known source position, then replay changes after that position. Make indexing idempotent so overlap is safe. Rebuild into a new index, validate counts/sample queries, then atomically switch an alias.

## Capacity and partitioning
At 5k events/s and an average 1 KB event, raw ingress is about 5 MB/s before replication and protocol overhead; a 4x burst is 20 MB/s. Benchmark actual document size, index amplification, bulk batch size, shard count and refresh interval. Too many tiny shards waste resources; oversized shards slow recovery.

Monitor consumer lag by both count and age, indexing latency, bulk rejection, retry rate, hot partitions, disk watermark and search latency. Scale consumers only while OpenSearch can absorb writes. Pause or slow consumers on sustained 429/rejection instead of retrying aggressively.

## Reliability and operations
- At-least-once delivery + deterministic document IDs + version checks.
- Retry transient errors with exponential backoff/jitter; DLQ malformed or permanently invalid records.
- Replay is bounded and audited; do not let replay starve live traffic.
- Tenant ID comes from verified identity and is enforced server-side in every search query.
- Keep secrets/PII out of events where possible; encrypt transport and storage.
- Reconcile source counts/version watermarks against index projections.
- Define lag SLO and communicate eventual consistency to clients.

## Trade-offs / follow-ups
**Why not update OpenSearch inside the API request?** Dual-write failure can leave DB and index divergent and couples request latency to search availability.

**What if an event is missing?** Monitor source position/connector health, restart from durable offsets and rebuild from snapshot if the gap cannot be proven safe.

**Exactly once?** Transport can redeliver. Make effects idempotent with stable IDs and entity versions.

**Interview answer:** “PostgreSQL remains the source of truth; CDC gives us a replayable change stream, while version-aware idempotent indexing, a gap-free snapshot handoff and alias-based rebuilds keep the search projection correct and recoverable.”
# HLD Interview Walkthrough — Multi-tenant Audit Event Platform
**Date:** 2026-10-20 | **Level:** SDE2 / Senior Backend

## 1. Clarify the problem
Accept security and business audit events from many tenants, support tenant-scoped search for 90 days, and retain a compliance archive for seven years.

Assumptions: 20,000 events/s sustained, bursts to 60,000/s; p95 acknowledgement target below 150 ms at normal load; accepted events must not be silently lost; at-least-once delivery is acceptable with deduplication; search is eventually consistent.

Ask whether ordering is needed per resource, per tenant or globally. Global ordering is much more expensive than per-resource ordering.

## 2. API and event contract

~~~http
POST /v1/audit-events:batch
Authorization: Bearer <token>
Idempotency-Key: <batch-id>
Content-Type: application/json
~~~

Derive tenant identity from verified identity/membership, not a body field. Each event carries eventId, schemaVersion, occurredAt, actor, action, resourceType, resourceId and minimized payload. Reject oversized batches, unknown schemas and invalid fields before acknowledging.

The response means “durably accepted by the ingestion log,” not “already searchable.” Return an acceptance identifier and document eventual indexing latency.

## 3. Architecture

~~~text
Producers
   |
API Gateway / authentication / quotas / size limits
   |
Ingestion API ---- schema validation and tenant authorization
   |
Replicated Kafka (key = tenant + stable shard, if needed)
   |                         |
   v                         v
Search indexer            Archive consumer
   |                         |
OpenSearch                Object storage
(90-day query)             (7-year retention)
   |
Search API with tenant filter

DLQ + controlled replay; metrics/traces/admin audit -> monitoring/SIEM
~~~

- **Gateway:** identity, request size, quotas and coarse abuse protection.
- **Ingestion service:** schema validation, stable event IDs, authorization and durable broker acknowledgement.
- **Kafka:** absorbs bursts, decouples consumers and retains replayable history for a configured window.
- **OpenSearch:** query-optimized derived index, not the canonical acceptance log.
- **Object storage:** lower-cost long retention and lifecycle management.
- **DLQ/replay tooling:** quarantine poison events with reason and original bytes; replay is an audited operation.

## 4. Capacity and partitioning

If average serialized event is 1 KB, 20k events/s is roughly 20 MB/s before replication, protocol overhead and indexes. A 3x burst is 60 MB/s. Size partitions, brokers, network and storage with benchmark data; do not infer capacity from a single average-size calculation.

Partitioning by tenant preserves order within a tenant partition but a huge tenant can become hot. A stable tenant sub-shard distributes a large tenant but loses global tenant order. If order is required only per resource, use a stable resource key. State the ordering contract first.

Use per-tenant quotas and fair scheduling so one noisy tenant cannot consume the entire ingestion budget. Track hot partitions and skew.

## 5. Durability, idempotency and outbox boundary

- Configure broker replication and producer acknowledgements for the required durability; define behavior when replicas are unavailable.
- Return accepted only after the broker confirms according to that policy.
- Keep event IDs stable across client retries.
- Consumers deduplicate by (tenantId, eventId) or use idempotent upserts.
- Broker acknowledgement does not mean the search index or archive is current.
- If a business database transaction must atomically create the audit event, insert an outbox row in that same transaction and relay it. A separate ingestion API does not automatically need an outbox if its acceptance contract is based on broker acknowledgement.

Exactly-once across database, broker, search index and object store is not a free feature. Prefer at-least-once transport plus idempotent consumers and reconciliation.

## 6. Failure matrix

| Failure | Expected behavior |
| --- | --- |
| Broker unavailable | Backpressure/retryable rejection; never claim accepted |
| Search index unavailable | Kafka retains events; consumer lag rises; replay after recovery |
| Archive writer fails | Bounded retry, alert, DLQ if exhausted, replay and range reconciliation |
| Poison schema | Quarantine original bytes and reason; no silent drop |
| Duplicate delivery | Deduplicate by tenant/event ID |
| One tenant overloads | Tenant quota, fair scheduling, hot-partition alerts |
| Consumer crashes mid-batch | Replay batch; idempotent sink prevents duplicate effects |
| Invalid tenant claim | Reject before ingestion; emit security telemetry |

A lease expiry does not stop a paused worker from waking up later. Use fencing if a stale worker could overwrite newer state.

## 7. Security, privacy and retention

Encrypt transport and storage, minimize payload PII, use separate producer/search/archive/operator roles, audit privileged reads, and scope every query by tenant. Tenant scope must also exist in cache keys, archive policies and replay tools. Object retention locks and legal holds should be designed alongside key rotation, deletion obligations and incident response; retention does not replace access control.

## 8. Observability and SLOs

Track accepted/rejected rate, broker publish latency/errors, lag by partition and tenant, event age, dedup count, index freshness, archive lag, DLQ depth, replay throughput and authorization denials. Alert on growing lag and oldest-event age, not only process liveness. Avoid logging sensitive payloads.

## 9. Deployment and testing

1. Load-test realistic event sizes and skewed tenant distributions.
2. Kill consumers between DB write and offset commit; verify idempotency.
3. Pause a worker beyond lease expiry; verify fencing.
4. Disable search and archive independently; verify durable ingestion and lag alerts.
5. Test tenant A cannot query tenant B through search, cache, replay or admin paths.
6. Exercise schema evolution and poison-event replay.
7. Reconcile counts/checksums between accepted log ranges and archive manifests.

## 10. Trade-offs to explain aloud

Kafka provides replay and throughput but adds operational complexity. OpenSearch improves query latency but duplicates data and needs lag management. Object storage is economical but not interactive. Strong global ordering reduces parallelism. The pragmatic contract is durable acceptance, at-least-once delivery, idempotent processing, tenant isolation and measurable reconciliation.

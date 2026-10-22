# HLD: Reliable Multi-Tenant Webhook Delivery
**Date:** 22 October 2026 | **SDE2 / Senior Backend**

## Requirements and guarantee
Tenants register HTTPS endpoints and event subscriptions. Persist delivery intent, send signed payloads, record attempts, retry transient failures, allow authorized replay, and isolate tenants. Promise at-least-once attempts, not exactly-once HTTP effects: the receiver may commit while its response is lost.

## Sizing example
10M events/day × 3 subscribed endpoints = 30M jobs/day, about 347/sec average. At a 5x peak, about 1,735/sec before retries. Recalculate from actual latency, payload size, retry rate and retention.

## Architecture
~~~text
Business transaction: domain row + outbox
 -> relay / CDC -> durable stream
 -> idempotent subscription fan-out
 -> durable delivery jobs
 -> tenant/endpoint scheduler
 -> bounded HTTPS workers
 -> attempt history, retry schedule, DLQ, metrics
~~~

Outbox closes the DB/broker dual-write gap. Durable jobs decouple business latency from customer endpoints. Per-tenant scheduling prevents one slow endpoint starving others.

## Data and flow
Store outbox events, subscriptions (tenant, endpoint, event type, secret version, quota), delivery state (status, next attempt, count) and attempt history (status code, error, latency, request ID). Unique tenant + endpoint + event ID prevents duplicate jobs. Index due work by status and next attempt; validate with real query plans. Keep secrets in a secret manager.

1. Commit business row and outbox row in one DB transaction.
2. Relay publishes; after broker acknowledgement it marks the row published.
3. Fan-out idempotently creates delivery jobs.
4. Scheduler claims due work with a short lease; never hold DB locks during HTTP.
5. Worker signs canonical body with HMAC, timestamp and stable event ID.
6. Send HTTPS with strict timeouts; record the attempt.
7. Retry transient failures with exponential backoff + jitter. Exhausted/permanent failures go to DLQ; replay is authorized and audited.

## Failure handling
- Relay crash after publish can duplicate; consumers deduplicate.
- Timeout is ambiguous; reuse event ID and require receiver idempotency.
- For per-aggregate order, partition by aggregate ID and stop later events overtaking retries, or document out-of-order behavior.
- SSRF defense: HTTPS, block prohibited private/loopback/link-local targets, revalidate DNS at connection time, restrict redirects and enforce egress rules.
- Apply tenant quotas, fair scheduling, endpoint-level circuit breakers and bounded global concurrency.
- Multi-region active-active may duplicate. Define ownership, fencing, RPO/RTO and replay semantics.

## Metrics and trade-offs
Track oldest outbox age, backlog/queue age, retry amplification, per-tenant success and timeout rate, p95/p99 latency, DLQ count, worker saturation and DB pool use. Polling outbox is simple but adds DB load; CDC reduces polling but adds connector operations. A queue absorbs bursts; a DB scheduler simplifies due-time queries. Define source of truth in a hybrid.

**Spoken answer:** “I use an outbox, durable per-endpoint jobs, bounded workers and signed HTTPS. I assume at-least-once because timeouts are ambiguous. Stable event IDs, receiver idempotency, retry/DLQ handling, tenant fairness and SSRF defenses make it reliable.”

References: https://www.postgresql.org/about/news/ · https://aws.amazon.com/about-aws/whats-new/2026/10/aws-Lambda-supports-oauth-kafka-esm/

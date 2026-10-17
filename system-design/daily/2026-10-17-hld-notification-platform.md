# HLD — Multi-Channel Notification Platform (2026-10-17)
**Audience:** Product-company Senior Backend; 45-minute original interview exercise.

## 1. Requirements and estimates
Build email, SMS and push delivery for multiple tenants. Support transactional templates, user opt-outs/quiet hours, request idempotency, delivery status, retries, tenant quotas and audit. Assume 1 million logical notifications/minute peak and two channels on average: ~33,333 provider attempts/sec before retries. These are illustrative numbers, not observed traffic. API p99 for durable acceptance <200ms; provider acceptance and device receipt have separate SLOs. Do not promise exactly-once delivery to arbitrary providers.

## 2. APIs, entities and invariants
~~~http
POST /v1/notifications
Idempotency-Key: tenant-scoped-key
{"tenantId":"t1","recipientId":"u42","templateId":"payment-ok",
 "channels":["EMAIL","PUSH"],"variables":{"amount":"100"}}
202 {"notificationId":"n77","status":"ACCEPTED"}

GET /v1/notifications/n77
PUT /v1/users/u42/preferences
~~~
Schema: notification(id,tenant,recipient,template,status,created_at,idempotency_key); delivery(id,notification,channel,status,attempt,next_retry_at,provider_id); preference(tenant,user,channel,allowed,quiet_hours,version); outbox(id,aggregate_id,payload,published_at). Unique (tenant,idempotency_key). Invariants: 202 means committed; tenant isolation on every operation; never dispatch opted-out channels; stable delivery identity on retry.

## 3. Component design
~~~text
Client -> Gateway/Auth -> Notification API -> PostgreSQL + outbox
                                            |
                                       Outbox relay
                                            v
                                       Event stream
                                            |
                                 Preferences/templates
                                /         |         \
                           Email queue  SMS queue   Push queue
                               |          |          |
                            Workers     Workers     Workers
                                \         |         /
                                Provider adapters
                                        |
                             Callbacks -> delivery DB
~~~
Flow: authenticate and validate; insert notification + outbox atomically; relay publishes event (possibly duplicate); orchestrator checks preferences, renders templates and creates channel deliveries; workers enforce tenant/provider rate limits, call provider with deadline and persist result; signed callbacks update status. Why outbox? DB commit could succeed while event publish fails; relay closes that gap. Every consumer still deduplicates.

## 4. Partitioning and scale
Partition event stream by tenant+recipient where per-user ordering matters. Isolate very large tenants to prevent hot partitions. Scale workers independently per channel/provider. Use delayed retries with exponential backoff+jitter, bounded attempts and DLQ. Index deliveries by (status,next_retry_at) and notifications by (tenant,id). Large templates/assets in object storage. Protect origin DB from status polling with appropriate caching.

## 5. Failure modes
**Worker sends then crashes:** Provider may already have accepted the message. Reuse provider idempotency key if supported; otherwise duplicate send remains possible and is documented. **Provider outage:** Circuit breaker, quota-aware backoff, alternate provider only if allowed, explicit degraded status. **Preferences change:** For marketing/regulated opt-outs, recheck before dispatch, not just at acceptance. **Multi-region:** Home region ownership and documented RPO/RTO; active-active needs fencing to avoid duplicate sends. **Callback replay:** Verify signature and dedupe by provider event ID.

## 6. Security, metrics, tests
PII encryption, scoped credentials, tenant authorization, audit, retention/deletion, safe template escaping and secret rotation. Metrics: acceptance p99, enqueue-to-send p95, queue age, attempts/logical notification, duplicate ratio, opt-out violations, provider latency/cost and DLQ age. Load-test 1M/min and inject outbox relay duplicates, DB failover, consumer rebalance, provider timeout and worker crash. Verify no opt-out violations.

## 7. Trade-offs and speaking script
Synchronous provider calls are simpler but couple API availability to providers; event-driven outbox adds infrastructure in exchange for durability and decoupling. No exactly-once claim for arbitrary external effects.

0–5 min requirements; 5–10 API/invariants; 10–20 flow/schema; 20–30 scale; 30–40 failures/security; 40–45 trade-offs.

**Interview answer:** “I separate durable acceptance from external delivery, bridge DB and events with an outbox, and treat idempotency, preferences, tenant fairness and recovery as core requirements.”

Reference: https://aws.amazon.com/builders-library/timeouts-retries-and-backoff-with-jitter/

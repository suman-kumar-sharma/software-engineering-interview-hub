# Daily Software Engineering Interview Preparation — 2026-10-20

**Audience:** Product-company SDE2 / Senior Backend. These are representative exercises, not claims that a named company asked these exact questions.

Detailed walkthroughs: [HLD — Multi-tenant Audit Event Platform](../../../system-design/daily/2026-10-20-hld-audit-event-platform.md) · [LLD — Concurrent Token Bucket](../../../system-design/daily/2026-10-20-lld-token-bucket-rate-limiter.md)

## Q1. CompletableFuture timeout: did the underlying HTTP work stop?

An API fans out to three services. One future times out after 200 ms. That does not guarantee the HTTP client or remote server stopped working. The future may complete exceptionally while the underlying operation continues.

~~~java
CompletableFuture<Response> call =
    CompletableFuture.supplyAsync(() -> client.fetch(), ioExecutor)
        .orTimeout(200, TimeUnit.MILLISECONDS)
        .exceptionally(ex -> fallbackResponse());
~~~

Configure connect/request timeouts in the actual client, propagate one end-to-end deadline budget, and use bounded executors for blocking work. Cancellation is best-effort; side effects still need idempotency. Blocking calls in the common pool can starve unrelated tasks.

**Interview answer:** “I propagate a deadline budget, configure timeouts at the I/O client, and never assume a timed-out future has cancelled remote work.”

## Q2. Kafka consumer updated the database but crashed before offset commit

Assume the message can be replayed; prevent the business effect from being repeated. Give each operation a stable event ID and enforce it durably in the database.

~~~sql
CREATE TABLE processed_event (
  consumer_name text NOT NULL,
  event_id uuid NOT NULL,
  processed_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (consumer_name, event_id)
);
~~~

Insert the processed-event row and apply the business update in the same database transaction. Commit the Kafka offset after the durable DB transaction. A DB commit and offset commit are not one atomic transaction by default. For external payment APIs, use the provider's idempotency key and reconcile timeouts with unknown outcomes.

**Twist:** Kafka EOS does not automatically make an external payment gateway exactly-once.

**Interview answer:** “I design for replay, enforce durable idempotency in the same transaction as the state change, and use an outbox or provider idempotency for external effects.”

## Q3. PostgreSQL work queue with multiple workers

Workers must claim distinct jobs; a crashed worker must not strand a job forever.

~~~sql
BEGIN;
WITH candidate AS (
  SELECT id FROM job
  WHERE status = 'READY'
     OR (status = 'RUNNING' AND lease_until < now())
  ORDER BY priority DESC, created_at
  FOR UPDATE SKIP LOCKED LIMIT 1
)
UPDATE job j
SET status = 'RUNNING',
    lease_owner = :worker_id,
    lease_until = now() + interval '60 seconds',
    attempts = attempts + 1
FROM candidate c
WHERE j.id = c.id
RETURNING j.*;
COMMIT;
~~~

SKIP LOCKED lets workers skip rows already locked by another claimant. The lease supports crash recovery. Index the ready/priority scan and inspect the query plan; do not promise O(1) without evidence.

**Critical race:** A paused worker can wake after its lease expired and a new worker has completed the job. Use monotonically increasing fencing tokens where stale writes are dangerous, plus idempotent handlers and bounded retries.

**Interview answer:** “I atomically claim work with SKIP LOCKED, recover abandoned jobs with leases, and use idempotency plus fencing to prevent stale workers corrupting newer state.”

## Q4. Prevent cache stampede on a hot key

When a popular key expires, thousands of requests can all hit the database. Use TTL jitter to spread expirations, stale-while-revalidate to serve bounded-stale data during refresh, and local single-flight to coalesce same-key requests within one JVM.

A distributed lease may reduce duplicate refreshes across replicas, but lease expiry and network pauses mean it is not a correctness lock for arbitrary writes. Keep the database authoritative, cap concurrent refreshes and set refresh timeouts.

**Interview answer:** “I combine jitter and stale-while-revalidate with per-process single-flight; distributed coordination is an optimization, not the source of correctness.”

## Q5. Multi-tenant authorization: why is checking the URL tenant ID insufficient?

A caller changes /tenants/alpha/invoices/42 to /tenants/beta/invoices/42. Authentication proves identity, not access to that tenant or object.

Derive tenant identity from verified claims or server-side membership, not a trusted request parameter. Check membership and ownership on every request. Scope repository queries by the authorized tenant; consider database row-level security as defense in depth. Test cross-tenant reads, updates, exports, async consumers and cache access.

**Twist:** A cache key containing only invoiceId can leak data even if the controller is correct. Include tenant scope in cache keys, search filters and audit events.

**Interview answer:** “Tenant-scoped authorization belongs at service and data boundaries, with object-level tests and tenant-aware cache keys.”

## Q6. Virtual threads and database connection pools

Virtual threads make blocking tasks cheaper to represent; they do not create more database connections or CPU. A JDBC pool with 40 connections remains a bottleneck under 20,000 requests.

~~~java
private final Semaphore dbPermits = new Semaphore(40);

Result queryWithLimit() throws InterruptedException {
    dbPermits.acquire();
    try {
        return repository.query();
    } finally {
        dbPermits.release();
    }
}
~~~

Use bounded admission and timeouts instead of unlimited waiting. Align permits with pool capacity, release them in finally, and calculate the aggregate limit across replicas: ten instances with 40 permits each may create 400 concurrent DB operations.

**Interview answer:** “Virtual threads reduce thread-management cost, but I still bound concurrency at scarce downstream resources and calculate limits across replicas.”

## Q7. DSA: longest substring with at most K distinct characters

Maintain a sliding window and frequency map. Expand right; while distinct count exceeds K, shrink left; track the best width.

~~~java
int longestAtMostK(String s, int k) {
    if (k <= 0 || s.isEmpty()) return 0;
    Map<Character, Integer> freq = new HashMap<>();
    int left = 0, best = 0;
    for (int right = 0; right < s.length(); right++) {
        freq.merge(s.charAt(right), 1, Integer::sum);
        while (freq.size() > k) {
            char c = s.charAt(left++);
            int n = freq.get(c) - 1;
            if (n == 0) freq.remove(c);
            else freq.put(c, n);
        }
        best = Math.max(best, right - left + 1);
    }
    return best;
}
~~~

For eceba with K=2, the best window is ece, length 3. Each character enters and leaves at most once: O(n) expected time and O(min(n, alphabet)) space. Java char represents UTF-16 code units; use code-point iteration if Unicode code points are required.

**Interview answer:** “The invariant is at most K distinct characters in the active window; both pointers only move forward, so time is linear.”

## Q8. Retries made an outage worse

Three layers with three attempts each can produce up to 27 downstream calls per request. Retries become load generators during an outage.

Retry only transient failures and safe/idempotent operations. Use a shared deadline and attempt budget, exponential backoff with jitter, respect Retry-After where applicable, and do not retry validation failures. Combine retries with circuit breaking, concurrency limits and metrics for attempt count, exhausted budgets and queue depth.

**Interview answer:** “I bound retries and deadlines, add jittered backoff, require idempotency and use admission control to protect dependencies.”

## HLD — Multi-tenant Audit Event Platform

**Goal:** Accept audit events from many tenants, make accepted events durable, support tenant-scoped search for 90 days and retain compliance archives for seven years. Never silently drop an accepted event.

**Assumptions:** 20k events/s sustained, burst to 60k/s, p95 acknowledgement target below 150 ms under normal load. At-least-once delivery is acceptable if duplicates are safely handled.

~~~text
Producers -> Gateway/Auth/Quota -> Ingestion API -> Replicated Kafka
Kafka -> Search indexer -> OpenSearch (90-day search)
Kafka -> Archive consumer -> Object storage (7-year retention)
Kafka -> DLQ/replay tooling
Metrics + traces + admin audit -> Monitoring/SIEM
~~~

**API:** POST /v1/audit-events:batch. Derive tenant identity from authenticated context, not client-supplied tenantId. Validate schema, payload size, actor, action, resource, event ID and schema version.

**Partitioning:** Tenant as key preserves per-tenant order but a very large tenant can create a hot partition. Sharding a tenant improves throughput but weakens global tenant ordering. Clarify whether ordering is required per resource, per tenant or not at all.

**Durability and idempotency:** Acknowledge only after broker durability settings have accepted the record. Broker acknowledgement does not mean the search index is current. Keep event IDs stable across retries and deduplicate consumers by tenant plus event ID. If a business DB transaction must atomically create the audit event, write an outbox row in that same transaction and relay it.

**Failure handling:** If broker is unavailable, reject/backpressure instead of false success. If search is down, retain Kafka data and replay later. Archive failures need bounded retries, alerting and reconciliation. Quarantine poison events with original bytes and reason. Apply tenant scope in APIs, search filters, cache keys, archive policy and admin tools.

**Security:** Encrypt in transit/at rest, minimize PII, separate operator roles, audit privileged reads and define retention/legal-hold policy. Object retention controls do not replace access control or key lifecycle management.

**Observability:** accepted/rejected rate, broker latency, consumer lag by partition/tenant, index freshness, archive lag, DLQ depth and event age. Alert on lag growth, not only process liveness.

**Trade-offs:** Kafka provides replay and throughput but adds partition/consumer operations. OpenSearch is a derived index. Object storage is cheap for long retention but not an interactive search store. At-least-once plus idempotent consumers is more honest than claiming end-to-end exactly-once.

## LLD — Concurrent Token Bucket Rate Limiter

A token bucket allows bursts up to capacity and replenishes at a steady rate. One atomic critical section must refill, check and deduct tokens. The linked LLD contains the Java 17 implementation, deterministic tests, complexity and distributed deployment caveats.

Invariant: 0 <= tokens <= capacity. Use System.nanoTime() for elapsed duration, not wall-clock time. A process-local limiter cannot enforce a global quota across replicas; use a shared atomic store or gateway-level limiter for global enforcement.

## Engineering updates — verified 20 October 2026

1. **PostgreSQL 19 RC 1, 15 October:** RC 2 is planned for 12 November and GA for 19 November, subject to issues found. Do not treat an RC as production GA. The announcement notes fixes around concurrent repack, planner behavior, logical replication and parallel autovacuum. [Official announcement](https://www.postgresql.org/about/news/postgresql-19-rc-1-released-3409/).
2. **AWS Lambda Web Functions, 12 October:** AWS describes native HTTP framework execution with multi-concurrent execution and global distribution. Evaluate framework compatibility, runtime limits, latency and observability against API Gateway plus Lambda or container hosting. [AWS announcement](https://aws.amazon.com/blogs/aws/announcing-aws-lambda-web-functions/).
3. **GitHub engineering, 14 October:** GitHub described reducing star-history retrieval from 2,500 requests to 24 by avoiding per-stargazer identity retrieval. Design aggregate endpoints around the result needed rather than fetching every underlying record. [Engineering article](https://github.blog/engineering/).

## Revision checklist
Explain timeout versus cancellation, Kafka replay and DB idempotency, lease/fencing races, tenant isolation beyond controllers, sliding-window O(n), and the audit platform's durability/ordering/replay trade-offs.

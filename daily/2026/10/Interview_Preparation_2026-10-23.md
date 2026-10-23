
# Daily Interview Preparation — 2026-10-23
**Target:** Product-company SDE2 / Senior Backend | **Stack:** Java 17+, Spring Boot, Kafka, PostgreSQL, Redis, AWS

These are representative practice questions; no company attribution is made without a verified original interview source.

## 1. Virtual threads and backpressure
**Question:** A Spring service makes blocking downstream calls. Do virtual threads mean we can accept unlimited traffic?

**Reasoning:** No. Virtual threads make blocked tasks cheaper to represent; they do not increase database connections, downstream quotas, CPU, or memory. Put a bulkhead around the scarce dependency.

~~~java
Semaphore inventorySlots = new Semaphore(80);

Inventory readInventory(String sku) throws InterruptedException {
    if (!inventorySlots.tryAcquire(150, TimeUnit.MILLISECONDS)) {
        throw new RejectedExecutionException("inventory capacity exhausted");
    }
    try {
        return inventoryClient.fetch(sku); // configure a client deadline too
    } finally {
        inventorySlots.release();
    }
}
~~~

The semaphore caps in-flight calls per instance. With ten instances, the fleet could permit roughly 800 calls; use per-tenant quotas or a shared limiter if a fleet-wide cap is required. Configure client timeouts and release permits in finally.

**Follow-up:** Choose the permit count from load tests, downstream p99 latency, connection-pool size and saturation metrics. Do not hide overload behind an unbounded queue.

## 2. CompletableFuture timeout versus cancellation
If a future times out after 300 ms, the underlying HTTP request is not necessarily cancelled. The future completes exceptionally; the I/O stops only if the client and cancellation wiring support it. Propagate an absolute deadline and configure client-native connect/read/request timeouts. Avoid the common ForkJoinPool for blocking I/O.

A timeout does not roll back remote work. If a payment times out after the server commits, a blind retry can double-charge. Use an idempotency key and reconcile ambiguous outcomes.

## 3. Kafka exactly-once semantics
Kafka transactions can provide exactly-once processing for Kafka-to-Kafka workflows with a transactional producer and consumers configured appropriately, such as read_committed. This does not automatically include PostgreSQL or HTTP effects.

For DB effects, write an inbox/deduplication row with a unique event ID and the domain mutation in one database transaction. If offset commit fails, the message may replay; the unique inbox key makes the second application a no-op. Ordering is per partition; key by aggregate ID when aggregate ordering matters. Increasing partition count can change key-to-partition mapping, so plan migration and replay.

## 4. Transactional outbox and duplicate delivery
The outbox atomically commits domain state and the intent to publish. It cannot atomically commit a broker publish and a database acknowledgement. If the broker accepts the event and the relay crashes before marking it published, the event may be sent again.

Use stable event IDs, idempotent consumers, bounded batches, exponential backoff with jitter, and lease expiry. Claim PostgreSQL rows with SKIP LOCKED in a short transaction, then publish outside it; never hold row locks across a network call. Monitor oldest unpublished age and expired leases.

## 5. PostgreSQL optimistic concurrency
For relatively rare conflicts, use a version column or conditional update:

~~~sql
UPDATE inventory
SET available = available - :qty,
    version = version + 1
WHERE sku = :sku
  AND version = :expected_version
  AND available >= :qty;
~~~

One affected row means success. Zero rows may mean stale version, missing SKU, or insufficient stock; distinguish these according to the business contract. Reject non-positive quantity before SQL. On conflict, re-read and recompute with bounded retries—never retry the same stale version. For hot rows, compare a short pessimistic lock or atomic conditional update.

## 6. Redis cache stampede
When a hot key expires, thousands of requests can hit the database. Combine TTL jitter, request coalescing, stale-while-revalidate and a DB bulkhead. Store a value with fresh and hard-expiry times. During the stale window, return stale data while one leased refresher recomputes it; after hard expiry, use bounded waiting or a safe fallback.

A lease reduces duplicate refreshes but does not guarantee one refresher under pauses or lease expiry. Cache keys for tenant data must include authorization scope. A client-provided tenant header is not proof of authorization.

## 7. DSA: longest substring with at most K distinct characters
Use a sliding window. Expand right; while distinct count exceeds K, move left and decrement frequencies.

~~~java
static int longestAtMostKDistinct(String s, int k) {
    if (s == null) throw new IllegalArgumentException("s is null");
    if (k <= 0 || s.isEmpty()) return 0;
    Map<Character, Integer> freq = new HashMap<>();
    int left = 0, best = 0;
    for (int right = 0; right < s.length(); right++) {
        freq.merge(s.charAt(right), 1, Integer::sum);
        while (freq.size() > k) {
            char drop = s.charAt(left++);
            int count = freq.get(drop) - 1;
            if (count == 0) freq.remove(drop);
            else freq.put(drop, count);
        }
        best = Math.max(best, right - left + 1);
    }
    return best;
}
~~~

Time O(n), space O(min(n, alphabet)), because each pointer advances at most n times. Dry run: eceba with k=2 yields best window ece of length 3. Java char represents UTF-16 code units; use code points if the problem defines Unicode code points as characters.

## 8. SQL keyset pagination
Deep OFFSET often scans and discards many rows; concurrent inserts can cause duplicate or skipped records between pages. Use keyset pagination for sequential navigation:

~~~sql
SELECT id, created_at, status
FROM orders
WHERE tenant_id = :tenant
  AND (created_at, id) < (:last_created_at, :last_id)
ORDER BY created_at DESC, id DESC
LIMIT :page_size;
~~~

A matching index is (tenant_id, created_at DESC, id DESC). Include id to break timestamp ties. Bind tenant scope from the authenticated principal. Keyset pagination is not ideal when the user must jump directly to an arbitrary page number.

## 9. Spring Security tenant authorization
Authentication answers “who are you?” Authorization answers “may you act on this tenant's resource?” Enforce tenant membership at the service/domain boundary, not only at the URL. Verify membership from trusted claims or policy storage; never trust a tenant ID supplied by a request header alone. Test REST endpoints, async jobs, message consumers and admin paths. Log actor, tenant, action and outcome, but never tokens or secrets.

---

# HLD — Durable Multi-Tenant Job Scheduler
**Problem:** Millions of tenants schedule jobs seconds to months ahead. Jobs survive process restarts, expose status/cancellation and retry transient failures. Delivery may duplicate; external effects must be idempotent.

## Requirements and SLO
APIs: create/get/cancel; idempotency key; tenant quotas; bounded payloads; audit history. Define precision explicitly—for example, p99 job-start lag ≤ 5 seconds at agreed normal load. Clarify recurring schedules, regional RTO/RPO and whether handlers are internal task types or arbitrary customer code.

## Architecture

~~~text
Client -> API/Auth/Quota -> Job DB + Outbox
                           -> Due-time scheduler shards
Outbox relay -> durable queue -> bounded workers -> handler
                              -> status/retry store
Metrics + DLQ + audited replay + tenant audit
~~~

The database is the source of truth for schedule state; the queue is a dispatch buffer, not the only record that a job exists. The outbox keeps job state and dispatch intent in one transaction.

## Data model and indexing
Store job ID, tenant ID, idempotency key, request hash, payload reference, due time, state, attempt, next retry time, lease expiry, version and timestamps. Enforce unique (tenant_id, idempotency_key). Index scheduled jobs by due time and retry-wait jobs by retry time. Keep large/sensitive payloads in object storage when appropriate.

## Scheduling and claiming
Start with indexed DB scans and short conditional claims/leases. Publish after the claim transaction commits. SKIP LOCKED or compare-and-set updates can prevent multiple schedulers claiming the same row. Never hold DB row locks during a broker call. At higher volume, time buckets or a timing wheel can improve wakeups, but rebuild the near-term horizon from durable state after restart.

Queue-delay-only designs are not sufficient for arbitrary long scheduling horizons; keep the database authoritative.

## State machine and reliability
SCHEDULED -> DISPATCHED -> RUNNING -> SUCCEEDED; transient failures move to RETRY_WAIT; permanent or exhausted failures move to DEAD. Cancellation wins only before execution starts. It cannot undo a side effect already in flight.

Use exponential backoff with full jitter, max attempts, retry budgets and per-tenant concurrency caps. Worker claims carry a version/fencing token so a stale worker cannot overwrite a newer attempt. If publish succeeds but DB acknowledgement fails, dispatch can repeat. If a handler's side effect succeeds but status write fails, execution can repeat. Give handlers a stable idempotency key or reconciliation path.

## Capacity, observability and security
Estimate peak creation rate, due-time distribution, average/p99 handler duration, retry amplification, payload bytes and tenant skew. Monitor due-job lag, oldest outbox age, queue age, lease expiry, attempts per job, failure class, DLQ depth, handler saturation and tenant rejection.

Authenticate tenant identity, enforce quotas before persistence, validate payload and schedule horizon, encrypt sensitive payloads, restrict worker egress and avoid arbitrary URL execution (SSRF). A DB scan is simple but can pressure the database at very high volume; time buckets improve throughput but add recovery complexity.

**Interview close:** “I persist schedule state first, dispatch via an outbox-backed queue, claim work with leases and fencing versions, and assume at-least-once delivery. Idempotency protects business effects. I scale based on due-job lag and handler capacity rather than API QPS alone.”

See the dedicated HLD and LLD files linked from INDEX.md.

## Engineering updates verified October 23, 2026
1. **Spring Boot 4.2.0-RC1 and 4.1.2 released October 23.** Treat RC as compatibility-test material, not an automatic production upgrade. Review official notes and align dependency versions: https://spring.io/blog/2026/10/23/spring-boot-4-2-0-RC1-available-now/
2. **AWS Continuum for code vulnerabilities became generally available October 22.** Automated findings and pull requests still need human review, tests, least-privilege credentials and staged rollout: https://aws.amazon.com/blogs/aws/aws-continuum-for-code-vulnerabilities-is-now-generally-available/
3. **Amazon ECS Gateway announced October 20.** It simplifies ECS ingress provisioning; assess exposure, networking and existing load-balancer requirements: https://aws.amazon.com/blogs/aws/introducing-amazon-ecs-gateway-a-fully-managed-ingress-solution-for-ecs-services/
4. **GitHub described agent-scale Git infrastructure migration October 21.** Per-reference quorums and independently scalable object reads illustrate how to separate consistency and storage bottlenecks: https://github.blog/engineering/the-bridge-to-agent-scale-git-infrastructure/

## Revision checklist
- Timeout versus cancellation?
- Kafka EOS versus external side-effect idempotency?
- How does outbox recover publish/ack ambiguity?
- Derive a keyset index from predicate and order?
- Define scheduler SLO and overload behavior?

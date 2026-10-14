# Interview Preparation — 2026-10-14 (English)

> **Audience:** Product-company SDE2 / Senior Backend Engineer. **Difficulty:** 9 original practice questions (8 advanced backend + 1 DSA), one full HLD, one full LLD. **Research date:** 14 October 2026, IST. These are **original interview-style exercises**, not verified transcripts from named employers. The engineering-news items are linked to dated official sources. No private LinkedIn feed was accessed.

## Today's index

| ID | Topic | Difficulty | Core test |
|---|---|---|---|
| Q1 | Java concurrency: bounded fan-out | Hard | Backpressure, resource bounds |
| Q2 | Kafka: ordering, hot keys, poison events | Hard | Partition and retry semantics |
| Q3 | Cache stampede / single-flight | Hard | Failure and concurrency |
| Q4 | PostgreSQL write skew | Hard | Transactional invariants |
| Q5 | Spring Security multi-tenant JWT | Hard | Authorization vs authentication |
| Q6 | JVM memory growth investigation | Medium–Hard | Production diagnostics |
| Q7 | Sliding window: at most K distinct | Medium | DSA correctness |
| Q8 | AWS SQS + Lambda partial batch failures | Hard | Idempotent processing |
| Q9 | Retry storms and circuit breakers | Hard | Resilience |
| HLD | Multi-tenant notification platform | Hard | Scale, delivery, security |
| LLD | Bounded blocking queue in Java | Hard | Concurrency and lifecycle |

**Interview framework:** First state invariants and assumptions, describe the naive solution, expose its failure mode, propose a correct design, then explain trade-offs, complexity, tests and operational evidence.

---

## Q1. Design a bounded Java fan-out aggregator (Hard — Concurrency)

**Question.** An API needs to call up to 200 independent pricing providers. Each call can block for 100–800 ms. How do you maximize concurrency without exhausting threads, connections or downstream quotas?

### Reasoning

Launching 200 asynchronous tasks does **not** mean the system has capacity for 200 concurrent HTTP calls. A fixed pool limits running threads, but its default unbounded work queue may still accumulate millions of requests. A virtual-thread-per-task executor reduces thread overhead, but does not limit sockets, provider QPS or the DB pool. Use **admission control** before submitting tasks, plus connection limits, deadlines and per-provider bulkheads.

**Invariants:** at most 16 outstanding tasks in this process; a permit is returned on success/failure; each task has an external I/O deadline; no waiting task holds a DB connection; result ordering is deterministic if the API requires it.

### Runnable Java 17 demonstration

```java
import java.util.*;
import java.util.concurrent.*;

public final class BoundedFanOut {
    record Item(int id) {}

    static List<Integer> run(List<Item> items)
            throws InterruptedException, ExecutionException {
        ExecutorService pool = Executors.newFixedThreadPool(8);
        Semaphore inFlight = new Semaphore(16);
        List<Future<Integer>> futures = new ArrayList<>();
        try {
            for (Item item : items) {
                inFlight.acquire(); // backpressure BEFORE queueing
                try {
                    futures.add(pool.submit(() -> {
                        try {
                            return item.id() * 2; // replace with deadline-bound HTTP call
                        } finally {
                            inFlight.release();
                        }
                    }));
                } catch (RejectedExecutionException e) {
                    inFlight.release();
                    throw e;
                }
            }
            List<Integer> result = new ArrayList<>();
            for (Future<Integer> f : futures) result.add(f.get());
            return result;
        } finally {
            pool.shutdownNow();
        }
    }

    public static void main(String[] args) throws Exception {
        System.out.println(run(List.of(new Item(1), new Item(2))));
    }
}
```

**Output:** `[2, 4]`. `Semaphore` bounds submitted-but-incomplete work; the pool bounds actively executing tasks. In production, a finite HTTP timeout is essential: a stuck call can hold a permit forever. For very large input streams, avoid storing every Future; consume completions incrementally or stream batches. The example's `finally` interrupts outstanding tasks on early failure; design cancellation and partial-result behavior explicitly.

**Complexity:** O(n) submission and result collection, O(n) futures as shown, O(1) semaphore operations, maximum 16 outstanding submitted tasks. Real latency is governed by service time, pool width, downstream capacity and timeout policy.

**Edge cases:** cancellation, interruption while acquiring, partial failure, a slow provider, empty list, provider-specific quotas, connection-pool saturation, overload response (429/503).

**Follow-ups:** How would you replace the pool with virtual threads while preserving the semaphore? How do you implement an end-to-end deadline rather than independent 2-second timeouts? Why can increasing the pool worsen p99?

**Interview answer:** “I bound concurrency at the admission point, not just at the executor. I apply per-dependency bulkheads and deadlines, propagate cancellation, and measure queueing separately from downstream latency.”

Reference: https://docs.oracle.com/en/java/javase/25/docs/api/java.base/java/util/concurrent/Semaphore.html

---

## Q2. Kafka: preserve per-order ordering while surviving retries and poison messages (Hard)

**Question.** Order events `CREATED → PAID → SHIPPED` must be processed in order for each order ID, but unrelated orders should process concurrently. One `PAID` event is malformed. How do you handle it?

### Solution derivation

A Kafka topic is partitioned; Kafka provides ordering **within a partition**, not across the whole topic. Publish every event for an order using `orderId` as the record key, with a stable partitioning scheme. Consumers in a group can process different partitions concurrently.

A naive “send failed event to retry topic, then process next event” **breaks per-order processing order**. If `PAID` is delayed but `SHIPPED` proceeds, the order state becomes invalid.

**Choose a business policy:**
1. Strict ordering: pause/block the affected partition, retry with bounded backoff, then move the poison record to a quarantined workflow only after a documented operator/business decision. This impacts unrelated keys sharing that partition.
2. Key-scoped sequencing: persist per-order expected sequence/version, buffer later events for that order, and process other keys. This improves concurrency but adds durable buffering, expiry, gap detection and recovery complexity.
3. State-machine validation: apply monotonic legal transitions using database compare-and-set; reject/reconcile out-of-order events rather than blindly mutating state.

**Producer:** key by order ID, use idempotent producer configuration, consider transactional outbox so DB state and event intent are atomic. **Consumer:** commit offsets only after durable state change or durable quarantine decision; deduplicate event IDs. An event moving to a DLQ is **not** proof that downstream business state was completed.

**Concrete dry run:** Partition P1 receives O7-CREATED(seq1), O7-PAID(seq2), O7-SHIPPED(seq3). If seq2 fails, strict mode holds seq3. A key-scoped design may allow O8 events to continue while O7 waits, but only with durable per-key sequencing.

**Trade-offs:** More partitions increase parallelism but also operational cost; one hot `orderId` remains sequential. Partition-count changes can change key-to-partition mapping for later events; plan ordering boundaries during migration.

**Complexity:** O(1) average per record state lookup (with indexed key) plus durable I/O; memory/buffer size depends on backlog and poison-record policy.

**Follow-ups:** What happens during a consumer-group rebalance? Can you guarantee global order? How do you handle sequence gaps and late events? What is the recovery procedure after DLQ replay?

**Interview answer:** “I use orderId as the partition key for normal ordering, but explicitly design retry and DLQ handling so a failed event cannot silently allow invalid later state transitions.”

Reference: https://kafka.apache.org/documentation/

---

## Q3. A hot cache key expires and 5,000 requests hit the database (Hard)

**Question.** A product-price cache key expires exactly during a flash sale. Requests spike from 100/s to 5,000/s. What do you do?

### Why naive caching fails

Cache-aside normally does `get → miss → database read → set`. At expiry, thousands of callers may observe the same miss, generating a **cache stampede**. Merely increasing Redis capacity does not reduce origin fan-out.

**Layered design:**
- **Request coalescing / single-flight:** one loader per key per instance; followers await the same result. An in-process `ConcurrentHashMap<Key, CompletableFuture<Value>>` can coalesce local misses. Always remove completed/failed flights safely and bound waiter time.
- **Cross-instance coordination:** for expensive keys, a short lease or distributed single-flight protocol reduces duplicate loads. A lease is not a correctness lock; account for owner crashes, fencing and stale leases.
- **Stale-while-revalidate:** return a recently expired value briefly when business semantics allow it; refresh asynchronously. Never use stale data for authoritative balances or final checkout pricing.
- **TTL jitter and proactive refresh:** spread expirations, prewarm hot keys, apply negative caching with short TTLs to protect missing IDs.
- **Origin bulkhead:** cap DB load and degrade predictably instead of flooding the primary.

**Dry run:** 5,000 requests land on 10 app instances. Local coalescing can reduce origin loads from 5,000 to ~10, assuming each instance has one loader and no additional cross-instance coordination. This is an illustrative estimate, not a guaranteed bound.

**Correctness:** A cached product price is advisory; checkout should verify authoritative price/version. Handle failures by clearing the in-flight future so the next request can retry. Do not cache errors forever.

**Complexity:** local map lookup expected O(1); additional memory O(number of keys currently loading + waiters); database load depends on number of distinct keys and coordinating instances.

**Follow-ups:** How do you avoid stale overwrite after a price update? How do you handle a Redis outage? What happens if the refresh takes longer than the lease?

**Interview answer:** “I prevent stampedes with single-flight, bounded origin concurrency, jittered TTLs and carefully scoped stale reads, while keeping authoritative decisions on the source of truth.”

---

## Q4. PostgreSQL write skew: two doctors leave the hospital with no one on call (Hard)

**Question.** Two doctors are on call. The rule is “at least one doctor must remain on call.” Each doctor concurrently checks that another doctor is on call and then marks themselves off call. Why can both transactions commit?

### Derive the anomaly

Initial state: Alice=ON, Bob=ON. Transaction T1 reads Bob=ON, then updates Alice=OFF. T2 reads Alice=ON, then updates Bob=OFF. The transactions write **different rows**. Under snapshot isolation / PostgreSQL Repeatable Read, they can both commit even though the cross-row invariant is broken. This is **write skew**, not a simple lost update.

**Approach A:** Use `SERIALIZABLE` isolation and retry the **whole transaction** on SQLSTATE `40001`. PostgreSQL detects dangerous read/write dependencies and aborts a conflicting transaction.

**Approach B:** Explicitly serialize access to the invariant, e.g., lock a shared schedule/shift row with `SELECT ... FOR UPDATE` before checking and updating the doctor rows. All writers must follow the same protocol.

```sql
BEGIN TRANSACTION ISOLATION LEVEL SERIALIZABLE;
SELECT count(*) FROM on_call WHERE shift_id = 7 AND active;
-- if count > 1, mark this doctor off call
UPDATE on_call SET active = false
WHERE shift_id = 7 AND doctor_id = 42;
COMMIT;
-- On SQLSTATE 40001: rollback and retry the entire transaction.
```

**Why a Java synchronized block is insufficient:** multiple service replicas and other database clients do not share one JVM lock.

**Tests:** start two transactions with a barrier so both read the original state; assert at most one succeeds in removing itself; retry and verify at least one remains.

**Trade-offs:** Serializable provides a powerful invariant model but can abort transactions under contention. A shared-row lock makes contention explicit but can become a hotspot. Keep transactions short.

**Interview answer:** “Write skew occurs because each transaction updates a different row after reading a shared invariant. I protect the invariant with serializable isolation plus full retries or a shared database lock.”

Reference: https://www.postgresql.org/docs/current/transaction-iso.html

---

## Q5. Spring Security: prevent cross-tenant data access despite a valid JWT (Hard)

**Question.** Tenant A's user sends `GET /tenants/B/invoices/123` with a valid JWT. Why is JWT validation alone insufficient?

### Security boundary

**Authentication** proves the token is valid and identifies a principal. **Authorization** decides whether that principal may access a specific resource. Signature, `iss`, `aud`, `exp`, `nbf` validation are necessary, but do not establish access to tenant B.

**Recommended flow:**
1. Validate JWT signature, issuer, audience and expiry with Spring Security resource-server configuration.
2. Resolve effective tenant from a **trusted claim / server-side membership**, not blindly from a request header.
3. Check user role **within that tenant** and verify invoice ownership with a tenant-scoped repository query.
4. Use `WHERE tenant_id = :authorizedTenant AND invoice_id = :id`; never fetch by invoice ID alone and authorize after exposing data.
5. Audit denied cross-tenant attempts without logging tokens or sensitive payloads.

```java
// Repository sketch: tenant is derived from validated authorization context.
@Query("select i from Invoice i where i.id = :id and i.tenantId = :tenantId")
Optional<Invoice> findAuthorized(@Param("id") UUID id,
                                  @Param("tenantId") UUID tenantId);
```

**Example:** Alice belongs only to tenant A. The URL says B, but the authorization context says A. Reject before data access or return a safe not-found response. If Alice belongs to both tenants, require an explicit authorized tenant selection and check the selected tenant membership.

**Defense in depth:** database row-level security where suitable, tenant ID in unique keys and cache keys, strict service-to-service scopes, signed identity propagation and negative tests for insecure direct object references.

**Follow-ups:** How do you handle key rotation? Revoked access with a long-lived JWT? Background jobs without user context? Cross-tenant admin roles?

**Interview answer:** “JWT verification authenticates the caller, but every data access must enforce tenant-scoped authorization using trusted membership and tenant-qualified queries.”

Reference: https://docs.spring.io/spring-security/reference/servlet/oauth2/resource-server/jwt.html

---

## Q6. JVM heap keeps growing after a deployment; CPU is normal (Medium–Hard)

**Question.** Heap usage trends upward over several hours, full GC becomes more frequent, but CPU remains moderate. How do you investigate?

### Evidence-first workflow

Distinguish **heap allocation rate** from **live-set retention**. A healthy app can allocate rapidly yet reclaim memory. A leak-like pattern is post-GC used heap climbing toward `-Xmx`.

**Steps:** Compare before/after deployment and request mix. Inspect heap-used-after-GC, GC pause time, allocation rate, old-generation occupancy, thread count, direct-buffer memory, connection pools and cache size. Capture a heap dump in a controlled environment (dumps can pause processes and contain secrets), inspect dominator tree and paths to GC roots. Compare two snapshots if possible.

**Likely causes:** unbounded cache, `ThreadLocal` retaining request objects, static collection, listener not deregistered, long-lived queue, reactive subscription not cancelled. Off-heap growth needs native-memory and direct-buffer investigation rather than only heap dump.

**Example:** A new metrics tag uses `userId` as label. Cardinality grows with each request and the metrics registry retains series. Fix the label cardinality, not just `-Xmx`.

**Operational response:** protect service via rollout halt/rollback if p99 and OOM risk are rising; preserve enough evidence before restarting; test with representative load and post-GC baseline.

**Follow-ups:** How do you distinguish leak vs traffic increase? Why can RSS rise when Java heap looks stable? What are risks of heap dumps in production?

**Interview answer:** “I compare post-GC live-set and allocation behavior, locate retained object graphs, and validate the fix under sustained representative load.”

---

## Q7. DSA: longest substring with at most K distinct characters (Medium)

**Question.** Given a string and integer `k`, return the length of the longest substring containing at most `k` distinct characters.

### Derivation

Brute force enumerates O(n²) substrings and checks distinct characters. Instead, maintain a **sliding window** `[left,right]` and a frequency map. Expand right; while distinct count exceeds `k`, shrink left. Once valid, update maximum length.

```java
import java.util.*;

public final class AtMostKDistinct {
    static int longest(String s, int k) {
        if (s == null || k <= 0) return 0;
        Map<Character, Integer> freq = new HashMap<>();
        int left = 0, best = 0;
        for (int right = 0; right < s.length(); right++) {
            char c = s.charAt(right);
            freq.merge(c, 1, Integer::sum);
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
    public static void main(String[] args) {
        System.out.println(longest("eceba", 2)); // 3 ("ece")
    }
}
```

**Invariant:** After the inner while-loop, the window has at most k distinct characters. Every index enters/leaves the window at most once, so **O(n)** time and O(min(k, alphabet size)) map space. Java `char` counts UTF-16 code units, not Unicode code points; use `codePoints()` if the problem defines characters as Unicode code points.

**Edge cases:** empty string, `k=0`, `k>number of distinct`, repeated characters, null policy, surrogate pairs.

**Follow-ups:** Exactly K distinct? Use `atMost(k) - atMost(k-1)` for **count** of substrings with exactly K, not directly for longest length. How do you adapt to a streaming input?

**Interview answer:** “I maintain a valid sliding window and shrink only when distinct count exceeds K; each pointer advances at most n times.”

---

## Q8. AWS Lambda consumes SQS batches: one poison record causes duplicates (Hard)

**Question.** Lambda receives 10 SQS messages; 9 succeed and one fails. Why can the 9 successful messages be processed again, and how do you avoid unnecessary retries?

### Mechanics

By default, when the invocation fails, the entire batch can become visible again after the queue visibility timeout. That means the 9 already-successful records can be retried. Enable `ReportBatchItemFailures` and return the failed message identifiers in `batchItemFailures`; keep each handler idempotent anyway because delivery is at least once.

**Simplified response:**
```json
{
  "batchItemFailures": [
    { "itemIdentifier": "failed-sqs-message-id" }
  ]
}
```

**Implementation checklist:** Catch per-record exceptions; return only failures; configure event source mapping's `FunctionResponseTypes` with `ReportBatchItemFailures`; choose visibility timeout greater than function timeout and appropriate for retry; configure DLQ/redrive policy; monitor oldest message age and DLQ depth.

**FIFO caveat:** With ordered FIFO processing, stop at the first failure and report the failed **and unprocessed** records, otherwise processing later records can violate order. Distinguish SQS message ID from business idempotency ID.

**Follow-ups:** What if the function times out without returning a response? What if the downstream DB commits and Lambda crashes? How do you handle long-running jobs exceeding visibility timeout?

**Interview answer:** “I use partial batch responses to avoid replaying successful records, but design the business handler as idempotent because SQS/Lambda delivery remains at least once.”

References: https://docs.aws.amazon.com/lambda/latest/dg/services-sqs-errorhandling.html and https://docs.aws.amazon.com/lambda/latest/dg/with-sqs.html

---

## Q9. A downstream service slows down; retries make the outage worse (Hard)

**Question.** A payment authorization dependency times out for 10% of calls. Every upstream service retries three times. Why does this amplify failure, and how do you fix it?

### Retry amplification

If 4 service layers each make up to 3 attempts independently, one original request can induce up to **3^4 = 81 attempts** along a fully retried path (illustrative worst-case; actual graph and failure point matter). Retries increase queueing and can turn partial overload into collapse.

**Solution:**
- Establish a **single end-to-end deadline**; reserve time for network and downstream work.
- Retry only transient, classified failures and **idempotent operations** (or operations protected by idempotency keys). Never blindly retry a payment whose outcome is unknown.
- Use exponential backoff with jitter, a small retry budget, and a single chosen retry layer.
- Circuit breaker prevents repeated calls to an unhealthy dependency; bulkhead limits concurrent in-flight work; bounded queue and load shedding prevent resource exhaustion.
- Track attempted requests vs original requests, retry rate, circuit state, saturation, timeout rate and p99.

**Example policy:** deadline 800 ms, at most 2 total attempts at the edge, 50–150 ms jittered wait only if remaining budget allows. Values are illustrative and require load testing.

**Trade-offs:** Fail-fast protects capacity but increases visible errors. Hedged requests may reduce tail latency for safe read-only calls but increase load; do not hedge non-idempotent writes.

**Follow-ups:** Difference between retry and circuit breaker? What is a retry storm? How do you avoid synchronized retries after a regional outage?

**Interview answer:** “I cap retries under a shared deadline, use jitter and idempotency, and combine a circuit breaker with concurrency limits so recovery traffic cannot overwhelm the dependency.”

Reference: https://resilience4j.readme.io/docs/retry

---

# HLD — Design a multi-tenant notification delivery platform (45-minute senior interview)

## 1. Clarify requirements (first 5 minutes)

**Functional:** tenants create templates; publish email/SMS/push notifications; support transactional and bulk campaigns; track delivery state; retry transient failures; honor opt-outs and channel preferences; support provider webhooks. **Nonfunctional:** tenant isolation, cost caps, 99.9%+ accepted-event durability target (hypothetical SLO), no uncontrolled provider bursts, auditability, observability, regional privacy requirements.

**Explicit assumptions (not external facts):** 50 million notifications/day (~579 average/s), peak 20x (~11,600/s), 100k tenants, 30-day hot delivery status retention, provider throughput is independently rate-limited. These numbers size the *example*, not a real system.

## 2. Back-of-envelope

50M/day × 30 days = **1.5B delivery attempts/records** if one record per notification and no retries. At 500 bytes per record, ~750 GB raw before indexes/replicas/metadata. In reality attempts, webhooks and audit entries multiply this. Partition hot data by tenant/hash and time; archive older records.

## 3. Architecture

```text
Client / SDK
     |
API Gateway / AuthN / tenant quotas
     |
Notification API -----> PostgreSQL: requests + outbox (same TX)
     |                                    |
     |                              Outbox relay
     |                                    v
     |                           Kafka / durable queue
     |                                    |
     v                              Routing workers
Status API <--- Status store <--- [Email | SMS | Push] adapters
                                         |
                                Provider APIs / webhooks
                                         |
                             Webhook ingest + signature verify
                                         |
                               Status event processor / audit
```

**Why the outbox?** If the API commits notification intent in PostgreSQL but crashes before publishing to Kafka, an ordinary dual write loses work. Insert `notification` and `outbox` in one DB transaction; relay retries publication. Consumer idempotency handles relay duplicates.

**Why channel adapters?** Providers have different rate limits, error classes, signatures, pricing and payload formats. Adapters isolate those details and allow routing/failover.

## 4. APIs and data model

```http
POST /v1/notifications
Authorization: Bearer ...
Idempotency-Key: tenant-unique-client-key

{"templateId":"order-shipped","recipientId":"u-42",
 "channels":["email","push"],"variables":{"orderId":"O-9"}}
```

Return `202 Accepted` with `notificationId` and status URL, not a false claim of provider delivery.

```text
tenant(tenant_id, plan, quotas, status)
template(tenant_id, template_id, version, channel, body_ref)
notification(tenant_id, notification_id, idempotency_key, recipient_id,
             template_version, state, created_at, payload_ref)
delivery_attempt(tenant_id, notification_id, channel, attempt_no,
                 provider_id, status, error_code, next_attempt_at)
outbox(event_id, aggregate_id, payload, created_at, published_at)
preference(tenant_id, recipient_id, channel, opt_in, updated_at)
UNIQUE (tenant_id, idempotency_key)
UNIQUE (tenant_id, notification_id, channel, attempt_no)
```

Keep sensitive recipient addresses encrypted or tokenized, with retention limits. Keep large payloads out of the hot relational table when justified.

## 5. State machine and delivery semantics

```text
ACCEPTED -> QUEUED -> SENDING -> SENT_TO_PROVIDER
                                |            |
                                v            v
                            RETRY_WAIT   DELIVERED / BOUNCED
                                |
                             FAILED / DLQ
```

A provider's `200 OK` means **accepted by provider**, not necessarily delivered to recipient. A webhook may arrive before an HTTP response; status updates need monotonic versioning or an explicit event/state reconciliation rule. Deduplicate provider webhook IDs. Treat uncertain outcomes carefully: querying provider status may be safer than blindly resending.

## 6. Scaling, fairness and resilience

Partition queue by tenant+recipient or notification ID according to ordering needs. Avoid one huge tenant monopolizing workers: weighted fair scheduling, per-tenant token buckets, channel/provider bulkheads and per-provider circuit breakers. Preserve tenant quotas in a shared store with atomic counters or durable usage accounting. Retry only transient failures with exponential backoff+jitter; dead-letter poison records with a replay tool and operator audit.

**Failure scenarios:**
- API crashes after DB commit: outbox relay publishes later.
- Queue delivers twice: worker deduplicates attempt/operation ID.
- Provider times out after accepting request: provider idempotency key or status reconciliation.
- Tenant sends 1M notifications at once: quota + per-tenant queues prevent noisy-neighbor overload.
- Webhook forged: verify signature, timestamp/replay window and provider identity.
- Region outage: define RPO/RTO; asynchronous cross-region failover can duplicate work, so globally unique IDs and reconciliation remain necessary.

## 7. Observability, security and tests

**Metrics:** accepted rate, queued age (p95/p99), provider error rate, time-to-delivery, retry amplification, DLQ depth, tenant-level quota rejections, spend, outbox lag. **Traces:** propagate notification ID across API/outbox/worker/provider; do not log PII or tokens. **Security:** tenant-scoped authorization, encryption, webhook signature verification, secrets rotation, consent and unsubscribe enforcement.

**Tests:** duplicate API idempotency key; crash between DB commit and queue publication; duplicate queue delivery; provider accepted but timeout; webhook before request completion; opt-out racing with send; tenant burst; provider outage; reprocessing DLQ record.

## 8. Trade-offs and interviewer-ready close

Kafka offers partitioned event streams and replay; SQS can reduce operational burden for independent work items. Exactly-once *recipient delivery* cannot generally be promised across external providers; deliver at least once internally and make business effects idempotent where supported. Favor PostgreSQL+outbox for accepted intent, a durable queue for delivery work, and separate status/audit paths.

**Spoken summary:** “I start by making accepted notification intent durable, then separate dispatch from provider delivery. I isolate tenants and providers with quotas and bulkheads, use idempotency and reconciliation for uncertain outcomes, and expose delivery states honestly.”

---

# LLD — Implement a thread-safe bounded blocking queue (45-minute Java interview)

## 1. Clarify the contract

Implement `put(T)` that blocks when full, `take()` that blocks when empty, and `close()` that wakes blocked threads. Non-null elements only. After close, no new puts; existing elements may drain; when drained, `take()` returns `null` as an end-of-stream marker. This is an **in-memory** queue, not a durable broker.

**Invariants:** `0 <= size <= capacity`; every enqueued item is removed at most once; no lost wake-ups; waiting happens in a loop; shutdown wakes both producer and consumer waiters.

## 2. Class model and sequence

```text
BoundedBlockingQueue<T>
  - ArrayDeque<T> deque
  - int capacity
  - ReentrantLock lock
  - Condition notEmpty
  - Condition notFull
  - boolean closed
  + put(T): void
  + take(): T | null
  + close(): void
  + size(): int

Producer: lock -> while full await(notFull) -> add -> signal(notEmpty) -> unlock
Consumer: lock -> while empty await(notEmpty) -> remove -> signal(notFull) -> unlock
Close:    lock -> closed=true -> signalAll both -> unlock
```

## 3. Complete runnable Java 17 code

```java
import java.util.*;
import java.util.concurrent.locks.*;

public final class BoundedBlockingQueue<T> {
    private final ArrayDeque<T> deque = new ArrayDeque<>();
    private final int capacity;
    private final ReentrantLock lock = new ReentrantLock();
    private final Condition notEmpty = lock.newCondition();
    private final Condition notFull = lock.newCondition();
    private boolean closed;

    public BoundedBlockingQueue(int capacity) {
        if (capacity <= 0) throw new IllegalArgumentException("capacity");
        this.capacity = capacity;
    }

    public void put(T item) throws InterruptedException {
        Objects.requireNonNull(item, "item");
        lock.lockInterruptibly();
        try {
            while (deque.size() == capacity && !closed) {
                notFull.await();
            }
            if (closed) throw new IllegalStateException("queue closed");
            deque.addLast(item);
            notEmpty.signal();
        } finally {
            lock.unlock();
        }
    }

    // Returns null only after close and complete drain.
    public T take() throws InterruptedException {
        lock.lockInterruptibly();
        try {
            while (deque.isEmpty() && !closed) {
                notEmpty.await();
            }
            if (deque.isEmpty()) return null;
            T item = deque.removeFirst();
            notFull.signal();
            return item;
        } finally {
            lock.unlock();
        }
    }

    public int size() {
        lock.lock();
        try { return deque.size(); }
        finally { lock.unlock(); }
    }

    public void close() {
        lock.lock();
        try {
            closed = true;
            notEmpty.signalAll();
            notFull.signalAll();
        } finally {
            lock.unlock();
        }
    }

    public static void main(String[] args) throws Exception {
        BoundedBlockingQueue<Integer> q = new BoundedBlockingQueue<>(2);
        Thread producer = new Thread(() -> {
            try {
                for (int i = 1; i <= 3; i++) q.put(i);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } finally {
                q.close();
            }
        });
        producer.start();
        Integer item;
        while ((item = q.take()) != null) System.out.println(item);
        producer.join();
    }
}
```

**Expected output:** 1, 2, 3 on separate lines. `ArrayDeque` is not thread-safe alone; all accesses are guarded by the same lock. `await()` atomically releases the lock while waiting and reacquires it before returning. The `while` loops defend against spurious wake-ups and changed conditions. `signalAll` on close prevents threads from waiting forever.

## 4. Complexity and tests

Each `put`/`take` is O(1) queue work, excluding time spent waiting. Space O(capacity). `close` is O(waiters) to signal blocked threads in practice.

**Test plan:** capacity=1 blocks second producer; a consumer unblocks producer; empty queue blocks consumer; close wakes both types of waiter; put after close throws; take after close drains then returns null; interruption while waiting releases locks; 10 producers/10 consumers deliver exactly N unique IDs without loss or duplicates; capacity=0 rejected; null element rejected.

**Trade-offs:** This queue is intentionally unfair by default; a fair `ReentrantLock(true)` can reduce starvation but may lower throughput. Java already provides `ArrayBlockingQueue`; implementing this from scratch is for interview reasoning, not a recommendation to replace battle-tested libraries. No persistence, cross-process sharing or priority scheduling.

## 5. Follow-up questions and answer

How would you add `offer(timeout)` and `poll(timeout)`? Use `awaitNanos` and a remaining-time loop. How would you implement metrics without racing? Expose size under lock and record waiting time. How does close differ from interrupt? Close is a queue lifecycle event; interruption is per-thread cancellation.

**Spoken answer:** “I maintain capacity and lifecycle invariants under one lock, use separate conditions for full and empty, and recheck predicates after every wake-up. Closing wakes all waiters and allows a deterministic drain.”

---

# Engineering developments — verified as of 14 October 2026

These are **official announcements**, not private social-feed discoveries. Some were published before today; their date is explicitly stated.

### N1. AWS Lambda Web Functions — 12–13 October 2026

AWS announced Lambda Web Functions on **12 October** and published a technical walkthrough on **13 October**. The new primitive runs standard **Node.js HTTP servers** without an adapter layer; the launch walkthrough specifies Node.js 24 on Arm64, 2 vCPUs and 2 GB per environment, revisions and HTTPS endpoints. This is **not** a general announcement that Spring Boot/Java servers run on this Web Functions primitive. **Why it matters:** serverless HTTP architecture, concurrency and billing assumptions differ from traditional Lambda event handlers. **Watch:** language/runtime expansion, CloudFront/IAM-auth design, concurrency isolation and operational pricing.

Sources: https://aws.amazon.com/blogs/aws/announcing-aws-lambda-web-functions/ and https://aws.amazon.com/blogs/compute/exploring-aws-lambda-web-functions-for-building-web-applications/

### N2. GitHub App least-privilege permissions — 13 October 2026

GitHub added fine-grained `repository_creation:write` and `installation_repositories:write` permissions and improvements for app installation management and rotating user tokens. **Why it matters:** automation and CI integrations can reduce reliance on broad repository-administration permissions or classic PATs. **Watch:** organization policy, installation scopes, token expiry and audit logging.

Source: https://github.blog/changelog/2026-10-13-github-app-improvements-for-least-privilege-and-management/

### N3. AWS Prime Day 2026 scale retrospective — 13 October 2026 roundup

AWS's **13 October** roundup cites Prime Day metrics: DynamoDB processed over 59 trillion requests across the event and peaked at 192 million requests/s; Lambda handled over 2.3 trillion invocations/day. These are **AWS-reported figures**, not independent performance benchmarks. **Why it matters:** illustrates large-scale partitioning, operational load testing and service quotas, not a guarantee that a single application can attain these numbers. **Watch:** design for hotspots, load-shedding, quotas and graceful degradation.

Source: https://aws.amazon.com/blogs/aws/aws-weekly-roundup-prime-day-2026-by-the-numbers-claude-haiku-5-5-amazon-nova-2-5-sonic-and-more-october-13-2026/

### N4. Spring patch cadence — published 21 September 2026; next train planned 22 October

Spring reported moving regular patches to a monthly synchronized release train, with the next regular patch train planned for **22 October 2026**. This is a **watch item**, not a claim that those patches have already shipped. **Why it matters:** patching and dependency BOM verification should be coordinated with vulnerability triage. **Watch:** actual release notes, affected components and regression testing.

Source: https://spring.io/blog/2026/09/21/releasing-spring-for-modern-challenges/

---

## Revision checklist (20 minutes)

- **4 min:** Explain bounded concurrency vs bounded threads; draw permit acquisition and release.
- **4 min:** Explain Kafka per-key ordering and the poison-message retry dilemma.
- **4 min:** Dry-run write skew and contrast Repeatable Read with Serializable.
- **4 min:** Implement sliding window and defend O(n).
- **4 min:** Explain HLD notification outbox + LLD condition-variable invariants.

## Source integrity

Questions, solutions, code, calculations and hypothetical system sizes are **original educational material**, not copied from interview posts or claimed to be asked by specific employers. Official source URLs appear beside applicable claims. No private LinkedIn access, employer interview transcript verification, or production execution of these Java examples is claimed. The Java snippets are intended for Java 17+; the complete standalone classes can be compiled individually with matching filenames.

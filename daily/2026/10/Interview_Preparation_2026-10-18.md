# Interview Preparation — 2026-10-18 (English)
**Audience:** Product-company SDE2 / Senior Backend Engineer · **10 original interview exercises** · Java 17+ · Sunday, 18 October 2026 (IST)

> **Evidence policy:** Questions below are original interview-style exercises, **not** attributed to named companies or claimed to be from private LinkedIn posts. Official API references and dated engineering announcements are linked. Research cutoff: 18 October 2026.

## Quick navigation
1. CompletableFuture timeout budgets; 2. Kafka poison-pill handling; 3. PostgreSQL SKIP LOCKED fairness; 4. DynamoDB conditional idempotency; 5. Multi-tenant JWT authorization; 6. ThreadLocal memory retention; 7. Top K frequent elements; 8. SQS FIFO ordering; 9. Retry storm and bulkheads; 10. Bounded concurrency with Semaphore.
- [HLD: Multi-Region Feature Flag Platform](../../../system-design/daily/2026-10-18-hld-feature-flags.md)
- [LLD: Concurrent Idempotency Coordinator](../../../system-design/daily/2026-10-18-lld-idempotency-coordinator.md)
- [Hinglish daily classroom edition](../../../daily/hinglish/2026/10/Interview_Preparation_2026-10-18.md)

## Q1. How would you enforce a 400 ms end-to-end deadline across three asynchronous dependencies? (Hard)
**Requirements:** Product page needs inventory, price and recommendations. Inventory and price are required; recommendations optional. Budget includes queueing, connection acquisition and network time, not merely method execution.

**Reasoning:** Launch independent I/O concurrently. Compute a monotonic absolute deadline, pass the *remaining* budget into each downstream call, and distinguish required from optional results. `CompletableFuture.orTimeout` times out the future but does **not** necessarily cancel the underlying HTTP request; configure actual client timeouts and cancellation/connection cleanup separately.

```java
record ProductView(String inventory, String price, String recommendations) {}
CompletableFuture<ProductView> aggregate(
    CompletableFuture<String> inventory,
    CompletableFuture<String> price,
    CompletableFuture<String> recommendations) {
  CompletableFuture<String> optional =
      recommendations.completeOnTimeout("unavailable", 250, TimeUnit.MILLISECONDS)
                     .exceptionally(ex -> "unavailable");
  return inventory.thenCombine(price, (i, p) -> new String[]{i, p})
      .thenCombine(optional, (core, r) -> new ProductView(core[0], core[1], r))
      .orTimeout(400, TimeUnit.MILLISECONDS);
}
```

**Important:** The snippet illustrates composition; a production deadline should be calculated at ingress and propagated to clients. If the optional future is shared elsewhere, `completeOnTimeout` mutates that same future—consider a dependent copy. Avoid accidentally blocking with `join()` inside a small executor.

**Complexity:** O(1) composition overhead, network latency dominated by slowest required dependency, not sum when truly concurrent. **Edge cases:** one required service fails, all services slow, queue saturation, request cancellation, stale fallback, partial data policy. **Follow-up:** How do you ensure timeouts do not leave expensive downstream work running? **Answer:** HTTP client cancellation and per-dependency deadline/connection controls.

**Reference:** https://docs.oracle.com/en/java/javase/17/docs/api/java.base/java/util/concurrent/CompletableFuture.html

## Q2. One Kafka message repeatedly crashes the consumer. How do you prevent a partition from stalling? (Hard)
**Problem:** An invalid schema payload at offset 45 fails every attempt; if the consumer never advances, later records on that partition remain blocked.

**Design:** Classify transient vs permanent failures. Transient errors get bounded retries with exponential backoff and jitter. Permanent deserialization/validation failures should go to a durable DLQ **with original topic/partition/offset, exception class, schema ID and correlation ID**; advance the source offset only after the DLQ write is durably acknowledged. Use a compatible error-handling deserializer so the framework can handle decode failures rather than crashing before the listener.

**Ordering trade-off:** Sending record 45 to DLQ and continuing record 46 relaxes strict end-to-end business ordering. If strict ordering is required, pause partition and alert/operator repair instead. Retry topics can also reorder; choose explicitly.

```java
// Illustrative Spring Kafka wiring (adapt to the installed Spring Kafka version):
DeadLetterPublishingRecoverer recoverer =
    new DeadLetterPublishingRecoverer(kafkaTemplate);
DefaultErrorHandler handler = new DefaultErrorHandler(
    recoverer, new FixedBackOff(1_000L, 3L));
handler.addNotRetryableExceptions(IllegalArgumentException.class);
```

**Correctness:** Configure DLQ producer acknowledgement, error handler commit behavior, transactional semantics if appropriate and replay idempotency. A DLQ is not a black hole: track age, rate, ownership and replay tooling. **Complexity:** O(1) per record excluding retries; retries multiply latency and load. **Follow-up:** How do you avoid replay double side effects? Stable business idempotency key plus durable unique constraint.

**Reference:** https://docs.spring.io/spring-kafka/reference/kafka/annotation-error-handling.html

## Q3. Implement a PostgreSQL work queue with SKIP LOCKED. Is it fair? (Hard)
**Problem:** Multiple workers claim pending jobs without processing the same job concurrently.

```sql
BEGIN;
WITH picked AS (
  SELECT id FROM jobs
  WHERE state = 'READY' AND available_at <= now()
  ORDER BY priority DESC, created_at, id
  FOR UPDATE SKIP LOCKED
  LIMIT 20
)
UPDATE jobs j
SET state='CLAIMED', claimed_at=now(), lease_until=now()+interval '2 minutes'
FROM picked WHERE j.id=picked.id
RETURNING j.*;
COMMIT;
```

**Why it works:** Row locks prevent two concurrent transactions from claiming the same rows; SKIP LOCKED avoids waiting behind a locked row. However it provides an **inconsistent view** and is appropriate for queue-like tables, not general financial reporting. Claim is only a lease: if worker crashes, a reaper must safely return expired claims to READY. For long jobs, heartbeat/extend lease. Use a monotonically increasing claim generation or fencing token so an expired worker cannot overwrite a newer owner's result.

**Fairness:** Not guaranteed. Hot high-priority jobs may starve low-priority jobs; skip-locked order is not global FIFO. Use aging/priority bands, metrics for oldest job and maximum wait, and limit batches.

**Indexes:** partial index on READY jobs ordered by priority/availability as supported by query plan; verify EXPLAIN. **Complexity:** Index-dependent, not simply O(1). **Follow-up:** What if worker completes external side effect but crashes before marking DONE? Durable idempotency/reconciliation, not just SKIP LOCKED.

**Reference:** https://www.postgresql.org/docs/current/sql-select.html

## Q4. DynamoDB conditional writes for idempotent order creation (Hard)
**Scenario:** API Gateway retries POST /orders because client saw a timeout. A duplicate must not create another order.

**Approach:** Partition key `TENANT#tenantId#IDEMP#key`, item with `requestHash`, status, orderId and TTL. Claim with conditional `attribute_not_exists(PK)`; if condition fails, strongly read existing item and compare request hash. Same hash + SUCCEEDED returns recorded order; mismatch returns 409; IN_PROGRESS returns 202/retry-after or waits boundedly. Coordinate order creation and idempotency claim atomically with `TransactWriteItems` **when both items are in DynamoDB and transaction limits permit**. If downstream external payment is involved, a DynamoDB transaction cannot atomically cover that payment.

```text
Put idempotency record IF absent
TransactWriteItems(idempotency SUCCESS, order row) with conditions
Return stable orderId for retries
```

**Pitfalls:** TTL deletion is asynchronous; do not assume exact expiry instant. Use tenant-scoped keys, deterministic request fingerprint, a durable result reference and an explicit retention contract. **Follow-up:** How to handle abandoned IN_PROGRESS? Lease/claim owner plus reconciliation, never blindly replay unknown external effects.

**Reference:** https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Expressions.ConditionExpressions.html

## Q5. Spring Security: preventing cross-tenant JWT authorization leaks (Hard)
**Threat:** A user from tenant A changes `/tenants/A/orders/1` to `/tenants/B/orders/1`. Valid JWT authentication alone does not prove authorization to tenant B.

**Design:** Validate issuer, audience, signature, expiry and algorithm allowlist via trusted JWT decoder. Derive tenant from trusted token claims or server-side membership, never trust arbitrary request header alone. Check tenant membership **and** object ownership at service/data-access boundary. Scope SQL queries by `tenant_id`; PostgreSQL RLS can add defense in depth but connection/session context must be set safely per transaction.

```java
// Example: ensure authenticated principal is allowed in requested tenant.
@PreAuthorize("@tenantAuthorizer.canRead(authentication, #tenantId)")
public OrderDto getOrder(UUID tenantId, UUID orderId) {
  return repo.findByTenantIdAndId(tenantId, orderId)
      .map(this::toDto)
      .orElseThrow(NotFoundException::new);
}
```

**Follow-up:** Is `@PreAuthorize` enough? No—self-invocation/proxy boundaries, alternate call paths, direct repository use and data-layer scoping still matter. Test tenant A reading tenant B's ID, stale memberships, service tokens, cache-key tenant leakage and token key rotation.

**Reference:** https://docs.spring.io/spring-security/reference/servlet/oauth2/resource-server/jwt.html

## Q6. Java ThreadLocal memory retention in a thread pool (Medium/Hard)
**Scenario:** A servlet filter stores `currentTenant` in `ThreadLocal` but forgets to clear it. Pooled worker thread is reused for another user, causing context leakage and memory retention.

```java
private static final ThreadLocal<String> TENANT = new ThreadLocal<>();
void handle(String tenant, Runnable work) {
  TENANT.set(tenant);
  try { work.run(); }
  finally { TENANT.remove(); }
}
```

**Why:** A pooled thread lives longer than a request; its ThreadLocalMap retains values until removed/expunged. Weak references to keys do not mean values disappear immediately. Never put huge request graphs in ThreadLocal; prefer explicit context passing where feasible. **Follow-up:** How to diagnose? Heap histogram/dump, retained paths, pool thread names and correlation of leak with request volume. **Complexity:** O(1) expected get/set/remove; retention cost proportional to leaked values and threads.

**Reference:** https://docs.oracle.com/en/java/javase/17/docs/api/java.base/java/lang/ThreadLocal.html

## Q7. DSA: Top K Frequent Elements with a bounded heap (Medium)
**Question:** Given `nums` and `k`, return any k elements with highest frequencies; order arbitrary. Use `O(n + m log k)` time where m is distinct count.

```java
static int[] topK(int[] nums, int k) {
  Map<Integer,Integer> count = new HashMap<>();
  for (int x : nums) count.merge(x, 1, Integer::sum);
  if (k < 0 || k > count.size()) throw new IllegalArgumentException();
  PriorityQueue<Integer> min = new PriorityQueue<>(
      (a,b) -> { int cmp = Integer.compare(count.get(a),count.get(b));
                 return cmp != 0 ? cmp : Integer.compare(a,b); });
  for (int x : count.keySet()) {
    min.offer(x);
    if (min.size() > k) min.poll();
  }
  int[] out = new int[k];
  for (int i=0; i<k; i++) out[i] = min.poll();
  return out;
}
```

**Dry run:** `[1,1,1,2,2,3], k=2` -> counts 1:3, 2:2, 3:1 -> {1,2}. **Complexity:** O(n + m log(k+1)), O(m+k) memory. **Edge cases:** k=0, negatives, equal frequency, huge n. **Follow-up:** Could you do O(n)? Bucket by frequency using O(n) extra space. **Interview tip:** Comparator uses `Integer.compare` instead of subtraction to avoid overflow.

## Q8. AWS SQS FIFO: does it guarantee exactly-once order processing? (Hard)
**Answer:** No blanket exactly-once business-effect guarantee. FIFO queues support ordering within a `MessageGroupId` and a deduplication interval, but consumer redelivery can still occur, e.g. visibility timeout expiry or failed delete. Put all events for one order in one group to preserve that order's sequence; distribute different orders across groups for concurrency.

**Flow:** receive -> idempotent DB state transition -> delete message only after durable success. Visibility timeout should cover typical processing plus extension heartbeat. DLQ redrive needs care: moving one failed message can permit later messages to progress and violate business-level sequence assumptions. Use durable business IDs and conditional version transitions.

**Follow-up:** One hot customer dominates group? Group serializes processing; choose a group key matching actual ordering requirement, not unnecessarily broad tenant ID.

**Reference:** https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/FIFO-queues.html

## Q9. A dependency is failing. Why can retries take down the healthy services? (Hard)
**Problem:** 1,000 incoming requests/s × 3 retries can create up to 4,000 attempts/s against a struggling dependency. Each attempt holds a thread, socket or connection and worsens queueing. Retry storms can cascade.

**Design:** Retry only transient, idempotent operations; cap attempts; use exponential backoff with jitter, a **global deadline**, circuit breaker and concurrency bulkhead. Put a bounded queue in front of downstream work, reject fast under overload, expose retry budget. For payment writes use an idempotency key and never blindly replay an unknown outcome.

```java
Semaphore bulkhead = new Semaphore(40);
if (!bulkhead.tryAcquire()) throw new RejectedExecutionException("busy");
try {
    return downstreamCall(); // client timeout and deadline still required
} finally {
    bulkhead.release();
}
```

**Follow-up:** Why is a circuit breaker not a rate limiter? Circuit breaker reacts to failure state; rate limiter caps traffic; bulkhead limits concurrent work. They solve different problems. **Metrics:** in-flight, rejection rate, attempts per logical request, dependency p99, timeout rate.

## Q10. Java 17: bound concurrent external calls without blocking all virtual threads (Hard)
**Scenario:** Service receives 5,000 concurrent requests but downstream DB pool has 30 connections. More threads cannot create more DB capacity.

**Approach:** Gate access with a semaphore sized from measured downstream concurrency, set bounded acquisition timeout and fail/degrade gracefully. This is relevant to both platform and virtual threads.

```java
final Semaphore permits = new Semaphore(30);
String fetch() throws Exception {
  if (!permits.tryAcquire(80, TimeUnit.MILLISECONDS))
      throw new TimeoutException("dependency saturated");
  try { return blockingHttpCall(); }
  finally { permits.release(); }
}
```

**Little's Law intuition:** concurrency ≈ throughput × latency. At 300 req/s and 100 ms dependency time, average 30 concurrent calls. This is a sizing estimate, not a substitute for peak-tail measurements. **Edge cases:** fairness, cancellation, permit leaks, deadline propagation, priority inversion. **Follow-up:** Should you create a fixed virtual-thread pool? Usually virtual threads are created per task, while scarce resources are limited with semaphores/pools.

**Reference:** https://docs.oracle.com/en/java/javase/17/docs/api/java.base/java/util/concurrent/Semaphore.html

## Today's HLD — Multi-Region Feature Flag Platform
**Full 45-minute walkthrough:** [English HLD](../../../system-design/daily/2026-10-18-hld-feature-flags.md) · [Hinglish HLD](../../../system-design/hinglish/daily/2026-10-18-hld-feature-flags.md)

**Interview path:** requirements (200k SDKs, 2M evaluations/s assumed) -> control plane vs local SDK data plane -> revisioned PostgreSQL + outbox -> signed snapshots/CDN -> deterministic rollout hash -> eventual propagation, kill-switch limitations -> SLOs and chaos tests. Explain why a remote lookup per evaluation creates a dependency bottleneck.

## Today's LLD — Concurrent Idempotency Coordinator
**Full Java 17 runnable implementation and tests:** [English LLD](../../../system-design/daily/2026-10-18-lld-idempotency-coordinator.md) · [Hinglish LLD](../../../system-design/hinglish/daily/2026-10-18-lld-idempotency-coordinator.md)

**Interview path:** same-key concurrent callers -> atomic `putIfAbsent` owner -> shared `CompletableFuture` -> request fingerprint -> completed-entry TTL -> conditional eviction -> crash/durable payment caveat. The example compiled using `javac --release 17` and passed a 20-caller concurrency smoke test; do not confuse this with a full proof of correctness.

## Engineering developments worth reading (verified official announcements)
1. **16 October 2026 — GitHub open-source license compliance GA.** Enterprise dependency governance can now enforce centralized license policies. For backend teams, evaluate transitive license policy in CI and review dependency exceptions; this is a governance capability, not a claim that it fixes vulnerabilities. Official: https://github.blog/changelog/month/10-2026/
2. **14 October 2026 — GitHub Actions workflow lockfiles public preview.** Workflow dependency pinning helps reduce mutable tag supply-chain risk; assess rollout and support status before changing CI. Official: https://github.blog/changelog/month/10-2026/
3. **13 October 2026 — AWS Lambda Web Functions walkthrough.** AWS describes running standard Node.js web servers on Lambda with multi-concurrent execution and an HTTPS endpoint. Evaluate cold starts, concurrency model, billing and operational fit before migrating Spring/Java services. Official: https://aws.amazon.com/blogs/compute/
4. **22 October 2026 — upcoming Spring coordinated patch train.** Spring announced a regular patch train for that date, **not yet released as of this report**. Track dependency CVEs and test upgrades. Official: https://spring.io/blog/2026/09/21/releasing-spring-for-modern-challenges/

**Confidence:** News dates and capabilities from official announcements; the exercises, architecture assumptions and solutions are original educational material, not leaked interview questions. No private LinkedIn access or posting.

## 20-minute revision plan
5 min: derive Kafka poison-pill DLQ vs strict ordering. 5 min: walk through SKIP LOCKED lease failure. 5 min: dry-run Top K heap. 5 min: speak feature-flag architecture and idempotency race aloud. For every question, state correctness invariant, bottleneck, failure mode and trade-off.

# Daily Software Engineering Interview Preparation — 2026-10-21

**Audience:** Product-company SDE2 / Senior Backend Engineer. These are representative practice questions, not claims that a named company asked these exact questions.

**System design:** [HLD — Payment Reconciliation Platform](../../../system-design/daily/2026-10-21-hld-payment-reconciliation-platform.md) · [LLD — Bounded Async Bulkhead Executor](../../../system-design/daily/2026-10-21-lld-bounded-bulkhead-executor.md)

## Q1. CompletableFuture timed out. Did the HTTP request stop?

A service fans out to three downstream APIs. One future times out after 200 ms, but the remote operation may still be running. Timeout completion and transport cancellation are different things.

~~~java
CompletableFuture<Response> call =
    CompletableFuture.supplyAsync(() -> client.fetch(), ioExecutor)
        .orTimeout(200, TimeUnit.MILLISECONDS)
        .exceptionally(ex -> fallbackResponse());
~~~

Configure connect/request timeouts in the actual HTTP client and propagate one end-to-end deadline budget. Keep blocking I/O off the common pool and use bounded executors. Cancellation is best-effort; side effects still require idempotency. Be careful: orTimeout completes the future exceptionally, but does not by itself prove that the underlying operation or remote server stopped.

**Follow-up:** If a timed-out payment call might have succeeded, query status using the stable operation ID before retrying.

**Interview answer:** “I propagate a deadline, configure timeouts at the transport layer, and treat timeout as an unknown outcome for side-effecting operations.”

## Q2. Kafka preserves order, so why did my events arrive out of order?

Kafka guarantees order **within a partition**, not across all partitions. If events for the same order are sent with different keys, they can land in different partitions and be processed concurrently.

~~~java
ProducerRecord<String, OrderEvent> record =
    new ProducerRecord<>("order-events", event.orderId(), event);
producer.send(record);
~~~

Use a stable key matching the required ordering boundary, such as orderId. Do not use a random key if per-order order matters. A consumer rebalance can also cause replay: the old consumer may have completed a side effect but not committed its offset. Make handlers idempotent and commit offsets only after durable processing.

**Trade-off:** A single key/partition preserves that key's order but a very hot key can bottleneck. Global ordering requires much stronger serialization and usually sacrifices throughput.

**Interview answer:** “I define the ordering scope first, key records by that entity, and assume replay is possible even when partition order is preserved.”

## Q3. PostgreSQL worker queue: why SKIP LOCKED is not enough

Multiple workers should claim different jobs without waiting on rows already claimed. A lease allows recovery when a worker dies.

~~~sql
WITH candidate AS (
  SELECT id
  FROM job
  WHERE status = 'READY'
     OR (status = 'RUNNING' AND lease_until < now())
  ORDER BY priority DESC, created_at, id
  FOR UPDATE SKIP LOCKED
  LIMIT 1
)
UPDATE job j
SET status = 'RUNNING',
    lease_owner = :worker_id,
    lease_until = now() + interval '45 seconds',
    attempts = attempts + 1,
    fencing_token = fencing_token + 1
FROM candidate c
WHERE j.id = c.id
RETURNING j.*;
~~~

Run the claim/update in one transaction. Add an index aligned with the real predicate/order and verify using EXPLAIN ANALYZE. SKIP LOCKED prevents workers from claiming the same currently locked row; it does not stop a paused worker from waking after its lease expires.

**Critical twist:** A lease is not fencing. Every completion/write should carry the claim's fencing token, and the database should reject stale tokens. External side effects must be idempotent too.

**Interview answer:** “I atomically claim with SKIP LOCKED, recover with leases, and fence stale workers so an expired owner cannot overwrite newer work.”

## Q4. Design an idempotent POST API when the same key arrives with a different payload

Example: client sends a transfer with key K and amount 100. It times out, then retries key K with amount 900 due to a client bug. Returning the old result without checking payload hides a serious mismatch.

Store a request fingerprint with the idempotency key and scope it to tenant/customer + operation. Enforce uniqueness in the database; application-level check-then-insert races under concurrency.

~~~sql
CREATE TABLE idempotency_record (
  tenant_id text NOT NULL,
  idempotency_key text NOT NULL,
  request_hash bytea NOT NULL,
  status text NOT NULL,
  response_json jsonb,
  created_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (tenant_id, idempotency_key)
);
~~~

Within one transaction, reserve the key, apply the business mutation and persist the final response (or durable operation ID). Same key + same hash returns the existing outcome; same key + different hash returns a conflict. Define retention longer than the client's retry horizon. For long-running workflows, persist IN_PROGRESS and recover stale work with a lease.

**Interview answer:** “Idempotency requires a durable unique key plus payload equivalence and a transactional business boundary—not merely caching the last HTTP response.”

## Q5. Why can @TransactionalEventListener still lose an event?

An AFTER_COMMIT listener avoids running before the transaction commits, but it is not a durable message queue. The process can crash after database commit and before the listener successfully publishes to Kafka.

For business-critical events, write an outbox row in the same DB transaction as the business change. A separate publisher retries outbox delivery; consumers deduplicate by event ID. Expect at-least-once publication.

Use AFTER_COMMIT listeners for in-process follow-up work where loss is acceptable or another durable mechanism exists. Do not assume a listener annotation makes the database and Kafka atomic.

**Follow-up:** A publisher can crash after send but before marking the outbox row published. The event may be sent again, so the consumer still needs idempotency.

**Interview answer:** “For critical integration events I use a transactional outbox; AFTER_COMMIT alone provides timing semantics, not durable delivery.”

## Q6. Production JVM memory grows: leak or allocation pressure?

Start with evidence, not a larger heap. Compare heap after full GC across equivalent load windows. A rising post-GC live set suggests retention; high allocation with a stable live set suggests allocation pressure.

Workflow:
1. Correlate heap, allocation rate, GC pause, CPU, request rate and deploy changes.
2. Capture JFR during representative load; inspect allocation hot spots and lock contention.
3. If retention is suspected, compare heap dumps and dominator trees; find the GC roots retaining large object graphs.
4. Inspect caches, unbounded maps, ThreadLocal values, listeners, queues and static collections.
5. Reproduce with realistic traffic and verify the fix with post-GC live-set and latency.

Do not dump heap blindly on a latency-sensitive node: the operation can pause or consume significant disk. Use a replica/canary where possible and protect dump files because they may contain secrets or personal data.

**Interview answer:** “I distinguish allocation rate from retained live set, use JFR and heap-dump dominators, then validate the suspected retaining reference under load.”

## Q7. DSA: longest substring without repeating characters

Use a sliding window. Maintain the last index of each character. When a repeated character falls inside the current window, move the left boundary just past its previous occurrence.

~~~java
import java.util.*;

static int longestUniqueSubstring(String s) {
    Map<Character, Integer> last = new HashMap<>();
    int left = 0, best = 0;
    for (int right = 0; right < s.length(); right++) {
        char c = s.charAt(right);
        Integer previous = last.put(c, right);
        if (previous != null && previous >= left) {
            left = previous + 1;
        }
        best = Math.max(best, right - left + 1);
    }
    return best;
}
~~~

For “abba”, when the second “b” arrives, left jumps to index 2; it must not move backwards when the final “a” is seen. Each character is processed once: expected O(n) time and O(min(n, alphabet size)) space.

**Edge cases:** empty string returns 0; all identical characters returns 1. Java char iterates UTF-16 code units, not Unicode code points. If the contract means user-perceived characters, grapheme-cluster handling is a different problem.

## Q8. Local token bucket versus distributed rate limiter

A token bucket has capacity C and refill rate R tokens/second. For elapsed time dt, refill with min(C, tokens + dt*R); allow the request only if at least one token remains, then deduct one.

A local bucket is fast but each application replica gets its own allowance, so scaling from one to ten replicas can multiply the effective quota. For a global tenant quota, use a shared atomic operation (for example a Redis Lua script) or a centralized quota service. Read-modify-write as separate Redis calls is racy.

**Trade-offs:** Shared coordination adds latency and an availability dependency. Decide fail-open versus fail-closed per endpoint risk, and use a bounded local fallback only with an explicitly accepted overshoot. Key by authenticated tenant identity, not an untrusted request header.

**Interview answer:** “I select the quota scope first, implement refill-and-deduct atomically, and make the consistency/availability trade-off explicit.”

## Q9. Cache stampede: why does adding Redis not fix it?

A popular key expires; thousands of requests miss simultaneously and all hit the database. Redis stores the value, but expiry can synchronize misses.

Options:
- request coalescing/single-flight so one caller refreshes and others await it;
- TTL jitter to spread expirations;
- stale-while-revalidate when stale data is acceptable;
- short negative caching for known absence;
- bounded refresh concurrency and a database fallback budget.

Do not hold a distributed lock indefinitely. Use a lease with owner token and compare-and-delete; still protect the database because locks can expire while work continues. Cache-aside invalidation also needs a clear consistency contract.

**Interview answer:** “I combine expiry jitter, per-key request coalescing and bounded refresh; stale serving is only enabled where the product's freshness contract allows it.”

## Engineering updates — 21 October 2026

1. **Oracle October 2026 Critical Patch Update (20 Oct):** Oracle published the quarterly security update covering Java SE and other product families. Check the exact runtime/vendor build and patch supported production JDKs through the normal rollout process. Do not infer a build number from a generic announcement. [Oracle CPU announcement](https://blogs.oracle.com/security/october-2026-critical-patch-update-released) · [Oracle Java CPU mapping](https://docs.oracle.com/en-us/iaas/releasenotes/java-management/jdk-cpu-october-2026.htm)
2. **GitHub package firewall public preview (20 Oct):** A supply-chain control aimed at package-installation-time malware. For backend teams, combine registry controls with lockfiles, dependency review, secret isolation and reproducible builds; preview availability/coverage may vary. [GitHub Changelog](https://github.blog/changelog/month/10-2026/)
3. **AWS Lambda Web Functions (12 Oct):** Node.js HTTP workloads can handle requests directly with native HTTP streaming and multiple concurrent requests per warm environment. It is currently Node.js-specific in the announcement; do not assume a Java runtime is supported. [AWS announcement](https://aws.amazon.com/about-aws/whats-new/2026/10/aws-lambda-web-functions/)
4. **Kafka 4.2.2 (29 Sep):** Bug-fix release; review the upstream upgrade notes and your vendor's support matrix before planning an upgrade. [Apache Kafka announcement](https://kafka.staged.apache.org/blog/2026/09/29/apache-kafka-4.2.2-release-announcement/)

## Revision checklist

- Explain why timeout is not cancellation.
- State the ordering scope before choosing a Kafka key.
- Describe lease expiry and fencing separately.
- Explain same idempotency key + different payload behavior.
- Compare outbox with AFTER_COMMIT listeners.
- Distinguish allocation pressure from memory retention.
- Solve the sliding window aloud and discuss Unicode semantics.
- Explain local quota overshoot across replicas.
- Design a stampede strategy that respects freshness requirements.

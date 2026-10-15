# Interview Preparation — 2026-10-15

Product-company SDE2/Senior Java Backend interview practice. Original questions, no employer attribution.

## Contents

Nine original interview exercises (not employer-attributed). **Continue reading:** [Q3–Q9 deep dives](Interview_Preparation_2026-10-15_Deep_Dive.md), [comprehensive HLD](../../../system-design/daily/2026-10-15-hld-flash-sale-inventory.md), and [Hinglish edition](../../hinglish/2026/10/Interview_Preparation_2026-10-15.md).

## Q1. CompletableFuture: 250 ms end-to-end deadline (Hard)

**Scenario:** Profile, points and recommendations are fetched from three services. Profile is required, other fields optional. A single slow dependency must not violate a 250 ms response target.

**Derivation:** Record a monotonic deadline at ingress. Launch independent calls concurrently, but limit simultaneous work. Wait for mandatory profile only within the remaining budget. Optional services receive fallbacks; the HTTP clients must enforce network timeouts. A request-level deadline must also cover serialization and queuing.

```java
import java.util.concurrent.*;
class Aggregator {
    private final ExecutorService pool = Executors.newFixedThreadPool(16);
    record Result(String profile, String points) {}
    Result fetch() throws Exception {
        long end = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(250);
        var profile = CompletableFuture.supplyAsync(() -> "profile", pool);
        var points = CompletableFuture.supplyAsync(() -> "gold", pool)
            .completeOnTimeout("unknown", 100, TimeUnit.MILLISECONDS)
            .exceptionally(e -> "unknown");
        try {
            String p = profile.get(Math.max(1, end-System.nanoTime()),
                                   TimeUnit.NANOSECONDS);
            return new Result(p, points.getNow("unknown"));
        } finally { profile.cancel(true); points.cancel(true); }
    }
}
```

**Important:** CompletableFuture cancellation does not reliably stop underlying HTTP I/O. A fixed thread pool has an unbounded task queue; production needs bounded admission, HTTP deadlines and bulkheads. The code illustrates coordination, not a full SLA-enforcing server.

**Dry run:** profile 70 ms, points 600 ms: return profile with fallback. **Complexity:** O(1) orchestration for fixed fan-out; downstream capacity dominates. **Follow-up:** What if profile fails? Fail explicitly or use approved stale data. What about virtual threads? They do not create database capacity.

**Interview answer:** “I budget a single end-to-end deadline, isolate downstream concurrency, degrade optional dependencies and enforce client-side timeouts.”

Source: https://docs.oracle.com/en/java/javase/25/docs/api/java.base/java/util/concurrent/CompletableFuture.html

## Q2. PostgreSQL job queue with SKIP LOCKED

Claim jobs in a short transaction using FOR UPDATE SKIP LOCKED. Store a lease deadline and claim generation. After a crash, another worker can reclaim the expired job. External side effects must remain idempotent.

**SQL claim:** `SELECT id FROM jobs WHERE status='READY' FOR UPDATE SKIP LOCKED LIMIT 10` inside a transaction, followed by `UPDATE jobs SET status='RUNNING', lease_until=now()+interval '2 minutes', attempt=attempt+1 WHERE id IN (...)`. Commit immediately. Use `WHERE attempt=:claimedAttempt` for completion. Retries require backoff and maximum attempts; strict exactly-once side effects require durable idempotency, not just a claim lock.

**Queue observability:** Track ready-job age, expired leases, attempts, failures and processing latency. Retry transient errors with jitter and cap the attempt count. A PostgreSQL queue is simple at moderate scale but can suffer row churn and bloat at very high volume.

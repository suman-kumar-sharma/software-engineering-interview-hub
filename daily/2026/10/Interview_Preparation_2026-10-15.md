# Interview Preparation — 2026-10-15

Product-company SDE2/Senior Java Backend interview practice. Original questions, no employer attribution.

## Contents

Nine original SDE2/Senior questions: CompletableFuture deadlines; PostgreSQL job claims; Kafka rebalances; Spring transaction propagation; Redis fencing; shortest subarray with negatives; RDS restore latency; Java safe publication; refresh-token rotation. Includes one HLD and one LLD. These are original interview-style exercises, not verified employer questions.

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

# Advanced Java Backend Practice — 2026-10-25
## Java Concurrency and the JMM: Visibility Is Not Atomicity

**Runtime:** Java 21  
**Time budget:** 15 minutes — 3-minute concept, 8-minute exercise, 4-minute walkthrough  
**Scenario:** An order API counts successful requests under heavy concurrency.

## Prerequisites
- Basic Java threads and `volatile`
- `AtomicLong` and `LongAdder`
- Compound operations: read, modify, write

## 1. Concept question — 3 minutes

### Question
A Spring service counts completed orders:

```java
private volatile long completedOrders;

void recordSuccess() {
    completedOrders++;
}
```

The field is `volatile`. Is the count guaranteed to be correct when hundreds of request threads call `recordSuccess()` concurrently?

### Worked answer
**No. `volatile` gives visibility and ordering guarantees for individual volatile reads and writes; it does not make `completedOrders++` atomic.**

An increment is conceptually three steps:
1. Read the current value.
2. Add one locally.
3. Write the result.

Two threads can read 10 at the same time, both calculate 11, and both write 11. Two successful requests happened, but the counter advanced only once. This is a **lost update**.

`volatile` is useful for a shared flag or publishing a single value when the rest of the protocol is correct. It is not a substitute for an atomic read-modify-write operation.

The Java Memory Model defines happens-before relationships. A volatile write happens-before a subsequent read of that same field, but that guarantee does not combine several operations into one transaction. See JLS `17.4.5: https://docs.oracle.com/javase/specs/jls/se21/html/jls-17.html#jls-17.4.5

### Choose the counter by its purpose
- **Exact atomic count / sequence within one JVM:** `AtomicLong`.
- **High-contention telemetry/statistics:** often `LongAdder`, where a concurrently changing snapshot need not be perfectly atomic.
- **A global business invariant across replicas:** neither local class is sufficient by itself; use a database conditional update, a distributed atomic primitive, or another authoritative coordination mechanism.

**Hinglish summary:** `volatile` se visibility improve hoti hai, lekin `count++` ke read + add + write steps ek saath atomic nahi bante. Visibility aur atomicity alag problems hain.

## 2. Practical exercise with complete solution — 8 minutes

### Problem
A service receives concurrent order-completion events. Build a small harness comparing:
1. A broken `volatile long` counter.
2. An exact per-JVM `AtomicLong` counter.
3. A high-throughput `LongAdder` metric.

Use 32 worker threads, each recording 100,000 events. After all workers finish, explain which values should equal the expected total and why.

### Derive the design
- **Demonstrate the race:** keep the volatile increment intentionally broken; never use it for production accounting.
- **Exact atomic increment:** use `AtomicLong.incrementAndGet()`.
- **Scalable metrics:** use `LongAdder.increment()`. It can spread updates across internal variables under contention, improving throughput at the cost of more space. Its `sum()` is not an atomic snapshot while updates are happening.
- **Trustworthy final comparison:** wait for every task to finish before reading final totals. The harness uses `Future.get()`; after all tasks complete, there are no concurrent updates in this example.

### Complete runnable Java 21 example

Save as `CounterComparison.java`:

```java
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;

public class CounterComparison {

    private static final int THREADS = 32;
    private static final int EVENTS_PER_THREAD = 100_000;

    // Intentionally incorrect for concurrent increments.
    private volatile long brokenCount;

    // Atomic update; exact for this counter within this JVM.
    private final AtomicLong exactCount = new AtomicLong();

    // High-contention statistics counter.
    private final LongAdder metricCount = new LongAdder();

    private void recordSuccess() {
        brokenCount++;              // read-modify-write race
        exactCount.incrementAndGet();
        metricCount.increment();
    }

    public static void main(String[] args) throws Exception {
        CounterComparison demo = new CounterComparison();
        ExecutorService pool = Executors.newFixedThreadPool(THREADS);
        List<Future<?>> futures = new ArrayList<>();

        for (int t = 0; t < THREADS; t++) {
            futures.add(pool.submit(() -> {
                for (int i = 0; i < EVENTS_PER_THREAD; i++) {
                    demo.recordSuccess();
                }
            }));
        }

        try {
            // Propagate task failures and wait for every worker.
            for (Future<?> future : futures) {
                future.get();
            }
        } finally {
            pool.shutdown();
        }

        long expected = (long) THREADS * EVENTS_PER_THREAD;

        System.out.println("Expected:     " + expected);
        System.out.println("volatile++:   " + demo.brokenCount);
        System.out.println("AtomicLong:   " + demo.exactCount.get());
        System.out.println("LongAdder:    " + demo.metricCount.sum());
    }
}
```

Expected total: **3,200,000**.
- `AtomicLong` should report 3,200,000.
- `LongAdder.sum()` should report 3,200,000 because all workers have finished before the read.
- The volatile counter is not guaranteed to report the expected total. Its exact observed value varies by execution and hardware; do not assume it must be lower in every imaginable test run.

The fixed pool bounds the worker count. The concurrency lesson is about the shared counter, not about choosing an executor for a web server.

### Production choice
For request-rate dashboards, successful/failed operation counts, or other telemetry, `LongAdder` is often a good fit. A monitoring scrape can observe a value while updates are happening, so that snapshot may not include every concurrent increment.

For an in-process sequence that requires each thread to obtain a distinct number, `AtomicLong.getAndIncrement()` can be appropriate. It is not a durable sequence across restarts or multiple service replicas.

For a quota such as “never charge more than 100 credits across all instances,” do not rely on a local `AtomicLong`: each replica has separate memory. Enforce the invariant in the authoritative shared system, for example with a conditional database update in a transaction.

## 3. Walkthrough, mistakes and trade-offs — 4 minutes

### What happens in the race?
Suppose the counter is 10:

| Step | Thread A | Thread B |
|---|---|---|
| 1 | Reads 10 | |
| 2 | | Reads 10 |
| 3 | Calculates 11 | Calculates 11 |
| 4 | Writes 11 | |
| 5 | | Writes 11 |

Two increments were attempted, but the final value is 11 rather than 12.

With an atomic increment, the update is indivisible relative to other atomic updates to that variable; the two calls produce 11 and 12.

### Common mistakes
1. **“Volatile makes it thread-safe.”** It does not make compound operations atomic.
2. **Using `LongAdder` for a hard limit.** Its `sum()` is not an atomic snapshot during concurrent updates, so it is unsuitable as the sole mechanism for admission control or enforcing an exact quota.
3. **Assuming `AtomicLong` is distributed.** It coordinates threads sharing that one object, not separate JVMs.
4. **Reading before workers finish.** The metric may legitimately still be increasing. The exercise waits on all futures first.
5. **Using a metric as the source of truth.** Metrics can be reset, sampled, or lost on restart. Business accounting belongs in a durable authoritative system.
6. **Ignoring contention costs.** A single `AtomicLong` can become a hot cache line under extreme write contention. `LongAdder` can improve statistical-counter throughput, but uses more memory and provides weaker snapshot semantics.

### Interview-ready answer
> A volatile field provides visibility and ordering for individual reads and writes, but increment is a compound read-modify-write operation and can lose updates. I use AtomicLong when each update must be atomic within a JVM, and LongAdder for highly contended telemetry where a perfectly atomic live snapshot is unnecessary. If the invariant spans service replicas or must survive restarts, I move enforcement to a durable shared system such as a transactional database conditional update.

### Follow-up questions with answers

**Q1. Does `AtomicLong` guarantee a unique sequence across two application instances?**  
No. Each JVM has its own object and sequence. Use a shared sequence service or database-generated identifier if global uniqueness is required.

**Q2. Is `LongAdder.sum()` always wrong?**  
No. It is useful for statistics. The API says the returned sum is not an atomic snapshot during concurrent updates. Once updates stop, reading the sum gives the accumulated total.

**Q3. Can I replace `volatile long` with `synchronized`?**  
Yes, a synchronized increment method can make the compound operation mutually exclusive, provided every access follows the same locking discipline. It may serialize a hot counter; atomics or LongAdder may better fit the requirement.

**Q4. Does a correct in-memory counter make order completion durable?**  
No. It only counts events observed by this process. Persist order state transactionally and design event delivery/idempotency separately.

## Primary references
- Java SE 21 JLS, `17.4.5 — happens-before: https://docs.oracle.com/javase/specs/jls/se21/html/jls-17.html#jls-17.4.5
- Java SE 21 `AtomicLong` API: https://docs.oracle.com/en/java/javase/21/docs/api/java.base/java/util/concurrent/atomic/AtomicLong.html
- Java SE 21 `LongAdder` API: https://docs.oracle.com/en/java/javase/21/docs/api/java.base/java/util/concurrent/atomic/LongAdder.html
- Java SE 21 concurrency package: https://docs.oracle.com/en/java/javase/21/docs/api/java.base/java/util/concurrent/package-summary.html

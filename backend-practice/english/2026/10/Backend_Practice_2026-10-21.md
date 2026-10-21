# Advanced Java Backend Practice — 2026-10-21
## Java 21 virtual threads: bound database concurrency, not threads

**Audience:** Experienced Java/Spring/AWS backend developer  
**Runtime:** Java 21 (JEP 444)  
**Plan:** 3-minute concept · 8-minute exercise · 4-minute review

## 1. Concept check (3 minutes)

**Question.** An order API gets 8,000 concurrent requests. The team uses `Executors.newVirtualThreadPerTaskExecutor()`, but HikariCP is capped at 32 connections and pool wait/latency keeps rising. Do virtual threads solve the bottleneck?

**Answer.** No. Virtual threads make it cheaper to represent many tasks that spend time waiting; they do not increase database query capacity, connection count, CPU, memory, or downstream quotas. Java 21's virtual-thread-per-task executor creates an unbounded number of virtual threads. Do not pool virtual threads to enforce a resource limit. Instead, use explicit admission control for the scarce resource, plus the connection pool's own limits and timeouts.

Example: if HikariCP allows 32 connections, a semaphore with 24 permits can cap this repository's concurrent DB work and leave planned headroom for other paths. That only works if other consumers and replica count are accounted for: a semaphore is local to one JVM, not a cluster-wide quota.

**Hinglish intuition:** Bahut saare waiting customers ko handle kar paana kitchen ki cooking capacity nahi badhata. Database ko ek time par kitne operations dene hain, uska limit alag se set karo.

Primary references: [JEP 444: Virtual Threads](https://openjdk.org/jeps/444), [Java 21 Executors API](https://docs.oracle.com/en/java/javase/21/docs/api/java.base/java/util/concurrent/Executors.html), [Java 21 Semaphore API](https://docs.oracle.com/en/java/javase/21/docs/api/java.base/java/util/concurrent/Semaphore.html).

## 2. Practical exercise + complete solution (8 minutes)

### Requirements
- Java 21; blocking JDBC is acceptable.
- HikariCP maximum pool size = 32.
- This repository may run at most 24 DB operations concurrently.
- Wait at most 75 ms for a permit, then reject as overloaded (map to HTTP 503; use 429 only if it represents a caller quota).
- Use a one-second statement timeout and configure a finite pool connection-acquisition timeout.
- Release the permit on every success/failure path.
- Never hold the permit while doing unrelated remote calls or CPU-heavy work.

### Derive the design
1. **Scarce resource:** database/query capacity, not virtual threads.
2. **Acquire order:** get the permit before obtaining a connection; otherwise a connection can be wasted while waiting for admission.
3. **Bounded wait:** timed `tryAcquire` prevents an unlimited queue of parked requests.
4. **Guaranteed release:** `finally` covers SQL exceptions, mapping errors, and resource-close failures.
5. **Independent safeguards:** semaphore limits this code path, Hikari limits connections, and pool/query timeouts limit different waits.
6. **Correct semantics:** overload is not “order not found”; don't return an empty result for rejected work.

### Minimal repository implementation

Wire the `DataSource` to a configured HikariCP bean in Spring. Table/column names are illustrative.

```java
import javax.sql.DataSource;
import java.sql.*;
import java.util.Optional;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

public final class OrderRepository {
    private static final long ADMISSION_WAIT_MS = 75;
    private static final int QUERY_TIMEOUT_SECONDS = 1;

    // Example: pool max 32; reserve headroom for other consumers.
    private final Semaphore dbSlots = new Semaphore(24);
    private final DataSource dataSource;

    public OrderRepository(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    public Optional<Order> findById(long orderId)
            throws SQLException, InterruptedException {
        // If interrupted here, no permit was acquired.
        if (!dbSlots.tryAcquire(ADMISSION_WAIT_MS, TimeUnit.MILLISECONDS)) {
            throw new DependencyOverloadedException(
                "Database concurrency limit reached");
        }

        try (Connection c = dataSource.getConnection();
             PreparedStatement ps = c.prepareStatement(
                 "SELECT id, status FROM orders WHERE id = ?")) {
            ps.setLong(1, orderId);
            ps.setQueryTimeout(QUERY_TIMEOUT_SECONDS);

            try (ResultSet rs = ps.executeQuery()) {
                if (!rs.next()) return Optional.empty();
                return Optional.of(new Order(
                    rs.getLong("id"), rs.getString("status")));
            }
        } finally {
            dbSlots.release();
        }
    }

    public record Order(long id, String status) {}

    public static final class DependencyOverloadedException
            extends RuntimeException {
        public DependencyOverloadedException(String message) {
            super(message);
        }
    }
}
```

### Virtual-thread usage (demonstration)

```java
import java.util.concurrent.*;

try (ExecutorService executor =
         Executors.newVirtualThreadPerTaskExecutor()) {
    var future = executor.submit(() -> repository.findById(42L));
    System.out.println(future.get(2, TimeUnit.SECONDS));
}
```

This is a small demo, not a recommendation to create an executor per HTTP request. In a server, use lifecycle-managed framework/server virtual-thread support or one managed executor. The repository semaphore limits concurrent DB access; the executor does not.

## 3. Walkthrough, edge cases and trade-offs (4 minutes)

### Dry run
- 24 calls hold permits and can execute database work.
- Call 25 waits up to 75 ms.
- If a permit is released in time, call 25 proceeds.
- Otherwise it gets an overload exception instead of sitting in an unlimited queue.
- If SQL throws, `finally` releases the permit; capacity does not leak.

### Common mistakes
1. **“Virtual threads are cheap, so unlimited requests are safe.”** Tasks still retain request state, buffers and objects; downstream systems remain bounded.
2. **Use a fixed pool of 24 virtual threads as a limiter.** JEP 444 recommends not pooling virtual threads; limit access to the resource explicitly.
3. **Acquire a connection before the permit.** A scarce connection may sit idle while the caller waits for admission.
4. **Release only on success.** Any exception can leak a permit; use `finally`.
5. **Treat the semaphore as global.** Four replicas with 24 permits each can admit 96 operations. Size per-instance limits against the total DB budget or use a true shared limiter.
6. **Assume JDBC timeout is an end-to-end deadline.** Driver cancellation differs. Configure pool acquisition and driver-supported statement/network timeouts; test with the actual driver.
7. **Hold the permit too long.** Release it as soon as the protected DB operation ends.

### Spring transaction caveat
With `@Transactional`, a transaction interceptor may acquire a connection before the method body obtains the permit. Keep admission outside the transaction boundary—for example, an outer non-transactional bean obtains admission and calls a separate proxied transactional bean—or verify the exact transaction manager behavior in tests. Do not assume annotation ordering fixes it.

### Production checklist
Measure permit wait/rejections, active DB operations, Hikari active/idle/pending connections, query latency, timeout counts, HTTP latency and 5xx rate. Tune 24 permits and 75 ms from realistic load tests; these are examples, not universal defaults.

### Interview-ready summary
“Virtual threads improve blocking-code scalability, but do not add database capacity. I would use virtual threads for request concurrency and a timed semaphore sized against the pool and total DB budget. Acquire admission before a connection, release in `finally`, configure pool/query timeouts, reject excess load explicitly, and monitor admission plus pool-wait metrics. The semaphore is per JVM, so replica count and other DB consumers matter.”

### Follow-ups with answers
- **Why not set Hikari pool size to 8,000?** The DB may not support that concurrency; extra connections can increase contention, memory and context switching.
- **Does a semaphore wait always block a carrier thread?** Supported blocking operations can park a virtual thread without necessarily occupying its carrier, but waiters still consume memory and time; bounded admission matters.
- **Should permits equal pool size?** Not necessarily. Other repositories, jobs, health checks and transactions may use the pool; leave headroom or coordinate a shared limiter.
- **What if the request is cancelled after acquiring a permit?** `finally` still handles Java exceptional exits, but HTTP cancellation does not guarantee JDBC work stopped. Verify driver cancellation and timeout behavior.

## Sources
- [OpenJDK JEP 444 — Virtual Threads](https://openjdk.org/jeps/444)
- [Java 21 Executors API](https://docs.oracle.com/en/java/javase/21/docs/api/java.base/java/util/concurrent/Executors.html)
- [Java 21 Semaphore API](https://docs.oracle.com/en/java/javase/21/docs/api/java.base/java/util/concurrent/Semaphore.html)
- [Java 21 ExecutorService API](https://docs.oracle.com/en/java/javase/21/docs/api/java.base/java/util/concurrent/ExecutorService.html)

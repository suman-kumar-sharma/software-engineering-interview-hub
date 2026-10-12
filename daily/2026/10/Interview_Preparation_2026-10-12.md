# Interview Preparation — 2026-10-12

**Focus:** Java · Spring Boot · concurrency · PostgreSQL · distributed systems · DSA · AWS · AI engineering · application security  
**Research date:** 12 October 2026 (India Standard Time)  
**Coverage:** Newly *discovered* publicly readable LinkedIn posts and dated official engineering announcements. A post discovered today may have been published earlier; this is **not** a claim that every post was published on 12 October.  
**Account access:** No access to the reader's private LinkedIn feed, connections-only content, or saved posts.

> **Evidence standard.** Each interview prompt below is supported by the linked publicly readable LinkedIn post, but any attribution to an employer is the **post author's claim**, not an independently verified company interview transcript. Answers are independently reasoned and checked against official API or platform documentation where applicable. Code is original, not copied from the posts. A standalone JDK 21 code check was performed for applicable examples; Spring-specific snippets are architectural examples and need a Spring project to run.
>
> **Deduplication:** Compared with `Interview_Preparation_2026-10-11.md`. Yesterday's account aggregation, `ExecutorService`/`CompletableFuture`, Hibernate N+1/500k-row processing, transfer ledger, Kafka payment deduplication, basic `HashMap` vs `ConcurrentHashMap`, and account-locking questions are not repeated. Today's `ConcurrentHashMap` question specifically covers *mutability and multi-operation invariants*, not the previous basic comparison.

## At a glance

| ID | Interview question | Category | Difficulty | LinkedIn source |
|---|---|---|---|---|
| Q1 | Implement a thread-safe, TTL-based in-memory cache without third-party libraries | Java concurrency / LLD | Hard | [Naushad A. — Java backend questions](https://www.linkedin.com/posts/naushad-a-93081b179_capgemini-java-javadeveloper-activity-7506268903199862785-eYzi) |
| Q2 | How do virtual threads schedule; when should they not be used? | Java 21+ / JVM | Medium–Hard | [Naushad A.](https://www.linkedin.com/posts/naushad-a-93081b179_capgemini-java-javadeveloper-activity-7506268903199862785-eYzi) |
| Q3 | Can `ConcurrentHashMap` still have visibility or correctness bugs? | Java Memory Model | Hard | [Naushad A.](https://www.linkedin.com/posts/naushad-a-93081b179_capgemini-java-javadeveloper-activity-7506268903199862785-eYzi) |
| Q4 | Latency spiked after deployment: diagnose contention and isolate the cause | Production / observability | Hard | [Naushad A.](https://www.linkedin.com/posts/naushad-a-93081b179_capgemini-java-javadeveloper-activity-7506268903199862785-eYzi) |
| Q5 | Why does Spring `@Transactional` not work for self-invocation? | Spring AOP / transactions | Medium | [Rani Dhage — Java microservices questions](https://www.linkedin.com/posts/rani-dhage_java-microservices-latest-interview-questions-activity-7486081122397765632-jwec) |
| Q6 | Paginate a table with billions of records | SQL / system design | Hard | [Naushad A.](https://www.linkedin.com/posts/naushad-a-93081b179_capgemini-java-javadeveloper-activity-7506268903199862785-eYzi) |
| Q7 | Sliding Window Maximum in O(n) | DSA / deque | Medium | [Aviral Pruthi — DSA interview questions](https://www.linkedin.com/posts/aviral-pruthi-9450a8179_technology-innovation-careers-activity-7423682513732284416-t8ng) |
| Q8 | Design a reliable webhook dispatcher | System design / Kafka / security | Hard | [Rahul R Jain — machine coding and design](https://www.linkedin.com/posts/rahulrjain96_systemdesign-machinecoding-codinginterview-activity-7495324864568827904-4KPP) |
| Q9 | `Optional.orElse` vs `orElseGet`, `map` vs `flatMap` | Java APIs | Easy–Medium | [Sangamnath Ingalalli — Java interview questions](https://www.linkedin.com/posts/sangamnath-ingalalli_javainterview-springboot-microservices-activity-7496056752950505472-Z-i3) |
| Q10 | Detect the 132 pattern in linear time | DSA / monotonic stack | Hard | [Aviral Pruthi](https://www.linkedin.com/posts/aviral-pruthi-9450a8179_technology-innovation-careers-activity-7423682513732284416-t8ng) |

---

## Q1 — Thread-safe TTL cache without external libraries

**Original prompt (paraphrased):** Implement a thread-safe in-memory cache without third-party libraries. A useful production-level follow-up is TTL expiration and preventing multiple concurrent cache misses from hammering the same backend.

**Approach, step by step.** A normal `HashMap` is unsafe for concurrent access. A `ConcurrentHashMap<K, CompletableFuture<Entry<V>>>` allows one shared *in-flight* load per key. Under `compute`, a caller either reuses a valid cached result, waits for an existing load, or installs its own empty promise. Importantly, the potentially slow loader runs **outside** the map's `compute` callback, so it does not hold a map-bin lock while doing I/O. An expired entry is replaced lazily on the next access.

```java
import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;

public final class SingleFlightTtlCache<K, V> {
    private record Entry<V>(V value, long createdNanos) {}

    private final ConcurrentHashMap<K, CompletableFuture<Entry<V>>> entries =
            new ConcurrentHashMap<>();
    private final long ttlNanos;

    public SingleFlightTtlCache(Duration ttl) {
        Objects.requireNonNull(ttl);
        if (ttl.isZero() || ttl.isNegative())
            throw new IllegalArgumentException("TTL must be positive");
        ttlNanos = ttl.toNanos();
    }

    public V get(K key, Supplier<? extends V> loader) {
        Objects.requireNonNull(key);
        Objects.requireNonNull(loader);
        CompletableFuture<Entry<V>> mine = new CompletableFuture<>();

        CompletableFuture<Entry<V>> chosen = entries.compute(key, (k, current) -> {
            if (current == null || current.isCompletedExceptionally()) return mine;
            if (!current.isDone()) return current; // join the in-flight load
            Entry<V> old = current.getNow(null);
            if (old != null && System.nanoTime() - old.createdNanos() < ttlNanos)
                return current;
            return mine; // expired: this caller becomes the loader
        });

        if (chosen == mine) {
            try {
                V loaded = Objects.requireNonNull(loader.get(), "Loader returned null");
                mine.complete(new Entry<>(loaded, System.nanoTime()));
            } catch (Throwable failure) {
                mine.completeExceptionally(failure);
                entries.remove(key, mine); // compare-and-remove; don't remove a newer value
            }
        }
        return chosen.join().value(); // propagates a failed load as CompletionException
    }

    public void invalidate(K key) { entries.remove(key); }

    public static void main(String[] args) {
        var cache = new SingleFlightTtlCache<String, String>(Duration.ofSeconds(30));
        System.out.println(cache.get("config", () -> "loaded-once"));
        System.out.println(cache.get("config", () -> "not-called"));
    }
}
```

**Why it is correct:** `compute` atomically chooses the per-key future; only the caller whose `mine` future was installed invokes the loader. Others join that future. On failure, `remove(key, mine)` avoids deleting a newer replacement. `System.nanoTime()` is appropriate for elapsed time (unlike wall-clock time). **Limits:** This is a teaching implementation, not a complete cache product. It has **no size bound, eviction policy, refresh-ahead, or distributed invalidation**. A slow or hung loader can leave other callers waiting; enforce backend timeouts and consider a load deadline, cancellation policy, and metrics. A very large TTL can overflow `toNanos()`; production code should validate it. Invalidation during an in-flight load does not cancel that caller's already-running load.

**Complexity:** Expected O(1) map work per lookup; a cache miss additionally costs the loader's I/O/computation; memory O(number of distinct cached keys + in-flight loads), unbounded without eviction.

**Edge cases:** Concurrent cold misses; expiry exactly at TTL; loader returns `null`; loader throws; manual invalidation during load; cache stampede across **different application instances** (this implementation only coalesces within one JVM); very large keys/values; long-lived idle keys never being removed.

**Follow-ups:** How to add LRU? Use a bounded, synchronized eviction structure, accepting contention, or a proven cache library when allowed. How to avoid a cross-node stampede? Use distributed coordination or stale-while-revalidate with jitter and bounded refresh concurrency. Why not do the slow loader directly in `computeIfAbsent`? Its mapping function can block updates to the same bin, hurting throughput.

**Official reference:** [Java `ConcurrentHashMap.computeIfAbsent` semantics](https://docs.oracle.com/en/java/javase/25/docs/api/java.base/java/util/concurrent/ConcurrentHashMap.html).

## Q2 — Virtual threads: scheduling, pinning, and when not to use them

**Original prompt:** How does virtual-thread scheduling work, and when would you avoid virtual threads?

**Answer.** A virtual thread is a Java `Thread` scheduled by the JVM onto a smaller set of carrier (platform) threads. During supported blocking I/O, the virtual thread can unmount; its carrier becomes available for another task. This increases **concurrency and throughput for blocking workloads**; it does not make CPU work faster. Virtual threads are not pooled; create one per task, and bound scarce resources (DB connections, API concurrency) separately.

```java
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

public class VirtualThreadFanout {
    public static void main(String[] args) throws Exception {
        Semaphore downstreamLimit = new Semaphore(16);
        try (ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor()) {
            List<Future<String>> futures = new ArrayList<>();
            for (int i = 0; i < 100; i++) {
                final int requestId = i;
                futures.add(executor.submit(() -> {
                    downstreamLimit.acquire();
                    try {
                        Thread.sleep(20); // simulate blocking I/O
                        return "response-" + requestId;
                    } finally {
                        downstreamLimit.release();
                    }
                }));
            }
            for (Future<String> f : futures) System.out.println(f.get());
        }
    }
}
```

**Why this works:** 100 lightweight tasks can exist, while only 16 enter the simulated downstream service at a time. The semaphore models a scarce dependency; it is **not** a virtual-thread pool. In production, also use per-call timeouts and avoid submitting unbounded tasks faster than the system can process them.

**Important version-specific interview trap:** In **JDK 21–23**, blocking while inside `synchronized` could pin the carrier. **JDK 24+** changed monitor behavior (JEP 491), so ordinary synchronized-block I/O no longer causes that form of pinning. Native/foreign calls can still pin. Do not repeat the old blanket advice to replace every `synchronized` with `ReentrantLock` on JDK 25/26/27. Always state which JDK is under discussion.

**When not useful:** Long CPU-bound loops (carrier threads still limited by cores); code that holds scarce DB connections for a long time; workloads whose bottleneck is external service quotas; libraries with problematic native blocking; unbounded task creation without admission control. Virtual threads do not increase the DB pool size or remove backpressure requirements.

**Complexity:** O(n) task submission/result processing and O(n) future/task state for `n` tasks; actual wall time depends on downstream concurrency and I/O latency. **Edge cases:** Interrupted acquire/sleep; downstream outage; DB pool exhaustion; timeouts; cancellation; native pinning. **Follow-ups:** Why not pool virtual threads? How do carrier threads differ from a connection pool? How would you trace blocked virtual threads? Compare JDK 21 and JDK 25 pinning.

**Official references:** [Oracle virtual-thread guide (JDK 26)](https://docs.oracle.com/en/java/javase/26/core/virtual-threads.html) and [JDK 21 behavior](https://docs.oracle.com/en/java/javase/21/core/virtual-threads.html).

## Q3 — `ConcurrentHashMap` does not make the *values* or multi-step workflows thread-safe

**Original prompt:** What memory-visibility issues can remain even after using `ConcurrentHashMap`?

**Key distinction:** A `ConcurrentHashMap` safely publishes a **mapping** and provides the documented happens-before relationship for a successful per-key retrieval. It does **not** magically make a mutable object stored as a value thread-safe, nor make a sequence of `get`/modify/`put` operations atomic. Its iterators and aggregate operations are not an atomic snapshot of the whole map.

**Incorrect example:**

```java
// DON'T DO THIS: mutable value + non-atomic read-modify-write.
// class MutableCounter { int count; }
// map.get("A").count++; // can lose increments even though map is concurrent
```

**Correct approach: replace immutable values atomically per key.**

```java
import java.util.concurrent.ConcurrentHashMap;

public class AtomicAccountState {
    record State(long balanceCents, long version) {}
    private final ConcurrentHashMap<String, State> states = new ConcurrentHashMap<>();

    public State adjust(String id, long deltaCents) {
        return states.compute(id, (key, old) -> {
            State before = old == null ? new State(0, 0) : old;
            return new State(Math.addExact(before.balanceCents(), deltaCents),
                             Math.addExact(before.version(), 1));
        });
    }

    public static void main(String[] args) {
        var service = new AtomicAccountState();
        System.out.println(service.adjust("A", 250));
        System.out.println(service.adjust("A", -50)); // balance=200, version=2
    }
}
```

**Reasoning:** The remapping function is applied atomically for this key. `record` makes the state immutable, so a reader sees a complete pair of balance/version fields, not partially updated mutable state. **Caveat:** This is an **in-memory demo**, not a financial ledger; it is not durable or transactionally coordinated across processes. For two-key invariants (transfer A → B), per-key `compute` operations are insufficient; use a wider lock or a database transaction.

**Complexity:** Expected O(1) per-key update and O(k) storage for k keys; hash collisions/contended bins can degrade throughput. **Edge cases:** Arithmetic overflow; absent key; concurrent reads while updates occur; aggregate consistency across keys; value mutation outside map; mutable nested objects. **Follow-ups:** `AtomicLong` vs `LongAdder`? `LongAdder` excels at high-contention approximate reads but `sum()` is not an atomic snapshot. Why are `size()`/iteration not transactionally consistent under mutation? What if values contain a mutable `List`?

**Official reference:** [JDK `ConcurrentHashMap` contract](https://docs.oracle.com/en/java/javase/25/docs/api/java.base/java/util/concurrent/ConcurrentHashMap.html).

## Q4 — Production latency spikes immediately after deployment

**Original prompt:** How would you investigate a Java application's latency increase after deployment? How would you detect thread contention?

**Senior-level diagnostic sequence:**

1. **Stabilize:** Compare canary vs old version, error rate, p50/p95/p99, saturation, and recent configuration/schema changes. Roll back or shift traffic if SLO/customer impact is severe; preserve diagnostics when safe.
2. **Locate the layer:** Break request time into load balancer, application, downstream HTTP, JDBC connection acquisition, SQL execution, Kafka, and serialization. Compare traces across versions; correlate by deployment ID.
3. **Check saturation:** CPU throttling, GC pauses/allocation, heap, container memory, JVM thread counts, blocked threads, DB pool active/pending, downstream 429s, queue depth, and network latency.
4. **Inspect contention:** Take several thread dumps **over time**, not one screenshot. Check repeated `BLOCKED` stacks, lock owners, synchronized hot paths, and executor queue waits. Java Flight Recorder (JFR) gives allocation, lock, I/O, CPU and GC events.
5. **Test one hypothesis:** Example: a new synchronized cache refresh serializes requests, or a SQL query lost an index. Reproduce with load testing and compare traces/metrics before deploying the fix.
6. **Verify:** Confirm p99 and error budget recover, and add a regression benchmark or alert.

**Commands (run only with appropriate production access):**

```bash
# Three snapshots spaced apart can reveal persistent blocked stacks.
jcmd <PID> Thread.print -l > threads-1.txt
# Record a short profile; tune duration/overhead for your environment.
jcmd <PID> JFR.start name=latency settings=profile duration=60s filename=/tmp/latency.jfr
jfr summary /tmp/latency.jfr
```

**Optional Java diagnostic — platform-thread deadlock detection:**

```java
import java.lang.management.ManagementFactory;
import java.lang.management.ThreadMXBean;
import java.util.Arrays;

public class DeadlockProbe {
    public static void main(String[] args) {
        ThreadMXBean mx = ManagementFactory.getThreadMXBean();
        long[] ids = mx.findDeadlockedThreads();
        System.out.println(ids == null ? "No platform-thread deadlock detected"
                                       : Arrays.toString(ids));
    }
}
```

**Important:** `ThreadMXBean`'s thread and deadlock APIs are primarily for **platform threads**; absence of a detected deadlock does not prove virtual-thread workflows are healthy. Use appropriate JFR and virtual-thread dumps when needed.

**Complexity:** Diagnostic workflow, not a data-structure algorithm; the probe itself is effectively O(number of threads/synchronizers inspected) and implementation-dependent. **Edge cases:** CPU is low but p99 high (I/O wait or pool queue); GC is healthy but connection acquisition slow; only one AZ affected; a canary has different traffic; clock-skewed traces; correlated retries amplify load. **Follow-ups:** What if CPU suddenly reaches 90% without a code change? How do you separate lock contention from DB pool starvation? How would you investigate one slow AZ?

**Official reference:** [Java `ThreadMXBean` monitoring scope](https://docs.oracle.com/en/java/javase/25/docs/api/java.management/java/lang/management/ThreadMXBean.html).

## Q5 — Why Spring `@Transactional` self-invocation fails

**Original prompt:** How does `@Transactional` work internally, and why might a transaction not start when one method calls another method in the same service?

**Explanation:** In Spring's **default proxy mode**, the transaction interceptor runs when a call passes **through the Spring proxy**. A method calling `this.save()` inside the same object bypasses that proxy. Consequently, the annotation on `save()` is not applied *by that internal call*. If an outer method already has a transaction, the inner code still executes within it; the problem is that the inner method's own propagation/isolation rules are not intercepted.

**Incorrect (illustrative Spring code):**

```java
@Service
class OrderService {
    public void createOrder(Order order) {
        saveOrder(order); // self-invocation: no proxy interception here
    }

    @Transactional
    public void saveOrder(Order order) {
        // repository.save(order);
    }
}
```

**Recommended fix: move the transactional boundary into a separate Spring-managed service.**

```java
@Service
class OrderFacade {
    private final OrderWriter writer;
    OrderFacade(OrderWriter writer) { this.writer = writer; }

    public void createOrder(Order order) {
        writer.saveOrder(order); // call goes through writer's Spring proxy
    }
}

@Service
class OrderWriter {
    @Transactional
    public void saveOrder(Order order) {
        // repository.save(order);
    }
}
```

**Reasoning:** The facade's call enters `OrderWriter` through the injected bean reference, allowing Spring's transaction advice to run. Another legitimate approach is `TransactionTemplate` for programmatic control. AspectJ weaving can intercept self-invocation but adds build/runtime complexity. Avoid relying on self-injection or `AopContext.currentProxy()` unless there is a compelling reason.

**Rollback nuance:** By default, Spring rolls back for unchecked `RuntimeException` and `Error`, not every checked exception. `@Transactional(rollbackFor = SomeCheckedException.class)` changes this. Transactional annotations on `private` methods do not work as an external proxy boundary. Do not assume `REQUIRES_NEW` is honored on a self-call.

**Complexity:** O(1) interception overhead per call; business work and DB I/O dominate. **Edge cases:** Internal call, checked exception, `REQUIRES_NEW`, async thread boundaries, wrong transaction manager, method not reachable through proxy. **Follow-ups:** What if `createOrder()` is itself `@Transactional`? How does `TransactionTemplate` help? What is the difference between Spring AOP proxies and AspectJ weaving?

**Official reference:** [Spring declarative transaction annotations](https://docs.spring.io/spring-framework/reference/data-access/transaction/declarative/annotations.html).

## Q6 — Paginating billions of database rows: keyset pagination

**Original prompt:** How would you paginate a table containing billions of records?

**Problem with large OFFSET:** `LIMIT 50 OFFSET 500000000` requires the database to compute/skip many rows; it also produces unstable results when rows are inserted/deleted between requests. A deterministic `ORDER BY` is mandatory. For infinite-scroll or "next page", prefer **keyset (seek) pagination**.

**Schema and index (PostgreSQL):**

```sql
CREATE TABLE events (
    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    tenant_id BIGINT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL,
    payload JSONB NOT NULL
);
CREATE INDEX events_tenant_created_id_idx
    ON events (tenant_id, created_at DESC, id DESC);

-- First page for tenant 42:
SELECT id, created_at, payload
FROM events
WHERE tenant_id = 42
ORDER BY created_at DESC, id DESC
LIMIT 50;

-- Next page: supply (created_at, id) of the LAST row on prior page.
SELECT id, created_at, payload
FROM events
WHERE tenant_id = 42
  AND (created_at, id) < (TIMESTAMPTZ '2026-10-12 12:00:00+00', 987654)
ORDER BY created_at DESC, id DESC
LIMIT 50;
```

**Why it works:** With both sort columns descending, the row-value predicate selects rows *after* the cursor in descending order. `id` breaks ties when multiple rows share the same timestamp. The index supports the tenant filter and ordered seek. Use bound SQL parameters in application code, never string-concatenate user cursor values.

**Java cursor model (transport-level example):**

```java
import java.time.Instant;

public record EventCursor(Instant createdAt, long id) {
    public EventCursor {
        if (createdAt == null || id <= 0) throw new IllegalArgumentException();
    }
}
```

**API contract:** `GET /tenants/{id}/events?limit=50&cursor=...` returns `items` and an opaque `nextCursor`. Encode timestamp and ID (and optionally filter hash/tenant) into a signed or authenticated cursor so clients cannot silently change its meaning. Do not promise a globally consistent snapshot across pages unless you maintain one or specify an appropriate snapshot strategy. For a live feed, new rows may appear *before* the current cursor and will not appear in subsequent pages; this is normally desired.

**Complexity:** With a suitable B-tree index, roughly O(log N + k) index seek and k-row scan for page size k; OFFSET pagination can require scanning/skipping O(offset + k) rows. Actual plans depend on selectivity, visibility, and table layout. **Edge cases:** Identical timestamps; deleted cursor row; mutable sort key; tenant filter changes; ascending/descending mixed sort; back-pagination; arbitrary "go to page 10,000" (keyset cannot jump to an unknown cursor cheaply). **Follow-ups:** How would you paginate by score? How would you keep snapshot consistency? How would you handle an index not being used?

**Official reference:** [PostgreSQL LIMIT/OFFSET documentation](https://www.postgresql.org/docs/18/queries-limit.html).

## Q7 — Sliding Window Maximum in O(n)

**Original prompt:** Given integer array `nums` and window size `k`, return the maximum in every contiguous window of size `k`.

**Example:** `nums = [1,3,-1,-3,5,3,6,7], k = 3` → `[3,3,5,5,6,7]`.

**Reasoning:** Maintain a deque of **indices**, with values decreasing from front to back. At each new index: remove indices that left the window; remove smaller/equal trailing values that can never be the maximum of a future window; append the current index. The front is the maximum of the current window.

```java
import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Deque;

public class SlidingWindowMaximum {
    public static int[] maxSlidingWindow(int[] nums, int k) {
        if (nums == null || k < 1 || k > nums.length)
            throw new IllegalArgumentException("Invalid input or window size");
        int[] result = new int[nums.length - k + 1];
        Deque<Integer> deque = new ArrayDeque<>();
        for (int i = 0; i < nums.length; i++) {
            while (!deque.isEmpty() && deque.peekFirst() <= i - k)
                deque.removeFirst();
            while (!deque.isEmpty() && nums[deque.peekLast()] <= nums[i])
                deque.removeLast();
            deque.addLast(i);
            if (i >= k - 1) result[i - k + 1] = nums[deque.peekFirst()];
        }
        return result;
    }

    public static void main(String[] args) {
        System.out.println(Arrays.toString(maxSlidingWindow(
                new int[]{1, 3, -1, -3, 5, 3, 6, 7}, 3)));
    }
}
```

**Proof intuition:** Every index is inserted once and removed at most once. When a newer value is greater than or equal to an older one, the older index cannot win any future window that also contains the newer index, so discarding it is safe.

**Complexity:** O(n) time amortized, O(k) auxiliary deque space, plus O(n-k+1) output. **Edge cases:** `k=1`; `k=n`; negative numbers; duplicates; strictly increasing/decreasing input; invalid k and empty input (the implementation deliberately rejects k=0). **Follow-ups:** Why is a max-heap O(n log k) rather than O(n)? Can you output streaming maxima? What changes for sliding-window minimum?

**Official reference:** [Java `ArrayDeque` API](https://docs.oracle.com/en/java/javase/25/docs/api/java.base/java/util/ArrayDeque.html).

## Q8 — Design a reliable webhook dispatcher

**Original prompt:** Design a reliable webhook dispatcher with retries, Kafka-based processing, ordering, idempotency and failure recovery.

**Requirements to clarify:** Peak events/second, subscribers per event, delivery latency SLO, ordering scope (per event source or per subscriber), maximum retry window, endpoint verification, replay controls, and whether the receiver supports idempotency keys.

**Reference architecture:**

```text
Producer transaction
   ├── INSERT domain event
   └── INSERT outbox row  (same DB transaction)
             │
      Outbox publisher (retryable)
             │
       Kafka topic: events
             │
       Fan-out/subscription worker
             │
       delivery_jobs table (unique event_id + subscription_id)
             │
       leased delivery workers ──HTTP POST──> subscriber endpoint
             │                                  │
        retry/backoff/DLQ <──── response / timeout
             │
      delivery history + metrics + manual replay
```

**Core schema (conceptual PostgreSQL):**

```sql
CREATE TABLE delivery_jobs (
    delivery_id UUID PRIMARY KEY,
    event_id UUID NOT NULL,
    subscription_id UUID NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('PENDING','LEASED','DELIVERED','FAILED')),
    attempts INT NOT NULL DEFAULT 0,
    next_attempt_at TIMESTAMPTZ NOT NULL,
    lease_until TIMESTAMPTZ,
    last_http_status INT,
    UNIQUE (event_id, subscription_id)
);
CREATE INDEX delivery_due_idx ON delivery_jobs (next_attempt_at)
    WHERE status IN ('PENDING','FAILED');
```

**Step-by-step reliability design:**

1. **No event loss:** Write business state and an outbox event in the same local transaction. An outbox relay publishes to Kafka at least once; it may publish duplicates after a crash, so consumers must be idempotent.
2. **Fan-out:** Resolve active subscriptions and create one delivery job per `(event_id, subscription_id)` under a uniqueness constraint. A repeated Kafka event becomes a no-op for existing jobs.
3. **Leasing:** Workers claim due jobs using a DB transaction, `FOR UPDATE SKIP LOCKED` (or an equivalent queue). Record lease owner/deadline; expired leases can be reclaimed after a crash.
4. **Send securely:** Validate HTTPS destination, block loopback/link-local/private addresses unless explicitly approved, re-resolve DNS safely to mitigate SSRF, enforce connect/read timeouts and response-size limits, sign raw payload bytes using per-subscriber HMAC-SHA256, and include stable `X-Delivery-Id` and event ID.
5. **Retry:** Treat 2xx as success; retry timeouts, 408, 429 (respect bounded `Retry-After`), and 5xx with capped exponential backoff + jitter. Most other 4xx are configuration/permanent failures. Stop after a policy limit; put into a dead-letter state for inspection/replay.
6. **Ordering:** Partition Kafka by the chosen ordering key, e.g. subscription ID, but understand that retries can reorder deliveries unless the dispatcher serializes in-flight work for that key. Strict order reduces throughput; offer it as a contract only where required.
7. **Observability:** Metrics for delivery attempts, time-to-first-attempt, end-to-end age, retries, DLQ rate, subscriber latency, 429s, and worker lease expiry. Expose replay and audit trails.

**Original Java utility — HMAC signing:**

```java
import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.util.HexFormat;
import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;

public class WebhookSignature {
    public static String sign(byte[] payload, String secret)
            throws GeneralSecurityException {
        Mac mac = Mac.getInstance("HmacSHA256");
        mac.init(new SecretKeySpec(secret.getBytes(StandardCharsets.UTF_8), "HmacSHA256"));
        return HexFormat.of().formatHex(mac.doFinal(payload));
    }
}
```

**Security detail:** Sign a canonical envelope that includes delivery ID, timestamp and raw payload bytes; compare signatures in constant time at the receiver, reject stale timestamps, and rotate subscriber secrets. The code above signs *only* payload bytes to illustrate the primitive; it is not the full replay-safe protocol.

**Guarantees:** The sender can guarantee **at-least-once attempts**, not exactly-once HTTP side effects. A timeout after the receiver commits its work is indistinguishable from failure. The receiver must deduplicate the stable delivery/event ID to obtain effectively-once business processing.

**Complexity:** For E events and average S matching subscribers, O(E·S) delivery-job fan-out and storage, excluding retry amplification. Per job, DB indexing/queueing costs are approximately logarithmic in queued jobs; network latency dominates dispatch. **Edge cases:** Crash after POST but before marking delivered; receiver processes but response lost; DNS rebinding; tenant endpoint compromise; large payload; 429 storms; out-of-order retry; poison endpoint; webhook secret rotation. **Follow-ups:** How would you guarantee per-subscriber order? How do you prevent one subscriber from starving others? Can Kafka transactions guarantee exactly-once delivery over HTTP? (No.)

**Related public design prompt:** [Rahul R Jain](https://www.linkedin.com/posts/rahulrjain96_systemdesign-machinecoding-codinginterview-activity-7495324864568827904-4KPP).

## Q9 — `Optional.orElse()` vs `orElseGet()`, `map()` vs `flatMap()`

**Original prompt:** Explain `Optional.map()`, `flatMap()`, `orElse()` and `orElseGet()`.

**Answer:** `orElse(value)` receives an already evaluated argument, so `expensiveFallback()` runs even if the `Optional` contains a value. `orElseGet(supplier)` evaluates its supplier **only if empty**. `map` transforms a present value and wraps the result; `flatMap` expects the function itself to return an `Optional`, avoiding nested `Optional<Optional<T>>`.

```java
import java.util.Optional;

public class OptionalChoices {
    record User(Optional<String> email) {}

    static String fallback() {
        System.out.println("fallback evaluated");
        return "guest@example.invalid";
    }

    public static void main(String[] args) {
        Optional<String> present = Optional.of("alice@example.invalid");
        String eager = present.orElse(fallback()); // prints even though present
        String lazy = present.orElseGet(OptionalChoices::fallback); // no print

        Optional<User> user = Optional.of(new User(Optional.of("a@example.invalid")));
        Optional<Optional<String>> nested = user.map(User::email);
        Optional<String> flat = user.flatMap(User::email);
        System.out.println(eager + " / " + lazy + " / " + nested + " / " + flat);
    }
}
```

**Complexity:** Optional operations themselves are O(1), excluding mapping/supplier cost. **Edge cases:** `map` returns null → empty; `flatMap` must not return null; supplier may throw; avoid `Optional.get()` without presence checks; do not use Optional as a universal replacement for all fields. **Follow-ups:** What is `or()` vs `orElseGet()`? When is `Optional.stream()` useful? Why is `orElse(new ExpensiveObject())` potentially wasteful?

**Official reference:** [Java `Optional` API](https://docs.oracle.com/en/java/javase/25/docs/api/java.base/java/util/Optional.html).

## Q10 — Detect a 132 pattern in O(n)

**Original prompt:** Does there exist `i < j < k` such that `nums[i] < nums[k] < nums[j]`?

**Reasoning:** Traverse from right to left. Maintain a decreasing stack of candidates for the "3" (`nums[j]`). Whenever the current number is larger than the top, pop the smaller values; the greatest popped candidate can serve as the "2" (`nums[k]`). If a later-left number is less than that candidate, it supplies the "1" and the pattern exists.

```java
import java.util.ArrayDeque;
import java.util.Deque;

public class Pattern132 {
    public static boolean find132pattern(int[] nums) {
        if (nums == null || nums.length < 3) return false;
        Deque<Integer> stack = new ArrayDeque<>();
        int middle = Integer.MIN_VALUE; // candidate for nums[k]
        for (int i = nums.length - 1; i >= 0; i--) {
            if (nums[i] < middle) return true;
            while (!stack.isEmpty() && nums[i] > stack.peek()) {
                middle = Math.max(middle, stack.pop());
            }
            stack.push(nums[i]);
        }
        return false;
    }

    public static void main(String[] args) {
        System.out.println(find132pattern(new int[]{3, 1, 4, 2})); // true
        System.out.println(find132pattern(new int[]{1, 2, 3, 4})); // false
        System.out.println(find132pattern(new int[]{-1, 3, 2, 0})); // true
    }
}
```

**Correctness intuition:** When a right-side value is popped, the current larger value can be its "3", so the popped value is a valid "2" candidate. Any still-earlier value smaller than that "2" completes `1 < 2 < 3` in the required index order. `Integer.MIN_VALUE` is safe as the initial sentinel because no `int` can be smaller than it; equality does not satisfy the strict inequality.

**Complexity:** O(n) time amortized (each item pushed/popped at most once), O(n) stack space. **Edge cases:** Length < 3; equal values; negative values; `Integer.MIN_VALUE`; increasing/decreasing arrays; duplicates. **Follow-ups:** Can you return indices `(i,j,k)` instead of boolean? Can you solve in O(n²) as a baseline? Why must inequalities be strict?

---

## Engineering developments and important technical discussions

The following are **officially announced product/platform changes**, except the final design discussion, which is labeled as a public opinion/practice prompt. These are developments worth knowing, **not claims about how the entire software job market has changed**.

### N1 — 12 Oct 2026: GitHub adds authenticated commit pusher attribution (limited preview)

**What changed:** GitHub announced a separate authenticated *pusher* identity attached to commits, in addition to user-supplied author and committer strings. Enterprise customers in a limited public preview can use a `Committer must match pusher` repository rule.

**Why it matters:** Commit author metadata can be forged; security reviews and audit trails should distinguish who authored a patch from which authenticated identity pushed it. This matters for CI/CD provenance and repository governance.

**Action:** For sensitive repositories, review branch protection/rulesets, signed commits, permissions, and audit logging; assess the preview when eligible. Do not assume the feature is generally available.

**Official source:** [GitHub Changelog — 12 Oct 2026](https://github.blog/changelog/2026-10-12-push-attribution-public-preview/).

### N2 — 9 Oct 2026: AWS Lambda exposes Availability Zone ID via metadata endpoint

**What changed:** A new authenticated local metadata endpoint lets a Lambda execution environment discover its AZ **ID** (not merely an AZ name). AWS describes support across runtimes, container images, VPC functions, provisioned concurrency, and SnapStart.

**Why it matters:** For hot read paths to AZ-scoped ElastiCache, RDS/Aurora replicas, or internal services, same-AZ routing can reduce cross-AZ latency and transfer costs. AZ IDs are stable across AWS accounts, unlike AZ names that may map differently across accounts.

**Implementation detail:** AWS sets `AWS_LAMBDA_METADATA_API` and `AWS_LAMBDA_METADATA_TOKEN`. Read once per execution environment, cache appropriately, and use a **fallback** if the same-AZ resource is unhealthy/unavailable. For SnapStart, refresh after restore; do not persist a stale AZ decision in a snapshot. Keep the metadata token private.

**Official source:** [AWS Compute Blog — 9 Oct 2026](https://aws.amazon.com/blogs/compute/reducing-cross-az-latency-with-the-aws-lambda-metadata-endpoint/).

### N3 — 6 Oct 2026: AWS Lambda SnapStart now supports container images

**What changed:** AWS documented SnapStart support for container-image Lambda functions, including supported Java 11+ base images. Lambda snapshots an initialized execution environment and restores it for published versions to reduce cold-start time.

**Why it matters:** Java functions packaged as images can now get snapshot/restore startup benefits without converting to ZIP deployment. **But** state created before the snapshot (DB connections, cached credentials, IDs) may be stale or duplicated after restore.

**Action:** Use after-restore hooks (Java CRaC support where applicable) to refresh connections and secrets, measure restore duration, verify feature-region compatibility, and account for snapshot cache/restore pricing. SnapStart applies to **published versions**, not `$LATEST`; provisioned concurrency and some other features remain incompatible.

**Official source:** [AWS Compute Blog — 6 Oct 2026](https://aws.amazon.com/blogs/compute/get-started-with-aws-lambda-snapstart-for-container-images/).

### N4 — 10 Oct 2026: Copilot for JetBrains adds enterprise controls and diagnostics

**What changed:** GitHub announced enterprise-managed default models, a diagnostics **Fix** action, and an MCP setting that can prevent automatic server startup for Copilot/Claude. The update also ended support for JetBrains IDE 2025.1; version 2025.2+ is now required.

**Why it matters:** Java developers using IntelliJ/JetBrains can better govern agent tool startup and model selection. Security and platform teams should review MCP permissions, while developers can diagnose IDE/plugin problems with fewer manual steps.

**Official source:** [GitHub Changelog — 10 Oct 2026](https://github.blog/changelog/2026-10-10-new-controls-and-chat-improvements-in-copilot-for-jetbrains/).

### N5 — 7 Oct 2026: GitHub Copilot local sandboxing becomes generally available

**What changed:** Local sandboxing became GA for GitHub Copilot CLI, the Copilot app, and VS Code sessions using Agent Host. Tool execution can be restricted from filesystem, network, credentials, and other resources according to local or enterprise policies.

**Why it matters:** Coding agents can execute commands, not just suggest text. Sandboxing helps enforce least privilege; model selection alone is not a security boundary. Review what directories and secrets your agent can reach before allowing unattended modifications.

**Official source:** [GitHub Changelog — 7 Oct 2026](https://github.blog/changelog/2026-10-07-local-sandboxing-for-github-copilot-now-generally-available/).

### N6 — 7 Oct 2026: GitHub's new secret-detection model and preview boundaries

**What changed:** GitHub introduced a purpose-built model to identify credentials using surrounding code context. Existing AI-detected secret alerts for eligible Secret Protection customers switched to the model. AI push-protection checks are **private preview**, and new `/security-review` secret checks are **planned**, not broadly available; opt-in checks have separate AI-credit implications.

**Why it matters:** Token formats are not always recognizable by regex. Context-aware scanning complements deterministic secret scanning, but neither replaces credential rotation, least privilege, or reviewing false positives. Do not mistake a preview feature for GA.

**Official source:** [GitHub Changelog — 7 Oct 2026](https://github.blog/changelog/2026-10-07-purpose-built-model-for-leaked-secret-detection/).

### N7 — Spring security maintenance: unified patch train scheduled for 22 Oct 2026

**What changed:** On 21 September, the Spring team announced a consolidated release train for Spring portfolio patches and CVE fixes. The next regular patch train was scheduled for **22 October 2026**. This is an **upcoming date as of this report**, not a claim that patches have already shipped.

**Why it matters:** Backend teams should coordinate Spring Boot, Spring Framework, Spring Security, and dependent-library upgrades instead of assuming isolated security fixes are sufficient. Track applicable CVEs and test changes in staging.

**Official source:** [Spring engineering announcement — 21 Sep 2026](https://spring.io/blog/2026/09/21/releasing-spring-for-modern-challenges/).

### N8 — Architecture discussion: designing an AI chat platform as a backend system

**Source type:** Public LinkedIn **practice prompt/opinion**, not a verified employer question or product launch. A public system-design post includes designing an AI chat platform with LLM routing, streaming, context management, vector retrieval and rate limiting.

**Useful production outline:** API gateway + auth + tenant quotas → conversation metadata in a transactional DB → context builder and retrieval service → model router → streaming response gateway (SSE or WebSocket) → asynchronous moderation/audit/cost metrics. Add request IDs, prompt/tool-call tracing, token and monetary budgets, tool permission boundaries, prompt-injection defenses, cache policy, and failure-mode handling (timeouts, partial streams, fallback models). Keep private user documents separated by tenant and enforce access control **before** retrieval. Do not treat embeddings/vector search as authorization.

**Source:** [Artham Bhardwaj — system design practice prompts](https://www.linkedin.com/posts/artham-bhardwaj_systemdesign-softwareengineering-backend-activity-7481775700492836864-mIbZ).

---

## Revision plan — 20 minutes

| Minutes | Activity | What you should be able to say afterward |
|---|---|---|
| 0–5 | Q2 virtual threads + Q3 map-value invariants | “Virtual threads scale blocking concurrency; `ConcurrentHashMap` does not make mutable values atomic.” |
| 5–10 | Q5 transactions + Q6 keyset pagination | “Proxy boundaries matter; seek by an indexed deterministic cursor.” |
| 10–15 | Q7 sliding window + Q10 132 pattern | Explain why each index is pushed/popped at most once. |
| 15–20 | Q8 webhook dispatcher + N2/N3 AWS Lambda | Explain outbox + at-least-once HTTP, then AZ-aware routing and SnapStart restore safety. |

## Verification, freshness and limitations

- **Public LinkedIn question sources were readable during this research run.** Their post age indicators are relative and sometimes inconsistent with search crawl timestamps; therefore this document does not assert exact post publication dates unless explicitly shown in the post.
- **Employer names in posts are not verified** by the employers. Posts can contain promotional links or unverified claims; these were not adopted as factual evidence.
- **News is dated and linked to primary sources.** N1 is dated **12 Oct 2026**, N2 **9 Oct**, N3 **6 Oct**, N4 **10 Oct**, N5–N6 **7 Oct**, and the Spring schedule was announced **21 Sep** for **22 Oct**.
- **Scope:** 10 substantive, distinct questions selected from the public posts; a larger source post may list additional questions. No claim is made that every question ever posted on LinkedIn was captured.
- **Code execution:** Pure-JDK Java samples were compiled using OpenJDK 21. The 132-pattern and sliding-window algorithms passed randomized brute-force comparisons (14,000 arrays each), and the single-flight cache passed a 100-call concurrent same-key test with one loader execution. Spring snippets are illustrative and require Spring Boot/Spring Framework dependencies. SQL examples are designed for PostgreSQL but were not executed against a live database.
- **Account privacy:** No personalized LinkedIn feed, saved posts, or private account content was accessed. No likes, comments, connections, or posts were made.
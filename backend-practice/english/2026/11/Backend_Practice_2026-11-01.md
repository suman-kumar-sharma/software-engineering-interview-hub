# Advanced Java Backend Practice — Session 10

**Date:** 2026-11-01  
**Topic:** Resilient distributed-system design — cache stampede control with single-flight, stale-while-revalidate, TTL jitter, and bounded refresh capacity  
**Runtime:** Java 21, Spring Boot 3.x, Caffeine 3.x  
**Level:** Advanced backend engineering  
**Timebox:** 3-minute concept, 8-minute implementation, 4-minute derivation and trade-offs

## Prerequisites

You should be comfortable with Java concurrency, ConcurrentHashMap, CompletableFuture, thread pools, HTTP/database timeouts, and cache-aside design. The code below is a focused cache component; domain DTOs and repository implementations are intentionally application-specific.

## Production scenario and interview question

A product-catalogue API runs on 12 Kubernetes replicas. A popular product key receives 2,000 requests per second. Its cache entry expires, the source database takes 900 ms to reload it, and the replicas collectively send a burst of queries to the database. Database latency rises, more requests miss their deadlines, and retries add still more load.

**Design a cache that:**

1. Coalesces concurrent reloads for the same key within one JVM.
2. Returns a bounded-age stale value while a refresh is in progress, where the domain permits stale reads.
3. Prevents a burst of distinct expired keys from creating unbounded background work.
4. Bounds cache memory.
5. Fails predictably on a cold miss if refresh capacity is exhausted.
6. Does not cancel shared refresh work merely because one waiting HTTP request times out.
7. Explains what changes when the service runs on multiple replicas.

Assume illustrative starting values: fresh TTL 60 seconds, stale window 30 seconds, cold-miss wait 250 ms, refresh pool 4 workers, queue capacity 128, and cache maximum 100,000 entries. These are tuning inputs, not universal defaults.

---

# Part 1 — Concept explanation (3 minutes)

## 1. What is a cache stampede?

A cache stampede (also called a thundering herd) occurs when many requests discover that the same expensive value is missing or expired and all load it from the source at once.

Without coordination:

- 2,000 requests miss the same product key.
- 2,000 database reads may begin.
- The database slows down.
- Request timeouts trigger retries.
- The extra retries create even more source traffic.

The cache did not protect the database at the exact moment it was needed most.

## 2. Derive the required mechanisms

| Requirement | Mechanism | What it solves |
|---|---|---|
| Same key, many callers in one JVM | Per-key single-flight | One source load is shared by concurrent callers |
| Old value is acceptable briefly | Stale-while-revalidate | Stale callers return immediately while one refresh runs |
| Many keys expire together | TTL jitter | Spreads expirations across time |
| Source work is scarce | Bounded executor + bounded queue | Limits active and pending refresh tasks |
| Cache must not grow forever | Caffeine maximum size/weight | Bounds local memory |
| Cold miss cannot wait forever | Timed wait | Bounds caller latency |
| Multiple replicas exist | Shared cache/coordination where justified | Reduces cross-replica duplicate work |

**Important distinction:** A local single-flight map coordinates only threads in one JVM. With 12 replicas, the same key can still cause up to 12 concurrent source refreshes—normally far better than 2,000, but not a global single-flight guarantee.

## 3. Hinglish mental model

Socho ek popular product ka cache 60 seconds ke baad stale ho gaya. Ek hi time par 2,000 customers us product ko dekh rahe hain.

Agar sab customers directly database ko call kar dein, toh database par unnecessary load aa jayega.

Humein aisa rule chahiye: **ek key ke liye ek refresh; baaki users ko allowed time tak purana result mil sakta hai.** Saath hi refresh queue bounded honi chahiye, warna alag-alag expired keys milkar system ko overload kar sakti hain.

---

# Part 2 — Practical implementation (8 minutes)

## Step 1 — Define the cache state

For one key, there are three useful states:

| State | Behaviour |
|---|---|
| Fresh | Return immediately |
| Stale but within allowed stale window | Return stale immediately and trigger/join one background refresh |
| Missing or beyond stale window | Join one in-flight load and wait only for a bounded duration |

Use System.nanoTime() for elapsed-time comparisons. It is a monotonic time source intended for measuring elapsed time; do not use wall-clock timestamps for in-process TTL age calculations.

## Step 2 — Configure a bounded refresh executor

Do not use Executors.newFixedThreadPool() blindly for source refreshes: its default work queue is unbounded. Use explicit worker and queue limits.

~~~java
@Configuration
class CacheRefreshConfiguration {

    @Bean(destroyMethod = "shutdown")
    ThreadPoolExecutor catalogRefreshExecutor() {
        return new ThreadPoolExecutor(
            4,
            4,
            0L,
            TimeUnit.MILLISECONDS,
            new ArrayBlockingQueue<>(128),
            Thread.ofPlatform()
                .name("catalog-refresh-", 0)
                .factory(),
            new ThreadPoolExecutor.AbortPolicy()
        );
    }
}
~~~

Why AbortPolicy? When the pool and queue are full, submission fails explicitly with RejectedExecutionException. For a stale hit, the caller can still receive the permitted stale value. For a cold miss, the service can fail fast rather than running the database load on the HTTP request thread.

Avoid CallerRunsPolicy for this refresh path: it can execute the supposedly background source load on the request thread, defeating the separation between request latency and refresh capacity.

## Step 3 — Implement per-key single-flight and stale-while-revalidate

Dependency: use an approved current Caffeine 3.x release through your dependency-management/BOM policy.

~~~java
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;

import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.*;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.BiConsumer;

public final class StaleWhileRevalidateCache<K, V> {

    @FunctionalInterface
    public interface Loader<K, V> {
        V load(K key) throws Exception;
    }

    private static final class Entry<V> {
        final V value;
        final long loadedAtNanos;
        final long freshForNanos;
        final long staleForNanos;

        Entry(V value, long loadedAtNanos,
              long freshForNanos, long staleForNanos) {
            this.value = value;
            this.loadedAtNanos = loadedAtNanos;
            this.freshForNanos = freshForNanos;
            this.staleForNanos = staleForNanos;
        }

        boolean isFresh(long now) {
            long age = now - loadedAtNanos;
            return age >= 0 && age < freshForNanos;
        }

        boolean isStaleButUsable(long now) {
            long age = now - loadedAtNanos;
            long staleUntil = Math.addExact(
                freshForNanos, staleForNanos);

            return age >= freshForNanos && age < staleUntil;
        }
    }

    private final Cache<K, Entry<V>> entries;
    private final ConcurrentMap<K, CompletableFuture<Entry<V>>> inFlight =
        new ConcurrentHashMap<>();

    private final Executor refreshExecutor;
    private final Loader<K, V> loader;
    private final BiConsumer<K, Throwable> failureObserver;

    private final long baseFreshTtlNanos;
    private final long staleWindowNanos;
    private final long missWaitNanos;
    private final double jitterFraction;

    public StaleWhileRevalidateCache(
            long maximumEntries,
            Duration freshTtl,
            Duration staleWindow,
            Duration missWait,
            double jitterFraction,
            Executor refreshExecutor,
            Loader<K, V> loader,
            BiConsumer<K, Throwable> failureObserver) {

        if (maximumEntries < 1) {
            throw new IllegalArgumentException(
                "maximumEntries must be positive");
        }

        this.baseFreshTtlNanos =
            positiveNanos(freshTtl, "freshTtl");
        this.staleWindowNanos =
            positiveNanos(staleWindow, "staleWindow");
        this.missWaitNanos =
            positiveNanos(missWait, "missWait");

        if (jitterFraction < 0.0 || jitterFraction >= 1.0) {
            throw new IllegalArgumentException(
                "jitterFraction must be in [0, 1)");
        }

        this.jitterFraction = jitterFraction;
        this.refreshExecutor =
            Objects.requireNonNull(refreshExecutor);
        this.loader = Objects.requireNonNull(loader);
        this.failureObserver =
            Objects.requireNonNull(failureObserver);

        // Fail fast if the maximum possible age calculation can overflow.
        long maximumFresh = Math.round(
            baseFreshTtlNanos * (1.0 + jitterFraction));
        Math.addExact(maximumFresh, staleWindowNanos);

        this.entries = Caffeine.newBuilder()
            .maximumSize(maximumEntries)
            .build();
    }

    public V get(K key) {
        Objects.requireNonNull(key, "key");

        Entry<V> entry = entries.getIfPresent(key);
        long now = System.nanoTime();

        if (entry != null && entry.isFresh(now)) {
            return entry.value;
        }

        if (entry != null && entry.isStaleButUsable(now)) {
            // Do not wait: stale is explicitly allowed for this key/domain.
            startLoad(key);
            return entry.value;
        }

        // Cold miss or stale window exhausted: bounded wait for shared load.
        return awaitLoad(key, startLoad(key));
    }

    private CompletableFuture<Entry<V>> startLoad(K key) {
        CompletableFuture<Entry<V>> mine = new CompletableFuture<>();

        CompletableFuture<Entry<V>> existing =
            inFlight.putIfAbsent(key, mine);

        if (existing != null) {
            return existing;
        }

        try {
            refreshExecutor.execute(() -> {
                try {
                    V value = Objects.requireNonNull(
                        loader.load(key),
                        "Loader must not return null");

                    Entry<V> replacement = new Entry<>(
                        value,
                        System.nanoTime(),
                        jitteredFreshTtlNanos(),
                        staleWindowNanos
                    );

                    // Publish the cache entry before completing waiters.
                    entries.put(key, replacement);
                    mine.complete(replacement);

                } catch (Throwable error) {
                    mine.completeExceptionally(error);
                    observeFailure(key, error);

                    // Complete waiters, but do not swallow fatal JVM errors.
                    if (error instanceof Error fatal) {
                        throw fatal;
                    }
                } finally {
                    // Remove only our own future; never remove a newer load.
                    inFlight.remove(key, mine);
                }
            });
        } catch (RejectedExecutionException rejected) {
            mine.completeExceptionally(rejected);
            inFlight.remove(key, mine);
            observeFailure(key, rejected);
        }

        return mine;
    }

    private V awaitLoad(
            K key,
            CompletableFuture<Entry<V>> future) {

        try {
            return future.get(
                missWaitNanos, TimeUnit.NANOSECONDS).value;

        } catch (TimeoutException timeout) {
            // Deliberately do not cancel the shared future.
            throw new CacheLoadTimeoutException(
                "Timed out waiting for cache load: " + key, timeout);

        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw new CacheLoadInterruptedException(
                "Interrupted waiting for cache load: " + key,
                interrupted);

        } catch (ExecutionException failed) {
            throw new CacheLoadFailedException(
                "Cache source load failed: " + key,
                failed.getCause());
        }
    }

    private long jitteredFreshTtlNanos() {
        if (jitterFraction == 0.0) {
            return baseFreshTtlNanos;
        }

        double factor = ThreadLocalRandom.current().nextDouble(
            1.0 - jitterFraction,
            1.0 + jitterFraction);

        return Math.max(
            1L, (long) (baseFreshTtlNanos * factor));
    }

    private void observeFailure(K key, Throwable error) {
        // Observability must not change cache correctness.
        try {
            failureObserver.accept(key, error);
        } catch (RuntimeException ignored) {
            // Log this observer failure through the application's logger.
        }
    }

    private static long positiveNanos(
            Duration duration, String name) {
        Objects.requireNonNull(duration, name);

        if (duration.isZero() || duration.isNegative()) {
            throw new IllegalArgumentException(
                name + " must be positive");
        }

        return duration.toNanos();
    }

    public static final class CacheLoadTimeoutException
            extends RuntimeException {
        CacheLoadTimeoutException(String message, Throwable cause) {
            super(message, cause);
        }
    }

    public static final class CacheLoadInterruptedException
            extends RuntimeException {
        CacheLoadInterruptedException(String message, Throwable cause) {
            super(message, cause);
        }
    }

    public static final class CacheLoadFailedException
            extends RuntimeException {
        CacheLoadFailedException(String message, Throwable cause) {
            super(message, cause);
        }
    }
}
~~~

### Why this implementation is structured this way

**A. The in-flight map is separate from the value cache.**

The Caffeine cache stores bounded values. The concurrent map stores only currently active or queued loads. Once a load finishes, its future is removed using remove(key, mine).

That two-argument removal matters: it removes the entry only if the map still points to this exact future. An old completion must not delete a newer load.

**B. A stale read never waits for the source.**

When an entry is within its allowed stale window, get() calls startLoad(key) and returns the old value immediately. putIfAbsent ensures concurrent local callers share one future.

**C. A cold miss waits only for a bounded duration.**

The caller uses timed future.get(). If its own wait expires, it receives a timeout exception, but the shared load is not cancelled. Other callers may still be waiting, and the load may still populate the cache.

**D. TTL jitter and single-flight solve different problems.**

Single-flight coalesces requests for one key. Jitter spreads expiration for many different keys that might otherwise expire together.

**E. Caffeine bounds the value cache.**

A plain ConcurrentHashMap is not a memory-bounded cache. Caffeine's maximum size keeps the local value store bounded by entry count. If entries vary substantially in size, use a weight policy that approximates their memory/cost footprint.

## Step 4 — Wire it into a Spring service

~~~java
@Configuration
class ProductCacheConfiguration {

    @Bean
    StaleWhileRevalidateCache<String, ProductView> productCache(
            @Qualifier("catalogRefreshExecutor")
            ThreadPoolExecutor refreshExecutor,
            ProductRepository repository) {

        return new StaleWhileRevalidateCache<>(
            100_000,
            Duration.ofSeconds(60),
            Duration.ofSeconds(30),
            Duration.ofMillis(250),
            0.10,
            refreshExecutor,
            repository::loadProductView,
            (key, error) -> {
                // Increment a Micrometer counter and log key safely.
                // Avoid high-cardinality raw keys as metric labels.
            }
        );
    }
}
~~~

The repository loader should have its own database connection-acquisition and query timeouts. Do not let a cache refresh wait forever for the database.

## Step 5 — Dry run

Assume product key P42 currently contains value v1.

| Time | Event | Result |
|---|---|---|
| T0 | 100 requests arrive while v1 is fresh | All return v1 |
| T1 | v1 enters its stale window | First request schedules one refresh |
| T1 + 1 ms | 99 more requests arrive | All return v1; they join the same in-flight load implicitly by not scheduling a second one |
| T1 + 500 ms | Source returns v2 | Cache is replaced with v2 |
| T1 + 501 ms | Next request arrives | Returns v2 as fresh |

If the refresh executor is full at T1, the background refresh is rejected and observed, but the caller still receives v1 because stale is permitted. If there is no usable cache entry, rejection becomes an exceptional shared load and the cold-miss caller fails promptly.

---

# Part 3 — Derivation, mistakes, edge cases and trade-offs (4 minutes)

## 1. Failure matrix

| Failure | Expected behaviour |
|---|---|
| Source refresh succeeds | New value is stored, waiters receive it |
| Source refresh fails during stale window | Stale caller still gets old value; failure is observed |
| Refresh executor rejects | Stale value is still returned; cold miss fails |
| Cold-miss waiter times out | That caller fails; shared load continues |
| Cache entry is evicted | A later request behaves as a cold miss |
| Application process restarts | Local cache and in-flight work disappear; next request reloads |
| Twelve replicas receive the same cold key | Up to twelve local loads may happen without cross-replica coordination |

## 2. Common mistakes

1. **Using only ConcurrentHashMap.computeIfAbsent as the cache.** It can deduplicate a mapping operation, but it does not provide memory bounds, TTL policy, stale serving, or source admission control.
2. **Using an unbounded executor queue.** Many distinct expired keys can accumulate unlimited pending refresh work.
3. **Using CallerRunsPolicy without considering request threads.** A cache refresh may unexpectedly run on the HTTP request thread.
4. **Calling future.orTimeout() on the shared in-flight future.** This mutates the shared future and can fail every waiter. A caller-specific timed get() is safer when the shared load should continue.
5. **Cancelling a shared load when one HTTP request disconnects.** Other requests may still depend on that same load.
6. **Serving stale data for every domain.** Stale product descriptions may be acceptable; stale inventory, authorization, account balance, entitlement, or payment state may not be.
7. **Assuming local single-flight is distributed single-flight.** Every pod has its own in-memory map.

## 3. Production trade-offs and hardening

### Stale-data policy

Make the maximum stale age a domain decision. The sample allows a value to be served only until its configured stale window ends. For hard correctness domains, disable stale serving or use an authoritative read path.

### Source failure and repeated attempts

Single-flight prevents concurrent duplication while one load is in progress. It does not by itself prevent rapid sequential retries after a fast failure. Wrap the source loader in a circuit breaker and/or a per-key failure cooldown with jitter. While the circuit is open, continue serving stale data only if the domain's stale policy permits it; on a cold miss, fail fast.

### Multi-replica coordination

If one source load per pod is still too expensive, use a shared cache and consider a distributed refresh lease. A Redis lease commonly uses an atomic SET key token NX PX ttl; release must compare the stored owner token before deleting. But a lease can expire while a slow former owner is still working. If stale owners could overwrite newer results, use a monotonically increasing fencing/version token that the protected datastore checks. A lock alone is not a correctness proof.

### Operational metrics

Track at least:

- Fresh hit, stale hit, and cold-miss rates.
- Single-flight joins/coalesced callers.
- Refresh success, failure, rejection, and source latency.
- Refresh executor active workers and queue depth.
- Cache size/evictions and estimated memory.
- Age of value served and stale responses.
- Cold-miss timeout rate.
- Source DB pool wait and query latency.

Do not put raw product IDs or other high-cardinality keys into metric labels. Use structured logs or traces for sampled key-level diagnosis.

### Testing strategy

Use a controllable loader and latches:

1. Prime a key with v1.
2. Advance a fake time source or use a short test TTL.
3. Block the refresh loader.
4. Issue many concurrent stale reads.
5. Assert every caller receives v1 and the loader invocation count increases by exactly one.
6. Release the loader and assert a subsequent read returns v2.
7. Fill the refresh executor/queue and verify stale reads still return stale while cold misses fail predictably.
8. Force a loader exception and verify the failure observer is called and stale policy remains intact.

For deterministic TTL tests, inject a ticker/clock abstraction rather than sleeping in tests. Caffeine supports a custom Ticker for time-based policy tests; the sample implementation can similarly inject a monotonic time supplier in a test-oriented variant.

## 4. Interview-ready answer

> I would treat a cache stampede as a source-admission problem, not just a cache-miss problem. Within each JVM, I use a per-key single-flight future so simultaneous callers share one load. If stale data is permitted, stale-while-revalidate returns the previous value while that one refresh runs. TTL jitter spreads expiration across different keys, Caffeine bounds local memory, and a bounded refresh executor limits active and queued source work. Cold misses wait only for a caller-specific deadline; timing out one waiter does not cancel the shared load. I would explicitly call out that local single-flight does not coordinate replicas, and add a shared cache/lease only if cross-pod duplicate work justifies it. If a lease protects correctness rather than merely reducing load, the write target must enforce fencing/version checks.

## 5. Follow-up questions with answers

**Q1. Why not use only Caffeine's refreshAfterWrite?**

Caffeine's LoadingCache refresh is asynchronous: the old value remains available during refresh, and in-flight refreshes for the same key are deduplicated. That is often the simplest production choice. The custom implementation here is for making explicit the additional policies in this exercise: per-entry TTL jitter, a separate stale window, a bounded refresh executor, and a caller-specific cold-miss wait. Prefer the library's native loader/refresh features when they meet the requirements.

**Q2. Does single-flight solve cross-pod stampede?**

No. It is process-local. With N replicas, one hot key can still cause roughly N concurrent reloads. Use a shared cache and, if necessary, distributed coordination—while accounting for lease expiry and stale owners.

**Q3. Why is remove(key, mine) safer than remove(key)?**

A future can finish after a newer future has been installed. Conditional removal ensures the old task cannot delete the newer in-flight entry.

**Q4. Why not cancel the future when one request times out?**

The future is shared by multiple callers. One request's deadline is not authority to cancel work needed by other callers. Also, CompletableFuture cancellation is exceptional completion and does not inherently control the underlying source computation.

**Q5. Does TTL jitter prevent a hot-key stampede?**

Not by itself. It spreads expiration across different keys. Single-flight is what coalesces simultaneous requests for the same key.

**Q6. Can stale-while-revalidate be used for inventory?**

Only if the business contract explicitly permits bounded staleness. For inventory reservation, payment, permissions, and account balances, use authoritative conditional writes/reads or a domain-specific consistency protocol.

---

## Primary references

1. Java 21 ThreadPoolExecutor API — queueing, bounded queues and rejection policies: https://docs.oracle.com/en/java/javase/21/docs/api/java.base/java/util/concurrent/ThreadPoolExecutor.html
2. Java 21 CompletableFuture API — executor selection, copy, cancellation semantics and timeout methods: https://docs.oracle.com/en/java/javase/21/docs/api/java.base/java/util/concurrent/CompletableFuture.html
3. Caffeine — refresh semantics, stale value during refresh and in-flight deduplication: https://github.com/ben-manes/caffeine/wiki/Refresh
4. Caffeine — size-based and time-based eviction: https://github.com/ben-manes/caffeine/wiki/Eviction
5. Redis distributed locking pattern — tokenized leases and ownership-aware release: https://redis.io/docs/latest/develop/clients/patterns/distributed-locks/

**Final takeaway:** Single-flight controls duplicate work for one key, stale-while-revalidate controls caller latency, TTL jitter spreads expiry across keys, and a bounded executor protects the source. These are complementary controls—not substitutes for one another.

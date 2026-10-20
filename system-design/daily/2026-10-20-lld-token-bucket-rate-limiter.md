# LLD Interview Walkthrough — Concurrent Token Bucket Rate Limiter
**Date:** 2026-10-20 | **Java:** 17+ | **Level:** SDE2 / Senior Backend

## 1. Requirements and contract

Implement a process-local limiter with configurable bucket capacity and refill rate, a non-blocking tryAcquire(permits), thread safety, monotonic time and deterministic tests using an injected clock.

Non-goals: global quota across JVMs and persistence across restart. Those require a shared atomic mechanism or gateway.

## 2. Invariant and algorithm

Invariant: 0 <= tokens <= capacity. Before each acquisition, refill based on elapsed monotonic time, clamp to capacity, then atomically check and deduct. Synchronize the complete refill-check-deduct sequence; making only the token field volatile does not make the compound operation atomic.

~~~text
tryAcquire(n)
    validate n
    synchronized section:
        elapsed = monotonicNow - lastRefill
        tokens = min(capacity, tokens + elapsed * refillRate)
        if tokens < n: return false
        tokens -= n
        return true
~~~

## 3. Java 17 implementation

~~~java
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.function.LongSupplier;

public final class TokenBucket {
    private final double capacity;
    private final double tokensPerNano;
    private final LongSupplier nanoClock;
    private double tokens;
    private long lastRefillNanos;

    public TokenBucket(double capacity, double refillPerSecond,
                       LongSupplier nanoClock) {
        if (!Double.isFinite(capacity) || capacity <= 0) {
            throw new IllegalArgumentException("capacity must be finite and positive");
        }
        if (!Double.isFinite(refillPerSecond) || refillPerSecond <= 0) {
            throw new IllegalArgumentException("refill rate must be finite and positive");
        }
        this.nanoClock = Objects.requireNonNull(nanoClock);
        this.capacity = capacity;
        this.tokensPerNano = refillPerSecond / TimeUnit.SECONDS.toNanos(1);
        this.tokens = capacity;
        this.lastRefillNanos = nanoClock.getAsLong();
    }

    public synchronized boolean tryAcquire(int permits) {
        if (permits <= 0 || permits > capacity) {
            throw new IllegalArgumentException("permits must be positive and <= capacity");
        }
        refill();
        if (tokens < permits) return false;
        tokens -= permits;
        return true;
    }

    private void refill() {
        long now = nanoClock.getAsLong();
        long elapsed = now - lastRefillNanos;
        if (elapsed <= 0) return;
        tokens = Math.min(capacity, tokens + elapsed * tokensPerNano);
        lastRefillNanos = now;
    }
}
~~~

### Why these choices?
- synchronized protects refill, check and deduction as one atomic operation. A volatile double alone would not.
- System.nanoTime() measures elapsed duration; wall-clock adjustments must not create or remove tokens.
- Injecting LongSupplier makes time deterministic in tests.
- Refill is lazy; no background thread is needed.
- The method returns false immediately rather than queueing callers, leaving overload policy explicit at the API layer.

## 4. Deterministic and concurrent tests

~~~java
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

public class TokenBucketTest {
    public static void main(String[] args) throws Exception {
        AtomicLong clock = new AtomicLong(0);
        TokenBucket bucket = new TokenBucket(2, 1.0, clock::get);

        check(bucket.tryAcquire(2), "initial capacity");
        check(!bucket.tryAcquire(1), "bucket empty");
        clock.addAndGet(TimeUnit.SECONDS.toNanos(1));
        check(bucket.tryAcquire(1), "one token refilled");
        check(!bucket.tryAcquire(1), "token consumed");

        AtomicLong fixedClock = new AtomicLong(0);
        TokenBucket single = new TokenBucket(1, 0.1, fixedClock::get);
        int callers = 100;
        ExecutorService pool = Executors.newFixedThreadPool(20);
        CountDownLatch ready = new CountDownLatch(20);
        CountDownLatch start = new CountDownLatch(1);
        CountDownLatch done = new CountDownLatch(callers);
        AtomicInteger winners = new AtomicInteger();

        for (int i = 0; i < callers; i++) {
            pool.execute(() -> {
                ready.countDown();
                try {
                    start.await();
                    if (single.tryAcquire(1)) winners.incrementAndGet();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } finally {
                    done.countDown();
                }
            });
        }
        ready.await();
        start.countDown();
        if (!done.await(5, TimeUnit.SECONDS)) {
            pool.shutdownNow();
            throw new AssertionError("test timed out");
        }
        pool.shutdownNow();
        check(winners.get() == 1, "exactly one caller gets the only token");
        System.out.println("All token bucket checks passed");
    }

    private static void check(boolean condition, String message) {
        if (!condition) throw new AssertionError(message);
    }
}
~~~

The fake clock does not advance during the concurrent test, so only one caller can consume the initial token. In a JUnit suite, put executor shutdown in a finally block so a failing assertion cannot leak worker threads.

## 5. Complexity and trade-offs

Each call is O(1) time and O(1) memory. Synchronization serializes calls for one bucket; that can contend for an extremely hot key. Sharding per tenant or using striped state may improve throughput but complicates exact aggregate limits.

A process-local bucket resets on restart and each replica has its own capacity. Ten replicas each configured for 100 tokens can allow an aggregate burst near 1,000. A global quota requires a shared atomic operation (for example a Redis Lua script with server-side time and a defined failure policy) or a gateway that owns the quota. A shared store introduces network latency and availability trade-offs.

## 6. Production follow-ups

- **Clock:** monotonic elapsed time, not wall-clock timestamps.
- **Invalid request:** reject permits <= 0 or greater than capacity.
- **Floating point:** acceptable for approximate rate limiting; strict accounting may need fixed-point units and documented rounding.
- **Fairness:** synchronized access does not guarantee FIFO fairness.
- **Failure policy:** choose fail-open versus fail-closed per endpoint risk if a distributed limiter is unavailable.
- **Metrics:** allowed/rejected count, hot keys and limiter-store errors; avoid unbounded tenant labels.
- **Tests:** initial burst, refill boundary, long idle period clamps to capacity, concurrent final-token race, invalid permits and no clock advance.

**Spoken answer:** “I keep refill, check and deduction atomic; use monotonic time and an injectable clock; test the final-token race; and distinguish a process-local limiter from a globally enforced quota.”

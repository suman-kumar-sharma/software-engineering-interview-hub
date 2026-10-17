# LLD — Concurrent Token Bucket Rate Limiter (2026-10-17)
**Java 17, 45-minute Senior Backend exercise.** Single-JVM implementation, not a distributed quota.

## Requirements and design
Expose tryAcquire(key, permits). Each key has a bucket with capacity and refill rate. Multiple threads must never overspend tokens. Use ConcurrentHashMap for bucket lookup and a ReentrantLock per bucket to make refill + check + deduction atomic. Inject a monotonic nanosecond clock to make tests deterministic; refill lazily on requests.

**Invariant:** 0 <= tokens <= capacity. Every successful call consumes permits; rejected calls do not.

## Complete runnable Java 17
Save as TokenBucketLimiter.java:
~~~java
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.LongSupplier;

public final class TokenBucketLimiter {
  private final int capacity;
  private final double perNano;
  private final LongSupplier clock;
  private final ConcurrentHashMap<String, Bucket> buckets =
      new ConcurrentHashMap<>();

  private static final class Bucket {
    final ReentrantLock lock = new ReentrantLock();
    double tokens;
    long last;
    Bucket(int capacity, long now) {
      tokens = capacity;
      last = now;
    }
  }

  public TokenBucketLimiter(int capacity, double perSecond,
                            LongSupplier clock) {
    if (capacity <= 0 || !Double.isFinite(perSecond) ||
        perSecond <= 0 || clock == null)
      throw new IllegalArgumentException("bad configuration");
    this.capacity = capacity;
    this.perNano = perSecond / 1_000_000_000.0;
    this.clock = clock;
  }

  public boolean tryAcquire(String key, int permits) {
    if (key == null || permits <= 0 || permits > capacity)
      throw new IllegalArgumentException("bad request");
    Bucket b = buckets.computeIfAbsent(key,
        k -> new Bucket(capacity, clock.getAsLong()));
    b.lock.lock();
    try {
      long now = clock.getAsLong();
      long elapsed = Math.max(0L, now - b.last);
      b.tokens = Math.min(capacity, b.tokens + elapsed * perNano);
      if (now > b.last) b.last = now;
      if (b.tokens < permits) return false;
      b.tokens -= permits;
      return true;
    } finally {
      b.lock.unlock();
    }
  }

  public static void main(String[] args) {
    java.util.concurrent.atomic.AtomicLong time =
        new java.util.concurrent.atomic.AtomicLong();
    TokenBucketLimiter limiter =
        new TokenBucketLimiter(3, 1.0, time::get);
    System.out.println(limiter.tryAcquire("u1", 2)); // true
    System.out.println(limiter.tryAcquire("u1", 2)); // false
    time.addAndGet(1_000_000_000L);
    System.out.println(limiter.tryAcquire("u1", 2)); // true
  }
}
~~~

## Dry run, tests and complexity
Capacity=3, refill=1/sec. At t=0 take 2 => one token remains. Immediately take 2 => rejected. At t=1s refill to 2 => take 2 succeeds. **Race test:** Freeze fake clock, run 100 concurrent one-permit requests against capacity 10; exactly 10 succeed. Test separate keys, invalid permits, idle refill saturation and fractional refill.

Expected O(1) map lookup and per-key update, O(number of keys) memory. Hot keys serialize on their lock. Double precision can drift at extremes; use fixed-point for strict accounting. An unbounded number of keys causes memory growth; production eviction must define whether quota resets are acceptable.

## Distributed extension and interview answer
Local map cannot enforce quota across Spring Boot replicas. For global limits use an atomic Redis script or authoritative rate-limit service; define TTL, hot keys, clock policy and fail-open/fail-closed behavior. Sliding windows offer different burst semantics.

**Interview:** “I use lazy refill with an injected monotonic clock and per-key synchronization; I make the local-vs-distributed scope explicit.”

Reference: https://docs.oracle.com/en/java/javase/17/docs/api/java.base/java/util/concurrent/ConcurrentHashMap.html

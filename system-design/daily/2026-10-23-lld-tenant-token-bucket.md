
# LLD — Tenant-Aware Token Bucket (Java 17)
**Date:** 2026-10-23

## Requirements and algorithm
Each tenant has capacity B and refill rate r tokens/second. A request of cost c succeeds only when c tokens are available. Burst is bounded by B; long-term rate trends toward r. This implementation is thread-safe inside one JVM and testable without sleeping. It is not a globally shared quota.

For each tenant store token balance and last monotonic timestamp:
1. elapsed = max(0, now - lastRefill)
2. tokens = min(capacity, tokens + elapsed * refillRate)
3. subtract cost if enough tokens exist
4. update last timestamp

## Java 17 implementation
~~~java
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.LongSupplier;

public final class TenantTokenBucket {
    private static final double NANOS_PER_SECOND = 1_000_000_000.0;
    private final double capacity;
    private final double refillPerSecond;
    private final LongSupplier clock;
    private final ConcurrentHashMap<String, Bucket> buckets =
            new ConcurrentHashMap<>();

    public TenantTokenBucket(double capacity, double refillPerSecond) {
        this(capacity, refillPerSecond, System::nanoTime);
    }

    public TenantTokenBucket(double capacity, double refillPerSecond,
                             LongSupplier clock) {
        if (!Double.isFinite(capacity) || capacity <= 0)
            throw new IllegalArgumentException("capacity must be finite and > 0");
        if (!Double.isFinite(refillPerSecond) || refillPerSecond <= 0)
            throw new IllegalArgumentException("refill rate must be finite and > 0");
        this.capacity = capacity;
        this.refillPerSecond = refillPerSecond;
        this.clock = Objects.requireNonNull(clock);
    }

    public boolean tryAcquire(String tenantId, double cost) {
        if (tenantId == null || tenantId.isBlank())
            throw new IllegalArgumentException("tenantId is required");
        if (!Double.isFinite(cost) || cost <= 0 || cost > capacity)
            throw new IllegalArgumentException("cost must be in (0, capacity]");
        Bucket bucket = buckets.computeIfAbsent(
                tenantId, ignored -> new Bucket(capacity, clock.getAsLong()));
        return bucket.tryAcquire(cost, clock.getAsLong(), refillPerSecond);
    }

    private static final class Bucket {
        private final double capacity;
        private double tokens;
        private long lastRefillNanos;

        Bucket(double capacity, long now) {
            this.capacity = capacity;
            this.tokens = capacity;
            this.lastRefillNanos = now;
        }

        synchronized boolean tryAcquire(double cost, long now, double rate) {
            long elapsed = now - lastRefillNanos;
            if (elapsed > 0) {
                tokens = Math.min(capacity,
                        tokens + (elapsed / NANOS_PER_SECOND) * rate);
                lastRefillNanos = now;
            }
            if (tokens < cost) return false;
            tokens -= cost;
            return true;
        }
    }

    public int trackedTenantCount() {
        return buckets.size(); // production needs bounded eviction
    }
}
~~~

## Dry run
Capacity 5, refill 1 token/sec, bucket starts full. Cost 4 succeeds, leaving 1. Immediate cost 2 fails. Advance a fake clock by 2 seconds: two tokens refill, balance becomes 3; cost 2 succeeds and leaves 1. Cost greater than capacity is rejected because it can never pass.

## Complexity and correctness
Expected O(1) map lookup and a short per-bucket synchronized section; O(number of tracked tenants) memory. Synchronization makes refill/check/deduct atomic for one tenant while other tenants proceed independently. System.nanoTime measures elapsed time; wall clock can jump.

## Tests
- Initial full bucket and exhaustion.
- Fractional refill using an injected fake clock.
- Balance never exceeds capacity.
- Many concurrent calls with no time advance: accepted total cost cannot exceed initial capacity.
- Different tenants have independent balances.
- Invalid tenant, cost, capacity and refill rate are rejected.
- A backwards test-clock value must not grant tokens.

## Production caveats
- **Memory:** arbitrary tenant IDs can grow the map forever. Use authenticated IDs and bounded expiry/eviction (for example Caffeine), or a shared store.
- **Multiple instances:** each JVM has its own bucket, so N instances can allow roughly N times the configured budget. Partition quota among instances or use an atomic Redis/Lua script; define failover behavior.
- **Fairness:** token bucket limits rate but does not guarantee fairness among request classes. Add weighted priority or per-route buckets if required.
- **Configuration changes:** define how existing buckets adapt when capacity/refill rate changes.
- **Retry-after:** calculate from token deficit and refill rate; do not expose unrelated tenant state.

**Follow-ups:** Fixed windows allow boundary bursts; sliding logs are more precise but retain per-request timestamps. System.nanoTime is appropriate for elapsed durations. This implementation is not globally exact.

**Spoken answer:** “I keep a token balance and monotonic refill timestamp per tenant. Per-bucket synchronization makes refill and deduction atomic. The algorithm is O(1), but production requires bounded tenant cardinality and an explicit multi-instance quota strategy.”

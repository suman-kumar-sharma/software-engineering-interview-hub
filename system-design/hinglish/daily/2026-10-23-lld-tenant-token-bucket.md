
# LLD Classroom — Tenant-Aware Token Bucket (Java 17)
**Date:** 2026-10-23

## 1. Problem ko example se samjho
Maan lo tenant ko 5 requests ka burst allow hai aur 1 request/second ka refill rate hai. Token bucket mein request token consume karti hai; time ke saath tokens refill hote hain. Bucket full hone ke baad extra tokens add nahi hote.

Fixed window mein 12:00:59 par 5 aur 12:01:00 par 5 requests aa sakti hain. Boundary par burst double ho sakta hai. Token bucket burst ko capacity se cap karta hai aur long-term rate control karta hai.

## 2. State aur algorithm
Har tenant ke liye tokens aur lastRefillNanos rakho. Request par elapsed time nikalo, tokens refill karo, capacity tak clamp karo, phir cost afford ho toh deduct karo. Ye poora sequence atomic hona chahiye, warna do threads same token spend kar sakte hain.

## 3. Java 17 implementation
~~~java
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.LongSupplier;

public final class TenantTokenBucket {
    private static final double NANOS_PER_SECOND = 1_000_000_000.0;
    private final double capacity, refillPerSecond;
    private final LongSupplier clock;
    private final ConcurrentHashMap<String, Bucket> buckets =
            new ConcurrentHashMap<>();

    public TenantTokenBucket(double capacity, double refillPerSecond) {
        this(capacity, refillPerSecond, System::nanoTime);
    }

    public TenantTokenBucket(double capacity, double refillPerSecond,
                             LongSupplier clock) {
        if (!Double.isFinite(capacity) || capacity <= 0)
            throw new IllegalArgumentException("invalid capacity");
        if (!Double.isFinite(refillPerSecond) || refillPerSecond <= 0)
            throw new IllegalArgumentException("invalid refill rate");
        this.capacity = capacity;
        this.refillPerSecond = refillPerSecond;
        this.clock = Objects.requireNonNull(clock);
    }

    public boolean tryAcquire(String tenantId, double cost) {
        if (tenantId == null || tenantId.isBlank())
            throw new IllegalArgumentException("tenantId required");
        if (!Double.isFinite(cost) || cost <= 0 || cost > capacity)
            throw new IllegalArgumentException("invalid cost");
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

    public int trackedTenantCount() { return buckets.size(); }
}
~~~

## 4. Code walkthrough
ConcurrentHashMap tenant ke hisaab se bucket find/create karta hai. Bucket ke synchronized method ke andar refill aur token deduction dono hote hain. Isliye same tenant ki concurrent requests ek token ko double-spend nahi kar sakti; different tenants ki buckets independent hain.

System.nanoTime elapsed duration measure karta hai. currentTimeMillis wall clock hai, jo NTP ya manual adjustment se jump kar sakti hai. Double.isFinite NaN aur infinity ko reject karta hai. Cost capacity se badi ho toh request kabhi pass nahi ho sakti, isliye validation mein reject karo.

## 5. Dry run
Capacity 5, refill 1 token/second:
1. Start par 5 tokens.
2. Cost 4 allow; 1 bacha.
3. Turant cost 2 reject.
4. Fake clock 2 seconds aage: 2 tokens refill, balance 3.
5. Cost 2 allow; balance 1.

## 6. Complexity aur tests
Expected O(1) map lookup plus short synchronized section; memory O(unique tenants). Fake clock inject karke test karo, Thread.sleep ki zaroorat nahi.

Tests: initial full bucket, exhaustion, fractional refill, capacity se upar refill na hona, same tenant par many concurrent requests, independent tenant budgets, invalid cost/config, aur backward clock reading. Agar time advance nahi hua aur initial capacity 5 hai, toh 100 concurrent threads ke bawajood accepted total cost 5 se zyada nahi hona chahiye.

## 7. Production mein kya change hoga?
**Memory leak risk:** har arbitrary tenant ID ke liye bucket permanently rahi toh map badhta rahega. Authenticated tenant IDs use karo aur bounded expiry/eviction lagao.

**Multiple app instances:** Har JVM ka apna bucket hai. 5 instances hone par tenant ko approx 5x quota mil sakta hai. Distributed limit ke liye Redis Lua script mein refill/check/deduct atomic karo, ya quota instances mein partition karo. Redis failover par behavior define karo.

Token bucket rate limit karta hai, fairness guarantee nahi. Hot tenant ke request classes ke liye weighted priority ya per-route buckets chahiye ho sakte hain. Capacity/rate configuration badalne par existing bucket ka migration behavior bhi define karo.

**Interviewer follow-ups:** Fixed window mein boundary burst hota hai. Sliding log precise hai par per-request timestamps store karta hai. System.nanoTime elapsed duration ke liye sahi hai. Kya current implementation globally exact hai? Nahi, JVM-local hai.

**Professional English answer:** “I keep a token balance and monotonic refill timestamp per tenant. Per-bucket synchronization makes refill and deduction atomic. The algorithm is O(1), but production requires bounded tenant cardinality and an explicit multi-instance quota strategy.”

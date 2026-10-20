# LLD Classroom Hinglish — Concurrent Token Bucket Rate Limiter
**Date:** 2026-10-20 | **Java:** 17+ | **Level:** SDE2 / Senior Backend

## 1. Requirement ko example se samjho

Maan lo har tenant ko average 10 requests per second allow karni hain, lekin short burst mein 20 requests tak allow kar sakte hain. Bucket ki capacity 20 hai. Request aati hai toh token consume hota hai; time ke saath token refill hote hain. Bucket full hone ke baad extra refill store nahi karna, warna burst limit toot jayegi.

Contract: tryAcquire(permits) turant true/false dega, threads ke beech safe hoga aur test mein time control kar paayenge. Ye local JVM limiter hai; multiple replicas ka global quota is design ka part nahi.

## 2. Core invariant aur approach

Invariant hai: 0 <= tokens <= capacity. Har request par pehle elapsed time se refill calculate karo, capacity se clamp karo, phir check karo ki enough tokens hain ya nahi, aur agar hain toh deduct karo.

**Ye sab ek synchronized section mein kyun?** Socho bucket mein ek token bacha hai aur do threads ek saath check karte hain. Agar check aur decrement alag atomic operations nahi hain, dono true dekh kar token consume kar sakte hain. Isliye refill + check + deduction ko ek operation samjho.

~~~text
tryAcquire(n)
  n validate karo
  synchronized section mein:
    elapsed = monotonicNow - lastRefill
    tokens = min(capacity, tokens + elapsed * rate)
    enough nahi -> false
    warna tokens -= n, true
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

### Code ko samjho
- synchronized poore refill-check-deduct ko atomic banata hai. Sirf volatile field enough nahi.
- System.nanoTime() elapsed duration ke liye hai. Wall clock adjust hui toh refill ulta-seedha nahi hona chahiye.
- LongSupplier inject kiya hai taaki test mein fake clock advance kar sakein.
- Background thread nahi chahiye; refill lazily har request par hota hai.
- false turant return hota hai; request queue karna ya 429 bhejna API layer ka decision hai.

## 4. Deterministic aur concurrent test

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
        CountDownLatch ready = new CountDownLatch(callers);
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

Fake clock concurrent test mein advance nahi hoti, isliye 100 callers mein se sirf ek ko initial token milna chahiye. Real JUnit suite mein executor shutdown finally block mein rakho taaki failed assertion worker threads leak na kare.

## 5. Complexity aur production trade-offs

Har call O(1) time aur O(1) memory hai. Ek bucket par synchronized calls serialize hoti hain; hot key par contention ho sakta hai. Per-tenant sharding throughput improve kar sakti hai, par exact aggregate limit manage karna harder hai.

Local bucket restart par reset ho jaata hai aur har replica ka bucket alag hai. 10 replicas par capacity 100 each ho toh aggregate burst lagbhag 1000 tak ho sakta hai. Global quota ke liye shared atomic operation, jaise Redis Lua script with server-side time, ya quota-owning gateway chahiye. Shared store latency aur availability trade-off introduce karta hai.

## 6. Interviewer ke tricky follow-ups

- Clock: elapsed time ke liye monotonic clock; wall clock nahi.
- Invalid permits: zero, negative ya capacity se bade permits reject karo.
- Floating point: approximate rate limiting mein okay; strict accounting ke liye fixed-point aur rounding rules define karo.
- Fairness: synchronized FIFO fairness guarantee nahi karta.
- Distributed store down: endpoint risk ke hisaab se fail-open ya fail-closed decide karo.
- Metrics: allowed/rejected count, hot tenant aur store errors; tenant labels ki unbounded cardinality se bacho.
- Tests: initial burst, refill boundary, long idle ke baad capacity clamp, concurrent last-token race aur invalid input.

**Interview mein bolo:** “I make refill, check and deduction atomic, use monotonic time with an injectable clock, test the last-token race, and clearly distinguish a local limiter from a globally enforced quota.”

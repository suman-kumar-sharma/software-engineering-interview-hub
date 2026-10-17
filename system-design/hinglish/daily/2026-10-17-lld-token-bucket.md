# LLD — Token Bucket Rate Limiter | Hinglish Classroom (2026-10-17)
**Senior Backend 45-minute interview.** Java 17 implementation English companion mein hai: [Complete Java code](../../daily/2026-10-17-lld-token-bucket.md).

## 1. Socho ek real-world situation
Tumhari API ko per user 5 requests ki burst allow karni hai, phir har second 1 naya request token milna chahiye. Agar user 10 requests ek saath bheje toh unlimited requests accept nahi kar sakte. Isliye token bucket use karte hain.

**Bucket ko water tank samjho:** Capacity 5 tokens. Har accepted request ek token consume karti hai. Time ke saath refill hota hai, lekin bucket capacity se upar nahi ja sakta. Fixed-window counter mein minute boundary par double burst possible hota hai; token bucket smooth refill aur bounded burst deta hai.

## 2. Requirement aur invariant
Method: tryAcquire(userId, permits) -> true/false. Multiple threads ek hi user ko hit karenge. **Invariant:** Available tokens 0 se capacity ke beech rahenge. Agar sirf 1 token hai toh do threads simultaneously 1-1 token successfully nahi le sakte.

**Class design:** ConcurrentHashMap<String,Bucket> se per-user bucket; har Bucket mein tokens, lastRefillNanos aur ReentrantLock. Clock LongSupplier inject karo taaki unit tests mein fake time use kar sako. System.nanoTime elapsed duration ke liye suitable hai, wall-clock Date nahi.

## 3. Step-by-step algorithm
1. Validate user key aur permits. Agar permits > capacity hai toh reject/invalid input.
2. Map mein user ka bucket find/create karo.
3. Bucket lock acquire karo. Yahi atomicity boundary hai.
4. elapsed = max(0, now - lastRefill). Tokens = min(capacity, tokens + elapsed * refillRate).
5. Agar tokens < permits toh false; otherwise deduct karke true.
6. Finally lock release.

**Ab tum poochoge: ConcurrentHashMap already thread-safe hai, lock kyun?** Kyunki map structure safe hone ka matlab bucket ke tokens check aur deduct atomic hona nahi. Do threads same token dekhkar dono allow kar sakte hain. Per-key lock is race ko rokta hai. Alag users ke buckets parallel proceed kar sakte hain.

## 4. Dry run
Capacity=3, refill=1 token/sec.
- t=0: tokens=3. User 2 permits maangta hai -> true, tokens=1.
- t=0 immediately: 2 permits -> false, tokens=1.
- t=1 second: refill +1 -> tokens=2. User 2 maangta hai -> true, tokens=0.
- t=10 seconds: refill many tokens, lekin max capacity=3 hi hogi.

**Complexity:** Expected O(1) map lookup/update, memory O(unique users). Ek hot user par lock contention hoga.

## 5. Java code ka important section
~~~java
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
~~~
Yahan sabse important baat: refill, available-token check aur deduction **same lock** ke andar hain. Agar check lock ke bahar kiya toh race condition wapas aa jayegi.

## 6. Interviewer ka twist
**100 threads ek saath request bhejein:** Capacity 10, clock freeze, har thread 1 permit maange. Exactly 10 success hone chahiye. Fake clock use karke tests deterministic banao; Thread.sleep-based tests flaky hote hain.

**Million unique user IDs:** Map unbounded grow karega. Idle eviction/size cap chahiye, lekin eviction bucket reset kar sakta hai—attacker quota bypass kar sakta hai. Policy define karo.

**Multiple JVM replicas:** Local limiter global quota nahi enforce karega. Redis atomic Lua script ya centralized service use karo. Redis unavailable ho toh fail-open ya fail-closed product/security requirement ke hisaab se choose karo.

**Floating-point:** Double accounting extreme values par drift kar sakti hai. Strict quota accounting ke liye fixed-point integer implementation consider karo.

## 7. 45-minute interview script
5m requirements; 5m invariant; 10m classes; 10m Java walkthrough; 5m concurrency test; 5m eviction; 5m distributed design.

**Interview English:** “I use a monotonic clock, lazy token refill and per-key locking to ensure atomic permit deduction. I explicitly separate single-instance from distributed rate limiting.”

**Full code and reference:** [English LLD](../../daily/2026-10-17-lld-token-bucket.md) ; https://docs.oracle.com/en/java/javase/17/docs/api/java.base/java/util/concurrent/ConcurrentHashMap.html

# Interview Preparation — 2026-10-12 | Proper Hinglish Edition

> **Teacher mode ON:** Har concept ko aise samjhenge jaise class mein whiteboard par padha rahe hain. Situation → reasoning → solution → tricky follow-up → interview English. Ratta nahi, intuition build karna hai.

**Original technical report:** [English edition](../../../2026/10/Interview_Preparation_2026-10-12.md). Detailed original Java code, source links, research verification aur engineering updates wahan hain. Ye file Hinglish mein standalone concept walkthrough hai.

## Q1. Thread-safe TTL cache bina library

Maan lo tumne OTP cache mein store kiya jo 5 minute valid hai. Agar 6 minute baad bhi cache OTP return kar de toh security bug ho jayega. TTL ka matlab value ke saath expiry time bhi track karna.

**Design kaise socha?** `ConcurrentHashMap<Key, Entry>` rakho. Entry mein value aur expiry timestamp. `get()` par check karo: abhi expiry se pehle hai toh value return; otherwise miss. Background cleanup memory free karne ke liye hai, correctness ke liye mandatory nahi.

```java
record Entry<V>(V value, long expiresAtNanos) {}
ConcurrentHashMap<String, Entry<String>> cache = new ConcurrentHashMap<>();
// Read time par expiry verify karo; stale entry return mat karo.
```

**Tricky race:** Cleanup old expired entry delete karne gaya; tab tak kisi ne fresh value insert kar di. Blind `remove(key)` nayi value bhi delete kar sakta hai. `remove(key, oldEntry)` conditional removal better hai.

**Interview mein:** "I enforce expiration on reads and use conditional concurrent updates so cleanup cannot remove a refreshed entry."

---

## Q2. Virtual threads: asli fayda kya hai?

Socho 1000 customers restaurant mein order ka wait kar rahe hain. Har customer ke liye ek expensive dedicated waiter rakhna inefficient hoga. Virtual threads waiting tasks ko lightweight tarike se manage karne mein help karte hain.

Java virtual threads JVM-managed hain. Jab blocking I/O wait hota hai, carrier OS thread dusre tasks run kar sakta hai. Isliye blocking I/O workloads mein concurrency improve ho sakti hai.

**Lekin twist:** Tumhare DB pool mein 20 connections hain. 10,000 virtual threads bana diye, toh bhi ek waqt mein 10,000 DB queries execute nahi hongi. CPU cores bhi same hain. Virtual threads throughput magically unlimited nahi banate.

**Interview mein:** "Virtual threads make blocking concurrency cheaper, but I still enforce connection-pool limits, timeouts and downstream backpressure."

---

## Q3. ConcurrentHashMap ke andar object safe hai kya?

Maan lo tumhare paas strong locker hai, par locker ke andar rakhi diary ko do log ek saath edit kar rahe hain. Locker safe hone se diary ke updates safe nahi ho jaate.

`ConcurrentHashMap<String, Counter>` map access safe banata hai. Lekin agar `Counter.count++` mutable field hai toh race ho sakti hai.

**Approach:** Immutable value + `compute()`, `AtomicInteger`, ya appropriate locking use karo. Agar business operation do map keys ko ek saath update karta hai, per-key atomicity bhi enough nahi.

**Interview mein:** "ConcurrentHashMap protects its own operations, not arbitrary mutable state or cross-key invariants."

---

## Q4. Deployment ke baad p99 latency 3 second

Tumne deploy kiya aur API 200ms se 3 second ho gayi. Sabse pehle server size double kar doge? Nahi! Pehle pata karo bottleneck kahan hai.

**Step 1:** Before/after p95, p99, error rate compare karo. **Step 2:** Distributed trace dekho: API time DB mein ja raha hai ya downstream HTTP mein? **Step 3:** CPU, GC pauses, thread dumps, DB connection pool wait, slow SQL aur deployment diff check karo.

Example: CPU 30% hai lekin DB connection ke liye 2 second wait ho raha hai. CPU scaling se issue solve nahi hoga. Pool blindly badhaya toh DB overload ho sakta hai.

**Interview mein:** "I use latency decomposition and saturation metrics to isolate the regression before changing capacity or rolling back."

---

## Q5. Spring @Transactional self-invocation

Spring ka transaction proxy ko gatekeeper samjho. Bahar se service method proxy ke through call hota hai toh gatekeeper transaction start kar sakta hai. Lekin same class ke andar `this.savePayment()` call proxy bypass kar deta hai.

```java
@Service
class PaymentService {
    void checkout() { savePayment(); } // same-object call
    @Transactional
    public void savePayment() { /* database work */ }
}
```

Default proxy mode mein self-invocation se `@Transactional` advice apply nahi ho sakti. Isliye outer public transactional boundary banao ya transactional operation ko separate injected bean mein move karo.

**Interview mein:** "Proxy-based transactional advice is bypassed by self-invocation, so I move the transaction boundary to a proxied service call."

---

## Q6. Billions of rows: keyset pagination

Maan lo feed mein tum page number 50,000 par ja rahe ho. `OFFSET 1000000 LIMIT 20` mein database ko bahut rows skip karni pad sakti hain. Ye slow ho sakta hai.

**Better approach:** Last page ke last item ka cursor rakho. Example ordering `created_at DESC, id DESC`. Next page mein `(created_at, id) < (:lastTime, :lastId)` condition use karo, saath matching composite index.

**Why id?** Do records ka timestamp same ho sakta hai. ID deterministic tie-breaker deta hai.

**Interview mein:** "Keyset pagination follows an indexed cursor rather than scanning and discarding large offsets."

---

## Q7. Sliding Window Maximum O(n)

Array `[1,3,-1,-3,5,3,6,7]`, window size 3. Pehli window ka max 3, doosri ka bhi 3. Har window ko separately scan karoge toh O(nk).

**Trick:** Deque mein useful candidates ke **indices** rakho. New number aaya toh peeche se chhote/equal candidates hata do—woh future maximum nahi ban sakte. Front se expired index hata do. Front current max hai.

**Dry run result:** `[3,3,5,5,6,7]`. Har index at most once add/remove hota hai, isliye total **O(n)** time aur **O(k)** space.

**Interview mein:** "A monotonic deque stores candidate maxima, giving amortized linear time."

---

## Q8. Reliable webhook dispatcher

Tumhare app ko merchant ko "payment succeeded" event bhejna hai. HTTP request bheji, lekin response timeout ho gaya. Ab kya merchant ne event receive kiya? Ho bhi sakta hai, nahi bhi. Blind retry se duplicate event aa sakta hai.

**Design:** Business transaction ke saath outbox event durably save karo. Worker queue se event uthaye, signature ke saath POST kare, timeout par exponential backoff + jitter retry kare. Repeated failures DLQ mein jayen. Receiver unique event ID se deduplicate kare.

**Important:** "Exactly once HTTP delivery" ka promise mat karo. Durable intent + at-least-once attempts + idempotent consumer practical design hai.

**Interview mein:** "I use a transactional outbox, durable retries and event IDs so webhook delivery is reliable despite duplicate attempts."

---

## Q9. Optional: orElse vs orElseGet, map vs flatMap

Socho tumhare paas lunch already hai. `orElse(cookExpensiveMeal())` likhne par expensive meal phir bhi cook ho jayega, kyunki argument eagerly evaluate hota hai. `orElseGet(() -> cookExpensiveMeal())` tabhi cook karega jab Optional empty ho.

`map` wrapped value transform karta hai. Agar mapper already `Optional<T>` return karta hai toh nested `Optional<Optional<T>>` ban sakta hai. `flatMap` is nesting ko flatten karta hai.

**Interview mein:** "I use orElseGet for lazy fallbacks and flatMap for Optional-returning transformations."

---

## Q10. 132 pattern O(n) mein

Pattern ka naam thoda confusing hai. Hume teen indices `i < j < k` chahiye jinke values satisfy karein: `nums[i] < nums[k] < nums[j]`.

Example `[3,1,4,2]` mein `1,4,2` valid 132 pattern hai.

**Brute force:** Teen nested loops O(n³). **Optimize:** Right se left scan karo. Monotonic stack se bade candidate values track karo aur `third` naam ka value maintain karo jo 132 ka middle value ban sakta hai. Agar current number `third` se chhota mil gaya, pattern found.

```java
boolean find132pattern(int[] a) {
    Deque<Integer> stack = new ArrayDeque<>();
    int third = Integer.MIN_VALUE;
    for (int i = a.length - 1; i >= 0; i--) {
        if (a[i] < third) return true;
        while (!stack.isEmpty() && a[i] > stack.peek()) {
            third = stack.pop();
        }
        stack.push(a[i]);
    }
    return false;
}
```

Har number stack mein once push aur at most once pop hota hai: **O(n)** time, **O(n)** space. Strict inequality hai, isliye duplicates carefully handle karo.

**Interview mein:** "I scan right to left with a monotonic stack and track the candidate middle value in linear time."

---

## Aaj ka revision plan

Sabse pehle Q1 TTL cache aur Q3 ConcurrentHashMap races explain karo. Phir Q7 deque dry-run aur Q10 monotonic stack khud solve karo. Last mein Q8 webhook design ko whiteboard par 5 minute mein bolkar practice karo.

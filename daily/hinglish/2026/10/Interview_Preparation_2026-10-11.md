# Interview Preparation — 2026-10-11 | Proper Hinglish Edition

> **Classroom mode:** Socho main whiteboard pe padha raha hoon. Har question mein pehle relatable example, phir problem ka root cause, phir solution ka reasoning, phir interview ka twist. Technical code English mein hi hai.

**Original detailed report:** [English edition](../../../2026/10/Interview_Preparation_2026-10-11.md). Usme additional Java implementations, sources, detailed technical discussion aur engineering updates available hain. Ye file Hinglish mein padhne ke liye standalone conceptual walkthrough hai.

## Q1. Map.merge se account-wise transaction total

Socho tum ek banking app bana rahe ho. Transactions aaye: A ko 100, B ko 40, A ko 70. Ab account A ka total 170 aur B ka 40 chahiye. Har transaction ke liye poori list dobara scan karoge toh time waste hoga. Ek map mein account ID ko key aur running total ko value rakho.

**Approach kaise socha?** Pehli baar A mile toh amount store karo. Agli baar A mile toh existing amount mein add karo. Java ka `Map.merge()` exactly ye do cases handle karta hai.

```java
Map<String, Long> totals = new HashMap<>();
for (Transaction t : transactions) {
    totals.merge(t.accountId(), t.amountInPaise(), Long::sum);
}
```

**Dry run:** A:100 → {A=100}; B:40 → {A=100,B=40}; A:70 → {A=170,B=40}. Ek transaction ek hi baar process hui, isliye average **O(n)** time aur **O(k)** extra space (k unique accounts).

**Twist:** Real money ko floating-point `double` mein store karna risky hai. Paise as `long` ya `BigDecimal` better. Duplicate transaction IDs ko count karna hai ya reject, pehle clarify karo.

**Interview mein:** "I aggregate amounts in one pass with Map.merge, achieving expected linear time and space proportional to distinct accounts."

---

## Q2. ExecutorService aur CompletableFuture

Imagine ek restaurant. `ExecutorService` chefs ki team aur task queue manage karta hai. `CompletableFuture` batata hai order ka workflow: pehle cooking, phir packing, phir delivery. Dono ek dusre ke replacement nahi hain.

**Kaise decide karoge?** Tumhe sirf background task execute karna hai toh executor enough ho sakta hai. Agar multiple asynchronous steps ko chain, combine aur recover karna hai toh `CompletableFuture` convenient hai.

```java
ExecutorService pool = Executors.newFixedThreadPool(8);
CompletableFuture<String> result =
    CompletableFuture.supplyAsync(() -> fetchOrder(), pool)
        .thenApply(order -> "Packed: " + order)
        .exceptionally(ex -> "Failed");
```

`supplyAsync` ko executor nahi doge toh normally common ForkJoinPool use hota hai. `thenApply` non-async continuation completion thread par bhi chal sakta hai; `thenApplyAsync` scheduling alag karta hai.

**Twist:** Blocking HTTP/DB work common pool ko saturate kar sakta hai. Thread count, queue size, timeouts aur downstream limits discuss karo.

**Interview mein:** "ExecutorService controls execution resources; CompletableFuture provides asynchronous composition and error handling."

---

## Q3. Hibernate N+1 aur 500,000 records

Maan lo 100 students ki list nikaali. Phir har student ke subjects dekhne ke liye alag query chalayi. Ek parent query + 100 child queries = **101 queries**. Yehi N+1 hai.

**Pehle diagnose karo:** Hibernate SQL logging/metrics on karo. Sirf slow response dekhkar assume mat karo ki N+1 hi hai.

**Approach:** Agar UI ko sirf name aur subject count chahiye, full entities fetch karne ki jagah DTO projection. Agar limited parents aur child details chahiye toh selective fetch join. Badi lists mein batch fetching aur indexed pagination useful hain.

**500k records ka twist:** Sab records memory mein laake Java stream se filter karna expensive hai. Database mein filter karo, proper indexes lagao, pages/chunks mein process karo. Collection fetch join ke saath pagination unexpected result multiplication kar sakti hai.

**Interview mein:** "I first measure query count and access patterns, then choose projections, batching or selective fetching while keeping memory and pagination under control."

---

## Q4. High-volume fund transfer: debit ho gaya, credit fail

Tumne dost ko ₹500 bheje. Tumhare account se ₹500 kat gaye, par uske account mein nahi aaye. Ab user bolega: mere paise kahan gaye? Isliye fund-transfer design ka pehla question scaling nahi, **correctness** hai.

**Invariant:** Paisa create, lose ya duplicate nahi hona chahiye. Har transfer ka unique `transferId` hoga.

**Same database:** Debit aur credit ko ek ACID transaction mein rakho. Dono successful honge ya dono rollback.

**Alag services/databases:** Yahan ek local `@Transactional` se global atomicity nahi milegi. Durable transfer state maintain karo: `INITIATED → DEBITED → CREDITED → COMPLETED`. Outbox se events reliably publish karo, har step idempotent rakho, aur reconciliation job se pending transfers check karo.

**Important twist:** Credit API timeout hua. Kya credit fail hua? Zaroori nahi! Ho sakta hai credit successful ho gaya ho par response lost ho. Blind retry ya refund se double effect ho sakta hai. Pehle transfer status query/reconcile karo.

**Interview mein:** "I define the money-conservation invariant first, then choose a single ACID transaction or a durable cross-service workflow with idempotency and reconciliation."

---

## Q5. Kafka se duplicate payment kyun ho sakta hai?

Maan lo payment consumer ne ₹1,000 charge kar diye. Ab woh Kafka offset commit karne hi wala tha ki application crash ho gayi. Restart par Kafka wahi message dobara de sakta hai, kyunki usko processing complete hone ka durable confirmation nahi mila.

**Problem:** Consumer phir charge karega toh customer se do baar paise kat sakte hain.

**Solution derive karo:** Har business payment ka stable `paymentId` rakho. Database mein unique constraint se duplicate business operation reject karo. External gateway ko bhi idempotency key do. Offset tab commit karo jab durable processing state record ho gayi ho.

```java
// Simplified flow, not a complete atomic payment implementation:
if (paymentRepository.isCompleted(paymentId)) return;
paymentGateway.charge(amount, paymentId); // gateway idempotency key
paymentRepository.markCompleted(paymentId);
```

**Twist:** Sirf `isCompleted()` check race-safe nahi hai. Do workers parallel mein check pass kar sakte hain. Atomic DB reservation/unique constraint aur unknown gateway outcomes ki reconciliation zaroori hai.

**Interview mein:** "Kafka may redeliver after a crash between processing and offset commit. I use durable deduplication and end-to-end idempotency to prevent duplicate business effects."

---

## Q6. HashMap vs ConcurrentHashMap

Do threads counter ki value 5 read karte hain. Dono +1 karke 6 write kar dete hain. Expected 7 tha! Isko **lost update** bolte hain.

`HashMap` concurrent writes ke liye safe nahi. `ConcurrentHashMap` internal map operations safe karta hai, par `get → increment → put` ka poora sequence atomic nahi banata.

```java
ConcurrentHashMap<String, Integer> counts = new ConcurrentHashMap<>();
counts.merge("A", 1, Integer::sum);
```

**Twist:** `LongAdder` high-contention metrics ke liye useful ho sakta hai, par exact instantaneous snapshot guarantee samajhkar use karo. Agar multiple JVM instances hain, local map global counter nahi ban sakta.

**Interview mein:** "Thread-safe data structures do not automatically make multi-step business operations atomic."

---

## Q7. Optimistic vs pessimistic locking

Account mein ₹1,000 hain. Do ₹800 transfer requests ek hi waqt aa gayi. Dono ne ₹1,000 read kiya aur dono approve kar dein toh overspending ho jayegi.

**Optimistic locking:** Row mein `version` rakho. Update sirf tab karo jab expected version same ho. Kisi aur ne change kar diya toh update fail aur retry.

**Pessimistic locking:** Transaction pehle row lock leti hai, doosra transaction wait karta hai.

**Kab kya?** Conflicts rare hain toh optimistic efficient ho sakta hai. High contention par repeated retries expensive hain; pessimistic locking useful ho sakta hai. Multiple account rows lock karte waqt fixed order follow karo taaki deadlock risk kam ho.

**Twist:** Sirf Java `synchronized` distributed Spring Boot replicas ko coordinate nahi karta. Database constraints/transactions se invariant enforce karo.

**Interview mein:** "I choose locking based on contention and enforce the balance invariant transactionally, including crash recovery."

---

## Aaj ka revision plan

Har question ko pehle bina dekhe 1 minute mein explain karo. Phir code aur tricky follow-up bolo. Sabse zyada focus fund-transfer correctness, Kafka idempotency aur concurrency races par karo.

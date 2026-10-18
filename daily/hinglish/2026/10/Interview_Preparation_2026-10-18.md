# Interview Preparation — 2026-10-18 | Hinglish Classroom Edition

**Audience:** Product-company SDE2/Senior Java Backend. Ye 10 original practice questions hain. Har concept real-life example se samjhenge, phir reasoning, code, dry run, edge cases aur professional English answer.

**English report:** [October 18 English](../../../2026/10/Interview_Preparation_2026-10-18.md)

## Detailed system design
- [HLD Feature Flags — Hinglish](../../../../system-design/hinglish/daily/2026-10-18-hld-feature-flags.md)
- [LLD Idempotency — Hinglish](../../../../system-design/hinglish/daily/2026-10-18-lld-idempotency-coordinator.md)


## Q1. CompletableFuture mein 400ms deadline (Hard)
**Socho** ek shopping app ka product page khul raha hai. Inventory, price aur recommendations teen services se aati hain. Product manager bolta hai, "400ms ke andar response chahiye." Agar teen APIs ek ke baad ek 200ms leti hain, toh total 600ms. Isliye independent calls parallel start karo.

**Ab problem kya hai?** Har dependency ko 400ms ka timeout de doge toh queue, connection acquisition aur retries milakar end-to-end budget exceed ho sakta hai. Request start par monotonic clock se deadline calculate karo; har downstream call ko *remaining time* do. Inventory aur price required hain; recommendations optional, isliye unke failure par fallback.

```java
CompletableFuture<String> optional = recommendations
    .completeOnTimeout("unavailable", 250, TimeUnit.MILLISECONDS)
    .exceptionally(ex -> "unavailable");
CompletableFuture<String> result = inventory
    .thenCombine(price, (i,p) -> i + "|" + p)
    .thenCombine(optional, (core,r) -> core + "|" + r)
    .orTimeout(400, TimeUnit.MILLISECONDS);
```

**Dry run:** inventory 120ms, price 200ms, recommendation 900ms. Recommendation 250ms par fallback; response roughly 250ms plus overhead. Price 500ms le toh 400ms timeout.

**Interviewer twist:** `orTimeout` future timeout karta hai; underlying HTTP request necessarily cancel nahi hoti. Client timeout aur cancellation alag configure karo. Shared future par `completeOnTimeout` mutate hota hai.

**English answer:** "I launch independent dependencies concurrently, propagate one end-to-end deadline, and apply fallbacks only to optional data."

## Q2. Kafka poison-pill message baar-baar crash kara raha hai (Hard)
**Maan lo** payment-events partition mein offset 45 par invalid payload aa gaya. Consumer usko parse karta hai, fail hota hai, restart hota hai, phir offset 45. Offset 46 ke valid events bhi process nahi ho pa rahe.

**Step-by-step:** Pehle decide karo transient error hai ya permanent. DB temporarily unavailable toh bounded retries + exponential backoff + jitter. Invalid schema permanently hai toh original topic/partition/offset, exception aur correlation ID ke saath DLQ mein bhejo. **DLQ durable acknowledgement ke baad** hi source offset advance karo.

**Dry run:** offset 45 -> bounded retries -> DLQ ack -> source offset commit -> offset 46 process. Agar DLQ write fail toh offset advance mat karo.

**Tricky question:** "Record 45 skip karke 46 process kiya toh ordering?" Haan, end-to-end business ordering break ho sakti hai. Strict ordering required ho toh partition pause karo aur repair/replay plan rakho. Retry topics bhi reorder kar sakte hain.

**English answer:** "I classify failures, dead-letter permanent poison records durably, and explicitly decide whether skipping is compatible with ordering requirements."

## Q3. PostgreSQL SKIP LOCKED worker queue (Hard)
**Socho** 5 workers ek table se jobs pick kar rahe hain. Sabko same READY row mil gayi toh job multiple baar run ho sakti hai. Isliye DB transaction ke andar row lock lo; already locked rows skip karo.

```sql
BEGIN;
WITH picked AS (
 SELECT id FROM jobs WHERE state='READY'
 ORDER BY priority DESC, created_at, id
 FOR UPDATE SKIP LOCKED LIMIT 10
)
UPDATE jobs j SET state='CLAIMED',
 lease_until=now()+interval '2 minutes'
FROM picked WHERE j.id=picked.id RETURNING j.*;
COMMIT;
```

**Ab interviewer ka twist:** Worker claim ke baad crash? Lease expire hone par reaper reclaim karega. Old worker late return hua aur new owner ka result overwrite kar diya toh? Claim generation/fencing token verify karo.

**Fairness:** SKIP LOCKED strict FIFO nahi. High-priority jobs ke flood se low-priority jobs starve ho sakti hain. Aging policy aur oldest-waiting-job metric useful hain.

**English answer:** "SKIP LOCKED enables concurrent queue claiming, but I add leases, fencing and starvation monitoring for reliable execution."

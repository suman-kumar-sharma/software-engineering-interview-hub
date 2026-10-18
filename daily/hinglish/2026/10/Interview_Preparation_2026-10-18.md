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

## Q4. DynamoDB conditional write se duplicate order kaise rokoge? (Hard)
**Socho** customer ne "Place Order" dabaya. Order ban gaya, lekin network response lost. Client ne retry kiya. Agar har retry naya order create kare, customer ko duplicate order mil sakta hai.

**Pehle invariant samjho:** Same tenant + same idempotency key + same request = same logical result. Agar same key par different request body aaye, 409 conflict.

**Approach:** DynamoDB item key `TENANT#tenantId#IDEMP#requestKey`. First request `attribute_not_exists(PK)` condition se claim kare. Doosra request conditional failure dekhega, existing item strongly read karega, request hash compare karega. Status SUCCESS hai toh stored orderId return; IN_PROGRESS hai toh bounded wait/retry-after. Agar order aur idempotency record dono DynamoDB mein hain toh `TransactWriteItems` ke through atomic persistence design karo.

**Twist:** TTL deletion asynchronous hai; exact second par record delete hoga, guarantee nahi. Payment gateway DynamoDB transaction ke andar nahi aata. External charge ke liye gateway idempotency aur unknown-result reconciliation zaroori hai.

**English answer:** "I use a tenant-scoped conditional claim, validate the request fingerprint and persist a stable result for duplicate retries."

## Q5. JWT valid hai phir bhi tenant B ka data leak ho gaya! (Hard)
**Maan lo** SaaS platform mein tenant A aur B hain. A ka user authenticated hai. Usne URL mein tenant ID B kar diya aur B ka order mil gaya. JWT valid hone ke bawajood security breach kaise?

**Root cause:** Authentication ka matlab "user kaun hai"; authorization ka matlab "is user ko ye resource allowed hai?" JWT signature valid hona authorization nahi.

**Solution layers:** Trusted issuer, audience, expiry aur signature validate karo. Tenant membership trusted token claims ya server-side lookup se verify karo. Service method mein permission check; repository query mein `tenant_id` filter. Cache key mein bhi tenantId include karo.

```java
@PreAuthorize("@tenantAuthorizer.canRead(authentication, #tenantId)")
public OrderDto getOrder(UUID tenantId, UUID orderId) {
    return repository.findByTenantIdAndId(tenantId, orderId)
        .map(this::map)
        .orElseThrow(NotFoundException::new);
}
```

**Interviewer twist:** `@PreAuthorize` proxy-based interception same-class self-invocation mein bypass ho sakti hai. Isliye DB-level scoping defense-in-depth rakho. RLS use karo toh DB session tenant context safely set/reset karna hoga.

**English answer:** "I validate authentication separately from tenant authorization and scope every data-access path by tenant."

## Q6. ThreadLocal se memory aur security context leak (Medium/Hard)
**Socho** Tomcat thread pool ka Thread-1 tenant A ki request handle karta hai. Request khatam hui lekin `ThreadLocal.remove()` nahi hua. Same thread tenant B ke request ke liye reuse ho gaya. Ab stale context read ho sakta hai.

**Kyun?** Pool thread request se bahut zyada der zinda rehta hai. ThreadLocalMap mein value retained reh sakti hai; weak reference key ka matlab value immediately free nahi hoti.

```java
TENANT.set(tenantId);
try {
    processRequest();
} finally {
    TENANT.remove();
}
```

**Twist:** ThreadLocal mein huge request graph store karoge toh heap retention badhega. Diagnose with heap dump, retained paths aur thread-pool metrics. Explicit context passing kabhi-kabhi clearer hai.

**English answer:** "Pooled threads outlive requests, so I always clear ThreadLocal state in finally to avoid retention and cross-request leakage."

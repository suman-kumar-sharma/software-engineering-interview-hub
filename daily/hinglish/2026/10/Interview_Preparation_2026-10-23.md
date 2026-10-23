
# Daily Interview Preparation — 2026-10-23 (Hinglish Classroom Edition)
**Target:** Product-company SDE2 / Senior Backend. Har topic ko situation → reasoning → edge case → interview answer ke flow mein samjho.

Ye representative practice questions hain. Verified source ke bina kisi company ke naam se attribute nahi kiya gaya hai.

## 1. Virtual threads ka matlab unlimited traffic nahi
Socho ek request teen downstream APIs call karti hai. Virtual threads blocked work ko cheaper banati hain, lekin DB connections, CPU aur remote-service quota unlimited nahi kar deti. Isliye scarce dependency ke around bulkhead chahiye.

~~~java
Semaphore inventorySlots = new Semaphore(80);

Inventory readInventory(String sku) throws InterruptedException {
    if (!inventorySlots.tryAcquire(150, TimeUnit.MILLISECONDS)) {
        throw new RejectedExecutionException("inventory capacity exhausted");
    }
    try {
        return inventoryClient.fetch(sku);
    } finally {
        inventorySlots.release();
    }
}
~~~

Semaphore(80) ek instance par max 80 in-flight calls allow karta hai. tryAcquire queue ko 150 ms tak limit karta hai. finally har outcome mein permit release karta hai. Das instances hon toh total roughly 800 calls allow ho sakti hain; fleet-wide limit ke liye quota partition ya shared limiter chahiye.

**Interviewer pooche:** 80 ka number kaise choose kiya? Load test, downstream p99 latency, connection pool aur saturation metrics se. Unbounded queue se overload chhupana solution nahi.

## 2. CompletableFuture timeout remote call ko zaroori nahi rokta
Maan lo future timeout ho gaya. Iska matlab future exceptional complete hua; underlying HTTP I/O cancel hua ya nahi, client implementation par depend karta hai. Har hop ko deadline pass karo aur client-native connect/read/request timeout set karo.

Payment ka example socho: server ne payment commit kar di, lekin response time par nahi aaya. Blind retry double charge kar sakta hai. Idempotency key aur reconciliation chahiye. Timeout rollback nahi hota.

## 3. Kafka exactly-once kahan tak hai?
Kafka transactions Kafka-to-Kafka workflow mein exactly-once semantics de sakte hain. Lekin PostgreSQL ya HTTP side effect automatic cover nahi hote.

Agar DB commit ho gaya aur offset commit se pehle consumer crash hua, message replay hoga. Inbox table mein unique event ID aur domain mutation ko ek DB transaction mein likho. Duplicate aayega toh unique constraint second mutation rok sakta hai.

Ordering partition ke andar hoti hai. Aggregate ke events ordered chahiye toh aggregate ID ko key banao. Partition count badalne se key mapping badal sakti hai—migration/replay ka plan bhi answer ka part hai.

## 4. Outbox relay duplicate kyon bhej sakta hai?
Socho relay ne broker ko event publish kiya. Broker accept kar chuka, par relay published mark karne se pehle crash ho gaya. Restart par same row dobara publish ho sakti hai.

Outbox domain update aur publish-intent ko atomic banata hai, broker delivery ko magically exactly-once nahi. Stable event ID, idempotent consumer, retry jitter aur lease expiry rakho. PostgreSQL mein SKIP LOCKED se short transaction mein row claim karo; network call ke dauran row lock mat pakde raho.

## 5. Optimistic locking
Agar conflict kam hain toh version column se stale update detect karo. Update mein expected version match hona chahiye. Zero rows ka matlab stale version, missing record ya insufficient stock ho sakta hai—business contract ke hisaab se handle karo. Quantity zero/negative ko reject karo.

Stale version aaya toh wahi old update retry mat karo. Latest state read karo, business decision dobara calculate karo, aur bounded retry lagao. Hot row par short pessimistic lock ya atomic conditional update compare karo.

## 6. Cache stampede ka solution
Maan lo ek popular key expire hoti hai aur hazaar requests DB ko hit karti hain. TTL jitter, request coalescing aur stale-while-revalidate combine karo. Fresh window mein fresh data; stale window mein stale response ke saath ek leased refresh; hard expiry ke baad bounded wait ya safe fallback.

Lease duplicate refresh ko kam karti hai, lekin process pause/lease expiry mein multiple refreshers possible hain. DB ke around alag bulkhead rakho. Tenant data ke cache key mein tenant/auth scope include karo.

## 7. DSA: at most K distinct characters
Window ko right se grow karo. Jab distinct count K se bada ho, left se characters hatao aur frequency decrement karo. Har pointer maximum n baar move karta hai, isliye O(n) time.

~~~java
static int longestAtMostKDistinct(String s, int k) {
    if (s == null) throw new IllegalArgumentException("s is null");
    if (k <= 0 || s.isEmpty()) return 0;
    Map<Character, Integer> freq = new HashMap<>();
    int left = 0, best = 0;
    for (int right = 0; right < s.length(); right++) {
        freq.merge(s.charAt(right), 1, Integer::sum);
        while (freq.size() > k) {
            char drop = s.charAt(left++);
            int count = freq.get(drop) - 1;
            if (count == 0) freq.remove(drop);
            else freq.put(drop, count);
        }
        best = Math.max(best, right - left + 1);
    }
    return best;
}
~~~

Dry run eceba, k=2: best window ece, length 3. Space O(min(n, alphabet)). Java char UTF-16 code unit hai; Unicode code point semantics hon toh code points par iterate karo.

## 8. SQL keyset pagination
Deep OFFSET par DB ko rows scan karke discard karni pad sakti hain. Inserts ke saath page boundaries bhi shift ho sakti hain. Keyset mein last row ka cursor use karo: created_at aur id dono compare karo, phir descending order mein next batch lo.

Index (tenant_id, created_at DESC, id DESC) helpful hai. Id tie-breaker hai jab timestamps same hon. Tenant ID authenticated context se lo. Keyset sequential next-page ke liye useful hai; arbitrary page number par jump karne ke liye nahi.

## 9. Spring Security tenant authorization
Authentication user ko identify karti hai; authorization decide karti hai ki user us tenant ke resource par action kar sakta hai ya nahi. Sirf controller URL par role check kaafi nahi. Service/domain, async jobs aur message consumers mein bhi membership verify karo. Client ke tenant header ko blindly trust mat karo.

Logs mein actor, tenant, action, outcome aur correlation ID rakho; token/secret kabhi nahi.

---

# HLD Classroom — Durable Multi-Tenant Job Scheduler

Socho har customer apne invoice reminders, reports ya background jobs future ke liye schedule karta hai. Server restart ho jaaye tab bhi job rehna chahiye. Job due time par chale, fail ho toh retry ho, aur user status dekh sake.

## 1. Requirements fix karo
Create/get/cancel APIs, idempotency key, per-tenant quotas, payload size limit aur audit chahiye. Timing SLO measurable banao—maan lo normal agreed load par p99 start lag 5 seconds se kam. Recurring jobs aur multi-region failover ko clarify karo.

## 2. Architecture

~~~text
Client -> API/Auth/Quota -> Job DB + Outbox
                           -> Scheduler shards
Outbox relay -> durable queue -> bounded workers -> handler
                              -> status/retry store
Metrics + DLQ + audited replay
~~~

DB source of truth hai kyunki queue messages duplicate/delay ho sakte hain. Outbox job aur dispatch intent ko same DB transaction mein save karta hai. Queue ka role dispatch buffer hai, schedule ka eklauta record nahi.

## 3. Data model
Job row mein tenant, idempotency key, request fingerprint, due time, state, attempts, retry time, lease, version aur payload reference rakho. Unique tenant plus idempotency key duplicate create request ko rokta hai. Same key par different payload aaya toh conflict.

## 4. Scheduler kaise claim kare?
Indexed due-time scan se start kar sakte ho. Short transaction mein conditional claim/lease set karo, commit karo, phir queue publish karo. SKIP LOCKED ya conditional update concurrent schedulers ko same job claim karne se bachata hai. Network call ke dauran DB lock hold mat karo.

High volume par time buckets ya timing wheel efficient ho sakte hain, lekin restart ke baad durable DB se timer horizon rebuild karna hoga. Queue-delay-only approach long horizons ke liye risky hai.

## 5. State machine aur failures
SCHEDULED -> DISPATCHED -> RUNNING -> SUCCEEDED. Temporary failure RETRY_WAIT; permanent ya max attempts DEAD.

Cancel aur start race karte hain; conditional transition jeetne wala decide karta hai. Running side effect ko cancel undo nahi kar sakti. Worker crash par lease expire; next worker naya attempt/version claim kare. Fencing version purane worker ko newer status overwrite karne se rokta hai.

Broker publish ke baad DB ack fail ho toh duplicate dispatch ho sakta hai. Handler side effect ke baad status save fail ho toh execution repeat ho sakti hai. Stable idempotency key ya reconciliation use karo.

## 6. Retry, overload, security
Exponential backoff + full jitter, max attempts, retry budget aur per-tenant concurrency caps rakho. Permanent validation failure ko retry mat karo. DLQ mein reason, redrive authorization, audit aur rate limit ho. Quota cross hone par defer/reject karo; backlog ko unlimited mat hone do.

Tenant ID trusted authentication context se verify karo. Payload schema, size aur schedule horizon validate karo. Sensitive payload encrypt karo, worker egress restrict karo aur arbitrary URL fetch avoid karo—SSRF ka risk hai.

## 7. Capacity aur trade-offs
Due-time distribution, handler duration, retries, payload size aur tenant skew estimate karo. Metrics: due-job lag, oldest outbox age, queue age, lease expiry, attempts, DLQ depth, handler saturation aur tenant rejection.

DB polling recoverable aur simple hai, lekin scale par DB pressure badhta hai. Timing wheel fast hai, par restart/reconciliation complex. Multi-region active-active ke liye ownership/fencing model chahiye; sirf async replication duplicate execution nahi rokta.

**Interview answer:** “Main schedule DB mein persist karta hoon, outbox-backed queue se dispatch karta hoon, leases/fencing se worker ownership manage karta hoon aur at-least-once delivery assume karta hoon. Idempotency side effects ko protect karti hai; due-job lag aur handler capacity scaling guide karte hain.”

## Engineering updates — 23 October 2026
- **Spring Boot 4.2.0-RC1 aur 4.1.2 release hue.** RC production upgrade ka automatic signal nahi; compatibility tests aur release notes review karo. https://spring.io/blog/2026/10/23/spring-boot-4-2-0-RC1-available-now/
- **AWS Continuum for code vulnerabilities 22 October ko GA hua.** AI findings/fixes ko review, tests, least-privilege credentials aur staged rollout ke saath use karo. https://aws.amazon.com/blogs/aws/aws-continuum-for-code-vulnerabilities-is-now-generally-available/
- **Amazon ECS Gateway 20 October ko announce hua.** ECS ingress setup simplify karta hai; networking aur exposure evaluate karo. https://aws.amazon.com/blogs/aws/introducing-amazon-ecs-gateway-a-fully-managed-ingress-solution-for-ecs-services/
- **GitHub ne 21 October ko agent-scale Git infrastructure migration explain ki.** Per-reference quorum aur scalable object reads consistency aur storage ko alag bottlenecks ki tarah handle karte hain. https://github.blog/engineering/the-bridge-to-agent-scale-git-infrastructure/

## Revision
Timeout cancellation kyun nahi? Kafka EOS DB ko kyun cover nahi karta? Outbox duplicate kaise handle karta hai? Keyset index kaise derive karoge? Scheduler overload mein kya karega?

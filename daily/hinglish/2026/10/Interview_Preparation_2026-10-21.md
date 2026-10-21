# Daily Software Engineering Interview Preparation — 2026-10-21 (Proper Hinglish)

**Audience:** Product-company SDE2 / Senior Backend. Ye representative practice questions hain; kisi named company ke exact interview questions hone ka claim nahi hai.

**System design:** [HLD — Payment Reconciliation Platform](../../../system-design/hinglish/daily/2026-10-21-hld-payment-reconciliation-platform.md) · [LLD — Bounded Async Bulkhead Executor](../../../system-design/hinglish/daily/2026-10-21-lld-bounded-bulkhead-executor.md)

## Q1. CompletableFuture timeout hua — kya HTTP request bhi ruk gayi?

Socho service teen downstream APIs ko parallel call kar rahi hai. Ek future 200 ms mein timeout ho gaya. Lekin remote server par kaam abhi bhi chal sakta hai. Future ka exceptional completion aur actual network request cancel hona do alag cheezein hain.

~~~java
CompletableFuture<Response> call =
    CompletableFuture.supplyAsync(() -> client.fetch(), ioExecutor)
        .orTimeout(200, TimeUnit.MILLISECONDS)
        .exceptionally(ex -> fallbackResponse());
~~~

Timeout HTTP client ke connect/request level par configure karo aur ek end-to-end deadline budget propagate karo. Blocking I/O ko common pool mein mat daalo; bounded executor use karo. Side effect wale operations mein idempotency zaroori hai. orTimeout future ko exceptional complete karta hai, par isse prove nahi hota ki underlying operation ya remote server ruk gaya.

**Interviewer twist:** Payment call timeout hua toh ho sakta hai payment successful ho chuki ho aur response lost hua ho. Stable operation ID se status check karo, blind retry nahi.

**Interview answer:** “I propagate a deadline, configure transport-level timeouts, and treat timeouts on side-effecting operations as unknown outcomes.”

## Q2. Kafka order preserve karta hai, phir events out of order kaise hue?

Kafka order ek partition ke andar preserve karta hai, poore topic ke across nahi. Agar same order ke events alag keys ke saath bheje, woh different partitions mein ja sakte hain aur parallel process honge.

~~~java
ProducerRecord<String, OrderEvent> record =
    new ProducerRecord<>("order-events", event.orderId(), event);
producer.send(record);
~~~

Ordering ka scope pehle decide karo—per order, per account ya global? Per-order ordering chahiye toh stable orderId key rakho. Random key mat use karo. Consumer rebalance par replay bhi ho sakta hai: old consumer side effect kar chuka tha par offset commit se pehle crash ho gaya. Isliye handler idempotent hona chahiye.

**Trade-off:** Ek hot key ka sara traffic ek partition par aa sakta hai. Global ordering ke liye serialization chahiye, jo throughput ghata sakti hai.

**Interview answer:** “I define the ordering boundary first, key by that entity, and design for replay even when partition order is preserved.”

## Q3. PostgreSQL worker queue mein SKIP LOCKED alone enough kyun nahi?

Maan lo multiple workers jobs pick kar rahe hain. Do workers ek hi job process na karein, isliye row ko transaction mein lock karke claim karte hain. Worker crash ho toh lease expire hone par doosra worker job recover kar sakta hai.

~~~sql
WITH candidate AS (
  SELECT id FROM job
  WHERE status = 'READY'
     OR (status = 'RUNNING' AND lease_until < now())
  ORDER BY priority DESC, created_at, id
  FOR UPDATE SKIP LOCKED
  LIMIT 1
)
UPDATE job j
SET status = 'RUNNING',
    lease_owner = :worker_id,
    lease_until = now() + interval '45 seconds',
    attempts = attempts + 1,
    fencing_token = fencing_token + 1
FROM candidate c
WHERE j.id = c.id
RETURNING j.*;
~~~

Claim aur update ek transaction mein hon. Real predicate/order ke hisaab se index banao aur EXPLAIN ANALYZE se plan verify karo. SKIP LOCKED currently locked row skip karta hai, lekin expired lease wala purana worker baad mein wake up ho sakta hai.

**Important twist:** Lease aur fencing same cheez nahi. Har claim ko increasing fencing token do; stale worker ka completion/write reject hona chahiye. External side effect bhi idempotent rakho.

**Interview answer:** “I atomically claim with SKIP LOCKED, recover with leases, and fence stale workers so an expired owner cannot overwrite newer work.”

## Q4. Same idempotency key alag payload ke saath aaye toh?

Maan lo client key K ke saath ₹100 transfer request bhejta hai. Timeout hua, phir bug ki wajah se same K ke saath ₹900 retry kar diya. Agar hum bina payload check kiye purana response return karein toh mismatch hide ho jayega.

Idempotency key ko tenant/customer aur operation ke scope mein store karo, saath request fingerprint. Database unique constraint lagao; application-level check-then-insert concurrent requests mein race kar sakta hai.

~~~sql
CREATE TABLE idempotency_record (
  tenant_id text NOT NULL,
  idempotency_key text NOT NULL,
  request_hash bytea NOT NULL,
  status text NOT NULL,
  response_json jsonb,
  created_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (tenant_id, idempotency_key)
);
~~~

Same key + same hash: existing result return. Same key + different hash: conflict response. Business mutation aur final response/operation ID ko transactionally persist karo. Long-running workflow ho toh IN_PROGRESS state aur stale-work recovery lease chahiye.

**Interview answer:** “Idempotency needs a durable unique key, payload equivalence and a transactional business boundary—not just caching the last HTTP response.”

## Q5. @TransactionalEventListener AFTER_COMMIT ke baad bhi event lose kaise ho sakta hai?

Ye listener transaction commit ke baad chalta hai, lekin durable queue nahi hai. Database commit ho gaya aur Kafka publish karne se pehle process crash ho gaya toh event lost ho sakta hai.

Critical integration event ke liye transactional outbox use karo. Business row aur outbox row same DB transaction mein save honge. Separate publisher retry karega; consumer event ID se deduplicate karega. Delivery at-least-once ho sakti hai.

AFTER_COMMIT listener un in-process tasks ke liye theek hai jahan loss acceptable hai ya koi aur durable mechanism hai. Annotation database aur Kafka ko atomic nahi banata.

**Twist:** Publisher send ke baad aur published flag update karne se pehle crash hua toh event duplicate publish ho sakta hai. Consumer idempotency phir bhi chahiye.

**Interview answer:** “For critical integration events I use a transactional outbox; AFTER_COMMIT alone gives timing semantics, not durable delivery.”

## Q6. JVM memory badh rahi hai — leak ya allocation pressure?

Heap bada dene se pehle evidence collect karo. Similar load windows mein full GC ke baad live heap compare karo. Agar post-GC live set badhta ja raha hai, retention leak ho sakta hai. Agar allocation rate high hai lekin live set stable hai, allocation pressure ho sakta hai.

Step-by-step:
1. Heap, allocation rate, GC pause, CPU, traffic aur deployment changes correlate karo.
2. Representative load par JFR capture karo; allocation hot spots aur lock contention dekho.
3. Leak suspect ho toh heap dump compare karo, dominator tree aur GC roots dekho.
4. Caches, unbounded maps, ThreadLocal, listeners, queues aur static collections inspect karo.
5. Realistic load par fix verify karo.

Production node par bina plan heap dump mat lo—pause, disk usage aur sensitive data exposure ho sakta hai. Possible ho toh replica/canary use karo.

**Interview answer:** “I distinguish allocation rate from retained live set, use JFR and heap-dump dominators, and validate the retaining reference under load.”

## Q7. DSA: longest substring without repeating characters

Sliding window use karo. Har character ka last index map mein rakho. Repeated character current window ke andar mile toh left boundary ko uske previous index ke baad le jao.

~~~java
import java.util.*;

static int longestUniqueSubstring(String s) {
    Map<Character, Integer> last = new HashMap<>();
    int left = 0, best = 0;
    for (int right = 0; right < s.length(); right++) {
        char c = s.charAt(right);
        Integer previous = last.put(c, right);
        if (previous != null && previous >= left) {
            left = previous + 1;
        }
        best = Math.max(best, right - left + 1);
    }
    return best;
}
~~~

“abba” dry run karo. Dusra b aate hi left index 2 ho jayega. Last a par left ko peeche nahi le jana. Har character ek baar process hota hai: expected O(n) time aur O(min(n, alphabet size)) space.

**Edge cases:** empty string ka answer 0; “aaaa” ka answer 1. Java char UTF-16 code unit hai, Unicode code point nahi. Agar requirement user-perceived character ki hai toh grapheme clusters alag problem hain.

## Q8. Local token bucket ya distributed rate limiter?

Token bucket mein capacity C aur refill rate R tokens/second hota hai. Elapsed time dt ke liye tokens = min(C, tokens + dt*R). Ek token available hai toh request allow karo aur token deduct karo.

Local bucket fast hai, lekin 10 replicas hain toh har replica apna quota de sakta hai; effective total quota 10x ho sakta hai. Global tenant quota ke liye Redis Lua script jaisi atomic shared operation ya centralized quota service chahiye. Redis ko separate read-modify-write calls mein use karoge toh race hogi.

Shared coordination latency aur dependency add karta hai. Endpoint risk ke hisaab se fail-open/fail-closed decide karo. Local fallback se overshoot hoga toh woh accepted trade-off explicitly document karo. Quota key authenticated tenant identity se derive karo, untrusted header se nahi.

**Interview answer:** “I select the quota scope first, implement refill-and-deduct atomically, and make the consistency/availability trade-off explicit.”

## Q9. Redis hai, phir bhi cache stampede kyun hota hai?

Maan lo popular key expire hui. Ek hi moment par hazaar requests miss karengi aur sab database hit karengi. Redis use karna expiry ke synchronized miss ko automatically solve nahi karta.

Solutions:
- Per-key single-flight: ek request refresh kare, baaki wait karein.
- TTL jitter: expiry times spread karo.
- Stale-while-revalidate: stale value temporarily serve karo, background refresh chalao—sirf jab freshness contract allow kare.
- Known absence ke liye short negative cache.
- Refresh concurrency aur DB fallback budget bounded rakho.

Distributed lock ko indefinite mat rakho. Lease aur owner token use karo, compare-and-delete karo; phir bhi database protect karo, kyunki lock work complete hone se pehle expire ho sakta hai.

**Interview answer:** “I combine expiry jitter, per-key request coalescing and bounded refresh; stale serving is enabled only where the freshness contract allows it.”

## Engineering updates — 21 October 2026

1. **Oracle October 2026 Critical Patch Update (20 Oct):** Java SE samet multiple product families ke security updates release hue. Apne exact runtime/vendor build ko verify karke normal rollout process se production JDK patch karo. Generic announcement se build number guess mat karo. [Oracle CPU announcement](https://blogs.oracle.com/security/october-2026-critical-patch-update-released) · [Java CPU mapping](https://docs.oracle.com/en-us/iaas/releasenotes/java-management/jdk-cpu-october-2026.htm)
2. **GitHub package firewall public preview (20 Oct):** Package installation time par malware risk reduce karne ke liye supply-chain control. Backend teams lockfiles, dependency review, CI secret isolation aur reproducible builds bhi use karein. Preview ki availability/coverage vary kar sakti hai. [GitHub Changelog](https://github.blog/changelog/month/10-2026/)
3. **AWS Lambda Web Functions (12 Oct):** Announcement mein Node.js HTTP workloads ke liye direct request handling, native streaming aur warm environment mein concurrent requests hain. Abhi announcement ke basis par Java runtime support assume mat karna. [AWS announcement](https://aws.amazon.com/about-aws/whats-new/2026/10/aws-lambda-web-functions/)
4. **Kafka 4.2.2 (29 Sep):** Bug-fix release. Upgrade se pehle upstream upgrade notes aur apne vendor ka support matrix check karo. [Apache Kafka announcement](https://kafka.staged.apache.org/blog/2026/09/29/apache-kafka-4.2.2-release-announcement/)

## Revision checklist

- Timeout aur cancellation ka difference explain karo.
- Kafka key choose karne se pehle ordering scope define karo.
- Lease expiry aur fencing ka difference batao.
- Same idempotency key + different payload ka response define karo.
- Outbox aur AFTER_COMMIT listener compare karo.
- Allocation pressure aur retained memory distinguish karo.
- Sliding window dry run aur Unicode semantics explain karo.
- Multiple replicas mein local quota overshoot samjhao.
- Freshness contract ke hisaab se cache stampede solution choose karo.

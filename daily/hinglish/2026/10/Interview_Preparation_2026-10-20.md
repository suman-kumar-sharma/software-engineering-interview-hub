# Daily Software Engineering Interview Preparation — 2026-10-20
## Proper Hinglish Classroom Edition

**Target:** Product-company SDE2 / Senior Backend. Ratta nahi maarna: pehle situation samjho, phir root cause, phir solution derive karo. Code aur technical terms English mein hain, explanation natural Roman Hindi + English mein hai.

Detailed designs: [HLD — Audit Event Platform](../../../system-design/hinglish/daily/2026-10-20-hld-audit-event-platform.md) · [LLD — Token Bucket](../../../system-design/hinglish/daily/2026-10-20-lld-token-bucket-rate-limiter.md)

## Q1. CompletableFuture timeout hua — kya HTTP call bhi ruk gayi?

Socho tumhari API teen services ko parallel call karti hai. Ek future 200 ms mein timeout ho gaya. Iska matlab ye nahi ki HTTP client ya remote server ka kaam bhi pakka band ho gaya. Future exceptional complete ho sakta hai jab underlying request abhi bhi chal rahi ho.

~~~java
CompletableFuture<Response> call =
    CompletableFuture.supplyAsync(() -> client.fetch(), ioExecutor)
        .orTimeout(200, TimeUnit.MILLISECONDS)
        .exceptionally(ex -> fallbackResponse());
~~~

Is code mein fallback aa sakta hai, par client.fetch() ke andar network request cancel hui—ye guarantee nahi. Actual HTTP client mein connect/request timeout configure karo, ek end-to-end deadline budget propagate karo, aur blocking work ko bounded executor mein rakho. Common pool par blocking calls doosre tasks ko starve kar sakti hain.

**Interview answer:** “I propagate a deadline budget, configure timeouts at the I/O client, and never assume a timed-out future has cancelled remote work.”

## Q2. Kafka message process hua, DB update hua, phir offset commit se pehle crash

Maan lo order database mein PAID ho gaya, lekin offset commit hone se pehle app crash ho gayi. Restart par Kafka same event dobara de sakta hai. Ab business effect repeat nahi hona chahiye, chahe message repeat ho.

Har operation ka stable event ID rakho. Sirf in-memory HashSet enough nahi, kyunki restart aur multiple replicas mein state share nahi hoti. Database unique constraint lagao aur processed-event insert plus business update ek hi DB transaction mein karo. DB transaction ke baad offset commit karo. External payment API ke liye provider idempotency key use karo aur timeout ke baad unknown outcome reconcile karo.

~~~sql
CREATE TABLE processed_event (
  consumer_name text NOT NULL,
  event_id uuid NOT NULL,
  processed_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (consumer_name, event_id)
);
~~~

**Twist:** Kafka exactly-once external payment gateway ko automatically exactly-once nahi banata; Kafka offset aur DB commit alag systems hain.

**Interview answer:** “I design for replay, enforce durable idempotency in the same transaction as the state change, and use an outbox or provider idempotency for external effects.”

## Q3. PostgreSQL work queue mein same job do workers pick na karein

Socho 10 workers hain. Sabne pehle SELECT LIMIT 1 chalaya toh sab same job pick karne ki koshish kar sakte hain. Aur worker claim ke baad crash ho gaya toh job permanently stuck nahi rehni chahiye.

~~~sql
BEGIN;
WITH candidate AS (
  SELECT id FROM job
  WHERE status = 'READY'
     OR (status = 'RUNNING' AND lease_until < now())
  ORDER BY priority DESC, created_at
  FOR UPDATE SKIP LOCKED LIMIT 1
)
UPDATE job j
SET status = 'RUNNING',
    lease_owner = :worker_id,
    lease_until = now() + interval '60 seconds',
    attempts = attempts + 1
FROM candidate c
WHERE j.id = c.id
RETURNING j.*;
COMMIT;
~~~

SKIP LOCKED ka matlab locked row par wait karke same job lene ke bajaye next available row dekho. Lease expiry se crashed worker ka job recover ho sakta hai.

**Dangerous twist:** Purana worker pause hua, lease expire hui, naya worker job complete kar gaya, phir purana worker resume ho gaya. Stale write rokne ke liye fencing token use karo aur downstream purane token reject kare. Handler idempotent aur retries bounded bhi hone chahiye.

**Interview answer:** “I atomically claim work using SKIP LOCKED, recover abandoned jobs through leases, and use idempotency plus fencing where stale workers could corrupt newer state.”

## Q4. Cache stampede ko kaise rokenge?

Maan lo popular product ka cache expire hua aur ek hi second mein 5000 requests aayi. Sabko cache miss mila aur sab database query karne lagi. Cache ne DB load kam karna tha, par expiry ne traffic ka bomb gira diya.

TTL jitter se keys alag time expire hoti hain. Stale-while-revalidate allowed stale data serve karte hue ek refresh chalata hai. Local single-flight ek JVM ke callers ko same refresh share karwata hai. Distributed lease duplicate refresh kam kar sakti hai, lekin lease expiry aur network pause ki wajah se use business writes ka correctness lock mat samjho.

Refresh timeout aur concurrency cap rakho; sab requests ko forever wait mat karao.

**Interview answer:** “I combine TTL jitter and stale-while-revalidate with per-process single-flight; distributed coordination is an optimization, while the database remains authoritative.”

## Q5. Multi-tenant security mein URL ka tenant ID check enough kyun nahi?

Maan lo user alpha tenant ka member hai. Woh URL badal kar beta tenant ka invoice maangta hai. Login prove karta hai kaun user hai; ye prove nahi karta ki use beta ka data dekhne ki permission hai.

Tenant identity verified claims ya server-side membership se nikalo, client ke body/query parameter par blindly trust mat karo. Har request par tenant membership aur object ownership check karo. Repository query mein authorized tenant scope lagao; zarurat ho toh DB row-level security bhi. Cross-tenant read, update, export, async consumer aur cache leakage test karo.

**Twist:** Controller sahi hai, par Redis key sirf invoiceId hai. Tenant scope key mein nahi hua toh cached data leak ho sakta hai.

**Interview answer:** “Tenant-scoped authorization belongs at service and data boundaries, with object-level tests and tenant-aware cache keys.”

## Q6. Virtual threads ke baad bhi DB connection pool limit kyun?

20,000 requests aa gayin aur virtual threads lightweight hain. Lekin JDBC pool mein 40 connections hain. DB ke saath ek waqt mein active connection-based work phir bhi limited hai.

~~~java
private final Semaphore dbPermits = new Semaphore(40);

Result queryWithLimit() throws InterruptedException {
    dbPermits.acquire();
    try {
        return repository.query();
    } finally {
        dbPermits.release();
    }
}
~~~

Semaphore scarce resource ke around concurrency cap karta hai. finally zaroori hai taaki exception par bhi permit release ho. Production mein bounded wait, timeout aur overload response define karo. 10 replicas mein 40 permits each ka matlab 400 concurrent DB operations ho sakte hain—global capacity bhi calculate karo.

**Interview answer:** “Virtual threads lower the cost of blocking concurrency, but I still cap work at scarce downstream resources and calculate aggregate limits across replicas.”

## Q7. DSA: longest substring with at most K distinct characters

Chalo eceba aur K=2 lete hain. Hume longest continuous substring chahiye jisme do se zyada unique characters na hon. ece valid hai, length 3. eceb mein e, c, b teen distinct ho gaye, toh left side se window shrink karni padegi.

~~~java
int longestAtMostK(String s, int k) {
    if (k <= 0 || s.isEmpty()) return 0;
    Map<Character, Integer> freq = new HashMap<>();
    int left = 0, best = 0;
    for (int right = 0; right < s.length(); right++) {
        freq.merge(s.charAt(right), 1, Integer::sum);
        while (freq.size() > k) {
            char c = s.charAt(left++);
            int n = freq.get(c) - 1;
            if (n == 0) freq.remove(c);
            else freq.put(c, n);
        }
        best = Math.max(best, right - left + 1);
    }
    return best;
}
~~~

Right pointer character add karta hai. Distinct count limit se bahar ho toh left pointer characters remove karta hai jab tak window valid na ho. Har character ek baar enter aur ek baar exit karta hai: O(n) expected time. Java char UTF-16 unit hai; Unicode code points count karne hon toh code-point iteration chahiye.

**Interview answer:** “The invariant is at most K distinct characters in the active window; both pointers only move forward, so time is linear.”

## Q8. Retries outage ko aur bada kyun bana sakti hain?

Socho request teen services se guzarti hai aur har layer teen attempts karti hai. Worst case mein downstream ko 3 × 3 × 3 = 27 calls mil sakti hain. Service slow hai aur retries usi par extra load daal rahi hain.

Sirf transient errors retry karo, aur operation idempotent/safe hona chahiye. Shared deadline aur attempt budget rakho, exponential backoff ke saath jitter use karo, validation errors retry mat karo. Circuit breaker, concurrency cap aur retry metrics bhi rakho.

**Interview answer:** “I bound retries and deadlines, add jittered backoff, require idempotency and use admission control to protect dependencies.”

## HLD classroom walkthrough — Multi-tenant Audit Event Platform

Socho enterprise ke multiple tenants hain. Har login, permission change, payment ya admin operation ka audit event chahiye. Security team last 90 days search karegi, compliance ke liye 7 saal archive rakhna hai. Sabse important requirement: **accepted event chup-chaap lose nahi hona chahiye.**

Assumptions: 20,000 events/second sustained, 60,000 burst, normal load mein acknowledgement p95 under 150 ms. At-least-once delivery acceptable hai agar duplicates safely handle hote hain.

~~~text
Producers -> Gateway/Auth/Quota -> Ingestion API -> Replicated Kafka
Kafka -> Search indexer -> OpenSearch (90-day search)
Kafka -> Archive consumer -> Object storage (7-year retention)
Kafka -> DLQ/replay tooling
Metrics + traces + admin audit -> Monitoring/SIEM
~~~

Gateway identity aur quota enforce karta hai. Ingestion API schema validate karti hai aur broker durable acknowledgement ke baad hi accepted bolti hai. Kafka burst absorb karta hai aur replay deta hai. OpenSearch derived search index hai, source of truth nahi. Object storage long retention ke liye economical hai; DLQ bad event ko reason ke saath quarantine karta hai.

**Ordering trade-off:** Kafka key tenant ID hai toh per-tenant order mil sakta hai, par bada tenant hot partition bana sakta hai. Tenant ke andar shards throughput badhate hain, par global order weak ho jata hai. Pehle clarify karo ki ordering per resource chahiye, per tenant ya bilkul nahi.

**Failure handling:** Broker unavailable ho toh false success mat do. Search down ho toh Kafka retain kare aur lag monitor ho. Archive failure par bounded retry, alert, replay aur reconciliation. Duplicate event tenantId + eventId se deduplicate karo. Tenant scope API, search, archive policy, cache keys aur admin tools sab jagah enforce karo.

**Outbox kab?** Agar business DB transaction ke saath audit event atomic chahiye, same transaction mein outbox row likho aur relay publish kare. Independent ingestion API mein outbox automatically required nahi.

**Interview mein bolo:** “I separate durable ingestion from derived search and archival consumers. The API acknowledges only after durable broker acceptance; consumers are replay-safe and tenant isolation is enforced end to end.”

## LLD classroom walkthrough — Concurrent Token Bucket

Socho har tenant ko average 10 requests/second allow karni hain, par short burst mein 20 requests tak. Bucket capacity 20 hai; request ek token leti hai aur time ke saath tokens refill hote hain.

**Invariant:** tokens 0 se neeche ya capacity se upar nahi. Refill, check aur deduction ek atomic critical section mein hone chahiye, warna do threads same last token consume kar sakte hain.

Java 17 implementation, deterministic tests, complexity aur distributed deployment caveats is [detailed Hinglish LLD](../../../system-design/hinglish/daily/2026-10-20-lld-token-bucket-rate-limiter.md) mein hain. Elapsed time ke liye System.nanoTime() use karo, wall clock nahi. Local limiter ek JVM tak limited hai; global quota ke liye shared atomic store ya gateway-level limiter chahiye.

## Aaj ke engineering updates — 20 October 2026

1. **PostgreSQL 19 RC 1, 15 October:** RC 2 12 November aur GA 19 November ke liye planned hain, issue milne par dates badal sakti hain. RC ko production GA mat samjho. Concurrent repack, planner, logical replication aur parallel autovacuum ke fixes announcement mein listed hain. [Official announcement](https://www.postgresql.org/about/news/postgresql-19-rc-1-released-3409/).
2. **AWS Lambda Web Functions, 12 October:** Native HTTP framework execution, multi-concurrent execution aur global distribution ke saath naya serverless primitive announce hua. Framework compatibility, latency, runtime limits aur observability ko API Gateway + Lambda/container approach ke against compare karo. [AWS announcement](https://aws.amazon.com/blogs/aws/announcing-aws-lambda-web-functions/).
3. **GitHub engineering, 14 October:** Star-history feature mein per-stargazer identity fetch karne ke bajaye aggregation se requests 2500 se 24 tak reduce hui. Aggregate answer chahiye toh saara raw data fetch karna zaroori nahi. [Engineering article](https://github.blog/engineering/).

## Revision checklist
Timeout vs cancellation, Kafka replay and DB idempotency, lease/fencing race, tenant isolation beyond controller, sliding-window O(n), aur audit platform ke durability/ordering/replay trade-offs explain karne ki practice karo.

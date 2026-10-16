# Interview Preparation — 2026-10-16 (English)

**Target:** Product-company SDE2/Senior Java Backend; nine original interview exercises, not attributed to any employer. Java 17+. All questions are designed for advanced backend interviews. **Learning path:** reason about invariants, derive approach, dry-run, complexity, failure cases, and follow-ups.

## Q1. ThreadLocal leakage in Spring request handlers — Hard
**Problem:** Tenant A's identifier appears in tenant B's logs under load. Explain why.
**Derivation:** ThreadLocal belongs to a worker thread, not to an HTTP request. Pooled threads are reused. A failed request that never clears context contaminates later requests. Async handoffs may lose context because a different thread is used.
**Solution:** Set context only from authenticated identity, clear in finally, propagate immutable context explicitly across async boundaries. In servlet apps, scope cleanup to a filter. Never trust tenant IDs from arbitrary headers.
```java
static final ThreadLocal<String> TENANT = new ThreadLocal<>();
void handle(String authenticatedTenant) {
    TENANT.set(authenticatedTenant);
    try { performWork(); }
    finally { TENANT.remove(); }
}
```
**Dry run:** Worker-2 handles A then throws; without finally, Worker-2 handles B while carrying A. **Complexity:** O(1) context access. **Follow-up:** MDC, virtual threads, tracing, propagation across CompletableFuture. **Interview answer:** ThreadLocal follows the worker lifecycle, so I scope it with try/finally and propagate authenticated request context explicitly.
**Reference:** https://docs.oracle.com/en/java/javase/25/docs/api/java.base/java/lang/ThreadLocal.html

## Q2. Kafka ordering with a hot account key — Hard
**Problem:** 24 partitions, 40 consumers in one group, accountId-keyed events. One VIP account produces 30% of all events.
**Derivation:** Kafka order is per partition. The account key preserves order by mapping to one partition, but creates a hot-key bottleneck. At most 24 group consumers actively own partitions; 16 are idle. Adding consumers cannot split a single account's ordering constraint.
**Solution:** Measure per-partition lag, processing rate, rebalance and poison events. Optimize per-event work, batch downstream writes, isolate VIP streams, or shard by sub-entity only if business order permits. Repartitioning may change future key mappings and requires migration planning.
**Dry run:** VIP emits 30k/sec, its partition processes 10k/sec; backlog grows 20k/sec. **Complexity:** O(1) key routing, throughput bounded by hottest partition. **Follow-up:** Can you guarantee global order? No, not across partitions. **Interview answer:** Preserve per-key order and treat hot-key capacity as a separate scaling constraint.
**Reference:** https://kafka.apache.org/documentation/

## Q3. CDC transactional outbox versus exactly-once claims — Hard
**Problem:** Order commits, Kafka publish fails. Avoid distributed 2PC.
**Derivation:** Commit the order and outbox event in the same relational transaction; Debezium/CDC reads committed changes and publishes. CDC may replay after restart; downstream consumers need durable idempotency keyed by eventId. Aggregate version and stable key help detect gaps and preserve meaningful order.
```sql
BEGIN;
INSERT INTO orders(id,status) VALUES (:id,'CREATED');
INSERT INTO outbox(event_id,aggregate_id,event_type,payload)
VALUES (:event_id,:id,'OrderCreated',:payload);
COMMIT;
```
**Failure analysis:** DB rollback → neither row exists. CDC outage → event remains durable for later publication. CDC publishes then crashes before checkpoint → duplicate possible. Consumer commit then offset failure → replay. **Complexity:** O(1) transactional writes/event plus CDC overhead. **Follow-up:** WAL retention, connector lag, poison events, schema evolution. **Interview answer:** Outbox guarantees atomic business state and publish intent; idempotent consumers handle at-least-once delivery.
**Reference:** https://debezium.io/documentation/reference/stable/transformations/outbox-event-router.html

## Q4. Cache stampede after Redis expiry — Hard
**Problem:** 20,000 concurrent requests miss a hot cache entry and hit PostgreSQL.
**Derivation:** One cache miss per caller becomes a thundering herd. Single-flight coalesces loads within one process; distributed lock/lease or stale-while-revalidate handles multiple nodes where stale reads are permitted. Add TTL jitter, bounded waiting, rate limits, and load shedding.
**Dry run:** 100 callers on one JVM → one loader, 99 await its future. **Caveat:** A JVM-local map does not coordinate across replicas; inventory reservation cannot rely on stale cache values. **Complexity:** O(1) expected coordination/caller, dominated by database load. **Follow-up:** Lock expiry, fencing tokens, negative caching, stale data policy. **Interview answer:** I coalesce concurrent loads, spread expiries and degrade safely without making the cache the source of truth.
**Reference:** https://redis.io/docs/latest/develop/use/patterns/

## Q5. PostgreSQL write skew under snapshot isolation — Hard
**Problem:** At least one doctor must be on call. Alice and Bob each see the other on call and both go off duty.
**Derivation:** PostgreSQL REPEATABLE READ can allow write skew: both transactions read a snapshot, then update different rows. No row-level lost update occurs, but a cross-row invariant breaks. SERIALIZABLE with retry on SQLSTATE 40001, or a shared guard-row lock, can enforce the invariant.
```sql
BEGIN ISOLATION LEVEL SERIALIZABLE;
SELECT COUNT(*) FROM doctors WHERE on_call = true;
-- Only go off call when at least two are on call.
UPDATE doctors SET on_call=false WHERE id=:doctor_id;
COMMIT; -- Retry entire transaction on 40001.
```
**Dry run:** Both see count=2; snapshot isolation may commit both; PostgreSQL SERIALIZABLE aborts one dangerous transaction. **Complexity:** Indexed predicate query + update, possible retries. **Follow-up:** Why SELECT FOR UPDATE of own row is insufficient; avoid external side effects before commit. **Interview answer:** I distinguish write skew from lost updates and protect the predicate-level invariant.
**Reference:** https://www.postgresql.org/docs/current/transaction-iso.html

## Q6. Retry storms and deadline budgets — Hard
**Problem:** Five service layers each retry three times during a 503 outage.
**Derivation:** Maximum amplification can reach 3^5=243 deepest-layer attempts per user request if every retry reaches it. Retrying at every layer overloads a recovering dependency.
**Solution:** Retry at one selected boundary, bounded exponential backoff with full jitter, one end-to-end deadline, retry budget, circuit breaker, bulkhead and idempotency keys for side-effecting operations. Respect 429 Retry-After when applicable.
**Dry run:** 1→3→9→27→81→243. **Complexity:** O(attempts), capped. **Follow-up:** Hedging, cancellation, circuit half-open probes, how retries affect p99. **Interview answer:** I avoid multiplicative retries by centralizing bounded attempts under the original request deadline.
**Reference:** https://aws.amazon.com/builders-library/timeouts-retries-and-backoff-with-jitter/

## Q7. DSA: Minimum Window Substring — Medium-Hard
**Problem:** Find the shortest substring of s containing all characters of t, including duplicates.
**Derivation:** Expand right until every required character instance is covered; then shrink left while valid. A deficit map and missing count track validity.
```java
static String minWindow(String s, String t) {
    if (s.isEmpty() || t.isEmpty()) return "";
    java.util.Map<Character,Integer> need = new java.util.HashMap<>();
    for (char c : t.toCharArray()) need.merge(c,1,Integer::sum);
    int missing=t.length(),left=0,best=0,len=Integer.MAX_VALUE;
    for(int right=0;right<s.length();right++){
        char c=s.charAt(right); int n=need.getOrDefault(c,0);
        if(n>0) missing--; need.put(c,n-1);
        while(missing==0){
            if(right-left+1<len){len=right-left+1;best=left;}
            char d=s.charAt(left++); int v=need.get(d)+1;
            need.put(d,v); if(v>0) missing++;
        }
    }
    return len==Integer.MAX_VALUE?"":s.substring(best,best+len);
}
```
**Dry run:** ADOBECODEBANC with ABC → BANC. **Complexity:** O(|s|+|t|) time; O(distinct UTF-16 char units) space. **Edge cases:** empty t, repeated letters, impossible window. **Follow-up:** Full Unicode code points need different iteration. **Interview answer:** Variable sliding window with deficit counts; each pointer advances at most n times.

## Q8. DSA: Minimum ship capacity in D days — Medium-Hard
**Problem:** Packages ship in original order; minimize capacity to finish in at most D days.
**Derivation:** Feasibility is monotonic. Binary search between max(weight) and sum(weights). Greedy check starts a new day when adding the next package exceeds candidate capacity.
```java
static long ship(int[] a,int days){
    if(a.length==0||days<=0) throw new IllegalArgumentException();
    long lo=0,hi=0;
    for(int x:a){if(x<=0)throw new IllegalArgumentException();lo=Math.max(lo,x);hi+=x;}
    while(lo<hi){long mid=lo+(hi-lo)/2,load=0;int used=1;
        for(int x:a){if(load+x>mid){used++;load=0;}load+=x;}
        if(used<=days)hi=mid;else lo=mid+1;
    }return lo;
}
```
**Dry run:** [1,2,3,1,1], D=4 → 3. **Complexity:** O(n log(sum−max+1)) time, O(1) space. **Follow-up:** D≥n, large sums use long, ordering constraint. **Interview answer:** I binary-search the minimum feasible capacity because the predicate is monotonic.

## Q9. AWS: Cross-account S3 reads without static credentials — Hard
**Problem:** ECS service in account A reads a narrow S3 prefix in account B.
**Design:** ECS task role A assumes dedicated read role B via STS; B trust policy permits exact principal, A identity policy permits sts:AssumeRole, B role permits narrow s3:GetObject and optional ListBucket prefix. Check bucket explicit denies, object ownership, SSE-KMS key policy and kms:Decrypt. Use short-lived credentials, TLS, public-access block and CloudTrail S3 data events as needed.
**Failure tests:** Allowed key works; disallowed prefix fails; missing KMS grant fails; VPC endpoint policy denies are visible. **Complexity:** O(1) IAM/STS setup, per-request S3 network cost. **Follow-up:** Confused deputy and external IDs for third-party roles, STS refresh, region/data residency. **Interview answer:** Least-privilege task identity plus scoped STS role assumption and independently verified S3/KMS access.
**Reference:** https://docs.aws.amazon.com/IAM/latest/UserGuide/tutorial_cross-account-with-roles.html

## HLD — Multi-tenant immutable audit-event platform
**Full 45-minute design:** [Audit Platform HLD](../../../system-design/daily/2026-10-16-hld-multitenant-audit.md).
**Requirements:** ingest immutable audit events; tenant-scoped time-range search; exports; retention/legal hold. Assume average 5k events/sec, peak 50k/sec, 1KB/event: ~432GB/day raw at average before indexing/replication. **Architecture:** authenticated producers → ingest API → durable broker → archive to S3 Object Lock and indexed OpenSearch read model → tenant-scoped query/export API. **Invariants:** no cross-tenant access; canonical archive rebuilds index; ingestion acknowledgment only after durable handoff. **Trade-offs:** Kafka vs Kinesis, search index vs cold archive, partitioning and hot tenants, eventual search consistency. **Tests:** replay, duplicate events, region loss, retention, malicious cross-tenant queries, 50k/sec soak.

## LLD — Thread-safe rolling-window circuit breaker
**Full design + Java 17 implementation:** [Circuit Breaker LLD](../../../system-design/daily/2026-10-16-lld-circuit-breaker.md).
**State machine:** CLOSED records last N completed outcomes; failure ratio opens; OPEN rejects until cooldown; HALF_OPEN allows one probe; success closes and clears history, failure reopens. **Concurrency:** serialize permission checks, CAS/lock one probe, ignore stale completion from calls admitted before OPEN. **Complexity:** O(1) with maintained failure counter and O(N) history. **Tests:** 100 simultaneous half-open callers, threshold, cooldown, late completion, hung probe and failure classification.

## Verified engineering developments (as of 2026-10-16)
**October 15, 2026 — GitHub AI Scan for pull requests generally available.** Complements CodeQL for coverage gaps; review false positives and keep threat modeling. Source: https://github.blog/changelog/month/10-2026/
**October 15, 2026 — GitHub agentic autofix for code scanning alerts generally available.** Proposed changes still need human review, regression tests and least-privilege. Source: https://github.blog/changelog/month/10-2026/
**October 14, 2026 — GitHub Actions workflow lockfiles public preview.** Locks action dependencies to reviewed commits, reducing tag drift. Check plan eligibility in docs. Source: https://github.blog/changelog/2026-10-14-github-actions-workflow-lockfiles-in-public-preview/
**October 13, 2026 — AWS Lambda Web Functions announcement.** Native Node.js HTTP server execution, concurrency and active-CPU billing; evaluate supported runtimes before assuming Java support. Source: https://aws.amazon.com/blogs/compute/
**September 21, 2026 — Spring synchronized patch train announced.** October 22 patch window is planned, not yet released. Source: https://spring.io/blog/2026/09/21/releasing-spring-for-modern-challenges/

## Revision plan
Spend 5 minutes each on Kafka hot keys, PostgreSQL write skew, HLD tenant isolation, LLD half-open concurrency and two DSA dry runs. Explain every answer aloud before reading the model solution.
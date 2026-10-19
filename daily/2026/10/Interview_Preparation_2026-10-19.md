# Interview Preparation — 2026-10-19

Daily Java backend interview preparation.

**Audience:** Product-company SDE2 / Senior Backend. Original practice exercises, not attributed to named employers. Java 17+, HLD, LLD, official references. Research cutoff: 19 Oct 2026.

## Q1 — Safe publication and double-checked locking (Java, Hard)

**Problem:** Implement lazy singleton; why is volatile needed? Without correct publication, another thread may see a reference without guaranteed visibility of initialized fields. Volatile reference establishes happens-before; synchronized protects single initialization.

~~~java
final class Registry {
    private static volatile Registry instance;
    private Registry() {}
    static Registry get() {
        Registry x=instance;
        if(x==null) synchronized(Registry.class) {
            x=instance;
            if(x==null) instance=x=new Registry();
        }
        return x;
    }
}
~~~

**Dry run:** A and B read null; A constructs inside monitor; B sees non-null on recheck. **Complexity:** O(1) time/space per access. **Edges:** constructor escape, exceptions, mutable state. **Follow-up:** Does volatile make ++ atomic? No. **Interview:** “Volatile safely publishes the instance; synchronized ensures a single construction.” Source: https://docs.oracle.com/javase/specs/jls/se17/html/jls-17.html#jls-17.4.5

## Q2 — Kafka rebalance and zombie external writes (Hard)

**Problem:** A loses partition ownership to B, but its old DB write finishes later. Kafka group epochs fence offset commits, not arbitrary external DB writes. Enforce event-ID deduplication and monotonic business versions in the DB.

~~~sql
UPDATE projections SET value=:v, version=:newVersion
WHERE entity_id=:id AND version < :newVersion;
~~~

**Dry run:** B writes version 13, stale A tries 12; affected rows=0. **Complexity:** expected O(1) indexed lookup per event plus write. **Edges:** first insert race, gaps, duplicate replay, offsets after DB commit. **Follow-up:** Why is Kafka exactly-once not external payment exactly-once? **Interview:** “I fence stale business writes with conditional versions, independently of Kafka group ownership.” Source: https://kafka.apache.org/42/operations/consumer-rebalance-protocol/


## Q3 — PostgreSQL write skew (Database, Hard)

**Scenario:** Two doctors must leave at least one on call. Each sees the other available and updates only their own row. Under snapshot isolation both may commit, violating the cross-row invariant. Locking only updated rows does not protect the predicate.

**Solution:** Lock a shared guard row, redesign the invariant, or use PostgreSQL SERIALIZABLE and retry the entire transaction on SQLSTATE 40001.

~~~sql
BEGIN ISOLATION LEVEL SERIALIZABLE;
SELECT count(*) FROM doctors WHERE on_call;
-- if count > 1: UPDATE doctors SET on_call=false WHERE id=:doctorId;
COMMIT;
~~~

**Dry run:** Both read count=2; SERIALIZABLE detects a dangerous dependency and aborts at least one. **Complexity:** query cost depends on index; retries add variable cost. **Edges:** bounded retries/jitter; avoid external side effects inside retried transaction. **Follow-up:** Why isn't REPEATABLE READ enough? **Interview:** “Write skew needs predicate-level coordination or serializable execution.” Source: https://www.postgresql.org/docs/18/transaction-iso.html

## Q4 — JWT signing-key rotation (Security, Hard)

**Scenario:** K1 signs tokens expiring at 11:00. Issuer switches to K2 at 10:15. Deleting K1 immediately invalidates valid tokens.

**Approach:** Publish K2 in trusted JWKS before using it; wait for verifier cache propagation; sign new tokens with kid=K2; retain K1 through old-token TTL plus skew. Validate signature, algorithm allowlist, issuer, audience, expiry, not-before and tenant authorization. Unknown kid triggers rate-limited JWKS refresh, never arbitrary URLs from token headers.

**Dry run:** K1 tokens keep verifying while K2 tokens verify after refresh. **Complexity:** signature verification per request. **Edges:** compromised-key emergency revocation, clock skew, region lag. **Follow-up:** How to revoke one JWT early? **Interview:** “I overlap trusted verification keys and rotate signing only after propagation.” Source: https://docs.spring.io/spring-security/reference/servlet/oauth2/resource-server/jwt.html


## Q5 — JVM heap grows while CPU remains normal (JVM, Hard)

**Scenario:** After deployment p99 latency climbs and old-generation occupancy grows, yet average CPU remains normal.

**Approach:** Compare allocation rate, post-GC live set, GC pauses, thread count and safepoints against baseline. Capture bounded JFR profile, inspect allocation hot spots and retained objects. Distinguish a leak (live set increases after collections) from allocation churn (short-lived garbage). Check unbounded caches, forgotten listeners, ThreadLocal retention and ORM entity graphs.

~~~bash
jcmd <pid> JFR.start name=investigation settings=profile duration=120s filename=/tmp/app.jfr
jcmd <pid> GC.heap_info
~~~

**Dry run:** JSON copies increase allocations 10x, GC pressure increases p99; remove copies and remeasure. **Complexity:** diagnostic overhead depends on sampling configuration. **Edges:** sensitive heap dumps, native memory vs heap, container memory limit. **Follow-up:** Why not force System.gc()? **Interview:** “I use JFR and live-set evidence before changing the GC algorithm.” Source: https://docs.oracle.com/en/java/javase/25/jfapi/flight-recorder-api-programmers-guide.pdf


## Q6 — Spring WebFlux and blocking work (Hard)

**Problem:** A blocking JDBC call runs on a Reactor event-loop and reduces throughput. **Approach:** Prefer nonblocking drivers, or isolate blocking work using boundedElastic with strict database concurrency limits. Backpressure does not create more DB connections. **Dry run:** 1000 requests, 20 DB connections: at most 20 can actively use the DB. **Complexity:** constant scheduling overhead plus queued tasks. **Edge cases:** timeouts, cancellation and queue saturation. **Interview:** “I keep event loops nonblocking and bound downstream work.”


## Q8 — Rendezvous hashing for shard assignment

For every key, compute a stable hash score against each shard; choose the highest-scoring shard. Adding a shard moves only keys for which the new shard wins. Unlike hash(key)%N, existing shard comparisons remain stable. **Dry run:** A scores 8, B scores 3 -> A; add C score 9 -> C. **Complexity:** O(number of shards) per lookup, O(1) auxiliary space. **Edges:** stable node IDs, deterministic tie-breaks, hot keys and weighted nodes. **Follow-up:** Compare with consistent-hash rings. **Interview:** “Highest-random-weight hashing minimizes remapping without a ring.”

## Q9 — Kubernetes liveness vs readiness

Liveness checks whether the process can recover without restart; readiness checks whether it should receive traffic; startup probe protects slow initialization. Do not restart healthy pods merely because a shared database is temporarily unavailable. **Dry run:** DB outage can make readiness false while liveness remains true. **Complexity:** O(1) lightweight checks. **Edges:** flapping, zero ready endpoints. **Interview:** “I separate process health from traffic eligibility.”

## Q10 — Why a larger HikariCP pool can increase latency

A database has finite CPU, I/O and lock capacity. Raising a pool from 20 to 200 may create more contention and slower queries. Measure active/idle/pending connections, acquisition time, SQL p99, lock waits, DB CPU and replica count. Little's Law gives approximate concurrency L=throughput*response-time, not a universal pool formula. **Dry run:** 200 operations/s * 0.1s DB hold time ~=20 active operations before burst allowance. **Complexity:** capacity-planning, not algorithmic. **Edges:** long transactions, virtual threads, per-pod pool multiplication. **Interview:** “I optimize query time and concurrency limits together rather than blindly adding connections.”

## Today's comprehensive system design

**HLD — Global Gaming Leaderboard:** [requirements, architecture, schema, scale, failure recovery, testing and trade-offs](../../../system-design/daily/2026-10-19-hld-global-leaderboard.md). Key decision: authoritative durable score database plus Redis rank projection; Kafka event IDs prevent replay errors.

**LLD — Concurrent LFU Cache:** [complete Java 17 implementation, invariant, tests and complexity](../../../system-design/daily/2026-10-19-lld-concurrent-lfu-cache.md). Key decision: frequency buckets + ordered sets + single monitor for atomic state transitions.

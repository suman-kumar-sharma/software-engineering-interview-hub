# Production API Latency: Complete Java/Spring Boot Debugging Playbook

**Level:** Senior Backend / Product-company interview | **Stack:** Java 17+, Spring Boot, HikariCP, MySQL, Micrometer, distributed tracing

## Interview prompt
**An API that normally responds in 150 ms now takes 3 seconds. How do you diagnose and resolve it?**

## 1. Interview opening: scope before guessing
> First I determine the blast radius and user impact: which endpoints, tenants, regions, and instances are affected; when regression began; and whether it coincides with a deployment, traffic surge, or dependency degradation. I compare p50/p95/p99 latency, request rate, error rate, and saturation against baseline. I use a trace of an affected request to find where the time is spent, then test hypotheses with application, JVM, database, and downstream metrics. I mitigate user impact safely while investigating and verify recovery before closing the incident.

**Clarifying questions:** All endpoints or one? Sudden or gradual? Only peak traffic? Recent deploy/config/schema change? Is client-perceived latency high but server latency normal? Any 429/503/504? One AZ or all? Are retries inflating load?

## 2. Triage and protect customers
1. Confirm SLO breach using **p95/p99**, not just average; check throughput, errors, saturation (RED metrics).
2. Compare API gateway/load balancer duration against server duration. Large gap suggests queueing, network, TLS, or proxy issues.
3. Correlate onset with deploy, traffic, feature flag, scheduled job, DB migration, autoscaling event.
4. If regression clearly follows release, consider canary rollback or disable flag; coordinate incident response. Rollback is not automatic if migrations are incompatible.
5. Reduce amplification: bounded retries with jitter, rate limits, load shedding, temporary cache where correctness allows. Avoid blindly increasing threads/connections.
6. Record timestamps, affected versions, dashboards, trace IDs and mitigation results.

## 3. Build a latency budget
Example **illustrative** trace for a 3,000 ms request:
| Stage | Time | Evidence | Next check |
|---|---:|---|---|
| Gateway / queue | 250 ms | ALB / gateway timing | ingress saturation |
| Authentication + controller | 30 ms | span timing | unlikely primary |
| Wait for DB connection | 1,200 ms | Hikari pending/active | pool contention |
| SQL execution | 1,300 ms | DB span / slow query log | locks, plan, indexes |
| Downstream call | 180 ms | client span | retry/timeout |
| Serialization | 40 ms | application span | payload size |
| **Total** | **3,000 ms** | illustrative sequential path | prioritize DB |

**Caveat:** Parallel spans overlap; do not sum durations blindly. Check critical path and whether timing includes queueing.

## 4. Spring Boot observability
Dependencies: `spring-boot-starter-actuator`, `micrometer-registry-prometheus`; use Micrometer Tracing with an exporter appropriate to your environment.

```properties
management.endpoints.web.exposure.include=health,info,prometheus
management.metrics.distribution.percentiles-histogram.http.server.requests=true
spring.datasource.hikari.maximum-pool-size=10
spring.datasource.hikari.connection-timeout=20000
```

**Secure actuator endpoints** (private network/authentication); do not expose sensitive management endpoints publicly. Metrics often include `http.server.requests`, `hikaricp.connections.active`, `hikaricp.connections.idle`, `hikaricp.connections.pending`, `hikaricp.connections.timeout`, JVM GC pause and process CPU metrics; verify exact names/tags for your Boot/Micrometer version.

Useful dashboard panels: endpoint/route p50/p95/p99, RPS, 5xx, active requests, Hikari active/idle/pending/timeouts, JVM heap-after-GC, GC pause time, CPU throttling, DB query latency and lock waits, outbound dependency latency. Avoid user ID and raw URL path labels (cardinality/privacy).

**Distributed tracing:** Follow trace ID gateway → service → JDBC → downstream; identify dominant span, retries and hidden wait time. Use sampled traces carefully; ensure slow/error traces are represented.

## 5. Branch by evidence, not intuition

### A. CPU saturated
- Compare process CPU, container CPU quota/throttling, per-core saturation and traffic.
- Capture Java Flight Recorder (JFR) profile and inspect hottest stacks, allocation pressure and GC.
- Correlate native thread IDs from `top -H -p <pid>` with `jcmd <pid> Thread.print -l` (IDs may require hex conversion depending on output).
- Causes: tight loops, expensive JSON/regex, crypto, excessive serialization, high GC, noisy neighbor, throttling.
- Fix hot path/algorithm, reduce allocations, tune concurrency, scale if truly CPU-bound.

### B. GC or memory pressure
```bash
jcmd <pid> GC.heap_info
jcmd <pid> JFR.start name=latency settings=profile duration=60s filename=/tmp/latency.jfr
jcmd <pid> Thread.print -l > /tmp/threads.txt
```
Commands need suitable JVM permissions; profiling/dumps have overhead and can expose sensitive data. Use production-safe policies. Look at post-GC heap trend, allocation rate, pause durations, full GCs, container memory, direct/native memory. Increasing heap alone is not proof of a leak. For leak suspicion, inspect a safely captured heap dump offline using MAT and GC roots.

### C. Thread saturation / lock contention
- Thread dump multiple times a few seconds apart. Look for many request threads waiting on same monitor, lock, socket, DB pool, or executor future.
- **RUNNABLE is not equivalent to using CPU**: Java threads in native socket I/O can appear RUNNABLE.
- Inspect Tomcat busy threads, executor active count, queue depth and rejections.
- Deadlocks: `jcmd <pid> Thread.print -l` and thread dump lock ownership.
- Mitigate long critical sections, blocking operations on constrained executors, unbounded queues, and synchronous fan-out.
- Virtual threads improve scalability of blocking waits but do not make DB pools or downstream services unlimited.

### D. DB connection pool exhausted
- Hikari **active ≈ max**, **idle = 0**, **pending > 0**, connection acquisition latency/timeouts rising.
- Determine whether the problem is leaked manually acquired JDBC connections, long transactions, slow SQL, locks, or concurrency exceeding DB capacity.
- `JdbcTemplate` and Spring Data JPA normally release managed connections appropriately; `Connection.close()` on a pooled connection returns it to pool.
- `@Transactional` can hold a connection across blocking network calls depending on when it was acquired. Keep transactions short and avoid remote calls inside them.
- Hikari leak detection can log connections held past a threshold, **not necessarily actual leaks**.
- Do not increase pool to 100 without checking MySQL max connections and aggregate replicas.

### E. Database suddenly slow (MySQL)
- Inspect slow query log / performance_schema, query fingerprints, p95 latency, DB CPU/IO, connections, transaction length, lock waits, row count/data skew, recent plan changes.
- For a representative slow query, use `EXPLAIN ANALYZE` on MySQL 8+ in a controlled environment; it **executes** the query, so avoid running expensive statements casually in production.
```sql
EXPLAIN ANALYZE
SELECT id, status
FROM orders
WHERE customer_id = 42 AND status = 'PENDING'
ORDER BY created_at DESC
LIMIT 20;
```
- Look for large scans, unexpected row estimates, expensive sort, many rows examined vs returned, and blocking locks.
- Candidate index, **after checking workload and selectivity**:
```sql
CREATE INDEX idx_orders_customer_status_created
ON orders(customer_id, status, created_at DESC);
```
- Indexes have write/storage costs; validate against actual execution plan and query workload. Full scans can be optimal on small tables or low-selectivity predicates. Check N+1 JPA queries and pagination.

### F. Downstream dependency slow
- Inspect per-host client latency, connection pool, DNS/TLS, timeout, retry counts, circuit breaker state.
- Separate connection timeout, read timeout, overall deadline. Set finite timeouts and propagate deadline across calls.
- Exponential backoff with jitter for **safe/idempotent** retries only; cap retries to avoid retry storms.
- Apply bulkheads, circuit breakers and fallback only when semantics permit.

### G. Gateway/network or payload
- Compare upstream vs application timestamps and durations; check ingress queueing, proxy timeouts, load balancer target health, network retransmits, DNS resolution and cross-region hops.
- Large response bodies, serialization, compression and synchronous logging can inflate tail latency.

## 6. Production investigation example
**Incident:** `GET /orders` p95 rises from 180 ms to 4.2 s after a release; RPS roughly unchanged.

1. Dashboards: CPU 35%, GC normal, 5xx rising; Hikari active 10/10, pending 45, timeouts growing.
2. Traces: 2.8 s waiting for connection and 1.1 s executing order query.
3. MySQL slow query log: query changed to fetch all customer orders; `EXPLAIN` shows excessive rows scanned and sort.
4. Root cause: new filter/query shape bypassed existing index, so queries hold DB connections longer. Pool saturation is a **symptom**, not the root cause.
5. Mitigate via feature rollback if safe; add bounded pagination, optimize query/index after validation.
6. Verify p95/p99, DB scanned rows, Hikari pending, timeout/error rate and customer experience return to baseline.
7. Prevent recurrence with query-plan checks for hot endpoints, representative load tests, alerts on pool wait and latency, canary deployments.

**Little's Law intuition:** At 100 requests/s with 0.2 s average in-flight duration, expected concurrency ≈ 20. If duration grows to 2 s at same arrival rate, concurrency ≈ 200. This is a *system-wide* approximation under stable conditions; it does **not** mean 200 DB connections are needed. Measure actual DB connection hold time.

## 7. Common interview follow-ups
**Q: CPU normal but API slow?** A: Investigate I/O waits, DB pool queueing, locks, network, external dependencies, thread queues and ingress.

**Q: Only p99 high?** A: Tail latency from sporadic GC pauses, slow DB plans/locks, retry outliers, noisy neighbors or uneven load; inspect traces of slowest requests.

**Q: Hikari max 10, 200 concurrent requests: failure?** A: Not necessarily. Requests can wait and many do not hold connections simultaneously. Failure depends on connection hold duration, arrival rate, queueing and acquisition timeout.

**Q: Can we just scale pods?** A: Only if bottleneck is horizontally scalable; more pods can overwhelm shared DB or downstream, and each pod adds its own pool.

**Q: Is an index always the answer?** A: No. Inspect execution plan, selectivity, statistics, lock waits and write trade-offs first.

**Q: How to prove a fix?** A: Compare same workload and percentiles, error rate, DB metrics, traces and SLO; monitor after deployment.

## 8. Hinglish: student-friendly mental model
Socho ek restaurant mein 10 tables hain (Hikari pool). Customers (API requests) badh gaye, lekin waiter (SQL query) har table ko 2 second ki jagah 20 second tak occupy kar raha hai. Bahar line badhegi. Sirf tables 10 se 50 kar dena zaroori nahi ki solution ho: kitchen (MySQL) overload ho sakta hai.

**Debug ka sequence:** Pehle dekho *kaunsi API* slow hai aur *kab se*. Phir p95/p99, error rate aur traffic compare karo. Trace mein dekho time kahan ja raha hai: connection lene mein, SQL chalane mein, ya dusri service ke response mein? Agar Hikari pending high hai, turant pool badhane se pehle pata karo connection itni der busy kyun hai. SQL slow hai toh query plan, index, locks aur N+1 queries check karo. Fix ke baad same traffic pe latency aur pool metrics verify karo.

**Interview mein bolo:** “Main pehle symptom ko measure karunga, phir trace se bottleneck isolate karunga, evidence-based mitigation lagaunga, aur regression prevention ke liye alerts/load tests add karunga.”

## 9. Checklist for on-call
- [ ] Baseline vs current p50/p95/p99, RPS, errors, impacted routes
- [ ] Compare ingress latency with app spans
- [ ] Recent deploy/flags/config/DB changes
- [ ] CPU quota, GC pauses, heap-after-GC, executor saturation
- [ ] Hikari active/idle/pending/timeouts
- [ ] MySQL slow queries, EXPLAIN, locks, IO
- [ ] Downstream timeouts, retries, breaker state
- [ ] Safe mitigation and rollback strategy
- [ ] Verify SLO and root cause; document timeline and prevention

## References
- [Spring Boot Actuator metrics](https://docs.spring.io/spring-boot/reference/actuator/metrics.html)
- [Spring Boot tracing](https://docs.spring.io/spring-boot/reference/actuator/tracing.html)
- [HikariCP configuration](https://github.com/brettwooldridge/HikariCP)
- [MySQL EXPLAIN ANALYZE](https://dev.mysql.com/doc/refman/8.0/en/explain.html)
- [JDK jcmd](https://docs.oracle.com/en/java/javase/21/docs/specs/man/jcmd.html)

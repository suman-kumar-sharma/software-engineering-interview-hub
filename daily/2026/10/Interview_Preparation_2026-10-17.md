# Interview Preparation — 2026-10-17 (English)
**Track:** 9 original product-company SDE2/Senior backend questions; detailed HLD and LLD linked below. These are practice exercises, not verified questions from named companies. No private LinkedIn data accessed.

**System design:** [HLD — notification platform](../../../system-design/daily/2026-10-17-hld-notification-platform.md) | [LLD — concurrent token bucket](../../../system-design/daily/2026-10-17-lld-token-bucket.md). Companion: [Hinglish](../../../daily/hinglish/2026/10/Interview_Preparation_2026-10-17.md).

## Q1 — 20k RPS enters an API; downstream handles 2k RPS. Prevent overload.
**Derive:** Sustained arrival > capacity makes an unbounded queue grow. Limit active downstream calls, bound queue, set total deadlines and reject excess work (429 quota or 503 saturation). If product permits asynchronous completion, accept durably with 202 and expose status. Virtual threads don't increase downstream capacity.
~~~java
import java.util.concurrent.*;
class BoundedService {
  final ThreadPoolExecutor pool = new ThreadPoolExecutor(
      16,16,0,TimeUnit.MILLISECONDS,new ArrayBlockingQueue<>(200),
      new ThreadPoolExecutor.AbortPolicy());
  Future<String> submit() { return pool.submit(() -> "done"); }
}
~~~
**Dry run:** 16 active + 200 waiting; next rejected. Sizes illustrative. **Complexity:** O(1) queue admission, bounded memory. **Twist:** CallerRunsPolicy may block HTTP/event-loop threads; enforce retry budgets. **Answer:** “I protect the dependency with bounded concurrency, queueing and explicit load shedding.”
Source: https://docs.oracle.com/en/java/javase/17/docs/api/java.base/java/util/concurrent/ThreadPoolExecutor.html

## Q2 — Kafka rebalance after payment charge but before offset commit.
**Timeline:** A charges P42; crashes before offset commit; B replays P42. Kafka ordering/transactions do not atomically include arbitrary payment gateways. **Solution:** stable payment ID, DB unique constraint, durable state machine, gateway idempotency key, and reconciliation for unknown timeouts. Commit offset after durable handling. **Dry run:** B reuses P42 and finds completed business state; no second charge. **Twist:** a timeout is not proof of failure. **Answer:** “I design payment side effects to be idempotent across rebalances and crashes.”
Sources: https://kafka.apache.org/documentation/ ; https://docs.spring.io/spring-kafka/reference/kafka/receiving-messages/message-listener-container.html

## Q3 — PostgreSQL queue with concurrent job claims.
Use short transactions and SKIP LOCKED. External processing happens after claim commit:
~~~sql
WITH picked AS (
 SELECT id FROM jobs WHERE status='READY' AND run_at<=now()
 ORDER BY run_at,id FOR UPDATE SKIP LOCKED LIMIT 50
)
UPDATE jobs j SET status='RUNNING',owner=:worker,
 lease_until=now()+interval '2 minutes'
FROM picked WHERE j.id=picked.id RETURNING j.*;
~~~
**Dry run:** Worker A locks 1–50; B skips and claims 51–100. **Failure:** A crashes; lease expires and job is reclaimed; external effects need idempotency. **Complexity:** Index-dependent, use EXPLAIN ANALYZE; add index on ready run_at/id. **Twist:** stale worker must not finalize after ownership changes. **Answer:** “SKIP LOCKED coordinates claims; leases and idempotency handle crash recovery.”
Source: https://www.postgresql.org/docs/current/sql-select.html

## Q4 — Redis lease expires while an old worker is paused.
**Problem:** A owns lease token 41, pauses; B takes over with 42 and writes; A resumes and can corrupt state. **Solution:** Monotonically increasing fencing tokens, validated atomically by authoritative storage. Accept 42, reject stale 41. A Redis compare-and-delete lock release is not sufficient fencing. **Trade-off:** if storage cannot check tokens, use conditional DB version updates or a single writer. **Answer:** “Leases coordinate work; fencing protects the resource from stale owners.”
Source: https://redis.io/docs/latest/develop/use/patterns/distributed-locks/

## Q5 — Heap rises after each request. Investigate ThreadLocal.
Pooled threads outlive requests; forgotten ThreadLocal values can retain request objects and even leak user context.
~~~java
private static final ThreadLocal<RequestContext> CTX = new ThreadLocal<>();
void handle(RequestContext ctx) {
  CTX.set(ctx);
  try { process(); } finally { CTX.remove(); }
}
~~~
**Diagnosis:** compare post-GC baselines; inspect heap dominators, GC roots, allocation rate and thread-local references. Rising heap alone does not prove leak. **Complexity:** expected O(1) get/set/remove. **Twist:** async handoff doesn't automatically propagate context. **Answer:** “I prove retention with heap evidence and clean request state in finally.”
Source: https://docs.oracle.com/en/java/javase/17/docs/api/java.base/java/lang/ThreadLocal.html

## Q6 — Shortest nonempty subarray with sum >= K (negative values allowed).
Ordinary sliding window fails because negative values break monotonic sums. Let prefix P[j]-P[i]>=K; minimize j-i. Maintain increasing prefix sums in a deque.
~~~java
import java.util.*;
class Shortest {
  static int solve(int[] a,long k) {
    int n=a.length,ans=n+1; long[] p=new long[n+1];
    for(int i=0;i<n;i++) p[i+1]=p[i]+a[i];
    Deque<Integer> q=new ArrayDeque<>();
    for(int j=0;j<=n;j++){
      while(!q.isEmpty()&&p[j]-p[q.peekFirst()]>=k)
        ans=Math.min(ans,j-q.removeFirst());
      while(!q.isEmpty()&&p[j]<=p[q.peekLast()]) q.removeLast();
      q.addLast(j);
    }
    return ans==n+1?-1:ans;
  }
}
~~~
**Dry run:** [2,-1,2], K=3 -> prefix [0,2,1,3] -> 3. **Proof:** each index enters/exits deque once; O(n) time and O(n) space. **Edge cases:** all negatives, no answer, duplicates, overflow (long). **Answer:** “Prefix sums and a monotonic deque replace invalid two-pointer reasoning.”
Source: https://docs.oracle.com/en/java/javase/17/docs/api/java.base/java/util/ArrayDeque.html

## Q7 — Retry storms and cascading failures.
Four layers each making up to three attempts can amplify one request to 3^4=81 downstream attempts, depending on call graph. Set end-to-end deadline, one retry layer, bounded attempts, exponential backoff with jitter, circuit breaker, bulkhead and load shedding. Retry only transient/idempotent operations. **Twist:** POST payment without idempotency key is unsafe to retry blindly. **Answer:** “I enforce a retry budget and total deadline to avoid amplifying overload.”
Source: https://aws.amazon.com/builders-library/timeouts-retries-and-backoff-with-jitter/

## Q8 — Secure refresh-token rotation.
Short-lived access JWT, opaque rotating refresh token. Store protected token hash, family/session ID, generation and expiry. Refresh in one DB transaction: lock current token, validate, mark used, create next generation. Two concurrent refresh requests: only one succeeds. Old-token reuse may indicate theft; revoke family under risk policy. Secure HttpOnly cookie, HTTPS, CSRF model, entropy, audit and logout. Access JWT remains valid until expiry unless separately revoked. **Answer:** “I treat refresh rotation as an atomic state transition and detect token-family replay.”
Source: https://datatracker.ietf.org/doc/html/rfc9700

## Q9 — Hot cache key expires; 30,000 requests hit the database.
Cache stampede: synchronized misses overload origin. Choose freshness contract first. Use per-key single-flight, TTL jitter, bounded origin concurrency, proactive refresh, or stale-while-revalidate where acceptable. **Dry run:** one refresh per replica rather than thousands; distributed coordination may be needed. **Twist:** bank balance cannot tolerate arbitrary stale data. **Answer:** “I combine single-flight and jitter with explicit consistency requirements.”
Source: https://aws.amazon.com/builders-library/caching-challenges-and-strategies/

## Engineering developments — source-checked
- **15 Oct 2026, GitHub:** Agentic autofix GA for eligible enterprise code-scanning alerts; it proposes fixes, reruns CodeQL and creates draft PRs. Review security changes and budget. https://github.blog/changelog/2026-10-15-agentic-autofix-for-code-scanning-alerts-general-availability/
- **15 Oct 2026, GitHub:** Multi-repository support in Slack/Teams workflows. Evaluate permission boundaries. https://github.blog/changelog/2026-10-15-copilot-in-slack-and-teams-adds-model-and-repository-updates/
- **13 Oct 2026, AWS:** Lambda Web Functions walkthrough for standard Node.js HTTP servers; do not assume Java support at launch. https://aws.amazon.com/blogs/compute/exploring-aws-lambda-web-functions-for-building-web-applications/
- **13 Oct update, security research:** GitGuardian reports GhostAction malicious CI workflow campaign. Review workflow changes and secrets. https://blog.gitguardian.com/ghostaction-github-actions-supply-chain-attack-returns/
- **30 Sep 2026, AWS:** Aurora PostgreSQL direct Iceberg/Parquet querying using DuckDB integration. Consider IAM and query isolation. https://aws.amazon.com/blogs/aws/amazon-aurora-postgresql-now-supports-direct-querying-of-apache-iceberg-and-parquet-data-in-your-data-lake/

**Revision:** 5 min deque dry run; 5 min Kafka replay; 5 min SQL claims; 10 min HLD failure modes; 10 min LLD concurrency tests. All questions are original exercises, not attributed to employers.

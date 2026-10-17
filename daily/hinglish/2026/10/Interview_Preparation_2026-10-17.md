# Interview Preparation — 2026-10-17 | Hinglish Classroom
**Aaj ka focus:** Nine original SDE2/Senior backend questions. Main teacher ki tarah real-world example se samjhaunga, phir approach derive karenge, twist aur English answer. Ye kisi company ke verified questions nahi. Private LinkedIn feed access nahi hua.

**Full system design:** [HLD Hinglish](../../../../system-design/hinglish/daily/2026-10-17-hld-notification-platform.md) | [LLD Hinglish](../../../../system-design/hinglish/daily/2026-10-17-lld-token-bucket.md) | [English daily](../../../../daily/2026/10/Interview_Preparation_2026-10-17.md).

## Q1 — 20k RPS aa raha hai, downstream 2k hi handle kar sakta hai
**Socho:** Restaurant mein 20,000 orders/sec aa rahe hain, kitchen sirf 2,000 bana sakti hai. Tum unlimited queue banaoge toh waiting time aur memory dono badhenge. Backend mein same problem hai.
**Solution kaise nikala?** Downstream safe concurrency measure karo, worker count aur waiting queue bounded rakho, request deadline do, extra requests ko 429 (quota) ya 503 (server overload) return karo. Product async completion allow kare toh durable queue + 202 status do.
~~~java
ThreadPoolExecutor pool=new ThreadPoolExecutor(
 16,16,0,TimeUnit.MILLISECONDS,new ArrayBlockingQueue<>(200),
 new ThreadPoolExecutor.AbortPolicy());
~~~
**Dry run:** 16 active + 200 waiting, next reject. Numbers illustrative hain. **Twist:** Virtual threads downstream capacity nahi badhate; CallerRunsPolicy HTTP thread block kar sakti hai. **Complexity:** O(1) admission, bounded memory.
**Interview English:** “I bound concurrency and queue depth and load-shed excess work instead of allowing unbounded latency.”

## Q2 — Kafka rebalance ke baad double payment?
**Maan lo:** A consumer ne ₹1,000 charge kiye, lekin offset commit se pehle crash ho gaya. Naya consumer same Kafka message dobara padh lega. Kafka ko external gateway payment ka atomic proof nahi mila.
**Approach:** Stable paymentId, DB unique constraint, durable status, provider idempotency key aur UNKNOWN timeout reconciliation. Offset durable processing ke baad commit karo.
**Dry run:** P42 dobara aaya; gateway ko same key bhejo, completed state check karo, duplicate charge roko. **Twist:** Sirf exists-check race-safe nahi; DB transaction/constraint chahiye. Timeout ko definite failure mat samjho.
**Interview English:** “I make payment effects idempotent across consumer rebalances and crashes.”

## Q3 — PostgreSQL mein 20 workers same job na uthayein
**Socho:** 20 teachers 100 answer sheets check kar rahe hain. Ek sheet par do teachers wasteful duplicate kaam na karein. DB row lock se claim karo, doosre worker ko locked row skip karne do.
~~~sql
WITH picked AS (
 SELECT id FROM jobs WHERE status='READY' AND run_at<=now()
 ORDER BY run_at,id FOR UPDATE SKIP LOCKED LIMIT 50
)
UPDATE jobs j SET status='RUNNING',owner=:worker,
 lease_until=now()+interval '2 minutes'
FROM picked WHERE j.id=picked.id RETURNING j.*;
~~~
**Dry run:** A claims 1–50; B skips and claims 51–100. HTTP processing transaction ke bahar karo. **Crash twist:** Lease expire par job reclaim hogi, so side effect idempotent rakho aur stale completion owner/version se reject karo. **Complexity:** index/plan dependent; EXPLAIN ANALYZE.
**Interview English:** “I use short SKIP LOCKED claims with leases and idempotent recovery.”

## Q4 — Redis lock hone par bhi stale worker kaise likh deta hai?
**Maan lo:** A ka lease 10 second tha, woh 15 second pause ho gaya. B ko new lease mil gaya aur B ne data update kiya. A resume hoke purana data overwrite kar sakta hai!
**Root cause:** Lock expire hone se purana process physically write karna band nahi karta.
**Solution:** Increasing fencing token: A=41, B=42. Storage latest token 42 accept kare, late 41 reject kare. Redis compare-and-delete safe unlock hai, fencing nahi.
**Twist:** Agar downstream token validate nahi karta, DB version conditional update ya single-writer model choose karo.
**Interview English:** “Fencing tokens prevent stale lease owners from mutating authoritative state.”

## Q5 — Java ThreadLocal se memory leak kaise?
**Socho:** Office desk par har employee file chhod de aur desk reuse hota rahe. Thread pool worker bhi multiple requests handle karta hai; ThreadLocal mein purana request object retained reh sakta hai.
~~~java
private static final ThreadLocal<RequestContext> CTX=new ThreadLocal<>();
void handle(RequestContext c) {
 CTX.set(c);
 try { process(); } finally { CTX.remove(); }
}
~~~
**Diagnosis:** Post-GC heap compare karo, heap dump dominator tree aur GC roots inspect karo. Rising heap alone proof nahi. **Twist:** Security context next user request mein leak ho sakta hai; async context propagation explicit hoti hai.
**Interview English:** “I confirm retention with heap evidence and remove per-request ThreadLocal values in finally.”

## Q6 — Negative numbers ke saath shortest subarray >= K
**Maan lo:** Array [2,-1,2], K=3. Normal sliding window reliable nahi, kyunki -1 add karne se sum ghatega. Hume prefix sum use karna hoga.
**Derivation:** P[j]-P[i]>=K aur j-i minimum. Increasing prefix values ke candidate indices deque mein rakho. Front se valid window mile toh answer update; back se dominated prefix remove.
~~~java
import java.util.*;
class Shortest {
 static int solve(int[] a,long k){
  int n=a.length,ans=n+1;long[] p=new long[n+1];
  for(int i=0;i<n;i++)p[i+1]=p[i]+a[i];
  Deque<Integer> q=new ArrayDeque<>();
  for(int j=0;j<=n;j++){
   while(!q.isEmpty()&&p[j]-p[q.peekFirst()]>=k)
    ans=Math.min(ans,j-q.removeFirst());
   while(!q.isEmpty()&&p[j]<=p[q.peekLast()])q.removeLast();
   q.addLast(j);
  }
  return ans==n+1?-1:ans;
 }
}
~~~
**Dry run:** Prefix [0,2,1,3], j=3 par sum 3, answer length 3. **Complexity:** Har index once push/pop -> O(n) time/space. **Twist:** No answer, all negatives, long prefix sums.
**Interview English:** “I use prefix sums and a monotonic deque because negative values invalidate ordinary two pointers.”

## Q7 — Retry laga kar outage aur bada kyun?
**Socho:** Restaurant ka phone busy hai aur har caller teen baar redial kare. Receptionist aur busy ho jayega. Microservices mein four layers × three attempts worst-case 3^4=81 downstream attempts bana sakte hain (call graph dependent).
**Solution:** Total deadline, bounded retry budget at one layer, exponential backoff+jitter, circuit breaker, concurrency limit aur load shedding. Sirf transient/idempotent failures retry.
**Twist:** Payment POST bina idempotency key retry mat karo.
**Interview English:** “I use deadlines and retry budgets so recovery logic does not amplify overload.”

## Q8 — Refresh token rotation ko secure kaise banayein?
**Maan lo:** Attacker ne user ka old refresh token copy kar liya. Agar woh baar-baar use kar sake toh session hijack risk hai.
**Solution:** Opaque random refresh token; DB mein protected hash, family ID, generation, expiry. Refresh transaction mein current token lock, validate, old used mark, next generation issue. Do simultaneous refresh mein ek hi success. Old token reuse theft signal ho sakta hai; risk policy se family revoke.
**Twist:** Access JWT expiry tak valid rahega jab tak separate revocation/denylist nahi. Secure HttpOnly cookies, HTTPS, CSRF model bhi important.
**Interview English:** “I rotate refresh tokens atomically and detect replay at the token-family level.”

## Q9 — Hot cache key expire aur 30k requests DB ko hit karein
**Socho:** Stadium ticket counter sabke liye same second khula. Har request origin DB tak gayi toh system gir sakta hai. Isko cache stampede bolte hain.
**Step-by-step:** Pehle freshness requirement decide karo. Per-key single-flight se ek refresh per replica; TTL jitter se synchronized expiry avoid; stale-while-revalidate sirf stale-tolerant data par; DB concurrency bounded.
**Dry run:** 30k misses ko per-replica one refresh tak reduce kar sakte ho. **Twist:** Bank balance/inventory ko arbitrary stale return nahi kar sakte.
**Interview English:** “I combine single-flight, TTL jitter and origin protection based on the freshness contract.”

## Verified engineering developments
- **15 Oct 2026:** GitHub agentic autofix GA for eligible enterprise security alerts; CodeQL rerun and draft PR, human review required. https://github.blog/changelog/2026-10-15-agentic-autofix-for-code-scanning-alerts-general-availability/
- **15 Oct 2026:** GitHub Slack/Teams multi-repository support; cross-repo permission boundary samjho. https://github.blog/changelog/2026-10-15-copilot-in-slack-and-teams-adds-model-and-repository-updates/
- **13 Oct 2026:** AWS Lambda Web Functions standard Node.js server walkthrough; Java launch support assume mat karo. https://aws.amazon.com/blogs/compute/exploring-aws-lambda-web-functions-for-building-web-applications/
- **13 Oct update:** GitGuardian GhostAction CI workflow tampering research; token/secret hygiene review karo. https://blog.gitguardian.com/ghostaction-github-actions-supply-chain-attack-returns/
- **30 Sep 2026:** Aurora PostgreSQL Iceberg/Parquet queries with DuckDB; IAM, S3 cost aur isolation evaluate karo. https://aws.amazon.com/blogs/aws/amazon-aurora-postgresql-now-supports-direct-querying-of-apache-iceberg-and-parquet-data-in-your-data-lake/

**Revision:** Kafka replay timeline draw karo; shortest-subarray deque dry run; SQL job claim explain; linked HLD/LLD whiteboard par 45 minutes mein practice karo.

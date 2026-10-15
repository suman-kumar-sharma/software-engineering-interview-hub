# Interview Preparation — 2026-10-15 | Hinglish Classroom

**Audience:** Product-company SDE2/Senior Backend. Original practice questions, kisi company ka verified interview transcript nahi. Socho main whiteboard par tumhe padha raha hoon: situation → problem → logic → interviewer twist → English answer.

## Q1. CompletableFuture: 250 ms deadline
Maan lo dashboard ko profile, points aur recommendations teen services se chahiye. Profile mandatory hai, recommendations optional. Ab recommendation service 2 second le rahi hai. Kya user ko bhi wait karwana hai? Nahi! Request start hote hi common deadline note karo; independent calls parallel chalao. Profile ko remaining budget tak wait karo aur optional fields late hon toh fallback do. **Twist:** `future.cancel(true)` se HTTP call hamesha stop nahi hoti; client timeout aur bounded concurrency bhi chahiye. **English:** “I enforce one end-to-end deadline and degrade optional dependencies while protecting downstream capacity.”

## Q2. PostgreSQL SKIP LOCKED queue
Socho 50 delivery boys ek hi parcel list se parcels utha rahe hain. Same parcel do log utha lenge toh duplicate delivery! Database mein `SELECT ... FOR UPDATE SKIP LOCKED` se short transaction mein jobs claim karo. Job RUNNING, lease expiry aur attempt number store karo. Worker crash ho toh lease expire hone par doosra claim kar sake. **Twist:** A pause hua, B ne attempt 2 claim kiya, A wapas aaya. A ka DONE update `WHERE attempt=1` fail hona chahiye. External payment double na ho, iske liye idempotency alag chahiye. **English:** “I combine atomic claims, expiring leases, generation checks and idempotent effects.”

## Q3. Kafka zombie consumer
Maan lo A partition process kar raha tha aur GC pause aa gaya. Kafka ne partition B ko de diya. B ne version 42 save kiya. A jagkar version 41 likhne laga. **Problem:** Partition ownership change external database ko protect nahi karta. **Fix:** Events ko account ID se key karo aur DB mein atomic version check rakho: `UPDATE projection SET version=:v WHERE id=:id AND version<:v`. A ka old update reject. Agar saare intermediate events chahiye toh previous version exactly match karao aur gaps retry karo. **English:** “I fence stale consumers at the state store and deduplicate events.”

## Q4. Spring REQUIRES_NEW audit rollback
Socho `placeOrder()` ne `this.audit()` call kiya aur baad mein exception throw kar di. Audit par `REQUIRES_NEW` tha, phir bhi rollback kyu? Default Spring proxy model mein same-object call proxy bypass karta hai. Audit method ko alag injected Spring bean mein move karo, tab independent transaction start ho sakta hai. **Twist:** REQUIRES_NEW ko extra DB connection lag sakta hai; pool size matter karta hai. **English:** “I invoke the transactional method through a proxy and verify the independent commit.”

## Q5. Redis lock aur fencing
Socho worker A ne 30-second lock liya, 40 second pause hua. B ko expired lock mil gaya. A resume hua. Ab dono write kar sakte hain! Lock expiry old thread ko stop nahi karti. **Fix:** Har owner ko monotonically increasing fencing token do, DB latest accepted token se purane writes reject kare. A=7, B=8: B ke baad A fail. **Twist:** Random UUID fencing number nahi; token source ka ordering guarantee important hai. **English:** “A lease needs storage-enforced fencing or transactional idempotency.”

## Q6. Shortest subarray sum >= K with negatives
Array `[2,-1,2]`, K=3: answer 3. Normal sliding window negatives ke saath fail kar sakti hai, kyunki left se negative remove karne par sum badh sakta hai. Prefix sum `p[i]` banao. Hume smallest `i-j` chahiye jahan `p[i]-p[j]>=K`. Increasing-prefix monotonic deque useful candidate indices rakhti hai. Har index ek baar push/pop: **O(n) time, O(n) space**. **Twist:** Sum ke liye `long` use karo to avoid overflow. **English:** “Negative numbers break the normal window invariant, so I use prefix sums and a monotonic deque.”

## Q7. RDS restore ke baad 10x slow queries
Maan lo disaster recovery drill mein RDS PostgreSQL snapshot restore hua. Instance ready hai, lekin first queries slow. Index suddenly kharab nahi hua: EBS blocks lazily hydrate ho rahe ho sakte hain. AWS ke 14 October 2026 analysis mein 10–15x query slowdown observed. **Fix:** Critical heap, indexes, TOAST ko `pg_prewarm` prefetch se warm karo; storage initialization progress monitor karo. `VACUUM ANALYZE` statistics refresh karta hai, full hydration guarantee nahi. **English:** “I distinguish storage hydration from SQL-plan regressions and prewarm before production cutover.” Source: https://aws.amazon.com/blogs/database/reducing-storage-initialization-impact-in-amazon-rds-for-postgresql-after-snapshot-restore/

## Q8. Java safe publication
Socho 100 threads singleton access kar rahe hain. Double-checked locking mein `instance` volatile nahi, toh safe publication guarantee missing ho sakti hai. **Fix:** `private static volatile Singleton instance;` plus correct synchronization, ya simpler initialization-on-demand holder. `volatile` visibility aur ordering deta hai, `count++` ko atomic nahi banata. **English:** “Volatile establishes the publication guarantees needed by double-checked locking.”

## Q9. Refresh token rotation
User ke paas refresh token R1 hai; attacker ne copy kar liya. User R1 exchange karke R2 leta hai. Attacker R1 reuse kare toh kya? Server hashed token ID aur family track kare; rotation atomic ho; reused token detect ho toh family revoke aur reauthentication. **Twist:** Parallel client refresh race bhi replay jaisi lag sakti hai. Client single-flight aur explicit server policy rakho. **English:** “I rotate tokens atomically, detect reuse and revoke compromised session families.”

## HLD. Flash-sale inventory reservation
Socho 10 lakh users 10,000 shoes ke liye ek minute mein request bhejte hain. **Invariant:** Available stock negative nahi hona chahiye. API: `POST /reservations` with idempotency key; `GET /reservations/{id}`; confirm, cancel, expire. Gateway → rate limit → Reservation Service → PostgreSQL inventory and reservation tables → outbox → Kafka → payment/order services. Redis optional read cache hai, stock correctness ka source nahi. DB transaction mein conditional stock reserve + reservation insert karo. States PENDING, CONFIRMED, EXPIRED, CANCELLED; expiry vs payment confirmation race ko conditional transitions se solve karo. 1,000,000/60 ≈ 16,667 requests/s; admission limit karo. Metrics: oversell, p99, DB locks, expiry lag, unknown payments. **English:** “I enforce stock invariants transactionally, then scale admission and asynchronous workflows separately.”

## LLD. Concurrent delayed-task scheduler
Socho app ko 5 minute baad reminder bhejna hai. Multiple producers tasks add karte hain, worker due task run karta hai, user cancel bhi kar sakta hai. Classes: `Job(id, dueTime, action, AtomicReference<State>)`; `DelayQueue<Job>` ordering ke liye; `ConcurrentHashMap<UUID,Job>` lookup ke liye. States SCHEDULED → RUNNING → DONE, ya SCHEDULED → CANCELLED. Worker `compareAndSet(SCHEDULED,RUNNING)` se claim kare; cancel sirf SCHEDULED se success. **Twist:** Task exception worker ko kill na kare; catch and record. In-memory scheduler restart par tasks lose karega—durability chahiye toh DB-backed jobs. **English:** “I separate due-time ordering from task identity and enforce transitions atomically.”

## Engineering developments (verified)

- **14 Oct 2026:** GitHub Actions workflow lockfiles public preview; dependency references ko reviewed commits se pin karne ki capability: https://github.blog/changelog/2026-10-14-github-actions-workflow-lockfiles-in-public-preview/
- **14 Oct 2026:** AWS RDS PostgreSQL restore hydration aur `pg_prewarm` guide: https://aws.amazon.com/blogs/database/reducing-storage-initialization-impact-in-amazon-rds-for-postgresql-after-snapshot-restore/
- **14 Oct 2026:** AWS re:Invent serverless guide highlights Lambda durable functions, Managed Instances and MicroVMs: https://aws.amazon.com/blogs/compute/your-serverless-attendees-guide-to-aws-reinvent-2026/
- **21 Sep announcement, upcoming 22 Oct:** Spring portfolio patch-train cadence; future release, abhi shipped claim nahi: https://spring.io/blog/2026/09/21/releasing-spring-for-modern-challenges/

**Revision:** Q6 deque dry run, Q2 lease race, Q3 zombie consumer, HLD stock invariant aur LLD cancel-vs-run race khud explain karo.

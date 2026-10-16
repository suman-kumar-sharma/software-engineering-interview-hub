# Interview Preparation — 2026-10-16 | Hinglish Classroom Edition

**Aaj ka target:** Product-company SDE2/Senior Backend. Ye original practice questions hain, kisi company ke verified interview transcripts nahi. Socho main tumhe whiteboard par padha raha hoon: pehle real-world situation, phir root cause, phir solution ka reasoning, Java code/dry run, tricky follow-ups aur professional English answer.

## Q1. ThreadLocal mein doosre tenant ka data kyun aa raha hai? (Hard)
**Socho** tumne ek Spring Boot SaaS banaya. Tenant A ki request process hui aur uska tenantId ThreadLocal mein set hua. Request ke beech exception aayi. Ab wohi thread pool se nikal kar tenant B ki request handle karne laga. Logs mein A ka tenantId dikh gaya!
**Ab problem kya hai?** ThreadLocal ka lifetime HTTP request nahi, thread ka lifetime hai. Pool threads reuse hote hain. Agar cleanup nahi kiya toh stale data next request tak survive karega. Doosri taraf, async execution doosre thread par gaya toh context missing bhi ho sakta hai.
**Chalo solution derive karein:** Authenticated identity se tenant resolve karo. Har request ke start mein set karo, finally mein remove karo. Async task ko immutable request context explicitly pass karo. Sirf header se tenantId trust mat karo.
```java
static final ThreadLocal<String> TENANT = new ThreadLocal<>();
void handle(String authenticatedTenant) {
    TENANT.set(authenticatedTenant);
    try { doWork(); }
    finally { TENANT.remove(); }
}
```
**Dry run:** Worker-1 pe A → exception → finally clears. Ab B aaye toh stale A nahi milega. **Complexity:** O(1). **Interviewer twist:** MDC, CompletableFuture context propagation, virtual threads. **English answer:** 'ThreadLocal is scoped to the worker thread, so I clean it in finally and propagate authenticated context explicitly.'

## Q2. Kafka mein ordering chahiye, par ek account bahut hot hai (Hard)
**Maan lo** 24 Kafka partitions hain aur tumne 40 consumers deploy kar diye. Tum soch rahe ho 40 consumers = zyada throughput. Lekin consumer group mein ek partition ko ek waqt mein ek consumer own karta hai, toh maximum 24 active honge. 16 idle rahenge.
**Ab VIP account ka twist:** Ek account 30% traffic generate karta hai. AccountId ko key banaya hai taaki uske A1,A2,A3 events order mein rahen. Saare events ek partition mein jayenge. Wahi partition bottleneck ban gaya. Aur consumers add karne se us account ka traffic split nahi hoga.
**Solution kaise sochen?** Pehle partition-wise lag measure karo. Processing optimize karo, downstream batch karo, VIP ke liye dedicated stream consider karo. Agar business allow kare tabhi sub-entity based sharding; warna account-wide ordering toot jayegi.
**Dry run:** VIP 30k events/sec bhejta hai, consumer 10k process karta hai; backlog har second 20k badhta hai. **Complexity:** key routing O(1), throughput hot partition se limited. **Twist:** Partition count badhane se future key mapping change ho sakti hai; migration carefully plan karo. **English answer:** 'I preserve per-account order through keyed partitions and address hot-key capacity separately from consumer count.'

## Q3. Database order save hua, Kafka event publish fail: Outbox (Hard)
**Socho** customer ne order place kiya. Database mein order CREATED save ho gaya. Ab OrderCreated event Kafka par bhejna tha, lekin network fail. Inventory service ko event nahi mila. Data inconsistent ho gaya.
**Root cause:** DB commit aur Kafka publish do alag systems hain. Local transaction sirf database ko atomic banati hai; Kafka ko nahi.
**Approach:** Same DB transaction mein order row aur outbox event row dono insert karo. CDC/Debezium committed outbox rows read karke Kafka par publish karega. Agar CDC down ho toh event durable rahega. Agar publish ke baad crash ho gaya toh event dobara aa sakta hai—consumer ko idempotent banana padega.
```sql
BEGIN;
INSERT INTO orders(id,status) VALUES (:id,'CREATED');
INSERT INTO outbox(event_id,aggregate_id,payload)
VALUES (:event_id,:id,:payload);
COMMIT;
```
**Dry run:** DB rollback → dono rows absent. DB commit + CDC crash → outbox row present, baad mein publish. CDC duplicate publish → consumer eventId se deduplicate kare. **Complexity:** constant extra DB write per event. **Twist:** WAL retention, ordering by aggregateId, schema changes, replication lag. **English answer:** 'Transactional outbox makes business state and delivery intent atomic; consumers still need idempotency.'
**Reference:** https://debezium.io/documentation/reference/stable/transformations/outbox-event-router.html

## Q4. Redis cache expire hote hi database par 20,000 requests (Hard)
**Maan lo** ek popular product ki Redis cache entry exactly 12 baje expire hui. Us waqt 20,000 users ne product open kiya. Sabko cache miss mila aur sab PostgreSQL ko query bhejne lage. DB connection pool exhaust ho gaya. Isko cache stampede ya thundering herd kehte hain.
**Pehla thought:** Har request ko database hit karne ki zarurat hi kya hai? Ek request data fetch kare, baaki same result ka wait karein. Isko single-flight/request coalescing bolte hain.
**Lekin ek aur twist:** Agar tumhare 20 app replicas hain toh ek JVM ka single-flight sirf usi replica ke callers ko coalesce karega. Cross-instance coordination ke liye distributed lease, bounded wait ya stale-while-revalidate policy chahiye. TTL jitter se sab keys ek saath expire nahi hongi.
**Dry run:** Same JVM par 100 callers → ek loader, 99 await. **Complexity:** expected O(1) map coordination; DB load main cost. **Correctness:** Stale inventory count ko actual stock reservation ka source of truth mat banao. **English answer:** 'I combine request coalescing, jitter, bounded waiting and carefully controlled stale reads to prevent stampedes.'

## Q5. PostgreSQL write skew: dono doctors off-duty kaise ho gaye? (Hard)
**Socho** hospital mein rule hai: kam se kam ek doctor on-call hona chahiye. Alice aur Bob dono on-call hain. Alice transaction check karti hai: Bob on-call hai, main off ho sakti hoon. Bob bhi same waqt check karta hai: Alice on-call hai, main off ho sakta hoon.
**Ab dikkat:** Dono alag rows update karte hain. PostgreSQL REPEATABLE READ snapshot isolation mein dono transactions commit ho sakti hain. Result: zero doctors on-call! Isko write skew bolte hain.
**Solution derive karo:** Problem ek row ka lost update nahi, multiple rows ka invariant hai. SERIALIZABLE isolation + SQLSTATE 40001 par poori transaction retry, ya shared guard-row lock use karo.
```sql
BEGIN ISOLATION LEVEL SERIALIZABLE;
SELECT COUNT(*) FROM doctors WHERE on_call=true;
-- count >= 2 ho tabhi current doctor off-duty karo
UPDATE doctors SET on_call=false WHERE id=:id;
COMMIT;
```
**Dry run:** Dono count=2 dekhte hain. SERIALIZABLE mein dangerous dependency detect karke ek transaction abort hogi; usko fresh snapshot par retry karna hoga. **Complexity:** query + update + possible retries. **Twist:** Sirf apni row SELECT FOR UPDATE karna cross-row invariant ko protect nahi karta. **English answer:** 'I enforce predicate-level invariants with serializable retries or a shared lock, not just row-level optimistic checks.'
**Reference:** https://www.postgresql.org/docs/current/transaction-iso.html

## Q6. Retry storm: 503 ko 243 requests mein badal diya! (Hard)
**Maan lo** user request API Gateway → Service A → B → C → D → payment dependency tak jaati hai. Har layer bolti hai: failure aaya toh 3 baar retry karo. Agar sab layers retry karein, deepest dependency ko ek request ke badle 3^5 = 243 attempts tak mil sakte hain.
**Ab problem kya hai?** Dependency already unhealthy hai. Tum usi par aur load daal rahe ho, recovery aur slow ho jayegi.
**Solution:** Retry ek carefully chosen boundary par rakho. Exponential backoff + random jitter, max attempts, overall request deadline, circuit breaker, bulkhead aur idempotency key use karo. 4xx validation errors retry mat karo; 429 Retry-After respect karo.
**Dry run:** 1→3→9→27→81→243. **Complexity:** bounded attempts. **Interviewer twist:** POST payment retry karoge toh double charge? Gateway idempotency key mandatory. **English answer:** 'I prevent retry amplification by centralizing retries, enforcing a deadline budget and isolating failing dependencies.'
**Reference:** https://aws.amazon.com/builders-library/timeouts-retries-and-backoff-with-jitter/

## Q7. DSA: Minimum Window Substring (Medium-Hard)
**Chalo example se samjho.** s = ADOBECODEBANC, t = ABC. Hume s ka sabse chhota continuous substring chahiye jisme A, B aur C sab present hon. Answer BANC.
**Brute force:** Har possible substring check karoge toh bahut slow. **Observation:** Window mein required characters milne tak right pointer badhao. Jaise hi sab mil gaye, left pointer aage badha kar window chhoti karo. Agar koi required char missing ho gaya, right phir badhao.
**Data structure:** Map mein har required character ka deficit aur ek missing count. Jab new character needed ho, missing decrease. Left se character remove karne par deficit positive hua toh missing increase.
```java
Map<Character,Integer> need = new HashMap<>();
for(char ch:t.toCharArray()) need.merge(ch,1,Integer::sum);
// right expand karo, missing==0 hone par left shrink karo.
// Har pointer at most n steps leta hai.
```
**Dry run:** ADOBEC → valid length 6; aage window shrink/expand hoti hai; BANC length 4 best. **Complexity:** O(|s|+|t|) time, O(unique chars) space. **Twist:** t mein duplicate letters; Unicode code points vs Java char. **English answer:** 'I use a sliding window with deficit counts and shrink greedily whenever the window is valid.'
**Complete Java implementation:** [English edition](../../../2026/10/Interview_Preparation_2026-10-16.md#q7-dsa-minimum-window-substring--medium-hard).

## Q8. DSA: Ship capacity binary search on answer (Medium-Hard)
**Socho** courier ke paas weights [1,2,3,1,1] hain. Packages ka order change nahi kar sakte. 4 din ke andar bhejna hai. Minimum ship capacity kitni hogi? Answer 3.
**Approach derive karo:** Capacity 2 possible hi nahi kyunki weight 3 ka package hai. Capacity 8 possible hai kyunki saare ek din mein jayenge. Toh answer max(weight) aur sum(weight) ke beech hai.
**Key observation:** Agar capacity 5 se shipping possible hai, toh capacity 6 se bhi possible hogi. Ye monotonic property hai. Isliye answer par binary search karo.
**Feasibility check:** Left se packages add karte jao. Agla package capacity exceed kare toh next day start. Total used days <= D toh capacity feasible.
**Dry run:** Capacity 3 par [1,2], [3], [1,1] = 3 days. **Complexity:** O(n log(sum−max+1)), O(1) space. **Twist:** Total weights int overflow kar sakte hain, long use karo; D >= n ho toh max weight answer. **English answer:** 'I binary-search the smallest feasible capacity because the greedy feasibility predicate is monotonic.'
**Complete Java implementation:** [English edition](../../../2026/10/Interview_Preparation_2026-10-16.md#q8-dsa-minimum-ship-capacity-in-d-days--medium-hard).

## Q9. AWS: Cross-account S3 read bina access key store kiye (Hard)
**Maan lo** Spring Boot service AWS Account A ke ECS mein run kar rahi hai. Files Account B ke S3 bucket mein hain. Kya application.properties mein AWS access key rakh doge? Bilkul nahi—rotation, leakage aur blast radius ka risk hai.
**Step-by-step:** ECS task role A ko sts:AssumeRole permission do. Account B mein dedicated read role banao jiska trust policy exact A principal allow kare. B role ko sirf required bucket prefix ka s3:GetObject do. Agar SSE-KMS encryption hai toh KMS key policy aur kms:Decrypt permission bhi check karo.
**Request flow:** ECS task identity → STS temporary credentials → Account B role → S3 allowed object. CloudTrail S3 data events enable karo where audit needed. Denied prefix test bhi karo; sirf success test enough nahi.
**Twist:** Bucket explicit Deny, VPC endpoint policy, KMS key policy, credential expiry. **English answer:** 'I use ECS task identity and scoped cross-account STS role assumption, with least-privilege S3 and KMS permissions.'
**Reference:** https://docs.aws.amazon.com/IAM/latest/UserGuide/tutorial_cross-account-with-roles.html

## HLD Classroom: Multi-tenant immutable audit-event platform
**Socho** tum 2,000 companies ke liye SaaS bana rahe ho. Har company ke login, role change, deletion events record hone chahiye. Company A ko kabhi B ke events nahi dikhne chahiye. Search fast ho aur compliance ke liye purane records safely retained rahen.
**Requirement se architecture kaise niklega?** Producers direct OpenSearch ko write karein toh index outage par events lose ho sakte hain. Isliye beech mein durable Kafka/Kinesis broker. Broker se ek sink immutable S3 archive banaye, doosra OpenSearch search index. Search index corrupt ho jaye toh archive se rebuild. Export heavy hai, toh async job queue.
```text
Producers -> Authenticated Ingest API -> Kafka/Kinesis
                                  |-> S3 immutable archive
                                  |-> OpenSearch read index
Query UI -> OIDC + tenant authZ -> Query API -> OpenSearch
Export API -> Job Queue -> Worker -> tenant-scoped export
```
**Sizing whiteboard:** Average 5,000 events/sec × 1KB × 86,400 sec ≈ 432GB/day raw. Peak 50k/sec ≈ 50MB/sec. Index replicas, broker replication aur object retention ka cost separately calculate karo.
**Tenant isolation:** TenantId request body se blindly trust mat karo. Authenticated identity se resolve karo. Query, export, S3 object paths aur signed cursors sab mein tenant scope enforce karo. Cross-tenant security tests mandatory.
**Failure scenario:** Index sink crash hua? Broker replay se reindex; eventId deterministic document ID rakho. Broker unavailable? 202 success mat bhejo. Archive sink slow? Lag alert karo; ingestion policy durable broker capacity se bounded hai.
**Trade-offs:** Kafka vs Kinesis; hot vs cold storage; shared index vs per-tenant index; eventTime vs ingestTime; legal hold vs deletion. **Interviewer follow-up:** Global ordering possible? Costly; per-stream order choose karo. Tamper-proof? Absolute guarantee nahi; signed manifests and protected trust anchors ke threat model ko state karo.
**English answer:** 'I use a durable event stream, immutable canonical archive, rebuildable search index and tenant authorization at every boundary.'
**Detailed architecture:** [HLD guide](../../../../system-design/daily/2026-10-16-hld-multitenant-audit.md).

## LLD Classroom: Thread-safe rolling-window circuit breaker
**Socho** payment dependency baar-baar fail ho rahi hai. Har request usi ko hit kare toh threads, connections aur latency waste honge. Circuit breaker ek safety switch hai jo repeated failures dekhkar temporarily calls reject karta hai.
**States ko relatable example se samjho:** CLOSED = normal traffic allowed. OPEN = dependency unhealthy, fail fast. HALF_OPEN = cooldown ke baad ek test request bhejo. Test success → CLOSED; fail → OPEN.
**Invariants:** HALF_OPEN mein ek hi probe admit hoga. CLOSED mein last N completed calls ka failure ratio threshold cross kare toh OPEN. OPEN cooldown ke pehle koi call nahi. Late completion purane CLOSED calls se state corrupt nahi honi chahiye.
**Class design:** RollingCircuitBreaker fields: State, ArrayDeque<Boolean> outcomes, windowSize, threshold, openedAtNanos, cooldownNanos, probeInFlight, ReentrantLock. execute(Callable<T>) pehle permission acquire kare, operation run kare, finally mein success/failure record kare.
**Concurrency twist:** 100 threads ek saath cooldown ke baad aaye. Lock ke andar OPEN→HALF_OPEN aur probeInFlight=true atomically set karoge toh exactly one probe allow hoga. Baaki CircuitOpenException paayenge.
**Dry run:** N=4, threshold=50%. Results Success,Fail,Success,Fail → 2/4 fail → OPEN. Cooldown ke baad one probe success → CLOSED and history reset. Probe fail → OPEN again.
**Complexity:** Naive deque scan O(N) per completion; running failure counter rakho toh O(1) updates, O(N) space. **Failure twist:** Probe hang ho gaya toh HALF_OPEN stuck; timeout/lease chahiye. **Testing:** 100 concurrent probes, clock injection, boundary ratio, late completion, classification of business 4xx.
**English answer:** 'I model explicit state transitions, serialize half-open probe admission and combine the breaker with timeouts, bulkheads and bounded retries.'
**Complete Java 17 LLD:** [Circuit breaker guide](../../../../system-design/daily/2026-10-16-lld-circuit-breaker.md).

## Engineering developments — verified through 16 October 2026
**15 Oct — GitHub AI Scan for PRs GA:** CodeQL gaps ko supplement karne ke liye scanning coverage expand hua. Automated result review still necessary. Source: https://github.blog/changelog/month/10-2026/
**15 Oct — GitHub agentic autofix GA:** Code scanning alerts ke proposed fixes ko review/tests ke saath adopt karo. Source: https://github.blog/changelog/month/10-2026/
**14 Oct — GitHub Actions lockfiles public preview:** Workflow action dependency commits lock karne ka option; plan eligibility docs se verify karo. Source: https://github.blog/changelog/2026-10-14-github-actions-workflow-lockfiles-in-public-preview/
**13 Oct — AWS Lambda Web Functions:** Standard Node.js web server on Lambda, concurrency model aur billing changes; Java runtime support assume mat karo. Source: https://aws.amazon.com/blogs/compute/
**21 Sep — Spring patch train announcement:** 22 Oct patches planned hain, abhi release nahi hue. Source: https://spring.io/blog/2026/09/21/releasing-spring-for-modern-challenges/

## 25-minute revision
5 min Kafka hot-key sketch; 5 min PostgreSQL write skew; 5 min HLD tenant isolation; 5 min circuit breaker state machine; 5 min DSA dry runs. Har topic ka English interview answer bina dekhe bolkar practice karo.
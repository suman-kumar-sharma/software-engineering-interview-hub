# Interview Preparation — 2026-10-14 | Proper Hinglish Classroom Edition

> **Aaj ka session:** Socho main tumhare saath whiteboard par baitha hoon. Har question mein hum pehle ek situation imagine karenge, phir dekhenge ki naive solution fail kyun hota hai, phir production-grade approach khud derive karenge. Java keywords/code English mein, explanation natural Roman-script Hinglish mein. **9 questions + 1 HLD + 1 LLD**; product-company SDE2/Senior level. Ye original practice questions hain, kisi named company ka verified interview transcript nahi. **Research cutoff:** 14 October 2026, IST.

**English companion:** [Interview_Preparation_2026-10-14.md](../../../2026/10/Interview_Preparation_2026-10-14.md). Dono ka filename same hai; directory alag hai. Official references neeche diye gaye hain. Private LinkedIn feed access ka claim nahi hai.

## Aaj ka learning roadmap

| Question | Concept | Pehle kya samjhoge? |
|---|---|---|
| Q1 | Bounded fan-out | Threads zyada = throughput zyada, ye hamesha sach kyun nahi? |
| Q2 | Kafka ordering | Failed event ke baad next event process karna dangerous kyun? |
| Q3 | Cache stampede | Ek key expire hone se DB kaise gir sakta hai? |
| Q4 | PostgreSQL write skew | Dono transactions individually correct, final state galat kaise? |
| Q5 | JWT tenant security | Valid token hone ke baad bhi data leak kaise? |
| Q6 | JVM heap | Heap badh raha hai toh leak kaise confirm karoge? |
| Q7 | Sliding window | O(n²) ko O(n) kaise banate hain? |
| Q8 | SQS/Lambda | 9 successful messages dubara kyun aate hain? |
| Q9 | Retry storms | Retry kab system ko aur damage karti hai? |
| HLD | Notification system | 5 crore notifications/day ko reliable kaise bhejoge? |
| LLD | Blocking queue | Lock aur condition se thread-safe queue kaise banegi? |

---

## Q1. 200 providers ko parallel call karna hai — Java concurrency aur backpressure (Hard)

### 1. Socho ek real-world scenario

Tum price-comparison application bana rahe ho. Ek user product search karta hai, aur tumhe 200 sellers se price mangwana hai. Har seller ka API 100–800 ms leta hai.

Pehla idea kya aayega? “200 threads bana do, sab parallel call kar lenge!”

**Ab problem kya hai?** Seller ki API ki capacity 20 requests/s hai. Tumne 200 concurrent calls bhej diye. Seller 429 dega, network sockets bhar jayenge, aur tumhare application ki memory bhi queue mein pending tasks ki wajah se grow karegi.

Yahan **concurrency** aur **capacity** alag cheezein hain. Java 21 virtual threads use karne se thread cost kam hoti hai, lekin seller ka quota aur DB connection pool unlimited nahi hota.

### 2. Chalo solution derive karte hain

Hume do limits chahiye: **execution limit** aur **admission limit**.

- Executor ke 8 workers honge: maximum 8 tasks actively execute karenge.
- Semaphore ke 16 permits honge: maximum 16 tasks submitted-but-incomplete honge.
- Permit **task submit karne se pehle** lena hai. Warna executor ki unbounded queue mein lakhon tasks aa sakte hain.
- Har external HTTP call par timeout/deadline hona chahiye. Warna stuck task permit kabhi release nahi karega.
- `finally` mein permit release karna compulsory hai, chahe task successful ho ya exception aaye.

### 3. Java code ko line-by-line samjho

```java
import java.util.*;
import java.util.concurrent.*;

public final class BoundedFanOut {
    record Item(int id) {}

    static List<Integer> run(List<Item> items)
            throws InterruptedException, ExecutionException {
        ExecutorService pool = Executors.newFixedThreadPool(8);
        Semaphore inFlight = new Semaphore(16);
        List<Future<Integer>> futures = new ArrayList<>();

        try {
            for (Item item : items) {
                inFlight.acquire(); // pehle capacity reserve
                try {
                    futures.add(pool.submit(() -> {
                        try {
                            return item.id() * 2; // demo: actual HTTP call nahi
                        } finally {
                            inFlight.release(); // failure mein bhi release
                        }
                    }));
                } catch (RejectedExecutionException ex) {
                    inFlight.release();
                    throw ex;
                }
            }

            List<Integer> answers = new ArrayList<>();
            for (Future<Integer> f : futures) answers.add(f.get());
            return answers;
        } finally {
            pool.shutdownNow();
        }
    }

    public static void main(String[] args) throws Exception {
        System.out.println(run(List.of(new Item(1), new Item(2))));
    }
}
```

**Dry run:** 2 items diye toh `[2,4]` print hoga. 200 items par at most 16 outstanding tasks honge. Jab 16 permits use ho gaye, producer `acquire()` par wait karega. Koi task complete hoga, permit release karega, tab next submit hoga.

**Complexity:** n tasks ke liye O(n) submission + collection, O(n) Futures store ho rahe hain. Bohot bada input ho toh futures ko batches mein consume karo, saare store mat karo.

### 4. Interviewer ka twist

“Agar 8 threads hain toh 16 permits kyun?” — Taaki 8 execute ho sakte hain aur up to 8 queued ho sakte hain, lekin queue unbounded nahi hogi. “Agar HTTP call hang ho jaye?” — Deadline, cancellation aur dependency-level circuit breaker. “Virtual threads laga dein?” — Haan, par semaphore phir bhi chahiye.

**English interview answer:** “I enforce admission control with a semaphore before task submission, limit active execution, and apply per-dependency deadlines and bulkheads.”

Reference: https://docs.oracle.com/en/java/javase/25/docs/api/java.base/java/util/concurrent/Semaphore.html

---

## Q2. Kafka mein ordering: CREATED, PAID, SHIPPED (Hard)

### 1. Situation imagine karo

Customer ne order O7 create kiya. Events aaye:

```text
O7 CREATED  (sequence 1)
O7 PAID     (sequence 2)
O7 SHIPPED  (sequence 3)
```

Agar SHIPPED pehle process ho gaya aur PAID baad mein, business state galat ho jayegi. Tum kahoge: “Kafka ordering guarantee karta hai.” **Lekin ek important condition hai:** ordering **ek partition ke andar** hoti hai, poore topic mein global ordering nahi.

### 2. Partition key ka decision

Producer har O7 event ko `key=orderId` ke saath publish kare. Normal stable partition mapping mein O7 ke saare events same partition par jayenge, isliye consumer unhe partition order mein dekhega.

Ab imagine karo PAID event malformed hai. Consumer fail hua. Tumne usko retry topic mein bhej diya aur SHIPPED process kar diya.

**Ab problem kya hai?** Retry topic ne ordering tod di! Kafka partition order mein messages aaye the, lekin business processing order mein nahi rahe.

### 3. Production mein options kaise choose karoge?

**Option A — Strict partition ordering:** PAID fail toh partition pause/block. Backoff ke saath bounded retry. Isse O7 safe rahega, lekin us partition ke doosre orders bhi delay honge.

**Option B — Per-order sequencing:** Database mein `expectedSequence` maintain karo. O7 ka seq2 pending hai toh seq3 durable buffer mein hold karo. O8 ke events continue kar sakte hain. Ye scale better karta hai, par gap detection, durable buffer aur cleanup complex ho jate hain.

**Option C — State machine:** `CREATED → PAID → SHIPPED` allowed transitions ko DB conditional update se enforce karo. Out-of-order event ko reject/reconcile karo.

### 4. Dry run aur failure cases

O7 seq1 complete. Seq2 fail. Seq3 arrive. Strict mode mein seq3 execute nahi hoga. Key-scoped mode mein seq3 buffer hoga. Agar seq2 kabhi recover nahi hua, manual reconciliation ya timeout policy chahiye. DLQ mein bhejna payment complete hone ke barabar **nahi** hai.

**Twist:** Consumer rebalance ke time in-flight processing ka kya? Commit sirf durable state change ke baad. Duplicate event ID dedupe karo. Partition count change hone par mapping change ho sakti hai; migration plan karo.

**Complexity:** indexed order-state lookup roughly O(1) expected, but database I/O dominant. Hot order key inherently sequential rahega.

**English interview answer:** “I key records by order ID for partition ordering and design retries so failed events cannot let later transitions violate the order state machine.”

Reference: https://kafka.apache.org/documentation/

---

## Q3. Cache stampede: ek key expire hui aur DB par 5000 requests (Hard)

### 1. Chalo ek flash sale ka example lete hain

Tumhare e-commerce app mein ek popular phone ki price Redis mein cached hai. TTL 5 minutes hai. Exactly 5 minutes baad key expire hui. Usi second 5,000 customers ne price request kar di.

Normal code:

```text
cache.get(price) -> MISS
database.query(price)
cache.set(price)
```

Sabko miss mila. Sab database query karne lage. **Ek cache key ki expiry ne database overload kar diya.** Isko cache stampede bolte hain.

### 2. Hum kaise solve karenge?

**Pehla layer — Single-flight:** Ek application instance par same key ke liye ek hi loader DB jayega. Baaki requests usi `CompletableFuture` ka result wait karengi. `ConcurrentHashMap<Key, CompletableFuture<Value>>` se local coalescing design kar sakte ho.

**Doosra layer — Distributed coordination:** 10 application instances hain toh local single-flight ke baad bhi approx 10 DB loads aa sakte hain. Bahut expensive key ke liye short distributed lease helpful ho sakti hai. Lekin lease expiry aur crash ko handle karo; lease ko financial correctness lock mat samjho.

**Teesra layer — Stale-while-revalidate:** Kuch use cases mein 10-second purani product description dikha sakte ho aur background mein refresh kar sakte ho. **Lekin account balance ya final checkout amount stale mat maan lena.**

**Chautha layer — TTL jitter:** Sab keys ek saath expire na hon, isliye random TTL variation. Origin bulkhead bhi rakho.

### 3. Example se impact samjho

5,000 requests, 10 app instances. Local single-flight sahi kaam kare toh DB requests approx 5,000 se 10 tak gir sakti hain—ye illustrative estimate hai, guaranteed number nahi.

**Twist:** Loader fail ho gaya toh completed exceptional future map mein forever nahi rehna chahiye. Remove/retry policy chahiye. Price update ke baad stale refresh nayi value overwrite na kare; version compare ya fencing use karo.

**English interview answer:** “I combine request coalescing, TTL jitter, origin concurrency limits and carefully scoped stale-while-revalidate to protect the database.”

---

## Q4. PostgreSQL write skew: do doctors aur zero on-call staff (Hard)

### 1. Pehle story samjho

Hospital mein Alice aur Bob dono on-call hain. Rule: **kam se kam ek doctor on-call rehna chahiye**.

Alice system mein check karti hai: “Bob on-call hai? Haan. Main off ho sakti hoon.”

Same time Bob check karta hai: “Alice on-call hai? Haan. Main bhi off ho sakta hoon.”

Dono transactions individually logical lag rahe hain. Lekin final state: Alice OFF, Bob OFF. Rule break!

### 2. Aisa kaise hua?

Dono transactions ne initial snapshot dekha. Alice ki transaction Alice row update karti hai; Bob ki transaction Bob row. **Different rows update ho rahi hain**, toh ordinary lost-update conflict detect nahi hota.

PostgreSQL `REPEATABLE READ` snapshot isolation mein write skew possible hai. Isliye “repeatable read hai toh sab safe” kehna galat hai.

### 3. Do solutions derive karte hain

**Approach 1 — SERIALIZABLE isolation.** Database dangerous dependency detect karke ek transaction abort kar sakta hai. App ko SQLSTATE `40001` par **poori transaction** retry karni hogi.

```sql
BEGIN TRANSACTION ISOLATION LEVEL SERIALIZABLE;
SELECT count(*) FROM on_call WHERE shift_id = 7 AND active;
-- count > 1 ho toh current doctor ko off mark karo
UPDATE on_call
SET active = false
WHERE shift_id = 7 AND doctor_id = 42;
COMMIT;
```

**Approach 2 — Shared row lock.** Shift/schedule ki common row `SELECT ... FOR UPDATE` se lock karo. Har doctor update isi protocol ko follow kare. Ek waqt mein ek hi transaction invariant check karegi.

### 4. Interviewer ka twist

“Java synchronized laga do?” — Multiple Spring Boot replicas ka shared JVM nahi hai. “Serializable se performance issue?” — Conflict par retries, monitoring aur short transactions chahiye. “Test kaise?” — Barrier laga kar dono transactions ko same initial state read karwao, phir assert karo ki at least ek doctor ON bachta hai.

**English interview answer:** “Write skew breaks cross-row invariants without a direct write conflict. I use serializable isolation with whole-transaction retries or a shared database lock.”

Reference: https://www.postgresql.org/docs/current/transaction-iso.html

---

## Q5. Valid JWT hai, phir bhi tenant B ka invoice leak kaise? (Hard)

### 1. Situation

Tum multi-tenant SaaS bana rahe ho. Tenant A ki user Alice login karti hai. JWT valid hai. Alice URL change karti hai:

`GET /tenants/B/invoices/123`

Agar backend sirf JWT signature check karke invoice ID 123 fetch kar de, toh Alice ko tenant B ka data mil sakta hai.

**Ab difference samjho:** Authentication poochta hai “Tum kaun ho?” Authorization poochta hai “Tumhe ye specific invoice dekhne ki permission hai?”

### 2. Step-by-step secure flow

1. Spring Security token ki signature, issuer, audience, expiry validate kare.
2. Trusted token claims ya server-side membership se Alice ke allowed tenants resolve karo.
3. Requested tenant B ke liye membership aur role check karo.
4. DB query mein **tenant filter mandatory**: `WHERE tenant_id = authorizedTenant AND id = invoiceId`.
5. Audit mein access denial record karo, par raw token/PII log mat karo.

```java
@Query("select i from Invoice i " +
       "where i.id = :id and i.tenantId = :tenantId")
Optional<Invoice> findAuthorized(
        @Param("id") UUID id,
        @Param("tenantId") UUID tenantId);
```

**Important:** `tenantId` ko blindly request header se pass mat karna. Pehle trusted identity ke against authorize karo.

### 3. Follow-up kaise handle karoge?

“Alice do tenants ki member hai?” — Explicit active tenant selection aur membership verification. “JWT revoke karna hai?” — Short TTL, revocation/session strategy, membership change invalidation. “Cache mein tenant ID nahi?” — Cross-tenant cache leakage! Cache keys tenant-scoped rakho. Database RLS defense-in-depth ho sakta hai.

**English interview answer:** “A valid JWT authenticates the caller but does not authorize cross-tenant resource access. I enforce membership and tenant-qualified queries on every request.”

Reference: https://docs.spring.io/spring-security/reference/servlet/oauth2/resource-server/jwt.html

---

## Q6. JVM heap grow ho raha hai, CPU normal hai — debugging (Medium–Hard)

### 1. Socho production incident hua

Kal deploy kiya. Aaj heap 2 GB se 6 GB tak badh raha hai. CPU 35% hai. Team bolti hai “Memory leak!” Tum turant `-Xmx` badha doge?

Nahi. **Pehle confirm karo ki live objects retain ho rahe hain ya bas allocation traffic badha hai.**

### 2. Investigation ka logical sequence

**Step 1:** GC ke baad used heap dekho. Agar har full GC ke baad baseline badhta ja raha hai, retention ka strong signal hai.

**Step 2:** Allocation rate, old-gen occupancy, GC pauses, thread count, direct memory aur cache size compare karo. RSS badh raha ho aur heap stable ho toh off-heap/native memory bhi check karo.

**Step 3:** Safe window mein heap dump lo; dominator tree aur GC roots inspect karo. Heap dump sensitive data contain kar sakta hai, isliye access control zaroori.

**Step 4:** Suspect code dekho: unbounded map, `ThreadLocal`, static collection, never-closed listener, queue backlog, high-cardinality metrics.

### 3. Ek practical twist

New metrics code mein `userId` ko label bana diya. Har naye user par ek nayi metric series store ho rahi hai. Memory badhti ja rahi hai. Heap badhana symptom postpone karega, root cause nahi fix karega.

**Production action:** SLO risk high ho toh rollback. Evidence preserve karo, fix ko realistic sustained load par validate karo.

**English interview answer:** “I separate allocation from retained live-set growth, inspect GC and heap dominators, then validate the suspected retention path under load.”

---

## Q7. Longest substring with at most K distinct characters (Medium — DSA)

### 1. Pehle example

String `eceba`, `k=2`. Hume longest continuous substring chahiye jisme maximum 2 distinct characters hon.

`ece` mein e aur c hain—sirf 2 types, length 3. `eceb` mein e,c,b ho gaye—3 types, invalid.

### 2. Brute force se optimized approach kaise aaya?

Brute force mein har starting point se har ending point tak substring check karoge: O(n²) ya aur zyada depending distinct-count implementation.

**Observation:** Window mein jab distinct count `k` se zyada ho jata hai, toh left boundary aage badhaye bina valid window nahi milegi. Isi se sliding window derive hoti hai.

- `right` ko aage badhao aur character frequency increment karo.
- Jab map mein distinct keys `k` se zyada ho, `left` ko aage badhao, frequency decrement/remove karo.
- Valid window ke baad `best` update karo.

### 3. Complete Java 17 code

```java
import java.util.*;

public final class AtMostKDistinct {
    static int longest(String s, int k) {
        if (s == null || k <= 0) return 0;

        Map<Character, Integer> count = new HashMap<>();
        int left = 0, best = 0;

        for (int right = 0; right < s.length(); right++) {
            char added = s.charAt(right);
            count.merge(added, 1, Integer::sum);

            while (count.size() > k) {
                char removed = s.charAt(left++);
                int next = count.get(removed) - 1;
                if (next == 0) count.remove(removed);
                else count.put(removed, next);
            }
            best = Math.max(best, right - left + 1);
        }
        return best;
    }

    public static void main(String[] args) {
        System.out.println(longest("eceba", 2)); // 3
    }
}
```

### 4. Dry run

`e`: map {e:1}, best 1. `ec`: {e:1,c:1}, best 2. `ece`: {e:2,c:1}, best 3. `eceb`: 3 distinct, left se e remove, phir c remove, valid `eb` bachta hai. End par answer 3.

**Complexity:** `left` aur `right` dono max n steps move karte hain. Isliye **O(n)** time, O(min(k, alphabet size)) frequency map space.

**Twist:** Java `char` UTF-16 code unit hai; Unicode emoji/code point requirement ho toh code-point-based approach use karo. “Exactly K distinct substrings count?” — `atMost(k) - atMost(k-1)` se count mil sakta hai, longest length directly nahi.

**English interview answer:** “I keep a frequency map for a sliding window, shrink whenever the distinct count exceeds K, and obtain linear time because each pointer advances at most n times.”

---

## Q8. AWS Lambda + SQS: 10 messages mein ek fail, 9 dubara kyun? (Hard)

### 1. Story se samjho

SQS queue se Lambda ko 10 messages ka batch mila. 9 messages process ho gaye. Tenth message par exception aayi. Tumne socha sirf tenth retry hoga.

**Lekin default behavior mein poora batch failure ke baad visible ho sakta hai.** Matlab 9 successful messages bhi dobara aa sakte hain. Payment ya order processing ho toh duplicate side effects ka risk.

### 2. Correct solution

Event source mapping mein `ReportBatchItemFailures` enable karo. Handler har message individually process kare, failed message IDs collect kare aur response mein sirf failed IDs return kare.

```json
{
  "batchItemFailures": [
    { "itemIdentifier": "failed-sqs-message-id" }
  ]
}
```

**Ab kya hoga?** Lambda successful messages ko unnecessary retry nahi karega, aur failed message visibility timeout ke baad retry ho sakta hai.

**Lekin idempotency phir bhi chahiye.** Lambda crash ho sakti hai, response lost ho sakta hai, duplicate deliveries possible hain.

### 3. Configuration aur tricky follow-ups

Visibility timeout function timeout se bada hona chahiye. DLQ + redrive policy rakho. `ApproximateAgeOfOldestMessage` aur DLQ depth monitor karo.

**FIFO queue ka twist:** Ordering preserve karne ke liye first failure ke baad processing stop karo aur failed **plus unprocessed** message IDs return karo. Warna later message process karke sequence violate kar sakte ho.

**English interview answer:** “I enable partial batch responses, return failed item identifiers, and keep handlers idempotent because SQS/Lambda processing is at least once.”

References: https://docs.aws.amazon.com/lambda/latest/dg/services-sqs-errorhandling.html and https://docs.aws.amazon.com/lambda/latest/dg/with-sqs.html

---

## Q9. Retry storm: recovery ki jagah outage aur badh gayi (Hard)

### 1. Socho payment dependency slow ho gayi

Payment provider 10% calls par timeout de raha hai. Tumhari service retry 3 baar karti hai. Uske upar order service bhi retry 3 baar. API gateway bhi 3 attempts. Ab provider par original traffic se bahut zyada calls ja rahi hain.

**Problem:** Jo service already slow hai, usi par hum extra load bhej rahe hain. Queue badhegi, latency aur badhegi, aur retries aur trigger hongi—negative feedback loop.

Agar 4 layers har ek 3 attempts kar sakti hain, illustrative worst-case path mein **3^4 = 81** attempts ho sakte hain. Real count topology par depend karega.

### 2. Ab resilient design kaise sochoge?

**Ek end-to-end deadline** define karo. Example: request ke paas total 800 ms hai. Har layer independently 2 seconds wait nahi kar sakti.

**Retry policy:** Sirf transient failures retry karo; idempotent operations ya idempotency-key-protected writes. Backoff exponential + jitter rakho taaki sab clients ek hi millisecond mein retry na karein.

**Circuit breaker:** Dependency unhealthy ho toh repeated attempts rokne mein help karta hai. **Bulkhead:** Concurrent calls limit karta hai. **Load shedding:** Capacity khatam ho toh jaldi 429/503 do, poore app ko block mat karo.

### 3. Interviewer ka tricky question

“Payment gateway timeout hua—dobara charge kar doge?” — Nahi, outcome unknown ho sakta hai. Idempotency key aur gateway status reconciliation chahiye.

“Circuit breaker aur retry mein difference?” — Retry transient failure ko recover karne ki koshish karta hai; circuit breaker repeated failing calls ko temporarily short-circuit karta hai.

**English interview answer:** “I control retry amplification with one deadline and retry budget, jittered backoff, idempotency, circuit breakers and per-dependency concurrency limits.”

Reference: https://resilience4j.readme.io/docs/retry

---

# HLD — Multi-tenant Notification Platform (45-minute product-company interview)

## Step 1. Interviewer se requirements kaise clarify karoge?

Socho tumhe WhatsApp/email/SMS jaisa notification infrastructure design karna hai, jo alag-alag businesses ko serve kare. E-commerce tenant order updates bhejega, bank transaction alerts bhejega, school fee reminders bhejega.

**Sabse pehla sawal:** Kya notification instantly recipient tak pahunchne ki guarantee chahiye, ya system ka request accept karna durable hona chahiye? Ye dono alag hain.

Functional requirements: templates, transactional/bulk sends, email/SMS/push, preferences, opt-out, retries, delivery status, provider webhooks. Nonfunctional: tenant isolation, quotas, cost control, observability, security, audit trail.

**Sizing assumptions (interview exercise, real measurements nahi):** 5 crore notifications/day, peak 20x, 1 lakh tenants, 30 days hot retention.

Average QPS = 50,000,000 / 86,400 ≈ **579/s**. Peak ~**11,600/s**. 30 days mein 1.5 billion notification records. 500 bytes/record assumption par ~750 GB raw; indexes, replicas, retries aur audit isse aur badhayenge.

## Step 2. Naive solution kyun fail hoga?

`POST /send` aaya, controller ne directly email provider ko HTTP call kar di. Provider 10 seconds slow ho gaya toh API threads block. Provider timeout hua toh client retry karega, duplicate send possible. DB save successful, event publish fail—notification permanently lost.

**Ab hum API acceptance aur actual delivery ko separate karenge.**

## Step 3. Architecture whiteboard par draw karo

```text
Client / SDK
    |
API Gateway (JWT, tenant authorization, quota)
    |
Notification API
    |---- PostgreSQL: notification + outbox [same transaction]
    |                          |
    |                     Outbox relay
    |                          v
    |                     Kafka / SQS
    |                          |
    |                 Routing / retry workers
    |                    /     |      \
    |                 Email   SMS     Push adapters
    |                    \     |      /
    |                   External providers
    |                          |
    |                  Signed webhooks
    |                          |
    +---- Status API <--- Delivery state store
```

**Har component kyun?**

- **API Gateway:** authentication, per-tenant rate limits, request size limits.
- **Notification API:** validate template, consent, tenant scope; request ko durable accept kare.
- **PostgreSQL:** source of truth for notification intent and idempotency.
- **Transactional outbox:** DB commit ke baad Kafka publish fail ho jaye toh notification lose na ho. Same DB transaction mein notification aur outbox insert karo.
- **Queue:** burst ko absorb kare, workers ko independently scale kare.
- **Adapters:** Email/SMS/Push provider ke alag API contracts aur rate limits isolate kare.
- **Webhook consumer:** provider ka delivered/bounced status verify karke update kare.

## Step 4. API aur data model design

```http
POST /v1/notifications
Authorization: Bearer <token>
Idempotency-Key: tenant-unique-request-42

{
  "templateId": "order-shipped",
  "recipientId": "user-42",
  "channels": ["email", "push"],
  "variables": {"orderId": "O-9"}
}
```

Response `202 Accepted` + `notificationId`. **Important:** 202 ka matlab user ko email mil gaya nahi; sirf request durably accept hui.

```text
Tenant(tenantId, plan, quota, status)
Template(tenantId, templateId, version, channel, bodyRef)
Notification(tenantId, notificationId, idempotencyKey,
             recipientId, templateVersion, status, createdAt)
DeliveryAttempt(tenantId, notificationId, channel,
                attemptNo, providerId, status, nextAttemptAt)
Outbox(eventId, aggregateId, payload, publishedAt)
Preference(tenantId, recipientId, channel, optIn)
UNIQUE(tenantId, idempotencyKey)
```

**Tenant ID har jagah kyun?** Agar cache key sirf `notificationId` hui ya query mein tenant filter missing hua toh cross-tenant data leak ho sakta hai.

## Step 5. State machine ko dhyan se samjho

```text
ACCEPTED -> QUEUED -> SENDING -> SENT_TO_PROVIDER
                              |              |
                              v              v
                          RETRY_WAIT    DELIVERED / BOUNCED
                              |
                            FAILED / DLQ
```

Provider ne HTTP 200 diya toh usually “provider ne accept kiya” samjho; recipient ne actual message receive kiya, ye alag status hai.

**Tricky race:** Provider webhook `DELIVERED` pehle aa gaya, HTTP response baad mein. Agar simple last-write-wins use kiya toh state DELIVERED se SENT_TO_PROVIDER regress ho sakti hai. Monotonic state transitions, event timestamps/versions aur reconciliation chahiye.

## Step 6. Scale aur noisy neighbor problem

Maan lo tenant A ne ek saath 10 lakh promotional SMS submit kar diye. Tenant B ke critical bank alerts delay nahi hone chahiye.

**Solution:** Per-tenant quota, weighted fair scheduling, channel-specific worker pools, provider-specific token buckets/bulkheads. Kafka partition key tenant+recipient ya notification ID choose karo depending ordering need; SQS bhi independent deliveries ke liye reasonable option hai.

**Retries:** transient 429/5xx par exponential backoff+jitter; permanent invalid address ko endlessly retry mat karo. Poison messages DLQ mein, replay with audit.

## Step 7. Failure injection ke saath design verify karo

- **DB commit ke baad API crash:** Outbox relay event publish karega.
- **Kafka same event dobara de:** Idempotency key se duplicate business effect roko.
- **Provider ne send kiya par timeout aaya:** Provider idempotency ya status lookup; blindly resend mat karo.
- **Provider outage:** Circuit breaker + queue buffering + alternate provider policy.
- **Webhook spoofed:** Signature, timestamp/replay window verify.
- **Region down:** RPO/RTO clarify; failover mein duplicates possible, global IDs/reconciliation zaroori.

**Metrics:** outbox lag, queue age p99, tenant quota rejections, provider failure, time-to-delivery, retry count, DLQ depth, cost per tenant. **Privacy:** encrypted recipient info, retention limits, no tokens/PII in logs.

## Step 8. Interview mein final 60-second explanation

> “I durably accept notification intent using a transactional outbox, decouple delivery with a queue, and isolate tenants and providers using quotas and bulkheads. Delivery is at least once internally; I handle duplicates and uncertain provider outcomes with idempotency, state machines and reconciliation.”

**Follow-ups:** Kafka vs SQS? Tenant ordering vs throughput? How to guarantee unsubscribe race? How to handle a provider webhook received before request acknowledgment? What is the cost model at 50M/day?

---

# LLD — Thread-safe Bounded Blocking Queue (45-minute Java interview)

## Step 1. Problem ko simple words mein samjho

Maan lo ek factory mein 2-slot ka tray hai. Producer items tray mein rakhta hai; consumer nikalta hai.

- Tray full ho toh producer wait kare.
- Tray empty ho toh consumer wait kare.
- Factory band ho rahi hai toh waiting threads ko pata chalna chahiye.
- Kisi bhi time tray mein capacity se zyada items nahi hone chahiye.

Java mein isko **bounded blocking queue** bolenge.

**Contract:** `put(T)` full par block kare, `take()` empty par block kare, `close()` new puts reject kare aur blocked threads wake kare. Close ke baad existing items drain honge; drain complete hone par `take()` null return karega. Null items allowed nahi hain.

## Step 2. Invariants pehle likho

1. `0 <= size <= capacity`.
2. Ek item ko ek hi consumer remove kare.
3. Wait karte waqt lock hold karke dusre thread ko block nahi karna.
4. Wake-up ke baad condition dobara check karni hai.
5. Close par waiting producer aur consumer dono wake hon.

## Step 3. Kaunsi classes choose karoge aur kyun?

`ArrayDeque<T>` internal storage. `ReentrantLock` se mutual exclusion. Do `Condition`: `notFull` producer ke liye aur `notEmpty` consumer ke liye.

**Ek condition kyun nahi?** Alag wait sets hone se targeted signal de sakte ho. Consumer item nikaale toh `notFull.signal()`; producer item add kare toh `notEmpty.signal()`.

**While loop kyun?** Thread wake hone ka matlab condition true hai, ye guarantee nahi. Spurious wakeups aur competing threads ke wajah se `if` galat ho sakta hai.

## Step 4. Complete runnable Java 17 code

```java
import java.util.*;
import java.util.concurrent.locks.*;

public final class BoundedBlockingQueue<T> {
    private final ArrayDeque<T> deque = new ArrayDeque<>();
    private final int capacity;
    private final ReentrantLock lock = new ReentrantLock();
    private final Condition notEmpty = lock.newCondition();
    private final Condition notFull = lock.newCondition();
    private boolean closed;

    public BoundedBlockingQueue(int capacity) {
        if (capacity <= 0) throw new IllegalArgumentException("capacity");
        this.capacity = capacity;
    }

    public void put(T item) throws InterruptedException {
        Objects.requireNonNull(item);
        lock.lockInterruptibly();
        try {
            while (deque.size() == capacity && !closed) {
                notFull.await();
            }
            if (closed) throw new IllegalStateException("queue closed");
            deque.addLast(item);
            notEmpty.signal();
        } finally {
            lock.unlock();
        }
    }

    // close + drain ke baad null: end of stream
    public T take() throws InterruptedException {
        lock.lockInterruptibly();
        try {
            while (deque.isEmpty() && !closed) {
                notEmpty.await();
            }
            if (deque.isEmpty()) return null;
            T item = deque.removeFirst();
            notFull.signal();
            return item;
        } finally {
            lock.unlock();
        }
    }

    public int size() {
        lock.lock();
        try { return deque.size(); }
        finally { lock.unlock(); }
    }

    public void close() {
        lock.lock();
        try {
            closed = true;
            notEmpty.signalAll();
            notFull.signalAll();
        } finally {
            lock.unlock();
        }
    }

    public static void main(String[] args) throws Exception {
        BoundedBlockingQueue<Integer> queue = new BoundedBlockingQueue<>(2);
        Thread producer = new Thread(() -> {
            try {
                for (int i = 1; i <= 3; i++) queue.put(i);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } finally {
                queue.close();
            }
        });
        producer.start();
        Integer item;
        while ((item = queue.take()) != null) System.out.println(item);
        producer.join();
    }
}
```

### Code walkthrough — actual threads mein kya hoga?

Capacity=2. Producer 1 put karega, phir 2. Ab tray full. Agar consumer slow hai toh producer 3 put karne se pehle `notFull.await()` par rukega.

**`await()` kya karta hai?** Lock ko atomically release karta hai aur thread ko wait karwata hai. Isliye consumer lock acquire karke item remove kar sakta hai.

Consumer 1 nikalta hai aur `notFull.signal()` karta hai. Producer wake hota hai, lock dobara leta hai, condition check karta hai, phir 3 add karta hai.

Producer complete hone par `close()` karta hai. Consumer remaining items drain karega; queue empty aur closed hone par `take()` null return karega. **Output:** 1, 2, 3 alag lines par.

### Complexity aur concurrency correctness

`put` aur `take` ka queue work O(1), waiting time exclude karke. Space O(capacity). Har queue mutation same lock ke andar hoti hai, isliye lost update nahi. `signalAll` close par waiting threads ko permanently stuck rehne se bachata hai.

### Tests jo interviewer ko impress karenge

- Capacity 1: second producer block hona chahiye, consumer remove kare toh unblock.
- Empty queue: consumer block; producer put kare toh unblock.
- Close: waiting producer exception dekhe, waiting consumer null dekhe.
- Close ke time items present: pehle drain, phir null.
- Interrupt: waiting thread safely interrupt ho aur lock leak na ho.
- 10 producers + 10 consumers: unique IDs count exactly N, no duplicates/missing.
- Invalid capacity 0 aur null item reject.

**Twist:** `ArrayBlockingQueue` already JDK mein hai. Interview ke liye implementation likhna useful hai, production mein proven library prefer karoge. `ReentrantLock(true)` fairness improve kar sakta hai par throughput trade-off. Ye in-memory queue hai—Kafka/SQS ki tarah durable nahi.

**English interview answer:** “I protect the capacity invariant with one lock, use separate conditions for producers and consumers, loop on wait predicates, and wake all blocked threads on close.”

---

# Engineering updates — 14 October 2026 tak verified

## 1. AWS Lambda Web Functions — 12–13 October 2026

**Kya naya hai?** AWS ne 12 October ko Web Functions announce kiye, aur 13 October ko technical walkthrough publish kiya. Ab standard **Node.js HTTP server** Lambda Web Functions mein adapter ke bina run ho sakta hai. Launch walkthrough mein Node.js 24, Arm64, 2 vCPUs aur 2 GB per environment specify hai.

**Hume kya samajhna hai?** Traditional Lambda event handler aur long-lived HTTP server model alag hain. Multiple requests same execution environment handle kar sakti hain. Authentication, CloudFront, IAM aur cost model compare karna hoga.

**Important caution:** Is announcement ko “Java Spring Boot Web Functions officially supported” mat samajhna—launch walkthrough specifically Node.js ke liye hai.

Official: https://aws.amazon.com/blogs/compute/exploring-aws-lambda-web-functions-for-building-web-applications/

## 2. GitHub Apps least-privilege permissions — 13 October 2026

**Kya change hua?** GitHub ne `repository_creation:write` aur `installation_repositories:write` jaise finer-grained permissions introduce kiye.

**Real-world fayda:** CI/CD integration ko repository create karna hai, toh unnecessary broad administration permission kyun dena? Least privilege se blast radius kam hota hai. Token rotation, app installations aur audit logs review karo.

Official: https://github.blog/changelog/2026-10-13-github-app-improvements-for-least-privilege-and-management/

## 3. AWS Prime Day retrospective — 13 October 2026

AWS ke official roundup ke mutabik Prime Day 2026 ke dauran DynamoDB ne 59 trillion se zyada requests handle ki, peak 192 million requests/s tha; Lambda ne 2.3 trillion se zyada daily invocations handle ki. **Ye AWS-reported aggregate scale numbers hain**, tumhare single application ki benchmark guarantee nahi.

**Architecture lesson:** Hot partitions, quotas, retry amplification, load shedding aur operational observability pehle se design karne padte hain.

Official: https://aws.amazon.com/blogs/aws/aws-weekly-roundup-prime-day-2026-by-the-numbers-claude-haiku-5-5-amazon-nova-2-5-sonic-and-more-october-13-2026/

## 4. Spring monthly patch train — 21 September announcement, 22 October planned

Spring ne synchronized monthly patch cadence explain ki hai. Next regular patch train **22 October 2026 ke liye planned** hai; aaj usko already released mat maan lena.

**Action:** Apni Spring Boot/Security versions, BOM aur dependency scan review karo. Actual release notes aane par affected CVEs aur regression tests evaluate karo.

Official: https://spring.io/blog/2026/09/21/releasing-spring-for-modern-challenges/

---

## 20-minute revision — kaise practice karna hai

**Minute 0–4:** Q1 mein batao thread limit aur admission limit mein kya difference hai. **4–8:** Q2 mein Kafka retry se ordering kaise toot sakti hai, example ke saath bolo. **8–12:** Q4 ka Alice/Bob write skew whiteboard par draw karo. **12–16:** Q7 ka `eceba` dry run aur O(n) proof. **16–20:** HLD outbox aur LLD `await()/signal()` explain karo.

**Golden rule:** Interviewer ko sirf answer nahi, **“maine ye design decision kyun liya?”** bhi samjhao. Agar tum reason, trade-off aur failure case clearly bol sakte ho, toh concept genuinely samajh aa gaya.

## Verification and attribution

Sabhi questions original interview-style exercises hain; kisi company ya private LinkedIn post ko falsely attribute nahi kiya gaya. News claims dated official sources se liye gaye hain. System sizes hypothetical assumptions hain. Java 17+ ke standalone examples matching filename ke saath compile karne ke liye likhe gaye hain; is report mein actual runtime test execute hone ka claim nahi hai.

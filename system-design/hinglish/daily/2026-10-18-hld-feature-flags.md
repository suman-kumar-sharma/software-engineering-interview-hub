# HLD Classroom: Multi-Region Feature Flag Platform — Proper Hinglish
**18 October 2026 · 45-minute senior backend interview**

## 1. Socho ek real company ka scenario
Maan lo tumhari company ka shopping app 10 countries mein chal raha hai. Team ne naya checkout design banaya. Agar sab users ko ek saath dikha diya aur bug nikla, revenue impact ho sakta hai. Isliye product manager bolta hai: "Pehle 5% users ko dikhao, phir 20%, aur zarurat pade toh turant band karo." Yahi feature flag system ka use case hai.

**Ab problem kya hai?** Har request par central service ko call karoge toh checkout ka latency badhega aur feature-flag service down hote hi checkout bhi down. Senior engineer ka pehla design decision: **flag evaluate application ke andar locally karo**, network se sirf configuration updates lao.

Interviewer ko pehle assumptions bolo: 20k tenants, har tenant ~200 flags, 200k SDK instances, peak 2 million evaluations/sec. Ye hypothetical interview numbers hain. Writes bahut kam; reads bahut zyada. Target local evaluation p99 under 1ms aur healthy network mein propagation p99 under 10 seconds. Ye targets hain, guaranteed observed metrics nahi.

## 2. Requirements ko whiteboard par todte hain
Functional: flag create/update, percentage rollout, user segment, environment, audit history, rollback, emergency disable. Non-functional: low latency, high availability, tenant isolation, safe offline behavior, observability.

**Interviewer twist:** "Agar internet chala gaya toh flag ka kya hoga?" SDK last-known-good configuration use karega. Har flag ke liye default define karenge. Sensitive security authorization ko feature flag ke bharose nahi chhodenge.

## 3. Architecture kaise derive hua?

```text
Product manager -> Admin UI -> RBAC/API -> PostgreSQL + outbox
                                          |
                                     Event stream
                                          |
                                Regional distributors
                                          |
                                  Signed snapshots
                                          |
                            Java SDK local in-memory rules
                                          |
                          request ke time local evaluation
```

Is diagram mein do alag duniya hain. **Control plane** mein admin config change karta hai. **Data plane** mein actual user request flag evaluate karti hai. Control plane slow ya temporarily unavailable ho sakta hai, lekin data plane ko chalna chahiye.

**Outbox kyun?** Socho DB update success hua lekin event publish karne se pehle app crash. Event missing hua toh SDK purana rule use karega. Same DB transaction mein outbox row insert karo, worker event later publish karega. Event duplicate aaye toh revision number se ignore kar do.

**Immutable snapshot kyun?** Agar rule list update hote waqt user request aa gayi toh half-old, half-new rules ka bug. Naya snapshot complete build karo, signature/checksum validate karo, phir atomic reference swap karo.

## 4. Percentage rollout step by step
Maan lo flag 'new-checkout' ko 10% users par enable karna hai. Agar `Math.random()` har request par use kiya, toh ek hi user ko kabhi old checkout aur kabhi new checkout milega. Bad UX!

Isliye stable `userId`, `tenantId`, `flagKey` aur salt ko hash karo. Hash ko 10,000 buckets mein map karo. Bucket 0–999 enabled; baaki disabled. Same input = same bucket, chahe request kisi bhi server par jaye.

**Twist:** Java aur JavaScript SDK ke hash results different hue toh same user ko different variant. Isliye canonical algorithm aur cross-language golden test vectors zaroori hain. Email hash kar dena automatically privacy-safe nahi hota; opaque user ID prefer karo.

## 5. Database aur APIs ko samjho
Flag table ka logical key `(tenantId, environment, flagKey)`; saath `revision`, `rules`, `default`, `updatedAt`. Audit table mein kisne kab change kiya. Outbox same transaction mein insert.

`PUT /flags/{key}` with `If-Match: revision`: do admins ek saath edit kar rahe hain toh lost update prevent hota hai. Conflict par 409. SDK endpoint `GET /sdk/snapshots?since=revision` ETag use karega. SDK token sirf apne environment ka read access dega.

## 6. Scale calculation: interviewer ko impress karne ka practical tarika
2 million flag evaluations/second hain. Agar har evaluation network call kare toh 2 million remote QPS! Local evaluation se synchronous QPS almost zero. Lekin 200k SDKs 30 seconds mein ek baar poll karein toh average ~6,667 polls/sec. Ab CDN, ETag, jitter aur delta snapshots se cost reduce karo.

20k tenants × 200 flags = 4 million definitions. Har SDK ko sab tenants ke flags nahi bhejne—sirf scoped tenant/environment configuration.

## 7. Failures ko honestly explain karo
- **Kafka down:** Outbox event safe; publisher retry karega; SDK polling backup.
- **Region down:** SDK old signed snapshot use karega; doosri region se refresh possible.
- **Wrong rule:** Schema validation, staged rollout aur rollback.
- **Kill switch:** Fast push + polling, lekin network partition mein instant global disable guarantee nahi. Ye limitation openly bolo.
- **Tenant data leak:** Scoped tokens, RBAC, encrypted transport, signed config aur audit.
- **Old snapshot replay:** Version monotonicity aur signature validation; controlled rollback alag signed revision se.

**Interview English:** "I separate a strongly controlled authoring plane from an eventually consistent local evaluation plane. SDKs use immutable signed snapshots, deterministic bucketing and last-known-good fallback."

## 8. Testing aur 45-minute presentation
0%, 10%, 100% rollout test karo; same user across Java/JS same bucket. Two-admin race, outbox replay, corrupted snapshot, offline SDK, 10x poll spike, region outage, rollback aur stale config alert test karo.

Time allocation: 5 min requirements, 10 min architecture, 7 min APIs/data, 8 min rollout logic, 10 min failures/scale, 5 min tradeoffs and summary.

**Aakhri interviewer twist:** "Strictly consistent global flag change possible?" Network partition ke time simultaneous worldwide guarantee aur offline availability dono nahi milenge. Requirement clarify karke consistency/availability trade-off bolo.

**Official reading:** https://docs.aws.amazon.com/prescriptive-guidance/latest/cloud-design-patterns/transactional-outbox.html ; https://kafka.apache.org/documentation/

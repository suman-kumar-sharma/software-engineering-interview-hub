# HLD Interview Playbook — Hinglish

## Interviewer kya check karta hai?
Sirf architecture diagram nahi. Kya aap requirements clarify kar sakte ho? Traffic ka estimate nikal sakte ho? Trade-offs justify kar sakte ho? Failure aane par system ka behavior samjha sakte ho?

## 45-minute structure

### 0–5 min: Requirements
**Bolo:** "Before jumping into architecture, I'd like to clarify the core use cases, scale and consistency expectations."

**Samjho:** Pehle problem ki boundary set karo. Example: URL shortener mein create aur redirect important hain; analytics optional ho sakta hai. Scope fix nahi kiya toh unnecessary components add kar doge.

### 5–10 min: Back-of-envelope estimates
Daily requests ko 86,400 se divide karke average QPS nikalo. Peak factor assumption clearly bolo. Storage estimate: objects/day × bytes/object × retention. **Har estimate ka purpose** kisi design decision ko justify karna hai.

### 10–15 min: API + database
Endpoints aur read/write access patterns define karo. "SQL ya NoSQL?" ka answer trend se nahi, access pattern, consistency aur operational needs se decide karo.

### 15–25 min: Minimum viable architecture
```text
Client → API Gateway → Service → Database
                           ↘ Cache (read-heavy case)
                           ↘ Queue → Worker (async work)
```
Har component ko reason ke saath add karo. Ek complete write aur read request trace karo.

### 25–35 min: Bottleneck deep dive
**Bolo:** "The biggest bottleneck is likely X because Y. I'd initially choose A and move to B when metric M crosses threshold T."

Cache hot keys, DB index, shard distribution, retry storms, consumer lag aur ordering par baat karo.

### 35–42 min: Reliability + security
Timeout, bounded retry with jitter, idempotency, DLQ, multi-AZ, backups, rate limits, auth, p99 latency aur alerts. **Important:** "Retry" ka matlab duplicate side effect ho sakta hai; idempotency mandatory ho sakti hai.

### 42–45 min: Summary
"With these assumptions, the design satisfies our key user journeys. The trade-off is A versus B; I'd monitor X before scaling Y."

## Product-company follow-ups
- Agar traffic 10x ho jaye toh pehle kya fail hoga?
- DB outage mein read/write ka behavior kya hoga?
- Eventual consistency kab acceptable hai?
- Region fail hone par RPO/RTO kya target hoga?
- Cache invalidation aur stale reads ka plan kya hai?

[Detailed English playbook](../../interview-framework/hld-interview-playbook.md)

# HLD Interview Playbook — How to Answer

## What the interviewer evaluates
Problem framing, prioritization, system boundaries, data modeling, scalability, consistency, failure handling, and communication—not a memorized diagram.

## 45-minute speaking plan

### 1. Clarify scope (0–5 minutes)
Say: **"Before designing, I'll confirm the core use cases, expected scale, latency target, and consistency needs."**

Ask:
- Who are the users? What are the top two read/write journeys?
- Peak QPS, daily active users, object size, retention, growth?
- Availability, latency p95/p99, geographic coverage, compliance?
- Must writes be immediately visible? Is eventual consistency acceptable?
- What is explicitly out of scope?

Write assumptions when numbers are missing. Ask permission to proceed.

### 2. Estimate only what influences architecture (5–10 minutes)
Compute approximate average and peak QPS, storage/day, network throughput, hot-key concentration. Example: 10 million operations/day ≈ 116 average QPS; at 10× peak ≈ 1,160 peak QPS. State assumptions and round.

Do not overengineer small workloads.

### 3. API and data model (10–15 minutes)
Specify endpoints, request/response contracts, IDs, idempotency, indexing, retention, and ownership. Define read/write access patterns **before** choosing SQL vs NoSQL.

### 4. Draw the simplest working architecture (15–25 minutes)
```text
Client → CDN/WAF → Load Balancer/API Gateway → Stateless Service
                                             ↘ Cache
                                             ↘ Primary Database
                                             ↘ Queue → Async Workers
```
Use components only when justified. Trace one write and one read end-to-end.

### 5. Deep dive on likely bottleneck (25–35 minutes)
Select 1–2: key generation, partitioning, caching, consistency, ordering, search indexing, fan-out, concurrency, or geographic replication.

Say: **"The main risk is X because Y. I would start with A and introduce B when metric M crosses threshold T."**

### 6. Reliability and operations (35–42 minutes)
- Timeouts, retries with jitter, idempotency, dead-letter queues.
- Multi-AZ redundancy, backups and restore testing, RPO/RTO.
- Rate limiting, authN/authZ, encryption, secrets management.
- Monitoring: p95/p99 latency, error rate, saturation, queue lag, cache hit rate.
- Explicitly explain consistency and failure semantics.

### 7. Summarize (42–45 minutes)
Say: **"We meet the primary use cases with X. The main trade-off is Y versus Z. At larger scale, I'd first measure M and then evolve component N."**

## Common traps
- Choosing Kafka/Kubernetes/sharding without evidence.
- Quoting exact capacity numbers without assumptions.
- Treating cache invalidation or retries as free.
- Claiming exactly-once end-to-end without an idempotency strategy.
- Forgetting abuse, observability, data recovery, or operational costs.

## Interview scoring self-check
Can you explain requirements, APIs, data ownership, critical flows, bottleneck, consistency choice, failure handling, and the trade-off in under two minutes?

# HLD Hinglish Walkthrough — Multi-tenant Audit Event Platform
**Date:** 2026-10-20 | **Audience:** SDE2 / Senior Backend

## Problem ko samjho
Maan lo ek company ke multiple tenants hain. Har login, permission change, payment aur admin action ka audit event store karna hai. Last 90 days searchable hona chahiye aur compliance ke liye 7 years archive chahiye. Sabse important rule: API ne event accepted bola toh woh chup-chaap lose nahi hona chahiye.

Assumptions: 20,000 events/sec normal load, 60,000/sec burst, normal load mein p95 acknowledgement under 150 ms. At-least-once delivery acceptable hai agar duplicate events safe handle hote hain. Search index thoda late update ho sakta hai.

## Architecture

~~~text
Producers
   |
Gateway: authentication, quota, request size
   |
Ingestion API: schema validation + tenant authorization
   |
Replicated Kafka
   |                         |
Search consumer           Archive consumer
   |                         |
OpenSearch                Object storage
90-day search             7-year retention
   |
Tenant-filtered search API

DLQ + controlled replay; metrics/traces -> monitoring
~~~

Gateway identity aur quota enforce karta hai. Ingestion API event schema validate karti hai aur broker ke durable acknowledgement ke baad hi accepted bolti hai. Kafka burst absorb karta hai, consumers ko independently scale karne deta hai aur replay provide karta hai. OpenSearch derived search index hai, original acceptance log nahi. Object storage long retention ke liye economical hai. DLQ bad event ko reason aur original bytes ke saath quarantine karta hai.

## API aur tenant isolation
POST /v1/audit-events:batch accept kare. Tenant identity verified authentication context se derive karo; body ke tenantId par blindly trust mat karo. Event mein eventId, schemaVersion, timestamp, actor, action aur resource reference rakho. Oversized batches aur invalid schemas reject karo. Response ka matlab durable acceptance hai, immediate search visibility nahi.

Tenant scope API, repository/search filter, cache key, archive policy aur replay tools sab jagah enforce karo. Privileged reads audit karo, payload PII minimize karo aur data transit/storage encryption use karo.

## Partitioning ka trade-off
Kafka key tenant ID hui toh tenant ke andar order mil sakta hai, lekin ek huge tenant hot partition bana sakta hai. Tenant ke andar stable shards throughput badhate hain, par tenant-wide ordering weak ho sakti hai. Interviewer se clarify karo: order per resource chahiye, per tenant ya global? Global order throughput aur availability ko constrain karti hai.

Average event 1 KB maan kar 20k events/sec roughly 20 MB/s payload hai; 3x burst 60 MB/s. Replication, network aur index overhead alag se add honge. Actual event-size distribution aur skew ke saath benchmark karo.

## Reliability decisions
- Broker unavailable: backpressure/retryable rejection; false success nahi.
- Search down: Kafka events retain kare; lag monitor ho aur recovery par replay ho.
- Archive writer fail: bounded retry, alert, DLQ aur reconciliation.
- Duplicate delivery: tenantId + eventId se dedupe.
- Invalid schema: original payload aur reason quarantine.
- Consumer DB write ke baad crash: replay-safe/idempotent sink.
- Worker lease expire hone ke baad old worker resume ho: fencing token stale writes roke.

Agar business database transaction ke saath audit event atomically create karna hai, same transaction mein outbox row likho aur relay publish kare. Separate ingestion API jahan broker acknowledgement acceptance contract hai, wahan outbox automatically mandatory nahi.

## Observability aur test plan
Accepted/rejected rate, broker latency, tenant/partition-wise lag, oldest event age, index freshness, archive lag, DLQ depth aur replay throughput track karo. Sirf pod alive hona enough nahi; lag badhta rahe toh user-visible delay grow karega.

Load-test skewed tenants. Consumer ko DB write aur offset commit ke beech kill karo. Search/archive ko separately disable karo. Tenant A se tenant B ka data search/cache/replay se access karne ki koshish karo. Archive counts/checksums reconcile karo.

**Interview mein bolo:** “I separate durable ingestion from derived search and archival. API broker durability confirm hone ke baad acknowledge karti hai; consumers replay-safe hain, tenant isolation end-to-end enforce hoti hai, aur lag plus reconciliation observable hain.”

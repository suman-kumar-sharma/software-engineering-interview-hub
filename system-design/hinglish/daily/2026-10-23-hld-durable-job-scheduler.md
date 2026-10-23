
# HLD Classroom — Durable Multi-Tenant Job Scheduler
**Date:** 2026-10-23 | **Level:** SDE2 / Senior Backend

## 1. Pehle requirements
Socho tenants aaj job schedule karte hain jo kal ya agle mahine chalna hai. Service restart ho tab bhi job rehna chahiye. Status dekh sakein, execution se pehle cancel kar sakein aur transient failure par retry ho.

Interviewer se timing precision, recurring jobs, payload size, handler type aur regional RTO/RPO poochho. Example SLO: normal agreed load par p99 job-start lag 5 seconds se kam. Duplicate delivery ko normal maano; exactly-once external effect promise mat karo.

## 2. API aur idempotency
POST create, GET status, DELETE cancel. Create request mein idempotency key rakho. Unique tenant plus idempotency key duplicate create ko rokta hai. Same key par different payload aaye toh request fingerprint compare karke conflict return karo.

## 3. Architecture

~~~text
Client -> API/Auth/Quota -> Job DB + Outbox
                           -> Scheduler shards
Outbox relay -> durable queue -> bounded workers -> handler
                              -> status/retry store
Metrics + DLQ + audited replay
~~~

DB source of truth hai kyunki queue messages duplicate/delay ho sakte hain. Outbox job aur dispatch intent ko same DB transaction mein save karta hai. Queue ka role dispatch buffer hai, schedule ka eklauta record nahi.

## 4. Scheduler kaise claim kare?
Indexed due-time scan se start kar sakte ho. Short transaction mein conditional claim/lease set karo, commit karo, phir queue publish karo. SKIP LOCKED ya conditional update concurrent schedulers ko same job claim karne se bachata hai. Network call ke dauran DB lock hold mat karo.

High volume par time buckets ya timing wheel efficient ho sakte hain, lekin restart ke baad durable DB se timer horizon rebuild karna hoga. Queue-delay-only approach long horizons ke liye risky hai.

## 5. State machine aur failures
SCHEDULED -> DISPATCHED -> RUNNING -> SUCCEEDED. Temporary failure RETRY_WAIT; permanent ya max attempts DEAD.

Cancel aur start race karte hain; conditional transition jeetne wala decide karta hai. Running side effect ko cancel undo nahi kar sakti. Worker crash par lease expire; next worker naya attempt/version claim kare. Fencing version purane worker ko newer status overwrite karne se rokta hai.

Broker publish ke baad DB ack fail ho toh duplicate dispatch ho sakta hai. Handler side effect ke baad status save fail ho toh execution repeat ho sakti hai. Stable idempotency key ya reconciliation use karo.

## 6. Retry, overload, security
Exponential backoff plus full jitter, max attempts, retry budget aur per-tenant concurrency caps rakho. Permanent validation failure ko retry mat karo. DLQ mein reason, redrive authorization, audit aur rate limit ho. Quota cross hone par defer/reject karo; backlog ko unlimited mat hone do.

Tenant ID trusted authentication context se verify karo. Payload schema, size aur schedule horizon validate karo. Sensitive payload encrypt karo, worker egress restrict karo aur arbitrary URL fetch avoid karo—SSRF ka risk hai.

## 7. Capacity aur trade-offs
Due-time distribution, handler duration, retries, payload size aur tenant skew estimate karo. Metrics: due-job lag, oldest outbox age, queue age, lease expiry, attempts, DLQ depth, handler saturation aur tenant rejection.

DB polling recoverable aur simple hai, lekin scale par DB pressure badhta hai. Timing wheel fast hai, par restart/reconciliation complex. Multi-region active-active ke liye ownership/fencing model chahiye; sirf async replication duplicate execution nahi rokta.

**Interview answer:** “Main schedule DB mein persist karta hoon, outbox-backed queue se dispatch karta hoon, leases/fencing se worker ownership manage karta hoon aur at-least-once delivery assume karta hoon. Idempotency side effects ko protect karti hai; due-job lag aur handler capacity scaling guide karte hain.”

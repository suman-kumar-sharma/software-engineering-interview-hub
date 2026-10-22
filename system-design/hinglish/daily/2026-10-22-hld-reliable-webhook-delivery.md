# HLD (Hinglish): Reliable Webhook Delivery
**22 October 2026 | SDE2 / Senior Backend**

## Problem
Socho order create hua aur merchant ke endpoint par webhook bhejna hai. Endpoint down ho sakta hai, worker crash ho sakta hai, aur timeout ke baad sender ko nahi pata receiver ne side effect commit kiya ya nahi. Isliye at-least-once delivery attempts promise karo, generic exactly-once HTTP effect nahi.

## Architecture
~~~text
Business DB transaction: domain row + outbox
 -> relay / CDC -> durable stream
 -> idempotent fan-out -> delivery jobs
 -> tenant/endpoint scheduler -> bounded HTTPS workers
 -> attempt history, retries, DLQ, metrics
~~~

Outbox DB commit aur broker publish ka gap handle karta hai. Durable jobs business API ko slow endpoint se alag rakhte hain. Tenant-wise scheduling se ek slow customer sabke workers consume nahi karta.

## Flow
1. Business row aur outbox row ek transaction mein commit.
2. Relay publish kare, acknowledgement ke baad published mark kare.
3. Fan-out unique tenant + endpoint + event ID se duplicate jobs roke.
4. Scheduler short lease se due job claim kare; HTTP call ke waqt DB lock hold mat karo.
5. Worker stable event ID, timestamp aur HMAC signature ke saath HTTPS call kare.
6. Transient failures par exponential backoff + jitter; exhausted failures DLQ mein.
7. Replay authorized aur audit logged ho.

## Failure cases
- Publish ke baad relay crash hua toh duplicate possible; consumer dedupe kare.
- Timeout ka matlab receiver ne kuch nahi kiya, ye proof nahi. Same event ID retry mein rakho.
- Per-order ordering chahiye toh aggregate ID se partition karo aur retry ordering define karo.
- User URL se SSRF risk: HTTPS enforce, prohibited private/loopback destinations block, connection par DNS revalidate, redirects restrict aur egress controls.
- Per-tenant quota, fair scheduling aur bounded global concurrency rakho.
- Multi-region active-active duplicate kar sakta hai; ownership, fencing aur replay policy define karo.

## Capacity and monitoring
10M events/day × 3 endpoints = 30M jobs/day, ~347/sec average; peak 5x ~1,735/sec before retries. Ye assumptions hain, telemetry se validate karo. Oldest outbox age, queue age, retry amplification, tenant-wise timeout, p95/p99 latency, DLQ count aur worker saturation track karo.

Polling outbox simple par DB load deta hai. CDC polling kam par connector complexity badhata hai. Queue burst absorb karti hai; DB scheduler due-time query simplify karta hai.

**Interview answer:** “Outbox aur durable jobs use karunga, bounded workers se signed HTTPS bhejunga. At-least-once assume karunga kyunki timeout ambiguous hai. Stable IDs, receiver idempotency, retries, DLQ, tenant fairness aur SSRF defense reliability dete hain.”

Reference: https://www.postgresql.org/about/news/

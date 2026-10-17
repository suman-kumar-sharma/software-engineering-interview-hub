# HLD — Notification Platform | Hinglish Classroom (2026-10-17)
**Target:** Product-company Senior Backend. Socho main whiteboard par 45-minute interview ki preparation karwa raha hoon.

## 1. Pehle problem samjho
Maan lo tum Swiggy/Flipkart type platform bana rahe ho. Order successful hua toh user ko push aur email bhejna hai. Kisi ko SMS nahi chahiye, kisi ne marketing notifications opt-out kiya hai. Aur provider kabhi timeout karta hai. Ab humko reliable system banana hai.

**Interviewer se clarify karo:** Transactional vs marketing? Kitni notifications? Delivery ka matlab provider ne accept kiya ya user ne dekha? Retry kitni baar? Tenant-wise quotas? Assumption: 1 million logical notifications/minute peak, average 2 channels, roughly 33k channel attempts/sec before retries. Yeh sizing exercise hai, actual measured traffic nahi. API 202 response ka p99 target 200ms; actual phone delivery alag SLO.

## 2. API aur database kyun aisa?
~~~http
POST /v1/notifications
Idempotency-Key: tenant-specific-key
{"tenantId":"t1","recipientId":"u42","templateId":"payment",
 "channels":["EMAIL","PUSH"]}
202 {"notificationId":"n77","status":"ACCEPTED"}
GET /v1/notifications/n77
PUT /v1/users/u42/preferences
~~~
**Socho client network timeout ke baad same POST dobara bhej de.** Agar har baar new notification banayi toh duplicate email jayega. Isliye (tenant,idempotency_key) unique rakho.

Tables: notification, per-channel delivery, user preference, outbox. Notification ACCEPTED ka matlab DB commit ho chuka hai; provider ne deliver kar diya, yeh claim nahi. Tenant isolation har query mein enforce karo.

## 3. Architecture step by step
~~~text
Client -> API/Auth -> Notification API -> PostgreSQL + Outbox
                                       |
                                  Outbox Relay
                                       v
                                   Event Bus
                                       |
                               Preferences/Templates
                               /        |        \
                         Email queue  SMS queue  Push queue
                              |         |         |
                            Workers   Workers   Workers
                               \        |        /
                               Provider Adapters
                                      |
                              Callback -> Status DB
~~~
**Ab ek tricky failure:** Database mein notification save ho gayi, lekin Kafka publish se pehle service crash. User ko 202 mil gaya, event lost! Isliye notification aur outbox row same DB transaction mein insert karo. Relay later event publish karega. Relay duplicate bhi publish kar sakta hai, toh consumer idempotent chahiye.

**Flow:** Validate/auth -> idempotency -> DB+outbox commit -> relay -> preferences check -> template render -> per-channel delivery queue -> worker -> provider -> callback/status. Marketing opt-out ko send se just pehle bhi check karna pad sakta hai.

## 4. Scale kaise karoge?
33k attempts/sec par single worker enough nahi. Email, SMS, push alag worker pools. Per-provider rate limit aur per-tenant quota rakho; ek giant tenant dusron ko starve na kare. Stream partition by tenant+recipient agar per-user ordering chahiye; hot partitions monitor karo. Retry exponential backoff+jitter with max attempts; poison messages DLQ. Status polling ko indexed queries/cache se handle karo.

## 5. Interviewer ka favourite twist
**Provider ne email accept kar liya, worker status DB mein likhne se pehle crash:** Retry duplicate send kar sakta hai. Provider idempotency support ho toh same delivery ID use karo; warna exactly-once ka false promise mat karo.

**Preference badal gayi:** Marketing opt-out ke liye send-time recheck. **Provider down:** Circuit breaker, bounded retries, explicit degraded status. **Callback fake:** Signature verify karo. **Multi-region:** Tenant home-region ownership; active-active mein fencing/coordination. **Security:** PII encryption, tenant authorization, retention, template escaping, secret rotation.

## 6. Tests aur trade-offs
Load test 1M/min; inject DB failover, relay duplicate, worker crash after provider send, callback replay aur provider timeout. Track acceptance p99, queue age, provider error rate, retries per notification, duplicate sends, opt-out violations and cost.

Synchronous direct send simple hai but API provider outage se fail hogi. Outbox+queues complex hain but durable acceptance and decoupling deti hain.

**45-minute plan:** 5m requirements; 5m APIs; 10m architecture; 10m scaling; 10m failure/security; 5m trade-offs.

**Interview English:** “I decouple durable notification acceptance from provider delivery using a transactional outbox, idempotent channel workers, preference enforcement and bounded retries.”

Reference: https://aws.amazon.com/builders-library/timeouts-retries-and-backoff-with-jitter/

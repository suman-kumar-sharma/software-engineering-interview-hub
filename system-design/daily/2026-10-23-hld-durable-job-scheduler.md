
# HLD Interview Walkthrough — Durable Multi-Tenant Job Scheduler
**Date:** 2026-10-23 | **Level:** SDE2 / Senior Backend

## 1. Clarify requirements
Assume tenants schedule one-shot jobs seconds to months ahead. Jobs survive process restarts, expose status and can be cancelled before execution. Transient failures retry. At-least-once delivery is acceptable; exactly-once external side effects are not promised.

Ask about precision, recurring schedules, maximum payload, handler type, regional RTO/RPO and duplicate execution. A measurable example is p99 start lag ≤ 5 seconds at agreed normal load.

## 2. API contract
- POST creates a job with task type, due time, bounded payload/reference and idempotency key.
- GET returns state, attempts, timestamps and sanitized error.
- DELETE requests cancellation and reports whether the conditional transition won.
- List uses tenant-scoped keyset pagination.

Unique tenant plus idempotency key prevents duplicate creates. Store a request fingerprint; the same key with different content returns conflict.

## 3. Architecture

~~~text
Client -> API/Auth/Quota -> Job DB + Outbox
                           -> Due-time scheduler shards
Outbox relay -> durable queue -> bounded workers -> handler
                              -> status/retry store
Metrics + DLQ + audited replay + tenant audit
~~~

The database is schedule source of truth. The queue is a dispatch buffer. The outbox commits the job and dispatch intent together, closing the database dual-write gap.

## 4. Persistence
Store job ID, tenant ID, idempotency key, request hash, task type, payload reference, due time, state, attempt, next retry time, lease expiry, version and timestamps. Index scheduled rows by due time and retry-wait rows by retry time. Keep large/sensitive payloads outside the hot relational row where appropriate.

## 5. Scheduling options
**Indexed DB scan:** simplest and recoverable. Claim small batches in short transactions; publish after commit.  
**Time buckets/timing wheel:** efficient near-term wakeups, but rebuild from durable state after restart.  
**Queue-delay-only:** not a good sole source for long scheduling horizons or restrictive delay limits.

Use SKIP LOCKED or compare-and-set transitions so multiple schedulers do not claim the same row. Do not hold row locks during network calls.

## 6. State machine and races
SCHEDULED -> DISPATCHED -> RUNNING -> SUCCEEDED; transient failures enter RETRY_WAIT; permanent/exhausted failures enter DEAD.

- Cancel versus start: conditional transitions race; one wins. Cancellation cannot undo an in-flight external side effect.
- Worker crash: lease expires; a new worker claims a new attempt.
- Stale worker: use a version/fencing token so an old attempt cannot overwrite newer state.
- Broker accepted publish but DB ack failed: dispatch may duplicate.
- Handler side effect succeeded but status update failed: execution may repeat; use a stable idempotency key or reconciliation.

## 7. Retry and overload
Use exponential backoff with full jitter, max attempts, retry budgets and per-tenant concurrency caps. Classify transient versus permanent errors; ambiguous side effects need reconciliation before retry. DLQ should have reason codes, authorization, redaction, replay rate limits and audit trail.

Apply admission control before backlog becomes unbounded. Defer or reject over-quota tenants instead of degrading every tenant.

## 8. Security
Derive tenant from verified identity and check it on every read/write. Validate task type, payload schema, schedule horizon and payload size. Encrypt sensitive data, restrict worker egress and avoid arbitrary URL fetches (SSRF). Do not log secrets.

## 9. Capacity and operations
Estimate create rate, due-time distribution, handler service time, retry amplification, payload bytes and tenant skew. Track due-job lag, oldest outbox age, queue age, lease expiry, attempts/job, error class, DLQ depth, handler saturation and tenant rejections. Scale workers from measured downstream capacity, not CPU alone.

## 10. Regional recovery and trade-offs
Begin with active/passive unless RTO/RPO requires more. Define scheduler ownership transfer and fencing. Async replication alone does not prevent two regions from running the same job. DB scans are simple but can pressure the database; timing wheels improve timer throughput but add reconciliation complexity.

**Close:** “I persist the schedule, dispatch via an outbox-backed queue, claim work with leases and fencing, and assume at-least-once delivery. Idempotency belongs at the side-effect boundary. I scale using due-job lag and handler capacity, while quotas protect noisy tenants.”

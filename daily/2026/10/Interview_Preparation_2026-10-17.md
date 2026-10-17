# Interview Preparation — 2026-10-17

## Q1: Backpressure

When arrivals exceed downstream capacity, a bounded queue, concurrency limit and explicit rejection protect latency and memory. Virtual threads do not increase database or downstream capacity. Use end-to-end deadlines and a retry budget.

## Q2: Kafka consumer rebalance

A rebalance can replay records after external side effects but before offset commits. Use stable business idempotency keys, durable unique constraints and provider idempotency support. Reconcile unknown payment outcomes.

## Q3: PostgreSQL SKIP LOCKED job claims

Use SELECT ... FOR UPDATE SKIP LOCKED in a short transaction to claim due jobs. Commit before calling an external provider; keep owner, lease_until and attempt counters. Expired leases may be reclaimed, so business side effects must be idempotent. Two workers skip each other's locked rows. Index by status and run_at. Complexity depends on query plan; verify with EXPLAIN ANALYZE. Follow-up: stale workers must not finalize work after a lease is stolen.

Reference: https://www.postgresql.org/docs/current/sql-select.html

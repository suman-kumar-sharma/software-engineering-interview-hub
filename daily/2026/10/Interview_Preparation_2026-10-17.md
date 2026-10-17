# Interview Preparation — 2026-10-17

## Q1: Backpressure

When arrivals exceed downstream capacity, a bounded queue, concurrency limit and explicit rejection protect latency and memory. Virtual threads do not increase database or downstream capacity. Use end-to-end deadlines and a retry budget.

## Q2: Kafka consumer rebalance

A rebalance can replay records after external side effects but before offset commits. Use stable business idempotency keys, durable unique constraints and provider idempotency support. Reconcile unknown payment outcomes.

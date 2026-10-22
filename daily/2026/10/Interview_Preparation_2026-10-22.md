# Interview Preparation — 22 October 2026

Focus: Java 17+, Spring, Kafka, PostgreSQL, Redis and AWS for product-company SDE2/Senior Backend interviews.

1. Parallel fan-out: use a bounded executor, request deadline and client timeouts. Cancellation is best effort.
2. Volatile: visibility is not atomicity; count++ can lose updates. Use atomic operations or a lock for compound invariants.
3. Kafka: commit business mutation and inbox event ID in one DB transaction, then commit offset. A unique constraint handles redelivery.
4. Outbox: write business state and outbox row atomically; relay may publish duplicates, so consumers remain idempotent.
5. Pagination: keyset cursor with a unique tie-breaker avoids deep OFFSET scans; verify the composite index with EXPLAIN ANALYZE.
6. Redis: local single-flight, TTL jitter and bounded origin concurrency help with stampedes; define acceptable staleness.
7. DSA: longest unique substring uses a sliding window and last-seen index map; O(n) expected time.
8. AWS: Lambda self-managed Kafka event source mappings added OAuth authentication on 9 October 2026.

HLD: reliable webhook delivery with outbox, durable jobs, bounded workers, signatures, retry and dead-letter handling.
LLD: idempotency coordinator; local in-flight coalescing is not durable cross-replica idempotency.

References: AWS Lambda Kafka OAuth announcement; PostgreSQL project news; Java SE specifications.

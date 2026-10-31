# Advanced Java Backend Practice

Self-contained sessions with questions, complete worked answers and explanations of how to derive each solution. The chat overview targets 15 minutes; reference files contain the full English and natural Roman-script Hinglish versions.

## Layout

- `english/YYYY/MM/Backend_Practice_YYYY-MM-DD.md`: comprehensive English solution.
- `hinglish/YYYY/MM/Backend_Practice_YYYY-MM-DD.md`: equally comprehensive Roman-script Hinglish explanation.

Topics rotate through Java concurrency/JMM, virtual threads and resource limits, CompletableFuture, Kafka reliability, JVM internals/diagnostics and resilient distributed systems. Sessions include constraints, reasoning, code/design, dry runs, failure cases, production trade-offs, interview answers and authoritative sources.

## Session index

| 2026-10-31 | JVM diagnostics: JFR, thread dumps and diagnosing remote I/O while holding a DB connection | [English](english/2026/10/Backend_Practice_2026-10-31.md) · [Hinglish](hinglish/2026/10/Backend_Practice_2026-10-31.md) |
| Date | Topic | Reference |
|---|---|---|
| 2026-10-28 | Kafka reliability: transactional outbox, Debezium CDC and idempotent consumers | [English](english/2026/10/Backend_Practice_2026-10-28.md) · [Hinglish](hinglish/2026/10/Backend_Practice_2026-10-28.md) |
| 2026-10-25 | Java concurrency/JMM: volatile visibility vs atomic increments; AtomicLong vs LongAdder | [English](english/2026/10/Backend_Practice_2026-10-25.md) · [Hinglish](hinglish/2026/10/Backend_Practice_2026-10-25.md) |
| 2026-10-24 | CompletableFuture orchestration: mandatory payment, optional shipping and timeouts | [English](english/2026/10/Backend_Practice_2026-10-24.md) · [Hinglish](hinglish/2026/10/Backend_Practice_2026-10-24.md) |
| 2026-10-21 | Java 21 virtual threads: bound database concurrency, not threads | [English](english/2026/10/Backend_Practice_2026-10-21.md) · [Hinglish](hinglish/2026/10/Backend_Practice_2026-10-21.md) |
| 2026-10-18 | Kafka concurrent workers: safe offset commits and rebalances | [English](english/2026/10/Backend_Practice_2026-10-18.md) · [Hinglish](hinglish/2026/10/Backend_Practice_2026-10-18.md) |
| 2026-10-17 | Retry storms: deadlines, jitter, bulkheads and retry budgets | [English](english/2026/10/Backend_Practice_2026-10-17.md) · [Hinglish](hinglish/2026/10/Backend_Practice_2026-10-17.md) |
| 2026-10-14 | JVM diagnostics: allocation vs retention; bounded LRU cache | [English](english/2026/10/Backend_Practice_2026-10-14.md) · [Hinglish](hinglish/2026/10/Backend_Practice_2026-10-14.md) |
| 2026-10-11 | Safe configuration reloads: immutable snapshots and CAS | [English](english/2026/10/Backend_Practice_2026-10-11.md) |

Scheduled sessions are configured to publish English and Hinglish companions through the connected GitHub integration. Each run must report publication only after confirming successful writes; a failed push must be stated explicitly.

# Advanced Java Backend Practice

Self-contained sessions with questions, worked solutions and explanations of how to derive the solution. Core reading fits approximately 15 minutes; reference files include deeper production and interview guidance.

## Layout

- `english/YYYY/MM/Backend_Practice_YYYY-MM-DD.md`: comprehensive English solution.
- `hinglish/YYYY/MM/Backend_Practice_YYYY-MM-DD.md`: same topic explained naturally in Roman-script Hinglish.

Topics rotate through concurrency/JMM, virtual threads, CompletableFuture, Kafka reliability, JVM internals and resilient distributed systems. Each report includes constraints, reasoning, Java code or design, walkthrough, edge cases, failure handling, tradeoffs, interview answers and sources.

## Session index

| Date | Topic | Reference |
|---|---|---|
| 2026-10-18 | Kafka concurrent workers: safe offset commits and rebalances | [English](english/2026/10/Backend_Practice_2026-10-18.md) · [Hinglish](hinglish/2026/10/Backend_Practice_2026-10-18.md) |
| 2026-10-17 | Retry storms: deadlines, jitter, bulkheads and retry budgets | [English](english/2026/10/Backend_Practice_2026-10-17.md) · [Hinglish](hinglish/2026/10/Backend_Practice_2026-10-17.md) |
| 2026-10-14 | JVM diagnostics: allocation vs retention; bounded LRU cache | [English](english/2026/10/Backend_Practice_2026-10-14.md) · [Hinglish](hinglish/2026/10/Backend_Practice_2026-10-14.md) |
| 2026-10-11 | Safe configuration reloads: immutable snapshots and CAS | [Worked solution with Hinglish explanation](english/2026/10/Backend_Practice_2026-10-11.md) |

Future scheduled sessions are configured to publish English and Hinglish companions through the connected GitHub integration. Publication depends on successful access in each run; a failed push must be reported explicitly.

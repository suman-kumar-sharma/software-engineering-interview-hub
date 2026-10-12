# Comprehensive HLD & LLD Interview Rubric

Use this rubric to practice *communication, correctness, and trade-off reasoning*. A technically elaborate design with no clear requirements is a weak answer.

## Scoring (100 points)

| Area | HLD | LLD |
|---|---:|---:|
| Clarifying questions and assumptions | 15 | 15 |
| Requirements and invariants / SLOs | 10 | 15 |
| API and data model / object model | 15 | 20 |
| Core flow, correctness, and implementation | 20 | 25 |
| Scaling, concurrency, failure modes | 20 | 10 |
| Trade-offs, extensibility, testability | 10 | 10 |
| Communication and final recap | 10 | 5 |

### What a strong answer sounds like

**Clarify:** "I'll optimize for the two main user journeys and state assumptions before choosing storage."

**Choose:** "Option A is simpler and sufficient at our estimated scale. Option B improves X but adds Y. I'd begin with A and measure Z."

**Handle failure:** "The write is committed before acknowledging success; retries carry an idempotency key. A timeout means the result may be unknown, not necessarily failed."

**Validate:** "I'd load test at peak QPS, verify p99 latency, inject downstream failures, and test duplicate requests."

## HLD: questions to ask yourself
1. Are functional and nonfunctional requirements separated and prioritized?
2. Are traffic, storage, network, and growth estimates mathematically defensible?
3. Are critical API contracts, pagination, errors, and idempotency explicit?
4. Does the data model match read/write access patterns and index needs?
5. Can you trace a read and write across every component?
6. Is the consistency model defined *per operation*?
7. What happens on cache miss, database outage, region outage, queue backlog, duplicate delivery?
8. Are auth, abuse prevention, PII, retention, backups, and observability addressed?
9. Which component saturates first, and what metrics trigger an evolution?
10. What would you cut if the interviewer gave you 10 minutes less?

## LLD: questions to ask yourself
1. Can you list operations and invariants before naming classes?
2. Are entities, value objects, and services cohesive?
3. Do interfaces isolate *real variation* rather than decorate every class?
4. Is the public API explicit about invalid states and failures?
5. Are state transitions legal and testable?
6. Is concurrency safe for the stated deployment model?
7. Are database constraints and transactions used for multi-instance safety?
8. Are external side effects idempotent or compensatable?
9. Do unit tests cover boundaries, races, and repeated operations?
10. Can you explain which SOLID principles and patterns were applied, and why?

## Practice loop (60 minutes)
- 5 min: read prompt, ask clarification questions.
- 35 min: speak and sketch as though interviewed.
- 10 min: compare against reference solution and rubric.
- 10 min: rewrite weak areas, then record a 90-second summary.

## Red flags
- "Exactly once" claimed for external effects without deduplication or idempotency.
- Redis described as the source of truth without a durability decision.
- Database sharding proposed before capacity estimates.
- Java `synchronized` presented as protection across service replicas.
- An async queue added without delivery semantics or backpressure.
- A payment considered successful just because the request was sent.
- Incomplete or untested code described as production-ready.

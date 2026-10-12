# System Design Interview Playbook

This section teaches **how to answer**, not just what to memorize. Practice speaking while drawing diagrams and explaining trade-offs.

## Recommended structure

```text
system-design/
├── README.md
├── interview-framework/
│   ├── hld-interview-playbook.md
│   └── lld-interview-playbook.md
├── hld/
│   └── url-shortener.md
├── lld/
│   └── parking-lot.md
└── templates/
    ├── hld-template.md
    └── lld-template.md
```

## Suggested interview timing

| Stage | HLD (45 min) | LLD (45 min) |
| --- | ---: | ---: |
| Clarify requirements and scope | 5 | 5 |
| Define interfaces and data model | 5 | 5 |
| Sketch high-level design / classes | 10 | 10 |
| Walk through core flows / implement | 10 | 15 |
| Bottlenecks, edge cases, trade-offs | 10 | 7 |
| Recap and questions | 5 | 3 |

## Comprehensive additions
- [Interview scoring rubric and practice checklist](interview-framework/comprehensive-interview-rubric.md)
- [HLD: Distributed rate limiter](hld/rate-limiter.md)
- [LLD: Thread-safe LRU cache (Java 17)](lld/lru-cache.md)

## Study order
1. [HLD interview playbook](interview-framework/hld-interview-playbook.md)
2. [HLD worked example: URL shortener](hld/url-shortener.md)
3. [LLD interview playbook](interview-framework/lld-interview-playbook.md)
4. [LLD worked example: parking lot](lld/parking-lot.md)
7. Reuse the [HLD template](templates/hld-template.md) and [LLD template](templates/lld-template.md).

## Future case-study backlog

**HLD:** Rate limiter, notification platform, chat, news feed, distributed cache, file storage, payment service, ride matching, video streaming, job scheduler.

**LLD:** Elevator, library, vending machine, splitwise, chess, booking system, rate limiter, LRU cache, Kafka-like broker, task scheduler.

Each new case study should include: interview prompt, clarifying questions, assumptions, requirements, estimates (HLD), API/data model, diagrams, walkthrough, failure cases, trade-offs, and a 60-second interview recap. LLD additionally needs Java code, SOLID/design patterns, tests and extension points.

> These are educational reference solutions, not complete production implementations. Interviewers may prefer different trade-offs depending on clarified constraints.

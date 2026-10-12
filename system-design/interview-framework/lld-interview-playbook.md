# LLD Interview Playbook — How to Answer in Java

## What the interviewer evaluates
Clear responsibilities, object modeling, invariants, extensibility, executable behavior, tests, and communication.

## 45-minute speaking plan

### 1. Clarify requirements (0–5 minutes)
Say: **"I'll identify the core use cases and invariants first, then model only the classes needed for those flows."**

Ask about operations, supported variants, persistence, concurrency, failure behavior, and scope. Separate must-have from nice-to-have.

### 2. Identify invariants and public contracts (5–10 minutes)
Examples: one active ticket per parked vehicle; no spot assigned to two active tickets; payment only once per ticket. State method signatures and errors before implementation.

### 3. Draw relationships (10–20 minutes)
Identify entities, value objects, services, repositories, and strategies. Prefer composition over inheritance. Use interfaces at change boundaries—not for every class.

```text
Controller → Application Service → Repository
                  ↘ AllocationStrategy
Entities: Ticket, Spot, Vehicle
Value objects: SpotId, TicketId, Money
```

### 4. Implement happy path, then invariants (20–35 minutes)
Start with one end-to-end flow. Make state transitions explicit. Use constructor injection, immutable identifiers, meaningful exceptions, and cohesive methods.

Say: **"I am keeping allocation separate from the service so nearest-first can later be swapped for EV/accessible allocation without changing ticket lifecycle."**

### 5. Tests, concurrency, extensions (35–42 minutes)
Cover: normal operation, no capacity, invalid state, duplicate requests, concurrency, payment failure, and retries. Explain transaction boundaries and locking.

### 6. Recap (42–45 minutes)
Say: **"The key invariant is X; service Y owns the transition, repository Z persists it, and strategy W isolates the variable policy."**

## Practical Java checklist
- Use enums for finite state, records for immutable data where appropriate.
- Keep business rules out of controllers and repositories.
- Prefer explicit state transitions to boolean flags.
- Use `BigDecimal` or integer minor units for money, not `double`.
- Avoid overusing Singleton/Factory patterns merely to name patterns.
- Distinguish thread-safe in-memory design from multi-instance database safety.
- Tests should prove invariants, not just method calls.

## Common mistakes
Huge UML before requirements, God objects, premature abstractions, missing failure paths, static global state, non-atomic check-then-act, and no test strategy.

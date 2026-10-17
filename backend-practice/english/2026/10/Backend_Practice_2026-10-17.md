# Preventing Retry Storms with Deadlines and Retry Budgets
Date: 2026-10-17 | Runtime: Java 21 service; library-neutral design pseudocode
Prerequisites: blocking HTTP calls, timeouts, basic semaphore usage. Core reading: 15 minutes (3 concept / 8 exercise / 4 review). This rotates from JVM diagnostics to distributed-system resilience.

## 3-minute concept question with answer
**Question:** Service A calls B, which calls C. Each layer allows three total attempts. How many calls can one request generate at C?
**Answer:** Up to nine if A retries B three times and each B invocation retries C three times. If three separate caller layers each repeat the entire next layer three times, the leaf can receive 27 attempts. Always clarify attempts versus retries: three retries means four attempts.

Retries can help transient failures but add load precisely when a dependency may be struggling. Choose one retry owner for the logical operation, bound its time and attempts, randomize retry timing, and limit aggregate retries. A circuit breaker, a concurrency bulkhead and a retry budget have distinct jobs; none supplies the others automatically.

## 8-minute practical exercise
Order lookup is a read-only, retry-safe operation. Implement a policy with:
- 600 ms local end-to-end budget, including waits and backoff.
- At most three total attempts.
- Each HTTP attempt gets at most 180 ms, reduced to remaining budget.
- Immediate admission into a dependency bulkhead; reject when full.
- Full-jitter exponential retry delays: first retry cap 25 ms; second 50 ms; absolute cap 100 ms.
- Shared per-instance/per-dependency retry token bucket: capacity 20, refill 5 tokens/second. Original attempts consume no retry token.
- Cancellation propagates. Permanent failures and local overload are not retried.
These numbers are teaching assumptions, not universal production defaults. HTTP and SDK hidden retries must be disabled or included explicitly.

## Deriving the solution
1. A per-attempt timeout multiplied by attempts is not an overall deadline. Create one monotonic deadline before any queueing.
2. Recalculate remaining time after every delay/admission. Never reset the 600 ms budget on retry.
3. Put the concurrency limit around each actual HTTP attempt, releasing it before backoff.
4. Use immediate bulkhead acquisition to avoid an unbounded admission queue.
5. Retry only a known transient outcome and a safe operation. Caller cancellation is not transient failure.
6. Use jitter to spread retries, then a shared retry bucket to cap added traffic. Per-request attempt limits alone do not bound fleet retry rate.
7. Preserve server Retry-After minimum delays or decline/defer when the deadline cannot accommodate them.

## Complete design solution
```text
lookup(orderId, budget = 600 ms):
    deadline = monotonicNow() + budget
    for attempt in 1..3:                 # total attempts, not 3 retries
        propagate cancellation/interruption
        remaining = deadline - monotonicNow()
        if remaining < 20 ms: fail DeadlineExceeded

        if attempt > 1:
            if remaining < 40 ms: fail DeadlineExceeded
            cap = min(100 ms, 25 ms * 2^(attempt - 2))
            delay = randomUniform(0, min(cap, remaining - 40 ms))
            cancellableSleep(delay)
            # Recheck after sleeping; scheduling can overshoot.

        remaining = deadline - monotonicNow()
        if remaining < 20 ms: fail DeadlineExceeded

        if not dependencyBulkhead.tryAcquireImmediately():
            fail LocalOverload          # do not retry local overload
        try:
            propagate cancellation/interruption
            remaining = deadline - monotonicNow()
            if remaining < 20 ms: fail DeadlineExceeded
            if attempt > 1 and not sharedRetryBucket.tryConsume():
                fail RetryBudgetExhausted

            timeout = min(180 ms, remaining - 10 ms)
            response = client.lookup(orderId, timeout)
            # Adapter must bound pool wait/connect/TLS/read as supported,
            # disable hidden retries and release response resources.
        catch KnownTransientTransportFailure as error:
            outcome = retryable(error)
        finally:
            dependencyBulkhead.release()

        if monotonicNow() >= deadline: fail DeadlineExceeded
        if response succeeded: return response
        if outcome/response is permanent: fail originalFailure
        if response is 429 or 503 with valid Retry-After:
            # Honor server's minimum delay; jitter may add time, not subtract.
            # If it cannot fit with a useful attempt, fail/defer; never retry early.
            apply server delay constraint to next iteration
        if attempt == 3: fail lastFailure
```

Here response/outcome is per-attempt state, initialized fresh each iteration. The server-delay branch augments the next iteration's sleep with its minimum delay constraint; it does not add a second independent retry policy. Validate HTTP-date Retry-After using wall time, convert the resulting nonnegative delay to the local monotonic budget, and reject absurd/malformed values according to an explicit policy. Do not blindly retry all IOExceptions: classify client/transport failures. Authentication, validation, serialization bugs and permanent business failures normally fail immediately.

The token bucket is a synchronized shared component: under one short lock, compute elapsed monotonic time, refill tokens to min(20, tokens + elapsedSeconds*5), update its time marker, and atomically subtract one only when tokens >= 1. It has no blocking wait. Consume immediately before a retry's HTTP attempt, after admission. Never hold the bucket lock during network work. It is a rate policy, not a semaphore; tokens refill with time, not on completion.

## Java building blocks
Use elapsed-time subtraction based on System.nanoTime for local deadlines; do not serialize its values across hosts. Semaphore.tryAcquire() supplies immediate local admission, and release belongs in finally only after successful acquisition. Pass explicit Duration-style remaining limits into the client adapter. Timeout support varies: verify whether it covers connection-pool acquisition, DNS, TLS and body consumption. A future timeout alone cannot guarantee the remote operation stopped.

## 4-minute dry run
Illustrative elapsed times:
- t=0: first call admitted; fails transiently after 180 ms.
- First jitter sample 17 ms: t=197; second call receives 180 ms and one retry token.
- It fails at t=377. Second jitter sample 31 ms: t=408.
- Third call receives min(180, 192-10)=180 ms and a second token; succeeds at t=550.
All paths release bulkhead capacity before waiting. If scheduling delay leaves 15 ms at the next check, no further call starts. If the bucket is empty, a retry is denied even though request time remains.

This is a cooperative deadline, not a hard real-time guarantee: GC/scheduling and client cancellation behavior can delay method return. Reject late results, use real transport timeouts and propagate cancellation. Client timeout does not prove a remote side effect failed.

## Edge cases, tradeoffs and observability
- 10 replicas with refill 5/s permit approximately 50 retry tokens/s after initial bursts; it is not a global 5/s limit. Fleet-wide limits need coordination or allocated budgets.
- Failed attempts consume capacity until their underlying work actually ends. Do not release merely because a wrapper timed out while work continues.
- Read-only lookup is safe here. Payment writes require a stable idempotency key and reconciliation after uncertain outcomes.
- Token exhaustion should not trigger another layer's automatic retries.
- Circuit breakers can stop attempts during sustained failure, but recovery probes and state transitions need testing.
- Record logical requests versus actual attempts, retries denied, bulkhead rejection, remaining budget, attempt latency and retry success. Define amplification as downstream attempts / original logical requests.
- In Spring deployments, inventory gateway/client/SDK/annotation retry policies so only the chosen owner retries this operation.

## Verification plan
Use injectable monotonic clock, seeded random generator, sleeper and scripted client. Check: success first time; transient then success; permanent error only once; zero tokens denies retry; full bulkhead produces no HTTP call; sleep overshoot prevents next attempt; interruption releases acquired capacity; server delay beyond deadline does not retry early; max three attempts; late success is rejected. This report supplies pseudocode and a test plan, not executed tests or a production-ready HTTP adapter.

## Follow-ups with answers
**Why jitter if attempts are capped?** It changes timing correlation; caps bound count.
**Why a bucket if there is a bulkhead?** Bulkhead bounds in-flight work, bucket bounds retry rate.
**Should 429 always retry?** Only if operation/policy permits and server delay fits the deadline.
**Why not increase timeouts until errors disappear?** Longer waits retain resources and may violate upstream budgets.
**Interview answer:** “I pick one retry owner, use an inherited deadline and capped attempt timeouts, retry only safe transient outcomes, add jitter, release attempt capacity before backoff, and cap aggregate retries with a shared budget. I verify transport cancellation and observe amplification rather than only final success.”

## Primary references
- [AWS: timeouts, retries, backoff and jitter](https://aws.amazon.com/builders-library/timeouts-retries-and-backoff-with-jitter/)
- [Java 21 System.nanoTime](https://docs.oracle.com/en/java/javase/21/docs/api/java.base/java/lang/System.html#nanoTime())
- [Java 21 Semaphore](https://docs.oracle.com/en/java/javase/21/docs/api/java.base/java/util/concurrent/Semaphore.html)
- [HTTP Retry-After semantics, RFC 9110](https://www.rfc-editor.org/rfc/rfc9110.html#name-retry-after)

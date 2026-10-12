# HLD Case Study: Distributed API Rate Limiter

**Interview prompt:** Design a rate limiter for a public API with 100k requests/s peak, per-user and per-API-key quotas, and low overhead. Explain multi-region behavior.

## 1. Clarify aloud
"Are limits per second, minute, or day? Must every request be counted globally? Is the product allowed to occasionally exceed a limit during partitions? Should rejected calls count? Are authenticated and anonymous clients treated differently?"

**Assumptions:** Per-key 100 requests/minute with optional burst of 20; 99.9%+ availability; fail-open for noncritical read endpoints and fail-closed for high-risk operations; globally coordinated exact counts are not required.

## 2. Requirements
- Fast allow/deny decision; return HTTP 429 with `Retry-After`.
- Per-tenant quotas and optional endpoint overrides.
- Predictable bursts; avoid unfairness.
- Prevent hot-key abuse; monitor rejected traffic.
- Support configuration updates without redeployment.

## 3. Capacity
At 100k requests/s, a central datastore call on every request requires at least 100k operations/s before replication and retries. Sharding and local enforcement may be needed. A 1 ms added hop contributes ~1 ms to latency even without queueing.

## 4. API contract
```text
check(key, policy, cost=1) → {allowed, remaining, resetAt}
```
HTTP response when rejected:
```http
HTTP/1.1 429 Too Many Requests
Retry-After: 12
RateLimit-Limit: 100
RateLimit-Remaining: 0
```
Header format must match the API contract actually adopted.

## 5. Architecture
```text
Client → Edge/API Gateway → Rate-limit middleware → Backend
                          ↘ Policy cache
                          ↘ Counter service → Sharded Redis
                                      ↘ Metrics/events
```
Policy is versioned; counters have TTL. Use a consistent hash of tenant/key to spread load. Avoid storing raw API keys in metric labels.

## 6. Algorithm decision
| Algorithm | Advantages | Weaknesses |
| --- | --- | --- |
| Fixed window | Simple, cheap | Boundary bursts up to 2× |
| Sliding log | Precise | High memory at scale |
| Sliding counter | Bounded state | Approximate |
| Token bucket | Allows controlled bursts | Requires atomic refill/update |
| Leaky bucket | Smooth output | May queue or reject bursts |

**Choose token bucket** for burst allowance. Track tokens and last refill timestamp. Atomic update is essential.

Pseudocode for one key:
```text
elapsed = max(0, now - lastRefill)
tokens = min(capacity, tokens + elapsed * refillRate)
if tokens >= cost: tokens -= cost; allow
else: reject
save(tokens, now) atomically
```
Redis Lua or a datastore transaction ensures concurrent decisions do not both consume the same token. Consider monotonic server-side time and bounded clock skew.

## 7. Failure and consistency
- Redis unavailable: documented fail-open/closed policy by endpoint, local emergency limiter.
- Hot tenant: local token leasing or hierarchical limiters; leasing can over-admit by bounded amount.
- Cross-region: independent regional budgets are simple but permit total overshoot. Global strict quota adds latency and availability trade-offs.
- Retry storms: reject early and communicate backoff; avoid retries on 429 without delay.
- Configuration propagation: version policies; define acceptable stale-policy window.

## 8. Observability and tests
Metrics: decision latency p99, allow/deny rate, Redis error rate, quota config age, hot-key distribution, backend protection effectiveness. Test concurrent bursts, window boundaries, clock skew, region isolation, and Redis failover.

## 9. Spoken interview wrap-up
"I'd enforce token buckets near the gateway with atomic counters in a sharded low-latency store. The main choice is burst fairness versus exact global limits. I'd start with per-region quotas and explicit failure policies, then add coordinated budgets only if product requirements justify cross-region consistency. I'd monitor p99 decision latency and rejected traffic to validate protection."

## Follow-up questions
How do you handle weighted requests? How would you support premium tiers? What happens with a global 1M/day quota? How do you rate-limit unauthenticated traffic behind NAT?

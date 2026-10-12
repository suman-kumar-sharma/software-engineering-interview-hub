# Distributed Rate Limiter HLD — Hinglish

**Problem:** Public API par per-user/API-key request limit enforce karna hai. Assume 100k requests/s peak, 100 requests/minute per key, burst allowed.

## Requirement clarification
"Should limits be globally exact? Can requests exceed quota slightly during region partitions? Which endpoints fail-open versus fail-closed?"

## Algorithm samjho
- **Fixed window:** Simple, lekin boundary par 2× burst aa sakta hai.
- **Sliding log:** Accurate but memory heavy.
- **Token bucket:** Tokens refill hote rehte hain; request token consume karti hai. Controlled burst support hota hai.

**Why token bucket?** Product ko occasional burst allow karna hai, lekin sustained abuse prevent karna hai.

## Architecture
```text
Client → API Gateway → RateLimiter → Backend
                          ↘ Redis (atomic token update)
                          ↘ Config cache
```

## Critical race
Agar 2 requests same waqt 1 remaining token dekhein, dono allow nahi honi chahiye. **Read-then-write separately unsafe hai.** Redis Lua script/transaction se atomic refill-and-consume operation karo.

## Failure handling
- Redis unavailable: critical write endpoints fail-closed; lower-risk reads policy ke hisaab se fail-open with local emergency limiter.
- Hot API key: local token leasing, bounded overshoot.
- Multi-region: Independent quotas fast hain but global overshoot allow karte hain; strict global enforcement latency aur availability cost badhata hai.
- Retry storm: 429 ke saath backoff guidance.

## Interview mein English answer
"I'd enforce token buckets at the gateway with atomic updates in a sharded low-latency store. I would document whether limits are regional or globally coordinated and define fail-open or fail-closed behavior per endpoint. The core trade-off is low latency and availability versus exact global quota enforcement."

**Deep-dive English reference:** [Rate Limiter](../../hld/rate-limiter.md)

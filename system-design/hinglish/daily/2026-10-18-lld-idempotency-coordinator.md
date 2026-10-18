# Idempotency Coordinator — Hinglish LLD (18 October 2026)

Socho ek customer ne same request do baar bhej di. Backend ko dono baar actual operation execute nahi karna chahiye. Isliye hum request key par ek hi owner choose karte hain.

## Step-by-step
1. `ConcurrentHashMap.putIfAbsent` se ek thread owner banta hai.
2. Baaki threads usi key ka `CompletableFuture` wait karte hain.
3. Owner complete kare toh sabko same result milta hai.
4. Different request fingerprint same key par aaye toh conflict.
5. Successful result ke baad TTL start hota hai.
6. Expired entry conditional remove hoti hai, nayi entry accidentally delete nahi hoti.

## Interview twist
Maan lo service restart ho gayi. In-memory map lost! Isliye multi-instance production use mein database-backed durable idempotency, unique constraint aur downstream coordination required hai.

**Java 17 complete implementation, complexity, tests and dry run:** [English detailed LLD](../../daily/2026-10-18-lld-idempotency-coordinator.md).

**Interview English:** "I use atomic ownership, a shared future and conditional expiry to coalesce concurrent requests in one JVM."

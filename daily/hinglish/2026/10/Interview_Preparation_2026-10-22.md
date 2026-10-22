# Interview Preparation — 22 October 2026 (Hinglish)
**Focus:** Product-company SDE2/Senior Backend; Java 17+, Spring, Kafka, PostgreSQL, Redis, AWS.

1. **Parallel API fan-out:** Independent calls ko bounded executor par CompletableFuture se parallel chalao. Deadline aur HTTP timeout set karo. Cancel karne se remote call zaroori nahi rukti.
2. **volatile:** Visibility deta hai, atomicity nahi. count++ mein read aur write alag hain, isliye update lost ho sakta hai. Standalone counter ke liye AtomicInteger; compound invariant ke liye lock ya conditional DB update.
3. **Kafka redelivery:** DB commit ke baad offset se pehle crash hua toh message dobara aayega. Same DB transaction mein eventId unique inbox table mein insert karo aur business mutation karo; DB commit ke baad offset commit.
4. **Outbox:** Business row aur outbox event ek transaction mein. Relay publish ke baad crash kare toh duplicate publish ho sakta hai; consumer idempotent hona chahiye.
5. **Keyset pagination:** Cursor (created_at, order_id) aur matching composite index. order_id timestamp tie ko break karta hai. EXPLAIN ANALYZE se plan verify karo.
6. **Redis stampede:** Local single-flight, TTL jitter, acceptable ho toh stale-while-revalidate, aur bounded DB concurrency. Distributed lease correctness lock nahi.
7. **DSA:** Longest unique substring ke liye sliding window + last-seen index; expected O(n). Java char UTF-16 unit hai, Unicode code point se alag ho sakta hai.
8. **AWS Kafka OAuth:** 9 Oct 2026 ko self-managed Kafka Lambda event source mappings ke liye OAuth announce hua. Provider, token rotation, TLS aur least privilege check karo.

## HLD — Webhook Delivery
Outbox → durable stream → fan-out → per-tenant scheduler → bounded HTTPS workers. HMAC signature, backoff+jitter, attempt history aur DLQ replay. Guarantee at-least-once; timeout receiver ke commit ka proof nahi. [HLD](../../../../system-design/hinglish/daily/2026-10-22-hld-reliable-webhook-delivery.md)

## LLD — Idempotency
ConcurrentHashMap ek JVM mein duplicate in-flight work coalesce karta hai, replicas ke across nahi. Durable guarantee ke liye tenant/key unique constraint, request hash aur stored result chahiye. [LLD](../../../../system-design/hinglish/daily/2026-10-22-lld-concurrent-idempotency.md)

## Fresh engineering watch
- PostgreSQL 19 RC 1 (15 Oct): production upgrade se pehle staging compatibility test. https://www.postgresql.org/about/news/
- Java SE 27 released: runtime/patch policy deliberate rakho. https://docs.oracle.com/javase/specs/
- AWS Kafka OAuth: https://aws.amazon.com/about-aws/whats-new/2026/10/aws-Lambda-supports-oauth-kafka-esm/

**Revision:** Outbox duplicate kyun kar sakta hai? Volatile atomic kyun nahi? Keyset cursor mein order_id kyun? Webhook receiver retry ko kaise dedupe kare?

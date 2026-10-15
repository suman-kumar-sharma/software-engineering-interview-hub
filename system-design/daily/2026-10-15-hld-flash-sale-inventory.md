# HLD Interview — Flash-sale inventory reservation (2026-10-15)

**Level:** Product-company SDE2/Senior. **Format:** 45-minute whiteboard interview. Original design exercise, not an employer-attributed question.

## 1. Clarify requirements (minutes 0–5)

Ask: Is stock per SKU or per warehouse? Can one user reserve multiple units? What is reservation TTL? Is payment synchronous? Can displayed stock be stale? Assume one hot SKU with 10,000 units, reservation TTL five minutes, payment external, and displayed stock may lag but overselling is forbidden.

**Functional:** Create reservation with idempotency key, retrieve status, confirm after payment, cancel, expire, reconcile unknown payment outcomes. **Nonfunctional:** Strong stock correctness, p99 checkout response under 300 ms when admitted, graceful overload, auditability and disaster recovery.

## 2. Back-of-envelope (minutes 5–8)

One million attempts in one minute = approximately 16,667 attempts/second. At most 10,000 can reserve one unit each. If every request locks the same inventory row, the database will become a hotspot. Most attempts must be cheaply rejected, queued or shaped. Separate **admission throughput** from **successful reservation throughput**.

## 3. Architecture (minutes 8–16)

```text
Clients
  |
API Gateway / Auth / Per-user rate limit
  |
Per-SKU admission controller
  |
Reservation Service --------> Redis (display-only availability hint)
  |
PostgreSQL primary: inventory, reservations, outbox
  |                           |
  |                           +--> Outbox publisher --> Kafka
  |                                                  |
  +<-- reconciliation <------ Payment/Order workers --+
  |
Expiry worker: conditional state transition + stock release
```

**Why PostgreSQL primary?** One transaction can atomically update stock and create reservation. Redis read cache can be stale; never use it as sole authority for last-unit allocation. Kafka provides asynchronous delivery of committed state changes, but does not make DB and payment gateway one atomic transaction.

## 4. API and data model (minutes 16–24)

```http
POST /v1/reservations
Idempotency-Key: client-generated-uuid
{"sku":"SHOE-42","quantity":1}

201 Created
{"reservationId":"r-123","status":"PENDING","expiresAt":"..."}
GET /v1/reservations/r-123
POST /v1/reservations/r-123/cancel
```

Payment confirmation is accepted only from authenticated internal/PSP integration, not arbitrary clients.

```sql
CREATE TABLE inventory (
 sku TEXT PRIMARY KEY,
 on_hand BIGINT NOT NULL CHECK(on_hand>=0),
 reserved BIGINT NOT NULL DEFAULT 0 CHECK(reserved>=0),
 sold BIGINT NOT NULL DEFAULT 0 CHECK(sold>=0),
 CHECK(reserved+sold<=on_hand)
);
CREATE TABLE reservations (
 id UUID PRIMARY KEY,
 sku TEXT NOT NULL REFERENCES inventory(sku),
 user_id TEXT NOT NULL,
 status TEXT NOT NULL,
 expires_at TIMESTAMPTZ NOT NULL,
 idempotency_key TEXT NOT NULL UNIQUE,
 request_hash TEXT NOT NULL,
 payment_id TEXT UNIQUE
);
CREATE TABLE outbox (
 id UUID PRIMARY KEY, aggregate_id UUID NOT NULL,
 event_type TEXT NOT NULL, payload JSONB NOT NULL,
 published_at TIMESTAMPTZ
);
```

**Invariant:** `reserved + sold <= on_hand` always. Idempotency key maps to one request payload; conflicting reuse returns 409.

## 5. State transitions and transactions (minutes 24–31)

**Reserve:** In one DB transaction, check idempotency, conditionally update stock, insert PENDING reservation and outbox row. Conditional SQL:

```sql
UPDATE inventory
SET reserved=reserved+1
WHERE sku=:sku AND on_hand-sold-reserved>=1;
```

Exactly one row updated means stock allocated. A zero-row update means sold out. For duplicate idempotency keys, handle unique-key conflict by rolling back and reading the previously committed result; verify payload hash.

**Confirm:** Only after verified payment success, atomically transition `PENDING -> CONFIRMED`, decrement reserved and increment sold, add outbox event. **Expire/cancel:** Only `PENDING -> EXPIRED/CANCELLED` may release reserved stock. All transitions must be conditional and transactional.

## 6. Failure scenarios (minutes 31–38)

1. **Two buyers race for final unit:** Conditional update serializes row change; only one succeeds.
2. **Expiry races with payment success:** Conditional PENDING transition decides winner; loser reconciles. Never adjust stock twice.
3. **DB commits but Kafka unavailable:** Outbox row survives; publisher retries. Consumer deduplicates by event ID.
4. **PSP timeout:** Treat outcome UNKNOWN; query status with stable idempotency key before retry/refund.
5. **Hot SKU overload:** Per-SKU admission shaping and queueing; optionally partition stock into conserved allocation buckets only with rigorous reconciliation.
6. **Replica lag:** Read replicas can show stale stock, but final allocation always hits primary.

## 7. Reliability, security and tests (minutes 38–43)

Monitor invariant violations (target zero), p95/p99, DB lock wait, deadlocks, outbox age, payment UNKNOWN states, reservation expiry lag, per-SKU queue depth. Rate-limit bots, authorize user/tenant access, protect payment callbacks and audit sensitive transitions.

Tests: concurrent last-unit requests with real PostgreSQL; duplicate idempotency keys; PSP callback delivered twice; crash after commit/before publish; expiry-vs-confirm race; DB failover; backlog and load tests at 16k+ attempts/sec. DR snapshot restore requires storage hydration checks before cutover.

## 8. Interview close (minutes 43–45)

“I would first guarantee that stock cannot be oversold using conditional database updates and transactional reservation states. I would use outbox for reliable events, idempotency and reconciliation for payments, and scale admission separately. If a single inventory row becomes the bottleneck, I would benchmark and then consider allocation buckets with explicit conservation and reconciliation.”

**Trade-off:** Single-row SQL is simpler and safer but limits hot-SKU throughput. Sharded allocation scales but adds rebalancing and reconciliation complexity. Redis-only allocation is fast but requires a defensible consistency model and failure recovery.

**Related:** [October 15 Hinglish classroom](../../daily/hinglish/2026/10/Interview_Preparation_2026-10-15.md).

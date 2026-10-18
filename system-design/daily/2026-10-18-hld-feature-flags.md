# HLD Interview: Multi-Region Feature Flag Evaluation Platform
**18 October 2026 · Product-company SDE2/Senior · 45-minute interview**

## 1. Problem and assumptions (0–5 minutes)
Design a feature-flag platform like a managed configuration service. Product teams create boolean, percentage, user-segment and kill-switch flags. SDKs in Java/Node/mobile evaluate flags on the request path. Assume 20,000 tenants, 200 flags per tenant, 200,000 SDK instances globally, peak 2 million evaluations/second, and fewer than 100 writes/second. These are **interview assumptions**, not measured vendor statistics.

**Clarify:** Should evaluation work without network? Yes: local SDK evaluation using a cached signed snapshot. Target p99 < 1 ms for local evaluation (design goal), config propagation p99 < 10 s in healthy regions, 99.99% control-plane availability target. Emergency kill switch prioritizes rapid propagation; no false promise of instantaneous global consistency. Strong consistency for authoring/version allocation; eventual consistency for read replicas and SDKs.

### Functional
- Create/update/archive flag, environments (dev/stage/prod), percentage rollout, segment rules, schedule.
- Evaluate by tenant + environment + flag + user attributes; deterministic result.
- Publish revisions, audit who changed what, rollback, watch revisions, metrics.
- Tenant isolation, authentication, quotas, approvals for production.

### Non-functional
- Evaluation remains available during control-plane outages.
- Bounded staleness in healthy network, safe fallback when offline.
- Low overhead and predictable hashing, encryption in transit, RBAC.
- No user PII in analytics unless explicitly authorized and minimized.

## 2. Architecture and critical path (5–15 minutes)

```text
 Admin UI -> Auth/RBAC -> Flag API -> Primary DB (flags, versions, audit)
                             |              |
                             +--> Outbox --> Kafka/EventBus
                                              |
                                      Regional config publishers
                                      /        |         \
                               Region A    Region B    Region C
                                  |            |           |
                              CDN/object store: signed snapshots
                                  |            |           |
                            SDK watch/poll + local atomic snapshot
                                          |
                       evaluate(flag, context) in application process
                                          |
                               sampled exposure events -> pipeline
```

**Control plane** handles writes and approvals. **Data plane** is SDK local evaluation; it must not call the API for every request. This avoids making feature flags a new global availability dependency. A revisioned snapshot is compiled and distributed through regional fan-out and a polling fallback. SDK swaps an immutable snapshot atomically, so no request sees half-applied rules.

## 3. Data and APIs (15–22 minutes)
```sql
CREATE TABLE flag_definition (
 tenant_id UUID NOT NULL, environment TEXT NOT NULL,
 flag_key TEXT NOT NULL, revision BIGINT NOT NULL,
 rules_json JSONB NOT NULL, default_json JSONB NOT NULL,
 updated_at TIMESTAMPTZ NOT NULL,
 PRIMARY KEY (tenant_id, environment, flag_key)
);
CREATE TABLE flag_audit (
 tenant_id UUID NOT NULL, flag_key TEXT NOT NULL,
 revision BIGINT NOT NULL, actor_id UUID NOT NULL,
 change_json JSONB NOT NULL, created_at TIMESTAMPTZ NOT NULL
);
-- Outbox event inserted in same DB transaction as definition update.
```

`PUT /v1/tenants/{tenant}/environments/{env}/flags/{key}` with `If-Match: revision` prevents lost updates; return 409 on conflict. `POST /flags/{key}/publish` records approval + immutable revision. `GET /sdk/snapshots?since=rev` returns signed snapshot or delta with ETag; `GET /sdk/stream` optionally streams invalidation hints. SDK uses last-known-good if unavailable. Separate control-plane credentials from read-only environment-scoped SDK tokens.

## 4. Deterministic percentage rollout
For a user-scoped 10% rollout, compute a stable cryptographic hash of `tenantId|flagKey|rolloutSalt|stableUserId`, map to bucket `0..9999`, and enable for bucket < 1000. Never use Java's mutable random generator per request: the same user must receive the same variant across replicas. Use a published canonical algorithm across all language SDKs, test with golden vectors, and version it before changing. Rule priority: explicit deny / kill switch -> allow/deny segments -> percentage -> default. Flag types and rule ordering must be specified and tested.

**Privacy:** Hashing an email does not magically anonymize it; prefer a stable opaque user identifier, restrict exposure logs and define retention.

## 5. Failure modes and trade-offs (22–35 minutes)
| Failure | Design response |
|---|---|
| Kafka/event bus down | Transactional outbox preserves revision; publisher retries; SDK poll fallback |
| One region offline | SDK retains last-known-good snapshot, other regions continue |
| Partial rollout | Immutable versioned snapshot + checksum/signature + atomic swap |
| Control plane DB unavailable | No new writes; existing SDK evaluation continues |
| Invalid rule published | Schema validation, preview, staged rollout, one-click rollback to prior revision |
| SDK cache stale | Version/age metrics, freshness SLO, optional conservative fail-closed per sensitive flag |
| Kill switch delayed | Fast invalidation path plus poll; document propagation window; for critical authorization do **not** rely on flags as the only control |
| Tenant flood | Rate limit write APIs, quotas, partition event fan-out, isolate tenant credentials |
| Compromised config distribution | Signed snapshots, key rotation, scoped tokens, TLS and audit trails |

**Consistency decision:** DB authoring uses optimistic revision compare-and-swap. SDK snapshots are eventually consistent by design. If product requires strictly synchronized global activation, this architecture cannot guarantee it under network partition; explain the availability/consistency trade-off.

**Scaling math:** 2M evaluations/s × remote request would be catastrophic; local SDK evaluation turns that into roughly zero synchronous network calls. With 200k SDKs polling every 30 seconds, average polling load ≈ 6.7k requests/s before jitter; add randomized jitter, ETag/304, CDN caching and delta distribution. 20k tenants × 200 flags = 4M flag definitions; partition storage and avoid per-SDK full snapshots.

## 6. Reliability, observability and security
Metrics: config publish latency p50/p99, revision lag by region, SDK snapshot age, invalid signatures, evaluator p99, rollout exposure distribution, error/fallback rates. Trace control-plane changes using revision IDs, not raw user attributes. Alerts for stuck outbox, stale SDK fleet, abnormal rule exposure. Threat model includes cross-tenant reads, SDK key theft, malicious rollout, replayed old snapshot and analytics leakage.

Test plan: golden vectors across Java/JS, property tests for 0/100% rollout, concurrent revision updates, offline SDK, corrupted signature, rollback, 10x polling spike, region partition and outbox replay. Chaos-test stale config and demonstrate safe defaults.

## 7. Interview narrative and follow-ups (35–45 minutes)
**Say:** "I split control plane from local evaluation data plane. Authoring is revisioned and auditable; regional distribution is eventually consistent; SDKs atomically swap signed immutable snapshots. This removes synchronous network dependency from every application request. I measure revision lag and explicitly document the kill-switch propagation limit."

**Follow-up 1: Why not Redis GET on every evaluation?** Even a fast Redis adds latency, network failure modes and 2M QPS cost; Redis may accelerate distribution, but not the hot path.

**Follow-up 2: Can we guarantee kill switch reaches everyone in 1 second?** No under partitions/offline clients. Give a measured SLO and separate truly safety-critical authorization from rollout flags.

**Follow-up 3: How do you prevent a rollout from changing assignments?** Stable bucket algorithm, immutable salt, flag-specific identity key, golden vectors, versioned migration.

**Follow-up 4: What is the consistency boundary?** Revision allocation and outbox atomic in primary DB; region/SDK propagation asynchronous.

**Further reading (official):** https://docs.aws.amazon.com/prescriptive-guidance/latest/cloud-design-patterns/transactional-outbox.html ; https://kafka.apache.org/documentation/ ; https://docs.aws.amazon.com/AmazonCloudFront/latest/DeveloperGuide/Expiration.html

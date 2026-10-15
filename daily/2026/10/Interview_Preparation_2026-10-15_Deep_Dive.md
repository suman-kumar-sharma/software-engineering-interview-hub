# October 15, 2026 — Advanced Backend Deep Dives (English)

Companion to [today's main report](Interview_Preparation_2026-10-15.md). Original product-company SDE2/Senior practice problems; no fabricated employer attribution.

## Q3 — Kafka rebalance and zombie consumer (Hard)

Consumer A owns partition P and pauses during a long GC event. B gets P and persists account version 42. A resumes and tries version 41. Rebalance ownership does not automatically prevent A from writing to an external database.

**Invariant:** Old state must not overwrite new state. Key events by account ID to preserve per-partition order. Persist event-ID deduplication and enforce a version check at the state store.

```sql
UPDATE account_projection
SET balance=:balance, version=:incoming
WHERE account_id=:id AND version<:incoming;
```

**Dry run:** B writes 42; A's 41 update affects zero rows. If every version must be applied, require `version=:incoming-1` and buffer/retry gaps. Kafka consumer-group generation is not automatically a database fencing token. Stop fetching and drain/cancel in-flight tasks on revocation; commit offsets only after durable effects. Duplicates can still happen after crashes.

**Follow-ups:** Kafka ordering is per partition, not global. Poison-event retry may violate per-key order unless designed carefully. **Complexity:** O(1) application processing plus indexed database operations. **Interview:** “I fence zombie consumers at the database using persisted versions and idempotency.”

Source: https://kafka.apache.org/documentation/

## Q4 — Spring transaction propagation and independent audit (Hard)

**Prompt:** `placeOrder()` calls `this.audit()`, then throws. Why does the audit row roll back even with `REQUIRES_NEW`?

**Root cause:** Spring default proxy-based `@Transactional` advice is bypassed by self-invocation. A method annotation alone does not force a new transaction when the call never crosses the proxy.

```java
@Service
class OrderService {
    private final AuditService audit;
    OrderService(AuditService audit) { this.audit=audit; }

    @Transactional
    public void placeOrder() {
        // save order
        audit.writeAttempt("ORDER_ATTEMPT");
        throw new IllegalStateException("reject");
    }
}
@Service
class AuditService {
    @Transactional(propagation=Propagation.REQUIRES_NEW)
    public void writeAttempt(String action) { /* persist audit */ }
}
```

**Derivation:** Proxy intercepts `AuditService.writeAttempt`, suspends outer transaction, starts independent physical transaction, commits audit, resumes outer. Outer exception rolls back order only. **Caveat:** Independent transaction may need a second connection; size Hikari pool accordingly. Default rollback for unchecked exceptions differs from checked exceptions. For events that must commit *with* the order, use an outbox instead.

**Follow-up:** Audit failure policy: fail order or degrade? Decide from compliance requirements. **Interview:** “I use a proxied collaborator for REQUIRES_NEW and verify commit boundaries in integration tests.”

Source: https://docs.spring.io/spring-framework/reference/data-access/transaction/declarative/tx-propagation.html

## Q5 — Redis distributed locks and fencing (Hard)

**Scenario:** Worker A holds a 30-second lock but pauses for 40 seconds. B acquires expired lock and updates inventory. A resumes. Both can write even if Redis locking itself worked as configured.

**Reasoning:** Lease expiry does not halt the old worker. Use storage-enforced monotonically increasing fencing tokens, e.g. A token 7, B token 8. Once DB accepted 8, it rejects 7 atomically. A random UUID identifies an owner but is not an ordered fencing token. A Redis INCR-based fence needs explicit consideration of failover and durability assumptions; do not claim it is always a linearizable authority.

**Additional safeguards:** Release lock with compare-and-delete Lua script so A cannot delete B's lock. Enforce stock constraints in the database. For external payment APIs that cannot validate fencing tokens, use durable idempotency keys and reconciliation.

**Complexity:** One lock acquisition and one conditional write; network round trips dominate. **Interview:** “Leases reduce concurrent entry but correctness requires a fence or an idempotent transactional operation.”

Source: https://redis.io/docs/latest/develop/use/patterns/distributed-locks/

## Q6 — Shortest subarray sum >= K, negative numbers (Hard DSA)

**Prompt:** Return the length of the shortest nonempty contiguous subarray whose sum is at least K, or -1. Negatives allowed.

**Why normal sliding window fails:** Removing a negative value can *increase* window sum, violating the monotonicity assumption of the two-pointer method.

**Derivation:** Prefix sums `p[i]`. Sum of [j,i) is `p[i]-p[j]`. We want minimal i-j with difference >= K. Keep candidate prefix indices in an increasing-prefix monotonic deque. Pop valid starts from front and dominated starts from back.

```java
import java.util.*;
public class ShortestSubarray {
    public static int solve(int[] a, long k) {
        long[] p = new long[a.length+1];
        for (int i=0;i<a.length;i++) p[i+1]=p[i]+a[i];
        Deque<Integer> q=new ArrayDeque<>();
        int best=a.length+1;
        for (int i=0;i<p.length;i++) {
            while (!q.isEmpty() && p[i]-p[q.peekFirst()]>=k)
                best=Math.min(best,i-q.removeFirst());
            while (!q.isEmpty() && p[i]<=p[q.peekLast()])
                q.removeLast();
            q.addLast(i);
        }
        return best==a.length+1?-1:best;
    }
    public static void main(String[] args) {
        System.out.println(solve(new int[]{2,-1,2},3)); // 3
        System.out.println(solve(new int[]{1},2));      // -1
        System.out.println(solve(new int[]{84,-37,32,40,95},167)); // 3
    }
}
```

**Proof sketch:** A later prefix <= earlier prefix dominates it as a starting point because it gives at least as large a sum and a shorter subarray for future ends. Each index is pushed and popped at most once. **Complexity:** O(n) time, O(n) space. **Edge cases:** empty array, all negatives, K <= 0, duplicate prefixes, integer overflow (use long).

**Interview:** “Prefix sums and a monotonic deque recover linear time when negative values invalidate the usual sliding-window invariant.”

## Q7 — RDS PostgreSQL latency after snapshot restore (Hard)

**Incident:** RDS instance reports available after snapshot restore, but some queries are 10–15 times slower. AWS documented on **14 October 2026** that EBS blocks can hydrate lazily from snapshots; first-touch reads fetch nonhydrated blocks. This is an observed test range, not a universal factor.

**Runbook:** Check `StorageOperationStatus` and `StorageOperationPercentProgress` in RDS DescribeDBInstances. Use `pg_prewarm` in prefetch mode on hot heap, indexes and TOAST. Refresh statistics with ANALYZE, but VACUUM/ANALYZE do not guarantee full block hydration. Run representative query plans, track read latency and control prewarming I/O before switching DR traffic.

**Follow-up:** Distinguish hydration from planner regression or connection saturation. **Interview:** “I diagnose lazy snapshot initialization, prewarm critical data and verify p95/p99 before cutover.”

Source: https://aws.amazon.com/blogs/database/reducing-storage-initialization-impact-in-amazon-rds-for-postgresql-after-snapshot-restore/

## Q8 — Java memory model and safe singleton publication (Hard)

**Problem:** Double-checked locking with nonvolatile singleton may expose insufficiently published state.

```java
final class Settings {
    private static volatile Settings instance;
    private Settings() {}
    static Settings get() {
        Settings local=instance;
        if(local==null) synchronized(Settings.class) {
            local=instance;
            if(local==null) instance=local=new Settings();
        }
        return local;
    }
}
```

**Reasoning:** A volatile write publishes construction to subsequent volatile reads and establishes the needed happens-before visibility. Volatile does not make `count++` atomic. Simpler alternatives: initialization-on-demand holder and enum singleton. **Follow-up:** Distinguish visibility, ordering and atomicity. **Interview:** “Correct double-checked locking relies on volatile safe publication.”

## Q9 — Refresh-token rotation and replay detection (Hard)

**Scenario:** R1 is exchanged for R2. Attacker later reuses stolen R1. If both remain valid, rotation offers weak replay protection.

**Design:** Store hashed refresh-token IDs and token-family/session metadata. Atomically mark R1 consumed and create R2. A reused consumed token triggers risk policy: revoke family and require reauthentication. Access tokens short-lived; use secure HttpOnly cookies for browser refresh, CSRF defenses, rate limits and device-level logout.

**Race:** Two legitimate refresh requests may arrive concurrently. Use client single-flight and define server replay/grace policy; never silently allow indefinite token reuse. **Interview:** “I atomically rotate refresh tokens and revoke the session family when reuse indicates compromise.”

---
See [the Hinglish classroom edition](../../hinglish/2026/10/Interview_Preparation_2026-10-15.md) for step-by-step explanations.

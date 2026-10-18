# LLD Interview: Concurrent Idempotency Request Coordinator
**18 October 2026 · Java 17 · SDE2/Senior · 45-minute machine coding**

## 1. Clarify requirements
We need a library wrapping `execute(key, requestFingerprint, action)`. Concurrent identical requests must share one in-flight result. Reusing the same key with a different request is rejected. Successful results are cached for a TTL. Failed executions may be retried; waiters for that attempt observe the failure. Memory can be reclaimed with expiration sweeps.

**Scope:** single JVM; thread-safe in-memory coalescing. **Not** durable payment exactly-once processing. External side effects require gateway idempotency, DB unique constraints, a durable state machine, and reconciliation.

## 2. API and invariants
- At most one owner of a key at a time within this JVM, using `putIfAbsent`.
- Same key and same fingerprint: wait for the owner's `CompletableFuture`.
- Same key but different fingerprint: reject with conflict (HTTP 409 in an API).
- TTL starts **after success**, never while work is still running.
- Failed owner completes waiters exceptionally and conditionally removes entry; subsequent call can retry.
- Eviction is conditional on exact entry identity to avoid removing a replacement.

State diagram:

```text
ABSENT --putIfAbsent--> IN_FLIGHT --success--> COMPLETED --TTL--> ABSENT
                            |
                            +--failure--> FAILED / removed --> ABSENT
duplicate same fingerprint -> join in-flight/completed future
duplicate different fingerprint -> conflict
```

## 3. Java 17 implementation (complete runnable example)
Save as `IdempotencyDemo.java`:

```java
import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

public class IdempotencyDemo {
  public static final class Coordinator<V> {
    private static final class Entry<V> {
      final String fingerprint;
      final CompletableFuture<V> result = new CompletableFuture<>();
      volatile long expiresAtNanos = Long.MAX_VALUE;
      Entry(String fingerprint) { this.fingerprint = fingerprint; }
      boolean expired(long now) {
        return result.isDone() && expiresAtNanos - now <= 0;
      }
    }

    private final ConcurrentHashMap<String, Entry<V>> entries = new ConcurrentHashMap<>();
    private final long ttlNanos;

    public Coordinator(Duration ttl) {
      if (ttl.isNegative() || ttl.isZero() || ttl.compareTo(Duration.ofDays(1)) > 0)
        throw new IllegalArgumentException("TTL must be in (0, 1 day]");
      this.ttlNanos = ttl.toNanos();
    }

    public V execute(String key, String fingerprint, Supplier<V> action) {
      Objects.requireNonNull(key);
      Objects.requireNonNull(fingerprint);
      Objects.requireNonNull(action);

      while (true) {
        Entry<V> existing = entries.get(key);
        if (existing != null) {
          if (!existing.fingerprint.equals(fingerprint))
            throw new IllegalArgumentException("Idempotency key reused with different request");
          if (existing.expired(System.nanoTime())) {
            entries.remove(key, existing);
            continue;
          }
          return existing.result.join();
        }

        Entry<V> mine = new Entry<>(fingerprint);
        if (entries.putIfAbsent(key, mine) != null) continue;

        try {
          V value = action.get();
          mine.expiresAtNanos = System.nanoTime() + ttlNanos;
          mine.result.complete(value);
          return value;
        } catch (Throwable failure) {
          mine.result.completeExceptionally(failure);
          entries.remove(key, mine);
          throw failure;
        }
      }
    }

    public int purgeExpired() {
      long now = System.nanoTime();
      int removed = 0;
      for (var e : entries.entrySet())
        if (e.getValue().expired(now) && entries.remove(e.getKey(), e.getValue()))
          removed++;
      return removed;
    }
  }

  public static void main(String[] args) throws Exception {
    Coordinator<String> coordinator = new Coordinator<>(Duration.ofMinutes(5));
    AtomicInteger calls = new AtomicInteger();
    ExecutorService pool = Executors.newFixedThreadPool(8);
    try {
      var futures = new java.util.ArrayList<Future<String>>();
      for (int i = 0; i < 20; i++) {
        futures.add(pool.submit(() ->
          coordinator.execute("payment-1", "body-hash", () -> {
            calls.incrementAndGet();
            try { Thread.sleep(20); }
            catch (InterruptedException e) { Thread.currentThread().interrupt(); }
            return "charged";
          })));
      }
      for (var future : futures)
        if (!"charged".equals(future.get())) throw new AssertionError();
      if (calls.get() != 1) throw new AssertionError("calls=" + calls.get());
      System.out.println("same-key concurrent executions=" + calls.get());
    } finally { pool.shutdownNow(); }
  }
}
```

**Verified locally:** `javac --release 17 IdempotencyDemo.java` and `java IdempotencyDemo` print `same-key concurrent executions=1` on OpenJDK 21 targeting Java 17. This is a concurrency smoke test, not a proof of all interleavings.

## 4. Walkthrough of the critical race
T1 and T2 arrive with the same key. Both read `null`. T1 wins `putIfAbsent` and executes action. T2 loses and loops; it now finds T1's entry and joins its future. If T1 succeeds, both receive the same result. If T1 fails, T2 sees that failure; T3 can later claim a new entry.

**Expiration race:** A sweeper sees an expired completed entry while another thread replaces it. `remove(key, expectedEntry)` ensures the sweeper cannot delete the new entry.

**Why not check-then-put?** Both threads could observe absence and perform the external side effect twice. The atomic map insertion is the linearization point for ownership.

## 5. Complexity and trade-offs
Expected O(1) map access per request. Waiting time equals the owner's execution time; `purgeExpired()` is O(n) in current entries. Space O(k) unique unexpired keys. The implementation is **unbounded** and needs a maximum-entry policy for production; simply evicting in-flight entries would violate single-owner semantics. Set a hard admission limit or tenant quotas and reject excess requests. Add owner timeouts and a policy for abandoned in-flight entries; do not casually rerun an action with unknown external outcome.

**Fingerprint:** Canonicalize method/path/body/tenant, then hash with SHA-256. A key should be scoped by tenant and operation. This sample expects a precomputed fingerprint and does not implement cryptographic canonicalization.

**Failure semantics:** Failed attempt is removed, so retry is allowed. If the action performed a payment and then threw because the response was lost, retry could duplicate the payment. Production payments must use a **durable** idempotency record and downstream idempotency token; do not claim this class solves exactly-once charging.

## 6. Tests
1. 100 same-key concurrent callers: action counter equals one, all values equal.
2. Different fingerprints same key: one accepted, one rejected.
3. Failure: all concurrent waiters observe exception; later retry succeeds.
4. TTL: completed entry reused before expiry; re-executed after expiry.
5. Sweep versus replace: new entry is never removed by stale sweep.
6. Slow action: TTL does not expire while in-flight.
7. Null result and action exception handling.
8. Multi-instance deployment: demonstrate why separate JVM maps cannot coordinate.

## 7. Interviewer follow-ups
**Q: What happens on JVM crash?** All state is lost. Use PostgreSQL unique `(tenant_id, operation, key)`, status `IN_PROGRESS/SUCCEEDED/FAILED`, request hash, durable response reference and a lease/claim policy. Reconcile unknown outcomes before retrying external effects.

**Q: How do you prevent memory blowup?** Admission quotas, max key cardinality, TTL sweep and metrics; never evict active operations silently.

**Q: What if client changes request body with same key?** Reject 409, preserving original key semantics.

**Q: Why CompletableFuture?** It lets concurrent callers wait for the same result without executing the action again.

**Spoken summary:** "I use atomic ownership per idempotency key, a shared future for duplicate requests, a request fingerprint for conflict detection, and conditional TTL eviction. This guarantees in-process coalescing only; durable external effects need database and downstream idempotency."

**Official API references:** https://docs.oracle.com/en/java/javase/17/docs/api/java.base/java/util/concurrent/ConcurrentHashMap.html ; https://docs.oracle.com/en/java/javase/17/docs/api/java.base/java/util/concurrent/CompletableFuture.html

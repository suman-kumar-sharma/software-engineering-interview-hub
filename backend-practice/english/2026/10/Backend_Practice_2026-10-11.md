# Safe Configuration Reloads — Java 21

Original session: 2026-10-11. Archived: 2026-10-13.

## Question and constraints

A backend reloads timeout and retry settings while requests execute. Readers must not acquire a lock, must use one consistent configuration version, and invalid updates must preserve the previous configuration. Concurrent reloaders must not overwrite a newer version with an older one. Versions come from an authoritative monotonically increasing source.

## Concept answer

Two volatile fields provide individual visibility, not an atomic pair. A reader can run after the timeout assignment but before the retry assignment and see mixed settings. Group related values into one immutable snapshot, publish its reference safely, and read that reference once per operation.

## Deriving the solution

1. Related values must change together: put them in one object.
2. Readers retain snapshots: keep objects immutable rather than modifying them.
3. Readers need visibility: use volatile publication semantics.
4. Writers may race: compare the version and use compare-and-set (CAS), rather than a separate check and unconditional set.
5. CAS can fail after a concurrent update: reread and revalidate the version in a loop.
6. Invalid input must never become current: validate during construction before installation.

## Complete implementation

```java
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;

record RetryConfig(long version, int timeoutMs, int maxRetries) {
    RetryConfig {
        if (version < 0) throw new IllegalArgumentException("Negative version");
        if (timeoutMs < 1 || timeoutMs > 30_000)
            throw new IllegalArgumentException("Invalid timeout");
        if (maxRetries < 0 || maxRetries > 5)
            throw new IllegalArgumentException("Invalid retry count");
    }
}

final class ConfigStore {
    private final AtomicReference<RetryConfig> current =
        new AtomicReference<>(new RetryConfig(0, 500, 2));

    RetryConfig snapshot() { return current.get(); }

    boolean install(RetryConfig candidate) {
        Objects.requireNonNull(candidate);
        while (true) {
            RetryConfig existing = current.get();
            if (candidate.version() <= existing.version()) return false;
            if (current.compareAndSet(existing, candidate)) return true;
        }
    }
}
```

Usage: capture `RetryConfig config = store.snapshot();` once and use both `config.timeoutMs()` and `config.maxRetries()` throughout the operation. Never read the holder twice for related settings.

## Race walkthrough

Current version is 10. A and B both read its reference. A installs version 12. B attempts CAS from the old reference to version 11 and fails. B rereads version 12 and rejects version 11. If B had used `set()` after its initial version check, it could have overwritten 12 with 11.

CAS compares reference identity, not record equality. The successful CAS is the installation's linearization point. `get()` has volatile-read semantics; CAS provides atomic conditional replacement and its specified memory effects.

## Correctness and limitations

- Readers see one complete immutable snapshot; an in-flight request may retain an older snapshot.
- Successful installations strictly increase the version. Equal versions are rejected; conflicting payloads at an equal version should be monitored separately.
- The holder itself must be safely published, for example through normal managed bean construction without leaking `this` during construction.
- Primitive record fields are immutable. Nested mutable collections require defensive copying and immutable elements.
- No database/network operation belongs inside the CAS loop.
- A single ordered writer can use a volatile snapshot. Multiple writers requiring conditional version ordering benefit from CAS.
- This coordinates one JVM only. Cross-replica rollout and version agreement require a separate distributed configuration protocol.
- Reader work is O(1). One installation attempt is O(1), but retries under contention have no fixed per-thread bound; this does not imply wait-free writes.

## Hinglish: solution tak kaise pahunche

Socho timeout aur retries ek settings packet hain. Agar dono alag-alag badloge, request beech mein aakar aadha purana aur aadha naya packet le sakti hai. Isliye pehla decision: dono ko ek immutable object mein rakho.

Ab object ko modify karne ki jagah naya packet banao aur reference replace karo. Request ek baar packet pakad legi, toh reload ke baad bhi uska packet consistent rahega.

Ab problem kya hai? Do reloaders current version 10 padhte hain. Ek version 12 lagata hai, dusra thodi der baad version 11 laga deta hai. Sirf pehle version check karna enough nahi: check aur update ke beech state badal gayi.

CAS ka matlab hai: 'Jo reference maine padha tha, current abhi bhi wahi hai toh mera update lagao.' Agar koi aur update jeet gaya, CAS fail hota hai. Phir latest version padho aur decide karo candidate abhi bhi newer hai ya nahi. Isi wajah se loop required hai.

Validation pehle karo, publication baad mein. Invalid packet construct hi nahi hoga, toh working configuration untouched rahegi.

## Interview answer

“I separate visibility from compound-state consistency. I publish immutable configuration snapshots through an atomic reference and capture one snapshot per request. A version check plus CAS prevents stale concurrent reloads from replacing newer state. Validation occurs before publication. This solves local concurrency; distributed rollout remains a separate concern.”

## Checks and follow-ups with answers

- Invalid timeout: construction throws; current snapshot remains unchanged.
- Older/equal version: install returns false.
- Newer version: install succeeds unless superseded by an even newer update.
- Concurrent versions 11 and 12: after both attempts complete, final version is 12.
- Why not synchronize everything? Correct locking can work, but would violate the lock-free-reader requirement.
- Why not `volatile` only? It publishes snapshots safely but lacks atomic conditional replacement for concurrent writers.
- Does adding sleep fix visibility? No; sleep does not establish synchronization semantics.

## Primary sources

- [Java 21 JLS happens-before rules](https://docs.oracle.com/javase/specs/jls/se21/html/jls-17.html#jls-17.4.5)
- [Java 21 AtomicReference API](https://docs.oracle.com/en/java/javase/21/docs/api/java.base/java/util/concurrent/atomic/AtomicReference.html)

This archive contains reasoning checks, not a claim that a concurrency stress test was executed.

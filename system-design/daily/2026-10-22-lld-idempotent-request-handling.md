# LLD: Idempotent Request Handling
**22 October 2026 | SDE2 / Senior Backend**

## Contract
Request identity is tenant ID + client-supplied key. Store a hash of canonical semantic fields. Same key and same hash returns the existing result; same key with a different hash returns conflict. The same key in another tenant is independent.

## Components
1. Request canonicalizer computes a stable hash.
2. Local in-flight registry coalesces same-key work in one JVM.
3. Durable repository has a primary key on tenant + request key, request hash, status and stored response.
4. Bounded executor performs blocking work.
5. Recovery process identifies abandoned IN_PROGRESS rows.

## Algorithm
Validate tenant/key, canonicalize request and attempt to insert IN_PROGRESS. The winner performs the business mutation and stores the result in the same database transaction where possible. If the key already exists, compare hashes: mismatch is conflict; a matching completed row returns its stored response. For long-running workflows, use conditional state transitions and recovery for abandoned work. Retain records for the documented client retry horizon.

## Concurrency and correctness
A local map can use put-if-absent to install one shared future; duplicate callers await it. Remove the exact future conditionally so an old completion cannot remove a newer entry. This only coalesces work within one JVM; restarts and other replicas are outside that guarantee. A caller timeout must not cancel shared work for every waiter.

## Tests and trade-offs
Test concurrent duplicate callers, different-payload key reuse, same key across tenants, operation failure, two replicas racing on the database uniqueness constraint, and a crash after an external side effect. Database uniqueness costs a write but provides cross-replica authority. External systems need their own request key or reconciliation; do not promise generic exactly-once effects.

**Interview summary:** Separate local duplicate-work reduction from durable request identity. Persist the request fingerprint and final result, and define retention, recovery and replay behavior.

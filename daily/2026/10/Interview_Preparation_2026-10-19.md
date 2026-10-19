# Interview Preparation — 2026-10-19

Daily Java backend interview preparation.

**Audience:** Product-company SDE2 / Senior Backend. Original practice exercises, not attributed to named employers. Java 17+, HLD, LLD, official references. Research cutoff: 19 Oct 2026.

## Q1 — Safe publication and double-checked locking (Java, Hard)

**Problem:** Implement lazy singleton; why is volatile needed? Without correct publication, another thread may see a reference without guaranteed visibility of initialized fields. Volatile reference establishes happens-before; synchronized protects single initialization.

~~~java
final class Registry {
    private static volatile Registry instance;
    private Registry() {}
    static Registry get() {
        Registry x=instance;
        if(x==null) synchronized(Registry.class) {
            x=instance;
            if(x==null) instance=x=new Registry();
        }
        return x;
    }
}
~~~

**Dry run:** A and B read null; A constructs inside monitor; B sees non-null on recheck. **Complexity:** O(1) time/space per access. **Edges:** constructor escape, exceptions, mutable state. **Follow-up:** Does volatile make ++ atomic? No. **Interview:** “Volatile safely publishes the instance; synchronized ensures a single construction.” Source: https://docs.oracle.com/javase/specs/jls/se17/html/jls-17.html#jls-17.4.5

## Q2 — Kafka rebalance and zombie external writes (Hard)

**Problem:** A loses partition ownership to B, but its old DB write finishes later. Kafka group epochs fence offset commits, not arbitrary external DB writes. Enforce event-ID deduplication and monotonic business versions in the DB.

~~~sql
UPDATE projections SET value=:v, version=:newVersion
WHERE entity_id=:id AND version < :newVersion;
~~~

**Dry run:** B writes version 13, stale A tries 12; affected rows=0. **Complexity:** expected O(1) indexed lookup per event plus write. **Edges:** first insert race, gaps, duplicate replay, offsets after DB commit. **Follow-up:** Why is Kafka exactly-once not external payment exactly-once? **Interview:** “I fence stale business writes with conditional versions, independently of Kafka group ownership.” Source: https://kafka.apache.org/42/operations/consumer-rebalance-protocol/


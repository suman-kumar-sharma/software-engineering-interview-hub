# JVM Diagnostics: Allocation Pressure vs Memory Retention
Date: 2026-10-14 | Java 21 HotSpot, G1 GC | Level: experienced backend engineer

## Learning plan and prerequisites
3 minutes: concept and answer. 8 minutes: incident exercise and worked solution. 4 minutes: code walkthrough, failure cases and interview follow-ups. Understand heap objects, reachability and basic GC. This session follows configuration/JMM practice and adds runtime diagnosis; no mastery is assumed.

## Concept question and answer
**Question:** Heap usage repeatedly rises to 3 GB in a 4 GB heap. Does this prove a memory leak?
**Answer:** No. Allocation consumes heap; unreachable objects can later be collected. The useful signal is the retained/live-set trend after comparable reclamation phases, correlated with traffic and workload. With G1, a young collection does not reclaim all old-generation garbage, so comparing arbitrary post-young-GC points is misleading. A rising retained baseline is evidence of retention pressure, not proof of a defect: legitimate cache warmup, backlog and increased active users can explain it too. Root ownership and lifecycle determine whether retention is unintended.

## Practical question
An order API's p99 increases from 180 ms to 1.8 s. Traffic is approximately stable. Synthetic observations:
- Allocation rate remains around 600 MB/s.
- After comparable old-generation reclamation, retained heap grows from 0.8 to 1.5 to 2.4 GB.
- A heap analysis shows a long-lived OrderCache retaining millions of OrderSnapshot objects.
- The cache is a ConcurrentHashMap keyed by every order ID ever requested, without eviction.
Explain how you diagnose, mitigate, fix and verify this. Provide a small bounded cache without external dependencies.

## Derivation: evidence before tuning
1. p99 is a symptom. Correlate latency with GC pauses/CPU, dependency latency, connection waits and queue sizes. Do not assume GC is the only cause.
2. Stable allocation plus a growing retained baseline shifts attention toward object lifetime and ownership.
3. Class counts identify candidates but not owners. Inspect retained sizes, dominators and paths to GC roots in a heap analyzer.
4. The given root chain (application cache -> map -> snapshots) and unlimited unique keys explain growth. GC cannot reclaim values while this root holds them.
5. Bound retention according to a business need. Here only the most recently accessed 1,000 immutable snapshots are useful; persistence remains in PostgreSQL.
6. Verify the live-set trend and latency under comparable load. More heap buys time; it does not correct unlimited retention.

## Diagnostic commands
```bash
# Run in the target JVM's host/container namespace as the appropriate user.
# 1234 is an example PID; /secure/diagnostics must exist and be writable.
jcmd 1234 help
jcmd 1234 GC.heap_info
jcmd 1234 JFR.start name=memory settings=profile duration=120s filename=/secure/diagnostics/memory.jfr
jcmd 1234 Thread.print -l

# Escalation only: assess pause/disk impact first.
jcmd 1234 GC.class_histogram
jcmd 1234 GC.heap_dump /secure/diagnostics/heap.hprof

# Works only if NMT was enabled at JVM startup.
jcmd 1234 VM.native_memory summary
```

These are instructional commands, not commands executed against a user production service. Inspect command help on the actual JVM first. JFR profile recordings provide sampled allocation/execution and GC evidence depending on enabled events; samples are not an exact census. Inspect the recording in JDK Mission Control. Thread.print is useful for platform-thread stacks/locks but is not a complete virtual-thread inventory.

Start with existing telemetry and a bounded recording, then escalate. Histograms and heap dumps can be disruptive. A default heap dump requests a full GC; avoid casually collecting it on a saturated primary. Check available disk, restrict access and treat recordings/dumps as sensitive because application data may be present. NMT requires startup enablement, e.g. -XX:NativeMemoryTracking=summary; it does not cover all possible native allocations.

## Worked code
```java
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

final class RecentOrderCache<V> {
    private final int capacity;
    private final LinkedHashMap<Long, V> entries =
        new LinkedHashMap<>(16, 0.75f, true);

    RecentOrderCache(int capacity) {
        if (capacity < 1) throw new IllegalArgumentException("capacity");
        this.capacity = capacity;
    }

    synchronized V get(long id) {
        return entries.get(id);
    }

    synchronized void put(long id, V value) {
        Objects.requireNonNull(value);
        entries.put(id, value);
        if (entries.size() > capacity) {
            var iterator = entries.entrySet().iterator();
            iterator.next();
            iterator.remove();
        }
    }

    synchronized int size() {
        return entries.size();
    }
}
```

### Why each choice exists
- LinkedHashMap with accessOrder=true maintains least-to-most-recent access order.
- get changes ordering, so it needs the same synchronization as put.
- put first inserts/replaces; if size exceeds capacity, remove the eldest entry.
- The instance monitor makes update plus eviction atomic relative to other operations. All fields remain private.
- Read-through database/network calls must execute outside the monitor. A cache miss returns null; values are non-null.
- Values should be immutable or independently thread-safe. Synchronizing the map does not protect later external mutations to a returned value.

### Dry run (capacity 2)
put(1,A), put(2,B): order [1,2].
get(1): order [2,1].
put(3,C): insert [2,1,3], evict 2, finish [1,3].
put(1,A2): replace without increasing size; 1 becomes most recent.
No successful public call leaves more than capacity entries, although insertion briefly creates capacity+1 inside the lock.

### Complexity and tradeoffs
Expected amortized O(1) get/put for hash lookup and linked ordering; O(capacity) retained entries. This bounds entry count, not bytes: one huge snapshot can still consume substantial memory. Use a maximum payload size or a cache with weight limits if sizes vary. Synchronized access serializes cache operations and may contend at high throughput. This is a teaching implementation; a mature bounded cache is often preferable for production concurrency, expiry, metrics and loading policies. Do not claim a TTL feature exists here.

## Mitigation, fix and verification
- First protect availability: constrain traffic/backlog, reduce cache retention safely, or roll back the offending change. Scale only after checking downstream limits.
- Avoid an unconditional cache clear across all replicas: a miss storm can overload PostgreSQL.
- Ship bounded retention with hit/miss/eviction metrics. Decide separately on stale-data expiry/invalidation.
- Test capacity rejection, LRU order, replacement, null rejection and concurrent access.
- Run a sustained workload with many unique IDs; confirm size <= limit after operations, stable retained memory after warmup, acceptable cache-lock contention and DB load.
- Compare p99, throughput, errors, allocation, GC CPU/pause totals and retained baseline under matched workload. A cache fix can reduce retention while increasing misses and DB latency.
- This report provides a reference implementation and test plan; it does not claim runtime tests were executed.

## Edge cases and follow-ups with answers
**What if RSS grows but heap stays stable?** Investigate native/direct buffers, thread stacks, metaspace, code cache, mapped files and allocator behavior. Heap tuning alone may not help; compare OS/container metrics and NMT if enabled.
**Is ConcurrentHashMap a bounded cache?** No. Thread safety does not supply capacity, expiry or lifecycle rules.
**Why not WeakHashMap?** Weak keys couple retention to GC reachability, not the desired LRU/business lifetime; it is not a deterministic substitute.
**What if many requests miss the same key?** Add per-key single-flight/loading coordination and bounded admission; do not perform slow loads under the global monitor.
**Why not System.gc()?** It does not remove strongly reachable entries or repair ownership; forced collection may add latency.
**Does a rising baseline always mean a leak?** No. Check intentional growth and queues; prove unexpected retention through ownership and workload comparison.

## Interview answer
“I separate allocation rate from retained live data and correlate latency with GC and dependency telemetry. I use a bounded JFR recording, then heap ownership analysis if needed. In this example an unbounded long-lived map retains every order, so I add a business-appropriate eviction policy, protect the database against miss storms, and verify memory and latency under sustained representative load.”

## Primary sources
- [Java 21 jcmd: commands and impact ratings](https://docs.oracle.com/en/java/javase/21/docs/specs/man/jcmd.html)
- [Java 21 LinkedHashMap: access order and synchronization](https://docs.oracle.com/en/java/javase/21/docs/api/java.base/java/util/LinkedHashMap.html)
- [Java 21 Native Memory Tracking](https://docs.oracle.com/en/java/javase/21/vm/native-memory-tracking.html)

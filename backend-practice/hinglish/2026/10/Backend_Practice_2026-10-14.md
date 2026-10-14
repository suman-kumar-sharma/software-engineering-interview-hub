# JVM Memory Problem Ko Diagnose Kaise Karein
Date: 2026-10-14 | Java 21 HotSpot, G1 GC

## 15-minute plan aur prerequisites
3 minute concept, 8 minute incident ka worked solution, 4 minute code walkthrough aur interviewer twists. Heap, object references aur basic GC pata hona chahiye. Pichhle session mein JMM/configuration dekha tha; aaj runtime par objects kitni der zinda rehte hain, woh samjhenge.

## Question: heap 3 GB pahunch gaya — leak hai?
**Answer:** Zaroori nahi. Socho restaurant ki tables par plates aa rahi hain. Plates aana allocation hai. Khana khatam hone ke baad plates hat sakti hain: objects unreachable hone par GC unhe reclaim kar sakta hai. Sirf peak dekhkar leak declare mat karo.

Asli sawal: comparable cleanup ke baad kitna data bach raha hai? G1 ka har young GC old generation ka saara garbage nahi uthata. Isliye random post-GC numbers compare karna galat diagnosis de sakta hai. Baseline badh rahi hai toh retention investigate karo, lekin genuine cache warmup ya pending requests bhi reason ho sakte hain.

## Practical incident
Order API ka p99 180 ms se 1.8 s ho gaya. Ye synthetic practice observations hain:
- Traffic lagbhag stable hai.
- Allocation around 600 MB/s hai.
- Comparable old-generation cleanup ke baad heap 0.8, phir 1.5, phir 2.4 GB reh raha hai.
- Heap analysis mein OrderCache millions of OrderSnapshot objects retain kar raha hai.
- Cache ConcurrentHashMap hai: har naye order ID ko permanently store karta hai; eviction nahi hai.

Question: diagnosis kaise karoge, fix kaise choose karoge, aur prove kaise karoge ki improvement hui?

## Solution tak pahunchne ka reasoning
**Pehla step: symptom aur cause alag rakho.** Slow API ka matlab automatically GC problem nahi. GC pause, CPU, DB latency, connection wait aur queues ek timeline par dekho.

**Doosra step: allocation vs retention.** Naye objects banne ki speed same hai, lekin cleanup ke baad bacha data badh raha hai. Ab hume object lifetime inspect karna hai.

**Teesra step: owner dhoondo.** Class histogram bolega OrderSnapshot bahut hain. Lekin kaun pakad ke baitha hai? Dominator tree aur GC-root paths dikhate hain: service -> cache -> map -> snapshots. Strong reference hai, isliye GC inhe hata nahi sakta.

**Chautha step: business requirement poochho.** Kya saare historical orders RAM mein chahiye? Nahi: sirf recently used 1,000 snapshots chahiye; permanent record PostgreSQL mein hai. Toh bounded LRU policy reasonable hai.

**Paanchwa step: implementation aur verification.** Sirf heap badha dena growth ko delay karega. Ownership fix karo, phir matched traffic par memory baseline aur p99 verify karo.

## Commands aur unka use
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

Ye practice commands hain; kisi real production JVM par execute nahi kiye gaye. Actual PID aur writable secure directory use karo; actual JVM par help check karo. Same host/container namespace aur appropriate OS user required hote hain.

Pehle existing dashboards aur bounded JFR recording lo. JMC mein allocation samples, GC aur execution evidence dekho; sample ko exact object count mat samjho. Thread.print platform-thread stacks/locks ke liye useful hai, lekin complete virtual-thread inventory nahi.

Histogram/heap dump ko first reflex mat banao: pause aur disk impact ho sakta hai. Default heap dump full GC request karta hai. Dump mein customer/application data ho sakta hai, isliye access restrict karo. NMT tabhi useful hoga jab startup par -XX:NativeMemoryTracking=summary jaise option se enabled tha; woh har native allocation ko track nahi karta.

## Complete worked code
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

## Code ko line-by-line conceptually samjho
LinkedHashMap ka third constructor argument true hai: access order maintain karo. Jo entry abhi access hui woh recent side par chali jaati hai.

Ab important twist: get sirf value read nahi kar raha, ordering bhi modify kar raha hai. Isliye get ko bhi synchronized kiya hai. Put aur eviction same lock ke andar hain, toh doosra thread beech mein inconsistent operation nahi dekhega.

Capacity cross hui toh iterator ki first entry eldest hai; usko hata diya. DB/HTTP call lock ke andar mat karna. Cache miss par null milega; load bahar karo. Returned value immutable ya separately thread-safe honi chahiye: map ka lock value ke future mutation ko protect nahi karta.

## Dry run: capacity 2
1. put(1,A), put(2,B): order [1,2].
2. get(1): ab 1 recently used hai, order [2,1].
3. put(3,C): temporarily [2,1,3], phir 2 evict; final [1,3].
4. put(1,A2): value replace, entry count same, 1 most recent.

Public operation finish hone par size capacity se zyada nahi. Lock ke andar insert ke turant baad temporary capacity+1 ho sakta hai.

## Complexity aur production tradeoffs
Hash lookup aur linked ordering ki wajah se expected amortized O(1) get/put; retained entries O(capacity). Lekin 1,000 entries ka matlab fixed MB limit nahi! Ek snapshot bahut bada ho sakta hai. Variable-size data ke liye payload limit ya weight-based bounded cache consider karo.

Global synchronized lock high traffic par contention la sakta hai. Ye teaching implementation hai; production mein mature cache library often better concurrency, expiry aur metrics deti hai. Is code mein TTL nahi hai, isliye stale data ka rule separately design karna hoga.

## Incident mein practical action
Pehle availability bachao: offending rollout rollback, controlled retention reduction, traffic/backlog limit. Saare replicas ka cache ek saath clear kar doge toh PostgreSQL par miss storm aa sakta hai.

Fix ke baad hit, miss, eviction, cache size, DB QPS aur latency measure karo. Bahut saare unique IDs ke saath sustained load chalao. Warmup ke baad retained baseline stable honi chahiye, lekin p99 bhi check karo: memory bachakar DB ko overload karna successful fix nahi.

Checks: invalid capacity reject, null reject, replacement, exact LRU eviction, concurrent operations aur size bound. Yahan test plan diya hai; runtime tests execute hone ka claim nahi hai.

## Interviewer twists — answers saath mein
**Heap stable, RSS badh raha?** Direct buffers, native allocations, stacks, metaspace, mapped files aur allocator behavior dekho. Sirf -Xmx badalna solution nahi.

**ConcurrentHashMap use kiya tha, phir problem kyun?** Thread safety aur memory lifecycle do alag requirements hain. Map safe concurrent updates deta hai; automatic eviction nahi.

**WeakHashMap laga dein?** GC reachability business LRU requirement nahi hoti. Deterministic capacity policy ka replacement nahi.

**Same missing key par 100 requests?** Per-key single-flight/loading coordination aur admission control chahiye. Slow load global lock ke andar mat chalao.

**System.gc() se fix?** Strongly reachable cache entries reclaim nahi hongi. Extra collection latency badha sakta hai.

**Baseline badhi toh pakka leak?** Nahi. Genuine workload growth, cache warmup aur backlog rule out karo. Ownership aur expected lifecycle se decide karo.

## Professional English interview answer
“I distinguish allocation pressure from retained live data, correlate latency with GC and dependency telemetry, and inspect ownership rather than just object counts. Here an unbounded map retains all order snapshots. I introduce bounded eviction, protect the database from miss storms, and validate memory stability and latency under representative sustained load.”

## Primary references
- [Java 21 jcmd: commands and impact ratings](https://docs.oracle.com/en/java/javase/21/docs/specs/man/jcmd.html)
- [Java 21 LinkedHashMap: access order and synchronization](https://docs.oracle.com/en/java/javase/21/docs/api/java.base/java/util/LinkedHashMap.html)
- [Java 21 Native Memory Tracking](https://docs.oracle.com/en/java/javase/21/vm/native-memory-tracking.html)

# Advanced Java Backend Practice — 2026-10-31
## JVM Diagnostics: Find the Wait Before Tuning the JVM

**Runtime:** Java 21 (HotSpot JDK 21 tools and JFR commands), Spring Boot 3.x / Spring Framework 6.x, PostgreSQL, HikariCP  
**Time budget:** 15 minutes — 3-minute concept, 8-minute practical exercise, 4-minute walkthrough  
**Scenario:** An order API's p99 latency jumps from about 180 ms to 2.2 s. CPU is not saturated and post-GC heap usage is broadly stable. The team suspects the JVM, but the real bottleneck may be a remote call made while a database connection is held.

> Version note: All diagnostic command examples below are based on the JDK 21 `jcmd` and `jfr` documentation. Run the tools from the same JDK/runtime family as the target process where possible. The target JVM's `jcmd <pid> help` and `jfr metadata` are authoritative for the commands and event types actually available in that process. On a JDK 25/26 deployment, verify the local event set and syntax rather than assuming every event/configuration is identical.

## Prerequisites

- Basic JVM heap/GC, Java threads and thread states
- Spring transactions and JDBC connection pools
- HTTP client timeouts and service-to-service calls
- Metrics: latency percentiles, active/pending connections, CPU and GC pauses
- Basic Linux/container process access

## 1. Concept question — 3 minutes

### Question

An order endpoint has changed from p99 = 180 ms to p99 = 2.2 s. Throughput is flat, process CPU is around 30%, and heap usage after GC looks stable. HikariCP reports that active connections are at the configured maximum and pending borrowers are increasing.

**Do you immediately increase the heap, change the garbage collector, or increase the Hikari pool size? What evidence would you collect first?**

### Worked answer

**No tuning change is justified by these facts alone.** Low CPU and a stable post-GC heap make a CPU-saturation or steadily growing Java-heap-retention explanation less likely, but neither is ruled out. The Hikari signals point toward connection-pool pressure; they do not yet prove why connections are being held.

A JVM performance incident needs a wait-state diagnosis, not just a heap graph.

Think in four broad categories:

1. **Running Java code:** CPU samples concentrate in a hot method, serialization loop, regex, encryption, or inefficient algorithm.
2. **Waiting on synchronization or scheduling:** monitor contention, parked threads, executor queueing, or thread-pool starvation.
3. **Waiting on I/O or a dependency:** long socket reads/writes, database acquisition or execution, DNS, remote API, or filesystem waits.
4. **GC/safepoint or memory pressure:** pause durations correlate with the latency spike, allocation/retention grows, or native/container memory is exhausted.

JDK Flight Recorder (JFR) can record thread-stalling events, execution samples and GC activity. Oracle's JDK 21 troubleshooting guide specifically separates monitor waits, socket I/O, code execution and GC as distinct bottleneck classes. See [Troubleshoot Performance Issues Using Flight Recorder](https://docs.oracle.com/en/java/javase/21/troubleshoot/troubleshoot-performance-issues-using-jfr.html).

### Hinglish intuition

Pehle ye mat bolo ki "GC slow hai" ya "heap badha dete hain". CPU low hai aur GC ke baad heap stable hai, toh pehle wait time investigate karo. Hikari ke active connections max par hain aur pending requests badh rahi hain—iska matlab connection lene ke liye requests wait kar rahi hain. Lekin connection itni der tak occupied kyun hai, woh abhi prove nahi hua.

**Rule:** Symptom ko cause mat samjho. Thread, socket, database-pool aur GC evidence ko same incident time window mein correlate karo.

## 2. Practical exercise with complete worked solution — 8 minutes

### Problem and constraints

Assume the following incident:

- Java 21 Spring Boot order service.
- p99 latency rises from 180 ms to 2.2 s.
- CPU is approximately 30%; no sustained CPU saturation is visible.
- Post-GC heap occupancy is broadly stable.
- Hikari active connections reach the pool maximum; pending borrowers rise.
- A shipping-quote HTTP call is made from inside an order-creation method.
- You must diagnose with production-safe evidence before changing JVM or pool settings.

The sample evidence below is a **worked hypothetical incident**, not a claim about a real production system.

### Step 1 — Establish the incident window and compare application metrics

Compare a healthy 10-minute window with the degraded 10-minute window. Use the same replica, traffic class and time basis where possible.

Collect:

- HTTP p50/p95/p99, throughput, in-flight requests and errors.
- Hikari active, idle, maximum and pending connections; connection-acquisition and connection-usage duration if instrumented.
- HTTP client request latency, in-flight calls and connection-pool pending/leased counts.
- JVM process CPU, host/container CPU, CPU throttling, heap after GC, allocation rate, GC pause time and thread count.
- Database query latency, lock waits, active sessions and connection count.
- Distributed traces for slow orders, including spans around the shipping call and DB operations.

Averages hide tail behaviour. Align metrics and traces to the same timestamps. A single replica can be unhealthy while fleet-wide averages look normal.

### Step 2 — Capture a bounded JFR recording while the problem is happening

Run these commands **inside the target host/container**, as the same effective OS user that launched the JVM. First identify the PID:

```bash
jcmd -l
```

In a container, the process may be PID 1 inside the container. If process discovery does not show it, inspect processes from inside the container and use the actual JVM PID. The JDK 21 documentation notes that `jcmd` must run on the same machine and with the same effective user/group identifiers as the target process: [jcmd reference](https://docs.oracle.com/en/java/javase/21/docs/specs/man/jcmd.html).

Start a two-minute, bounded recording using the low-overhead default template:

```bash
PID=1
jcmd "$PID" JFR.start name=order-latency settings=default \
  duration=120s filename=/var/tmp/order-latency.jfr
jcmd "$PID" JFR.check name=order-latency verbose=true
```

Ensure the destination has enough free disk, appropriate permissions and restricted access. JFR files can contain stack traces, host/address information and application details; treat them as operationally sensitive.

After the recording finishes:

```bash
jfr summary /var/tmp/order-latency.jfr

jfr print --stack-depth 10 \
  --events jdk.SocketRead,jdk.SocketWrite,\
jdk.JavaMonitorEnter,jdk.JavaMonitorWait,jdk.ThreadPark,\
jdk.GCPhasePause,jdk.ExecutionSample,jdk.CPULoad \
  /var/tmp/order-latency.jfr
```

Use JDK Mission Control (JMC) when available to inspect thread-stalling timelines, hot methods, lock instances, socket I/O and GC pauses. The command-line `jfr` tool can print, summarize and aggregate recording data: [JDK 21 jfr reference](https://docs.oracle.com/en/java/javase/21/docs/specs/man/jfr.html).

**Recording choice:** `default.jfc` is intended for lower-overhead continuous recording. `profile.jfc` records more information and has a greater performance impact, so use it for a short, targeted capture when the default recording does not answer the question. Measure overhead in your own workload. Do not enable heap statistics casually during a latency incident; Oracle notes that JFR heap statistics can trigger old/full garbage collections and add pause time.

### Step 3 — Take thread-dump snapshots, not just one screenshot

```bash
jcmd "$PID" Thread.print -l > /var/tmp/threads-1.txt
sleep 10
jcmd "$PID" Thread.print -l > /var/tmp/threads-2.txt
sleep 10
jcmd "$PID" Thread.print -l > /var/tmp/threads-3.txt
```

Look for repeated stacks and patterns:

- `BLOCKED`: waiting to enter an intrinsic monitor.
- `WAITING` / `TIMED_WAITING`: may be parked, waiting on a condition, a queue, a future or I/O-related coordination; inspect the stack rather than guessing.
- Threads in `HikariPool.getConnection` or equivalent pool-borrowing code: likely waiting for a database connection.
- Threads in the shipping HTTP client's read path: possibly waiting for the remote service.
- The same lock owner and waiters across snapshots: possible monitor contention or deadlock.

A thread dump is a point-in-time snapshot. Repeated dumps help distinguish persistent waits from a momentary state. `Thread.print -l` includes stack traces and additional lock information; its impact is medium and depends on thread count. Check the target JVM's `jcmd <pid> help Thread.print` output before relying on options.

### Step 4 — Interpret the evidence as a hypothesis, then test it

Suppose the worked incident produces this evidence:

| Signal | Hypothetical observation | Interpretation |
|---|---|---|
| Process CPU | Around 30%, no sustained saturation | Not primarily CPU-bound |
| GC pause events | Very little pause time in the incident window | GC is unlikely to explain a 2-second tail by itself |
| Post-GC heap | No sustained upward baseline | No immediate evidence of growing retained Java heap |
| Hikari active/pending | Active at max; pending rises with p99 | Requests are waiting to borrow connections |
| JFR `jdk.SocketRead` | Long reads stack into `ShippingClient.fetchQuote` | A remote call is consuming wall-clock time |
| Thread dumps | Some request threads wait for Hikari connections; slow request stacks pass through shipping quote code | Consistent with connections being held while remote I/O runs |
| Database query latency | Normal for the affected query class | The wait is not necessarily database execution time |

**Working diagnosis:** the service opens a database transaction/connection and then performs a slow shipping HTTP call before committing. The connection remains checked out while the thread waits on the remote socket. Under load, enough connections are occupied by remote waits that new requests queue in Hikari. This creates a pool-acquisition queue and amplifies p99 latency.

Do not infer this from one JFR event alone. Correlate the same trace/request, transaction timing, pool usage duration and JFR stack/event window. A `SocketRead` event proves time was spent waiting on a socket operation; it does not, by itself, prove which business dependency caused the wait or whether a DB connection was held.

### Step 5 — Fix the transaction boundary instead of blindly enlarging pools

Bad shape:

```java
@Transactional
public OrderResult placeOrder(CreateOrder command) {
    // Transaction/connection may already be open.
    ShippingQuote quote = shippingClient.fetchQuote(command);
    return orderRepository.create(command, quote);
}
```

Better shape: do the remote quote before opening the short database transaction. Put transactional persistence in a separate Spring bean so the call crosses the transaction proxy.

```java
@Service
final class OrderFacade {
    private final ShippingClient shippingClient;
    private final OrderWriter orderWriter;

    OrderFacade(ShippingClient shippingClient, OrderWriter orderWriter) {
        this.shippingClient = shippingClient;
        this.orderWriter = orderWriter;
    }

    // Deliberately NOT @Transactional.
    public OrderResult placeOrder(CreateOrder command) {
        // Client must have bounded connection/response deadlines.
        ShippingQuote quote = shippingClient.fetchQuote(command);

        // Separate proxied bean starts a short DB transaction.
        return orderWriter.persist(command, quote);
    }
}

@Service
final class OrderWriter {
    private final OrderRepository orders;
    private final InventoryRepository inventory;

    OrderWriter(OrderRepository orders, InventoryRepository inventory) {
        this.orders = orders;
        this.inventory = inventory;
    }

    @Transactional
    public OrderResult persist(CreateOrder command, ShippingQuote quote) {
        // Re-check authoritative local invariants atomically.
        // Example SQL: UPDATE inventory
        // SET reserved = reserved + :qty
        // WHERE sku = :sku AND available - reserved >= :qty
        boolean reserved = inventory.reserveIfAvailable(
            command.sku(), command.quantity());

        if (!reserved) {
            throw new OutOfStockException();
        }

        Order order = orders.insert(command, quote);
        return OrderResult.from(order);
    }
}
```

The repository methods and domain records are application-specific; the important structure is that the remote call is outside the transaction and the DB-side invariant is rechecked atomically in the short transaction.

If the inventory itself is owned by a remote service, a local conditional update is not enough. Use an idempotent reservation API and a saga/outbox-based workflow with explicit compensation/reconciliation. A quote is not a reservation.

Spring proxy mode intercepts external calls through the bean proxy; self-invocation within the same object does not start a new transactional boundary. This is why the example separates `OrderFacade` and `OrderWriter`. See [Spring @Transactional proxy semantics](https://docs.spring.io/spring-framework/reference/data-access/transaction/declarative/annotations.html).

### Step 6 — Validate the fix with a controlled before/after experiment

Under comparable traffic, compare:

- p50/p95/p99 and throughput.
- Hikari active/pending and connection acquisition/usage duration.
- Shipping-call latency and timeout rate.
- DB query and lock latency.
- Error rate, order correctness and duplicate-command handling.

Expected improvement if the hypothesis is correct: shipping waits no longer occupy DB transactions/connections; pending borrowers and tail latency fall even if the remote shipping service remains slow. If they do not, revisit the hypothesis rather than increasing the pool automatically.

## 3. Walkthrough, mistakes and production trade-offs — 4 minutes

### How to reason from symptoms to a root cause

1. **High CPU + execution samples concentrated in one method:** investigate CPU hot paths, allocations in that path, serialization, regex, encryption, or algorithmic complexity.
2. **Low CPU + long `SocketRead`/`SocketWrite`:** investigate the remote dependency, network, client pool, timeout and retry behaviour.
3. **High monitor-enter/wait time:** inspect lock owner, critical-section duration and contention. Do not replace every `synchronized` block reflexively.
4. **Hikari active=max + pending rising:** inspect connection acquisition time and connection usage duration. The database may be slow, or application code may hold connections while doing unrelated work.
5. **GC pause time aligns with p99 spikes:** inspect the pause events, allocation rate, live-set trend and GC logs. Tune only after establishing the relationship.
6. **Heap looks stable but container RSS grows:** investigate direct/native allocations, thread stacks, memory-mapped files and native libraries. Native Memory Tracking must be enabled at JVM startup to provide useful baseline data; it does not account for every third-party native allocation.

For GC logs on JDK 21, enable unified logging at startup, for example:

```bash
-Xlog:gc*:file=/var/log/orders/gc.log:time,uptime,level,tags
```

The `-Xlog` unified logging syntax and GC tags are documented in the [JDK 21 java command reference](https://docs.oracle.com/en/java/javase/21/docs/specs/man/java.html). Do not change collectors or heap sizing merely because GC logs exist; first compare pause time with the actual latency window.

### Common mistakes

- **Increasing `-Xmx` immediately:** does not fix socket waits, lock contention or a database connection held across remote I/O.
- **Increasing Hikari max connections blindly:** may transfer overload to the database and increase lock/CPU contention. Measure database capacity and all consumers first.
- **Taking a heap dump as the first diagnostic action:** `jcmd GC.heap_dump` is high impact and, by default, requests a full GC unless `-all` is used. Plan it for an approved window/replica with disk capacity and sensitive-data controls. `GC.class_histogram` is also high impact on large heaps.
- **Reading one thread dump as a timeline:** it is only a snapshot. Take repeated samples and correlate with JFR/traces.
- **Assuming no JFR event means no wait:** many duration events have thresholds; short waits may not be recorded. Check recording settings and threshold before concluding.
- **Using `profile.jfc` continuously without measuring:** it records more and can add more overhead than the default configuration.
- **Confusing DB execution time with connection hold time:** a connection can be held for seconds while SQL itself takes milliseconds.
- **Ignoring container limits:** CPU throttling, memory pressure, DNS, sidecars and node-level contention can be outside the JVM's own CPU/heap charts.
- **Sharing raw recordings broadly:** JFR and heap artifacts can contain stack traces, network details and application data. Restrict access and scrub recordings before external sharing.

### Interview-ready answer

> I would not start by changing heap or GC flags. First I would align p99 latency, CPU, GC, Hikari pool metrics, downstream latency and traces to the same incident window. I would take a short JFR recording and repeated thread dumps. JFR separates CPU execution samples from socket I/O, monitor contention, parking and GC pauses. If CPU and GC are quiet but socket reads are long, and Hikari active/pending are saturated, I would inspect whether the application holds a DB connection or transaction while calling the remote service. I would move that remote call outside the short DB transaction, revalidate business invariants atomically, then compare p99, connection usage and correctness under the same load. I would only take a heap dump or increase pool/heap after evidence justifies the cost.

### Follow-up questions with answers

**Q1. Does a low CPU percentage prove the JVM is not the bottleneck?**  
No. Threads can be blocked on locks, parked, waiting on sockets or waiting for a pool. Also check container CPU throttling and host-level contention.

**Q2. What is the difference between `jdk.JavaMonitorEnter` and `jdk.JavaMonitorWait`?**  
Monitor-enter events relate to waiting to enter an intrinsic monitor; monitor-wait events relate to `Object.wait()`-style waiting. Inspect event type and stack before attributing the cause.

**Q3. Can a thread dump identify how long a thread has waited?**  
A normal dump is a snapshot, not a full wait-duration timeline. Use repeated dumps for persistence and JFR duration events for timed wait/I/O evidence.

**Q4. When would you take a heap dump?**  
When evidence points to retained objects or memory growth that needs object-graph analysis, and the operational cost is acceptable. A heap dump is not a first-line tool for a socket/pool wait incident.

**Q5. Why can adding DB connections make latency worse?**  
If the database is already at its efficient concurrency limit, more connections can increase scheduling, lock and I/O contention. If the actual problem is holding connections during remote waits, a larger pool only delays saturation and increases downstream pressure.

**Q6. How would you prove the fix?**  
Run a comparable load test or controlled rollout, compare latency percentiles, connection acquisition/usage duration, active/pending pool counts, dependency latency, error rate and business correctness. A lower p99 without preserved correctness is not a successful fix.

## Sources — primary documentation

1. [Oracle JDK 21: Troubleshoot Performance Issues Using Flight Recorder](https://docs.oracle.com/en/java/javase/21/troubleshoot/troubleshoot-performance-issues-using-jfr.html) — thread-stalling events, socket I/O, synchronization, execution samples and GC.
2. [Oracle JDK 21: jcmd command reference](https://docs.oracle.com/en/java/javase/21/docs/specs/man/jcmd.html) — diagnostic command syntax and impact classifications.
3. [Oracle JDK 21: jfr command reference](https://docs.oracle.com/en/java/javase/21/docs/specs/man/jfr.html) — `summary`, `print`, `view`, event filters and recording inspection.
4. [Oracle JDK 21: Flight Recorder configurations](https://docs.oracle.com/en/java/javase/21/jfapi/flight-recorder-configurations.html) — default vs profile recording configurations.
5. [Oracle JDK 21: java command reference](https://docs.oracle.com/en/java/javase/21/docs/specs/man/java.html) — unified logging with `-Xlog`.
6. [Spring Framework: Using @Transactional](https://docs.spring.io/spring-framework/reference/data-access/transaction/declarative/annotations.html) — proxy interception and self-invocation semantics.

**Takeaway:** Diagnose where wall-clock time goes before tuning JVM knobs. In backend services, a slow request may be waiting on a socket while holding a scarce database connection—not spending time executing Java code or collecting garbage.

# Advanced Java Backend Practice — 2026-10-31
## JVM Diagnostics: Pehle Pata Karo Request Wait Kahan Kar Rahi Hai

**Runtime:** Java 21 (HotSpot JDK 21 ke JFR/jcmd tools), Spring Boot 3.x / Spring Framework 6.x, PostgreSQL, HikariCP  
**Time budget:** 15 minutes — 3-minute concept, 8-minute practical exercise, 4-minute walkthrough  
**Scenario:** Order API ka p99 latency achanak 180 ms se 2.2 seconds ho gaya. CPU zyada busy nahi hai aur GC ke baad heap lagbhag stable hai. Team ko lagta hai JVM issue hai, lekin asli bottleneck remote HTTP call ho sakta hai jo database connection hold karke chal rahi hai.

> Version note: Neeche diye commands JDK 21 ke official `jcmd` aur `jfr` documentation ke hisaab se hain. Jis JVM ko diagnose kar rahe ho, koshish karo usi JDK/runtime family ke tools use karo. Actual process ke liye `jcmd <pid> help` aur recording ke liye `jfr metadata` check karo. Agar deployment JDK 25/26 par hai, event names aur options ko assume mat karo—usi runtime par verify karo.

## Prerequisites

- JVM heap, GC aur Java thread states ka basic idea
- Spring transactions aur JDBC connection pool
- HTTP client timeout aur service-to-service calls
- Metrics: latency percentiles, active/pending connections, CPU aur GC pauses
- Basic Linux/container process access

## 1. Concept question — 3 minutes

### Question

Order API ka p99 180 ms se 2.2 seconds ho gaya. Throughput lagbhag same hai, process CPU 30% ke aas-paas hai aur GC ke baad heap stable dikh raha hai. HikariCP mein active connections configured maximum tak pahunch gaye hain aur pending borrowers badh rahe hain.

**Kya tum turant heap badhaoge, GC change karoge ya Hikari pool size increase karoge? Sabse pehle kaunsa evidence collect karoge?**

### Complete answer

**Nahi. Abhi kisi tuning change ko justify karne ke liye enough evidence nahi hai.** CPU low aur GC ke baad heap stable hone se CPU saturation ya continuously badhte Java heap-retention ka chance kam lagta hai, lekin dono possibilities completely rule out nahi hoti. Hikari metrics pool pressure ka signal de rahe hain; woh abhi ye prove nahi karte ki connection itni der tak occupied kyun hai.

JVM issue investigate karte waqt humein request ka time kis jagah spend ho raha hai, ye samajhna hai.

Isko chaar categories mein socho:

1. **Java code execute ho raha hai:** CPU samples kisi expensive method mein jama ho rahe hain—serialization, regex, encryption ya inefficient loop.
2. **Thread wait kar rahi hai:** lock contention, parked thread, executor queue ya thread-pool starvation.
3. **I/O ya downstream ka wait:** socket read/write, database connection lene ka wait, SQL execution, DNS, remote API ya file-system operation.
4. **GC/safepoint ya memory pressure:** GC pause ke time hi latency badh rahi hai, allocation/retained memory badh rahi hai, ya native/container memory pressure hai.

JDK Flight Recorder (JFR) thread-stalling events, execution samples aur GC information capture kar sakta hai. Oracle ke JDK 21 guide mein monitor wait, socket I/O, code execution aur GC ko alag-alag bottleneck categories ke roop mein investigate kiya gaya hai: [Oracle — Troubleshoot Performance Issues Using Flight Recorder](https://docs.oracle.com/en/java/javase/21/troubleshoot/troubleshoot-performance-issues-using-jfr.html).

### Hinglish intuition

Sabse pehle ye mat bolo ki "GC slow hai" ya "heap badha do". CPU low hai aur GC ke baad heap stable hai, toh request kahaan wait kar rahi hai, usko investigate karo. Hikari ke active connections maximum par hain aur pending borrowers badh rahe hain—matlab requests ko database connection milne mein wait lag raha hai.

Lekin abhi asli sawal bacha hai: **connection itni der tak occupied kis wajah se hai?**

Ek request SQL chala rahi ho, ye zaroori nahi. Ho sakta hai usne connection le liya ho, transaction open kar di ho, aur phir remote service se response ka wait kar rahi ho.

**Yaad rakho:** Symptom ko root cause mat maan lo. Same time window mein thread, socket, connection-pool aur GC evidence ko correlate karo.

## 2. Practical exercise with complete solution — 8 minutes

### Problem aur constraints

Maan lo humein ye incident mila:

- Java 21 + Spring Boot order service.
- p99 latency 180 ms se 2.2 seconds.
- CPU around 30%; sustained CPU saturation nahi dikh rahi.
- GC ke baad heap broadly stable.
- Hikari active connections maximum par; pending borrowers badh rahe hain.
- Order creation method ke andar shipping-quote HTTP call hoti hai.
- JVM ya pool settings change karne se pehle production-safe diagnosis karna hai.

Neeche jo sample evidence diya hai woh **worked hypothetical example** hai. Ye kisi actual production system ka measurement ya claim nahi hai.

### Step 1 — Incident window fix karo aur application metrics compare karo

Healthy 10-minute window aur degraded 10-minute window ko compare karo. Jahan possible ho, same replica, same traffic type aur same time basis use karo.

Ye data collect karo:

- HTTP p50/p95/p99, request rate, in-flight requests aur error rate.
- Hikari active, idle, maximum aur pending connections. Agar instrument kiya hai toh connection-acquisition aur connection-usage duration bhi.
- HTTP client request latency, in-flight calls aur uske connection pool ke pending/leased counts.
- JVM process CPU, host/container CPU, CPU throttling, heap-after-GC, allocation rate, GC pause time aur thread count.
- Database query latency, lock waits, active sessions aur connection count.
- Slow order requests ke distributed traces. Shipping call aur DB operations ke spans ka duration dekho.

Average latency dekh ke khush mat ho jaana. p50 normal ho sakta hai jabki p99 bahut kharab ho. Metrics aur traces ko same timestamp par align karo. Ek replica unhealthy ho sakta hai, lekin fleet-wide average us problem ko chhupa sakta hai.

### Step 2 — Problem chal rahi ho tab bounded JFR recording lo

Commands target host/container ke andar, aur ideally usi OS user ke under chalao jis user ne JVM start ki thi.

Pehle process identify karo:

```bash
jcmd -l
```

Container ke andar Java process PID 1 ho sakta hai. Agar `jcmd -l` mein process nahi dikh raha, container ke andar process list check karke actual JVM PID use karo.

JDK 21 documentation ke mutabik `jcmd` ko target JVM wali machine par aur same effective user/group identifiers ke saath chalana hota hai: [Oracle JDK 21 jcmd reference](https://docs.oracle.com/en/java/javase/21/docs/specs/man/jcmd.html).

Ab do minute ki bounded recording start karo:

```bash
PID=1
jcmd "$PID" JFR.start name=order-latency settings=default \
  duration=120s filename=/var/tmp/order-latency.jfr
jcmd "$PID" JFR.check name=order-latency verbose=true
```

`default` configuration lower-overhead recording ke liye hoti hai aur continuous use ke liye suitable hai. Recording destination par disk space, permissions aur restricted access check karo. JFR file mein stack traces, host/address information aur application details ho sakte hain. Isliye ise sensitive operational artifact samjho.

Recording complete hone ke baad:

```bash
jfr summary /var/tmp/order-latency.jfr

jfr print --stack-depth 10 \
  --events jdk.SocketRead,jdk.SocketWrite,\
jdk.JavaMonitorEnter,jdk.JavaMonitorWait,jdk.ThreadPark,\
jdk.GCPhasePause,jdk.ExecutionSample,jdk.CPULoad \
  /var/tmp/order-latency.jfr
```

Agar JDK Mission Control (JMC) available hai, toh usme recording open karke thread-stalling timeline, hot methods, lock instances, socket I/O aur GC pauses dekho.

Command line ka `jfr` tool recording ko print, summarize aur aggregate kar sakta hai: [Oracle JDK 21 jfr reference](https://docs.oracle.com/en/java/javase/21/docs/specs/man/jfr.html).

**Kaunsi recording choose karein?**

- `default.jfc`: low-overhead continuous recording ke liye.
- `profile.jfc`: zyada detail collect karti hai, lekin iska performance impact bhi zyada ho sakta hai. Jab default recording se jawab na mile, tab short aur targeted capture ke liye use karo.
- Apne actual workload mein overhead measure karo.

Latency incident ke dauran heap statistics casually enable mat karo. Oracle warn karta hai ki JFR heap statistics old/full GC trigger kar sakti hain aur extra pause time add kar sakti hain.

### Step 3 — Ek thread dump nahi, multiple snapshots lo

```bash
jcmd "$PID" Thread.print -l > /var/tmp/threads-1.txt
sleep 10
jcmd "$PID" Thread.print -l > /var/tmp/threads-2.txt
sleep 10
jcmd "$PID" Thread.print -l > /var/tmp/threads-3.txt
```

Ab stacks aur thread states compare karo:

- **BLOCKED:** thread intrinsic monitor/lock enter karne ka wait kar rahi hai.
- **WAITING / TIMED_WAITING:** thread parked ho sakti hai, condition, queue, future ya kisi coordination point ka wait kar rahi ho sakti hai. Sirf state dekh ke exact cause assume mat karo; stack trace dekho.
- `HikariPool.getConnection` ya connection-borrowing code mein threads: database connection lene ka wait ho sakta hai.
- Shipping HTTP client ke read path mein threads: remote service se response ka wait ho sakta hai.
- Har snapshot mein same lock owner aur same waiters: monitor contention ya deadlock ka signal ho sakta hai.

Thread dump ek fixed moment ka snapshot hai. Multiple dumps se pata chalta hai ki wait persistent hai ya sirf ek moment ka tha. `Thread.print -l` stack traces ke saath extra lock information bhi deta hai. Is command ka impact medium hai aur thread count par depend karta hai. Target JVM ke `jcmd <pid> help Thread.print` se supported options verify karo.

### Step 4 — Evidence ko correlate karke hypothesis banao

Maan lo worked incident mein ye hypothetical results milte hain:

| Signal | Example observation | Iska matlab |
|---|---|---|
| Process CPU | 30% ke aas-paas, sustained saturation nahi | Primary bottleneck CPU nahi lag raha |
| GC pause events | Incident window mein bahut kam pause time | 2-second p99 ka explanation GC se aana unlikely |
| Post-GC heap | Sustained upward baseline nahi | Java heap retention grow hone ka immediate evidence nahi |
| Hikari active/pending | Active max par; pending p99 ke saath badh raha hai | Requests connection borrow karne ka wait kar rahi hain |
| JFR `jdk.SocketRead` | Long socket reads ke stack mein `ShippingClient.fetchQuote` | Remote call mein wall-clock time ja raha hai |
| Thread dumps | Kuch request threads Hikari connection ke liye wait kar rahi hain; slow request ka stack shipping quote path se guzarta hai | Connections remote I/O ke dauran hold ho sakti hain |
| DB query latency | Affected query type ke liye normal | Wait zaroori nahi ki SQL execution ke andar ho |

**Working diagnosis:** Order service database transaction/connection open karti hai, uske baad shipping HTTP call execute karti hai aur commit baad mein hota hai. Jab thread remote socket se response ka wait karti hai, database connection checked-out rehti hai.

Load badhne par bahut saari connections remote calls ke wait mein occupied ho jaati hain. Nayi requests ko Hikari se connection nahi milti, woh pending queue mein rukti hain, aur p99 aur badh jaata hai.

Is diagnosis ko ek JFR event se final mat maan lena. Same request/trace, transaction duration, connection usage duration aur JFR stack/event window correlate karo. `SocketRead` batata hai ki socket operation par time spend hua; woh apne aap prove nahi karta ki business dependency kaunsi thi ya us waqt DB connection held thi.

### Step 5 — Pool ko blindly badhane ke bajaye transaction boundary fix karo

**Problematic structure:**

```java
@Transactional
public OrderResult placeOrder(CreateOrder command) {
    // Transaction/connection pehle hi open ho sakti hai.
    ShippingQuote quote = shippingClient.fetchQuote(command);
    return orderRepository.create(command, quote);
}
```

Is method mein remote call transaction ke andar hai. Shipping service 1.5 seconds laga de, toh connection bhi utni der tak occupied reh sakti hai.

**Better structure:** Remote quote ko short DB transaction start hone se pehle fetch karo. Transactional persistence ko separate Spring bean mein rakho, taaki call transaction proxy ke through ho.

```java
@Service
final class OrderFacade {
    private final ShippingClient shippingClient;
    private final OrderWriter orderWriter;

    OrderFacade(ShippingClient shippingClient, OrderWriter orderWriter) {
        this.shippingClient = shippingClient;
        this.orderWriter = orderWriter;
    }

    // Is method par jaan-boojhkar @Transactional nahi hai.
    public OrderResult placeOrder(CreateOrder command) {
        // HTTP client mein bounded connection/response deadlines honi chahiye.
        ShippingQuote quote = shippingClient.fetchQuote(command);

        // Separate proxied bean short DB transaction start karega.
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
        // Authoritative local business condition ko atomically re-check karo.
        // Example SQL:
        // UPDATE inventory
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

Repository methods aur domain records tumhare application-specific hain. Important baat ye hai ki remote call transaction ke bahar ho aur DB-side business invariant short transaction ke andar atomically re-check ho.

Agar inventory kisi remote service ki ownership mein hai, toh local DB update enough nahi hoga. Wahan idempotent reservation API, saga/outbox workflow aur explicit compensation/reconciliation chahiye. **Quote reservation nahi hota.**

Spring ka default proxy mode external bean calls ko intercept karta hai. Same class ke andar ek method se doosre `@Transactional` method ko call karne par normal proxy interception nahi hoti. Isi liye `OrderFacade` aur `OrderWriter` ko separate beans rakha hai. Reference: [Spring @Transactional proxy semantics](https://docs.spring.io/spring-framework/reference/data-access/transaction/declarative/annotations.html).

### Step 6 — Fix ko controlled before/after experiment se validate karo

Comparable traffic ke under in cheezon ko compare karo:

- p50/p95/p99 aur throughput.
- Hikari active/pending, connection acquisition aur usage duration.
- Shipping-call latency aur timeout count.
- DB query aur lock latency.
- Error rate, order correctness aur duplicate-command handling.

Agar hypothesis correct hai, toh shipping wait ke dauran DB transaction/connection occupied nahi rahegi. Remote shipping service slow rahe tab bhi Hikari pending borrowers aur p99 girne chahiye.

Agar expected improvement nahi aata, hypothesis dobara evaluate karo. Sirf pool size badha dena proof nahi hai.

## 3. Walkthrough, mistakes aur production trade-offs — 4 minutes

### Symptoms se root cause tak kaise pahunchna hai?

1. **CPU high + execution samples ek method mein concentrated:** CPU hot path investigate karo—serialization, regex, encryption, temporary allocations ya algorithmic complexity.
2. **CPU low + long `SocketRead`/`SocketWrite`:** remote dependency, network, HTTP client pool, timeout aur retry behaviour check karo.
3. **Monitor-enter/wait duration high:** lock owner aur critical section ka duration dekho. Har `synchronized` ko bina evidence replace mat karo.
4. **Hikari active=max + pending badh raha:** connection acquisition aur connection usage duration check karo. DB slow ho sakta hai, ya application connection ko unrelated remote work ke dauran hold kar rahi ho sakti hai.
5. **GC pause p99 spike ke saath align hota hai:** pause events, allocation rate, live-set trend aur GC logs inspect karo. GC tuning tabhi karo jab relationship establish ho.
6. **Heap stable, lekin container RSS badh raha:** direct/native allocations, thread stacks, memory-mapped files aur native libraries investigate karo. Native Memory Tracking ko JVM startup par enable karna padta hai; ye har third-party native allocation track nahi karta.

### Java 21 par GC logs kaise collect karein?

JVM startup par unified logging enable kar sakte ho:

```bash
-Xlog:gc*:file=/var/log/orders/gc.log:time,uptime,level,tags
```

JDK 21 mein `-Xlog` unified logging syntax aur GC tags ki details [Oracle java command reference](https://docs.oracle.com/en/java/javase/21/docs/specs/man/java.html) mein hain.

Sirf GC logs available hain is wajah se collector ya heap settings change mat karo. Pehle verify karo ki GC pauses actual latency spike ke time par ho rahi hain ya nahi.

### Common mistakes

**1. Turant `-Xmx` badha dena**

Socket read, lock contention ya remote API call ke dauran held DB connection ka problem solve nahi hoga.

**2. Hikari max pool size blindly badhana**

Database ki concurrency capacity exceed ho sakti hai. Extra connections lock contention, CPU aur I/O pressure badha sakti hain. Pehle DB capacity aur sabhi consumers ka load samjho.

**3. Sabse pehle heap dump le lena**

`jcmd GC.heap_dump` high-impact command hai. Default behaviour mein `-all` option ke bina full GC request hoti hai. Isse latency pause aa sakta hai. Approved window/replica, disk capacity aur sensitive-data permissions ke saath hi lo. `GC.class_histogram` bhi large heap par high-impact ho sakta hai.

**4. Ek thread dump ko timeline samajh lena**

Thread dump sirf ek moment ka snapshot hai. Repeated dumps, JFR aur traces ko correlate karo.

**5. JFR event nahi mila toh wait nahi hai, aisa maan lena**

Kai duration events thresholds ke baad hi record hote hain. Settings check karo. Short waits record nahi hui hon, toh unko absent samajhna galat hoga.

**6. `profile.jfc` ko bina measurement continuously chalana**

Ismein zyada details collect hoti hain aur overhead bhi badh sakta hai. Default aur profile configuration ka purpose alag hai.

**7. DB query time ko connection-hold time samajhna**

SQL 5 ms mein complete ho sakti hai, lekin application ne connection 1.5 seconds hold rakhi ho kyunki woh beech mein shipping API ka wait kar rahi thi.

**8. Container limits ignore karna**

CPU throttling, memory pressure, DNS, sidecar aur node-level contention JVM ke internal CPU/heap charts se bahar ho sakte hain.

**9. Raw diagnostic files sabke saath share kar dena**

JFR aur heap artifacts mein stack traces, network details aur application data ho sakta hai. Access restrict karo aur external sharing se pehle zaroorat ke hisaab se scrub karo.

### Interview-ready answer

> Main heap ya GC flags change karne se start nahi karunga. Pehle p99 latency ko CPU, GC, Hikari metrics, downstream latency aur distributed traces ke saath same incident window mein correlate karunga. Short JFR recording aur repeated thread dumps lunga. JFR se pata chal sakta hai ki time CPU execution, socket I/O, monitor contention, parking ya GC pauses mein ja raha hai. Agar CPU aur GC quiet hain, lekin socket reads long hain aur Hikari active/pending saturated hai, toh check karunga ki application remote call ke dauran DB connection/transaction hold toh nahi kar rahi. Remote call ko short DB transaction ke bahar move karunga, business invariant ko transaction mein atomically re-check karunga, aur comparable load par p99, connection usage aur correctness verify karunga. Heap dump ya pool increase tabhi karunga jab evidence usko justify kare.

### Follow-up questions with answers

**Q1. Kya low CPU percentage prove karta hai ki JVM bottleneck nahi hai?**  
Nahi. Threads lock, socket, pool ya queue par wait kar sakti hain. Container CPU throttling aur host-level contention bhi check karo.

**Q2. `jdk.JavaMonitorEnter` aur `jdk.JavaMonitorWait` mein difference kya hai?**  
Monitor-enter event intrinsic monitor enter karne ke wait se related hai. Monitor-wait event `Object.wait()` style waiting se related hai. Event type aur stack dekh kar hi cause identify karo.

**Q3. Kya ek thread dump se exact wait duration pata chalti hai?**  
Usually nahi. Woh snapshot hai. Repeated dumps persistence dikhate hain; JFR duration events timed wait/I/O ka evidence dete hain.

**Q4. Heap dump kab lena chahiye?**  
Jab retained objects ya memory growth ka evidence ho aur object graph analyse karna zaroori ho. Socket/pool wait incident mein ye first-line tool nahi hai.

**Q5. Extra DB connections se latency aur kharab kaise ho sakti hai?**  
Agar database apni efficient concurrency limit ke paas hai, toh extra sessions se lock, scheduling aur I/O contention badh sakti hai. Agar actual problem remote wait ke dauran connection hold karna hai, toh bada pool sirf saturation delay karega aur downstream pressure bhi badha sakta hai.

**Q6. Fix successful hua, ye prove kaise karoge?**  
Comparable load test ya controlled rollout mein p99, connection acquisition/usage duration, active/pending count, dependency latency, error rate aur business correctness compare karunga. Sirf p99 kam hona enough nahi hai agar order correctness break ho rahi ho.

## Sources — official documentation

1. [Oracle JDK 21: Troubleshoot Performance Issues Using Flight Recorder](https://docs.oracle.com/en/java/javase/21/troubleshoot/troubleshoot-performance-issues-using-jfr.html) — thread-stalling events, socket I/O, synchronization, execution samples aur GC.
2. [Oracle JDK 21: jcmd command reference](https://docs.oracle.com/en/java/javase/21/docs/specs/man/jcmd.html) — diagnostic commands, syntax aur impact.
3. [Oracle JDK 21: jfr command reference](https://docs.oracle.com/en/java/javase/21/docs/specs/man/jfr.html) — `summary`, `print`, `view`, event filters aur recording analysis.
4. [Oracle JDK 21: Flight Recorder configurations](https://docs.oracle.com/en/java/javase/21/jfapi/flight-recorder-configurations.html) — default aur profile recording ka difference.
5. [Oracle JDK 21: java command reference](https://docs.oracle.com/en/java/javase/21/docs/specs/man/java.html) — `-Xlog` unified logging.
6. [Spring Framework: Using @Transactional](https://docs.spring.io/spring-framework/reference/data-access/transaction/declarative/annotations.html) — proxy interception aur self-invocation behaviour.

**Aaj ka takeaway:** JVM tuning se pehle request ka wait-state identify karo. Production backend mein slow request zaroori nahi ki Java code execute karne ya GC mein time spend kar rahi ho. Ho sakta hai woh remote socket ka wait kar rahi ho aur usi dauran ek scarce database connection hold karke baithi ho.

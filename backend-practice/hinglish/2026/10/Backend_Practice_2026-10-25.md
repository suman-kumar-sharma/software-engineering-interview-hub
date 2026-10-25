# Advanced Java Backend Practice — 2026-10-25
## Java Concurrency aur JMM: Visibility ka matlab Atomicity nahi hota

**Runtime:** Java 21  
**Time:** 15 minutes — 3-minute concept, 8-minute exercise, 4-minute walkthrough  
**Scenario:** Heavy traffic ke beech order API successful requests count karti hai.

## Prerequisites
- Java threads aur `volatile` ka basic idea
- `AtomicLong` aur `LongAdder`
- Compound operation: read, modify, write

## 1. Concept question — 3 minutes

### Question
Spring service completed orders count karti hai:

```java
private volatile long completedOrders;

void recordSuccess() {
    completedOrders++;
}
```

Field `volatile` hai. Kya hundreds of request threads ek saath `recordSuccess()` call karein, tab count guaranteed correct hoga?

### Worked answer
**Nahi. `volatile` individual volatile reads/writes ki visibility aur ordering guarantees deta hai; lekin `completedOrders++` ko atomic operation nahi banata.**

Increment ko conceptually teen steps mein socho:
1. Current value read karo.
2. Usmein locally 1 add karo.
3. Result wapas write karo.

Maan lo counter 10 hai. Thread A aur Thread B dono ek saath 10 read kar lete hain. Dono 11 calculate karte hain aur dono 11 write kar dete hain. Do successful requests hui, lekin counter sirf ek baar badha. Isko **lost update** kehte hain.

`volatile` shared flag ya single value publish karne ke liye useful hai, jab baaki protocol sahi ho. Lekin read-modify-write operation ko atomic nahi banata.

Java Memory Model happens-before relationship define karta hai. Ek volatile write, usi field ke subsequent volatile read se happens-before hota hai; par isse multiple operations ek transaction nahi ban jaate. JLS `17.4.5: https://docs.oracle.com/javase/specs/jls/se21/html/jls-17.html#jls-17.4.5

### Counter choose kaise karein?
- **JVM ke andar exact atomic count ya sequence:** `AtomicLong`.
- **High-contention metrics/statistics:** aksar `LongAdder`, jab concurrent updates ke dauran perfectly atomic snapshot zaroori na ho.
- **Multiple replicas ke beech global business rule:** koi local class akeli enough nahi. Database conditional update, distributed atomic primitive, ya authoritative coordination mechanism use karo.

Simple rule: `volatile` se visibility milti hai, atomicity nahi. `count++` ek single indivisible step nahi hai.

## 2. Practical exercise with complete solution — 8 minutes

### Problem
Order-completion events concurrently aa rahe hain. Ek Java program banao jo teen approaches compare kare:
1. Galat concurrent `volatile long` counter.
2. Exact per-JVM `AtomicLong` counter.
3. High-throughput metric ke liye `LongAdder`.

32 worker threads chalao; har worker 100,000 events record kare. Sab workers complete hone ke baad explain karo kaunse results expected total ke barabar hone chahiye aur kyun.

### Design tak kaise pahunchte hain?
- **Race demonstrate karni hai:** volatile increment ko jaan-bujhkar broken rakhenge. Production accounting mein aisa mat karna.
- **Exact atomic increment:** `AtomicLong.incrementAndGet()` use karenge.
- **Scalable metric:** `LongAdder.increment()` use karenge. High contention mein updates internal variables mein spread ho sakte hain, jis se throughput improve hota hai. Iske badle extra memory lag sakti hai. Concurrent updates ke dauran `sum()` atomic snapshot nahi hota.
- **Final comparison trustworthy ho:** total read karne se pehle har task complete hona chahiye. `Future.get()` ka wait ensure karta hai ki final read ke waqt worker updates chal nahi rahe.

### Complete runnable Java 21 example

File ka naam rakho `CounterComparison.java`:

```java
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;

public class CounterComparison {

    private static final int THREADS = 32;
    private static final int EVENTS_PER_THREAD = 100_000;

    // Concurrent increments ke liye jaan-bujhkar incorrect.
    private volatile long brokenCount;

    // Is JVM mein exact atomic counter.
    private final AtomicLong exactCount = new AtomicLong();

    // High-contention statistics counter.
    private final LongAdder metricCount = new LongAdder();

    private void recordSuccess() {
        brokenCount++;              // read-modify-write race
        exactCount.incrementAndGet();
        metricCount.increment();
    }

    public static void main(String[] args) throws Exception {
        CounterComparison demo = new CounterComparison();
        ExecutorService pool = Executors.newFixedThreadPool(THREADS);
        List<Future<?>> futures = new ArrayList<>();

        for (int t = 0; t < THREADS; t++) {
            futures.add(pool.submit(() -> {
                for (int i = 0; i < EVENTS_PER_THREAD; i++) {
                    demo.recordSuccess();
                }
            }));
        }

        try {
            // Har worker ka wait karo aur task failures propagate karo.
            for (Future<?> future : futures) {
                future.get();
            }
        } finally {
            pool.shutdown();
        }

        long expected = (long) THREADS * EVENTS_PER_THREAD;

        System.out.println("Expected:     " + expected);
        System.out.println("volatile++:   " + demo.brokenCount);
        System.out.println("AtomicLong:   " + demo.exactCount.get());
        System.out.println("LongAdder:    " + demo.metricCount.sum());
    }
}
```

Expected total: **3,200,000**.
- `AtomicLong` ko 3,200,000 report karna chahiye.
- `LongAdder.sum()` ko bhi 3,200,000 report karna chahiye, kyunki read se pehle saare workers finish ho chuke hain.
- Volatile counter ke liye expected total ki guarantee nahi hai. Observed value run aur hardware ke hisaab se badlegi; ye assume mat karo ki har possible run mein value zaroor lower hi hogi.

Fixed thread pool worker count ko bounded rakhta hai. Main lesson counter ki concurrency semantics hai, web server ke liye executor choose karna nahi.

### Production mein kya choose karein?
Request-rate dashboards, successful/failed operation counts aur telemetry ke liye `LongAdder` achha fit ho sakta hai. Monitoring scrape agar updates ke beech mein hota hai, toh snapshot mein kuch concurrent increments abhi include na hue hon.

JVM ke andar unique sequence number chahiye toh `AtomicLong.getAndIncrement()` useful hai. Lekin restart ke baad ya multiple replicas ke across durable/global sequence provide nahi karta.

Agar rule hai “saare instances milakar 100 credits se zyada charge nahi hone chahiye,” toh local `AtomicLong` par bharosa mat karo. Har replica ka apna memory space hai. Invariant ko authoritative shared system mein enforce karo—jaise transaction ke andar conditional database update.

## 3. Walkthrough, mistakes aur trade-offs — 4 minutes

### Race mein hota kya hai?
Counter 10 se start hota hai:

| Step | Thread A | Thread B |
|---|---|---|
| 1 | 10 read karta hai | |
| 2 | | 10 read karta hai |
| 3 | 11 calculate karta hai | 11 calculate karta hai |
| 4 | 11 write karta hai | |
| 5 | | 11 write karta hai |

Do increments attempt hue, lekin final value 12 ki jagah 11 hai.

Atomic increment mein dono updates ek doosre ke relative indivisible hote hain; ek call 11 aur doosri 12 produce karegi.

### Common mistakes
1. **“Volatile hai, toh thread-safe hai.”** Galat—compound operation atomic nahi hota.
2. **Hard limit ke liye `LongAdder` use karna.** Concurrent updates ke dauran `sum()` atomic snapshot nahi hai. Isliye ise akela admission control ya exact quota enforce karne ke liye use mat karo.
3. **`AtomicLong` ko distributed samajhna.** Ye ek shared object ke threads ko coordinate karta hai, alag JVMs ko nahi.
4. **Workers finish hone se pehle result read karna.** Metric ka badhna abhi normal ho sakta hai. Exercise mein sab futures ka wait kiya hai.
5. **Metric ko source of truth banana.** Metrics reset, sample ya restart par lose ho sakte hain. Business accounting durable authoritative system mein honi chahiye.
6. **Contention ignore karna.** Ek `AtomicLong` bahut high write contention mein hot spot ban sakta hai. `LongAdder` statistical-counter throughput improve kar sakta hai, lekin zyada memory leta hai aur live snapshot semantics weaker hain.

### Interview-ready answer
> Volatile field individual reads aur writes ki visibility aur ordering deta hai, lekin increment compound read-modify-write operation hai aur lost updates ho sakte hain. JVM ke andar har update atomic hona chahiye toh AtomicLong use karunga. High-contention telemetry ke liye LongAdder choose karunga, jab perfectly atomic live snapshot zaroori nahi hai. Agar invariant multiple service replicas mein apply hota hai ya restart ke baad bhi survive karna chahiye, toh enforcement ko durable shared system—jaise transactional database conditional update—mein rakhunga.

### Follow-up questions aur answers

**Q1. Kya `AtomicLong` do application instances ke beech unique sequence guarantee karta hai?**  
Nahi. Har JVM ka apna object aur sequence hai. Global uniqueness chahiye toh shared sequence service ya database-generated identifier use karo.

**Q2. Kya `LongAdder.sum()` hamesha galat hota hai?**  
Nahi. Statistics ke liye useful hai. API ke mutabik concurrent updates ke dauran returned sum atomic snapshot nahi hota. Updates rukne ke baad accumulated total read kiya ja sakta hai.

**Q3. Kya `volatile long` ko `synchronized` se replace kar sakte hain?**  
Haan. Synchronized increment method compound operation ko mutually exclusive bana sakta hai, agar saare accesses same locking discipline follow karein. Hot counter par serialization cost ho sakti hai; requirement ke hisaab se atomics ya LongAdder better fit ho sakte hain.

**Q4. Kya correct in-memory counter order completion ko durable bana deta hai?**  
Nahi. Ye sirf process ne observe kiye events count karta hai. Order state ko transactionally persist karo aur event delivery/idempotency ko separately design karo.

## Primary references
- Java SE 21 JLS `17.4.5 — happens-before: https://docs.oracle.com/javase/specs/jls/se21/html/jls-17.html#jls-17.4.5
- Java SE 21 `AtomicLong` API: https://docs.oracle.com/en/java/javase/21/docs/api/java.base/java/util/concurrent/atomic/AtomicLong.html
- Java SE 21 `LongAdder` API: https://docs.oracle.com/en/java/javase/21/docs/api/java.base/java/util/concurrent/atomic/LongAdder.html
- Java SE 21 concurrency package: https://docs.oracle.com/en/java/javase/21/docs/api/java.base/java/util/concurrent/package-summary.html

# Advanced Java Backend Practice — 2026-10-21
## Virtual Threads ka matlab unlimited database capacity nahi hota

**Audience:** Experienced Java/Spring/AWS backend developer  
**Runtime:** Java 21 (virtual threads Java 21 mein final feature hain — JEP 444)  
**Plan:** 3-minute concept · 8-minute exercise · 4-minute review

## 1. Concept check (3 minutes)

**Question.** Sale ke time order API par 8,000 concurrent requests aa jaati hain. Team `Executors.newVirtualThreadPerTaskExecutor()` use karti hai, lekin HikariCP pool max 32 connections ka hai. Latency aur pool wait phir bhi badh rahe hain. Kya virtual threads database bottleneck solve kar denge?

**Answer.** Nahi. Virtual threads un tasks ko kam overhead ke saath represent karte hain jo kaafi time wait karte hain. Lekin ye database ki query capacity, connection count, CPU, memory ya downstream quotas increase nahi karte. Java 21 ka virtual-thread-per-task executor unbounded number mein virtual threads bana sakta hai. Isliye virtual threads ko pool karke resource limit karne ke bajaye, scarce resource ke around explicit admission control lagao.

Example: HikariCP max 32 connections ka hai toh repository ke liye 24 permits ka semaphore is code path ki concurrent DB activity cap kar sakta hai, aur doosre paths ke liye planned headroom chhod sakta hai. Lekin doosre consumers aur replica count ka budget bhi calculate karo. Ek JVM ka semaphore poore cluster ki global limit nahi hota.

**Hinglish example:** Bahut saare waiting customers ko handle karna kitchen ki cooking capacity nahi badhata. Database ko ek time par kitne operations dene hain, uski limit alag se set karni padegi.

Primary references: [JEP 444: Virtual Threads](https://openjdk.org/jeps/444), [Java 21 Executors API](https://docs.oracle.com/en/java/javase/21/docs/api/java.base/java/util/concurrent/Executors.html), [Java 21 Semaphore API](https://docs.oracle.com/en/java/javase/21/docs/api/java.base/java/util/concurrent/Semaphore.html).

## 2. Practical exercise aur complete solution (8 minutes)

### Requirements
- Java 21; blocking JDBC allowed hai.
- HikariCP max pool size = 32.
- Is repository ke maximum 24 DB operations ek saath active hon.
- Permit ke liye max 75 ms wait; permit na mile toh overload exception throw karo. HTTP layer ise 503 map kare; 429 tab jab caller quota exceed hua ho.
- One-second statement timeout aur finite connection-pool acquisition timeout configure karo.
- Success, SQL failure ya runtime failure—har path par permit release ho.
- Unrelated remote calls ya CPU-heavy work ke dauraan permit hold mat karo.

### Solution tak kaise pahunchte hain?
1. **Scarce resource identify karo:** database/query capacity, virtual thread nahi.
2. **Permit pehle lo, connection baad mein:** warna connection le kar admission ka wait karoge aur scarce connection waste hoga.
3. **Wait bounded rakho:** `tryAcquire(timeout)` unlimited parked-request queue se bachata hai.
4. **Release guaranteed rakho:** `finally` SQL exceptions, mapping errors aur resource-close failures ke baad bhi chalega.
5. **Alag safeguards rakho:** semaphore is repository ka concurrency cap hai; Hikari connection count limit karta hai; pool aur query timeouts alag waits cover karte hain.
6. **Overload ko “not found” mat banao:** rejected work ka matlab order missing nahi hai.

### Minimal Java 21 repository

Spring mein `DataSource` ko configured HikariCP bean se inject karo. Table/column names example hain.

```java
import javax.sql.DataSource;
import java.sql.*;
import java.util.Optional;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

public final class OrderRepository {
    private static final long ADMISSION_WAIT_MS = 75;
    private static final int QUERY_TIMEOUT_SECONDS = 1;

    // Pool max 32; doosre consumers ke liye headroom.
    private final Semaphore dbSlots = new Semaphore(24);
    private final DataSource dataSource;

    public OrderRepository(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    public Optional<Order> findById(long orderId)
            throws SQLException, InterruptedException {
        // Yahan interrupt hua toh permit acquire nahi hua.
        if (!dbSlots.tryAcquire(ADMISSION_WAIT_MS, TimeUnit.MILLISECONDS)) {
            throw new DependencyOverloadedException(
                "Database concurrency limit reached");
        }

        try (Connection c = dataSource.getConnection();
             PreparedStatement ps = c.prepareStatement(
                 "SELECT id, status FROM orders WHERE id = ?")) {
            ps.setLong(1, orderId);
            ps.setQueryTimeout(QUERY_TIMEOUT_SECONDS);

            try (ResultSet rs = ps.executeQuery()) {
                if (!rs.next()) return Optional.empty();
                return Optional.of(new Order(
                    rs.getLong("id"), rs.getString("status")));
            }
        } finally {
            dbSlots.release();
        }
    }

    public record Order(long id, String status) {}

    public static final class DependencyOverloadedException
            extends RuntimeException {
        public DependencyOverloadedException(String message) {
            super(message);
        }
    }
}
```

### Virtual-thread usage (demonstration)

```java
import java.util.concurrent.*;

try (ExecutorService executor =
         Executors.newVirtualThreadPerTaskExecutor()) {
    var future = executor.submit(() -> repository.findById(42L));
    System.out.println(future.get(2, TimeUnit.SECONDS));
}
```

Ye demo hai; har HTTP request ke liye naya executor create karne ki salah nahi. Server mein framework/server ka lifecycle-managed virtual-thread support ya ek managed executor use karo. DB access ko limit semaphore karta hai, executor nahi.

## 3. Walkthrough, edge cases aur trade-offs (4 minutes)

### Dry run
- Pehli 24 calls permit le leti hain aur DB work kar sakti hain.
- 25th call max 75 ms wait karti hai.
- Agar is time mein koi permit release hota hai toh 25th call proceed karti hai.
- Warna overload exception milti hai; unlimited queue mein wait nahi hota.
- SQL exception aaye tab bhi `finally` permit release karta hai, isliye capacity leak nahi hoti.

### Common mistakes
1. **“Virtual threads cheap hain, toh unlimited requests safe hain.”** Har task request state, buffers aur objects retain karta hai. Database aur downstream systems phir bhi bounded hain.
2. **24 virtual threads ka fixed pool bana dena.** JEP 444 virtual threads ko pool na karne ki salah deta hai. Resource limit explicit semaphore mein rakho.
3. **Connection permit se pehle lena.** Admission wait ke dauraan DB connection occupied reh sakta hai.
4. **Permit sirf success par release karna.** Exception par permit leak hoga. `finally` use karo.
5. **Semaphore ko global limit samajhna.** 4 replicas × 24 permits = 96 concurrent operations possible. Total DB budget ke hisaab se per-instance cap set karo ya shared limiter use karo.
6. **JDBC timeout ko strict end-to-end deadline samajhna.** Driver cancellation behavior differ kar sakta hai. Pool acquisition timeout aur driver-supported statement/network timeouts configure karke actual driver ke saath test karo.
7. **Permit ko zyada der hold karna.** DB operation khatam hote hi release kar do.

### Spring transaction caveat
`@Transactional` interceptor method body chalne se pehle connection acquire kar sakta hai. Agar permit method ke andar acquire hua toh connection pehle se hold ho sakta hai. Admission ko transaction boundary ke bahar rakho—for example outer non-transactional bean permit le aur separate proxied transactional bean ko call kare—or apne transaction manager ke exact behavior ko test karo. Sirf annotation order par bharosa mat karo.

### Production checklist
Permit wait/rejections, active DB operations, Hikari active/idle/pending connections, query latency, timeout count, HTTP latency aur 5xx rate monitor karo. 24 permits aur 75 ms example values hain; realistic load test se tune karo.

### Interview-ready summary
“Virtual threads blocking-code scalability improve karte hain, database capacity nahi. Main request concurrency ke liye virtual threads use karunga, lekin DB access ko pool aur total DB budget ke hisaab se timed semaphore se limit karunga. Permit connection se pehle acquire hoga aur `finally` mein release hoga. Pool/query timeouts configure honge, overload ko explicit response milega, aur admission plus pool metrics monitor karunga. Semaphore per JVM hai, isliye replica count aur doosre DB consumers ko include karna zaroori hai.”

### Follow-ups (answers ke saath)
- **Hikari pool size 8,000 kyun na kar dein?** DB itni concurrency handle na kar paaye; extra connections contention, memory aur context switching badha sakte hain.
- **Semaphore wait virtual thread mein carrier ko block karta hai?** Supported blocking operation virtual thread ko park kar sakta hai bina carrier ko zaroori taur par occupy kiye. Phir bhi waiter memory/time leta hai, isliye bounded admission important hai.
- **Permits pool size ke equal hone chahiye?** Zaroori nahi. Jobs, health checks aur doosre repositories bhi pool use karte hain; headroom rakho.
- **Permit ke baad request cancel ho gayi?** Java exception/interruption par `finally` permit release karega. Lekin HTTP cancellation se JDBC query stop hona guaranteed nahi; driver behavior verify karo.

## Sources
- [OpenJDK JEP 444 — Virtual Threads](https://openjdk.org/jeps/444)
- [Java 21 Executors API](https://docs.oracle.com/en/java/javase/21/docs/api/java.base/java/util/concurrent/Executors.html)
- [Java 21 Semaphore API](https://docs.oracle.com/en/java/javase/21/docs/api/java.base/java/util/concurrent/Semaphore.html)
- [Java 21 ExecutorService API](https://docs.oracle.com/en/java/javase/21/docs/api/java.base/java/util/concurrent/ExecutorService.html)

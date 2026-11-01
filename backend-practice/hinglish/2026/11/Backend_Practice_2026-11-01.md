# Advanced Java Backend Practice — Session 10 (Hinglish)

**Date:** 2026-11-01  
**Topic:** Resilient distributed-system design — cache stampede ko single-flight, stale-while-revalidate, TTL jitter aur bounded refresh se control karna  
**Runtime:** Java 21, Spring Boot 3.x, Caffeine 3.x  
**Level:** Advanced backend engineering  
**Timebox:** 3-minute concept, 8-minute implementation, 4-minute solution derivation aur trade-offs

## Pehle kya aana chahiye?

Tumhe Java concurrency, ConcurrentHashMap, CompletableFuture, thread pools, HTTP/database timeouts aur cache-aside pattern ka basic idea hona chahiye. Neeche ka code ek focused cache component hai. Domain DTOs aur repository ka actual implementation tumhare project ke hisaab se hoga.

## Production scenario aur interview question

Maan lo tumhari product-catalogue API Kubernetes par 12 replicas mein chal rahi hai. Ek popular product key ko 2,000 requests per second milte hain. Cache entry expire ho jaati hai, database se reload hone mein 900 ms lagte hain, aur saare replicas ek saath database queries bhejne lagte hain. Database latency badhti hai, requests timeout hoti hain, aur retries aur load badha deti hain.

**Aisa cache design karo jo:**

1. Ek hi JVM mein same key ke concurrent reloads ko ek load mein coalesce kare.
2. Jab business stale read allow karta ho, refresh ke dauraan limited age tak purani value return kare.
3. Bahut saari alag expired keys ek saath aane par unbounded background work start na kare.
4. Cache memory ko bounded rakhe.
5. Cold miss par refresh capacity khatam ho toh predictable failure de.
6. Ek HTTP request ka timeout shared refresh ko cancel na kare.
7. Multiple application replicas hone par kya change hoga, ye clearly explain kare.

Example starting configuration: fresh TTL 60 seconds, stale window 30 seconds, cold-miss wait 250 ms, refresh pool 4 workers, queue capacity 128, aur cache maximum 100,000 entries. Ye sirf starting values hain; real traffic par tune karna padega.

---

# Part 1 — Concept samjho (3 minutes)

## 1. Cache stampede hota kya hai?

Maan lo ek popular key ka cache expire ho gaya. Ab ek hi time par bahut saari requests us key ko read karne aati hain. Sabko cache miss milta hai aur sab source database/API ko call kar deti hain.

Isko cache stampede ya thundering herd kehte hain.

Without coordination:

- 2,000 requests same product key par miss karti hain.
- 2,000 database reads start ho sakti hain.
- Database slow hone lagta hai.
- API timeouts badhte hain.
- Clients retry karte hain.
- Source par aur zyada load aa jata hai.

Yaani jis moment par cache database ko bachana chahiye tha, usi moment par woh miss ki wajah se database ko overload kar sakta hai.

## 2. Requirement se solution derive karo

| Requirement | Mechanism | Kya problem solve hogi |
|---|---|---|
| Same key ke multiple callers | Per-key single-flight | Ek source load share hoga |
| Thodi der old value acceptable hai | Stale-while-revalidate | Refresh ke dauraan stale value turant return hogi |
| Kai keys ek saath expire hoti hain | TTL jitter | Expiry alag-alag time par spread hogi |
| Source capacity limited hai | Bounded executor + queue | Active aur pending refresh count control hoga |
| Cache memory limited honi chahiye | Caffeine maximum size/weight | Local memory bounded rahegi |
| Cold miss indefinitely wait nahi karna chahiye | Timed wait | Caller latency bounded rahegi |
| Multiple replicas hain | Shared cache/coordination, jab justified ho | Cross-replica duplicate work kam hoga |

**Bahut important:** Local single-flight sirf ek JVM ke threads ko coordinate karta hai. Agar 12 replicas hain, toh same key ke liye abhi bhi maximum 12 local refreshes ho sakte hain. Ye 2,000 loads se kaafi better hai, lekin global single-flight guarantee nahi hai.

## 3. Ek simple example

Socho ek popular product ka cache 60 seconds baad stale ho gaya. 2,000 customers same product dekhna chahte hain.

Agar har customer database ko call kare, toh source par unnecessary load aayega.

Humein rule banana hai:

**Ek key ke liye ek refresh chalega; baaki requests allowed time tak purani value le sakti hain.**

Saath mein refresh queue bounded hogi, warna alag-alag expired keys milkar background workers ko overload kar sakti hain.

---

# Part 2 — Practical implementation (8 minutes)

## Step 1 — Cache ki 3 states banao

| State | Behaviour |
|---|---|
| Fresh | Value turant return |
| Stale, lekin allowed stale window ke andar | Purani value turant return, background refresh start/join |
| Missing ya stale window cross ho gaya | Ek shared load join karo aur limited time tak wait karo |

TTL age calculate karne ke liye Java mein System.nanoTime() use karenge. Ye elapsed time measure karne ke liye monotonic clock hai. In-process TTL age ke liye wall-clock time use karne ki zaroorat nahi.

## Step 2 — Bounded refresh executor configure karo

Sirf Executors.newFixedThreadPool() use mat kar dena. Uska default work queue unbounded hota hai. Humein worker count aur queue capacity dono control karne hain.

~~~java
@Configuration
class CacheRefreshConfiguration {

    @Bean(destroyMethod = "shutdown")
    ThreadPoolExecutor catalogRefreshExecutor() {
        return new ThreadPoolExecutor(
            4,
            4,
            0L,
            TimeUnit.MILLISECONDS,
            new ArrayBlockingQueue<>(128),
            Thread.ofPlatform()
                .name("catalog-refresh-", 0)
                .factory(),
            new ThreadPoolExecutor.AbortPolicy()
        );
    }
}
~~~

Yahan AbortPolicy ka fayda kya hai?

Jab 4 workers aur queue ke 128 slots bhar jaate hain, toh naya task RejectedExecutionException dega.

- Agar cache entry stale hai, hum purani allowed value phir bhi return kar sakte hain.
- Agar cache mein value hi nahi hai, request ko fast fail kar sakte hain.

CallerRunsPolicy yahan risky hai. Woh rejected background refresh ko current caller thread par chala sakti hai. Iska matlab database loading HTTP request thread par chalne lagegi. Hum background isolation chahte the, lekin woh khatam ho gaya.

## Step 3 — Per-key single-flight aur stale-while-revalidate implement karo

Dependency: project ki dependency-management policy ke according current approved Caffeine 3.x version use karo.

~~~java
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;

import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.*;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.BiConsumer;

public final class StaleWhileRevalidateCache<K, V> {

    @FunctionalInterface
    public interface Loader<K, V> {
        V load(K key) throws Exception;
    }

    private static final class Entry<V> {
        final V value;
        final long loadedAtNanos;
        final long freshForNanos;
        final long staleForNanos;

        Entry(V value, long loadedAtNanos,
              long freshForNanos, long staleForNanos) {
            this.value = value;
            this.loadedAtNanos = loadedAtNanos;
            this.freshForNanos = freshForNanos;
            this.staleForNanos = staleForNanos;
        }

        boolean isFresh(long now) {
            long age = now - loadedAtNanos;
            return age >= 0 && age < freshForNanos;
        }

        boolean isStaleButUsable(long now) {
            long age = now - loadedAtNanos;
            long staleUntil = Math.addExact(
                freshForNanos, staleForNanos);

            return age >= freshForNanos && age < staleUntil;
        }
    }

    private final Cache<K, Entry<V>> entries;
    private final ConcurrentMap<K, CompletableFuture<Entry<V>>> inFlight =
        new ConcurrentHashMap<>();

    private final Executor refreshExecutor;
    private final Loader<K, V> loader;
    private final BiConsumer<K, Throwable> failureObserver;

    private final long baseFreshTtlNanos;
    private final long staleWindowNanos;
    private final long missWaitNanos;
    private final double jitterFraction;

    public StaleWhileRevalidateCache(
            long maximumEntries,
            Duration freshTtl,
            Duration staleWindow,
            Duration missWait,
            double jitterFraction,
            Executor refreshExecutor,
            Loader<K, V> loader,
            BiConsumer<K, Throwable> failureObserver) {

        if (maximumEntries < 1) {
            throw new IllegalArgumentException(
                "maximumEntries must be positive");
        }

        this.baseFreshTtlNanos =
            positiveNanos(freshTtl, "freshTtl");
        this.staleWindowNanos =
            positiveNanos(staleWindow, "staleWindow");
        this.missWaitNanos =
            positiveNanos(missWait, "missWait");

        if (jitterFraction < 0.0 || jitterFraction >= 1.0) {
            throw new IllegalArgumentException(
                "jitterFraction must be in [0, 1)");
        }

        this.jitterFraction = jitterFraction;
        this.refreshExecutor =
            Objects.requireNonNull(refreshExecutor);
        this.loader = Objects.requireNonNull(loader);
        this.failureObserver =
            Objects.requireNonNull(failureObserver);

        long maximumFresh = Math.round(
            baseFreshTtlNanos * (1.0 + jitterFraction));
        Math.addExact(maximumFresh, staleWindowNanos);

        this.entries = Caffeine.newBuilder()
            .maximumSize(maximumEntries)
            .build();
    }

    public V get(K key) {
        Objects.requireNonNull(key, "key");

        Entry<V> entry = entries.getIfPresent(key);
        long now = System.nanoTime();

        if (entry != null && entry.isFresh(now)) {
            return entry.value;
        }

        if (entry != null && entry.isStaleButUsable(now)) {
            startLoad(key);
            return entry.value;
        }

        return awaitLoad(key, startLoad(key));
    }

    private CompletableFuture<Entry<V>> startLoad(K key) {
        CompletableFuture<Entry<V>> mine = new CompletableFuture<>();

        CompletableFuture<Entry<V>> existing =
            inFlight.putIfAbsent(key, mine);

        if (existing != null) {
            return existing;
        }

        try {
            refreshExecutor.execute(() -> {
                try {
                    V value = Objects.requireNonNull(
                        loader.load(key),
                        "Loader must not return null");

                    Entry<V> replacement = new Entry<>(
                        value,
                        System.nanoTime(),
                        jitteredFreshTtlNanos(),
                        staleWindowNanos
                    );

                    entries.put(key, replacement);
                    mine.complete(replacement);

                } catch (Throwable error) {
                    mine.completeExceptionally(error);
                    observeFailure(key, error);

                    if (error instanceof Error fatal) {
                        throw fatal;
                    }
                } finally {
                    inFlight.remove(key, mine);
                }
            });
        } catch (RejectedExecutionException rejected) {
            mine.completeExceptionally(rejected);
            inFlight.remove(key, mine);
            observeFailure(key, rejected);
        }

        return mine;
    }

    private V awaitLoad(
            K key,
            CompletableFuture<Entry<V>> future) {

        try {
            return future.get(
                missWaitNanos, TimeUnit.NANOSECONDS).value;

        } catch (TimeoutException timeout) {
            throw new CacheLoadTimeoutException(
                "Timed out waiting for cache load: " + key, timeout);

        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw new CacheLoadInterruptedException(
                "Interrupted waiting for cache load: " + key,
                interrupted);

        } catch (ExecutionException failed) {
            throw new CacheLoadFailedException(
                "Cache source load failed: " + key,
                failed.getCause());
        }
    }

    private long jitteredFreshTtlNanos() {
        if (jitterFraction == 0.0) {
            return baseFreshTtlNanos;
        }

        double factor = ThreadLocalRandom.current().nextDouble(
            1.0 - jitterFraction,
            1.0 + jitterFraction);

        return Math.max(
            1L, (long) (baseFreshTtlNanos * factor));
    }

    private void observeFailure(K key, Throwable error) {
        try {
            failureObserver.accept(key, error);
        } catch (RuntimeException ignored) {
            // Application logger ke through observer failure record karo.
        }
    }

    private static long positiveNanos(
            Duration duration, String name) {
        Objects.requireNonNull(duration, name);

        if (duration.isZero() || duration.isNegative()) {
            throw new IllegalArgumentException(
                name + " must be positive");
        }

        return duration.toNanos();
    }

    public static final class CacheLoadTimeoutException
            extends RuntimeException {
        CacheLoadTimeoutException(String message, Throwable cause) {
            super(message, cause);
        }
    }

    public static final class CacheLoadInterruptedException
            extends RuntimeException {
        CacheLoadInterruptedException(String message, Throwable cause) {
            super(message, cause);
        }
    }

    public static final class CacheLoadFailedException
            extends RuntimeException {
        CacheLoadFailedException(String message, Throwable cause) {
            super(message, cause);
        }
    }
}
~~~

### Is implementation ka actual logic samjho

**A. Value cache aur in-flight map alag kyun hain?**

Caffeine actual cached values rakhta hai aur maximum entries ke through memory ko bound karta hai.

ConcurrentHashMap sirf currently running ya queued loads ka future rakhta hai. Load complete hote hi future remove ho jaata hai.

Yahan ek subtle line hai:

~~~java
inFlight.remove(key, mine);
~~~

Iska matlab: key ko tabhi remove karo jab map abhi bhi isi future ko hold kar raha ho.

Maan lo purana load complete hua, lekin tab tak naya future install ho chuka hai. Purana load agar sirf key se remove karega, toh galti se naye load ko bhi hata sakta hai. Conditional removal is race ko prevent karta hai.

**B. Stale request wait kyun nahi karti?**

Agar entry allowed stale window ke andar hai, toh request background refresh start karti hai aur immediately purani value return kar deti hai.

Same key ke doosre callers aayenge toh putIfAbsent unhe already-running future ke saath coordinate karega. Doosra source load start nahi hoga.

**C. Cold miss par timed get kyun?**

~~~java
future.get(missWaitNanos, TimeUnit.NANOSECONDS);
~~~

Agar cache mein usable value nahi hai toh caller ko result chahiye. Lekin woh indefinitely wait nahi kar sakta.

Timeout aaya toh sirf us particular caller ko exception milega. Shared future cancel nahi hoga. Load complete hokar cache populate kar sakta hai aur doosre waiters ko result mil sakta hai.

**D. TTL jitter aur single-flight alag problems solve karte hain.**

Single-flight same key ke callers ko coalesce karta hai.

Jitter bahut saari different keys ke expiry times ko spread karta hai.

**E. Caffeine maximumSize kyun?**

Agar sirf ConcurrentHashMap use karoge toh values continuously store hoti rahengi aur automatic memory limit nahi hogi.

Caffeine maximumSize local cache ko entry-count ke basis par bound karta hai. Agar values ke size mein bahut difference hai, toh maximumWeight aur weigher consider karo.

## Step 4 — Spring mein wire kaise karna hai?

~~~java
@Configuration
class ProductCacheConfiguration {

    @Bean
    StaleWhileRevalidateCache<String, ProductView> productCache(
            @Qualifier("catalogRefreshExecutor")
            ThreadPoolExecutor refreshExecutor,
            ProductRepository repository) {

        return new StaleWhileRevalidateCache<>(
            100_000,
            Duration.ofSeconds(60),
            Duration.ofSeconds(30),
            Duration.ofMillis(250),
            0.10,
            refreshExecutor,
            repository::loadProductView,
            (key, error) -> {
                // Micrometer counter aur safe logging.
                // Raw product ID ko high-cardinality metric label mat banao.
            }
        );
    }
}
~~~

Repository loader mein apna database connection-acquisition timeout aur query timeout configure karo. Cache refresh ko kabhi indefinitely DB ka wait nahi karne dena.

## Step 5 — Dry run samjho

Maan lo product P42 ke cache mein v1 pada hai.

| Time | Event | Result |
|---|---|---|
| T0 | 100 requests aati hain aur v1 fresh hai | Sabko v1 return |
| T1 | v1 stale window mein enter karta hai | Pehli request ek refresh schedule karti hai |
| T1 + 1 ms | 99 aur requests aati hain | Sabko v1 milta hai; doosra refresh schedule nahi hota |
| T1 + 500 ms | Source v2 return karta hai | Cache v2 se replace hota hai |
| T1 + 501 ms | Next request aati hai | Fresh v2 milta hai |

Agar T1 par refresh executor full hai, toh background refresh reject hoga aur failure observe hoga. Lekin stale window allowed hone ki wajah se caller ko v1 mil jayega.

Agar cache mein koi usable entry hi nahi hai aur executor reject karta hai, toh future exceptional complete hoga aur cold-miss caller fail fast karega.

---

# Part 3 — Solution kaise derive kiya, mistakes aur production trade-offs (4 minutes)

## 1. Failure matrix

| Failure | Expected behaviour |
|---|---|
| Source refresh successful | New value cache mein store, waiters ko result |
| Stale window mein refresh fail | Stale callers ko old value; failure observe hoga |
| Refresh executor reject kare | Stale value return; cold miss fail |
| Cold-miss waiter timeout | Sirf woh caller fail; shared load continue |
| Cache entry evict ho gayi | Agli request cold miss banegi |
| Application restart ho gaya | Local cache aur in-flight load disappear; next request reload karegi |
| 12 replicas same cold key read karein | Cross-replica coordination nahi toh up to 12 local loads ho sakte hain |

## 2. Common mistakes

1. **Sirf ConcurrentHashMap.computeIfAbsent ko cache samajhna.** Ye mapping operation deduplicate kar sakta hai, lekin memory bounds, TTL, stale serving aur source admission control nahi deta.
2. **Unbounded executor queue rakhna.** Bahut saari distinct expired keys aayengi toh pending refresh tasks unlimited grow kar sakte hain.
3. **CallerRunsPolicy lagana bina request threads ka effect soche.** Background refresh request thread par execute ho sakta hai.
4. **Shared in-flight future par future.orTimeout() lagana.** Isse shared future modify hota hai aur sab waiters fail ho sakte hain. Caller-specific timed get safer hai.
5. **Ek HTTP request disconnect ho toh shared refresh cancel kar dena.** Baaki requests ko bhi isi load ki zaroorat ho sakti hai.
6. **Har business domain mein stale value serve karna.** Product description stale ho sakta hai; lekin inventory, authorization, account balance, entitlement aur payment state mein stale data dangerous ho sakta hai.
7. **Local single-flight ko distributed guarantee samajhna.** Har pod ka apna in-memory map hota hai.

## 3. Production trade-offs aur hardening

### Stale-data policy

Maximum stale age business decide karega. Sample mein configured stale window cross hone ke baad old value return nahi hoti.

Inventory, payment, account balance aur permissions jaise domains mein stale data avoid karo, ya explicit consistency contract define karo.

### Source failures aur repeated attempts

Single-flight ek in-flight load ke dauraan duplicate concurrent requests rokta hai. Lekin agar loader jaldi fail ho jaata hai, toh next request phir se load start kar sakti hai.

Isliye source loader ke around circuit breaker aur/or per-key failure cooldown with jitter lagao.

Circuit open hone par stale value tabhi do jab domain policy allow kare. Cold miss par fast fail karo.

### Multiple application replicas

Agar ek pod mein single-flight hai, toh 12 pods ke across 12 refreshes still possible hain.

Shared cache aur distributed refresh lease consider kar sakte ho, lekin distributed lock ko blindly correctness guarantee mat samajhna.

Redis lease ke liye common pattern:

~~~text
SET lock:key unique-token NX PX lease-time
~~~

Release karte waqt stored token compare karke hi delete karna chahiye.

Lekin maan lo old owner ka lease expire ho gaya aur old worker abhi bhi source se result laane mein busy hai. New owner lock le sakta hai aur newer result publish kar sakta hai. Purana worker baad mein aakar old result overwrite kar sakta hai.

Agar aisa overwrite correctness ko affect karta hai, toh monotonic fencing/version token use karo. Protected datastore ko older token wale writes reject karne chahiye.

### Monitoring mein kya dekho?

- Fresh hit, stale hit aur cold-miss rate.
- Single-flight joins/coalesced callers.
- Refresh success, failure, rejection aur source latency.
- Refresh pool active worker count aur queue depth.
- Cache size, eviction aur estimated memory.
- Serve ki gayi value ki age aur stale response count.
- Cold-miss timeout.
- Database pool wait aur source query latency.

Raw product IDs ko metric labels mat banao. High-cardinality metrics monitoring system ko expensive bana sakte hain. Key-level details ke liye sampled logs/traces better hain.

### Testing strategy

Controllable loader aur latches ke saath test karo:

1. Cache mein v1 load karo.
2. Test clock advance karo ya short test TTL rakho.
3. Refresh loader ko latch par block kar do.
4. Same key ke 100 concurrent stale reads bhejo.
5. Assert karo ki sabko v1 mila aur loader invocation count sirf ek badha.
6. Latch release karo, phir verify karo ki next read v2 return karta hai.
7. Refresh executor aur queue full karo. Verify karo stale reads still return karte hain, cold misses fail.
8. Loader exception force karo, verify failure observer call hua aur stale policy correct rahi.

TTL tests ke liye real sleep par dependent mat raho. Caffeine custom Ticker support karta hai. Apne implementation mein injectable monotonic time supplier rakh kar deterministic tests bana sakte ho.

## 4. Interview-ready answer

> Main cache stampede ko sirf cache-miss problem nahi, balki source-admission problem maanta hoon. Ek JVM ke andar per-key single-flight future use karta hoon taaki concurrent callers ek source load share karein. Jahan bounded stale data acceptable hai, stale-while-revalidate use karta hoon: old value immediately return hoti hai aur refresh background mein run hota hai. TTL jitter different keys ki expiry spread karta hai, Caffeine local memory bound karta hai, aur bounded refresh executor active aur queued work limit karta hai. Cold miss par caller-specific timeout rakhta hoon; ek caller ka timeout shared load cancel nahi karta. Main clearly bolunga ki local single-flight multiple replicas coordinate nahi karta. Cross-pod duplicate work expensive ho toh shared cache/lease consider karunga. Agar lease correctness protect kar raha hai, toh protected datastore par fencing/version check bhi enforce karna hoga.

## 5. Follow-up questions aur answers

**Q1. Sirf Caffeine refreshAfterWrite kyun nahi use karte?**

Caffeine LoadingCache ka refresh asynchronous hota hai. Refresh ke time old value available rehti hai aur same key ke in-flight refreshes deduplicate hote hain. Ye kaafi cases mein best simple production choice hai.

Custom implementation yahan isliye dikhaya hai kyunki humein alag-alag policies explicitly control karni thi: per-entry TTL jitter, separate stale window, bounded refresh executor aur caller-specific cold-miss wait.

Agar library ka native loader/refresh tumhari requirements satisfy karta hai, toh custom implementation likhne ki jagah Caffeine ka native feature prefer karo.

**Q2. Kya single-flight cross-pod stampede solve karta hai?**

Nahi. Ye process-local hai. N replicas hain toh ek hot key par approximately N concurrent reloads ho sakte hain. Shared cache aur zaroorat padne par distributed coordination use kar sakte ho.

**Q3. remove(key, mine) safer kyun hai?**

Old future complete hone tak map mein newer future aa sakta hai. Conditional remove ensure karta hai ki old future newer in-flight entry ko delete nahi kare.

**Q4. Ek request timeout ho toh shared future cancel kyun nahi karte?**

Future multiple callers ke beech shared hai. Ek caller ke deadline ka matlab ye nahi ki doosre callers ka work cancel kar do. CompletableFuture cancellation exceptional completion hai; underlying source computation ko automatically stop karne ki guarantee nahi hai.

**Q5. TTL jitter hot-key stampede prevent karta hai?**

Akele nahi. Ye different keys ki expiry spread karta hai. Same key ke concurrent callers ko coalesce karne ka kaam single-flight karta hai.

**Q6. Kya stale-while-revalidate inventory ke liye use kar sakte hain?**

Sirf tab jab business explicitly bounded staleness allow karta ho. Inventory reservation, payment, permissions aur account balances ke liye authoritative conditional writes/reads ya domain-specific consistency protocol chahiye.

---

## Primary references

1. Java 21 ThreadPoolExecutor API — bounded queues aur rejection policy: https://docs.oracle.com/en/java/javase/21/docs/api/java.base/java/util/concurrent/ThreadPoolExecutor.html
2. Java 21 CompletableFuture API — executor, copy, cancellation semantics aur timeouts: https://docs.oracle.com/en/java/javase/21/docs/api/java.base/java/util/concurrent/CompletableFuture.html
3. Caffeine — refresh ke waqt old value, async refresh aur in-flight deduplication: https://github.com/ben-manes/caffeine/wiki/Refresh
4. Caffeine — size-based aur time-based eviction: https://github.com/ben-manes/caffeine/wiki/Eviction
5. Redis distributed locking — tokenized lease aur ownership-aware release: https://redis.io/docs/latest/develop/clients/patterns/distributed-locks/

**Final takeaway:** Single-flight ek key ke duplicate work ko control karta hai, stale-while-revalidate caller latency ko control karta hai, TTL jitter keys ki expiry spread karta hai, aur bounded executor source ko overload se protect karta hai. Ye mechanisms ek-doosre ke alternatives nahi hain; ye milkar kaam karte hain.

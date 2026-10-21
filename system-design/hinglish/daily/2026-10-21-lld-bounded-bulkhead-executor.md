# LLD Interview — Bounded Async Bulkhead Executor (Proper Hinglish)
**Date:** 2026-10-21 | **Level:** SDE2 / Senior Java

## 1. Scenario samjho

Socho tumhari Spring Boot service inventory API ko call karti hai. Normal time par response 50 ms mein aata hai, lekin provider slow ho gaya aur request 5 second wait karne lagi. Agar har incoming request ke liye unlimited tasks queue karte gaye, toh memory bhar sakti hai aur poori application slow ho sakti hai.

Bulkhead ship ke compartments jaisa hai: ek compartment mein paani aaye toh poora ship doobna nahi chahiye. Yahan ek downstream dependency ki capacity limit baaki service ko protect karti hai.

Requirement: maximum W active calls, maximum Q queued calls. Dono full hon toh turant reject karo. Unlimited queue solution nahi hai.

## 2. Design kaise derive hua?

- Fixed worker count active concurrency limit karta hai.
- Bounded queue waiting work limit karti hai.
- AbortPolicy overload par clear rejection deta hai.
- CompletableFuture caller ko result/failure asynchronously deta hai.
- Timeout aur cancellation ko actual HTTP client ke saath coordinate karna hoga; future cancel karne se remote server automatically rukta nahi.

## 3. Java 17 implementation

File ka naam <code>BulkheadExecutor.java</code> rakho.

~~~java
import java.util.Objects;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

public final class BulkheadExecutor implements AutoCloseable {
    private final ThreadPoolExecutor pool;
    private final AtomicBoolean closed = new AtomicBoolean();

    public BulkheadExecutor(int workers, int queueCapacity, String name) {
        if (workers <= 0 || queueCapacity <= 0)
            throw new IllegalArgumentException("capacities must be positive");
        Objects.requireNonNull(name);
        pool = new ThreadPoolExecutor(
            workers, workers, 0L, TimeUnit.MILLISECONDS,
            new ArrayBlockingQueue<>(queueCapacity),
            r -> new Thread(r, name + "-worker"),
            new ThreadPoolExecutor.AbortPolicy());
    }

    public <T> CompletableFuture<T> submit(Callable<T> task) {
        Objects.requireNonNull(task);
        CompletableFuture<T> result = new CompletableFuture<>();
        if (closed.get()) {
            result.completeExceptionally(
                new RejectedExecutionException("bulkhead is closed"));
            return result;
        }
        try {
            pool.execute(() -> {
                try {
                    if (!result.isCancelled()) result.complete(task.call());
                } catch (Throwable failure) {
                    result.completeExceptionally(failure);
                    if (failure instanceof Error error) throw error;
                }
            });
        } catch (RejectedExecutionException ex) {
            result.completeExceptionally(ex);
        }
        return result;
    }

    public int activeCount() { return pool.getActiveCount(); }
    public int queuedCount() { return pool.getQueue().size(); }

    public boolean shutdownGracefully(long timeout, TimeUnit unit)
            throws InterruptedException {
        closed.set(true);
        pool.shutdown();
        if (pool.awaitTermination(timeout, unit)) return true;
        pool.shutdownNow();
        return pool.awaitTermination(timeout, unit);
    }

    @Override public void close() {
        closed.set(true);
        pool.shutdown();
    }
}
~~~

## 4. Code ko samjho

**Fixed pool:** workers=8 hai toh ek waqt par max 8 tasks run karenge. Number downstream capacity aur latency dekhkar choose karo.

**Bounded queue:** queueCapacity=32 matlab active workers ke alawa 32 tasks wait kar sakte hain. Total accepted-but-not-finished work approximately 40 tak bounded hai.

**AbortPolicy:** Queue full hui toh submission reject hoti hai. Caller fallback, HTTP 429/503 ya retry-after decide kar sakta hai. CallerRunsPolicy backpressure de sakti hai, lekin event-loop thread par slow network call chalana dangerous ho sakta hai.

**CompletableFuture:** Success par value, failure par exceptional completion. Ek task ki ordinary exception baaki tasks ko rokni nahi chahiye.

**Shutdown race:** Closed check ke turant baad shutdown start ho sakta hai; execute reject karega aur catch future mein failure report karega. Agar contract strict hai ki close start hone ke baad admission bilkul nahi, submit aur close ko lock se serialize karo.

## 5. Interviewer twist: cancellation

CompletableFuture.cancel(true) se zaroori nahi submitted Runnable interrupt ho. Local HTTP call timeout ho gaya toh remote provider request process karta reh sakta hai.

Isliye HTTP client par timeout set karo, deadline propagate karo, side effects ko idempotent rakho. Agar queued task cancellation par queue se nikalna zaroori hai, task handle/FutureTask maintain karke explicitly remove karo.

## 6. Complexity aur capacity

Queue insertion generally O(1), queue memory O(Q), worker resources O(W). Queue bounded hone se memory control hota hai, lekin latency guarantee nahi hoti. Downstream 5 seconds leta hai toh chhoti queue bhi wait kar sakti hai—queue-wait metric aur deadline-based rejection chahiye.

## 7. Testing

1. W tasks ko latch par block karo.
2. Q extra tasks submit karo.
3. Next task promptly reject honi chahiye.
4. Ek task exception throw kare; uska future fail ho, pool continue kare.
5. Concurrent submit/shutdown race test karo.
6. Load test mein p95/p99, queue wait, rejection count aur heap observe karo.

## 8. Production follow-ups

Har downstream dependency ka separate bulkhead ho sakta hai. Retry same bulkhead se guzre, warna retry storm limit bypass kar dega. Active workers, queue depth, rejected tasks, execution time aur queue wait metrics rakho.

Virtual threads lightweight hain, lekin DB connections ya provider capacity increase nahi karte. Multi-instance global limit chahiye toh local executor enough nahi.

**Interview mein English answer:** “I combine a fixed worker pool with a bounded queue and explicit rejection. This isolates downstream slowness and bounds memory. I coordinate deadlines and cancellation with the actual I/O client, and measure queue wait and rejection rate to tune capacity.”
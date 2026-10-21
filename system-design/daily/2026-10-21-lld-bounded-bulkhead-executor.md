# LLD Interview Walkthrough — Bounded Async Bulkhead Executor
**Date:** 2026-10-21 | **Level:** SDE2 / Senior Java

## 1. Problem and contract

A service calls a slow downstream dependency. Run at most W calls concurrently and queue at most Q more. If capacity and queue are full, reject immediately rather than allowing unbounded memory growth. Return a CompletableFuture for each accepted task.

Requirements: bounded workers/queue, explicit rejection, task failures visible to caller, graceful shutdown. Cancelling the future does not guarantee interruption of an already-running remote request.

## 2. Why not create one thread per request?

Unbounded threads/queues convert downstream slowness into heap pressure, context switching and process failure. A bulkhead isolates one dependency's concurrency from unrelated work. Set the limit based on downstream capacity, connection pools, latency and error budget.

## 3. Java 17 implementation

Save as <code>BulkheadExecutor.java</code>.

~~~java
import java.util.Objects;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

public final class BulkheadExecutor implements AutoCloseable {
    private final ThreadPoolExecutor pool;
    private final AtomicBoolean closed = new AtomicBoolean();

    public BulkheadExecutor(int workers, int queueCapacity, String name) {
        if (workers <= 0 || queueCapacity <= 0) {
            throw new IllegalArgumentException("capacities must be positive");
        }
        Objects.requireNonNull(name);
        this.pool = new ThreadPoolExecutor(
            workers, workers,
            0L, TimeUnit.MILLISECONDS,
            new ArrayBlockingQueue<>(queueCapacity),
            runnable -> {
                Thread t = new Thread(runnable, name + "-worker");
                t.setDaemon(false);
                return t;
            },
            new ThreadPoolExecutor.AbortPolicy()
        );
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
        } catch (RejectedExecutionException rejected) {
            result.completeExceptionally(rejected);
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

Example:
~~~java
try (BulkheadExecutor bulkhead =
         new BulkheadExecutor(8, 32, "inventory")) {
    CompletableFuture<String> f =
        bulkhead.submit(() -> callInventoryService());
    f.thenAccept(System.out::println)
     .exceptionally(ex -> { logFailure(ex); return null; });
}
~~~

## 4. Design reasoning

- Fixed worker count caps active concurrency; ArrayBlockingQueue caps waiting work.
- AbortPolicy makes overload visible. CallerRunsPolicy can propagate backpressure, but may unexpectedly run slow I/O on request/event-loop threads.
- CompletableFuture communicates result/failure without forcing caller to block.
- Submission can race with shutdown; the pool rejects after shutdown and the returned future is completed exceptionally.
- Ordinary task failure completes only that future exceptionally. Fatal Errors are rethrown after completion.
- shutdownNow interrupts workers best-effort. HTTP timeouts and cancellation must still be configured at the I/O layer.

## 5. Important race and limitation

CompletableFuture.cancel(true) does not reliably interrupt the Runnable submitted through execute, and a remote service may continue after local timeout. Do not claim transport cancellation you do not have. If queued-task removal on cancellation is required, keep a task handle/FutureTask and remove it from the queue explicitly.

The initial closed check may race with shutdown, but execute rejection is caught. If the contract requires no admission after close begins, serialize submit and close with a lock.

## 6. Complexity and capacity

Submission/queue insertion is O(1) expected; queue memory is O(Q), workers O(W). Maximum accepted-but-not-finished work is approximately W + Q. A bounded queue prevents memory runaway but does not guarantee latency; measure queue wait and reject requests whose deadlines cannot be met.

## 7. Tests

1. Block W tasks with latches; submit Q more; next task must be rejected promptly.
2. Task exception completes its own future exceptionally; pool continues.
3. Race submit with shutdown; every future either runs or completes exceptionally, never hangs silently.
4. Assert queue and active counts stay bounded.
5. Test cooperative interruption separately from HTTP client timeout.
6. Load test a slow dependency and observe p95/p99, rejection rate, queue wait and memory.

## 8. Production follow-ups

- Use one bulkhead per downstream dependency/isolation domain.
- Add deadlines, bounded retries with jitter and circuit breaking; retries must not bypass the same concurrency limit.
- Export active workers, queue depth, rejection count, execution time and queue wait.
- Size against connection pools: 100 workers do not help if only 10 DB connections exist.
- Virtual threads make blocking cheaper, but do not create more downstream capacity.
- A local executor is not a global limit across service instances.

**Spoken answer:** “I use a fixed-size executor with a bounded queue and explicit rejection so downstream slowness cannot cause unbounded resource growth. Futures expose task outcomes, while timeouts, cancellation and retries remain coordinated with the actual I/O client.”
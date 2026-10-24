# Advanced Java Backend Practice — 2026-10-24
## CompletableFuture orchestration: mandatory payment, optional shipping, and honest timeouts

**Audience:** Experienced Java/Spring/AWS backend developer  
**Runtime:** Java 21. CompletableFuture is available since Java 8; copy(), orTimeout(), completeOnTimeout(), and failedFuture() are available since Java 9.  
**Plan:** 3-minute concept · 8-minute exercise · 4-minute review

## 1. Concept check (3 minutes)

### Question

A checkout endpoint calls two independent services:
- Payment authorization is mandatory.
- Shipping quote is optional; if slow or unavailable, checkout can continue with “shipping quote unavailable”.
- The client-facing response budget is 700 ms.

The team writes:

~~~java
return CompletableFuture.supplyAsync(() -> paymentClient.authorize(request))
    .thenCombine(
        CompletableFuture.supplyAsync(() -> shippingClient.quote(request)),
        CheckoutResult::new
    )
    .orTimeout(700, TimeUnit.MILLISECONDS)
    .exceptionally(ex -> CheckoutResult.failed());
~~~

What is risky or incorrect?

### Worked answer

1. **Default executor:** Async CompletableFuture methods without an explicit executor normally use the common ForkJoinPool. Blocking HTTP/JDBC calls should generally use an explicitly managed executor suitable for that I/O workload.
2. **Failure policy is too broad:** The final exceptionally converts payment rejection, programming defects, executor rejection, and timeouts into the same result. It hides whether mandatory payment failed.
3. **Optional dependency is not optional:** A shipping failure makes thenCombine fail; there is no shipping-only fallback.
4. **Timeout is not cancellation:** orTimeout completes the future exceptionally after the deadline; it does not guarantee that the HTTP operation or remote payment stopped.
5. **Payment timeout is ambiguous:** The provider may have authorized the payment even if the caller timed out. A caller must not blindly start a new payment attempt with a new idempotency key.

**Mental model:** thenApply transforms a value; thenCompose flattens a future-returning operation; thenCombine joins two independent stages. exceptionally is a recovery boundary, so place it around only the failure that is genuinely recoverable.

Primary reference: [Java 21 CompletableFuture API](https://docs.oracle.com/en/java/javase/21/docs/api/java.base/java/util/concurrent/CompletableFuture.html).

## 2. Practical exercise with worked solution (8 minutes)

### Requirements

Implement a checkout summary with these constraints:

- Java 21.
- Start payment and shipping calls independently.
- Clients are synchronous/blocking, so pass explicit, caller-owned I/O executors.
- Payment is mandatory. A payment error must remain exceptional.
- Shipping is optional. Use an unavailable quote if it has not completed within 120 ms, or if it fails with a known ShippingUnavailableException / executor rejection.
- The payment result used for the response has a 650 ms timeout.
- The complete response has a 700 ms timeout.
- Do not call get() or join() in the composition.
- Do not shut down executors owned by the caller.
- Use a stable idempotency key for the business checkout operation.

### Step 1 — Model the result and dependencies

~~~java
import java.util.concurrent.*;
import java.util.function.Supplier;

record CheckoutRequest(String orderId, long amountCents, String destination) {}

record PaymentAuthorization(String authorizationId, String status) {}

record ShippingQuote(long amountCents, boolean available) {
    static ShippingQuote unavailable() {
        return new ShippingQuote(0, false);
    }
}

record CheckoutResult(
        PaymentAuthorization payment,
        ShippingQuote shipping) {}

interface PaymentClient {
    PaymentAuthorization authorize(CheckoutRequest request, String idempotencyKey);
}

interface ShippingClient {
    ShippingQuote quote(CheckoutRequest request);
}

final class ShippingUnavailableException extends RuntimeException {
    ShippingUnavailableException(String message) { super(message); }
}
~~~

The example assumes client adapters translate expected transport/service failures into documented exception types. Do not classify every RuntimeException as an optional-service outage.

### Step 2 — Convert executor rejection into a failed stage

Submitting to a bounded executor can reject synchronously, before supplyAsync returns a future. Normalize that case so the method consistently returns a CompletableFuture.

~~~java
static <T> CompletableFuture<T> supplyAsyncSafely(
        Supplier<T> task, Executor executor) {
    try {
        return CompletableFuture.supplyAsync(task, executor);
    } catch (RejectedExecutionException rejected) {
        return CompletableFuture.failedFuture(rejected);
    }
}

static Throwable unwrap(Throwable error) {
    Throwable current = error;
    while ((current instanceof CompletionException
            || current instanceof ExecutionException)
            && current.getCause() != null) {
        current = current.getCause();
    }
    return current;
}
~~~

failedFuture is available since Java 9. Catching rejection here does not hide it: the returned stage is still exceptional. The caller can apply the correct policy for each dependency.

### Step 3 — Compose the workflow

~~~java
static CompletableFuture<CheckoutResult> createCheckoutSummary(
        CheckoutRequest request,
        String idempotencyKey,
        PaymentClient paymentClient,
        ShippingClient shippingClient,
        Executor paymentIoExecutor,
        Executor shippingIoExecutor) {

    // Separate executors help isolate the mandatory and optional dependencies.
    CompletableFuture<PaymentAuthorization> paymentWork =
        supplyAsyncSafely(
            () -> paymentClient.authorize(request, idempotencyKey),
            paymentIoExecutor);

    CompletableFuture<ShippingQuote> shippingWork =
        supplyAsyncSafely(
            () -> shippingClient.quote(request),
            shippingIoExecutor);

    // Apply the response deadline to a copy, not the source operation.
    CompletableFuture<PaymentAuthorization> paymentForResponse =
        paymentWork.copy()
            .orTimeout(650, TimeUnit.MILLISECONDS);

    // A slow quote degrades to "unavailable"; expected failures do too.
    // Unexpected defects remain exceptional rather than being silently hidden.
    CompletableFuture<ShippingQuote> shippingForResponse =
        shippingWork.copy()
            .completeOnTimeout(
                ShippingQuote.unavailable(),
                120,
                TimeUnit.MILLISECONDS)
            .exceptionally(error -> {
                Throwable cause = unwrap(error);

                if (cause instanceof ShippingUnavailableException
                        || cause instanceof RejectedExecutionException) {
                    return ShippingQuote.unavailable();
                }

                throw new CompletionException(cause);
            });

    return paymentForResponse
        .thenCombine(shippingForResponse, CheckoutResult::new)
        .orTimeout(700, TimeUnit.MILLISECONDS);
}
~~~

### Why use copy() before timeout operators?

orTimeout and completeOnTimeout complete the future on which they are invoked. copy() gives the response path a dependent future, so the timeout/fallback policy does not itself force the source future to that outcome. The original operation can still finish and be observed for metrics or reconciliation.

This does **not** stop the underlying work. The HTTP client needs connection/acquisition and response deadlines of its own, and cancellation must be deliberately wired to the transport if supported.

### Acceptance criteria check

- **Parallel start:** Both calls are submitted independently; actual overlap depends on executor capacity.
- **Mandatory payment:** Payment exceptions are not converted into a successful checkout result.
- **Optional shipping:** Only its timeout and explicitly classified availability/rejection failures degrade to unavailable.
- **No blocking composition:** thenCombine coordinates completion without get() or join().
- **Deadlines:** Shipping response fallback is 120 ms; payment response branch is 650 ms; final composition is capped at 700 ms.
- **Executor ownership:** This method neither creates nor shuts down supplied executors.
- **Payment safety:** Reuse the same business idempotency key for retries/reconciliation of this checkout; do not regenerate it per network attempt.

## 3. Derivation, walkthrough, mistakes, and production trade-offs (4 minutes)

### Reason from requirements to operators

| Requirement | Design choice | Why |
|---|---|---|
| Two independent calls | Submit both supplyAsync tasks before combining | Avoid an artificial dependency |
| Payment must succeed | No payment fallback | Preserve exceptional completion for mandatory work |
| Shipping can degrade | completeOnTimeout plus narrow exceptionally | Produce a degraded result only for the optional dependency |
| One branch has its own budget | paymentWork.copy().orTimeout(...) | Keep response deadline separate from source work |
| Whole response has a cap | thenCombine(...).orTimeout(...) | Bound the returned stage |
| Bounded executor may reject | failedFuture wrapper | Make submission rejection part of async error handling |

### Dry run

- **Payment completes in 180 ms; shipping in 90 ms:** result is composed normally.
- **Payment completes in 300 ms; shipping takes 500 ms:** shipping response copy becomes unavailable at about 120 ms; checkout can return when payment completes.
- **Shipping fails with ShippingUnavailableException:** checkout degrades to unavailable shipping.
- **Shipping throws an unexpected NullPointerException:** the error remains exceptional; a coding defect is not mislabeled as a normal outage.
- **Payment response branch times out at 650 ms:** the composed result fails; the payment request may still be executing remotely.
- **The final stage reaches 700 ms:** the client-facing future times out, but upstream work is not automatically cancelled.

These are not hard real-time guarantees. Scheduling, queueing, and transport behavior mean operators mark future completion around the specified deadlines; they do not guarantee the remote service stopped at that instant.

### Production risks and improvements

1. **Payment timeout means unknown outcome, not “payment failed”.** Persist a checkout/payment attempt identifier, reuse its idempotency key, and reconcile with the provider before starting a logically new payment. Never hide this ambiguity behind a generic fallback.
2. **Use real transport timeouts.** Configure connection-pool acquisition, connect, and response/read timeouts. A future timeout alone may leave a blocked thread or socket occupied.
3. **Bound each executor.** Use managed executors with bounded queues and deliberate rejection policies. Separate bulkheads prevent slow optional shipping work from starving payment work.
4. **Measure late completions.** Observe the original paymentWork and shippingWork stages, not only response copies. Record timeout, rejection, fallback, latency, and late-completion metrics without logging payment secrets or personal data.
5. **Do not retry every exception.** Payment rejection is a business result; timeout is an ambiguous remote outcome; executor rejection is local overload; a programming defect needs investigation.
6. **Cancellation is cooperative and client-specific.** CompletableFuture.cancel(true) does not promise to interrupt the underlying computation. If a transport supports cancellation, wire it explicitly and still handle ambiguous payment outcomes.
7. **Spring integration:** Inject lifecycle-managed executors as beans; do not create/shut down a pool per request. Keep transaction boundaries explicit and avoid holding database connections while waiting on remote calls.

### Interview-ready summary

“I start independent I/O calls on explicitly managed executors, compose them with thenCombine, and put fallback handling only around the optional dependency. I use separate response futures for branch deadlines and a final deadline, while remembering that future timeout is not transport cancellation. Payment timeout is an unknown outcome, so I rely on a stable idempotency key and reconciliation rather than blindly retrying.”

### Follow-up questions with answers

**Q1. When is thenCompose preferable to thenCombine?**  
When the next async operation depends on the previous result, e.g. loadOrder(id).thenCompose(order -> loadPayment(order.paymentId())). thenCompose flattens a nested future. thenCombine joins two independent results.

**Q2. Does exceptionally catch errors from the original supplier?**  
Yes, if the failure propagates to that stage. Placement matters: a broad handler after the whole checkout can accidentally convert mandatory payment failures into success.

**Q3. Why not use allOf?**  
allOf is useful when all stages must finish but does not directly combine their typed values. You still need to compose each result, and it does not decide optional-failure policy.

**Q4. Does orTimeout cancel the HTTP request?**  
No guarantee. It completes the future exceptionally. Cancellation must be supported and wired by the client/transport; remote payment outcome may still need reconciliation.

## References

- [Java 21 CompletableFuture API](https://docs.oracle.com/en/java/javase/21/docs/api/java.base/java/util/concurrent/CompletableFuture.html)
- [Java 21 Executor API](https://docs.oracle.com/en/java/javase/21/docs/api/java.base/java/util/concurrent/Executor.html)
- [Java 21 CompletionStage API](https://docs.oracle.com/en/java/javase/21/docs/api/java.base/java/util/concurrent/CompletionStage.html)

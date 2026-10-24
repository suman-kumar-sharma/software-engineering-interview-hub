# Advanced Java Backend Practice — 2026-10-24
## CompletableFuture se checkout orchestration: mandatory payment, optional shipping, aur sahi timeout handling

**Audience:** Experienced Java/Spring/AWS backend developer  
**Runtime:** Java 21. CompletableFuture Java 8 se available hai; copy(), orTimeout(), completeOnTimeout(), aur failedFuture() Java 9 se available hain.  
**Plan:** 3-minute concept · 8-minute practical exercise · 4-minute review

## 1. Concept check (3 minutes)

### Sawal

Checkout API ko do independent services call karni hain:

- Payment authorization mandatory hai.
- Shipping quote optional hai. Slow ya unavailable ho toh checkout “shipping quote unavailable” ke saath continue kar sakta hai.
- Client ko response dene ka total budget 700 ms hai.

Team ye code likhti hai:

~~~java
return CompletableFuture.supplyAsync(() -> paymentClient.authorize(request))
    .thenCombine(
        CompletableFuture.supplyAsync(() -> shippingClient.quote(request)),
        CheckoutResult::new
    )
    .orTimeout(700, TimeUnit.MILLISECONDS)
    .exceptionally(ex -> CheckoutResult.failed());
~~~

Ismein kya problems hain?

### Worked answer

1. **Default executor ka risk:** Explicit executor na do toh async CompletableFuture methods normally common ForkJoinPool use karte hain. Blocking HTTP/JDBC calls ke liye workload ke hisaab se managed I/O executor dena generally better hota hai.
2. **Exception handling bahut broad hai:** Last wala exceptionally payment decline, programming bug, executor rejection aur timeout sabko ek hi result bana deta hai. Mandatory payment failure hide ho sakti hai.
3. **Shipping actually optional nahi hai:** Shipping fail hui toh thenCombine ka result bhi exceptional ho jayega. Shipping-only fallback define hi nahi hai.
4. **Timeout cancellation nahi hota:** orTimeout future ko deadline ke baad exceptionally complete karta hai; ye guarantee nahi karta ki HTTP call ya remote payment ruk gaya.
5. **Payment timeout ka matlab payment fail hona nahi:** Provider payment authorize kar chuka ho sakta hai, bas response late hua. Retry ke waqt naya idempotency key bana kar payment dobara start karna duplicate charge ka risk hai.

**Mental model yaad rakho:**
- thenApply: available value ko transform karo.
- thenCompose: next async call previous result par depend karti hai; nested future flatten karo.
- thenCombine: do independent stages ke results combine karo.
- exceptionally: recovery boundary hai. Sirf us failure ko recover karo jise business-wise recover karna safe hai.

Primary reference: [Java 21 CompletableFuture API](https://docs.oracle.com/en/java/javase/21/docs/api/java.base/java/util/concurrent/CompletableFuture.html).

## 2. Practical exercise with complete solution (8 minutes)

### Requirements

Checkout summary implement karni hai:

- Java 21.
- Payment aur shipping calls independently start hon.
- Dono clients synchronous/blocking hain; explicit, caller-owned I/O executors pass karo.
- Payment mandatory hai. Payment failure exceptional hi rehni chahiye.
- Shipping optional hai. 120 ms mein quote complete na ho, ya known ShippingUnavailableException / executor rejection aaye, toh unavailable quote use karo.
- Response ke liye payment branch ka timeout 650 ms hai.
- Complete response ka timeout 700 ms hai.
- Composition ke andar get() ya join() use nahi karna.
- Caller ke executors ko shutdown nahi karna.
- Business checkout ke liye stable idempotency key use karni hai.

### Step 1 — Data aur dependencies define karo

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

Assumption: client adapters expected transport/service failures ko documented exception types mein translate karte hain. Har RuntimeException ko shipping outage samajh kar suppress mat karo.

### Step 2 — Executor rejection ko failed stage mein convert karo

Bounded executor par task submit karte waqt rejection synchronously throw ho sakti hai—yaani supplyAsync future return karne se pehle hi exception aa sakti hai. Is case ko bhi CompletableFuture error flow mein lana useful hai.

Neeche ke helper methods ko ek utility/composer class ke andar rakho:

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

failedFuture Java 9 se available hai. Rejection ko hide nahi kiya ja raha; future ab bhi exceptional hai. Bas ab har dependency ke liye sahi policy apply kar sakte hain.

### Step 3 — Workflow compose karo

~~~java
static CompletableFuture<CheckoutResult> createCheckoutSummary(
        CheckoutRequest request,
        String idempotencyKey,
        PaymentClient paymentClient,
        ShippingClient shippingClient,
        Executor paymentIoExecutor,
        Executor shippingIoExecutor) {

    // Alag executors mandatory payment aur optional shipping ko isolate karte hain.
    CompletableFuture<PaymentAuthorization> paymentWork =
        supplyAsyncSafely(
            () -> paymentClient.authorize(request, idempotencyKey),
            paymentIoExecutor);

    CompletableFuture<ShippingQuote> shippingWork =
        supplyAsyncSafely(
            () -> shippingClient.quote(request),
            shippingIoExecutor);

    // Deadline response ke liye copy par lagao, original operation par nahi.
    CompletableFuture<PaymentAuthorization> paymentForResponse =
        paymentWork.copy()
            .orTimeout(650, TimeUnit.MILLISECONDS);

    // Slow shipping ko unavailable banao; expected failures ka fallback bhi do.
    // Unexpected coding defects exceptional hi rahenge.
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

### Timeout se pehle copy() kyun?

orTimeout aur completeOnTimeout us future ko complete karte hain jis par unhe call kiya gaya hai. copy() response path ke liye dependent future deta hai. Isliye response timeout/fallback source future ko usi outcome mein force nahi karta. Original operation baad mein bhi complete ho sakta hai, aur uski completion ko metrics ya reconciliation ke liye observe kiya ja sakta hai.

Lekin yaad rakho: **copy() bhi underlying HTTP operation ko stop nahi karta.** HTTP client mein connection acquisition, connect aur response/read timeouts configure karo. Transport cancellation supported ho toh use explicitly wire karo.

### Acceptance criteria

- **Independent start:** Dono tasks alag-alag submit hote hain. Actual overlap executor capacity par depend karega.
- **Payment mandatory:** Payment exception ko successful checkout result mein convert nahi kiya.
- **Shipping optional:** Sirf shipping timeout aur explicitly classified availability/rejection failures unavailable quote banate hain.
- **Non-blocking composition:** thenCombine use hua; get()/join() nahi.
- **Deadlines:** Shipping response fallback 120 ms; payment response branch 650 ms; final result 700 ms ke andar cap hota hai.
- **Executor ownership:** Method supplied executors ko create ya shutdown nahi karta.
- **Payment safety:** Is checkout ke retries/reconciliation mein same business idempotency key reuse karo; har network attempt par naya key mat banao.

## 3. Solution tak reasoning, dry run, mistakes aur production trade-offs (4 minutes)

### Requirement se operator kaise choose kiya?

| Requirement | Design choice | Reason |
|---|---|---|
| Dono calls independent hain | Dono supplyAsync tasks pehle submit, phir combine | Artificial dependency nahi banayi |
| Payment success mandatory hai | Payment par fallback nahi | Mandatory failure exceptional rehti hai |
| Shipping degrade kar sakti hai | completeOnTimeout + narrow exceptionally | Sirf optional dependency degrade hoti hai |
| Payment branch ka apna budget hai | paymentWork.copy().orTimeout(...) | Response deadline source work se separate hai |
| Overall response cap chahiye | thenCombine(...).orTimeout(...) | Returned stage ka maximum wait bound hota hai |
| Bounded executor reject kar sakta hai | failedFuture wrapper | Submission rejection bhi async error flow ka hissa hai |

### Dry run

- **Payment 180 ms mein, shipping 90 ms mein complete:** normal result.
- **Payment 300 ms mein, shipping 500 ms leti hai:** shipping response copy lagbhag 120 ms par unavailable ho jaati hai; payment complete hote hi checkout result ban sakta hai.
- **Shipping ShippingUnavailableException throw karti hai:** checkout unavailable shipping ke saath continue karta hai.
- **Shipping unexpected NullPointerException deti hai:** error exceptional hi rahega. Programming bug ko normal outage label nahi karenge.
- **Payment response branch 650 ms par timeout:** composed result fail hota hai; remote payment request abhi bhi execute ho sakti hai.
- **Final stage 700 ms tak complete nahi hota:** client-facing future timeout hota hai, lekin upstream operations automatically cancel nahi hote.

Ye hard real-time guarantees nahi hain. Scheduling delay, queueing aur transport behavior ki wajah se future completion deadline ke aas-paas ho sakti hai. Isse ye prove nahi hota ki remote service usi moment par ruk gayi.

### Production risks aur improvements

1. **Payment timeout = unknown outcome, payment failure nahi.** Checkout/payment attempt ID persist karo, same idempotency key reuse karo, aur logically nayi payment start karne se pehle provider se status reconcile karo. Generic fallback se uncertainty hide mat karo.
2. **Real transport timeouts lagao.** Connection-pool acquisition, connect aur response/read timeouts configure karo. Sirf future timeout se blocked thread ya socket free hone ki guarantee nahi.
3. **Executors bounded rakho.** Managed executors, bounded queues aur deliberate rejection policies use karo. Alag bulkheads se slow shipping work payment ke resources starve nahi karega.
4. **Late completion observe karo.** Sirf response copies nahi, original paymentWork aur shippingWork bhi metrics ke liye observe karo. Timeout, rejection, fallback, latency aur late completion track karo. Payment secrets ya personal data log mat karo.
5. **Har exception par retry mat karo.** Payment decline business outcome hai; timeout remote outcome ko ambiguous banata hai; executor rejection local overload hai; programming bug investigate karna chahiye.
6. **Cancellation cooperative aur client-specific hai.** CompletableFuture.cancel(true) underlying computation ko interrupt karne ki guarantee nahi deta. Transport cancellation supported ho toh explicitly wire karo; payment outcome phir bhi reconcile karna pad sakta hai.
7. **Spring integration:** Executors ko beans ke through lifecycle-manage karo; har request par pool create/shutdown mat karo. Transaction boundary clear rakho aur remote calls ka wait karte waqt DB connection hold karne se bacho.

### Interview mein concise answer

“Main independent I/O calls ko explicitly managed executors par start karta hoon aur thenCombine se results join karta hoon. Fallback sirf optional dependency par lagata hoon. Branch deadlines aur overall deadline ke liye response futures use karta hoon, lekin future timeout ko transport cancellation nahi maanta. Payment timeout ka outcome uncertain ho sakta hai, isliye stable idempotency key aur reconciliation use karta hoon, blind retry nahi.”

### Follow-up questions with answers

**Q1. thenCompose kab use karoge, thenCombine kab?**  
Agar next async operation previous result par depend kare, jaise loadOrder(id).thenCompose(order -> loadPayment(order.paymentId())), toh thenCompose. Ye nested future flatten karta hai. Do independent results ko join karna ho toh thenCombine.

**Q2. Kya exceptionally original supplier ke errors catch karta hai?**  
Haan, agar failure us stage tak propagate hoti hai. Placement important hai: poore checkout ke baad broad handler mandatory payment failure ko bhi success bana sakta hai.

**Q3. allOf kyun nahi?**  
allOf wait-for-all coordination ke liye useful hai, lekin typed results directly combine nahi karta. Results ko phir bhi compose/retrieve karna hoga; optional failure policy bhi tumhe define karni padegi.

**Q4. Kya orTimeout HTTP request cancel karta hai?**  
Guarantee nahi. Ye future ko exceptionally complete karta hai. Transport cancellation ko client ke through wire karna padta hai; payment ka remote outcome reconcile karna pad sakta hai.

## References

- [Java 21 CompletableFuture API](https://docs.oracle.com/en/java/javase/21/docs/api/java.base/java/util/concurrent/CompletableFuture.html)
- [Java 21 Executor API](https://docs.oracle.com/en/java/javase/21/docs/api/java.base/java/util/concurrent/Executor.html)
- [Java 21 CompletionStage API](https://docs.oracle.com/en/java/javase/21/docs/api/java.base/java/util/concurrent/CompletionStage.html)

# Retry Storm Ko Kaise Rokein: Deadline, Jitter aur Retry Budget
Date: 2026-10-17 | Java 21 service | Library-neutral design pseudocode
Prerequisites: HTTP timeout, blocking call aur semaphore basics. Plan: 3 minute concept, 8 minute solution, 4 minute walkthrough. Pichhle session mein JVM memory diagnose ki thi; aaj dependency fail hone par service ko stable rakhna seekhenge.

## Concept question aur direct answer
A -> B -> C chain hai. A, B ko maximum 3 baar call kar sakta hai. Har B invocation C ko maximum 3 baar call karta hai. C par kitni calls?
**Answer: maximum 9.** Agar teen separate retrying caller layers hon aur har layer 3 attempts kare, leaf par 27 calls tak pahunch sakti hain. Interview mein clarify karo: 3 retries matlab initial call plus 3, total 4 attempts.

Socho restaurant kitchen already slow hai. Har customer baar-baar wahi order bhej raha hai. Kitchen ka kaam kam hone ke bajaye aur badhega. Retry transient failure mein useful hai, lekin overload mein recovery ko aur difficult kar sakta hai.

## Practical question
Read-only order lookup banana hai. Requirements:
- Poore operation ka local budget 600 ms, backoff aur wait included.
- Maximum 3 total attempts.
- Har HTTP attempt maximum 180 ms, remaining time kam ho toh usse bhi kam.
- Dependency ki bulkhead full ho toh immediately reject.
- Retry delay full jitter: pehle retry ka cap 25 ms, doosre ka 50 ms, maximum cap 100 ms.
- Per-instance/per-dependency shared bucket: 20 tokens capacity, 5 tokens/second refill. Sirf retries tokens khayengi.
- Cancel/interruption propagate; permanent failures aur local overload retry nahi.
Ye teaching values hain; production values latency data aur SLO se choose karenge. SDK/client ke hidden retries disable ya explicitly account karo.

## Requirements se design tak kaise pahunche
**Pehle deadline:** Agar har attempt ko fresh 600 ms de doge toh total operation 600 ms mein finish nahi hoga. Ek start time lo; har stage ke baad remaining budget nikalo.

**Phir concurrency:** Bahut saari waiting calls threads/connections consume karti hain. Bulkhead actual attempt ke around lagao, backoff ke around nahi. Sleep karte waqt permit pakadoge toh useful calls ko jagah nahi milegi.

**Phir retry eligibility:** Har error temporary nahi. Wrong credentials, bad input aur code bug ko repeat karke success nahi milegi. Cancellation ko transient failure bana diya toh user cancel ke baad bhi kaam chalta rahega.

**Phir timing:** Sab replicas exactly 25 ms baad retry karenge toh fir ek saath load aayega. Random delay requests ko spread karta hai.

**Ab aggregate budget:** Har request ko 2 retries allow karna enough nahi; 10,000 requests ek saath fail ho sakti hain. Shared bucket added retry traffic ko control karti hai. Bucket token completion par return nahi hota; time ke saath refill hota hai.

## Complete solution pseudocode
```text
lookup(orderId, budget = 600 ms):
    deadline = monotonicNow() + budget
    for attempt in 1..3:                 # total attempts, not 3 retries
        propagate cancellation/interruption
        remaining = deadline - monotonicNow()
        if remaining < 20 ms: fail DeadlineExceeded

        if attempt > 1:
            if remaining < 40 ms: fail DeadlineExceeded
            cap = min(100 ms, 25 ms * 2^(attempt - 2))
            delay = randomUniform(0, min(cap, remaining - 40 ms))
            cancellableSleep(delay)
            # Recheck after sleeping; scheduling can overshoot.

        remaining = deadline - monotonicNow()
        if remaining < 20 ms: fail DeadlineExceeded

        if not dependencyBulkhead.tryAcquireImmediately():
            fail LocalOverload          # do not retry local overload
        try:
            propagate cancellation/interruption
            remaining = deadline - monotonicNow()
            if remaining < 20 ms: fail DeadlineExceeded
            if attempt > 1 and not sharedRetryBucket.tryConsume():
                fail RetryBudgetExhausted

            timeout = min(180 ms, remaining - 10 ms)
            response = client.lookup(orderId, timeout)
            # Adapter must bound pool wait/connect/TLS/read as supported,
            # disable hidden retries and release response resources.
        catch KnownTransientTransportFailure as error:
            outcome = retryable(error)
        finally:
            dependencyBulkhead.release()

        if monotonicNow() >= deadline: fail DeadlineExceeded
        if response succeeded: return response
        if outcome/response is permanent: fail originalFailure
        if response is 429 or 503 with valid Retry-After:
            # Honor server's minimum delay; jitter may add time, not subtract.
            # If it cannot fit with a useful attempt, fail/defer; never retry early.
            apply server delay constraint to next iteration
        if attempt == 3: fail lastFailure
```

Har attempt ki response/outcome state fresh initialize hogi. Retry-After branch next sleep par server ka minimum wait apply karti hai; woh alag retry loop start nahi karti. Server 1 second wait bole aur tumhare paas 200 ms ho, toh fail/defer karo: 100 ms mein retry mat karo.

HTTP-date Retry-After wall clock se delay mein convert hoga, phir local monotonic budget se compare hoga. Malformed/unreasonable values ka explicit rule rakho. Har IOException ko automatically transient mat samjho; actual client errors classify karo.

## Token bucket implement karne ki intuition
Ek shared object mein tokens aur lastRefillTime rakho. Short synchronized section mein elapsed time se refill karo:
tokens = min(20, tokens + elapsedSeconds*5).
Agar token >=1 hai toh 1 subtract karke allow; warna deny. Network call lock ke andar nahi. Token check atomic hona chahiye, warna do threads ek hi token spend kar sakte hain.

Original call token nahi leti. Retry admission milne aur time check hone ke baad network attempt se just pehle token lo. Token nahi mila toh permit finally mein release ho jayega aur call fail hogi.

## Java mein kaunse blocks use honge?
Elapsed time ke liye System.nanoTime subtraction use karo. Wall clock adjust ho sakta hai; local nanoTime ko doosre machine par deadline ki tarah mat bhejo. Semaphore.tryAcquire() immediate admission dega; successful acquire ke baad finally mein release karo.

HTTP adapter ko remaining timeout pass karo aur verify karo ki pool wait, connection, TLS aur response body kitna cover hota hai. Sirf CompletableFuture.orTimeout laga dene se already-running HTTP work necessarily stop nahi hota.

## Dry run
- t=0: first call starts, 180 ms baad transient failure.
- Jitter 17 ms mila: second call t=197 par starts, ek retry token consume.
- t=377 par fir failure; next jitter 31 ms.
- t=408 par third call starts. Remaining 192 ms; 10 ms reserve ke baad max 180 ms attempt allow.
- t=550 par success; result return.

Agar sleep overshoot karke sirf 15 ms bache, next call start nahi. Agar bucket empty ho toh time bachne ke bawajood retry deny: dependency ko protect karna bhi requirement hai.

600 ms hard real-time guarantee nahi hai. JVM pause/scheduling se return late ho sakta hai. Isliye late result reject, actual transport timeout aur cancellation propagation sab chahiye. Timeout ka matlab remote side par payment nahi hui, aisa assume mat karo.

## Production mistakes aur tradeoffs
- 10 replicas ki 5 tokens/s refill milkar around 50 retries/s ho sakti hai, initial bursts alag. Local policy global limit nahi.
- Network work chalta rahe aur wrapper timeout par permit release kar do toh actual concurrency limit toot sakti hai.
- Payment write retry karna ho toh same idempotency key aur unknown outcome reconciliation chahiye.
- Bulkhead full ho toh doosri layer automatic retry karegi, toh protection undermine ho sakti hai.
- Circuit breaker sustained outage mein useful ho sakta hai, lekin recovery probes/state transitions test karo.
- Metrics: logical requests, actual attempts, denied retries, bulkhead rejects, retry success aur attempt latency. Amplification = attempts / original requests.
- Spring Retry, gateway, SDK aur client sabki configuration inspect karo; chosen operation ka retry owner clear rakho.

## Verification plan
Fake clock, controlled sleeper, seeded random aur scripted client se test karo. First success; transient then success; permanent failure once; no token; full bulkhead; oversleep; interruption; Retry-After deadline se bada; max 3 attempts; late success. Har acquire ka release verify karo. Yahan pseudocode aur test plan hai; executed tests ka claim nahi.

## Interview twists with answers
**Jitter ke saath bucket kyun?** Jitter timing spread karta hai; aggregate retry volume cap nahi karta.
**Bucket ke saath bulkhead kyun?** Rate aur in-flight concurrency alag cheezein hain.
**429 pe retry?** Safe operation, server wait aur remaining deadline permit kare toh.
**Timeout badha dein?** Resources zyada der held rahenge; upstream deadline bhi break ho sakti hai.
**Professional English answer:** “I use one retry owner, a shared deadline, bounded attempt timeouts, safe failure classification, full jitter and a shared retry budget. I release attempt capacity before backoff and verify transport timeout semantics and amplification metrics.”

## Sources
- [AWS: timeouts, retries, backoff and jitter](https://aws.amazon.com/builders-library/timeouts-retries-and-backoff-with-jitter/)
- [Java 21 System.nanoTime](https://docs.oracle.com/en/java/javase/21/docs/api/java.base/java/lang/System.html#nanoTime())
- [Java 21 Semaphore](https://docs.oracle.com/en/java/javase/21/docs/api/java.base/java/util/concurrent/Semaphore.html)
- [HTTP Retry-After semantics, RFC 9110](https://www.rfc-editor.org/rfc/rfc9110.html#name-retry-after)

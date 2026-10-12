# URL Shortener HLD — Hinglish

**Problem:** Long URL ko short code mein convert karo; short code hit karne par original URL par redirect karo.

## Interview mein pehle kya poochoge?
"Do we need custom aliases, expiration, analytics, and multi-region support? What is expected peak redirect traffic?"

**Assume:** 100M new links/month, 1B redirects/day, 10× peak, 302 redirects.

## Estimate kaise nikala?
1B / 86,400 ≈ 11.6k redirects/s average. Peak ~116k/s. Creates 100M / (30×86,400) ≈ 39/s. **Read bahut zyada hai**, toh redirect path ko optimize karna hai. Yahi reason hai cache use karne ka.

## Architecture
```text
Create: Client → API → Link Service → Unique ID → DB
Read:   Client → Edge → Redirect Service → Cache → DB (miss)
                                             ↘ Async analytics
```

## Step-by-step flow
**Create:** URL validate karo → unique code generate karo → DB mein persist karo → success return. Agar DB commit fail, short URL issue mat karo.

**Redirect:** Code cache mein dekho → miss par DB query → expired/disabled check → 302 Location header. Analytics event async bhejo, taaki redirect latency slow na ho.

## Design choices: kyun?
- **Base62:** Compact URL-friendly alphabet. 7 chars = 62^7 combinations, lekin uniqueness enforce karni hogi.
- **Unique code:** Sequence + Base62 simple hai; distributed range allocation se bottleneck reduce ho sakta hai. Random code + unique constraint alternative hai.
- **Cache:** Hot links par DB load kam. Lekin disabled link ka stale cache issue handle karo.
- **DB:** Initially replicated SQL enough ho sakta hai; sharding tab jab measured scale justify kare.
- **Analytics:** At-least-once delivery mein duplicate events possible hain; accurate counts ke liye dedup strategy chahiye.

## Failure cases
- Cache down → DB fallback + load shedding.
- DB down → cached links continue kar sakte hain; creation fail safely.
- Viral link → edge caching/hot-key controls.
- Malicious links → scheme validation, abuse detection, rate limits.

## Interview mein 60-second English answer
"I'd split link creation and redirect into separate paths. Creation assigns a unique Base62 code and stores the mapping durably. Redirects are read-heavy, so I would use edge caching and Redis before hitting a replicated database. Analytics would be asynchronous. The key trade-off is cache staleness versus latency, especially when links are disabled. I'd start simple and scale based on p99 latency and cache miss rate."

**Deep-dive English reference:** [URL Shortener](../../hld/url-shortener.md)

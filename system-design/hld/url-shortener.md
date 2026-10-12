# HLD Worked Interview: Design a URL Shortener

**Prompt:** Design a service that creates short links and redirects users to long URLs. Assume 100M new links/month, 1B redirects/day, 99.9%+ availability, and a 100 ms p95 redirect target (illustrative requirements).

## 1. Clarifying questions (say these aloud)
- Are custom aliases allowed? Link expiration? Analytics? Authentication?
- Are redirects permanent (301) or temporary (302)?
- Is link creation strongly consistent? What happens if a custom alias is taken?
- Are links globally available? Are malicious URLs filtered?

**Assume:** create, redirect, optional TTL; custom aliases and detailed analytics are secondary. Redirect uses 302 by default to preserve flexibility.

## 2. Functional and nonfunctional requirements
- Create short link from valid HTTPS URL, return short code.
- Redirect short code; 404 unknown, 410 expired (policy choice).
- Prevent collisions; tolerate spikes and popular links.
- Availability and low read latency dominate. Creation can be slower.
- Rate-limit abuse and protect against malicious destinations.

## 3. Capacity math
- Creates: 100M / (30×86,400) ≈ **39 writes/s average**.
- Redirects: 1B / 86,400 ≈ **11,574 reads/s average**; 10× peak ≈ **116k reads/s**.
- Read/write ratio ≈ **300:1**.
- 100M links/month × 200 bytes/link ≈ **20 GB/month raw** (excluding indexes, replicas, overhead).
- Base62 with 7 chars has 62⁷ ≈ 3.5 trillion combinations; capacity does not guarantee uniqueness without collision handling.

## 4. APIs and model
```http
POST /v1/links
Content-Type: application/json
{"url":"https://example.org/articles/42","expiresAt":null}
→ 201 {"code":"Ab12XyZ","shortUrl":"https://sho.rt/Ab12XyZ"}

GET /Ab12XyZ
→ 302 Location: https://example.org/articles/42
```
`links(code PK, destination_url, created_at, expires_at, owner_id, status)`

Use unique index on `code`, TTL cleanup asynchronous, and destination validation.

## 5. Architecture
```text
Create: Client → API Gateway → Link Service → ID Generator → SQL/KV Store
                                  ↘ Abuse checks

Redirect: Client → CDN/Edge → Redirect Service → Redis Cache
                                          cache miss ↘ Link Store
                                  ↘ Async event → Queue → Analytics
```
**Write:** generate unique ID (e.g., allocated sequence ranges or random code + uniqueness constraint), encode Base62, persist, then return.

**Read:** look up cache, fall back to store, verify status/expiry, return redirect, emit analytics asynchronously. Negative-cache missing keys briefly to reduce miss storms.

## 6. Deep-dive trade-offs
- **Key generation:** centralized sequence is simple but can bottleneck; allocate ID ranges to workers. Random codes need uniqueness check/retry. Custom aliases need a unique constraint.
- **Caching:** cache hot redirects, apply TTL bounded by link expiration. On deletion/disable, invalidate cache or accept a documented short staleness window.
- **Database:** start with replicated relational storage; partition/shard by hash(code) only if throughput/storage justify it.
- **Consistency:** link creation must commit before success; redirects may read a replica/cache with bounded staleness if policy allows.
- **Analytics:** asynchronous at-least-once events, deduplicate by event ID if accurate counts are required; avoid slowing redirects.
- **Security:** URL scheme allowlist, malware/phishing checks, creation quotas, prevent internal-service fetches during URL validation, abuse reporting.

## 7. Failure scenarios
- Cache down → DB fallback with load shedding and rate limits.
- Database unavailable → cached links may still redirect, but creation fails safely.
- Queue unavailable → bounded buffer or best-effort analytics, depending on requirements.
- Hot key → edge caching, per-key load controls.
- Link disabled → invalidate cached mapping; document propagation behavior.

## 8. Interview closing answer (60 seconds)
"I'd separate write-heavy link creation from read-heavy redirects. Creation generates a unique Base62 code and commits the mapping. Redirects use an edge/cache-first path backed by a replicated store, while analytics are asynchronous. The read/write ratio makes caching the first scaling lever. The main trade-off is cache staleness on deletes versus redirect latency. I'd start with a simple replicated database and measure cache misses and p99 latency before sharding."

## Follow-ups
What changes for custom aliases? How would you handle 100M redirects/minute? How do you implement GDPR deletion? How would you choose 301 vs 302?

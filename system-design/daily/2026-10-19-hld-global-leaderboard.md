# HLD — Global Gaming Leaderboard (2026-10-19)

Original product-company SDE2 system-design exercise. Assume 10 million players, 50,000 score events/s peak and 20,000 read requests/s. Need season top 100, player rank and neighbors. Source of scores is authenticated game servers, not untrusted clients. Target in-region read p99 <150ms and eventual projection freshness <5s in normal operation.

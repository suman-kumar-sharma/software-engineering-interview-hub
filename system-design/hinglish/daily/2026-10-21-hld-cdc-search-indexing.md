# HLD Hinglish — CDC Search Indexing

Socho PostgreSQL original data rakhta hai aur OpenSearch search ke liye derived index hai. CDC se changes capture hote hain, Kafka unhe buffer karta hai, aur indexer idempotently apply karta hai. Details: event ordering, snapshot handoff, backpressure, replay aur tenant isolation.

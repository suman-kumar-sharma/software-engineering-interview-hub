# Interview Preparation --- 2026-10-11

**Focus:** Java backend engineering · Spring Boot · Kafka · distributed
systems · AWS · databases · system design · security\
**Research cutoff:** 11 October 2026 (IST)\
**Coverage:** Publicly readable LinkedIn posts and official technical
announcements. **Not** a scan of your private LinkedIn feed,
connections-only posts, or saved posts.

> **Evidence policy:** The interview questions below are **verified as
> appearing in the linked public posts**. Claims that an employer asked
> them are **the authors' personal reports, not independently verified
> company interview records**. Solutions and design guidance are
> independently developed and cross-checked against linked official
> documentation. The relative publication ages shown by LinkedIn are
> approximate.

## Today's quick index

  ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
  ID             Interview question       Topic          Difficulty     Original source
  -------------- ------------------------ -------------- -------------- --------------------------------------------------------------------------------------------------------------------------------------------------
  Q1             Aggregate account        Java / DSA     Easy--Medium   [EY interview-experience post](https://www.linkedin.com/posts/anubhi9_java-springboot-microservices-activity-7509305270301331457-_R6C)
                 transactions; use                                      
                 `Map.merge()`                                          

  Q2             `ExecutorService` vs     Java           Medium         [EY interview-experience post](https://www.linkedin.com/posts/anubhi9_java-springboot-microservices-activity-7509305270301331457-_R6C)
                 `CompletableFuture`;     concurrency                   
                 default pool                                           

  Q3             Hibernate N+1 and        Spring Data    Medium--Hard   [Infosys interview-experience
                 processing 500,000 rows  JPA / SQL                     post](https://www.linkedin.com/posts/er-naimuddin-alam-24067511b_infosys-infosysinterview-interviewexperience-activity-7512946809846210561-9u5K)

  Q4             Millions of transfers;   System design  Hard           [EY interview-experience post](https://www.linkedin.com/posts/anubhi9_java-springboot-microservices-activity-7509305270301331457-_R6C)
                 debit succeeds but       / transactions                
                 credit fails                                           

  Q5             Failed Kafka messages;   Kafka /        Hard           [Infosys
                 duplicates despite       reliability                   post](https://www.linkedin.com/posts/er-naimuddin-alam-24067511b_infosys-infosysinterview-interviewexperience-activity-7512946809846210561-9u5K)
                 healthy consumers                                      and [Kafka production-scenario
                                                                        post](https://www.linkedin.com/posts/satyamparmar_kafka-interview-question-1-consumer-lag-activity-7497085822249332736-rBc6)

  Q6             `HashMap` vs             Java           Medium         [EY interview-experience post](https://www.linkedin.com/posts/anubhi9_java-springboot-microservices-activity-7509305270301331457-_R6C)
                 `ConcurrentHashMap`      concurrency                   
                 under contention                                       

  Q7             Concurrent transfers,    Databases /    Hard           [Coforge interview-experience
                 optimistic/pessimistic   system design                 post](https://www.linkedin.com/posts/naushad-a-93081b179_coforge-coforgehiring-hiring2026-activity-7509911367194411008-Du-M)
                 locking, crash recovery                                
  ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

------------------------------------------------------------------------

## Q1. Aggregate account transactions using Java 8 `Map.merge()`

**Question (paraphrased):** For entries
`A:+100, B:+200, A:-50, C:+300, B:-100, A:+150`, calculate each
account's net change, explain the algorithm, and use `Map.merge()`.

**Reasoning.** Each transaction contributes to exactly one account.
Maintain a map from account ID to its running total.
`Map.merge(key, delta, Long::sum)` inserts `delta` when absent or adds
it to the previous value when present. A linked map preserves first-seen
account order for deterministic display; ordinary `HashMap` is
sufficient if order is irrelevant.

``` java
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class AccountTotals {
    record Tx(String account, long deltaCents) {}

    static Map<String, Long> totals(List<Tx> transactions) {
        Map<String, Long> result = new LinkedHashMap<>();
        for (Tx tx : transactions) {
            if (tx == null || tx.account() == null || tx.account().isBlank()) {
                throw new IllegalArgumentException("Missing account");
            }
            result.merge(tx.account(), tx.deltaCents(), Math::addExact);
        }
        return result;
    }

    public static void main(String[] args) {
        var input = List.of(
            new Tx("A", 100), new Tx("B", 200),
            new Tx("A", -50), new Tx("C", 300),
            new Tx("B", -100), new Tx("A", 150)
        );
        System.out.println(totals(input)); // {A=200, B=100, C=300}
    }
}
```

**Complexity:** Expected **O(n)** time with hash-map operations,
**O(k)** extra space for `k` distinct accounts. Pathological hash
collisions can worsen map operation costs; normal interview analysis
assumes good hashing. **Edge cases:** Empty input → empty map; negative
entries are permitted for *net-change aggregation*; zero-value entries;
large totals (use `Math.addExact` to fail on overflow); malformed
account identifiers. For real money, use integer minor units or
fixed-scale `BigDecimal`, not `double`.

**Follow-ups:** (1) Can you stream it?
`Collectors.groupingBy(Tx::account, summingLong(Tx::deltaCents))`, but
`summingLong` does not detect overflow. (2) Does this compute a bank
account's final balance? Only if the initial balance is zero or is added
separately. (3) How would you process 100 million entries? Partition by
account, aggregate locally, merge partial totals, and ensure input is
not double-counted.

**Interview answer in one line:** "I aggregate once per transaction into
a hash map, use exact integer addition for money, and distinguish net
deltas from authoritative ledger balances."

## Q2. `ExecutorService` vs `CompletableFuture`: which thread pool is used?

**Question:** Does `CompletableFuture` create a thread pool? What is its
default executor? When should you supply your own?

**Answer and reasoning.** `ExecutorService` owns task execution and
lifecycle; `Future` mainly provides blocking retrieval and cancellation.
`CompletableFuture` represents a completion stage and supports
composition (`thenApply`, `thenCompose`, `allOf`, `exceptionally`). A
`CompletableFuture` instance does **not** itself own a dedicated pool.
`supplyAsync()` / `runAsync()` and async stages without an explicit
executor normally use `ForkJoinPool.commonPool()`; when the common
pool's parallelism is below two, the JDK may create a new thread per
task. Non-`Async` continuations may run on the completing thread or
another caller; they are **not** automatically dispatched to a new
thread.

**Runnable Java 17+ example:**

``` java
import java.util.concurrent.*;

public class AsyncPricing {
    static int remotePrice(String sku) {
        return sku.equals("book") ? 250 : 100;
    }

    public static void main(String[] args) {
        ExecutorService ioPool = Executors.newFixedThreadPool(8);
        try {
            CompletableFuture<Integer> price =
                CompletableFuture.supplyAsync(() -> remotePrice("book"), ioPool);
            CompletableFuture<Integer> tax =
                price.thenApply(p -> p / 10);
            int total = price.thenCombine(tax, Integer::sum)
                             .orTimeout(2, TimeUnit.SECONDS)
                             .join();
            System.out.println(total); // 275
        } finally {
            ioPool.shutdown();
        }
    }
}
```

**Why this works:** `supplyAsync` schedules work on a controlled pool.
`thenApply` computes 10% tax after the price arrives. `thenCombine`
waits for both stages and adds them. `orTimeout` bounds *waiting for the
future*, but does **not guarantee interruption of the underlying I/O
operation**; configure network connect/read timeouts as well.

**Complexity:** For the example, O(1) algorithmic work and O(1) extra
data; remote I/O latency dominates. For `m` independent tasks, total
work is O(m), while wall time depends on executor size, service latency,
and resource limits.

**Production notes:** Avoid blocking database/HTTP calls on the shared
common pool; provide a bounded executor or a virtual-thread-per-task
executor for appropriate blocking workloads, while still enforcing DB
pool, rate, and downstream concurrency limits. `thenCompose` flattens a
stage-returning operation; `thenApply` transforms a value. Prefer
`allOf` for many independent requests. Propagate errors and timeouts
deliberately.

**Edge cases:** Executor rejection, remote timeout, cancellation,
exception propagation (`join` wraps in `CompletionException`), nested
async stages, and shared-pool starvation. **Follow-up:** "Do virtual
threads remove the need for DB pooling?" No: threads become cheaper, but
database connections remain finite.

**Official reference:** [Java `CompletableFuture`
API](https://docs.oracle.com/en/java/javase/25/docs/api/java.base/java/util/concurrent/CompletableFuture.html).

## Q3. Hibernate N+1 queries and efficient handling of 500,000 records

**Questions:** What causes N+1? How do you detect and fix it? How do you
process five lakh records without exhausting memory?

**Explanation.** Query 1 loads 100 orders. Later, accessing
`order.getCustomer()` for each order can trigger 100 more SQL queries if
the association was not fetched. That is N+1. It occurs because object
traversal and SQL fetching are separate concerns. `FetchType.LAZY` does
not *cause* N+1 by itself; accessing unfetched associations in a loop
does.

**Detect:** Enable SQL/query counts in a non-production environment,
inspect Hibernate statistics, use a profiler/APM, and compare database
query count as result size grows. Look for repeated
`SELECT ... WHERE id=?` queries.

**Fix for a to-one relationship (assuming `Order.customer` and
`Order.status` exist):**

``` java
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.EntityGraph;
import org.springframework.data.jpa.repository.JpaRepository;

interface OrderRepository extends JpaRepository<Order, Long> {
    @EntityGraph(attributePaths = "customer")
    Page<Order> findByStatus(OrderStatus status, Pageable pageable);
}
```

This explicitly fetches the `customer` relationship with each page. For
read-only views, an explicit DTO projection can be even better because
it selects only needed columns. For to-many relationships, **do not
blindly combine collection fetch-joins with database pagination**: row
multiplication can produce incorrect/in-memory pagination or excessive
memory use. Prefer a page of IDs followed by a second batched
association query, batch fetching, or DTO projections.

**Processing 500,000 rows:** Use **keyset pagination** instead of
`findAll()` or a deep `OFFSET`. With an indexed, monotonically
increasing primary key, fetch the next batch where `id > lastSeenId`.
Process each batch, checkpoint `lastSeenId`, and continue. For mutable
datasets, decide whether to use a snapshot/high-water mark or tolerate
new rows; deletes and updates affect the business semantics.

``` java
import java.sql.*;
import javax.sql.DataSource;

public class LargeTableScanner {
    private final DataSource dataSource;
    public LargeTableScanner(DataSource dataSource) { this.dataSource = dataSource; }

    public void scan() throws SQLException {
        long lastId = 0L;              // Assumes positive, increasing IDs.
        final int batchSize = 1_000;
        while (true) {
            int count = 0;
            try (Connection c = dataSource.getConnection();
                 PreparedStatement ps = c.prepareStatement(
                     "SELECT id, payload FROM events WHERE id > ? " +
                     "ORDER BY id LIMIT ?")) {
                ps.setLong(1, lastId);
                ps.setInt(2, batchSize);
                try (ResultSet rs = ps.executeQuery()) {
                    while (rs.next()) {
                        long id = rs.getLong("id");
                        String payload = rs.getString("payload");
                        process(id, payload); // Must be retry-safe/idempotent.
                        lastId = id;
                        count++;
                    }
                }
            }
            if (count < batchSize) break;
        }
    }

    private void process(long id, String payload) {
        // Domain-specific processing goes here.
    }
}
```

**Complexity:** With an index on `id`, approximately O(n) row processing
plus O(n / b) indexed batch lookups for batch size `b`; O(b) or less
application memory depending on driver buffering. The example streams
each JDBC result set and retains no entire dataset; driver-level
buffering can vary. **Critical caveat:** If `process()` writes external
effects and the process crashes before checkpoint persistence,
reprocessing can occur; persist a checkpoint and use idempotent effects.

**Follow-ups:** When is a fetch-join better than an entity graph? What
does `hibernate.default_batch_fetch_size` do? How would you avoid a
long-lived transaction and connection? How would you backfill while live
writes continue?

**Official reference:** [Spring Data JPA query methods, entity graphs,
and keyset
scrolling](https://docs.spring.io/spring-data/jpa/reference/jpa/query-methods.html).

## Q4. Design a high-volume fund transfer system; what if credit fails after debit?

**Question:** Design transfers for millions of transactions. Prevent
duplicate requests and partial debit/credit. Discuss ordering, Kafka,
databases, and Sagas.

### Start with the consistency boundary

For **two accounts in the same transactional relational database**, use
**one ACID transaction** that (a) claims an idempotency key, (b) locks
the account rows in deterministic order, (c) checks funds, (d) debits
and credits, (e) writes a durable transfer/ledger record, and (f)
commits. If any SQL statement fails before commit, the database rolls
back **both** balance changes. Do **not** split the debit and credit
into independent database transactions when atomicity is available.

For **accounts owned by different services/databases**, there is no
automatic cross-database rollback. Use a durable transfer state machine,
a Saga/orchestrator, and compensating operations, or a financial-grade
ledger/settlement design with pending and reserved states. Compensation
is a **new business operation**, not a magic rollback; it can also fail
and needs retries, reconciliation, and manual escalation. Never assume
Kafka alone makes money transfers exactly once.

**Proposed architecture:**

``` text
Client --Idempotency-Key--> API / Auth / Rate limit
                             |
                             v
                        Transfer service
                             |
                 ACID DB: accounts + transfer ledger
                             |
                   transactional outbox row
                             |
                    Outbox publisher --> Kafka
                                              |
                                  Notifications / Analytics
```

**PostgreSQL schema sketch:**

``` sql
CREATE TABLE accounts (
    id BIGINT PRIMARY KEY,
    balance_cents BIGINT NOT NULL CHECK (balance_cents >= 0)
);

CREATE TABLE transfers (
    request_key VARCHAR(128) PRIMARY KEY,
    from_account BIGINT NOT NULL,
    to_account BIGINT NOT NULL,
    amount_cents BIGINT NOT NULL CHECK (amount_cents > 0),
    status VARCHAR(20) NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Add immutable, balanced debit/credit journal lines for auditability;
-- add an outbox table in the same transaction for Kafka publication.
```

**Atomic Java/JDBC implementation (illustrative core, assumes PostgreSQL
and existing accounts):**

``` java
import javax.sql.DataSource;
import java.sql.*;

public final class TransferService {
    private final DataSource ds;
    public TransferService(DataSource ds) { this.ds = ds; }

    public void transfer(String key, long from, long to, long cents)
            throws SQLException {
        if (key == null || key.isBlank() || from == to || cents <= 0) {
            throw new IllegalArgumentException("Invalid transfer");
        }
        try (Connection c = ds.getConnection()) {
            c.setAutoCommit(false);
            try {
                int claimed;
                try (PreparedStatement ps = c.prepareStatement(
                    "INSERT INTO transfers(request_key,from_account,to_account," +
                    "amount_cents,status) VALUES (?,?,?,?,'PENDING') " +
                    "ON CONFLICT (request_key) DO NOTHING")) {
                    ps.setString(1, key);
                    ps.setLong(2, from);
                    ps.setLong(3, to);
                    ps.setLong(4, cents);
                    claimed = ps.executeUpdate();
                }
                if (claimed == 0) {
                    try (PreparedStatement ps = c.prepareStatement(
                        "SELECT from_account,to_account,amount_cents,status " +
                        "FROM transfers WHERE request_key=?")) {
                        ps.setString(1, key);
                        try (ResultSet rs = ps.executeQuery()) {
                            if (!rs.next() || rs.getLong(1) != from ||
                                rs.getLong(2) != to || rs.getLong(3) != cents ||
                                !"SUCCESS".equals(rs.getString(4))) {
                                throw new IllegalStateException(
                                    "Key reused with different/pending request");
                            }
                        }
                    }
                    c.commit(); // Existing successful request: no double debit.
                    return;
                }

                // Lock in global ID order to reduce deadlock risk.
                long low = Math.min(from, to), high = Math.max(from, to);
                long lowBalance = lockBalance(c, low);
                long highBalance = lockBalance(c, high);
                long fromBalance = from == low ? lowBalance : highBalance;
                long toBalance = to == low ? lowBalance : highBalance;
                if (fromBalance < cents) {
                    throw new IllegalStateException("Insufficient funds");
                }
                setBalance(c, from, Math.subtractExact(fromBalance, cents));
                setBalance(c, to, Math.addExact(toBalance, cents));
                try (PreparedStatement ps = c.prepareStatement(
                    "UPDATE transfers SET status='SUCCESS' WHERE request_key=?")) {
                    ps.setString(1, key);
                    ps.executeUpdate();
                }
                // Also INSERT balanced ledger lines and an outbox event here.
                c.commit();
            } catch (SQLException | RuntimeException ex) {
                c.rollback();
                throw ex;
            }
        }
    }

    private static long lockBalance(Connection c, long id) throws SQLException {
        try (PreparedStatement ps = c.prepareStatement(
                "SELECT balance_cents FROM accounts WHERE id=? FOR UPDATE")) {
            ps.setLong(1, id);
            try (ResultSet rs = ps.executeQuery()) {
                if (!rs.next()) throw new IllegalArgumentException("Account not found");
                return rs.getLong(1);
            }
        }
    }

    private static void setBalance(Connection c, long id, long cents)
            throws SQLException {
        try (PreparedStatement ps = c.prepareStatement(
                "UPDATE accounts SET balance_cents=? WHERE id=?")) {
            ps.setLong(1, cents);
            ps.setLong(2, id);
            if (ps.executeUpdate() != 1) {
                throw new SQLException("Account update failed");
            }
        }
    }
}
```

**Why the code is safe in its stated scope:** The unique idempotency key
serializes competing identical requests; `ON CONFLICT` waits on
concurrent conflicting inserts. The row locks prevent lost updates and
use consistent ordering. Both balance changes and the success record
commit together. A failure before commit rolls them back. On a response
timeout after a successful commit, retrying the same key returns without
charging again. **Scope limits:** Production implementations need an
immutable double-entry ledger, authentication/authorization, anti-fraud
controls, request-key retention policy, reconciliation, handling of
terminal failure responses, an outbox, and transaction monitoring. The
snippet is not a full banking platform.

**Throughput design:** Partition workload by account ownership/shard,
avoid hot accounts, scale stateless APIs, pool DB connections, monitor
lock wait times and tail latency, and use outbox-driven asynchronous
side effects. **Ordering:** Kafka guarantees partition order for records
written to the same partition, not a global order across all partitions.
Partitioning by one account ID helps per-account processing but a
transfer touches *two* accounts, so use database locking or a
well-designed coordination protocol for cross-account consistency.

**Complexity:** Each transfer performs O(1) SQL operations and holds
O(1) application state; actual latency is driven by DB contention,
locks, disk, and network. **Edge cases:** Insufficient balance, same
source/destination, duplicate idempotency key with different payload,
concurrent opposite-direction transfers, overflow, DB crash before/after
commit, Kafka unavailable after commit.

**Follow-ups:** Why is an outbox needed? Why not just `@Transactional`
across two microservices? How would you reconcile lost notifications?
What if a Saga's compensation also fails?

## Q5. Kafka is healthy, but payments are duplicated: how do you prevent it?

**Questions:** How do you handle failed Kafka messages? Why can offsets
advance while business data is wrong? What happens after a rebalance or
crash?

**Root cause:** Kafka's consumer delivery and an external SQL side
effect are not automatically one atomic transaction. Example: a consumer
writes a payment, crashes before committing its Kafka offset, and
processes the same record again after restart. Even when Kafka
transactions provide exactly-once processing *between Kafka topics*,
they do not automatically deduplicate an external database or HTTP
payment gateway.

**Correct pattern:** Give each business event a stable `eventId`; store
a unique processed-event marker and the business mutation **inside the
same database transaction**. Commit/acknowledge the Kafka offset only
after the DB transaction succeeds. If the consumer crashes after DB
commit but before offset commit, replay becomes a harmless no-op. For
external APIs, use the downstream provider's idempotency key or an
outbox/workflow with reconciliation.

``` sql
CREATE TABLE processed_events (
    consumer_name VARCHAR(80) NOT NULL,
    event_id UUID NOT NULL,
    PRIMARY KEY (consumer_name, event_id)
);
CREATE TABLE inventory (
    sku VARCHAR(80) PRIMARY KEY,
    available INTEGER NOT NULL CHECK (available >= 0)
);
```

**Spring Boot/Spring Kafka core (assumes the above tables and a
deserializable `OrderCreated` event):**

``` java
import java.util.UUID;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

record OrderCreated(UUID eventId, String sku, int quantity) {}

@Service
class InventoryService {
    private final JdbcTemplate jdbc;
    InventoryService(JdbcTemplate jdbc) { this.jdbc = jdbc; }

    @Transactional
    public void reserve(OrderCreated e) {
        if (e.eventId() == null || e.sku() == null || e.quantity() <= 0) {
            throw new IllegalArgumentException("Invalid event");
        }
        int inserted = jdbc.update(
            "INSERT INTO processed_events(consumer_name,event_id) " +
            "VALUES (?,?) ON CONFLICT DO NOTHING",
            "inventory-reserver", e.eventId());
        if (inserted == 0) return; // Duplicate: business effect already committed.

        int updated = jdbc.update(
            "UPDATE inventory SET available=available-? " +
            "WHERE sku=? AND available>=?",
            e.quantity(), e.sku(), e.quantity());
        if (updated != 1) {
            throw new IllegalStateException("Insufficient stock or unknown SKU");
        }
        // Marker and reservation commit together on successful return.
    }
}

@Component
class InventoryListener {
    private final InventoryService service;
    InventoryListener(InventoryService service) { this.service = service; }

    @KafkaListener(topics = "order-created", groupId = "inventory")
    public void consume(OrderCreated event) {
        service.reserve(event); // Propagate exceptions; do not swallow them.
    }
}
```

**Listener configuration:** Configure consumer
`enable.auto.commit=false` and container `AckMode.RECORD` (or another
deliberately chosen acknowledgment policy). A successfully returning
listener then advances the offset according to container semantics;
failed processing must be retried or sent to a dead-letter topic by a
configured error handler. A malformed message should not be retried
forever. For **insufficient stock**, the business decision may be to
publish `ReservationRejected` and commit, rather than endlessly
retrying; that depends on the workflow.

**Retry strategy:** Transient errors (DB timeout, temporary network
failure) → bounded exponential backoff with jitter; permanent errors
(schema invalid, business rejection) → explicit rejection event or DLT,
alert, and replay procedure. Monitor consumer lag,
`max.poll.interval.ms`, rebalance frequency, processing time, DB lock
waits, DLT volume, and **business-level duplicate counters**. Preserve
ordering when required: rerouting one event to a retry topic can allow
later same-key events to overtake it.

**Complexity:** Per event, expected O(1) indexed insert/update, O(1)
application memory; persistent dedup storage grows O(number of unique
event IDs retained). Use a retention policy compatible with maximum
replay horizon and audit requirements.

**Edge cases:** Crash after DB commit/before offset commit; two
consumers racing on same event ID; DLT publication failure; schema
poison pills; out-of-order events; dedup table retention too short;
duplicate events with *different* event IDs (producer/business must
define idempotency identity). **Follow-up:** Why doesn't Kafka
exactly-once guarantee an external credit-card charge occurs once?

**Official reference:** [Spring Kafka exception handling and dead-letter
publishing](https://docs.spring.io/spring-kafka/reference/kafka/annotation-error-handling.html).

## Q6. `HashMap` vs `ConcurrentHashMap`: which one for a concurrent counter?

**Question:** Explain the key differences, then implement safe frequency
counting from multiple threads.

**Answer:** `HashMap` is not thread-safe: concurrent mutations can lose
updates or expose inconsistent state. `ConcurrentHashMap` supports
concurrent access, atomic per-key operations such as `computeIfAbsent`,
and weakly consistent iterators. Its iterators are **not** a snapshot
and do not throw `ConcurrentModificationException` as a fail-fast
contract. It rejects null keys/values. `Collections.synchronizedMap`
wraps map operations with synchronization, but iteration still requires
external locking for a consistent traversal. Do not say modern
`ConcurrentHashMap` uses a fixed number of segment locks: that describes
older implementations, not Java 8+ internals.

``` java
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.LongAdder;

public class ConcurrentCounts {
    private final ConcurrentHashMap<String, LongAdder> counts =
        new ConcurrentHashMap<>();

    public void increment(String key) {
        counts.computeIfAbsent(key, ignored -> new LongAdder()).increment();
    }

    public long count(String key) {
        LongAdder adder = counts.get(key);
        return adder == null ? 0L : adder.sum();
    }
}
```

**Reasoning:** `computeIfAbsent` safely installs one counter per key;
`LongAdder` spreads increments under contention. `sum()` during
concurrent updates is not an atomic snapshot; use `AtomicLong` or
stronger coordination when exact linearizable reads matter.
**Complexity:** Expected O(1) per increment and O(k) keys stored;
contention and resize affect real throughput. **Edge cases:** Null keys,
overflow, compound check-then-act logic, iteration during writes, hot
keys. **Follow-up:** Is `map.get(key) + 1` safe on `ConcurrentHashMap`?
No; use `merge`, `compute`, or an atomic value.

## Q7. Concurrent transfers and crash recovery: optimistic vs pessimistic locking

**Question:** Two transfers target the same account simultaneously. How
do you prevent lost updates? What happens if the app crashes halfway
through?

**Pessimistic locking:** `SELECT ... FOR UPDATE` holds row locks until
transaction completion; it is useful when contention is high or retrying
a failed update is expensive. Lock rows in a consistent global order to
reduce deadlocks, but still retry selected deadlock/serialization
failures with bounded backoff. The Q4 JDBC implementation illustrates
this.

**Optimistic locking:** Store a `version` column and update
conditionally. If no row matches, someone else modified the account;
reload and retry the *entire* business transaction with a bounded retry
policy. Example:

``` sql
ALTER TABLE accounts ADD COLUMN version BIGINT NOT NULL DEFAULT 0;

UPDATE accounts
SET balance_cents = balance_cents - :amount,
    version = version + 1
WHERE id = :accountId
  AND version = :expectedVersion
  AND balance_cents >= :amount;
-- Require exactly one updated row; zero means conflict or insufficient funds.
```

**Crash behavior:** A DB transaction that did not commit is rolled back
by the DB when the connection/session dies. A committed transfer
survives an app crash, but the caller might not know it succeeded: the
idempotency key makes retry safe. Do not equate "application returned an
error" with "DB transaction definitely rolled back." For cross-service
work, use durable Saga states, compensation, and reconciliation.

**Trade-offs:** Pessimistic locks block contending transactions and risk
lock queues; optimistic locking has good throughput under low contention
but retries under conflicts. Both need idempotency and carefully defined
transaction boundaries. **Complexity:** O(1) DB statements per attempt,
but worst-case retries/lock wait are workload-dependent. **Edge cases:**
Deadlocks, timeouts, crash after commit, duplicate HTTP requests,
concurrent transfers in opposite directions, stale reads. **Follow-up:**
Can a distributed Redis lock replace the DB constraint? No; it can
supplement coordination but is not a substitute for database integrity
and transactional checks.

------------------------------------------------------------------------

## Engineering developments worth tracking

### 1. Java 27 is available --- 15 September 2026 (**verified, official**)

**What changed:** Java 27 shipped with nine JEPs. Notable items include
compact object headers by default (JEP 534), G1 as the default GC in all
environments (JEP 523), post-quantum hybrid key exchange for TLS 1.3
(JEP 527), and in-process JFR data redaction (JEP 536). Structured
Concurrency (JEP 533) remains a **preview**, and the Vector API (JEP
537) remains an **incubator** feature.

**Why it matters:** Memory footprint, runtime defaults, diagnostics, and
TLS compatibility can affect microservices. Don't assume preview APIs
are stable or production-ready just because they appear in a GA JDK.
Benchmark your service, review dependencies, and choose an
organizational JDK support policy before upgrading.

**Read:** [Oracle Java 27 announcement, 15 Sep
2026](https://www.oracle.com/news/announcement/oracle-releases-java-27-and-strengthens-post-quantum-cryptography-support-2026-09-15/).

### 2. Spring Boot 4.2.0-M2 --- 25 September 2026 (**verified, milestone/pre-release**)

**What changed:** The M2 milestone includes LDAP/LDAPS SSL bundle
support and support for OpenTelemetry semantic conventions and common
OTLP endpoint/header/compression configuration. The release announcement
describes 141 enhancements, fixes, dependency upgrades, and
documentation changes.

**Why it matters:** Standardized telemetry and secure LDAP configuration
reduce hand-written glue in enterprise Java services. **M2 is a
milestone**, not a stable GA recommendation; evaluate in a test
environment and keep production dependencies on supported stable
versions.

**Read:** [Spring Boot 4.2.0-M2 announcement, 25 Sep
2026](https://spring.io/blog/2026/09/25/spring-boot-4-2-0-M2-available-now/).

### 3. Apache Kafka 4.2.2 maintenance release --- 29 September 2026 (**verified, official**)

**What changed:** Kafka 4.2.2 is a maintenance/bug-fix release for the
4.2 line, including critical fixes. **Kafka 4.3.1 also exists**; 4.2.2
should not be described as the newest Kafka release overall.

**Why it matters:** Teams pinned to the 4.2 line should review release
notes, client/broker compatibility, and upgrade guidance rather than
upgrading blindly. Test consumer rebalances, idempotency, Kafka Streams
state stores, and rollback plans.

**Read:** [Kafka 4.2.2 announcement, 29 Sep
2026](https://kafka.apache.org/blog/2026/09/29/apache-kafka-4.2.2-release-announcement/).

### 4. AWS Well-Architected Agent preview --- 1 October 2026 (**verified, official**)

**What changed:** AWS announced a preview agent that evaluates cost,
security, performance, and reliability; it can review Terraform, CDK,
and CloudFormation and propose fixes/runbooks. Access is currently in
selected US Regions, with AWS Support plan requirements; workloads from
commercial AWS Regions can be onboarded.

**Why it matters:** Architecture review is moving toward AI-assisted
analysis of infrastructure-as-code and actual workload telemetry. Treat
proposed fixes as **reviewable changes**, not automatically safe
deployments. Verify IAM blast radius, availability impacts, and cost
before applying.

**Read:** [AWS announcement, 1 Oct
2026](https://aws.amazon.com/about-aws/whats-new/2026/10/aws-well-architected-agent/).

### 5. AI-assisted security review is expanding --- 6 October 2026 (**reported by Reuters**)

**What changed:** Anthropic expanded access to advanced models for
vetted cybersecurity teams through an updated Cyber Verification
Program, building on its vulnerability-finding initiative. Reuters
reported large numbers of findings; these are **reported project
figures**, not proof that any individual codebase will be secure with AI
scanning alone.

**Why it matters:** Secure software development increasingly combines
AI-based review with conventional SAST/DAST, dependency scanning, human
triage, and patch verification. For backend engineers, prioritize auth
boundaries, secret handling, SSRF, deserialization, supply-chain
vulnerabilities, and evidence-based remediation.

**Read:** [Reuters, 6 Oct
2026](https://www.reuters.com/legal/litigation/anthropic-opens-its-most-powerful-ai-models-more-security-teams-2026-10-06/).

------------------------------------------------------------------------

## Today's 15-minute revision plan

1.  **4 min --- Java:** Explain why `CompletableFuture` is not a thread
    pool and when `LongAdder` is useful.
2.  **4 min --- Kafka:** Walk through a crash *after* database commit
    but *before* offset commit. Explain why the dedup table fixes it.
3.  **4 min --- Transactions:** Explain why same-DB debit/credit should
    be one ACID transaction, and when a Saga is necessary.
4.  **3 min --- Production:** List two dangers of N+1 queries and two
    dangers of naïvely paginating a collection fetch-join.

### Source inventory and confidence

-   **Public LinkedIn posts, question text verified:**
    [EY](https://www.linkedin.com/posts/anubhi9_java-springboot-microservices-activity-7509305270301331457-_R6C),
    [Infosys](https://www.linkedin.com/posts/er-naimuddin-alam-24067511b_infosys-infosysinterview-interviewexperience-activity-7512946809846210561-9u5K),
    [Coforge](https://www.linkedin.com/posts/naushad-a-93081b179_coforge-coforgehiring-hiring2026-activity-7509911367194411008-Du-M),
    [Kafka
    scenario](https://www.linkedin.com/posts/satyamparmar_kafka-interview-question-1-consumer-lag-activity-7497085822249332736-rBc6).
    These are **not** proof that the companies themselves published or
    endorsed the question lists.
-   **Primary technical documentation:** [Java
    `CompletableFuture`](https://docs.oracle.com/en/java/javase/25/docs/api/java.base/java/util/concurrent/CompletableFuture.html),
    [Spring Data
    JPA](https://docs.spring.io/spring-data/jpa/reference/jpa/query-methods.html),
    [Spring Kafka
    errors](https://docs.spring.io/spring-kafka/reference/kafka/annotation-error-handling.html).
-   **Release and product announcements:**
    [Oracle](https://www.oracle.com/news/announcement/oracle-releases-java-27-and-strengthens-post-quantum-cryptography-support-2026-09-15/),
    [Spring](https://spring.io/blog/2026/09/25/spring-boot-4-2-0-M2-available-now/),
    [Apache
    Kafka](https://kafka.apache.org/blog/2026/09/29/apache-kafka-4.2.2-release-announcement/),
    [AWS](https://aws.amazon.com/about-aws/whats-new/2026/10/aws-well-architected-agent/).

**Limitations and deduplication:** This is the first dated digest in
this series, so there is no prior digest file to compare against. The
report deduplicates repeated question themes *within this document*.
Access was limited to publicly readable pages; no personal LinkedIn
feed, saved posts, or authenticated activity was accessed. Code snippets
are educational: standalone Java examples can be compiled directly;
Spring and JDBC fragments require a configured application, schema, and
integration tests before production use.

# Advanced Java Backend Practice — 2026-10-28
## Kafka Reliability: Transactional Outbox + Idempotent Consumers

**Runtime:** Java 21, Spring Boot 3.x / Spring for Apache Kafka 3.x, PostgreSQL 16+, Kafka 3.x or 4.x, Debezium 3.x  
**Time budget:** 15 minutes — 3-minute concept, 8-minute exercise, 4-minute walkthrough  
**Scenario:** An order service saves an order and publishes `OrderPlaced` for inventory, billing and notification services.

> Version note: APIs and configuration must be checked against the versions deployed. The design below relies on database transactions, Kafka delivery semantics and Debezium CDC, not on a particular convenience API.

## Prerequisites

- Spring `@Transactional` and relational database transactions
- Kafka topics, consumer groups, offsets and at-least-once delivery
- Basic SQL and unique constraints
- Difference between a timeout/failure observed by a caller and what may have committed remotely

## 1. Concept question — 3 minutes

### Question

An order endpoint does this:

~~~java
@Transactional
public Order placeOrder(CreateOrder command) {
    Order order = orderRepository.save(toOrder(command));
    kafkaTemplate.send("order-events",
        new OrderPlaced(order.id(), order.amountCents()));
    return order;
}
~~~

Assume `@Transactional` controls the PostgreSQL transaction. Can this guarantee that the order row and Kafka event either both commit or neither commits?

### Worked answer

**No. A normal database transaction does not automatically make a database write and a Kafka publish one atomic operation.** This is a dual-write problem.

Two failure windows illustrate it:

1. **Database commits, publish fails:** the order exists, but downstream services never learn about it.
2. **Kafka publish succeeds, database rolls back:** downstream services see an event for an order that does not exist.

Calling `send()` inside a method annotated `@Transactional` does not, by itself, enlist PostgreSQL and Kafka in one distributed transaction. Spring Kafka can coordinate Kafka records and Kafka consumer offsets in a Kafka transaction, but that is not the same as atomically committing an arbitrary relational database transaction with Kafka. See the official [Spring Kafka transaction reference](https://docs.spring.io/spring-kafka/reference/kafka/transactions.html).

### The design principle

**Write business state and an event record in the same database transaction; publish that event from the database log using change-data capture (CDC).** This is the transactional outbox pattern. Debezium's Outbox Event Router is designed to capture outbox-table changes and route them to Kafka: [official Debezium documentation](https://debezium.io/documentation/reference/stable/transformations/outbox-event-router.html).

Hinglish recap: Order aur event ko ek hi PostgreSQL transaction mein save karo. Agar transaction rollback hui, dono absent honge; commit hui, dono present honge. Uske baad CDC event ko Kafka tak pahunchata hai. Isse direct dual-write gap remove hota hai—lekin consumer ko duplicate delivery ke liye phir bhi ready rehna hai.

## 2. Practical exercise — 8 minutes

### Requirements

- An order and its `OrderPlaced` event must be committed together.
- A process crash must not silently lose a committed event.
- Consumers must tolerate redelivery without applying the same database-side effect twice.
- Preserve per-order ordering where required.
- No claim of global end-to-end “exactly once” across PostgreSQL, Kafka and arbitrary external services.

### Step 1 — Create the outbox table

Use the conventional column names expected by Debezium's default Outbox Event Router mapping, or explicitly configure the router to match your schema.

~~~sql
CREATE TABLE outbox_event (
    id             UUID PRIMARY KEY,
    aggregatetype  VARCHAR(255) NOT NULL,
    aggregateid    VARCHAR(255) NOT NULL,
    type           VARCHAR(255) NOT NULL,
    payload        JSONB NOT NULL,
    created_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX outbox_event_created_at_idx
    ON outbox_event (created_at);
~~~

- `id`: stable event ID, used for deduplication.
- `aggregateid`: order ID; configure the router to use it as the Kafka key when per-order ordering is required.
- `type`: event type, such as `OrderPlaced`.
- `payload`: versioned event envelope.
- `created_at`: operational metadata; it is not a safe global ordering mechanism by itself.

In production, define retention/cleanup that is compatible with connector progress and recovery. Do not delete rows merely because they are old; first understand whether the connector has captured them and how a restored connector resumes.

### Step 2 — Write the order and event atomically

The following is a focused Spring-style example. `orderRepository.insert(...)` represents the application's ordinary JDBC/JPA insert. `outboxRepository.append(...)` uses the same PostgreSQL data source and transaction manager.

~~~java
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.UUID;

record OrderPlaced(
        UUID eventId,
        UUID orderId,
        long amountCents,
        int schemaVersion) {}

@Repository
final class OutboxRepository {
    private final JdbcTemplate jdbc;
    private final ObjectMapper mapper;

    OutboxRepository(JdbcTemplate jdbc, ObjectMapper mapper) {
        this.jdbc = jdbc;
        this.mapper = mapper;
    }

    void append(UUID eventId, String aggregateType,
                String aggregateId, String eventType, Object payload) {
        try {
            String json = mapper.writeValueAsString(payload);
            jdbc.update("""
                INSERT INTO outbox_event
                    (id, aggregatetype, aggregateid, type, payload)
                VALUES (?, ?, ?, ?, CAST(? AS jsonb))
                """,
                eventId, aggregateType, aggregateId, eventType, json);
        } catch (JsonProcessingException e) {
            throw new IllegalArgumentException(
                "Cannot serialize outbox event", e);
        }
    }
}

@Service
final class OrderService {
    private final OrderRepository orderRepository;
    private final OutboxRepository outboxRepository;

    OrderService(OrderRepository orderRepository,
                 OutboxRepository outboxRepository) {
        this.orderRepository = orderRepository;
        this.outboxRepository = outboxRepository;
    }

    @Transactional
    public Order placeOrder(CreateOrder command) {
        Order order = orderRepository.insert(command);

        UUID eventId = UUID.randomUUID();
        OrderPlaced event = new OrderPlaced(
            eventId, order.id(), order.amountCents(), 1);

        outboxRepository.append(
            eventId, "Order", order.id().toString(),
            "OrderPlaced", event);

        return order;
    }
}
~~~

`Order`, `CreateOrder` and `OrderRepository` are application domain types; the shown repository contract is intentionally illustrative. The important condition is that both writes use the **same database transaction**. Do not put the outbox insert on another data source or use an independent transaction.

**What this fixes:** if order insertion, event serialization or outbox insertion fails, the transaction rolls back. If the transaction commits, the order and its outbox row are durable together. The HTTP request no longer needs to synchronously wait for Kafka publication.

### Step 3 — Publish with CDC, not a best-effort after-commit callback

Configure Debezium's PostgreSQL connector to capture the outbox table and apply the Outbox Event Router transformation. The connector reads committed database-log changes and publishes the corresponding records to Kafka. Configure the event key from `aggregateid` if same-order partition ordering is required.

Do not replace this with only:

~~~java
@TransactionalEventListener(phase = TransactionPhase.AFTER_COMMIT)
void publish(OrderPlaced event) {
    kafkaTemplate.send("order-events", event.orderId().toString(), event);
}
~~~

An after-commit callback avoids publishing before the DB commit, but a process crash after DB commit and before a successful Kafka send can still lose the event. A durable outbox plus recoverable CDC closes that gap.

### Step 4 — Make the consumer idempotent

A CDC connector, producer retry, consumer restart or offset-commit failure can cause an event to be observed again. Use a stable event ID and place deduplication plus the consumer's database side effect in the **same consumer database transaction**.

Schema:

~~~sql
CREATE TABLE processed_event (
    event_id UUID PRIMARY KEY,
    processed_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Example business table:
CREATE TABLE inventory_reservation (
    order_id UUID PRIMARY KEY,
    reserved_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
~~~

Focused Spring consumer example:

~~~java
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

@Component
final class InventoryConsumer {
    private final JdbcTemplate jdbc;

    InventoryConsumer(JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    @KafkaListener(topics = "order-events", groupId = "inventory")
    @Transactional
    public void onOrderPlaced(OrderPlaced event) {
        int inserted = jdbc.update("""
            INSERT INTO processed_event (event_id)
            VALUES (?)
            ON CONFLICT DO NOTHING
            """, event.eventId());

        if (inserted == 0) {
            // Already applied in a committed DB transaction.
            return;
        }

        // Must use the same database transaction/data source.
        jdbc.update("""
            INSERT INTO inventory_reservation (order_id)
            VALUES (?)
            """, event.orderId());
    }
}
~~~

This example assumes Spring's listener invokes the transactional method through its proxy, the DB transaction commits before the listener returns successfully, and the container manages Kafka offsets with auto-commit disabled. Configure and test those settings for the deployed Spring Kafka version. Do not manually commit the Kafka offset before the database transaction commits.

If the reservation insert fails, the DB transaction rolls back both the `processed_event` insert and the reservation. The listener must propagate the failure so the configured retry/error handler can act. If the DB commit succeeds but the process crashes before the Kafka offset is committed, redelivery finds the unique event ID and skips the duplicate side effect.

**Important boundary:** this provides effectively-once application of a database effect under the stated transaction assumptions; it does not make arbitrary external calls exactly once. If the consumer calls a payment provider or another service, use that system's idempotency key, or write a second outbox in the consumer's DB transaction and let another delivery process handle it.

### Step 5 — Kafka configuration and delivery semantics

For a direct Kafka producer, Kafka's idempotent producer protects against duplicate records caused by supported producer-protocol retries. Current Kafka producer documentation describes the relationship among `enable.idempotence`, `acks=all`, retries and in-flight requests: [official producer configuration](https://kafka.apache.org/42/configuration/producer-configs/).

But producer idempotence does **not** deduplicate two separate application-level sends of the same business event with a new producer sequence. The outbox event ID and consumer deduplication key solve a different problem.

Kafka transactions can atomically write Kafka records and consumed Kafka offsets for Kafka-to-Kafka processing. They do not automatically include PostgreSQL or an external HTTP service in that transaction. Read the [Spring Kafka transaction documentation](https://docs.spring.io/spring-kafka/reference/kafka/transactions.html) before claiming end-to-end exactly-once semantics.

## 3. Walkthrough, edge cases and trade-offs — 4 minutes

### Failure walkthrough

| Failure point | Durable state | Recovery |
|---|---|---|
| Order insert fails | Neither order nor outbox row commits | Return failure; caller may retry under an API idempotency policy |
| Outbox insert fails | Transaction rolls back order too | Fix/retry transaction |
| Process dies after DB commit, before Kafka publish | Order and outbox row remain | CDC resumes from its durable position and publishes |
| Consumer receives event twice | Same event ID is delivered again | Unique `processed_event.event_id` suppresses duplicate DB effect |
| Consumer DB transaction fails | Dedup marker and business effect roll back | Propagate failure; retry or route to DLT according to policy |
| Consumer DB commit succeeds, offset commit fails | DB effect exists; Kafka may redeliver | Dedup marker makes redelivery harmless |
| External side effect succeeds but consumer crashes before recording it | Remote effect may have happened | Use remote idempotency or another outbox; local dedup alone is insufficient |

### How to derive the design in an interview

1. **Identify the atomicity boundary.** The order and event intent live in PostgreSQL, so commit them in one DB transaction.
2. **Find the cross-system gap.** A direct Kafka send is not atomic with that DB transaction.
3. **Make publication recoverable.** Store an outbox row and publish it through CDC.
4. **Assume redelivery.** Use a stable event ID and a unique constraint in the consumer.
5. **Align the side effect with deduplication.** Commit the inbox marker and business DB effect together.
6. **State the boundary honestly.** External APIs and Kafka offsets need their own idempotency/recovery handling.

### Common mistakes

- Assuming `@Transactional` automatically covers both JDBC and Kafka.
- Treating Kafka producer idempotence as business-event deduplication.
- Using only an after-commit callback instead of durable publication intent.
- Committing a consumer offset before its DB effect is durable.
- Writing the dedup marker in a separate transaction from the business update.
- Assuming a unique inbox table protects a side effect performed in another service.
- Using event creation timestamps as a universal total order across concurrent transactions.
- Deleting outbox rows before understanding CDC offsets, snapshots and recovery.
- Ignoring poison messages, schema evolution, DLT monitoring and replay procedures.

### Production trade-offs and observability

- **Latency:** the HTTP response can complete after the database commit, before Kafka consumers process the event. Design the API contract around asynchronous propagation.
- **Storage:** the outbox grows until retention/cleanup removes eligible rows. Monitor table size, WAL retention and connector lag.
- **Ordering:** use aggregate IDs as Kafka keys for per-aggregate partitioning, and design updates for one aggregate to be ordered appropriately.
- **Schema evolution:** version event envelopes and keep consumers tolerant of compatible additions.
- **Retries and DLT:** classify transient vs permanent failures; alert on DLT growth and provide safe replay tooling.
- **Idempotency retention:** retain processed IDs for at least the maximum replay/redelivery horizon, or use a durable business uniqueness constraint.
- **Monitoring:** track outbox-to-Kafka lag, connector status, consumer lag, duplicate-suppression counts, transaction rollbacks and DLT volume.

### Interview-ready answer

> I would avoid a direct database-plus-Kafka dual write. The order service writes the order and a versioned outbox event in the same PostgreSQL transaction. Debezium CDC publishes committed outbox rows to Kafka, making publication recoverable after process crashes. Consumers use a stable event ID and a unique inbox/processed-event record in the same transaction as their database side effect. This gives reliable at-least-once delivery with idempotent effects under explicit transaction assumptions—not universal exactly-once execution across arbitrary external systems.

### Follow-up questions with answers

**Q1. Does Kafka producer idempotence remove the need for a consumer inbox?**  
No. It handles supported producer retry duplicates, not arbitrary re-publication of the same business event by application logic or downstream redelivery.

**Q2. Why is an after-commit listener insufficient?**  
The process can die after the database commit and before the callback successfully publishes. An outbox row is durable recovery state.

**Q3. What if the consumer calls a remote payment API?**  
The consumer's DB transaction cannot atomically commit that remote API call. Pass a stable idempotency key to the provider and reconcile uncertain outcomes; where appropriate, use another outbox.

**Q4. Does Kafka exactly-once semantics cover PostgreSQL writes?**  
Not by itself. Kafka transactions cover Kafka records and Kafka offsets in supported processing flows. A database write needs its own transaction and duplicate/recovery strategy.

**Q5. How do you preserve per-order ordering?**  
Publish with the order ID as the Kafka key, so events for that key route to the same partition, and ensure the source/connector and consumer processing preserve the required order. Parallel processing must not commit or apply later events ahead of earlier ones when business ordering matters.

## Authoritative references

- [Debezium Outbox Event Router — stable docs](https://debezium.io/documentation/reference/stable/transformations/outbox-event-router.html)
- [Spring for Apache Kafka — transactions](https://docs.spring.io/spring-kafka/reference/kafka/transactions.html)
- [Apache Kafka 4.2 producer configuration](https://kafka.apache.org/42/configuration/producer-configs/)
- [Apache Kafka documentation](https://kafka.apache.org/documentation/)

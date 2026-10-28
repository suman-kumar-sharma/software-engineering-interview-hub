
# Advanced Java Backend Practice — 2026-10-28
## Kafka Reliability: Transactional Outbox + Idempotent Consumer (Hinglish)

**Runtime:** Java 21, Spring Boot 3.x / Spring for Apache Kafka 3.x, PostgreSQL 16+, Kafka 3.x ya 4.x, Debezium 3.x  
**Time budget:** 15 minutes — 3-minute concept, 8-minute practical exercise, 4-minute walkthrough  
**Scenario:** Order service order save karta hai aur `OrderPlaced` event Inventory, Billing aur Notification services ko bhejna chahta hai.

> Version note: Apne deployed versions ke APIs/configuration ko verify karna. Neeche ka design database transaction, Kafka delivery semantics aur Debezium CDC par based hai—kisi ek convenience API par nahi.

## Prerequisites

- Spring `@Transactional` aur relational DB transactions
- Kafka topics, consumer groups, offsets aur at-least-once delivery
- Basic SQL aur unique constraints
- Caller ko failure/timeout dikhna aur remote system mein operation commit hona—dono alag ho sakte hain

## 1. Concept question — 3 minutes

### Question

Order endpoint ye code use karta hai:

~~~java
@Transactional
public Order placeOrder(CreateOrder command) {
    Order order = orderRepository.save(toOrder(command));
    kafkaTemplate.send("order-events",
        new OrderPlaced(order.id(), order.amountCents()));
    return order;
}
~~~

Maan lo `@Transactional` PostgreSQL transaction control karta hai. Kya guarantee hai ki order row aur Kafka event dono ek saath commit honge, ya dono rollback honge?

### Worked answer

**Nahi. Normal database transaction automatically database write aur Kafka publish ko ek atomic operation nahi banata.** Is problem ko dual-write kehte hain.

Do failure windows dekho:

1. **Database commit ho gaya, Kafka publish fail ho gaya:** order DB mein hai, lekin downstream services ko event nahi mila.
2. **Kafka publish ho gaya, database rollback ho gaya:** downstream services ko aise order ka event mil gaya jo DB mein exist hi nahi karta.

Sirf `@Transactional` method ke andar `kafkaTemplate.send()` call karne se PostgreSQL aur Kafka ek distributed transaction mein automatically enlist nahi hote. Spring Kafka transactions Kafka records aur Kafka consumer offsets ko coordinate kar sakte hain, lekin arbitrary relational DB transaction ko automatically usi atomic boundary mein include nahi karte. Official reference: [Spring Kafka transactions](https://docs.spring.io/spring-kafka/reference/kafka/transactions.html).

### Design principle

**Business state aur event intent ko ek hi database transaction mein save karo; phir database log se CDC ke through event publish karo.** Is pattern ko Transactional Outbox kehte hain. Debezium ka Outbox Event Router outbox table ke changes capture karke Kafka mein route karne ke liye bana hai: [official Debezium docs](https://debezium.io/documentation/reference/stable/transformations/outbox-event-router.html).

Hinglish recap: Order aur event ko ek hi PostgreSQL transaction mein likho. Transaction rollback hui toh dono absent; commit hui toh dono durable. Uske baad CDC event ko Kafka tak pahunchata hai. Direct dual-write ka gap solve hota hai—par consumer ko duplicate event milne ki possibility phir bhi handle karni hai.

## 2. Practical exercise — 8 minutes

### Requirements

- Order aur `OrderPlaced` event saath commit hon.
- Process crash se committed event silently lose na ho.
- Consumer redelivery hone par same database-side effect do baar apply na kare.
- Jahan zaroori ho, same order ke events ka order preserve ho.
- PostgreSQL, Kafka aur arbitrary external services ke across global “exactly once” ka false claim na ho.

### Step 1 — Outbox table banao

Debezium ke default Outbox Event Router mapping ke conventional column names use karo, ya router ko apne schema ke according configure karo.

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

- `id`: stable event ID; deduplication ke liye use hoga.
- `aggregateid`: order ID; same order ke events ko same Kafka key/partition par bhejne ke liye router configure kar sakte ho.
- `type`: event type, jaise `OrderPlaced`.
- `payload`: versioned event envelope.
- `created_at`: operational metadata hai; isko global ordering ka proof mat samajhna.

Production mein retention/cleanup connector ki progress aur recovery strategy ke saath compatible rakho. Sirf row purani hai isliye delete mat karo—pehle confirm karo ki connector ne usko capture kiya hai aur connector restore hone par resume kaise karega.

### Step 2 — Order aur event ko atomically likho

Ye focused Spring-style example hai. `orderRepository.insert(...)` application ka normal JDBC/JPA insert represent karta hai. `outboxRepository.append(...)` ko **same PostgreSQL datasource aur transaction manager** use karna chahiye.

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

`Order`, `CreateOrder` aur `OrderRepository` application ke domain types hain; repository call ka contract illustrative hai. Core condition ye hai ki dono writes **same DB transaction** ka part hon. Outbox ko doosre datasource ya independent transaction mein mat likhna.

**Isse kya fix hua?** Order insert, event serialization ya outbox insert fail hua toh transaction rollback hoga. Transaction commit hui toh order aur outbox row dono durable hain. HTTP request ko Kafka publish complete hone tak synchronously wait nahi karna padta.

### Step 3 — CDC se publish karo, sirf after-commit callback par depend mat karo

Debezium PostgreSQL connector configure karo taaki outbox table capture ho aur Outbox Event Router transformation apply ho. Connector committed database-log changes padhta hai aur corresponding records Kafka mein publish karta hai. Same order ke events ko same partition par route karna ho toh event key `aggregateid` configure karo.

Sirf is callback ko use karna enough nahi hai:

~~~java
@TransactionalEventListener(phase = TransactionPhase.AFTER_COMMIT)
void publish(OrderPlaced event) {
    kafkaTemplate.send("order-events", event.orderId().toString(), event);
}
~~~

After-commit callback ka fayda hai ki DB commit se pehle event publish nahi hota. Lekin DB commit ke baad aur successful Kafka send se pehle process crash ho gaya, toh event lose ho sakta hai. Durable outbox + recoverable CDC is gap ko close karte hain.

### Step 4 — Consumer ko idempotent banao

CDC connector, producer retry, consumer restart ya offset-commit failure ki wajah se event dobara aa sakta hai. Stable event ID use karo aur deduplication marker ko consumer ke database-side effect ke saath **same transaction** mein likho.

Schema:

~~~sql
CREATE TABLE processed_event (
    event_id UUID PRIMARY KEY,
    processed_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE inventory_reservation (
    order_id UUID PRIMARY KEY,
    reserved_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
~~~

Focused Spring consumer:

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
            // Is event ka DB effect pehle commit ho chuka hai.
            return;
        }

        // Same database transaction/data source mein hona chahiye.
        jdbc.update("""
            INSERT INTO inventory_reservation (order_id)
            VALUES (?)
            """, event.orderId());
    }
}
~~~

Ye example assume karta hai ki Spring listener transactional method ko proxy ke through invoke karta hai, DB transaction listener ke successful return se pehle commit hota hai, aur container auto-commit disabled rakh kar Kafka offsets manage karta hai. Deployed Spring Kafka version mein in settings ko configure aur test karo. DB transaction commit hone se pehle Kafka offset manually commit mat karna.

Agar reservation insert fail hua, DB transaction `processed_event` insert aur reservation dono rollback kar dega. Listener ko exception propagate karni chahiye taaki configured retry/error handler action le sake. Agar DB commit ho gaya, lekin offset commit hone se pehle process crash ho gaya, toh redelivery same event ID dekhegi aur duplicate DB effect skip ho jayega.

**Important boundary:** Upar wala design stated transaction assumptions ke andar database effect ko effectively-once apply karne mein madad karta hai. Arbitrary external calls ko exactly once nahi banata. Consumer payment provider ya kisi doosri service ko call karta hai, toh remote system ka idempotency key use karo, ya consumer DB mein second outbox likh kar alag delivery process chalao.

### Step 5 — Kafka configuration aur delivery semantics

Direct Kafka producer use karte waqt Kafka ka idempotent producer supported producer-protocol retries se hone wale duplicate records ko protect karta hai. Current Kafka producer docs `enable.idempotence`, `acks=all`, retries aur in-flight requests ke relationship ko explain karte hain: [official producer configuration](https://kafka.apache.org/42/configuration/producer-configs/).

Lekin producer idempotence same business event ko application dobara send kare aur naya producer sequence use ho, toh usko automatically deduplicate nahi karta. Outbox event ID aur consumer deduplication ek alag problem solve karte hain.

Kafka transactions Kafka-to-Kafka processing mein Kafka records aur consumed offsets ko atomically write kar sakte hain. Woh PostgreSQL ya external HTTP service ko automatically transaction mein include nahi karte. Exactly-once claim karne se pehle [Spring Kafka transaction docs](https://docs.spring.io/spring-kafka/reference/kafka/transactions.html) padho.

## 3. Walkthrough, edge cases aur trade-offs — 4 minutes

### Failure walkthrough

| Failure point | Durable state | Recovery |
|---|---|---|
| Order insert fail | Order aur outbox dono commit nahi honge | Failure return; API idempotency policy ke under caller retry kar sakta hai |
| Outbox insert fail | Order transaction bhi rollback | Transaction fix/retry |
| DB commit ke baad Kafka publish se pehle process crash | Order aur outbox row safe hain | CDC apni durable position se resume karke publish karega |
| Consumer ko same event do baar mile | Same event ID redeliver hoti hai | Unique `processed_event.event_id` duplicate DB effect rokta hai |
| Consumer DB transaction fail | Dedup marker aur business effect rollback | Exception propagate; retry ya DLT policy apply |
| Consumer DB commit successful, offset commit fail | DB effect exist karta hai; Kafka redelivery kar sakta hai | Dedup marker duplicate ko harmless banata hai |
| Remote side effect successful, par usko record karne se pehle consumer crash | Remote effect ho chuka ho sakta hai | Remote idempotency ya another outbox chahiye; local dedup alone enough nahi |

### Interview mein solution kaise derive karna hai?

1. **Atomicity boundary identify karo.** Order aur event intent PostgreSQL mein hain, toh ek DB transaction mein commit karo.
2. **Cross-system gap pakdo.** Direct Kafka send DB transaction ke saath atomic nahi hai.
3. **Publication recoverable banao.** Outbox row persist karo aur CDC se publish karo.
4. **Redelivery assume karo.** Stable event ID aur consumer mein unique constraint rakho.
5. **Dedup ko business effect ke saath align karo.** Inbox marker aur DB-side effect ek transaction mein commit hon.
6. **Boundary honestly batao.** External APIs aur Kafka offsets ke liye separate idempotency/recovery handling chahiye.

### Common mistakes

- Sochna ki `@Transactional` JDBC aur Kafka dono ko automatically cover karta hai.
- Kafka producer idempotence ko business-event deduplication samajhna.
- Durable outbox ki jagah sirf after-commit callback use karna.
- DB effect durable hone se pehle consumer offset commit karna.
- Dedup marker ko business update se alag transaction mein likhna.
- Sochna ki inbox table doosri service mein performed side effect ko protect kar degi.
- Event creation timestamp ko universal total order samajhna.
- CDC offsets, snapshots aur recovery samjhe bina outbox rows delete karna.
- Poison messages, schema evolution, DLT monitoring aur safe replay ko ignore karna.

### Production trade-offs aur observability

- **Latency:** HTTP response DB commit ke baad return ho sakta hai, jabki Kafka consumers ne abhi event process nahi kiya. API contract mein asynchronous propagation clear rakho.
- **Storage:** Outbox table retention tak grow karegi. Table size, WAL retention aur connector lag monitor karo.
- **Ordering:** Per-aggregate ordering ke liye order ID Kafka key banao; source/connector aur consumer processing ko required order preserve karna hoga.
- **Schema evolution:** Event envelope version karo; consumers ko compatible naye fields tolerate karne do.
- **Retries aur DLT:** Transient aur permanent errors classify karo; DLT growth par alert aur safe replay tooling rakho.
- **Idempotency retention:** Processed IDs ko maximum replay/redelivery horizon tak retain karo, ya durable business uniqueness constraint use karo.
- **Monitoring:** Outbox-to-Kafka lag, connector health, consumer lag, duplicate suppression count, transaction rollback aur DLT volume track karo.

### Interview-ready answer

> Main direct database-plus-Kafka dual write avoid karunga. Order service order aur versioned outbox event ko ek PostgreSQL transaction mein likhegi. Debezium CDC committed outbox rows Kafka mein publish karega, isliye process crash ke baad publication recover ho sakti hai. Consumer stable event ID aur unique processed-event/inbox record ko apne DB side effect ke saath same transaction mein commit karega. Isse explicit transaction assumptions ke andar reliable at-least-once delivery aur idempotent effects milte hain—not arbitrary external systems mein universal exactly-once execution.

### Follow-up questions with answers

**Q1. Kya Kafka producer idempotence se consumer inbox ki zaroorat khatam ho jaati hai?**  
Nahi. Woh supported producer retry duplicates handle karta hai, lekin application ke same business event ko dobara publish karne ya downstream redelivery ko nahi.

**Q2. After-commit listener sufficient kyun nahi?**  
DB commit ke baad callback successful publish se pehle process crash kar sakta hai. Outbox durable recovery state hai.

**Q3. Consumer remote payment API call karta hai toh?**  
Consumer DB transaction remote API ko atomically commit nahi kar sakti. Provider ko stable idempotency key do, uncertain outcome reconcile karo; zaroorat par another outbox use karo.

**Q4. Kya Kafka exactly-once PostgreSQL writes ko cover karta hai?**  
Apne aap nahi. Kafka transactions supported flows mein Kafka records aur Kafka offsets cover karte hain. DB writes ke liye apni transaction aur dedup/recovery strategy chahiye.

**Q5. Per-order ordering kaise preserve karoge?**  
Order ID ko Kafka key banao, taaki same key ke events same partition mein route hon. Source/connector aur consumer processing bhi required order preserve kare. Parallel processing mein later event ko earlier event se pehle apply ya commit mat karo jab business ordering matter karti ho.

## Authoritative references

- [Debezium Outbox Event Router — stable docs](https://debezium.io/documentation/reference/stable/transformations/outbox-event-router.html)
- [Spring for Apache Kafka — transactions](https://docs.spring.io/spring-kafka/reference/kafka/transactions.html)
- [Apache Kafka 4.2 producer configuration](https://kafka.apache.org/42/configuration/producer-configs/)
- [Apache Kafka documentation](https://kafka.apache.org/documentation/)

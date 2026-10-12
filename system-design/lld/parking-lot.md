# LLD Worked Interview: Parking Lot (Java)

**Prompt:** Design a multi-floor parking lot with multiple entrances/exits, spot types, tickets, and payment. Provide extensible allocation and pricing.

## 1. Clarify and scope
Ask: vehicle types? reservation? pricing? attendants? lost ticket? concurrent entrances? Assume CAR and BIKE, one spot per vehicle, hourly billing, no reservations. Payments are synchronous in the interview example.

## 2. Invariants
1. A spot has at most one active ticket.
2. A ticket transitions **ACTIVE → PAID → CLOSED**; invalid transitions fail.
3. A vehicle cannot obtain a second active ticket.
4. An exit releases its spot only after payment is confirmed.
5. Concurrent entry requests must not claim the same spot.

## 3. Classes and responsibilities
```text
ParkingService
  ├── SpotRepository (find / reserve / release)
  ├── TicketRepository (create / find / update)
  ├── AllocationStrategy (select suitable available spot)
  ├── PricingStrategy (calculate fee)
  └── PaymentGateway (charge)
Entities: ParkingSpot, Ticket, Vehicle
Value objects: spotId, ticketId, timestamps, money
```
**Interview explanation:** "Repositories own persistence, strategies own policies, and ParkingService orchestrates lifecycle transitions."

## 4. Java reference core (in-memory, single JVM)

This compact code demonstrates **atomic spot claiming** with `ConcurrentHashMap.compute` and explicit ticket states. It is **not** a multi-instance transactional implementation.

```java
import java.math.BigDecimal;
import java.time.*;
import java.util.*;
import java.util.concurrent.*;

public final class ParkingLot {
    enum VehicleType { CAR, BIKE }
    enum State { ACTIVE, PAID, CLOSED }
    record Vehicle(String plate, VehicleType type) {}
    record Spot(String id, VehicleType type) {}
    record Ticket(String id, String plate, String spotId, Instant entered, State state) {}

    private final ConcurrentMap<String, Spot> available = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, Spot> occupied = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, Ticket> tickets = new ConcurrentHashMap<>();
    private final Set<String> activePlates = ConcurrentHashMap.newKeySet();

    public ParkingLot(Collection<Spot> spots) {
        for (Spot spot : spots) {
            if (available.putIfAbsent(spot.id(), spot) != null)
                throw new IllegalArgumentException("Duplicate spot");
        }
    }

    // Synchronization makes the in-memory entry/exit transitions atomic per instance.
    public synchronized Ticket enter(Vehicle vehicle, Instant now) {
        Objects.requireNonNull(vehicle);
        if (!activePlates.add(vehicle.plate()))
            throw new IllegalStateException("Vehicle already parked");
        try {
            Spot selected = available.values().stream()
                .filter(s -> s.type() == vehicle.type())
                .min(Comparator.comparing(Spot::id))
                .orElseThrow(() -> new IllegalStateException("Lot full"));
            available.remove(selected.id());
            occupied.put(selected.id(), selected);
            Ticket ticket = new Ticket(UUID.randomUUID().toString(),
                vehicle.plate(), selected.id(), now, State.ACTIVE);
            tickets.put(ticket.id(), ticket);
            return ticket;
        } catch (RuntimeException e) {
            activePlates.remove(vehicle.plate());
            throw e;
        }
    }

    public synchronized BigDecimal exit(String ticketId, Instant now) {
        Ticket t = Optional.ofNullable(tickets.get(ticketId))
            .orElseThrow(() -> new IllegalArgumentException("Unknown ticket"));
        if (t.state() != State.ACTIVE)
            throw new IllegalStateException("Ticket not active");
        long minutes = Math.max(1, Duration.between(t.entered(), now).toMinutes());
        long hours = (minutes + 59) / 60; // ceil to started hour
        BigDecimal fee = BigDecimal.valueOf(hours * 20L);
        // Interview simplification: payment is assumed successful here.
        tickets.put(ticketId, new Ticket(t.id(), t.plate(), t.spotId(), t.entered(), State.CLOSED));
        Spot spot = occupied.remove(t.spotId());
        available.put(spot.id(), spot);
        activePlates.remove(t.plate());
        return fee;
    }

    public static void main(String[] args) {
        ParkingLot lot = new ParkingLot(List.of(new Spot("F1-A1", VehicleType.CAR)));
        Instant start = Instant.parse("2026-10-13T08:00:00Z");
        Ticket ticket = lot.enter(new Vehicle("KA01AB1234", VehicleType.CAR), start);
        System.out.println(lot.exit(ticket.id(), start.plusSeconds(3601))); // 40
    }
}
```

**Important:** This sample intentionally simplifies payment and lifecycle by closing after an assumed successful charge. A real implementation must model ACTIVE → PAID → CLOSED explicitly, use a payment idempotency key, and reconcile uncertain gateway outcomes. For distributed deployments, the synchronized keyword is insufficient; enforce spot ownership with database transactions/conditional writes and uniqueness constraints.

## 5. Production implementation approach
- `ParkingSpot(id, floor, type, status, version)`
- `Ticket(id, spot_id, vehicle_plate, entry_at, exit_at, state)`
- `Payment(id, ticket_id, amount_minor, status, idempotency_key)`
- DB unique partial index for active spot assignments and active vehicle plates (or equivalent transactional constraints).
- Entry: select candidate → conditional update `AVAILABLE → OCCUPIED` → create ticket atomically.
- Exit: calculate fee → initiate idempotent payment → on confirmed payment, transactionally mark ticket CLOSED and free spot.
- For payment timeouts, keep ticket in pending state and reconcile; never assume success.
- Replace fixed price with `PricingStrategy`, spot choice with `AllocationStrategy`.

## 6. Tests to explain aloud
- CAR cannot occupy BIKE spot; full lot rejects entry.
- Same plate cannot enter twice.
- Two concurrent entrances cannot allocate one spot.
- Repeated exit is rejected or returns prior receipt by idempotency policy.
- Payment declined → spot stays occupied.
- Lost ticket, clock skew, and duplicate payment callbacks are handled explicitly.

## 7. Interview closing answer (60 seconds)
"I separated allocation, pricing, ticket lifecycle, and persistence. The core invariant is that a spot cannot be assigned twice. In the single-process example I synchronize entry and exit, but in production I would use conditional updates and a database transaction because there may be many service instances. Payment must be idempotent, and I release a spot only after confirmation. The design can add EV spots, floors, or alternate pricing through strategies without rewriting ticket lifecycle."

## Follow-ups
How would you handle 100 entry gates? Reservations? Dynamic pricing? Lost tickets? Multiple cities and tenants?

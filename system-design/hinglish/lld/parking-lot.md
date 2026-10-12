# Parking Lot LLD — Hinglish

## Problem
Multi-floor parking lot; multiple entrances/exits; vehicle types, ticket, payment, spot release.

## Pehle invariants define karo
1. Ek spot par ek hi active ticket.
2. Same vehicle ka duplicate active ticket nahi.
3. Payment confirm hone ke baad hi spot release.
4. Ticket state transition valid ho: ACTIVE → PAID → CLOSED.

## Classes kaise derive karoge?
- `Vehicle`, `ParkingSpot`, `Ticket`: domain entities.
- `ParkingService`: entry/exit orchestrate.
- `SpotRepository`, `TicketRepository`: DB access.
- `AllocationStrategy`: nearest/EV/accessible selection.
- `PricingStrategy`: hourly/dynamic pricing.
- `PaymentGateway`: external payment.

**Reasoning:** Agar allocation algorithm change ho, payment code change nahi hona chahiye. Isliye Strategy interface.

## Entry flow
1. Vehicle validate + existing active ticket check.
2. Suitable available spot choose.
3. DB transaction mein conditional update se spot reserve karo aur ticket insert karo.
4. Success par ticket return; collision par retry another spot.

## Exit flow
1. Ticket read; valid state verify.
2. Fee calculate.
3. Payment initiate with idempotency key.
4. Gateway result unknown? Pending/reconcile; release mat karo.
5. Confirmed payment par transactionally close ticket + free spot.

## Concurrency insight
`synchronized` only same JVM. Production mein multiple instances hain, toh DB unique constraint + transaction zaroori hai. Double allocation is a correctness bug, sirf performance issue nahi.

## Interview English summary
"The design separates ticket lifecycle, allocation, pricing, and payment. I enforce the one-active-ticket-per-spot invariant transactionally. Payments use idempotency keys and unknown outcomes are reconciled before releasing a spot. Strategies allow new allocation and pricing policies without rewriting the core workflow."

**Deep-dive English reference:** [Parking Lot](../../lld/parking-lot.md)

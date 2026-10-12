# LLD Interview Playbook — Hinglish

## Pehle behavior, phir classes
**Bolo:** "I'll identify core operations and invariants before modeling classes."

LLD ka matlab har entity ke liye 10 interfaces banana nahi. **Responsibilities clear rakhna** aur change boundaries identify karna important hai.

## 45-minute plan
- **0–5 min:** Scope: kaunse operations, failure cases, concurrency?
- **5–10 min:** Invariants: kya cheez kabhi violate nahi honi chahiye?
- **10–20 min:** Entities, value objects, services, repositories, strategies.
- **20–35 min:** Happy-path Java implementation; phir invalid states aur errors.
- **35–42 min:** Tests, concurrency, extensibility.
- **42–45 min:** Design recap.

## Example: Parking lot
- **Entity:** `Ticket`, `ParkingSpot`, `Vehicle`.
- **Service:** `ParkingService` lifecycle orchestrate karega.
- **Repository:** Persistence ko handle karega.
- **Strategy:** `AllocationStrategy` decide karegi nearest/EV/accessible spot.
- **Invariant:** Ek spot par ek hi active ticket.

### Important concurrency concept
`synchronized` ek JVM ke threads ko coordinate karta hai. Agar 3 Spring Boot instances chal rahe hain, teen alag JVM locks honge. Isliye multi-instance environment mein DB conditional update/unique constraint + transaction chahiye.

### Design pattern ko force mat karo
Factory tab use karo jab object creation genuinely complex/variable ho. Strategy tab useful hai jab algorithm swap hota ho. SOLID ka objective maintainability hai, pattern-count nahi.

## Interview mein English answer
"I separated lifecycle orchestration from allocation and pricing policies. The primary invariant is that a spot cannot be allocated twice. In a distributed deployment, I'd enforce that invariant with database constraints and transactional updates rather than JVM locks."

## Follow-ups
Payment timeout? Duplicate callback? Two concurrent entries? Lost ticket? Dynamic pricing? Unit tests kaise likhoge?

[Detailed English playbook](../../interview-framework/lld-interview-playbook.md)

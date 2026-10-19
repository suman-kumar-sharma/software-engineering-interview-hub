# Interview Preparation — 2026-10-19 (Hinglish Classroom)

Teacher-style explanation with practical examples, Java 17+, HLD and LLD.

**Aaj ka focus:** Product-company SDE2 / Senior backend interview. Yeh original practice questions hain, kisi named company ke leaked questions nahi. Hum real example se start karenge, phir reason, approach, dry run, edge cases aur interview-ready English answer.

## Q1 — Java singleton mein volatile kyun?

**Socho** do threads ek hi time par singleton object maang rahe hain. Agar tum sirf double null-check likh do aur reference safely publish na ho, doosre thread ko initialized fields ki visibility guarantee nahi milti. **Ab karna kya hai?** Reference volatile rakho, aur object create karne ka critical section synchronized rakho.

~~~java
final class Registry {
 private static volatile Registry instance;
 private Registry() {}
 static Registry get() {
  Registry x=instance;
  if(x==null) synchronized(Registry.class) {
   x=instance;
   if(x==null) instance=x=new Registry();
  }
  return x;
 }
}
~~~

**Dry run:** A aur B dono null dekhte hain. A lock leke instance banata hai; B lock milne par dobara check karta hai aur existing object return karta hai. **Complexity:** O(1) per call. **Twist:** volatile se count++ atomic nahi hota. **Interview mein:** “Volatile ensures safe publication; synchronization prevents duplicate construction.”

## Q2 — Kafka consumer revoke hone ke baad bhi DB update kar de?

**Maan lo** Kafka partition pehle consumer A ke paas tha. A slow ho gaya, partition B ko mil gaya. B ne latest event apply kar diya, lekin A purana event complete karke database overwrite kar de—ye zombie write hai.

**Solution derive karo:** Kafka group epoch offset commit ko fence kar sakta hai, par external database automatically safe nahi hota. DB row mein monotonically increasing version rakho; incoming version tabhi apply karo jab latest ho. Stable eventId se duplicates reject karo.

~~~sql
UPDATE projections SET value=:v, version=:ver
WHERE entity_id=:id AND version < :ver;
~~~

**Dry run:** B version 13 write karta hai; A version 12 try karega toh rowCount=0. **Complexity:** indexed conditional update. **Twist:** new row insert race aur missing events handle karo. **Interview mein:** “I fence stale external writes independently of Kafka's group ownership.”


## Q3 — PostgreSQL mein write skew kya hota hai?

**Socho** hospital rule hai: kam se kam ek doctor on-call rahega. A dekhta hai B duty par hai, B dekhta hai A duty par hai. Dono apni-apni row off kar dete hain. Alag rows update hui, phir bhi hospital mein koi duty par nahi! **Ab problem samjhi?** Rule poori set of rows par hai, ek row par nahi.

**Approach:** Shared guard row lock karo ya SERIALIZABLE transaction use karo. PostgreSQL SQLSTATE 40001 de toh **poora transaction** dobara chalao, sirf failed UPDATE nahi.

~~~sql
BEGIN ISOLATION LEVEL SERIALIZABLE;
SELECT count(*) FROM doctors WHERE on_call;
-- count > 1 ho tab apni row off karo
COMMIT;
~~~

**Dry run:** Dono count=2 dekhte hain, serializable conflict mein at least ek abort hoga. **Twist:** transaction ke andar external payment/email mat karo jo retry par duplicate ho. **Interview:** “I protect predicate-level invariants using serialization or a shared lock.”

## Q4 — JWT signing key rotate karte waqt users logout kyun ho jaate hain?

**Maan lo** K1 se issue hua token 11 baje expire hoga. Tumne 10:15 par K2 activate karke K1 public key delete kar di. Ab valid K1 token bhi verify nahi hoga. **Solution:** Pehle JWKS mein K2 publish karo, verifier caches refresh hone do, phir K2 se sign karo. K1 ko old tokens expire hone aur clock-skew buffer tak rakho.

**Security checks:** issuer, audience, signature, allowed algorithm, expiry aur tenant permissions verify karo. Unknown kid par trusted JWKS ko controlled refresh karo; token ke header se arbitrary key URL trust mat karo.

**Dry run:** Old K1 tokens expire hone tak chalte hain, naye K2 tokens bhi valid. **Twist:** emergency compromised key rotation planned rotation se different hai. **Interview:** “I overlap verification keys and rotate signing only after propagation.”


## Q5 — JVM memory badh rahi hai, CPU normal hai

**Socho** deploy ke baad API p99 200ms se 2s ho gayi, lekin CPU 30% hai. CPU badhana sahi diagnosis nahi. **Kaise investigate karoge?** JFR se allocation profile, GC pause aur live heap compare karo. Har GC ke baad live set badhta rahe toh retained-object leak; temporary allocations badhein toh churn.

**Example:** JSON serializer har request mein 10 extra copies bana raha hai. Allocation rate badhega aur GC p99 ko hurt karega. **Approach:** baseline compare → JFR allocation stack → heap retention → targeted fix → load test. **Twist:** heap dump sensitive data rakh sakta hai; secure storage. **Complexity:** diagnostic overhead sampling par depend. **Interview:** “I distinguish retained heap growth from allocation churn before tuning GC.”


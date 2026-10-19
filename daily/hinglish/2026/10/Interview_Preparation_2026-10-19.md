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


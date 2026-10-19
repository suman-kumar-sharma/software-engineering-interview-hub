# LFU cache design

**Socho** capacity 2 ki cache hai. Sabse kam access wali key ko replace karna hai. Equal frequency ho toh jo key sabse pehle access hui thi, usko choose karna hai. Isliye frequency aur recency dono track karni hongi.

## Data structure kaise choose kiya?

Ek HashMap key se node deta hai. Dusra HashMap frequency se LinkedHashSet of keys deta hai. LinkedHashSet insertion order maintain karta hai, jo same-frequency LRU tie-break ke kaam aata hai. minFrequency minimum nonempty bucket batata hai. **Ab concurrency ka twist:** get() aur put() multiple maps update karte hain; sirf ConcurrentHashMap use karna enough nahi. Ek synchronized lock se poora operation atomic rakho.

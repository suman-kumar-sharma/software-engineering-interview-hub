# Thread-Safe LRU Cache LLD — Hinglish

## Problem
Fixed-capacity cache: `get`, `put`, least-recently-used eviction; expected O(1).

## Data structure kaise choose kiya?
- `HashMap<K, Node>`: key se node O(1) average lookup.
- Doubly linked list: Node ko O(1) detach aur front par move.
- Head = most recently used; tail = least recently used.

**Why not array?** Element ko front par move karna O(n) ho jayega.

## Example
Capacity 2:
1. put(1,A) → [1]
2. put(2,B) → [2,1]
3. get(1) → [1,2]
4. put(3,C) → [3,1], key 2 evicted.

## Thread safety
`get` bhi list modify karta hai, kyunki recency change hoti hai. Isliye sirf `put` lock karna enough nahi. Map + list mutations **ek lock ke andar atomic** rakho. Single `ReentrantLock` easiest correct baseline hai.

## Complexity
`get`: expected O(1); `put`: expected O(1); memory O(capacity). Lock contention par throughput drop ho sakta hai.

## Edge cases
Capacity 1, existing key update, null policy, repeated accesses, simultaneous puts, concurrent eviction. Production cache mein TTL, weight-based eviction, metrics aur stampede protection additional requirements hain.

## Interview English answer
"I'd combine a hash map with a doubly linked list to achieve expected constant-time lookups and recency updates. Because reads also mutate recency, I would guard both structures with one lock for correctness. If profiling reveals contention, I'd evaluate segmentation or a mature caching library rather than prematurely complicating synchronization."

**Runnable Java 17 implementation:** [LRU Cache](../../lld/lru-cache.md)

# LLD Case Study: Thread-Safe LRU Cache in Java

**Prompt:** Implement an in-memory bounded LRU cache with `get`, `put`, eviction, and thread safety. Target O(1) average-time operations.

## 1. Clarify
Ask whether TTL, null values, weighted entries, persistence, and concurrency are required. Assume positive fixed capacity, no TTL, no null keys/values, and one JVM.

## 2. Core invariants
- At most `capacity` entries.
- Every map entry has exactly one node in a doubly linked list.
- Most recently used node is at the front; least recently used at the back.
- Reads of existing keys update recency.
- A lock protects map/list mutation as one operation.

## 3. Data structures
HashMap gives expected O(1) lookup; doubly linked list gives O(1) detach and prepend. Sentinel head/tail nodes remove edge-case branches. Use one `ReentrantLock` for straightforward correctness.

## 4. Runnable Java 17 implementation
```java
import java.util.*;
import java.util.concurrent.locks.ReentrantLock;

public final class LruCache<K, V> {
    private final int capacity;
    private final Map<K, Node<K, V>> map = new HashMap<>();
    private final ReentrantLock lock = new ReentrantLock();
    private final Node<K, V> head = new Node<>(null, null);
    private final Node<K, V> tail = new Node<>(null, null);

    private static final class Node<K, V> {
        K key;
        V value;
        Node<K, V> prev, next;
        Node(K key, V value) { this.key = key; this.value = value; }
    }

    public LruCache(int capacity) {
        if (capacity <= 0) throw new IllegalArgumentException("capacity must be positive");
        this.capacity = capacity;
        head.next = tail;
        tail.prev = head;
    }

    public Optional<V> get(K key) {
        Objects.requireNonNull(key);
        lock.lock();
        try {
            Node<K, V> node = map.get(key);
            if (node == null) return Optional.empty();
            moveToFront(node);
            return Optional.of(node.value);
        } finally { lock.unlock(); }
    }

    public void put(K key, V value) {
        Objects.requireNonNull(key);
        Objects.requireNonNull(value);
        lock.lock();
        try {
            Node<K, V> existing = map.get(key);
            if (existing != null) {
                existing.value = value;
                moveToFront(existing);
                return;
            }
            Node<K, V> node = new Node<>(key, value);
            map.put(key, node);
            insertAfterHead(node);
            if (map.size() > capacity) {
                Node<K, V> lru = tail.prev;
                unlink(lru);
                map.remove(lru.key);
            }
        } finally { lock.unlock(); }
    }

    public int size() {
        lock.lock();
        try { return map.size(); }
        finally { lock.unlock(); }
    }

    private void moveToFront(Node<K, V> node) {
        unlink(node);
        insertAfterHead(node);
    }

    private void unlink(Node<K, V> node) {
        node.prev.next = node.next;
        node.next.prev = node.prev;
    }

    private void insertAfterHead(Node<K, V> node) {
        node.prev = head;
        node.next = head.next;
        head.next.prev = node;
        head.next = node;
    }

    public static void main(String[] args) {
        LruCache<Integer, String> cache = new LruCache<>(2);
        cache.put(1, "a");
        cache.put(2, "b");
        cache.get(1);
        cache.put(3, "c");
        assert cache.get(2).isEmpty();
        assert cache.get(1).orElseThrow().equals("a");
    }
}
```

## 5. Complexity
`get` and `put`: expected O(1) average, O(n) pathological hash collisions; space O(capacity). A single lock serializes operations and can limit throughput under contention.

## 6. Why this design?
- HashMap + linked list supports true recency-based eviction.
- `LinkedHashMap(accessOrder=true)` is a shorter alternative but still needs synchronization around access and mutation.
- Single lock is easier to prove correct than separate locks for map and list.
- This is an **in-process cache**, not a distributed cache or durable datastore.

## 7. Test plan
- Eviction after capacity reached.
- `get` refreshes recency; `put` existing key refreshes recency.
- Capacity 1; invalid capacity; null key/value.
- Repeated updates don't grow size.
- Multi-threaded operations preserve `size <= capacity` and no exceptions.
- Contention benchmark to determine whether segmentation is worth complexity.

## 8. Spoken interview wrap-up
"I used a hash map for lookup and a doubly linked list for recency. Both operations are expected O(1). Since even get changes the recency list, a single lock guards the combined map/list state. I'd start with this correct implementation and optimize lock contention only if profiling shows it matters."

## Follow-ups
How would you add TTL without scanning all entries? How do you handle variable-sized values? How would you avoid cache stampedes? How would you adapt for a distributed cache?

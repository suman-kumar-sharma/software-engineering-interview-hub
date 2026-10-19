# LLD — Thread-Safe LFU Cache (Java 17)

API get, put and size. Fixed capacity; evict least frequently used, tie-break by least recently used. In-process and thread-safe.

## Data structures and invariant

entries: HashMap<K,Node>; buckets: HashMap<frequency,LinkedHashSet<K>>; minFrequency tracks minimum nonempty bucket. LinkedHashSet preserves LRU order inside each frequency. One synchronized monitor guards all structures, making compound get/put atomic. Null values rejected, capacity zero accepted. Updating existing key increases its frequency.

## Complete Java 17 implementation

~~~java
import java.util.*;
public class LfuDemo {
 static final class LfuCache<K,V> {
  private final int capacity;
  private final Map<K,Node<K,V>> entries=new HashMap<>();
  private final Map<Integer,LinkedHashSet<K>> buckets=new HashMap<>();
  private int minFrequency=0;
  static final class Node<K,V> {
   final K key; V value; int frequency=1;
   Node(K k,V v){key=k;value=v;}
  }
  LfuCache(int c){if(c<0)throw new IllegalArgumentException();capacity=c;}
  synchronized Optional<V> get(K k){
   Node<K,V> n=entries.get(k);
   if(n==null)return Optional.empty();
   promote(n);return Optional.of(n.value);
  }
  synchronized void put(K k,V v){
   Objects.requireNonNull(k);Objects.requireNonNull(v);
   if(capacity==0)return;
   Node<K,V> n=entries.get(k);
   if(n!=null){n.value=v;promote(n);return;}
   if(entries.size()==capacity){
    LinkedHashSet<K> oldest=buckets.get(minFrequency);
    K victim=oldest.iterator().next();
    oldest.remove(victim);
    if(oldest.isEmpty())buckets.remove(minFrequency);

    entries.computeIfPresent(victim,(key,node)->null);
   }
   Node<K,V> fresh=new Node<>(k,v);
   entries.put(k,fresh);
   buckets.computeIfAbsent(1,x->new LinkedHashSet<>()).add(k);
   minFrequency=1;
  }
  private void promote(Node<K,V> n){
   int old=n.frequency;
   LinkedHashSet<K> group=buckets.get(old);
   group.remove(n.key);
   if(group.isEmpty()){
    buckets.remove(old);
    if(minFrequency==old)minFrequency=old+1;
   }
   n.frequency++;
   buckets.computeIfAbsent(n.frequency,x->new LinkedHashSet<>()).add(n.key);
  }
  synchronized int size(){return entries.size();}
 }
 public static void main(String[] args){
  LfuCache<Integer,String> c=new LfuCache<>(2);
  c.put(1,"A");c.put(2,"B");c.get(1);c.put(3,"C");
  System.out.println(c.get(2)); // Optional.empty
  System.out.println(c.get(1)); // Optional[A]
  System.out.println(c.get(3)); // Optional[C]
 }
}
~~~

**Validation:** This Java 17-compatible design was compiled and executed in a local smoke test with expected output: Optional.empty, Optional[A], Optional[C]. Complexity: expected O(1) get/put, O(capacity) memory; a global synchronized lock serializes operations.

## Edge cases, concurrency and interview defense

Dry run capacity=2: put(1,A), put(2,B) -> frequency-1 order [1,2]. get(1) moves 1 to frequency 2. put(3,C) evicts key 2, resets minFrequency=1. Test capacity 0/1, ties, updates, null rejection, 32 concurrent callers and a sequential reference model. Frequency int can overflow after extreme use; production should use long or aging. Global monitor is simple but limits throughput; striped locking makes cross-frequency eviction much harder. No TTL, persistence or distributed coherence. **Spoken answer:** “A key map plus ordered frequency buckets provides expected constant-time LFU eviction, and a single monitor protects all cross-map invariants.”

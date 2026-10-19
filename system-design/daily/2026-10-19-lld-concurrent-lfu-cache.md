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

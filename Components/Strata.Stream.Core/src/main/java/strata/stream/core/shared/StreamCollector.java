//////////////////////////////////////////////////////////////////////////////
// StreamCollector.java
//////////////////////////////////////////////////////////////////////////////

package strata.stream.core.shared;

import strata.foundation.core.collection.*;

import java.util.Map.Entry;
import java.util.stream.Collector;

public
class StreamCollector
{
    private StreamCollector() {}

    public static <T,C extends ICollection<T>> Collector<T,?,C>
    toCollection(Class<C> collectionType)
    {
        return
            Collector.of(
                getSupplier(collectionType),
                ICollection::add,
                (left,right) -> mergeCollections(left,right),
                output -> output);
    }

    public static <T> Collector<T,?,IList<T>>
    toList()
    {
        return
            Collector.of(
                () -> new SerializableList<T>(),
                SerializableList::add,
                (left,right) -> mergeCollections(left,right),
                output -> output);
    }

    public static <T> Collector<T,?,ISet<T>>
    toSet()
    {
        return
            Collector.of(
                () -> new SerializableSet<T>(),
                SerializableSet::add,
                (left,right) -> mergeCollections(left,right),
                output -> output);
    }


    public static <T extends Comparable<T>> Collector<T,?,IMultiSet<T>>
    toMultiSet()
    {
        return
            Collector.of(
                () -> new MultiSet<T>(),
                MultiSet::add,
                (left,right) -> mergeMultiSets(left,right),
                output -> output);
    }

    public static <K,V> Collector<Entry<K,V>,?,IMap<K,V>>
    toMap()
    {
        return
            Collector.of(
                () -> new SerializableMap<K,V>(),
                (map,entry) -> putEntry(map,entry),
                (left,right) -> mergeMaps(left,right),
                output -> output);
    }

    public static <K,V> Collector<Entry<K,V>,?,IMultiMap<K,V>>
    toListValuedMultiMap()
    {
        return
            Collector.of(
                () -> new ListValuedMultiMap<K,V>(),
                (map,entry) -> putEntry(map,entry),
                (left,right) -> mergeMultiMaps(left,right),
                output -> output);
    }

    public static <K,V> Collector<Entry<K,V>,?,IMultiMap<K,V>>
    toSetValuedMultiMap()
    {
        return
            Collector.of(
                () -> new SetValuedMultiMap<K,V>(),
                (map,entry) -> putEntry(map,entry),
                (left,right) -> mergeMultiMaps(left,right),
                output -> output);
    }

    @SuppressWarnings("unchecked")
    private static <T,C extends ICollection<T>> ISupplier<C>
    getSupplier(Class<C> collectionType)
    {
        switch (collectionType.getSimpleName())
        {
            case "SerializableList":
                return () -> (C)new SerializableList<T>();

            case "SerializableSet":
                return () -> (C)new SerializableSet<T>();

            default:
                throw
                    new IllegalArgumentException(
                        "Unsupported collection type: " +
                            collectionType.getName());
        }
    }

    private static <T,C extends ICollection<T>> C
    mergeCollections(C left,C right)
    {
        left.addAll(right);
        return left;
    }

    private static <T extends Comparable<T>,C extends IMultiSet<T>> C
    mergeMultiSets(C left,C right)
    {
        left.addAll(right);
        return left;
    }

    private static <K,V,C extends IMap<K,V>> C
    mergeMaps(C left,C right)
    {
        left.putAll(right);
        return left;
    }


    private static <K,V,C extends IMultiMap<K,V>> C
    mergeMultiMaps(C left,C right)
    {
        left.putAll(right);
        return left;
    }

    private static <K,V> V
    putEntry(IMap<K,V> map,Entry<K,V> entry)
    {
        if (map.containsKey(entry.getKey()))
            throw
                new IllegalArgumentException(
                    "Duplicate key: " + entry.getKey().toString());

        return map.put(entry.getKey(),entry.getValue());
    }

    private static <K,V> IMultiMap<K,V>
    putEntry(IMultiMap<K,V> map,Entry<K,V> entry)
    {
        return map.put(entry.getKey(),entry.getValue());
    }

}

//////////////////////////////////////////////////////////////////////////////

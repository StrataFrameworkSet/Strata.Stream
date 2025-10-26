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

    public static <T> Collector<T,?,IList<T>>
    toList()
    {
        return
            Collector.of(
                () -> new SerializableList<T>(),
                SerializableList::add,
                (left,right) ->
                    {
                    left.addAll(right);
                    return left;
                    },
                output -> output);
    }

    public static <T> Collector<T,?,ISet<T>>
    toSet()
    {
        return
            Collector.of(
                () -> new SerializableSet<T>(),
                SerializableSet::add,
                (left,right) -> {
                    left.addAll(right);
                    return left;
                },
                output -> output);
    }

    public static <T extends Comparable<T>> Collector<T,?,IMultiSet<T>>
    toMultiSet()
    {
        return
            Collector.of(
                () -> new MultiSet<T>(),
                MultiSet::add,
                (left,right) -> {
                    left.addAll(right);
                    return left;
                },
                output -> output);
    }

    public static <K,V> Collector<Entry<K,V>,?,IMap<K,V>>
    toMap()
    {
        return
            Collector.of(
                () -> new SerializableMap<K,V>(),
                (map,entry) -> map.put(entry.getKey(),entry.getValue()),
                (left,right) -> {
                    left.putAll(right);
                    return left;
                },
                output -> output);
    }

    public static <K,V> Collector<Entry<K,V>,?,IMultiMap<K,V>>
    toListValuedMultiMap()
    {
        return
            Collector.of(
                () -> new ListValuedMultiMap<K,V>(),
                (map,entry) -> map.put(entry.getKey(),entry.getValue()),
                (left,right) ->
                    {
                    left.putAll(right);
                    return left;
                    },
                output -> output);
    }

    public static <K,V> Collector<Entry<K,V>,?,IMultiMap<K,V>>
    toSetValuedMultiMap()
    {
        return
            Collector.of(
                () -> new SetValuedMultiMap<K,V>(),
                (map,entry) -> map.put(entry.getKey(),entry.getValue()),
                (left,right) ->
                    {
                    left.putAll(right);
                    return left;
                    },
                output -> output);
    }
}

//////////////////////////////////////////////////////////////////////////////

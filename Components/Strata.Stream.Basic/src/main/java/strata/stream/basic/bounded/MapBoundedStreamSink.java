//////////////////////////////////////////////////////////////////////////////
// MapBoundedStreamSink.java
//////////////////////////////////////////////////////////////////////////////

package strata.stream.basic.bounded;

import strata.stream.core.bounded.AbstractBoundedStreamSink;
import strata.stream.core.shared.IKeySelector;

import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

public
class MapBoundedStreamSink<K,T>
    extends AbstractBoundedStreamSink<T>
{
    private final Map<K,T>          store;
    private final IKeySelector<K,T> selector;

    public
    MapBoundedStreamSink(IKeySelector<K,T> sel)
    {
        this(new TreeMap<>(),sel);
    }

    public
    MapBoundedStreamSink(Map<K,T> stor,IKeySelector<K,T> sel)
    {
        store    = stor;
        selector = sel;
    }

    public MapBoundedStreamSink<K,T>
    clearStore()
    {
        store.clear();
        return this;
    }

    public Map<K,T>
    getStore() { return store; }

    @Override
    protected void
    process(T element)
    {
        store.put(selector.getKey(element),element);
    }
}

//////////////////////////////////////////////////////////////////////////////

//////////////////////////////////////////////////////////////////////////////
// MapUnboundedStreamSink.java
//////////////////////////////////////////////////////////////////////////////

package strata.stream.basic.unbounded;

import strata.stream.core.shared.IKeySelector;
import strata.stream.core.unbounded.AbstractUnboundedStreamSink;

import java.util.Map;
import java.util.TreeMap;

public
class MapUnboundedStreamSink<K,T>
    extends AbstractUnboundedStreamSink<T>
{
    private final Map<K,T>          store;
    private final IKeySelector<K,T> selector;

    public
    MapUnboundedStreamSink(IKeySelector<K,T> sel)
    {
        this(new TreeMap<>(),sel);
    }

    public
    MapUnboundedStreamSink(Map<K,T> stor,IKeySelector<K,T> sel)
    {
        store    = stor;
        selector = sel;
    }

    public void
    close() {}

    public MapUnboundedStreamSink<K,T>
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
        System.out.println("process(" + element + ")");
        store.put(selector.getKey(element),element);
    }
}

//////////////////////////////////////////////////////////////////////////////

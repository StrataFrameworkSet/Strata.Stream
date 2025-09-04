//////////////////////////////////////////////////////////////////////////////
// CollectionStreamSource.java
//////////////////////////////////////////////////////////////////////////////

package strata.stream.basic.bounded;

import strata.stream.core.bounded.AbstractBoundedStreamSource;
import strata.stream.core.bounded.IBoundedStream;

import java.util.Map;
import java.util.Map.Entry;

public
class MapBoundedStreamSource<K,T>
    extends AbstractBoundedStreamSource<Entry<K,T>>
{
    private final Map<K,T>                   source;
    private final IBoundedStream<Entry<K,T>> stream;

    public
    MapBoundedStreamSource(Map<K,T> src)
    {
        source = src;
        stream = BasicBoundedStream.of(source.entrySet().stream());
    }

    @Override
    protected IBoundedStream<Entry<K,T>>
    getStream()
    {
        return stream;
    }

    @Override
    public boolean
    hasSource()
    {
        return source != null;
    }

    public Map<K,T>
    getSource() { return source; }
}

//////////////////////////////////////////////////////////////////////////////

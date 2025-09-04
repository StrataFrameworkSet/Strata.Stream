//////////////////////////////////////////////////////////////////////////////
// CollectionStreamSource.java
//////////////////////////////////////////////////////////////////////////////

package strata.stream.basic.bounded;

import strata.stream.core.bounded.AbstractBoundedStreamSource;
import strata.stream.core.bounded.IBoundedStream;

import java.util.Collection;

public
class CollectionBoundedStreamSource<T>
    extends AbstractBoundedStreamSource<T>
{
    private final Collection<T>     source;
    private final IBoundedStream<T> stream;

    public
    CollectionBoundedStreamSource(Collection<T> src)
    {
        source = src;
        stream = BasicBoundedStream.of(source.stream());
    }

    @Override
    protected IBoundedStream<T>
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

    public Collection<T>
    getSource() { return source; }
}

//////////////////////////////////////////////////////////////////////////////

//////////////////////////////////////////////////////////////////////////////
// CollectionStreamSource.java
//////////////////////////////////////////////////////////////////////////////

package strata.stream.basic.bounded;

import strata.stream.core.bounded.AbstractBoundedStreamSource;
import strata.stream.core.bounded.IBoundedStream;

import java.util.List;

public
class ListBoundedStreamSource<T>
    extends AbstractBoundedStreamSource<T>
{
    private final List<T>           source;
    private final IBoundedStream<T> stream;

    public
    ListBoundedStreamSource(List<T> src)
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

    public List<T>
    getSource() { return source; }
}

//////////////////////////////////////////////////////////////////////////////

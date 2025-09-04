//////////////////////////////////////////////////////////////////////////////
// CollectionStreamSource.java
//////////////////////////////////////////////////////////////////////////////

package strata.stream.basic.bounded;

import strata.stream.core.bounded.AbstractBoundedStreamSource;
import strata.stream.core.bounded.IBoundedStream;

import java.util.Set;

public
class SetBoundedStreamSource<T>
    extends AbstractBoundedStreamSource<T>
{
    private final Set<T>            source;
    private final IBoundedStream<T> stream;

    public
    SetBoundedStreamSource(Set<T> src)
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

    public Set<T>
    getSource() { return source; }
}

//////////////////////////////////////////////////////////////////////////////

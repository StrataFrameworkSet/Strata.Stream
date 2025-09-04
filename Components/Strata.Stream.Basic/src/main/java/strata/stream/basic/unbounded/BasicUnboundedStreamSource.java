/// ///////////////////////////////////////////////////////////////////////////
// BasicUnboundedStreamSource.java
//////////////////////////////////////////////////////////////////////////////

package strata.stream.basic.unbounded;

import strata.foundation.core.concurrent.IBlockingQueue;
import strata.stream.core.unbounded.AbstractUnboundedStreamSource;
import strata.stream.core.unbounded.IUnboundedStream;

public
class BasicUnboundedStreamSource<T>
    extends AbstractUnboundedStreamSource<T>
{
    private IBlockingQueue<T>   source;
    private IUnboundedStream<T> stream;

    public
    BasicUnboundedStreamSource(IBlockingQueue<T> source)
    {
        this.source = source;
        //this.stream = BasicUnboundedStream.of(source);
    }

    @Override
    protected IUnboundedStream<T>
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

    public IBlockingQueue<T>
    getSource()
    {
        return source;
    }
}

//////////////////////////////////////////////////////////////////////////////

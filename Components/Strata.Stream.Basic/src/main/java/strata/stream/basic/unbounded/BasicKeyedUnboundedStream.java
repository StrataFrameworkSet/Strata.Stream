//////////////////////////////////////////////////////////////////////////////
// BasicKeyedUnboundedStream.java
//////////////////////////////////////////////////////////////////////////////

package strata.stream.basic.unbounded;

import strata.stream.core.shared.*;
import strata.stream.core.unbounded.*;

public
class BasicKeyedUnboundedStream<K,T>
    implements IKeyedUnboundedStream<K,T>
{
    @Override
    public <K> IKeyedUnboundedStream<K,T>
    keyBy(IKeySelector<K,T> selector)
    {
        return null;
    }

    @Override
    public ITimeWindowedUnboundedStream<T>
    windowBy(TimeAmount window)
    {
        return null;
    }

    @Override
    public IExecutableUnboundedStream<T>
    filter(IPredicate<? super T> predicate)
    {
        return null;
    }

    @Override
    public <R> IExecutableUnboundedStream<R>
    map(IFunction<? super T,? extends R> mapper)
    {
        return null;
    }

    @Override
    public <R> IExecutableUnboundedStream<R>
    flatMap(IFunction<T,Iterable<R>> mapper)
    {
        return null;
    }

    @Override
    public IUnboundedStreamExecutor
    forEach(IConsumer<? super T> action)
    {
        return null;
    }

    @Override
    public IUnboundedStreamExecutor
    sinkTo(IUnboundedStreamSink<T> sink)
    {
        return null;
    }
}

//////////////////////////////////////////////////////////////////////////////

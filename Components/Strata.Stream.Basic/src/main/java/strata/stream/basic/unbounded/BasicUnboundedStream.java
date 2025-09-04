/// ///////////////////////////////////////////////////////////////////////////
// BasicUnboundedStream.java
//////////////////////////////////////////////////////////////////////////////

package strata.stream.basic.unbounded;

import strata.stream.core.shared.*;
import strata.stream.core.unbounded.*;

import java.util.stream.Stream;

public
class BasicUnboundedStream<T>
    implements IUnboundedStream<T>
{
    private final Stream<T>   implementation;
    private final IController controller;

    public
    BasicUnboundedStream(Stream<T> implementation,IController controller)
    {
        this.implementation = implementation;
        this.controller     = controller;
    }

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

    public static <T> BasicUnboundedStream<T>
    of(Stream<T> implementation,IController controller)
    {
        return new BasicUnboundedStream<>(implementation,controller);
    }
}

//////////////////////////////////////////////////////////////////////////////

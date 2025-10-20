//////////////////////////////////////////////////////////////////////////////
// IUnboundedStream.java
//////////////////////////////////////////////////////////////////////////////

package strata.stream.core.unbounded;

import strata.stream.core.shared.*;

import java.time.Duration;

public
interface IUnboundedStream<T>
    extends IStream<T>
{
    <K> IKeyedUnboundedStream<K,T>
    keyBy(IKeySelector<K,T> selector);

    IWindowedUnboundedStream<T>
    windowBy(WindowPlan plan);

    IWindowedUnboundedStream<T>
    windowBy(Duration duration);

    IWindowedUnboundedStream<T>
    windowBy(int count);

    @Override
    IExecutableUnboundedStream<T>
    filter(IPredicate<? super T> predicate);

    @Override
    <R> IExecutableUnboundedStream<R>
    map(IFunction<? super T,? extends R> mapper);

    @Override
    <R> IExecutableUnboundedStream<R>
    flatMap(IFunction<T,Iterable<R>> mapper);

    IUnboundedStreamExecutor
    forEach(IConsumer<? super T> action);

    IUnboundedStreamExecutor
    sinkTo(IUnboundedStreamSink<T> sink);
}

//////////////////////////////////////////////////////////////////////////////
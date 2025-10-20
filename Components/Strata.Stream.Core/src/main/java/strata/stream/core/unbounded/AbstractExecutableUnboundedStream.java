//////////////////////////////////////////////////////////////////////////////
// AbstractExecutableUnboundedStream.java
//////////////////////////////////////////////////////////////////////////////

package strata.stream.core.unbounded;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import strata.stream.core.shared.*;

import java.time.Duration;

public abstract
class AbstractExecutableUnboundedStream<T>
    implements IExecutableUnboundedStream<T>
{
    private final Logger logger;

    protected
    AbstractExecutableUnboundedStream()
    {
        logger = LogManager.getLogger(getClass());
    }

    @Override
    public <K> IKeyedUnboundedStream<K,T>
    keyBy(IKeySelector<K,T> selector)
    {
        return getStream().keyBy(selector);
    }

    @Override
    public IWindowedUnboundedStream<T>
    windowBy(WindowPlan plan)
    {
        return getStream().windowBy(plan);
    }

    @Override
    public IWindowedUnboundedStream<T>
    windowBy(Duration duration)
    {
        return getStream().windowBy(duration);
    }

    @Override
    public IWindowedUnboundedStream<T>
    windowBy(int count)
    {
        return getStream().windowBy(count);
    }

    @Override
    public IExecutableUnboundedStream<T>
    filter(IPredicate<? super T> predicate)
    {
        return getStream().filter(predicate);
    }

    @Override
    public <R> IExecutableUnboundedStream<R>
    map(IFunction<? super T,? extends R> mapper)
    {
        return getStream().map(mapper);
    }

    @Override
    public <R> IExecutableUnboundedStream<R>
    flatMap(IFunction<T,Iterable<R>> mapper)
    {
        return getStream().flatMap(mapper);
    }

    @Override
    public IUnboundedStreamExecutor
    forEach(IConsumer<? super T> action)
    {
        return getStream().forEach(action);
    }

    @Override
    public IUnboundedStreamExecutor
    sinkTo(IUnboundedStreamSink<T> sink)
    {
        return getStream().sinkTo(sink);
    }

    protected abstract IUnboundedStream<T>
    getStream();
}

//////////////////////////////////////////////////////////////////////////////

//////////////////////////////////////////////////////////////////////////////
// AbstractExecutableUnboundedStream.java
//////////////////////////////////////////////////////////////////////////////

package strata.stream.core.unbounded;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import strata.stream.core.shared.*;

import java.util.concurrent.CompletionStage;

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
        logger.debug("keyBy({})", selector);
        return getStream().keyBy(selector);
    }

    @Override
    public ITimeWindowedUnboundedStream<T>
    windowBy(TimeAmount window)
    {
        logger.debug("windowBy({})", window);
        return getStream().windowBy(window);
    }

    @Override
    public IExecutableUnboundedStream<T>
    filter(IPredicate<? super T> predicate)
    {
        logger.debug("filter({})", predicate);
        return getStream().filter(predicate);
    }

    @Override
    public <R> IExecutableUnboundedStream<R>
    map(IFunction<? super T,? extends R> mapper)
    {
        logger.debug("map({})", mapper);
        return getStream().map(mapper);
    }

    @Override
    public <R> IExecutableUnboundedStream<R>
    flatMap(IFunction<T,Iterable<R>> mapper)
    {
        logger.debug("flatMap({})", mapper);
        return getStream().flatMap(mapper);
    }

    @Override
    public IUnboundedStreamExecutor
    forEach(IConsumer<? super T> action)
    {
        logger.debug("forEach({})", action);
        return getStream().forEach(action);
    }

    @Override
    public IUnboundedStreamExecutor
    sinkTo(IUnboundedStreamSink<T> sink)
    {
        logger.debug("sinkTo({})", sink);
        return getStream().sinkTo(sink);
    }

    protected abstract IUnboundedStream<T>
    getStream();
}

//////////////////////////////////////////////////////////////////////////////

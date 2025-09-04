//////////////////////////////////////////////////////////////////////////////
// FlinkKeyedUnboundedStream.java
//////////////////////////////////////////////////////////////////////////////

package strata.stream.flink.unbounded;

import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import strata.stream.core.shared.*;
import strata.stream.core.unbounded.*;

public
class FlinkKeyedUnboundedStream<K,T>
    implements IKeyedUnboundedStream<K,T>
{
    private final KeyedStream<T,K> implementation;

    public
    FlinkKeyedUnboundedStream(KeyedStream<T,K> imp)
    {
        implementation = imp;
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
        return
            new FlinkExecutableUnboundedStream<>(
                new FlinkUnboundedStream<>(
                    implementation.filter(e -> predicate.test(e))));
    }

    @Override
    public <R> IExecutableUnboundedStream<R>
    map(IFunction<? super T,? extends R> mapper)
    {
        return
            new FlinkExecutableUnboundedStream<>(
                new FlinkUnboundedStream<>(
                    implementation.map(e -> mapper.apply(e))));
    }

    @Override
    public <R> IExecutableUnboundedStream<R>
    flatMap(IFunction<T,Iterable<R>> mapper)
    {
        return
            new FlinkExecutableUnboundedStream<>(
                new FlinkUnboundedStream<>(
                    implementation.flatMap(
                        new FlinkFlatMapFunctionAdapter<>(mapper))));
    }

    @Override
    public IUnboundedStreamExecutor
    forEach(IConsumer<? super T> action)
    {
        return
            new FlinkUnboundedStreamExecutor(
                implementation
                    .map(e -> {action.accept(e);return e;})
                    .getExecutionEnvironment());
    }

    @Override
    public IUnboundedStreamExecutor
    sinkTo(IUnboundedStreamSink<T> sink)
    {
        implementation.sinkTo(new SinkAdapter<>(sink));

        return
            new FlinkUnboundedStreamExecutor(getEnvironment());
    }

    protected StreamExecutionEnvironment
    getEnvironment() { return implementation.getExecutionEnvironment(); }
}

//////////////////////////////////////////////////////////////////////////////

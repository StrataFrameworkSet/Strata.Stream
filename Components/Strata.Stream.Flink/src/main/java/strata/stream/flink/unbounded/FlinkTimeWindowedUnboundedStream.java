//////////////////////////////////////////////////////////////////////////////
// FlinkTimeWindowedUnboundedStream.java
//////////////////////////////////////////////////////////////////////////////

package strata.stream.flink.unbounded;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.AllWindowedStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import strata.stream.core.bounded.IBoundedStream;
import strata.stream.core.shared.*;
import strata.stream.core.unbounded.*;

import java.time.Duration;

public
class FlinkTimeWindowedUnboundedStream<T>
    implements ITimeWindowedUnboundedStream<T>
{
    private final DataStream<IBoundedStream<T>> implementation;

    public
    FlinkTimeWindowedUnboundedStream(AllWindowedStream<T,TimeWindow> imp)
    {
        implementation =
            imp.apply(new StreamOfStreamsConverter<T,Void>());

    }

    public <K>
    FlinkTimeWindowedUnboundedStream(WindowedStream<T,K,TimeWindow> imp)
    {
        implementation =
            imp.apply(new StreamOfStreamsConverter<T,K>());
    }

    @Override
    public <K> IKeyedUnboundedStream<K,IBoundedStream<T>>
    keyBy(IKeySelector<K,IBoundedStream<T>> selector)
    {
        return
            new FlinkKeyedUnboundedStream<>(
                implementation.keyBy(e -> selector.getKey(e)));
    }

    @Override
    public ITimeWindowedUnboundedStream<IBoundedStream<T>>
    windowBy(TimeAmount window)
    {
        return
            new FlinkTimeWindowedUnboundedStream<>(
                implementation.windowAll(
                    TumblingEventTimeWindows.of(toDuration(window))));
    }

    @Override
    public IExecutableUnboundedStream<IBoundedStream<T>>
    filter(IPredicate<? super IBoundedStream<T>> predicate)
    {
        return
            new FlinkExecutableUnboundedStream<>(
                new FlinkUnboundedStream<>(
                    implementation.filter(e -> predicate.test(e))));
    }

    @Override
    public <R> IExecutableUnboundedStream<R>
    map(IFunction<? super IBoundedStream<T>,? extends R> mapper)
    {
        return
            new FlinkExecutableUnboundedStream<>(
                new FlinkUnboundedStream<>(
                    implementation.map(e -> mapper.apply(e))));
    }

    @Override
    public <R> IExecutableUnboundedStream<R>
    flatMap(IFunction<IBoundedStream<T>,Iterable<R>> mapper)
    {
        return
            new FlinkExecutableUnboundedStream<>(
                new FlinkUnboundedStream<>(
                    implementation.flatMap(
                        new FlinkFlatMapFunctionAdapter<>(mapper))));
    }

    @Override
    public IUnboundedStreamExecutor
    forEach(IConsumer<? super IBoundedStream<T>> action)
    {
        return
            new FlinkUnboundedStreamExecutor(
                implementation
                    .map(e -> {action.accept(e);return e;})
                    .getExecutionEnvironment());
    }

    @Override
    public IUnboundedStreamExecutor
    sinkTo(IUnboundedStreamSink<IBoundedStream<T>> sink)
    {
        sink.accept(this);
        return new FlinkUnboundedStreamExecutor(getEnvironment());
    }

    protected StreamExecutionEnvironment
    getEnvironment() { return implementation.getExecutionEnvironment(); }

    private Duration
    toDuration(TimeAmount window)
    {
        return Duration.of(window.getSize(),window.getUnits().toChronoUnit());
    }

}

//////////////////////////////////////////////////////////////////////////////

//////////////////////////////////////////////////////////////////////////////
// FlinkTimeWindowedUnboundedStream.java
//////////////////////////////////////////////////////////////////////////////

package strata.stream.flink.unbounded;

import org.apache.flink.streaming.api.datastream.AllWindowedStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import strata.foundation.core.collection.Pair;
import strata.stream.core.bounded.IBoundedStream;
import strata.stream.core.shared.*;
import strata.stream.core.unbounded.*;

import java.time.Duration;

public
class FlinkCountWindowedUnboundedStream<T>
    implements IWindowedUnboundedStream<T>
{
    private final DataStream<IBoundedStream<T>> implementation;

    public FlinkCountWindowedUnboundedStream(AllWindowedStream<T,GlobalWindow> imp)
    {
        implementation =
            imp.apply(new StreamOfStreamsConverter<T,Void,GlobalWindow>());

    }

    public <K>
    FlinkCountWindowedUnboundedStream(WindowedStream<T,K,GlobalWindow> imp)
    {
        implementation =
            imp.apply(new StreamOfStreamsConverter<>());
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
    public IWindowedUnboundedStream<IBoundedStream<T>>
    windowBy(WindowPlan plan)
    {
        Duration duration = null;
        Integer  count    = null;

        if (plan.isPrimary(Duration.class))
        {
            Pair<Duration,Integer> pair = plan.getDurationOrCount();

            duration = pair.getFirst();
            count    = pair.getSecond();
        }
        else
        {
            Pair<Integer,Duration> pair = plan.getCountOrDuration();

            count = pair.getFirst();
            duration = pair.getSecond();
        }

        return
            new FlinkTimeWindowedUnboundedStream<>(
                implementation
                    .windowAll(TumblingEventTimeWindows.of(duration))
                    .trigger(DurationLimitedCountTrigger.of(count)));
    }

    @Override
    public IWindowedUnboundedStream<IBoundedStream<T>>
    windowBy(Duration duration)
    {
        return
            new FlinkTimeWindowedUnboundedStream<>(
                implementation.windowAll(
                    TumblingEventTimeWindows.of(duration)));
    }

    @Override
    public IWindowedUnboundedStream<IBoundedStream<T>>
    windowBy(int count)
    {
        return
            new FlinkCountWindowedUnboundedStream<>(
                implementation.countWindowAll(count));
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

}

//////////////////////////////////////////////////////////////////////////////

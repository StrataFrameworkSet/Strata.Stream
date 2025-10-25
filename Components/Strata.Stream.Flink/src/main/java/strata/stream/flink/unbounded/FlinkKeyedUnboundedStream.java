//////////////////////////////////////////////////////////////////////////////
// FlinkKeyedUnboundedStream.java
//////////////////////////////////////////////////////////////////////////////

package strata.stream.flink.unbounded;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import strata.foundation.core.collection.Pair;
import strata.stream.core.shared.*;
import strata.stream.core.unbounded.*;

import java.time.Duration;

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
        return
            new FlinkKeyedUnboundedStream<>(
                implementation.keyBy(e -> selector.getKey(e)));
    }

    @Override
    public IWindowedUnboundedStream<T>
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
    public IWindowedUnboundedStream<T>
    windowBy(Duration duration)
    {
        return
            new FlinkTimeWindowedUnboundedStream<>(
                implementation
                    .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                            .<T>forMonotonousTimestamps()
                            .withTimestampAssigner(new TimestampAssigner<>()))
                    .windowAll(TumblingEventTimeWindows.of(duration)));
    }

    @Override
    public IWindowedUnboundedStream<T>
    windowBy(int count)
    {
        return
            new FlinkCountWindowedUnboundedStream<>(
                implementation.countWindowAll(count));
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

    @SuppressWarnings("unchecked")
    @Override
    public IUnboundedStreamExecutor
    sinkTo(IUnboundedStreamSink<? super T> sink)
    {
        implementation.sinkTo(new SinkAdapter<>((IUnboundedStreamSink<T>)sink));

        return
            new FlinkUnboundedStreamExecutor(getEnvironment());
    }

    protected StreamExecutionEnvironment
    getEnvironment() { return implementation.getExecutionEnvironment(); }
}

//////////////////////////////////////////////////////////////////////////////

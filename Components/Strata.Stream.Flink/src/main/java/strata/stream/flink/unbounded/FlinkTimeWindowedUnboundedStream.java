//////////////////////////////////////////////////////////////////////////////
// FlinkTimeWindowedUnboundedStream.java
//////////////////////////////////////////////////////////////////////////////

package strata.stream.flink.unbounded;

import org.apache.flink.streaming.api.datastream.AllWindowedStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import strata.foundation.core.collection.ICollection;
import strata.foundation.core.collection.Pair;
import strata.stream.core.bounded.IBoundedStream;
import strata.stream.core.shared.*;
import strata.stream.core.unbounded.*;

import java.time.Duration;

public
class FlinkTimeWindowedUnboundedStream<T>
    implements IWindowedUnboundedStream<T>
{
    private final DataStream<ICollection<T>> implementation;

    public
    FlinkTimeWindowedUnboundedStream(AllWindowedStream<T,TimeWindow> imp)
    {
        implementation =
            imp.apply(new StreamOfListsConverter<T,Void,TimeWindow>());

    }

    public <K>
    FlinkTimeWindowedUnboundedStream(WindowedStream<T,K,TimeWindow> imp)
    {
        implementation =
            imp.apply(new StreamOfListsConverter<>());
    }


    @Override
    public <K> IKeyedUnboundedStream<K,ICollection<T>>
    keyBy(IKeySelector<K,ICollection<T>> selector)
    {
        return
            new FlinkKeyedUnboundedStream<>(
                implementation.keyBy(e -> selector.getKey(e)));
    }

    @Override
    public IWindowedUnboundedStream<ICollection<T>>
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
    public IWindowedUnboundedStream<ICollection<T>>
    windowBy(Duration duration)
    {
        return
            new FlinkTimeWindowedUnboundedStream<>(
                implementation.windowAll(TumblingEventTimeWindows.of(duration)));
    }

    @Override
    public IWindowedUnboundedStream<ICollection<T>>
    windowBy(int count)
    {
        return null;
    }

    @Override
    public IExecutableUnboundedStream<ICollection<T>>
    filter(IPredicate<? super ICollection<T>> predicate)
    {
        return
            new FlinkExecutableUnboundedStream<>(
                new FlinkUnboundedStream<>(
                    implementation.filter(e -> predicate.test(e))));
    }

    @Override
    public <R> IExecutableUnboundedStream<R>
    map(IFunction<? super ICollection<T>,? extends R> mapper)
    {
        return
            new FlinkExecutableUnboundedStream<>(
                new FlinkUnboundedStream<>(
                    implementation.map(e -> mapper.apply(e))));
    }

    @Override
    public <R> IExecutableUnboundedStream<R>
    flatMap(IFunction<ICollection<T>,Iterable<R>> mapper)
    {
        return
            new FlinkExecutableUnboundedStream<>(
                new FlinkUnboundedStream<>(
                    implementation.flatMap(
                        new FlinkFlatMapFunctionAdapter<>(mapper))));
    }

    @Override
    public IUnboundedStreamExecutor
    forEach(IConsumer<? super ICollection<T>> action)
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
    sinkTo(IUnboundedStreamSink<? super ICollection<T>> sink)
    {
        ((IUnboundedStreamSink<ICollection<T>>)sink).accept(this);
        return new FlinkUnboundedStreamExecutor(getEnvironment());
    }

    protected StreamExecutionEnvironment
    getEnvironment() { return implementation.getExecutionEnvironment(); }

}

//////////////////////////////////////////////////////////////////////////////

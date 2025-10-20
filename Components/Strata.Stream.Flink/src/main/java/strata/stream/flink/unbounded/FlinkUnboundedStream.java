//////////////////////////////////////////////////////////////////////////////
// FlinkUnboundedStream.java
//////////////////////////////////////////////////////////////////////////////

package strata.stream.flink.unbounded;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import strata.foundation.core.collection.Pair;
import strata.stream.core.shared.*;
import strata.stream.core.unbounded.*;

import java.time.Duration;

public
class FlinkUnboundedStream<T>
    implements IUnboundedStream<T>
{
    private final DataStream<T> implementation;
    private final Logger        logger;

    public
    FlinkUnboundedStream(DataStream<T> imp)
    {
        implementation = imp;
        logger         = LogManager.getLogger(getClass());
    }

    @Override
    public <K> IKeyedUnboundedStream<K,T>
    keyBy(IKeySelector<K,T> selector)
    {
        logger.debug("keyBy({})",selector);
        return
            new FlinkKeyedUnboundedStream<>(
                implementation.keyBy(e -> selector.getKey(e)));
    }

    @Override
    public IWindowedUnboundedStream<T>
    windowBy(WindowPlan plan)
    {
        logger.debug("windowBy({})",plan);
        Duration duration = null;
        Integer  count    = null;

        if (plan.isPrimary(Duration.class))
        {
            Pair<Duration,Integer> pair = plan.getDurationOrCount();

            duration = pair.getFirst();
            count = pair.getSecond();
        }
        else if (plan.isPrimary(Integer.class))
        {
            Pair<Integer,Duration> pair = plan.getCountOrDuration();

            count = pair.getFirst();
            duration = pair.getSecond();
        }

        return
            new FlinkTimeWindowedUnboundedStream<>(
                implementation
                    .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                            .<T>forMonotonousTimestamps()
                            .withTimestampAssigner(new TimestampAssigner<>()))
                    .windowAll(TumblingEventTimeWindows.of(duration))
                    .trigger(DurationLimitedCountTrigger.of(count)));
    }

    @Override
    public IWindowedUnboundedStream<T>
    windowBy(Duration duration)
    {
        logger.debug("windowBy({})",duration);
        return
            new FlinkTimeWindowedUnboundedStream<>(
                implementation
                    .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                            .<T>forMonotonousTimestamps()
                            .withTimestampAssigner(new TimestampAssigner<>()))
                    .windowAll(
                        TumblingEventTimeWindows.of(duration)));
    }

    @Override
    public IWindowedUnboundedStream<T>
    windowBy(int count)
    {
        logger.debug("windowBy({})",count);
        return
            new FlinkCountWindowedUnboundedStream<>(
                implementation
                    .countWindowAll(count));
    }

    @Override
    public IExecutableUnboundedStream<T>
    filter(IPredicate<? super T> predicate)
    {
        logger.debug("filter({})",predicate);
        return
            new FlinkExecutableUnboundedStream<>(
                new FlinkUnboundedStream<>(
                    implementation.filter(e -> predicate.test(e))));
    }

    @Override
    public <R> IExecutableUnboundedStream<R>
    map(IFunction<? super T,? extends R> mapper)
    {
        logger.debug("map({})",mapper);
        return
            new FlinkExecutableUnboundedStream<>(
                new FlinkUnboundedStream<>(
                    implementation.map(e -> mapper.apply(e))));
    }

    @Override
    public <R> IExecutableUnboundedStream<R>
    flatMap(IFunction<T,Iterable<R>> mapper)
    {
        logger.debug("flatMap({})",mapper);
        return
            new FlinkExecutableUnboundedStream<>(
                new FlinkUnboundedStream<>(
                    implementation
                        .flatMap(new FlinkFlatMapFunctionAdapter<>(mapper))));
    }

    @Override
    public IUnboundedStreamExecutor
    forEach(IConsumer<? super T> action)
    {
        logger.debug("forEach({})",action);
        return
            new FlinkUnboundedStreamExecutor(
                ((FlinkExecutableUnboundedStream<T>)
                     map(e -> {action.accept(e); return e;}))
                    .getStream()
                    .getEnvironment());
    }

    @Override
    public IUnboundedStreamExecutor
    sinkTo(IUnboundedStreamSink<T> sink)
    {
        logger.debug("sinkTo({})",sink);
        implementation.sinkTo(new SinkAdapter<>(sink));

        return
            new FlinkUnboundedStreamExecutor(getEnvironment());
    }

    protected StreamExecutionEnvironment
    getEnvironment()
    {
        return implementation.getExecutionEnvironment();
    }

    protected DataStream<T>
    getImplementation() { return implementation; }

}

//////////////////////////////////////////////////////////////////////////////

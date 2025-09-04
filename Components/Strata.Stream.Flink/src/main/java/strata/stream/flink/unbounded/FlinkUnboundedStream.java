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
    public ITimeWindowedUnboundedStream<T>
    windowBy(TimeAmount window)
    {
        logger.debug("windowBy({})",window);
        return
            new FlinkTimeWindowedUnboundedStream<>(
                implementation
                    .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                            .<T>forMonotonousTimestamps()
                            .withTimestampAssigner(new TimestampAssigner<>()))
                    .windowAll(
                        TumblingEventTimeWindows.of(toDuration(window))));
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

    private Duration
    toDuration(TimeAmount window)
    {
        return Duration.of(window.getSize(),window.getUnits().toChronoUnit());
    }
}

//////////////////////////////////////////////////////////////////////////////

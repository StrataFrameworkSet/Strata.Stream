//////////////////////////////////////////////////////////////////////////////
// AbstractKafkaUnboundedStream.java
//////////////////////////////////////////////////////////////////////////////

package strata.stream.kafka.unbounded;

import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import strata.stream.core.shared.*;
import strata.stream.core.unbounded.*;

import java.time.Duration;

public abstract
class AbstractKafkaUnboundedStream<K,T>
    implements IUnboundedStream<T>
{
    private final KafkaStreamWithBuilder<K,T> implementation;
    private final Logger                      logger;

    protected
    AbstractKafkaUnboundedStream(KStream<K,T> s,StreamsBuilder b)
    {
        implementation = new KafkaStreamWithBuilder<>(s,b);
        logger = LogManager.getLogger(getClass());
    }

    @Override
    public <K2> IKeyedUnboundedStream<K2,T>
    keyBy(IKeySelector<K2,T> selector)
    {
        logger.debug("keyBy({})", selector.getClass().getName());
        return
            new KafkaKeyedUnboundedStream<K2,T>(
                implementation.getStream(),
                implementation.getBuilder(),
                selector);
    }

    @Override
    public IWindowedUnboundedStream<T>
    windowBy(WindowPlan plan)
    {
        throw new UnsupportedOperationException("Not yet implemented");
    }

    @Override
    public IWindowedUnboundedStream<T>
    windowBy(Duration duration)
    {
        logger.debug("windowBy({})", duration.getClass().getName());
        return
            new KafkaTimeWindowedUnboundedStream<>(
                implementation.getStream(),
                implementation.getBuilder(),
                duration);
    }

    @Override
    public IWindowedUnboundedStream<T>
    windowBy(int count)
    {
        throw new UnsupportedOperationException("Not yet implemented");
    }

    @Override
    public IExecutableUnboundedStream<T>
    filter(IPredicate<? super T> predicate)
    {
        logger.debug("filter({})", predicate.getClass().getName());
        return
            new KafkaExecutableUnboundedStream<>(
                new KafkaUnboundedStream<>(
                    implementation
                        .getStream()
                        .filter((k,v) -> predicate.test(v)),
                    implementation.getBuilder()));
    }

    @Override
    public <R> IExecutableUnboundedStream<R>
    map(IFunction<? super T,? extends R> mapper)
    {
        logger.debug("map({})", mapper.getClass().getName());
        return
            new KafkaExecutableUnboundedStream<>(
                new KafkaUnboundedStream<>(
                    implementation
                        .getStream()
                        .mapValues((k,v) -> mapper.apply(v)),
                    implementation.getBuilder()));
    }

    @Override
    public <R> IExecutableUnboundedStream<R>
    flatMap(IFunction<T,Iterable<R>> mapper)
    {
        logger.debug("flatMap({})", mapper.getClass().getName());
        return
            new KafkaExecutableUnboundedStream<>(
                new KafkaUnboundedStream<>(
                    implementation
                        .getStream()
                        .flatMapValues(
                            new KafkaFlatMapFunctionAdapter<T,R>(mapper)),
                    implementation.getBuilder()));
    }

    @Override
    public IUnboundedStreamExecutor
    forEach(IConsumer<? super T> action)
    {
        logger.debug("forEach({})", action.getClass().getName());
        implementation
            .getStream()
            .foreach((k,v) -> action.accept(v));
        return new KafkaUnboundedStreamExecutor(implementation.getBuilder());
    }

    @SuppressWarnings("unchecked")
    @Override
    public IUnboundedStreamExecutor
    sinkTo(IUnboundedStreamSink<? super T> sink)
    {
        logger.debug("sinkTo({})",sink.getClass().getName());
        ((IUnboundedStreamSink<T>)sink).accept(this);
        return new KafkaUnboundedStreamExecutor(implementation.getBuilder());
    }

    public KStream<K,T>
    getImplementation() { return implementation.getStream(); }

    public StreamsBuilder
    getBuilder() { return implementation.getBuilder(); }
}

//////////////////////////////////////////////////////////////////////////////

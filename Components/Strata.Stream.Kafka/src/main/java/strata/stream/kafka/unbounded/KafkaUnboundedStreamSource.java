//////////////////////////////////////////////////////////////////////////////
// KafkaUnboundedStreamSource.java
//////////////////////////////////////////////////////////////////////////////

package strata.stream.kafka.unbounded;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import strata.stream.core.unbounded.AbstractUnboundedStreamSource;
import strata.stream.core.unbounded.IUnboundedStream;

import java.util.Optional;

public
class KafkaUnboundedStreamSource<K,T>
    extends AbstractUnboundedStreamSource<T>
{
    private final Optional<IUnboundedStream<T>> stream;

    public
    KafkaUnboundedStreamSource(Class<K> keyType,Class<T> valueType,String topic)
    {
        StreamsBuilder builder = new StreamsBuilder();

        stream =
            Optional.ofNullable(
                new KafkaUnboundedStream<>(
                    builder
                        .stream(
                            topic,
                            Consumed.with(
                                ReflectiveSerde.of(keyType),
                                ReflectiveSerde.of(valueType))),
                    builder));

    }

    @Override
    protected IUnboundedStream<T>
    getStream()
    {
        return stream.get();
    }

    @Override
    public boolean
    hasSource()
    {
        return stream.isPresent();
    }
}

//////////////////////////////////////////////////////////////////////////////

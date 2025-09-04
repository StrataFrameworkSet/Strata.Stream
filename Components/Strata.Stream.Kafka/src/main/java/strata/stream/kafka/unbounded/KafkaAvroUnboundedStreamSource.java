//////////////////////////////////////////////////////////////////////////////
// KafkaUnboundedStreamSource.java
//////////////////////////////////////////////////////////////////////////////

package strata.stream.kafka.unbounded;

import io.confluent.kafka.streams.serdes.avro.PrimitiveAvroSerde;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import strata.stream.core.unbounded.AbstractUnboundedStreamSource;
import strata.stream.core.unbounded.IUnboundedStream;

import java.util.Optional;

public
class KafkaAvroUnboundedStreamSource<K,T extends SpecificRecord>
    extends AbstractUnboundedStreamSource<T>
{
    private final String                        topic;
    private final Optional<IUnboundedStream<T>> stream;

    public
    KafkaAvroUnboundedStreamSource(String top)
    {
        StreamsBuilder builder = new StreamsBuilder();

        topic  = top;
        stream =
            Optional.of(
                new KafkaUnboundedStream<>(
                    builder
                        .stream(
                            topic,
                            Consumed.with(
                                new PrimitiveAvroSerde<K>(),
                                new SpecificAvroSerde<T>())),
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

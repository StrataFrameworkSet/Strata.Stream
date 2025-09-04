//////////////////////////////////////////////////////////////////////////////
// KafkaUnboundedStream.java
//////////////////////////////////////////////////////////////////////////////

package strata.stream.kafka.unbounded;

import io.confluent.kafka.streams.serdes.avro.PrimitiveAvroSerde;
import io.confluent.kafka.streams.serdes.avro.ReflectionAvroSerde;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serdes.ListSerde;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.TimeWindows;
import strata.stream.core.bounded.IBoundedStream;
import strata.stream.core.shared.TimeAmount;
import strata.stream.core.unbounded.ITimeWindowedUnboundedStream;

import java.util.ArrayList;

public
class KafkaTimeWindowedUnboundedStream<K,T>
    extends AbstractKafkaUnboundedStream<K,IBoundedStream<T>>
    implements ITimeWindowedUnboundedStream<T>
{
    @SuppressWarnings("unchecked")
    public KafkaTimeWindowedUnboundedStream(
        KStream<K,T>   s,
        StreamsBuilder b,
        TimeAmount     window)
    {
        super(
            s
                .groupBy((k,v) -> (K)"WINDOW-KEY")
                .windowedBy(TimeWindows.ofSizeWithNoGrace(window.toDuration()))
                .aggregate(
                    () -> new ArrayList<>(),
                    new ListAggregator<>(),
                    Materialized.with(
                        Serdes.String(),
                        new ListSerde<>(
                            new ArrayList<T>().getClass(),
                            Serdes.String())))
                .toStream()
                .map(new BoundedStreamMapper<>()),
            b);
    }
}

//////////////////////////////////////////////////////////////////////////////

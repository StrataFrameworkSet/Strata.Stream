//////////////////////////////////////////////////////////////////////////////
// KafkaUnboundedStream.java
//////////////////////////////////////////////////////////////////////////////

package strata.stream.kafka.unbounded;

import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.kstream.Suppressed.BufferConfig;
import strata.stream.core.bounded.IBoundedStream;
import strata.stream.core.shared.TimeAmount;
import strata.stream.core.unbounded.ITimeWindowedUnboundedStream;

import java.util.ArrayList;
import java.util.List;

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
        super(convertToWindowedStream(s,window),b);
    }

    private static <K> K
    getZeroKey(K originalKey)
    {
        return
            ZeroKeyMapper
                .getInstance()
                .map(originalKey)
                .orElse((K)"");
    }

    private static <K,T> KStream<K,IBoundedStream<T>>
    convertToWindowedStream(
        KStream<K,T> input,
        TimeAmount   window)
    {
        TimeWindowedKStream<K,T> windowed =
            input
                .groupBy((k,v) -> getZeroKey(k))
                .windowedBy(TimeWindows.ofSizeWithNoGrace(window.toDuration()));
        KTable<Windowed<K>,List<T>> aggregated =
            windowed
                .aggregate(
                    () -> new ArrayList<>(),
                    new ListAggregator<>(),
                    Materialized.with(
                        new SerializableSerde<>(),
                        new SerializableSerde<>()))
                .suppress(
                    Suppressed.untilWindowCloses(BufferConfig.unbounded()));
        KStream<K,IBoundedStream<T>> output =
            aggregated
                .toStream()
                .map(new BoundedStreamMapper<>());

        return output;
    }
}

//////////////////////////////////////////////////////////////////////////////

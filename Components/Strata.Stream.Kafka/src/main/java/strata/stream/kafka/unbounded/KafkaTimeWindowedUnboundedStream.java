//////////////////////////////////////////////////////////////////////////////
// KafkaUnboundedStream.java
//////////////////////////////////////////////////////////////////////////////

package strata.stream.kafka.unbounded;

import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.kstream.Suppressed.BufferConfig;
import strata.foundation.core.collection.ICollection;
import strata.stream.core.bounded.IBoundedStream;
import strata.stream.core.unbounded.IWindowedUnboundedStream;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

public
class KafkaTimeWindowedUnboundedStream<K,T>
    extends AbstractKafkaUnboundedStream<K,ICollection<T>>
    implements IWindowedUnboundedStream<T>
{
    @SuppressWarnings("unchecked")
    public
    KafkaTimeWindowedUnboundedStream(
        KStream<K,T>   s,
        StreamsBuilder b,
        Duration duration)
    {
        super(convertToWindowedStream(s,duration),b);
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

    private static <K,T> KStream<K,ICollection<T>>
    convertToWindowedStream(
        KStream<K,T> input,
        Duration     duration)
    {
        TimeWindowedKStream<K,T> windowed =
            input
                .groupBy((k,v) -> getZeroKey(k))
                .windowedBy(TimeWindows.ofSizeWithNoGrace(duration));
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
        KStream<K,ICollection<T>> output =
            aggregated
                .toStream()
                .map(new ListMapper<>());

        return output;
    }
}

//////////////////////////////////////////////////////////////////////////////

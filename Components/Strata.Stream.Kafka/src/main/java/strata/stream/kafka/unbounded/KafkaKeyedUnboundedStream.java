//////////////////////////////////////////////////////////////////////////////
// KafkaKeyedUnboundedStream.java
//////////////////////////////////////////////////////////////////////////////

package strata.stream.kafka.unbounded;

import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;
import strata.stream.core.shared.IKeySelector;
import strata.stream.core.unbounded.IKeyedUnboundedStream;

public
class KafkaKeyedUnboundedStream<K,T>
    extends AbstractKafkaUnboundedStream<K,T>
    implements IKeyedUnboundedStream<K,T>
{
    public <KO>
    KafkaKeyedUnboundedStream(KStream<KO,T> s,StreamsBuilder b,IKeySelector<K,T> selector)
    {
        super(
            s
                .selectKey((k,v) -> selector.getKey(v))
                .repartition(),
            b);
    }
}

//////////////////////////////////////////////////////////////////////////////
